import Darwin
import Foundation
import os

private struct BrowserLinkNodeScopes: Codable {
    let current: String
    let reclaim: [String]
}

enum BrowserLinkFileSystemError: LocalizedError {
    case remote(String)

    var errorDescription: String? {
        switch self {
        case .remote(let code): code
        }
    }
}

actor BrowserLinkStorageClient: RemoteStorageClientProtocol {
    private enum DownloadClass: Hashable {
        case data
        case lock

        var limit: Int {
            switch self {
            case .data: 2
            case .lock: 1
            }
        }
    }

    private static let writerID: String = {
        let key = "browser-link-writer-id"
        let stored = UserDefaults.standard.string(forKey: key)
        if let canonical = canonicalWriterID(stored) {
            if canonical != stored { UserDefaults.standard.set(canonical, forKey: key) }
            return canonical
        }
        let value = UUID().uuidString.lowercased()
        UserDefaults.standard.set(value, forKey: key)
        return value
    }()

    nonisolated static func canonicalWriterID(_ value: String?) -> String? {
        guard let value, let uuid = UUID(uuidString: value) else { return nil }
        return uuid.uuidString.lowercased()
    }

    nonisolated static func canonicalBrowserNodeID(_ value: String?) -> String? {
        guard let value,
              value.count == 43,
              let data = Data(base64URLEncoded: value),
              data.count == 32,
              data.base64URLEncodedString() == value else { return nil }
        return value
    }

    private struct Entry: Decodable {
        let path: String
        let name: String
        let isDirectory: Bool
        let size: Int64
        let creationDateMs: Int64?
        let modificationDateMs: Int64?
    }

    private struct DownloadInfo: Decodable {
        let transferID: String
        let size: Int64
    }

    private let client: BrowserLinkClient
    private var timestampToolsInstalled = false
    private var timestampToolsInstallationTask: Task<Bool, Never>?
    private var activeDownloads: [DownloadClass: Int] = [:]
    private var downloadWaiters: [DownloadClass: [UUID: CheckedContinuation<Void, Error>]] = [:]
    private var downloadWaiterOrder: [DownloadClass: [UUID]] = [:]
    private var reservedDownloadBytes: Int64 = 0

    init(client: BrowserLinkClient) {
        self.client = client
    }

    nonisolated static func makeProfile(
        pairing: BrowserLinkPairing,
        folderName: String,
        browserNodeID: String? = nil,
        reclaimBrowserNodeIDs: [String] = []
    ) -> ServerProfileRecord {
        let now = Date()
        let currentScope = canonicalBrowserNodeID(browserNodeID)
        let reclaimScopes = Array(Set(reclaimBrowserNodeIDs.compactMap(canonicalBrowserNodeID))).sorted()
        let scopes = currentScope.flatMap {
            try? JSONEncoder().encode(BrowserLinkNodeScopes(current: $0, reclaim: reclaimScopes))
        }
        return ServerProfileRecord(
            id: nil,
            name: folderName,
            storageType: StorageType.externalVolume.rawValue,
            connectionParams: scopes,
            sortOrder: 0,
            host: "local-browser",
            port: 0,
            shareName: "browser-link-\(pairing.sessionID)",
            basePath: "/",
            username: "",
            domain: nil,
            credentialRef: ServerProfileRecord.browserLinkCredentialRef(sessionID: pairing.sessionID),
            backgroundBackupEnabled: false,
            backgroundBackupMinIntervalMinutes: 1440,
            backgroundBackupRequiresWiFi: true,
            generateRemoteThumbnails: false,
            createdAt: now,
            updatedAt: now,
            writerID: writerID
        )
    }

    nonisolated func shouldSetModificationDate() -> Bool { false }

    nonisolated func shouldLimitUploadRetries(for _: Error) -> Bool { true }

    func connect() async throws {
        let ready = await client.isFileSystemReady
        browserLinkLog.info("Storage connect ready=\(ready)")
        guard ready else {
            throw RemoteStorageClientError.notConnected
        }
        await installTimestampToolsIfNeeded()
    }

    func disconnect() async {}

    func verifyWriteAccess() async throws {
        browserLinkLog.info("Storage write verification started")
        let path = "/.watermelon-link-write-test-\(UUID().uuidString.lowercased())"
        let temporary = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data().write(to: temporary)
        defer { try? FileManager.default.removeItem(at: temporary) }
        try await upload(localURL: temporary, remotePath: path, mode: .createIfAbsent, respectTaskCancellation: true, onProgress: nil)
        try await delete(path: path)
        await installTimestampToolsIfNeeded()
        browserLinkLog.info("Storage write verification succeeded")
    }

    private func installTimestampToolsIfNeeded() async {
        guard !timestampToolsInstalled else { return }
        if let timestampToolsInstallationTask {
            _ = await timestampToolsInstallationTask.value
            return
        }
        let task = Task { [self] in
            await BrowserLinkTimestampArtifacts.installTools(client: self, basePath: "/")
        }
        timestampToolsInstallationTask = task
        timestampToolsInstalled = await task.value
        timestampToolsInstallationTask = nil
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        guard let parentPath = Self.canonicalRemotePath(path) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let data = try await request("list", ["path": path])
        let entries = try JSONDecoder().decode([Entry].self, from: data)
        guard entries.count <= 100_000 else {
            throw Self.invalidRemoteEntryError()
        }
        return try entries.map { entry in
            guard let expectedPath = Self.joinRemotePath(parent: parentPath, name: entry.name) else {
                throw Self.invalidRemoteEntryError()
            }
            return try Self.validatedRemoteEntry(entry, expectedPath: expectedPath, rootNameAllowed: false)
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        guard let expectedPath = Self.canonicalRemotePath(path) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let data: Data
        do {
            data = try await request("metadata", ["path": path])
        } catch {
            if RemoteFaultLite.classify(error) == .notFound { return nil }
            throw error
        }
        if data == Data("null".utf8) { return nil }
        return try Self.validatedRemoteEntry(
            JSONDecoder().decode(Entry.self, from: data),
            expectedPath: expectedPath,
            rootNameAllowed: expectedPath == "/"
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await upload(
            localURL: localURL,
            remotePath: remotePath,
            mode: .replace,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        if respectTaskCancellation { try Task.checkCancellation() }
        let transferID = UUID().uuidString.lowercased()
        let attributes = try FileManager.default.attributesOfItem(atPath: localURL.path)
        guard let sizeValue = attributes[.size] as? NSNumber else {
            throw CocoaError(.fileReadUnknown)
        }
        let size = sizeValue.int64Value
        let control = Self.isWriteLockPath(remotePath)
        let priority = Self.requestPriority(
            operation: "upload_begin",
            arguments: ["path": remotePath]
        )
        let startedAt = Date()
        browserLinkLog.info("Upload started bytes=\(size) mode=\(String(describing: mode), privacy: .public)")
        do {
            _ = try await request("upload_begin", [
                "transferID": transferID,
                "path": remotePath,
                "mode": {
                    switch mode {
                    case .replace: "replace"
                    case .createIfAbsent: "create_if_absent"
                    }
                }(),
                "size": size,
            ], priority: priority, respectTaskCancellation: false)
            if respectTaskCancellation { try Task.checkCancellation() }
        } catch {
            scheduleUploadAbort(transferID: transferID)
            throw error
        }

        let handle: FileHandle
        do {
            handle = try FileHandle(forReadingFrom: localURL)
        } catch {
            scheduleUploadAbort(transferID: transferID)
            throw error
        }
        defer { try? handle.close() }
        let chunkSize = await client.uploadChunkBytes
        var transferred: Int64 = 0
        do {
            try await client.beginFileSystemUpload(
                transferID: transferID,
                expectedSize: size,
                control: control
            )
            while true {
                if respectTaskCancellation { try Task.checkCancellation() }
                let chunk = try handle.read(upToCount: chunkSize) ?? Data()
                if chunk.isEmpty { break }
                try await client.sendFileSystemUploadChunk(
                    transferID: transferID,
                    offset: transferred,
                    payload: chunk,
                    respectTaskCancellation: respectTaskCancellation
                )
                transferred += Int64(chunk.count)
                onProgress?(size > 0 ? min(1, Double(transferred) / Double(size)) : 1)
            }
            _ = try await request(
                "upload_finish",
                ["transferID": transferID],
                priority: priority,
                respectTaskCancellation: false
            )
            if let failure = await client.fileSystemUploadFailure(transferID: transferID) {
                throw failure
            }
            await client.endFileSystemUpload(transferID: transferID)
            onProgress?(1)
            let elapsed = max(0.001, Date().timeIntervalSince(startedAt))
            let mebibytesPerSecond = Double(transferred) / 1_048_576 / elapsed
            browserLinkLog.info("Upload completed bytes=\(transferred) seconds=\(elapsed) MiBps=\(mebibytesPerSecond)")
        } catch {
            let streamFailure = await client.fileSystemUploadFailure(transferID: transferID)
            await client.endFileSystemUpload(transferID: transferID)
            browserLinkLog.error("Upload failed afterBytes=\(transferred) type=\(String(reflecting: type(of: error)), privacy: .public)")
            scheduleUploadAbort(transferID: transferID)
            throw Self.mappedFileSystemError(
                Self.preferredUploadError(requestError: error, streamFailure: streamFailure),
                arguments: ["path": remotePath]
            )
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {}

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        try await download(
            remotePath: remotePath,
            localURL: localURL,
            expectedSize: nil,
            onProgress: onProgress
        )
    }

    func download(
        remotePath: String,
        localURL: URL,
        expectedSize: Int64?,
        onProgress: ((Double) -> Void)?
    ) async throws {
        for attempt in 0..<3 {
            do {
                try await performDownload(
                    remotePath: remotePath,
                    localURL: localURL,
                    expectedSize: expectedSize,
                    onProgress: onProgress
                )
                return
            } catch {
                guard attempt < 2, Self.isRetryableTransferError(error) else { throw error }
                try Task.checkCancellation()
                try await Task.sleep(for: .milliseconds(250 * (attempt + 1)))
            }
        }
    }

    private func performDownload(
        remotePath: String,
        localURL: URL,
        expectedSize: Int64?,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let downloadClass: DownloadClass = Self.isWriteLockPath(remotePath) ? .lock : .data
        let priority: BrowserLinkClient.FileSystemRequestPriority = downloadClass == .lock ? .control : .ordinary
        try await acquireDownloadSlot(downloadClass)
        defer { releaseDownloadSlot(downloadClass) }
        try Task.checkCancellation()
        let infoData = try await request(
            "download_begin",
            ["path": remotePath],
            priority: priority,
            respectTaskCancellation: false
        )
        let info: DownloadInfo
        do {
            info = try JSONDecoder().decode(DownloadInfo.self, from: infoData)
        } catch {
            if let object = try? JSONSerialization.jsonObject(with: infoData) as? [String: Any],
               let transferID = object["transferID"] as? String,
               UUID(uuidString: transferID)?.uuidString.lowercased() == transferID {
                _ = try? await request(
                    "download_abort",
                    ["transferID": transferID],
                    priority: .cleanup,
                    respectTaskCancellation: false
                )
            }
            throw Self.mappedFileSystemError(
                BrowserLinkFileSystemError.remote("invalid_size"),
                arguments: ["path": remotePath]
            )
        }
        guard UUID(uuidString: info.transferID)?.uuidString.lowercased() == info.transferID else {
            if !info.transferID.isEmpty {
                await abortPreparedDownload(transferID: info.transferID)
            }
            throw Self.mappedFileSystemError(
                BrowserLinkFileSystemError.remote("invalid_size"),
                arguments: ["path": remotePath]
            )
        }
        let capacityValues = try? FileManager.default.temporaryDirectory.resourceValues(forKeys: [
            .volumeAvailableCapacityForImportantUsageKey,
            .volumeAvailableCapacityKey,
        ])
        let availableCapacity = capacityValues?.volumeAvailableCapacityForImportantUsage
            ?? capacityValues?.volumeAvailableCapacity.map(Int64.init)
        switch BrowserLinkDownloadAdmissionPolicy.decision(
            size: info.size,
            expectedSize: expectedSize,
            availableCapacity: availableCapacity,
            reservedCapacity: reservedDownloadBytes,
            remotePath: remotePath
        ) {
        case .accepted:
            break
        case .invalidSize:
            await abortPreparedDownload(transferID: info.transferID)
            throw Self.mappedFileSystemError(
                BrowserLinkFileSystemError.remote("invalid_size"),
                arguments: ["path": remotePath]
            )
        case .insufficientCapacity:
            await abortPreparedDownload(transferID: info.transferID)
            throw RemoteStorageClientError.underlying(CocoaError(.fileWriteOutOfSpace))
        }
        reservedDownloadBytes += info.size
        var reservedRemainingBytes = info.size
        defer { reservedDownloadBytes = max(0, reservedDownloadBytes - reservedRemainingBytes) }
        browserLinkLog.info("Download started bytes=\(info.size)")
        var offset: Int64 = 0
        do {
            let chunks = try await client.beginFileSystemDownload(
                transferID: info.transferID,
                expectedSize: info.size
            )
            try Task.checkCancellation()
            try FileManager.default.createDirectory(
                at: localURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            if FileManager.default.fileExists(atPath: localURL.path) {
                try FileManager.default.removeItem(at: localURL)
            }
            guard FileManager.default.createFile(atPath: localURL.path, contents: nil) else {
                throw CocoaError(.fileWriteUnknown)
            }
            let handle = try FileHandle(forWritingTo: localURL)
            var handleNeedsClose = true
            defer {
                if handleNeedsClose { try? handle.close() }
            }
            _ = try await request(
                "download_start",
                ["transferID": info.transferID],
                priority: priority
            )
            try await client.startFileSystemDownload(transferID: info.transferID)
            var iterator = chunks.makeAsyncIterator()
            while offset < info.size {
                try Task.checkCancellation()
                let next = try await iterator.next()
                try Task.checkCancellation()
                guard let bytes = next,
                      !bytes.isEmpty,
                      Int64(bytes.count) <= info.size - offset else {
                    throw BrowserLinkFileSystemError.remote("invalid_chunk")
                }
                try handle.write(contentsOf: bytes)
                let writtenBytes = Int64(bytes.count)
                offset += writtenBytes
                reservedRemainingBytes -= writtenBytes
                reservedDownloadBytes = max(0, reservedDownloadBytes - writtenBytes)
                try await client.acknowledgeFileSystemDownload(
                    transferID: info.transferID,
                    receivedSize: offset
                )
                onProgress?(info.size > 0 ? min(1, Double(offset) / Double(info.size)) : 1)
            }
            try handle.close()
            handleNeedsClose = false
            await client.cancelFileSystemDownloadIdleTimeout(transferID: info.transferID)
            _ = try await request(
                "download_finish",
                ["transferID": info.transferID],
                priority: priority,
                respectTaskCancellation: false
            )
            await client.endFileSystemDownload(transferID: info.transferID)
        } catch {
            browserLinkLog.error("Download failed afterBytes=\(offset) type=\(String(reflecting: type(of: error)), privacy: .public)")
            await client.abandonFileSystemDownload(
                transferID: info.transferID,
                error: error
            )
            try? FileManager.default.removeItem(at: localURL)
            throw Self.mappedFileSystemError(error, arguments: ["path": remotePath])
        }
        onProgress?(1)
        browserLinkLog.info("Download completed bytes=\(offset)")
    }

    private func abortPreparedDownload(transferID: String) async {
        _ = try? await request(
            "download_abort",
            ["transferID": transferID],
            priority: .cleanup,
            respectTaskCancellation: false
        )
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func delete(path: String) async throws {
        do {
            _ = try await request("delete", ["path": path], respectTaskCancellation: false)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound { return }
            throw error
        }
    }

    func createDirectory(path: String) async throws {
        _ = try await request("create_directory", ["path": path], respectTaskCancellation: false)
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        _ = try await request(
            "move",
            ["sourcePath": sourcePath, "destinationPath": destinationPath],
            respectTaskCancellation: false
        )
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        _ = try await request(
            "copy",
            ["sourcePath": sourcePath, "destinationPath": destinationPath],
            respectTaskCancellation: false
        )
    }

    private func request(
        _ operation: String,
        _ arguments: [String: Any],
        priority: BrowserLinkClient.FileSystemRequestPriority? = nil,
        respectTaskCancellation: Bool = true
    ) async throws -> Data {
        let data = try JSONSerialization.data(withJSONObject: arguments)
        do {
            return try await client.fileSystemRequest(
                operation: operation,
                arguments: data,
                priority: priority ?? Self.requestPriority(operation: operation, arguments: arguments),
                respectTaskCancellation: respectTaskCancellation
            )
        } catch {
            throw Self.mappedFileSystemError(error, arguments: arguments)
        }
    }

    nonisolated static func mappedFileSystemError(_ error: Error, arguments: [String: Any]) -> Error {
        if let clientError = error as? BrowserLinkClientError {
            if case .connectionClosed = clientError {
                return RemoteStorageClientError.notConnected
            }
            return clientError
        }
        if let fileSystemError = error as? BrowserLinkFileSystemError {
            switch fileSystemError {
            case .remote(let code):
                switch code {
                case "not_found":
                    return NSError(domain: NSPOSIXErrorDomain, code: Int(ENOENT))
                case "name_collision":
                    return remoteStorageNameCollisionError(
                        path: arguments["path"] as? String ?? arguments["destinationPath"] as? String ?? ""
                    )
                case "channel_closed":
                    return RemoteStorageClientError.notConnected
                case "too_many_transfers", "transfer_timeout":
                    return NSError(
                        domain: NSURLErrorDomain,
                        code: NSURLErrorTimedOut,
                        userInfo: [
                            NSLocalizedDescriptionKey: code,
                            transferErrorCodeKey: code,
                        ]
                    )
                case "quota_exceeded":
                    return RemoteStorageClientError.underlying(CocoaError(.fileWriteOutOfSpace))
                case "permission_denied":
                    return RemoteStorageClientError.underlying(CocoaError(.fileWriteNoPermission))
                case "not_readable":
                    return RemoteStorageClientError.underlying(CocoaError(.fileReadNoPermission))
                case "type_mismatch", "invalid_range", "response_too_large":
                    return RemoteStorageClientError.underlying(BrowserLinkFileSystemError.remote(code))
                default:
                    return RemoteStorageClientError.underlying(BrowserLinkFileSystemError.remote(code))
                }
            }
        }
        return error
    }

    nonisolated static func isRetryableTransferError(_ error: Error) -> Bool {
        let nsError = error as NSError
        return nsError.domain == NSURLErrorDomain &&
            nsError.userInfo[transferErrorCodeKey] as? String != nil
    }

    nonisolated static func preferredUploadError(requestError: Error, streamFailure: Error?) -> Error {
        streamFailure ?? requestError
    }

    private func scheduleUploadAbort(transferID: String) {
        Task { [weak self] in
            guard let self else { return }
            _ = try? await self.request(
                "upload_abort",
                ["transferID": transferID],
                priority: .cleanup,
                respectTaskCancellation: false
            )
        }
    }

    private nonisolated static func isWriteLockPath(_ path: String) -> Bool {
        path.range(
            of: #"^/\.watermelon/locks/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.lock$"#,
            options: .regularExpression
        ) != nil
    }

    private nonisolated static let transferErrorCodeKey = "BrowserLinkFileSystemCode"

    nonisolated static func requestPriority(
        operation: String,
        arguments: [String: Any]
    ) -> BrowserLinkClient.FileSystemRequestPriority {
        if operation == "download_abort" || operation == "upload_abort" { return .cleanup }
        let lockDirectory = "/.watermelon/locks"
        let isLockRead: (String) -> Bool = { $0 == lockDirectory || isWriteLockPath($0) }
        let mutatesLockNamespace: (String) -> Bool = {
            $0 == "/" || $0 == "/.watermelon" || $0 == lockDirectory || $0.hasPrefix("\(lockDirectory)/")
        }
        switch operation {
        case "list", "metadata", "download_begin":
            guard let path = arguments["path"] as? String else { return .ordinary }
            return isLockRead(path) ? .control : .ordinary
        case "create_directory", "delete", "upload_begin":
            guard let path = arguments["path"] as? String else { return .ordinary }
            return mutatesLockNamespace(path) ? .control : .ordinary
        case "copy", "move":
            let paths = [arguments["sourcePath"], arguments["destinationPath"]].compactMap { $0 as? String }
            return paths.contains(where: mutatesLockNamespace) ? .control : .ordinary
        default:
            return .ordinary
        }
    }

    func acquireLockDownloadSlot() async throws {
        try await acquireDownloadSlot(.lock)
    }

    func releaseLockDownloadSlot() {
        releaseDownloadSlot(.lock)
    }

    func acquireDataDownloadSlot() async throws {
        try await acquireDownloadSlot(.data)
    }

    func releaseDataDownloadSlot() {
        releaseDownloadSlot(.data)
    }

    private func acquireDownloadSlot(_ downloadClass: DownloadClass) async throws {
        if activeDownloads[downloadClass, default: 0] < downloadClass.limit {
            activeDownloads[downloadClass, default: 0] += 1
            return
        }
        guard (downloadWaiters[downloadClass]?.count ?? 0) < 16 else {
            throw RemoteStorageClientError.unavailable
        }
        let waiterID = UUID()
        let timeoutTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(300))
            } catch {
                return
            }
            await self?.finishDownloadWaiter(
                downloadClass,
                waiterID,
                result: .failure(RemoteStorageClientError.unavailable)
            )
        }
        defer { timeoutTask.cancel() }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                downloadWaiters[downloadClass, default: [:]][waiterID] = continuation
                downloadWaiterOrder[downloadClass, default: []].append(waiterID)
                if Task.isCancelled {
                    finishDownloadWaiter(
                        downloadClass,
                        waiterID,
                        result: .failure(CancellationError())
                    )
                }
            }
        } onCancel: {
            Task { [weak self] in
                await self?.finishDownloadWaiter(
                    downloadClass,
                    waiterID,
                    result: .failure(CancellationError())
                )
            }
        }
    }

    private func releaseDownloadSlot(_ downloadClass: DownloadClass) {
        while let waiterID = downloadWaiterOrder[downloadClass]?.first {
            downloadWaiterOrder[downloadClass]?.removeFirst()
            if let continuation = downloadWaiters[downloadClass]?.removeValue(forKey: waiterID) {
                continuation.resume()
                return
            }
        }
        activeDownloads[downloadClass] = max(0, activeDownloads[downloadClass, default: 0] - 1)
    }

    private func finishDownloadWaiter(
        _ downloadClass: DownloadClass,
        _ id: UUID,
        result: Result<Void, Error>
    ) {
        guard let continuation = downloadWaiters[downloadClass]?.removeValue(forKey: id) else { return }
        downloadWaiterOrder[downloadClass]?.removeAll { $0 == id }
        continuation.resume(with: result)
    }

    nonisolated static func canonicalRemotePath(_ path: String) -> String? {
        guard path.utf8.count <= 4_096,
              !path.contains("\0") else {
            return nil
        }
        let components = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        guard components.allSatisfy({ RemotePathBuilder.isSafePathComponent($0) }) else {
            return nil
        }
        return components.isEmpty ? "/" : "/" + components.joined(separator: "/")
    }

    nonisolated static func joinRemotePath(parent: String, name: String) -> String? {
        guard RemotePathBuilder.isSafePathComponent(name),
              let canonicalParent = canonicalRemotePath(parent) else {
            return nil
        }
        return canonicalParent == "/" ? "/\(name)" : "\(canonicalParent)/\(name)"
    }

    private nonisolated static func validatedRemoteEntry(
        _ entry: Entry,
        expectedPath: String,
        rootNameAllowed: Bool
    ) throws -> RemoteStorageEntry {
        guard entry.size >= 0,
              validatesRemoteEntryPath(
                path: entry.path,
                name: entry.name,
                expectedPath: expectedPath,
                rootNameAllowed: rootNameAllowed
              ),
              let canonicalPath = canonicalRemotePath(entry.path) else {
            throw invalidRemoteEntryError()
        }
        return RemoteStorageEntry(
            path: canonicalPath,
            name: entry.name,
            isDirectory: entry.isDirectory,
            size: entry.size,
            creationDate: entry.creationDateMs.map { Date(timeIntervalSince1970: Double($0) / 1_000) },
            modificationDate: entry.modificationDateMs.map { Date(timeIntervalSince1970: Double($0) / 1_000) }
        )
    }

    nonisolated static func validatesRemoteEntryPath(
        path: String,
        name: String,
        expectedPath: String,
        rootNameAllowed: Bool
    ) -> Bool {
        guard let canonicalPath = canonicalRemotePath(path),
              canonicalPath == expectedPath,
              RemotePathBuilder.isSafePathComponent(name) else {
            return false
        }
        return rootNameAllowed || canonicalPath.split(separator: "/").last.map(String.init) == name
    }

    private nonisolated static func invalidRemoteEntryError() -> Error {
        RemoteStorageClientError.underlying(BrowserLinkFileSystemError.remote("invalid_response"))
    }
}

extension ServerProfileRecord {
    var browserLinkFreshTakeoverScopes: Set<String> {
        browserLinkNodeScopes?.reclaim
            .reduce(into: Set<String>()) { $0.insert($1) } ?? []
    }

    var browserLinkCurrentLockScope: String? {
        browserLinkNodeScopes?.current
    }

    private var browserLinkNodeScopes: BrowserLinkNodeScopes? {
        guard isBrowserLinkProfile,
              let connectionParams,
              let scopes = try? JSONDecoder().decode(BrowserLinkNodeScopes.self, from: connectionParams),
              BrowserLinkStorageClient.canonicalBrowserNodeID(scopes.current) != nil,
              scopes.reclaim.count <= 16,
              Set(scopes.reclaim).count == scopes.reclaim.count,
              scopes.reclaim.allSatisfy({ BrowserLinkStorageClient.canonicalBrowserNodeID($0) != nil }) else {
            return nil
        }
        return scopes
    }
}
