import Foundation

#if canImport(AMSMB2)
import AMSMB2
#endif

final class AMSMB2Client: RemoteStorageClientProtocol, @unchecked Sendable {
    private let config: SMBServerConfig

    #if canImport(AMSMB2)
    // connect() may swap this to the IPv4-resolved manager; safe under the connect-before-use invariant
    // (no operation runs against the client until connect() has completed).
    private var manager: SMB2Manager
    private let credential: URLCredential
    private let abandonmentHandle: SMBAbandonmentHandle
    #endif

    init(config: SMBServerConfig) throws {
        self.config = config

        #if canImport(AMSMB2)
        let user = [config.domain, config.username]
            .compactMap { value -> String? in
                guard let value, !value.isEmpty else { return nil }
                return value
            }
            .joined(separator: ";")

        let credential = URLCredential(user: user.isEmpty ? config.username : user, password: config.password, persistence: .forSession)
        self.credential = credential

        guard let manager = Self.makeManager(host: config.host, port: config.port, credential: credential) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        self.manager = manager
        self.abandonmentHandle = SMBAbandonmentHandle(manager: manager)
        #endif
    }

    #if canImport(AMSMB2)
    private static func makeManager(host: String, port: Int, credential: URLCredential) -> SMB2Manager? {
        guard let url = SMBEndpoint.url(host: host, port: port) else { return nil }
        return SMB2Manager(url: url, credential: credential)
    }
    #endif

    func shouldSetModificationDate() -> Bool {
        true
    }

    func cancelActiveOperationsForAbandonment() {
        #if canImport(AMSMB2)
        abandonmentHandle.abandon()
        #endif
    }

    func connect() async throws {
        #if canImport(AMSMB2)
        guard let normalizedHost = RemoteHostEndpoint.socketHost(config.host, strippingSMBScheme: true) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if let ip = await HostnameResolver.resolvedIPv4(normalizedHost),
           ip != normalizedHost,
           let ipManager = Self.makeManager(host: ip, port: config.port, credential: credential) {
            guard abandonmentHandle.install(ipManager) else { throw CancellationError() }
            do {
                try await ipManager.connectShare(name: config.shareName)
                manager = ipManager
                return
            } catch {
                if error is CancellationError || Task.isCancelled { throw error }
                // IPv4 fast path failed (stale/unreachable record); retry the original hostname below.
            }
        }
        try Task.checkCancellation()
        guard abandonmentHandle.install(manager) else { throw CancellationError() }
        try await manager.connectShare(name: config.shareName)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func disconnect() async {
        #if canImport(AMSMB2)
        try? await manager.disconnectShare(gracefully: false)
        #endif
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        #if canImport(AMSMB2)
        let queryPath = RemotePathBuilder.normalizePath(config.basePath)
        let attributes = try await manager.attributesOfFileSystem(forPath: queryPath)

        let available = (attributes[.systemFreeSize] as? NSNumber)?.int64Value
        let total = (attributes[.systemSize] as? NSNumber)?.int64Value
        if available == nil, total == nil {
            return nil
        }
        return RemoteStorageCapacity(availableBytes: available, totalBytes: total)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        #if canImport(AMSMB2)
        let remotePath = RemotePathBuilder.normalizePath(path)
        let items = try await manager.contentsOfDirectory(atPath: remotePath, recursive: false)

        return items.compactMap { values in
            let name = (values[.nameKey] as? String) ?? ""
            if name == "." || name == ".." { return nil }

            let isDirectory = (values[.isDirectoryKey] as? Bool) ?? false
            let sizeValue = values[.fileSizeKey] as? NSNumber
            let creationDate = values[.creationDateKey] as? Date
            let modificationDate = values[.contentModificationDateKey] as? Date

            let fullPath = RemotePathBuilder.normalizePath(remotePath + "/" + name)
            return RemoteStorageEntry(
                path: fullPath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        #if canImport(AMSMB2)
        let remotePath = RemotePathBuilder.normalizePath(path)
        do {
            let values = try await manager.attributesOfItem(atPath: remotePath)
            let name = (values[.nameKey] as? String) ?? (remotePath as NSString).lastPathComponent
            let isDirectory = (values[.isDirectoryKey] as? Bool)
                ?? ((values[.fileResourceTypeKey] as? URLFileResourceType) == .directory)
            let sizeValue = values[.fileSizeKey] as? NSNumber
            let creationDate = values[.creationDateKey] as? Date
            let modificationDate = values[.contentModificationDateKey] as? Date

            return RemoteStorageEntry(
                path: remotePath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return nil
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
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
        #if canImport(AMSMB2)
        let expectedByteCount = Self.fileSizeInBytes(for: localURL)
        let normalizedRemotePath = RemotePathBuilder.normalizePath(remotePath)
        do {
            try await manager.uploadItem(
                at: localURL,
                toPath: normalizedRemotePath,
                overwrite: mode == .replace,
                progress: { value in
                    if let normalized = Self.normalizedProgressValue(value, expectedByteCount: expectedByteCount) {
                        onProgress?(normalized)
                    }
                    guard respectTaskCancellation else { return true }
                    return !Task.isCancelled
                }
            )
            if respectTaskCancellation, Task.isCancelled {
                if mode == .replace {
                    await cleanupCancelledUploadIfNeeded(
                        remotePath: normalizedRemotePath
                    )
                }
                throw CancellationError()
            }
        } catch {
            if respectTaskCancellation, Task.isCancelled {
                if mode == .replace {
                    await cleanupCancelledUploadIfNeeded(
                        remotePath: normalizedRemotePath
                    )
                }
                throw CancellationError()
            }
            // A failed write after the destructive open leaves a torn body: `.replace`
            // (FILE_OVERWRITE_IF) already truncated any prior content, and a non-collision
            // `.createIfAbsent` exclusive-created the file. Remove it either way so a torn own lock from a
            // failed refresh/claim can't wedge the next write-lock acquire — matching SFTP/LocalVolume
            // `.replace` cleanup. Only a `.createIfAbsent` collision (pre-existing file) is left intact.
            let isCreateIfAbsentCollision = mode == .createIfAbsent && SMBErrorClassifier.isNameCollision(error)
            if !isCreateIfAbsentCollision {
                await cleanupCancelledUploadIfNeeded(remotePath: normalizedRemotePath)
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        #if canImport(AMSMB2)
        guard let safeDate = Self.safeSMBFileDate(date) else { return }
        try await manager.setAttributes(
            attributes: [.contentModificationDateKey: safeDate],
            ofItemAtPath: RemotePathBuilder.normalizePath(path)
        )
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    private static func normalizedProgressValue<T>(_ value: T, expectedByteCount: Int64?) -> Double? {
        if let double = value as? Double {
            return normalizedFraction(fromRawProgress: double, expectedByteCount: expectedByteCount)
        }
        if let float = value as? Float {
            return normalizedFraction(fromRawProgress: Double(float), expectedByteCount: expectedByteCount)
        }
        if let number = value as? NSNumber {
            return normalizedFraction(fromRawProgress: number.doubleValue, expectedByteCount: expectedByteCount)
        }
        return nil
    }

    private static func normalizedFraction(fromRawProgress raw: Double, expectedByteCount: Int64?) -> Double {
        let nonNegativeRaw = max(raw, 0)
        if nonNegativeRaw <= 1 {
            return min(max(nonNegativeRaw, 0), 1)
        }
        guard let expectedByteCount, expectedByteCount > 0 else {
            return min(max(nonNegativeRaw, 0), 1)
        }
        return min(max(nonNegativeRaw / Double(expectedByteCount), 0), 1)
    }

    private static func fileSizeInBytes(for fileURL: URL) -> Int64? {
        guard let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey]),
              let fileSize = values.fileSize else {
            return nil
        }
        return Int64(fileSize)
    }

    static func safeSMBFileDate(_ date: Date) -> Date? {
        let seconds = date.timeIntervalSince1970
        // AMSMB2 converts Date to timespec synchronously and traps on non-finite values.
        guard seconds.isFinite else { return nil }

        let conservativeMaximumSeconds: TimeInterval = 253_402_300_799 // 9999-12-31 23:59:59 UTC
        guard seconds >= 0 && seconds <= conservativeMaximumSeconds else { return nil }
        return date
    }

    private func cleanupCancelledUploadIfNeeded(remotePath: String) async {
        #if canImport(AMSMB2)
        do {
            let values = try await manager.attributesOfItem(atPath: remotePath)
            guard ((values[.isDirectoryKey] as? Bool) ?? false) == false else { return }
            try? await manager.removeItem(atPath: remotePath)
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return
            }
        }
        #endif
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        #if canImport(AMSMB2)
        try await manager.downloadItem(
            atPath: RemotePathBuilder.normalizePath(remotePath),
            to: localURL,
            progress: { receivedBytes, expectedBytes in
                if expectedBytes > 0 {
                    let progress = min(max(Double(receivedBytes) / Double(expectedBytes), 0), 1)
                    onProgress?(progress)
                }
                return !Task.isCancelled
            }
        )
        // A cancelled transfer stops mid-stream, leaving a truncated file that must never be treated as a
        // complete download (e.g. cached as a valid original). Remove it and surface the cancellation.
        if Task.isCancelled {
            try? FileManager.default.removeItem(at: localURL)
            throw CancellationError()
        }
        onProgress?(1.0)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func exists(path: String) async throws -> Bool {
        #if canImport(AMSMB2)
        do {
            _ = try await manager.attributesOfItem(atPath: RemotePathBuilder.normalizePath(path))
            return true
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return false
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func delete(path: String) async throws {
        #if canImport(AMSMB2)
        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        do {
            try await manager.removeItem(atPath: normalized)
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func createDirectory(path: String) async throws {
        #if canImport(AMSMB2)
        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else { return }

        var runningPath = ""
        let components = normalized.split(separator: "/")
        for component in components {
            runningPath += "/\(component)"
            if try await exists(path: runningPath) {
                continue
            }
            do {
                try await manager.createDirectory(atPath: runningPath)
            } catch {
                if !(try await exists(path: runningPath)) {
                    throw error
                }
            }
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        #if canImport(AMSMB2)
        try await manager.moveItem(atPath: RemotePathBuilder.normalizePath(sourcePath), toPath: RemotePathBuilder.normalizePath(destinationPath))
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        #if canImport(AMSMB2)
        let normalizedSource = RemotePathBuilder.normalizePath(sourcePath)
        let normalizedDestination = RemotePathBuilder.normalizePath(destinationPath)
        // AMSMB2 has no native copy on the protocol; round-trip via a local temp file.
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("smb-copy-\(UUID().uuidString)")
        do {
            try await manager.downloadItem(atPath: normalizedSource, to: tempURL, progress: nil)
            try await manager.uploadItem(at: tempURL, toPath: normalizedDestination, overwrite: true, progress: nil)
        } catch {
            try? FileManager.default.removeItem(at: tempURL)
            throw error
        }
        try? FileManager.default.removeItem(at: tempURL)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }
}

#if canImport(AMSMB2)
private final class SMBAbandonmentHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var manager: SMB2Manager?
    private var abandoned = false

    init(manager: SMB2Manager) {
        self.manager = manager
    }

    func install(_ manager: SMB2Manager) -> Bool {
        let shouldAbort = lock.withLock {
            if abandoned {
                return true
            }
            self.manager = manager
            return false
        }
        if shouldAbort {
            manager.disconnectShare(gracefully: false, completionHandler: nil)
        }
        return !shouldAbort
    }

    func abandon() {
        let manager = lock.withLock { () -> SMB2Manager? in
            guard !abandoned else { return nil }
            abandoned = true
            return self.manager
        }
        manager?.disconnectShare(gracefully: false, completionHandler: nil)
    }
}
#endif

nonisolated enum SMBErrorClassifier {
    // Clear object/path/file absence only. Share/client/session/backend faults (BAD_NETWORK_NAME,
    // REDIRECTOR_NOT_STARTED) are deliberately NOT here — a transient share/redirector outage must not
    // read as "the object isn't there" and let a Lite reconcile prune a still-present month.
    private static let notFoundStatusTokens: Set<String> = [
        "0XC000000F", // STATUS_NO_SUCH_FILE
        "0XC0000034", // STATUS_OBJECT_NAME_NOT_FOUND
        "0XC000003A", // STATUS_OBJECT_PATH_NOT_FOUND
        "0XC0000225", // STATUS_NOT_FOUND
        "STATUS_NO_SUCH_FILE",
        "STATUS_OBJECT_NAME_NOT_FOUND",
        "STATUS_OBJECT_PATH_INVALID",
        "STATUS_OBJECT_PATH_NOT_FOUND",
        "STATUS_OBJECT_PATH_SYNTAX_BAD",
        "STATUS_DFS_EXIT_PATH_FOUND",
        "STATUS_DELETE_PENDING",
        "STATUS_NOT_FOUND"
    ]

    private static let nameCollisionStatusTokens: Set<String> = [
        "0XC0000035", // STATUS_OBJECT_NAME_COLLISION
        "STATUS_OBJECT_NAME_COLLISION"
    ]

    // Share/client/session/backend faults: the connection or share is the problem, not a missing object.
    // BAD_NETWORK_NAME (share unreachable) and REDIRECTOR_NOT_STARTED (local SMB client not up) live here
    // so they classify retryable, never notFound.
    private static let connectionUnavailableStatusTokens: Set<String> = [
        "0XC0000037", // STATUS_PORT_DISCONNECTED
        "0XC00000B5", // STATUS_IO_TIMEOUT
        "0XC00000C3", // STATUS_INVALID_NETWORK_RESPONSE
        "0XC00000C9", // STATUS_NETWORK_NAME_DELETED
        "0XC00000CC", // STATUS_BAD_NETWORK_NAME
        "0XC00000FB", // STATUS_REDIRECTOR_NOT_STARTED
        "0XC0000128", // STATUS_FILE_CLOSED
        "0XC000020C", // STATUS_CONNECTION_DISCONNECTED
        "0XC000020D", // STATUS_CONNECTION_RESET
        "0XC000023A", // STATUS_CONNECTION_INVALID
        "0XC0000241", // STATUS_CONNECTION_ABORTED
        "0XC000026E", // STATUS_VOLUME_DISMOUNTED
        "STATUS_PORT_DISCONNECTED",
        "STATUS_IO_TIMEOUT",
        "STATUS_INVALID_NETWORK_RESPONSE",
        "STATUS_NETWORK_NAME_DELETED",
        "STATUS_BAD_NETWORK_NAME",
        "STATUS_REDIRECTOR_NOT_STARTED",
        "STATUS_FILE_CLOSED",
        "STATUS_CONNECTION_DISCONNECTED",
        "STATUS_CONNECTION_RESET",
        "STATUS_CONNECTION_INVALID",
        "STATUS_CONNECTION_ABORTED",
        "STATUS_VOLUME_DISMOUNTED",
        // Server-busy / explicit-retry statuses: back off and reconnect rather than failing the asset.
        "0XC0000205", // STATUS_INSUFF_SERVER_RESOURCES
        "0XC00000BF", // STATUS_NETWORK_BUSY
        "0XC000022D", // STATUS_RETRY
        "STATUS_INSUFF_SERVER_RESOURCES",
        "STATUS_NETWORK_BUSY",
        "STATUS_RETRY"
    ]

    private static let connectionUnavailablePOSIXCodes: Set<POSIXErrorCode> = [
        .ENETRESET,
        .ETIMEDOUT,
        .ENOTCONN,
        .ECONNRESET,
        .ECONNABORTED
    ]

    static func isNotFound(_ error: Error) -> Bool {
        // Fail closed on a mixed chain: when the same chain also carries a connection-unavailable /
        // backend / session token, the outcome is "couldn't tell", not "object absent". Raw consumers
        // (metadata / exists / delete) must not collapse such a chain into absence or success — this
        // mirrors RemoteFaultLite.classify's retryable-before-notFound priority at the raw seam.
        if isConnectionUnavailable(error) {
            return false
        }
        if containsAnyStatusToken(notFoundStatusTokens, in: error) {
            return true
        }
        return posixErrorCodes(in: error).contains(.ENOENT)
    }

    static func isNameCollision(_ error: Error) -> Bool {
        if containsAnyStatusToken(nameCollisionStatusTokens, in: error) {
            return true
        }
        return posixErrorCodes(in: error).contains(.EEXIST)
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        if containsAnyStatusToken(connectionUnavailableStatusTokens, in: error) {
            return true
        }
        return !posixErrorCodes(in: error).isDisjoint(with: connectionUnavailablePOSIXCodes)
    }

    private static func containsAnyStatusToken(_ statusTokens: Set<String>, in error: Error) -> Bool {
        for description in errorDescriptions(for: error) {
            let uppercased = description.uppercased()
            if statusTokens.contains(where: { uppercased.contains($0) }) {
                return true
            }
        }
        return false
    }

    private static func posixErrorCodes(in error: Error) -> Set<POSIXErrorCode> {
        Set(nsErrors(for: error).compactMap { nsError in
            guard nsError.domain == NSPOSIXErrorDomain else { return nil }
            return POSIXErrorCode(rawValue: Int32(nsError.code))
        })
    }

    private static func errorDescriptions(for error: Error) -> [String] {
        nsErrors(for: error).flatMap { nsError -> [String] in
            var values: [String] = [nsError.localizedDescription]
            if let explicit = nsError.userInfo[NSLocalizedDescriptionKey] as? String {
                values.append(explicit)
            }
            return values
        }
    }

    private static func nsErrors(for error: Error) -> [NSError] {
        var collected: [NSError] = []
        var pending: [NSError] = [error as NSError]
        var visited: Set<String> = []

        while let current = pending.popLast() {
            let key = "\(current.domain)#\(current.code)#\(current.localizedDescription)"
            guard visited.insert(key).inserted else { continue }
            collected.append(current)

            if let underlying = current.userInfo[NSUnderlyingErrorKey] as? NSError {
                pending.append(underlying)
            } else if let underlying = current.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying as NSError)
            }
        }

        return collected
    }
}
