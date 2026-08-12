import Foundation
import os.log

nonisolated private let dropboxClientLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "DropboxClient"
)

final actor DropboxClient: RemoteStorageClientProtocol,
    RemoteUploadCollisionPolicyClient,
    RemoteUploadOutcomeVerificationClient {
    struct Config: Sendable {
        let connection: CanonicalDropboxConnection
    }

    nonisolated private static let directUploadThreshold: Int64 = 64 * 1024 * 1024
    nonisolated private static let uploadChunkSize = 8 * 1024 * 1024
    nonisolated private static let requestTimeout: TimeInterval = 120
    nonisolated private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    nonisolated private static let stallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: 3 * 60,
        uploadResponseStall: 5 * 60,
        downloadFirstByte: 5 * 60,
        downloadStall: 3 * 60,
        pollInterval: 5
    )

    private let config: Config
    private let credential: DropboxCredentialBlob
    private let tokenProvider: any DropboxAccessTokenProviding
    private let sharedState: DropboxSharedState
    private let throttleKey: DropboxThrottleGate.Key
    private let session: URLSession
    private let transferSession: URLSession
    private let transferDelegate = URLSessionStallWatchdog.Delegate()
    nonisolated private let tasks = URLSessionTaskRegistry()

    init(
        config: Config,
        credential: DropboxCredentialBlob,
        tokenProvider: any DropboxAccessTokenProviding,
        sharedState: DropboxSharedState = DropboxSharedState(),
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.config = config
        self.credential = credential
        self.tokenProvider = tokenProvider
        self.sharedState = sharedState
        throttleKey = DropboxThrottleGate.Key(
            appKey: config.connection.appKey,
            accountID: credential.accountID
        )

        let metadataConfiguration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        metadataConfiguration.timeoutIntervalForRequest = Self.requestTimeout
        metadataConfiguration.timeoutIntervalForResource = Self.requestTimeout
        metadataConfiguration.urlCache = nil
        metadataConfiguration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: metadataConfiguration)

        let transferConfiguration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        transferConfiguration.timeoutIntervalForRequest = Self.transferTimeout
        transferConfiguration.timeoutIntervalForResource = Self.transferTimeout
        transferConfiguration.urlCache = nil
        transferConfiguration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        transferSession = URLSession(
            configuration: transferConfiguration,
            delegate: transferDelegate,
            delegateQueue: nil
        )
    }

    deinit {
        session.invalidateAndCancel()
        transferSession.invalidateAndCancel()
    }

    nonisolated var shouldDownloadRemoteFileForNameCollision: Bool { false }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        DropboxErrorClassifier.isNameCollision(error) || SMBErrorClassifier.isNameCollision(error)
    }

    nonisolated func shouldSetModificationDate() -> Bool { false }

    nonisolated func cancelActiveOperationsForAbandonment() {
        tasks.cancelAll()
    }

    func connect() async throws {
        try validateIdentity()
        _ = try await performRPC(
            endpoint: "files/list_folder",
            body: ["path": "", "recursive": false, "limit": 1]
        )
    }

    func disconnect() async {}

    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }

    func verifyWriteAccess() async throws {
        let config = config
        let credential = credential
        let tokenProvider = tokenProvider
        let sharedState = sharedState
        #if os(macOS)
        let cleanupClient = await DropboxClient(
            config: config,
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState
        )
        #else
        let cleanupClient = DropboxClient(
            config: config,
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState
        )
        #endif
        try await RemoteStorageWriteVerifier.verify(
            client: self,
            cleanupClientFactory: { cleanupClient },
            basePath: "/",
            failureCleanupPolicy: .waitForCompletion
        )
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let normalized = try Self.canonicalPath(path)
        var data = try await performRPC(
            endpoint: "files/list_folder",
            body: [
                "path": Self.apiPath(normalized),
                "recursive": false,
                "include_deleted": false,
                "include_non_downloadable_files": false
            ]
        )
        var entries: [RemoteStorageEntry] = []
        while true {
            let page = try Self.decode(DropboxListFolderPage.self, from: data)
            entries.append(contentsOf: page.entries.compactMap { metadata in
                guard metadata.tag == "file" || metadata.tag == "folder" else { return nil }
                let entryPath = Self.appending(metadata.name, to: normalized)
                return Self.remoteEntry(metadata, path: entryPath)
            })
            guard page.hasMore else { return entries }
            data = try await performRPC(
                endpoint: "files/list_folder/continue",
                body: ["cursor": page.cursor]
            )
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let normalized = try Self.canonicalPath(path)
        if normalized == "/" {
            return RemoteStorageEntry(
                path: "/",
                name: "Dropbox",
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            )
        }
        do {
            let data = try await performRPC(
                endpoint: "files/get_metadata",
                body: ["path": normalized, "include_deleted": false]
            )
            let item = try Self.decode(DropboxMetadata.self, from: data)
            return Self.remoteEntry(item, path: normalized)
        } catch {
            if DropboxErrorClassifier.isNotFound(error) { return nil }
            throw error
        }
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
        if respectTaskCancellation {
            try Task.checkCancellation()
            try await performUpload(localURL: localURL, remotePath: remotePath, mode: mode, onProgress: onProgress)
            return
        }
        let operation = Task {
            try await self.performUpload(localURL: localURL, remotePath: remotePath, mode: mode, onProgress: onProgress)
        }
        try await withTaskCancellationHandler {
            try await operation.value
        } onCancel: {}
    }

    func setModificationDate(_: Date, forPath _: String) async throws {}

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        let normalized = try Self.canonicalPath(remotePath)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let temporaryURL = try await performContentDownload(path: normalized, onProgress: onProgress)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try FileManager.default.createDirectory(
            at: localURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        if FileManager.default.fileExists(atPath: localURL.path) {
            try FileManager.default.removeItem(at: localURL)
        }
        try FileManager.default.moveItem(at: temporaryURL, to: localURL)
        onProgress?(1)
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func remoteFileMatches(localURL: URL, remotePath: String) async throws -> Bool {
        let normalized = try Self.canonicalPath(remotePath)
        guard normalized != "/" else { return false }
        let localValues = try localURL.resourceValues(forKeys: [.fileSizeKey, .isRegularFileKey])
        guard localValues.isRegularFile == true, let localSize = localValues.fileSize else { return false }
        let data = try await performRPC(
            endpoint: "files/get_metadata",
            body: ["path": normalized, "include_deleted": false]
        )
        let item = try Self.decode(DropboxMetadata.self, from: data)
        guard item.tag == "file",
              item.size == Int64(localSize),
              let remoteHash = item.contentHash else {
            return false
        }
        return remoteHash.caseInsensitiveCompare(try DropboxContentHasher.hexDigest(of: localURL)) == .orderedSame
    }

    func delete(path: String) async throws {
        let normalized = try Self.canonicalPath(path)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        _ = try await performRPC(endpoint: "files/delete_v2", body: ["path": normalized])
    }

    func createDirectory(path: String) async throws {
        let normalized = try Self.canonicalPath(path)
        if normalized == "/" { return }
        var current = "/"
        for component in try Self.pathComponents(normalized) {
            current = Self.appending(component, to: current)
            do {
                _ = try await performRPC(
                    endpoint: "files/create_folder_v2",
                    body: ["path": current, "autorename": false]
                )
            } catch {
                guard DropboxErrorClassifier.isNameCollision(error),
                      let existing = try await metadata(path: current),
                      existing.isDirectory else {
                    throw error
                }
            }
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let source = try Self.canonicalPath(sourcePath)
        let destination = try Self.canonicalPath(destinationPath)
        guard source != "/", destination != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        try await performRelocation(
            operation: .move,
            source: source,
            destination: destination
        )
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let source = try Self.canonicalPath(sourcePath)
        let destination = try Self.canonicalPath(destinationPath)
        guard source != "/", destination != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        try await performRelocation(
            operation: .copy,
            source: source,
            destination: destination
        )
    }

    private enum RelocationOperation: String, Sendable {
        case move
        case copy

        var endpoint: String { "files/\(rawValue)_v2" }
    }

    private func performRelocation(
        operation: RelocationOperation,
        source: String,
        destination: String
    ) async throws {
        let data = try await performRPC(
            endpoint: operation.endpoint,
            body: [
                "from_path": source,
                "to_path": destination,
                "allow_shared_folder": false,
                "autorename": false,
                "allow_ownership_transfer": false
            ]
        )
        _ = try Self.decode(DropboxRelocationResult.self, from: data)
    }

    private func performUpload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let normalized = try Self.canonicalPath(remotePath)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let values = try localURL.resourceValues(forKeys: [
            .fileSizeKey,
            .isRegularFileKey,
            .contentModificationDateKey
        ])
        guard values.isRegularFile == true, let fileSize = values.fileSize else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let clientModified = values.contentModificationDate
        do {
            if Int64(fileSize) <= Self.directUploadThreshold {
                let argument = try Self.commitArgument(
                    path: normalized,
                    mode: mode,
                    clientModified: clientModified
                )
                _ = try await performContentUpload(
                    endpoint: "files/upload",
                    argument: argument,
                    body: .file(localURL),
                    expected: [200],
                    onProgress: onProgress
                )
                onProgress?(1)
                return
            }
            try await uploadWithSession(
                localURL: localURL,
                size: Int64(fileSize),
                remotePath: normalized,
                mode: mode,
                clientModified: clientModified,
                onProgress: onProgress
            )
        } catch {
            if mode == .createIfAbsent, DropboxErrorClassifier.isNameCollision(error) {
                throw remoteStorageNameCollisionError(path: remotePath)
            }
            throw error
        }
    }

    private func uploadWithSession(
        localURL: URL,
        size: Int64,
        remotePath: String,
        mode: RemoteUploadMode,
        clientModified: Date?,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let startData = try await performContentUpload(
            endpoint: "files/upload_session/start",
            argument: ["close": false],
            body: .data(Data()),
            expected: [200],
            onProgress: nil
        )
        let sessionID = try Self.decode(DropboxUploadSessionStartResult.self, from: startData).sessionID
        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }
        var offset: Int64 = 0
        while offset < size {
            try Task.checkCancellation()
            let count = Int(min(Int64(Self.uploadChunkSize), size - offset))
            guard let chunk = try handle.read(upToCount: count), chunk.count == count else {
                throw CocoaError(.fileReadUnknown)
            }
            let cursor: [String: Any] = ["session_id": sessionID, "offset": offset]
            let isFinal = offset + Int64(count) == size
            if isFinal {
                _ = try await performContentUpload(
                    endpoint: "files/upload_session/finish",
                    argument: [
                        "cursor": cursor,
                        "commit": try Self.commitArgument(
                            path: remotePath,
                            mode: mode,
                            clientModified: clientModified
                        )
                    ],
                    body: .data(chunk),
                    expected: [200],
                    onProgress: nil
                )
            } else {
                _ = try await performContentUpload(
                    endpoint: "files/upload_session/append_v2",
                    argument: ["cursor": cursor, "close": false],
                    body: .data(chunk),
                    expected: [200],
                    onProgress: nil
                )
            }
            offset += Int64(count)
            onProgress?(min(1, Double(offset) / Double(size)))
        }
    }

    private func performRPC(endpoint: String, body: [String: Any]) async throws -> Data {
        let bodyData = try Self.jsonData(body)
        let url = try Self.endpointURL(host: "api.dropboxapi.com", endpoint: endpoint)
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let token = try await tokenProvider.accessToken(
                for: credential,
                appKey: config.connection.appKey,
                forceRefresh: attempt > 0
            )
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.timeoutInterval = Self.requestTimeout
            request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = bodyData
            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await tasks.data(for: request, in: session)
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            guard let http = response as? HTTPURLResponse else {
                throw RemoteStorageClientError.unavailable
            }
            if http.statusCode == 401, attempt == 0 { continue }
            try await validateResponse(data: data, response: http, expected: [200], endpoint: endpoint)
            return data
        }
        throw DropboxAuthenticationError.reauthenticationRequired
    }

    private func performContentUpload(
        endpoint: String,
        argument: [String: Any],
        body: URLSessionStallWatchdog.Body,
        expected: Set<Int>,
        onProgress: ((Double) -> Void)?
    ) async throws -> Data {
        let url = try Self.endpointURL(host: "content.dropboxapi.com", endpoint: endpoint)
        let argumentString = try Self.jsonHeaderValue(argument)
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let token = try await tokenProvider.accessToken(
                for: credential,
                appKey: config.connection.appKey,
                forceRefresh: attempt > 0
            )
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
            request.setValue(argumentString, forHTTPHeaderField: "Dropbox-API-Arg")
            request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
            let result: (Data, HTTPURLResponse)
            do {
                result = try await URLSessionStallWatchdog.runUpload(
                    session: transferSession,
                    delegate: transferDelegate,
                    registry: tasks,
                    request: request,
                    body: body,
                    onProgress: onProgress,
                    timeouts: Self.stallTimeouts,
                    makeStallError: { _, _, _, _ in URLError(.timedOut) }
                )
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            if result.1.statusCode == 401, attempt == 0 { continue }
            try await validateResponse(
                data: result.0,
                response: result.1,
                expected: expected,
                endpoint: endpoint
            )
            return result.0
        }
        throw DropboxAuthenticationError.reauthenticationRequired
    }

    private func performContentDownload(path: String, onProgress: ((Double) -> Void)?) async throws -> URL {
        let url = try Self.endpointURL(host: "content.dropboxapi.com", endpoint: "files/download")
        let argumentString = try Self.jsonHeaderValue(["path": path])
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let token = try await tokenProvider.accessToken(
                for: credential,
                appKey: config.connection.appKey,
                forceRefresh: attempt > 0
            )
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
            request.setValue(argumentString, forHTTPHeaderField: "Dropbox-API-Arg")
            let temporaryURL: URL
            let response: HTTPURLResponse
            do {
                (temporaryURL, response) = try await URLSessionStallWatchdog.runDownload(
                    session: transferSession,
                    registry: tasks,
                    request: request,
                    onProgress: onProgress,
                    timeouts: Self.stallTimeouts,
                    makeStallError: { _, _, _, _ in URLError(.timedOut) }
                )
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            if response.statusCode == 401, attempt == 0 {
                try? FileManager.default.removeItem(at: temporaryURL)
                continue
            }
            guard response.statusCode == 200 else {
                let data = (try? Data(contentsOf: temporaryURL)) ?? Data()
                try? FileManager.default.removeItem(at: temporaryURL)
                try await validateResponse(
                    data: data,
                    response: response,
                    expected: [200],
                    endpoint: "files/download"
                )
                throw RemoteStorageClientError.unavailable
            }
            return temporaryURL
        }
        throw DropboxAuthenticationError.reauthenticationRequired
    }

    private func validateIdentity() throws {
        guard credential.accountID == config.connection.accountID else {
            throw DropboxAuthenticationError.accountMismatch
        }
    }

    private func validateResponse(
        data: Data,
        response: HTTPURLResponse,
        expected: Set<Int>,
        endpoint: String
    ) async throws {
        guard expected.contains(response.statusCode) else {
            let error = DropboxErrorClassifier.makeServiceError(
                data: data,
                response: response,
                endpoint: endpoint
            )
            if let retryAfter = DropboxErrorClassifier.retryAfter(in: error) {
                await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
            }
            let requestID = error.userInfo[DropboxErrorClassifier.userInfoRequestIDKey] as? String ?? "-"
            let tagPath = error.userInfo[DropboxErrorClassifier.userInfoTagPathKey] as? String ?? "-"
            dropboxClientLog.error(
                "Dropbox request failed endpoint=\(endpoint, privacy: .public) status=\(response.statusCode, privacy: .public) tag=\(tagPath, privacy: .public) requestID=\(requestID, privacy: .public)"
            )
            throw error
        }
    }

    nonisolated private static func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        do {
            return try JSONDecoder().decode(type, from: data)
        } catch {
            throw DropboxAuthenticationError.invalidResponse
        }
    }

    nonisolated private static func jsonData(_ value: Any) throws -> Data {
        guard JSONSerialization.isValidJSONObject(value) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return try JSONSerialization.data(withJSONObject: value)
    }

    nonisolated private static func jsonHeaderValue(_ value: Any) throws -> String {
        let raw = String(decoding: try jsonData(value), as: UTF8.self)
        var encoded = ""
        encoded.reserveCapacity(raw.utf8.count)
        for scalar in raw.unicodeScalars {
            if (0x20 ... 0x7E).contains(scalar.value) {
                encoded.unicodeScalars.append(scalar)
            } else {
                for unit in String(scalar).utf16 {
                    encoded.append(String(format: "\\u%04x", unit))
                }
            }
        }
        return encoded
    }

    nonisolated private static func endpointURL(host: String, endpoint: String) throws -> URL {
        guard let url = URL(string: "https://\(host)/2/\(endpoint)") else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return url
    }

    nonisolated private static func commitArgument(
        path: String,
        mode: RemoteUploadMode,
        clientModified: Date?
    ) throws -> [String: Any] {
        var argument: [String: Any]
        switch mode {
        case .replace:
            argument = ["path": path, "mode": "overwrite", "autorename": false, "mute": true]
        case .createIfAbsent:
            argument = [
                "path": path,
                "mode": "add",
                "autorename": false,
                "mute": true,
                "strict_conflict": true
            ]
        }
        if let clientModified {
            argument["client_modified"] = DropboxDateCodec.string(from: clientModified)
        }
        return argument
    }

    nonisolated private static func canonicalPath(_ value: String) throws -> String {
        let components = value.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components where !Self.isValidPathComponent(component) {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return components.isEmpty ? "/" : "/" + components.joined(separator: "/")
    }

    nonisolated private static func pathComponents(_ path: String) throws -> [String] {
        let components = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components where !Self.isValidPathComponent(component) {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return components
    }

    nonisolated private static func isValidPathComponent(_ value: String) -> Bool {
        RemotePathBuilder.isSafePathComponent(value) && value.last?.isWhitespace != true
    }

    nonisolated private static func apiPath(_ normalizedPath: String) -> String {
        normalizedPath == "/" ? "" : normalizedPath
    }

    nonisolated private static func appending(_ name: String, to path: String) -> String {
        path == "/" ? "/\(name)" : "\(path)/\(name)"
    }

    nonisolated private static func remoteEntry(_ item: DropboxMetadata, path: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: path,
            name: item.name,
            isDirectory: item.tag == "folder",
            size: item.size ?? 0,
            creationDate: nil,
            modificationDate: DropboxDateCodec.date(from: item.serverModified)
                ?? DropboxDateCodec.date(from: item.clientModified)
        )
    }

    nonisolated private static func sanitizedTransportError(_ error: Error) -> Error {
        if error is CancellationError { return error }
        let ns = error as NSError
        guard ns.domain == NSURLErrorDomain else { return RemoteStorageClientError.unavailable }
        return NSError(
            domain: NSURLErrorDomain,
            code: ns.code,
            userInfo: [NSLocalizedDescriptionKey: URLError(URLError.Code(rawValue: ns.code)).localizedDescription]
        )
    }
}
