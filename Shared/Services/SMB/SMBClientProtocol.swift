import Foundation

struct SMBServerConfig: Sendable {
    let host: String
    let port: Int
    let shareName: String
    let basePath: String
    let username: String
    let password: String
    let domain: String?
}

struct RemoteStorageEntry: Sendable {
    let path: String
    let name: String
    let isDirectory: Bool
    let size: Int64
    let creationDate: Date?
    let modificationDate: Date?
}

struct RemoteStorageCapacity: Sendable {
    let availableBytes: Int64?
    let totalBytes: Int64?
}

enum RemoteUploadMode: Sendable {
    case replace
    case createIfAbsent
}

enum RemoteStorageClientError: LocalizedError {
    case notConnected
    case unavailable
    case invalidConfiguration
    case unsafeConditionalCreateUnsupported
    case externalStorageUnavailable
    case unsupportedStorageType(String)
    case underlying(Error)

    var errorDescription: String? {
        switch self {
        case .notConnected:
            return String(localized: "storage.client.notConnected")
        case .unavailable:
            return String(localized: "storage.client.unavailable")
        case .invalidConfiguration:
            return String(localized: "storage.client.invalidConfiguration")
        case .unsafeConditionalCreateUnsupported:
            return String(localized: "storage.client.unsafeConditionalCreateUnsupported")
        case .externalStorageUnavailable:
            return String(localized: "storage.client.externalUnavailable")
        case .unsupportedStorageType(let type):
            return String.localizedStringWithFormat(String(localized: "storage.client.unsupportedType"), type)
        case .underlying(let error):
            return error.localizedDescription
        }
    }

    static func isLikelyExternalStorageUnavailable(_ error: Error) -> Bool {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .externalStorageUnavailable:
                return true
            case .underlying(let underlying):
                return isLikelyExternalStorageUnavailable(underlying)
            default:
                return false
            }
        }
        return false
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .notConnected, .unavailable, .externalStorageUnavailable:
                return true
            case .underlying(let underlying):
                return isConnectionUnavailable(underlying)
            default:
                return false
            }
        }
        return false
    }
}

protocol RemoteStorageClientProtocol: Sendable {
    func shouldSetModificationDate() -> Bool
    func shouldLimitUploadRetries(for error: Error) -> Bool
    // True when this backend's MOVE is NOT independent — deleting the moved-from source loses the destination
    // (some cloud WebDAV gateways alias content). Publishers use it to skip temp→MOVE→delete. Resolved once per
    // session (WebDAV probes at runtime; others are known-independent). Default false.
    func resolveMoveIsNonIndependent(basePath: String) async -> Bool
    func cancelActiveOperationsForAbandonment()
    func reapAbandonedOperations() async
    func connect() async throws
    func disconnect() async
    func verifyWriteAccess() async throws
    func storageCapacity() async throws -> RemoteStorageCapacity?
    func list(path: String) async throws -> [RemoteStorageEntry]
    func metadata(path: String) async throws -> RemoteStorageEntry?
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws
    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws
    func setModificationDate(_ date: Date, forPath path: String) async throws
    func download(remotePath: String, localURL: URL) async throws
    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws
    func download(
        remotePath: String,
        localURL: URL,
        expectedSize: Int64?,
        onProgress: ((Double) -> Void)?
    ) async throws
    func exists(path: String) async throws -> Bool
    func delete(path: String) async throws
    func createDirectory(path: String) async throws
    func move(from sourcePath: String, to destinationPath: String) async throws
    func copy(from sourcePath: String, to destinationPath: String) async throws
}

extension RemoteStorageClientProtocol {
    func shouldSetModificationDate() -> Bool {
        true
    }

    func shouldLimitUploadRetries(for _: Error) -> Bool {
        false
    }

    func resolveMoveIsNonIndependent(basePath _: String) async -> Bool {
        false
    }

    func cancelActiveOperationsForAbandonment() {}

    func reapAbandonedOperations() async {
        await disconnectSafely()
    }

    func verifyWriteAccess() async throws {}

    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        switch mode {
        case .replace:
            try await upload(
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation,
                onProgress: onProgress
            )
        case .createIfAbsent:
            throw RemoteStorageClientError.unavailable
        }
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        try await download(remotePath: remotePath, localURL: localURL)
        onProgress?(1.0)
    }

    func download(
        remotePath: String,
        localURL: URL,
        expectedSize _: Int64?,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: onProgress)
    }

    /// Returns a local URL for a remote path if the underlying storage already keeps the file
    /// on this device's filesystem (e.g. external volumes). Returns nil otherwise — caller must
    /// `download(remotePath:localURL:)` to materialize. Default returns nil.
    func directReadURL(forRemotePath _: String) async -> URL? {
        nil
    }

    func disconnectSafely() async {
        if Task.isCancelled {
            let cleanupTask = Task.detached(priority: .utility) {
                await self.disconnect()
            }
            _ = await cleanupTask.value
            return
        }
        await disconnect()
    }
}

final class VerificationTemporaryFileLease: @unchecked Sendable {
    private let lock = NSLock()
    private let urls: [URL]
    private var reclaimed = false

    init(urls: [URL]) {
        self.urls = urls
    }

    func write(_ entries: [(Data, URL)]) throws {
        try lock.withLock {
            guard !reclaimed else { throw CancellationError() }
            for (data, url) in entries {
                try data.write(to: url)
            }
        }
    }

    func reclaim() {
        let urlsToRemove = lock.withLock { () -> [URL] in
            guard !reclaimed else { return [] }
            reclaimed = true
            return urls
        }
        remove(urlsToRemove)
    }

    func removeArtifacts() {
        remove(urls)
    }

    private func remove(_ urls: [URL]) {
        for url in urls {
            try? FileManager.default.removeItem(at: url)
        }
    }

    deinit {
        reclaim()
    }
}

final class VerificationTemporaryFileRegistry: @unchecked Sendable {
    private let lock = NSLock()
    private var leases: [ObjectIdentifier: VerificationTemporaryFileLease] = [:]
    private var abandoned = false

    func register(_ lease: VerificationTemporaryFileLease) -> Bool {
        let accepted = lock.withLock {
            guard !abandoned else { return false }
            leases[ObjectIdentifier(lease)] = lease
            return true
        }
        if !accepted {
            lease.reclaim()
        }
        return accepted
    }

    func unregister(_ lease: VerificationTemporaryFileLease) {
        _ = lock.withLock {
            leases.removeValue(forKey: ObjectIdentifier(lease))
        }
        lease.reclaim()
    }

    func abandon() {
        let activeLeases = lock.withLock { () -> [VerificationTemporaryFileLease] in
            guard !abandoned else { return [] }
            abandoned = true
            let active = Array(leases.values)
            leases.removeAll()
            return active
        }
        activeLeases.forEach { $0.reclaim() }
    }
}

enum RemoteStorageWriteVerifier {
    static let defaultTimeout: TimeInterval = 90
    static let externalVolumeTimeout: TimeInterval = 180

    static func verify(
        client: any RemoteStorageClientProtocol,
        cleanupClientFactory: @escaping @Sendable () throws -> any RemoteStorageClientProtocol,
        basePath: String,
        timeout: TimeInterval = defaultTimeout,
        cleanupRetryDelays: [TimeInterval] = RemoteProbeCleanupCoordinator.defaultRetryDelays
    ) async throws {
        let probeName = ".watermelon-probe-\(UUID().uuidString.lowercased())"
        let probeDirectoryPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: probeName
        )
        let probeFilePath = RemotePathBuilder.absolutePath(
            basePath: probeDirectoryPath,
            remoteRelativePath: "write-test"
        )
        let firstProbeData = Data("watermelon-write-probe-a".utf8)
        let secondProbeData = Data("watermelon-write-probe-b".utf8)
        let firstLocalURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(probeName)-upload-a")
        let secondLocalURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(probeName)-upload-b")
        let downloadedURL = FileManager.default.temporaryDirectory.appendingPathComponent("\(probeName)-download")
        let temporaryFiles = VerificationTemporaryFileLease(
            urls: [firstLocalURL, secondLocalURL, downloadedURL]
        )
        let cleanupCoordinator = RemoteProbeCleanupCoordinator(
            makeClient: cleanupClientFactory,
            probePaths: [probeFilePath, probeDirectoryPath],
            retryDelays: cleanupRetryDelays
        )
        let probeState = RemoteProbeAttemptState(cleanupCoordinator: cleanupCoordinator)
        try temporaryFiles.write([
            (firstProbeData, firstLocalURL),
            (secondProbeData, secondLocalURL)
        ])
        defer { temporaryFiles.reclaim() }
        let outcome = await NetworkRecovery.boundedAttempt(
            deadline: Date().addingTimeInterval(max(0, timeout)),
            onAbandon: {
                temporaryFiles.reclaim()
                client.cancelActiveOperationsForAbandonment()
                probeState.requestCleanup(.delayedConfirmation)
            },
            reap: { (_: Result<Void, Error>) in
                temporaryFiles.removeArtifacts()
                probeState.requestCleanup(.delayedConfirmation)
                await client.reapAbandonedOperations()
            },
            op: { () async -> Result<Void, Error> in
                do {
                    try await performVerification(
                        client: client,
                        basePath: basePath,
                        probeDirectoryPath: probeDirectoryPath,
                        probeFilePath: probeFilePath,
                        firstProbeData: firstProbeData,
                        firstLocalURL: firstLocalURL,
                        secondLocalURL: secondLocalURL,
                        downloadedURL: downloadedURL,
                        probeState: probeState
                    )
                    return .success(())
                } catch {
                    return .failure(error)
                }
            }
        )
        switch outcome {
        case .completed(.success):
            return
        case .completed(.failure(let error)):
            probeState.requestCleanup(.delayedConfirmation)
            throw error
        case .timedOut:
            if Task.isCancelled { throw CancellationError() }
            throw RemoteStorageClientError.unavailable
        }
    }

    private static func performVerification(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        probeDirectoryPath: String,
        probeFilePath: String,
        firstProbeData: Data,
        firstLocalURL: URL,
        secondLocalURL: URL,
        downloadedURL: URL,
        probeState: RemoteProbeAttemptState
    ) async throws {
        do {
            try await client.connect()
            try Task.checkCancellation()
            try await client.createDirectory(path: basePath)
            try Task.checkCancellation()
            probeState.markProbeMayExist()
            try await client.createDirectory(path: probeDirectoryPath)
            try Task.checkCancellation()
            try await client.upload(
                localURL: firstLocalURL,
                remotePath: probeFilePath,
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            try Task.checkCancellation()
            var collisionProven = false
            do {
                try await client.upload(
                    localURL: secondLocalURL,
                    remotePath: probeFilePath,
                    mode: .createIfAbsent,
                    respectTaskCancellation: true,
                    onProgress: nil
                )
            } catch {
                guard remoteStorageIsNameCollision(error) else { throw error }
                collisionProven = true
            }
            guard collisionProven else {
                throw RemoteStorageClientError.unsafeConditionalCreateUnsupported
            }
            try Task.checkCancellation()
            try await client.download(remotePath: probeFilePath, localURL: downloadedURL)
            try Task.checkCancellation()
            guard try Data(contentsOf: downloadedURL) == firstProbeData else {
                throw RemoteStorageClientError.unavailable
            }
            try await client.delete(path: probeFilePath)
            try Task.checkCancellation()
            try await client.delete(path: probeDirectoryPath)
            try Task.checkCancellation()
        } catch {
            if !Task.isCancelled {
                await client.disconnectSafely()
            }
            throw error
        }
        await client.disconnectSafely()
    }
}

final class RemoteProbeCleanupCoordinator: @unchecked Sendable {
    enum Mode: Int, Sendable {
        case ordinary
        case delayedConfirmation

        static func strongest(_ first: Mode?, _ second: Mode) -> Mode {
            guard let first else { return second }
            return first.rawValue >= second.rawValue ? first : second
        }
    }

    private static let passTimeout: TimeInterval = 30
    static let defaultRetryDelays: [TimeInterval] = [0, 2, 10]

    private let lock = NSLock()
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private let probePaths: [String]
    private let shouldConnect: Bool
    private let retryDelays: [TimeInterval]
    private var isRunning = false
    private var pendingMode: Mode?

    init(
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol,
        probePaths: [String],
        shouldConnect: Bool = true,
        retryDelays: [TimeInterval] = defaultRetryDelays
    ) {
        self.makeClient = makeClient
        self.probePaths = probePaths
        self.shouldConnect = shouldConnect
        self.retryDelays = retryDelays.isEmpty ? [0] : retryDelays.map { max(0, $0) }
    }

    func schedule(_ mode: Mode = .ordinary) {
        let shouldStart = lock.withLock {
            pendingMode = Mode.strongest(pendingMode, mode)
            if isRunning {
                return false
            }
            isRunning = true
            return true
        }
        guard shouldStart else { return }
        Task.detached(priority: .utility) { [self] in
            var mode = lock.withLock { () -> Mode in
                let mode = pendingMode ?? .ordinary
                pendingMode = nil
                return mode
            }
            while true {
                await runCampaign(mode: mode)
                let nextMode = lock.withLock { () -> Mode? in
                    if let pendingMode {
                        self.pendingMode = nil
                        return pendingMode
                    }
                    isRunning = false
                    return nil
                }
                guard let nextMode else { return }
                mode = nextMode
            }
        }
    }

    private func runCampaign(mode: Mode) async {
        for delay in retryDelays {
            if delay > 0 {
                try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            }
            let succeeded = await runPass()
            if succeeded, mode == .ordinary { return }
        }
    }

    private func runPass() async -> Bool {
        let clientHandle = NetworkAttemptClientHandle()
        let outcome = await NetworkRecovery.boundedAttempt(
            deadline: Date().addingTimeInterval(Self.passTimeout),
            onAbandon: { clientHandle.abandon() },
            reap: { (_: Bool) in await clientHandle.reap() },
            op: { [makeClient, probePaths, shouldConnect] in
                var client: (any RemoteStorageClientProtocol)?
                do {
                    let made = try makeClient()
                    client = made
                    guard clientHandle.install(made) else { throw CancellationError() }
                    if shouldConnect {
                        try await made.connect()
                        try Task.checkCancellation()
                    }
                    for path in probePaths {
                        do {
                            try await made.delete(path: path)
                        } catch {
                            guard RemoteFaultLite.classify(error) == .notFound else { throw error }
                        }
                        try Task.checkCancellation()
                    }
                    await made.disconnectSafely()
                    return true
                } catch {
                    if !Task.isCancelled, let client {
                        await client.disconnectSafely()
                    }
                    return false
                }
            }
        )
        switch outcome {
        case .completed(let succeeded):
            return succeeded
        case .timedOut:
            return false
        }
    }
}

final class RemoteProbeAttemptState: @unchecked Sendable {
    private let lock = NSLock()
    private let cleanupCoordinator: RemoteProbeCleanupCoordinator
    private var probeMayExist = false
    private var requestedMode: RemoteProbeCleanupCoordinator.Mode?

    init(cleanupCoordinator: RemoteProbeCleanupCoordinator) {
        self.cleanupCoordinator = cleanupCoordinator
    }

    func markProbeMayExist() {
        let mode = lock.withLock { () -> RemoteProbeCleanupCoordinator.Mode? in
            probeMayExist = true
            return requestedMode
        }
        if let mode {
            cleanupCoordinator.schedule(mode)
        }
    }

    func requestCleanup(_ mode: RemoteProbeCleanupCoordinator.Mode) {
        let modeToSchedule = lock.withLock { () -> RemoteProbeCleanupCoordinator.Mode? in
            requestedMode = RemoteProbeCleanupCoordinator.Mode.strongest(requestedMode, mode)
            return probeMayExist ? requestedMode : nil
        }
        if let modeToSchedule {
            cleanupCoordinator.schedule(modeToSchedule)
        }
    }
}

func remoteStorageNameCollisionError(path: String) -> NSError {
    NSError(
        domain: NSPOSIXErrorDomain,
        code: Int(EEXIST),
        userInfo: [NSLocalizedDescriptionKey: "File exists: \(path)"]
    )
}

func remoteStorageIsNameCollision(_ error: Error, maxDepth: Int = 32) -> Bool {
    var pending: [Error] = [error]
    var visited = Set<String>()
    while let next = pending.popLast(), visited.count < maxDepth {
        let ns = next as NSError
        let key = "\(ns.domain)#\(ns.code)#\(ns.localizedDescription)"
        guard visited.insert(key).inserted else { continue }
        if SMBErrorClassifier.isNameCollision(next) {
            return true
        }
        if ns.domain == NSCocoaErrorDomain, ns.code == NSFileWriteFileExistsError {
            return true
        }
        if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
            pending.append(inner)
        }
        if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
            pending.append(underlying)
        }
    }
    return false
}
