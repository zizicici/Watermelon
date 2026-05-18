import Foundation

struct SMBServerConfig {
    let host: String
    let port: Int
    let shareName: String
    let basePath: String
    let username: String
    let password: String
    let domain: String?
}

struct RemoteStorageEntry {
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

/// Callers MUST exhaust every case — silent fall-through hides "we don't know if our bytes landed" as success.
enum AtomicCreateResult: Sendable {
    case created
    case alreadyExists
    /// Backend completed but caller-side verification is inconclusive (timeout, read-after-write delay); each caller decides policy explicitly.
    case bestEffortRetry
}

/// Single-instance concurrency contract; `.serialOnly` callers must externally serialize fan-out.
enum ClientConcurrencyMode: Sendable {
    case concurrent
    case serialOnly
}

enum CreateGuarantee: Sendable, Equatable {
    /// Kernel- or server-enforced exclusivity — `.exclusive` is only reported when the backend verifies the contract end-to-end.
    case exclusive
    /// Peer's bytes can still win the path; callers stage + post-verify.
    case overwritePossible
}

/// Independent of size — S3's multipart-completion race is per-key regardless.
enum DataPathOverwriteRisk: Sendable, Equatable {
    case perKey
    case none
}

/// Unknown backends use exact presence and folded collision keys.
enum BackendNameCaseSensitivity: Sendable, Equatable {
    case caseSensitive
    case caseInsensitive
    case unknown

    var usesExactNameMatchingForPresence: Bool {
        self != .caseInsensitive
    }

    var foldsCaseForCollisionAvoidance: Bool {
        self != .caseSensitive
    }
}

extension AtomicCreateResult {
    var defaultGuarantee: CreateGuarantee {
        switch self {
        case .created: return .exclusive
        case .alreadyExists: return .exclusive
        case .bestEffortRetry: return .overwritePossible
        }
    }
}

enum RemoteStorageClientError: LocalizedError {
    case notConnected
    case unavailable
    case invalidConfiguration
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
}

protocol RemoteStorageClientProtocol: Sendable {
    func shouldSetModificationDate() -> Bool
    func shouldLimitUploadRetries(for error: Error) -> Bool
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
    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult
    /// Per-(path, size) because S3 multipart degrades the guarantee at the threshold.
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee
    var moveIfAbsentGuarantee: CreateGuarantee { get }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool
    var dataPathOverwriteRisk: DataPathOverwriteRisk { get }
    /// Existing-path upload is safe for heartbeat renewal only when peers never observe a missing file.
    var supportsLivenessSafeOverwriteUpload: Bool { get }
    /// Existing-destination move is safe for heartbeat renewal only when peers never observe a missing file.
    var supportsLivenessSafeOverwriteMove: Bool { get }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { get }
    var concurrencyMode: ClientConcurrencyMode { get }
    /// Shared read-after-write staleness budget consumed by metadata-write
    /// verification AND peer-heartbeat classification. `>0` for S3-compatible
    /// behind eventual-consistency proxies (R2, MinIO, B2) or WebDAV behind
    /// reverse-proxy / CDN caches.
    var readAfterWriteGraceSeconds: TimeInterval { get }
    /// Composing wrappers transitively expose this so `wrapIfSerial` doesn't double-wrap.
    var isSerialized: Bool { get }
    func setModificationDate(_ date: Date, forPath path: String) async throws
    func download(remotePath: String, localURL: URL) async throws
    func exists(path: String) async throws -> Bool
    func delete(path: String) async throws
    func createDirectory(path: String) async throws
    func move(from sourcePath: String, to destinationPath: String) async throws
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult
    func copy(from sourcePath: String, to destinationPath: String) async throws
}

extension RemoteStorageClientProtocol {
    func shouldSetModificationDate() -> Bool {
        true
    }

    func shouldLimitUploadRetries(for _: Error) -> Bool {
        false
    }

    var isSerialized: Bool { false }

    /// Fail-closed: treat uncustomized backends as overwrite-risky and exact-match.
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    /// Conservative default: backends must opt in explicitly after verifying the
    /// renewal-safety contract end-to-end.
    var supportsLivenessSafeOverwriteUpload: Bool { false }
    /// Derived: a backend can renew the liveness path iff at least one of the two
    /// renewal atoms is safe. `BackupV2RuntimeBuilder` consumes this — when false
    /// the orphan sweep MUST decline to run, since a stale heartbeat would let a
    /// peer delete our live staging files.
    var supportsLivenessSafeRenewal: Bool {
        supportsLivenessSafeOverwriteUpload || supportsLivenessSafeOverwriteMove
    }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func supportsExclusiveMoveIfAbsent(forDestinationPath _: String) async throws -> Bool {
        moveIfAbsentGuarantee == .exclusive
    }

    /// Single resolved query so callers don't re-derive `.exclusive || probe` ladders.
    func resolvedSupportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        if moveIfAbsentGuarantee == .exclusive { return true }
        return try await supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }

    /// Floor raises the minimum without losing the `readAfterWriteGraceSeconds` ceiling.
    func metadataReadAfterWriteDeadline(floorSeconds: TimeInterval) -> Date {
        Date().addingTimeInterval(max(floorSeconds, readAfterWriteGraceSeconds))
    }

    func verifyWriteAccess() async throws {}

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        if try await exists(path: remotePath) {
            return .alreadyExists
        }
        try await upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
        // exists+upload is TOCTOU; signal best-effort so callers can verify when correctness matters.
        return .bestEffortRetry
    }

    func directReadURL(forRemotePath _: String) async -> URL? {
        nil
    }

    /// Production backends must override this; exists+move is only a best-effort fallback.
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        if try await metadata(path: destinationPath) != nil {
            return .alreadyExists
        }
        try Task.checkCancellation()
        try await move(from: sourcePath, to: destinationPath)
        return .bestEffortRetry
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

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool
    ) async throws -> AtomicCreateResult {
        try await atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: nil
        )
    }
}
