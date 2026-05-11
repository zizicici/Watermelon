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

/// Persistence primitive contract: callers MUST decide each case explicitly via
/// exhaustive switch. Default-success on the catch-all (`if case .alreadyExists`
/// patterns + implicit fall-through) hides "we don't know if our bytes landed"
/// as "OK, continue" — and this anti-pattern reappeared across SnapshotWriter,
/// CommitLogWriter, and asset upload in successive reviews. Adding a new case
/// must intentionally break every caller's switch.
enum AtomicCreateResult: Sendable {
    /// Our bytes are durably at the path. Verified by post-write check on
    /// `.overwritePossible` backends; trusted on `.exclusive`.
    case created
    /// The path holds bytes that are NOT ours — peer write, residual stale file,
    /// or backend pre-existing object. Caller decides whether to error (metadata
    /// path: peer collision) or rename (asset path).
    case alreadyExists
    /// Backend completed the write but caller-side verification is inconclusive
    /// (network timeout on verify, S3 read-after-write delay). DO NOT treat as
    /// success without an explicit policy decision: snapshot path defers to
    /// commit log replay; asset path is uniquely renamed so retries can't
    /// double-write. Add new policy at each caller, not by silently passing through.
    case bestEffortRetry
}

/// What guarantee did the backend ACTUALLY provide for this specific
/// `atomicCreate` call? Per-operation, not per-backend (S3 single-part supports
/// If-None-Match; multipart historically doesn't). `MetadataCreateGate` adapts
/// `.overwritePossible` via UUID staging + post-move verify; the "we overwrote
/// peer at final" case is undetectable but writer-unique destination paths make
/// it inert. Asset uploads handle this via `forceWriterIDSuffix`.
/// Concurrency contract for a single client instance. Callers that fan out
/// (e.g. `refreshPhysicalPresenceOverlay`) MUST serialize when the client is
/// `.serialOnly`; otherwise behaviour is library-dependent.
enum ClientConcurrencyMode: Sendable {
    /// Multiple in-flight ops on the same instance are safe (actor-bound clients).
    case concurrent
    /// At most one in-flight op per instance — caller must serialize.
    case serialOnly
}

enum CreateGuarantee: Sendable, Equatable {
    /// Backend MUST refuse if the path is already taken. Contract: callers report
    /// `.exclusive` only when the backend's atomic-create semantic is verified
    /// (POSIX O_EXCL, S3 If-None-Match honored end-to-end). Backends that "support
    /// but might not enforce" report `.overwritePossible` so the gate stages safely.
    case exclusive
    /// The backend executed exists+upload internally (or honors conditional creates
    /// only optimistically). Peer's bytes can win the path. Callers use staging +
    /// post-verify to detect peer collision.
    case overwritePossible
}

extension AtomicCreateResult {
    /// Default guarantee inference for the common 3-result enum, used by clients
    /// that haven't yet wired explicit guarantee reporting through. The actual
    /// guarantee depends on backend internals — clients with backend-specific
    /// knowledge should override.
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
    /// What guarantee will `atomicCreate` provide for a file of this size at this
    /// path? The pair (path, size) matters because S3 multipart kicks in at a size
    /// threshold, and only multipart drops the guarantee from `.exclusive` to
    /// `.overwritePossible`. Default is conservative `.overwritePossible`.
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee
    var concurrencyMode: ClientConcurrencyMode { get }
    /// True when ops on this client are already serialized end-to-end. Composing
    /// wrappers can transitively expose this so `wrapIfSerial` doesn't double-wrap.
    var isSerialized: Bool { get }
    func setModificationDate(_ date: Date, forPath path: String) async throws
    func download(remotePath: String, localURL: URL) async throws
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

    var isSerialized: Bool { false }

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

    /// Convenience overload — keeps existing metadata-write callsites (commits,
    /// snapshots, repo.json, claims, liveness) unchanged. The new on-progress
    /// argument is only needed by the data-upload path (AssetProcessor+Upload).
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
