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

/// SMB folds names case-insensitively; S3/SFTP/WebDAV do not.
enum BackendNameCaseSensitivity: Sendable, Equatable {
    case caseSensitive
    case caseInsensitive
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
    var dataPathOverwriteRisk: DataPathOverwriteRisk { get }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { get }
    var concurrencyMode: ClientConcurrencyMode { get }
    /// Composing wrappers transitively expose this so `wrapIfSerial` doesn't double-wrap.
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

    /// Fail-closed: treat unknown backends as overwrite-risky and case-sensitive.
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }

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
