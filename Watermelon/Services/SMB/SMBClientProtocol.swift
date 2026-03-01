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
            return "Storage session is not connected."
        case .unavailable:
            return "Storage support is unavailable on this build."
        case .invalidConfiguration:
            return "Invalid storage configuration."
        case .externalStorageUnavailable:
            return "External storage is unavailable. Reconnect the drive and try again."
        case .unsupportedStorageType(let type):
            return "Unsupported storage type: \(type)."
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
                break
            }
        }

        let nsError = error as NSError
        if nsError.domain == NSCocoaErrorDomain {
            let unavailableCodes: Set<Int> = [
                NSFileNoSuchFileError,
                NSFileReadNoSuchFileError,
                NSFileReadNoPermissionError,
                NSFileWriteNoPermissionError,
                NSFileReadUnknownError,
                NSFileWriteUnknownError
            ]
            if unavailableCodes.contains(nsError.code) {
                return true
            }
        }
        return false
    }
}

protocol RemoteStorageClientProtocol: Sendable {
    func connect() async throws
    func disconnect() async
    func storageCapacity() async throws -> RemoteStorageCapacity?
    func list(path: String) async throws -> [RemoteStorageEntry]
    func metadata(path: String) async throws -> RemoteStorageEntry?
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws
    func setModificationDate(_ date: Date, forPath path: String) async throws
    func download(remotePath: String, localURL: URL) async throws
    func exists(path: String) async throws -> Bool
    func delete(path: String) async throws
    func createDirectory(path: String) async throws
    func move(from sourcePath: String, to destinationPath: String) async throws
}
