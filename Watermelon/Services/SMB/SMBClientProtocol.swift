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

enum RemoteStorageClientError: LocalizedError {
    case notConnected
    case unavailable
    case invalidConfiguration
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
        case .unsupportedStorageType(let type):
            return "Unsupported storage type: \(type)."
        case .underlying(let error):
            return error.localizedDescription
        }
    }
}

protocol RemoteStorageClientProtocol {
    func connect() async throws
    func disconnect() async
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

typealias SMBRemoteEntry = RemoteStorageEntry
typealias SMBClientError = RemoteStorageClientError
typealias SMBClientProtocol = RemoteStorageClientProtocol
