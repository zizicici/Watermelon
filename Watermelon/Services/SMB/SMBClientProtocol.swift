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

struct SMBRemoteEntry {
    let path: String
    let name: String
    let isDirectory: Bool
    let size: Int64
    let creationDate: Date?
    let modificationDate: Date?
}

enum SMBClientError: LocalizedError {
    case notConnected
    case unavailable
    case invalidConfiguration
    case underlying(Error)

    var errorDescription: String? {
        switch self {
        case .notConnected:
            return "SMB session is not connected."
        case .unavailable:
            return "SMB support is unavailable on this build."
        case .invalidConfiguration:
            return "Invalid SMB configuration."
        case .underlying(let error):
            return error.localizedDescription
        }
    }
}

protocol SMBClientProtocol {
    func connect() async throws
    func disconnect() async
    func list(path: String) async throws -> [SMBRemoteEntry]
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool) async throws
    func download(remotePath: String, localURL: URL) async throws
    func exists(path: String) async throws -> Bool
    func delete(path: String) async throws
    func createDirectory(path: String) async throws
    func move(from sourcePath: String, to destinationPath: String) async throws
}
