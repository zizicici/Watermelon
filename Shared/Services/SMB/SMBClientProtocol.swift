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

extension RemoteStorageClientProtocol {
    func shouldSetModificationDate() -> Bool {
        true
    }

    func shouldLimitUploadRetries(for _: Error) -> Bool {
        false
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
