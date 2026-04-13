import Foundation
import GRDB

enum StorageType: String, Codable {
    case smb
    case webdav
    case externalVolume
}

struct ServerProfileRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable, Sendable {
    static let databaseTableName = "server_profiles"

    var id: Int64?
    var name: String
    var storageType: String
    var connectionParams: Data?
    var sortOrder: Int
    var host: String
    var port: Int
    var shareName: String
    var basePath: String
    var username: String
    var domain: String?
    var credentialRef: String
    var createdAt: Date
    var updatedAt: Date

    var resolvedStorageType: StorageType {
        StorageType(rawValue: storageType) ?? .smb
    }

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }
}

struct SyncStateRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "sync_state"

    var stateKey: String
    var stateValue: String
    var updatedAt: Date
}

struct LocalAssetRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "local_assets"

    var assetLocalIdentifier: String
    var assetFingerprint: Data
    var resourceCount: Int
    var totalFileSizeBytes: Int64
    var updatedAt: Date
}

struct LocalAssetResourceRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "local_asset_resources"

    var assetLocalIdentifier: String
    var role: Int
    var slot: Int
    var contentHash: Data
    var fileSize: Int64
}
