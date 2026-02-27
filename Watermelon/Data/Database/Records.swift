import Foundation
import GRDB

struct ServerProfileRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "server_profiles"

    var id: Int64?
    var name: String
    var host: String
    var port: Int
    var shareName: String
    var basePath: String
    var username: String
    var domain: String?
    var credentialRef: String
    var createdAt: Date
    var updatedAt: Date

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
    var updatedAt: Date
}

struct LocalAssetResourceRecord: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "local_asset_resources"

    var assetLocalIdentifier: String
    var role: Int
    var slot: Int
    var contentHash: Data
}
