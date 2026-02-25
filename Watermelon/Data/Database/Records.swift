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

struct BackupAssetRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "assets"

    var id: Int64?
    var localIdentifier: String
    var mediaType: String
    var creationDate: Date?
    var modificationDate: Date?
    var locationJSON: String?
    var pixelWidth: Int
    var pixelHeight: Int
    var duration: TimeInterval
    var isLivePhoto: Bool
    var lastSeenAt: Date

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }
}

struct BackupResourceRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "resources"

    var id: Int64?
    var assetLocalIdentifier: String
    var resourceLocalIdentifier: String
    var resourceType: String
    var uti: String?
    var originalFilename: String
    var fileSize: Int64
    var fingerprint: String
    var sourceSignature: String?
    var remoteRelativePath: String
    var backedUpAt: Date
    var checksum: String?

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }
}

enum BackupJobStatus: String, Codable {
    case pending
    case running
    case paused
    case failed
    case done
}

struct BackupJobRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "backup_jobs"

    var id: Int64?
    var serverProfileID: Int64
    var status: BackupJobStatus
    var totalCount: Int
    var completedCount: Int
    var startedAt: Date
    var finishedAt: Date?
    var lastError: String?

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }
}

enum BackupJobItemStatus: String, Codable {
    case pending
    case running
    case skipped
    case success
    case failed
}

struct BackupJobItemRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "job_items"

    var id: Int64?
    var jobID: Int64
    var assetLocalIdentifier: String
    var resourceLocalIdentifier: String
    var fingerprint: String
    var status: BackupJobItemStatus
    var retryCount: Int
    var errorMessage: String?
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

struct RemoteManifestMeta: Codable, FetchableRecord, MutablePersistableRecord {
    static let databaseTableName = "manifest_meta"

    var version: Int
    var generatedAt: Date
    var appVersion: String
}
