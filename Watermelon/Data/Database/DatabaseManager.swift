import Foundation
import GRDB

final class DatabaseManager {
    let dbQueue: DatabaseQueue

    init(databaseURL: URL? = nil) throws {
        let url = databaseURL ?? Self.defaultDatabaseURL()
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        dbQueue = try DatabaseQueue(path: url.path)
        try migrator.migrate(dbQueue)
    }

    private var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("v1_schema") { db in
            try db.create(table: ServerProfileRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("name", .text).notNull()
                table.column("host", .text).notNull()
                table.column("port", .integer).notNull()
                table.column("shareName", .text).notNull()
                table.column("basePath", .text).notNull()
                table.column("username", .text).notNull()
                table.column("domain", .text)
                table.column("credentialRef", .text).notNull()
                table.column("createdAt", .datetime).notNull()
                table.column("updatedAt", .datetime).notNull()
                table.uniqueKey(["host", "shareName", "basePath", "username"])
            }

            try db.create(table: BackupAssetRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("localIdentifier", .text).notNull().unique()
                table.column("mediaType", .text).notNull()
                table.column("creationDate", .datetime)
                table.column("modificationDate", .datetime)
                table.column("locationJSON", .text)
                table.column("pixelWidth", .integer).notNull()
                table.column("pixelHeight", .integer).notNull()
                table.column("duration", .double).notNull()
                table.column("isLivePhoto", .boolean).notNull()
                table.column("lastSeenAt", .datetime).notNull()
            }

            try db.create(table: BackupResourceRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("assetLocalIdentifier", .text).notNull().indexed()
                table.column("resourceLocalIdentifier", .text).notNull()
                table.column("resourceType", .text).notNull()
                table.column("uti", .text)
                table.column("originalFilename", .text).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("fingerprint", .text).notNull().indexed()
                table.column("sourceSignature", .text)
                table.column("remoteRelativePath", .text).notNull().indexed()
                table.column("backedUpAt", .datetime).notNull()
                table.column("checksum", .text)
                table.uniqueKey(["assetLocalIdentifier", "resourceLocalIdentifier"])
            }

            try db.create(table: BackupJobRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("serverProfileID", .integer).notNull().indexed().references(ServerProfileRecord.databaseTableName, onDelete: .cascade)
                table.column("status", .text).notNull()
                table.column("totalCount", .integer).notNull()
                table.column("completedCount", .integer).notNull()
                table.column("startedAt", .datetime).notNull()
                table.column("finishedAt", .datetime)
                table.column("lastError", .text)
            }

            try db.create(table: BackupJobItemRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("jobID", .integer).notNull().indexed().references(BackupJobRecord.databaseTableName, onDelete: .cascade)
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("resourceLocalIdentifier", .text).notNull()
                table.column("fingerprint", .text).notNull().indexed()
                table.column("status", .text).notNull()
                table.column("retryCount", .integer).notNull()
                table.column("errorMessage", .text)
                table.column("updatedAt", .datetime).notNull()
                table.uniqueKey(["jobID", "resourceLocalIdentifier"])
            }

            try db.create(table: SyncStateRecord.databaseTableName) { table in
                table.column("stateKey", .text).notNull().primaryKey()
                table.column("stateValue", .text).notNull()
                table.column("updatedAt", .datetime).notNull()
            }

            try db.create(table: RemoteManifestMeta.databaseTableName) { table in
                table.column("version", .integer).notNull().primaryKey()
                table.column("generatedAt", .datetime).notNull()
                table.column("appVersion", .text).notNull()
            }
        }

        migrator.registerMigration("v2_content_hash_dedupe") { db in
            try db.drop(table: BackupJobItemRecord.databaseTableName)
            try db.drop(table: BackupJobRecord.databaseTableName)
            try db.drop(table: BackupResourceRecord.databaseTableName)
            try db.drop(table: BackupAssetRecord.databaseTableName)

            try db.create(table: BackupAssetRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("localIdentifier", .text).notNull().unique()
                table.column("mediaType", .text).notNull()
                table.column("creationDate", .datetime)
                table.column("modificationDate", .datetime)
                table.column("locationJSON", .text)
                table.column("pixelWidth", .integer).notNull()
                table.column("pixelHeight", .integer).notNull()
                table.column("duration", .double).notNull()
                table.column("isLivePhoto", .boolean).notNull()
                table.column("lastSeenAt", .datetime).notNull()
            }

            try db.create(table: BackupResourceRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("assetLocalIdentifier", .text).notNull().indexed()
                table.column("resourceLocalIdentifier", .text).notNull()
                table.column("resourceType", .text).notNull()
                table.column("uti", .text)
                table.column("originalFilename", .text).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("fingerprint", .text).notNull().indexed()
                table.column("sourceSignature", .text)
                table.column("remoteRelativePath", .text).notNull().indexed()
                table.column("backedUpAt", .datetime).notNull()
                table.column("checksum", .text)
                table.uniqueKey(["assetLocalIdentifier", "resourceLocalIdentifier"])
            }

            try db.create(table: BackupJobRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("serverProfileID", .integer).notNull().indexed().references(ServerProfileRecord.databaseTableName, onDelete: .cascade)
                table.column("status", .text).notNull()
                table.column("totalCount", .integer).notNull()
                table.column("completedCount", .integer).notNull()
                table.column("startedAt", .datetime).notNull()
                table.column("finishedAt", .datetime)
                table.column("lastError", .text)
            }

            try db.create(table: BackupJobItemRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("jobID", .integer).notNull().indexed().references(BackupJobRecord.databaseTableName, onDelete: .cascade)
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("resourceLocalIdentifier", .text).notNull()
                table.column("fingerprint", .text).notNull().indexed()
                table.column("status", .text).notNull()
                table.column("retryCount", .integer).notNull()
                table.column("errorMessage", .text)
                table.column("updatedAt", .datetime).notNull()
                table.uniqueKey(["jobID", "resourceLocalIdentifier"])
            }
        }

        return migrator
    }

    func read<T>(_ block: (Database) throws -> T) throws -> T {
        try dbQueue.read(block)
    }

    func write<T>(_ block: (Database) throws -> T) throws -> T {
        try dbQueue.write(block)
    }

    func latestServerProfile() throws -> ServerProfileRecord? {
        try read { db in
            try ServerProfileRecord
                .order(Column("updatedAt").desc)
                .fetchOne(db)
        }
    }

    func fetchServerProfiles() throws -> [ServerProfileRecord] {
        try read { db in
            try ServerProfileRecord.order(Column("updatedAt").desc).fetchAll(db)
        }
    }

    func fetchServerProfile(id: Int64) throws -> ServerProfileRecord? {
        try read { db in
            try ServerProfileRecord.fetchOne(db, key: id)
        }
    }

    func findServerProfile(host: String, shareName: String, basePath: String, username: String) throws -> ServerProfileRecord? {
        try read { db in
            try ServerProfileRecord
                .filter(Column("host") == host)
                .filter(Column("shareName") == shareName)
                .filter(Column("basePath") == basePath)
                .filter(Column("username") == username)
                .fetchOne(db)
        }
    }

    func saveServerProfile(_ profile: inout ServerProfileRecord) throws {
        let now = Date()
        profile.updatedAt = now
        if profile.id == nil {
            profile.createdAt = now
        }
        try write { db in
            try profile.save(db)
        }
    }

    func setSyncState(key: String, value: String) throws {
        var record = SyncStateRecord(stateKey: key, stateValue: value, updatedAt: Date())
        try write { db in
            try record.save(db)
        }
    }

    func syncStateValue(for key: String) throws -> String? {
        try read { db in
            try SyncStateRecord.fetchOne(db, key: key)?.stateValue
        }
    }

    func setActiveServerProfileID(_ id: Int64?) throws {
        if let id {
            try setSyncState(key: "active_server_profile_id", value: String(id))
        } else {
            try write { db in
                _ = try SyncStateRecord.deleteOne(db, key: "active_server_profile_id")
            }
        }
    }

    func activeServerProfileID() throws -> Int64? {
        guard let value = try syncStateValue(for: "active_server_profile_id"),
              let id = Int64(value) else {
            return nil
        }
        return id
    }

    static func defaultDatabaseURL() -> URL {
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return support.appendingPathComponent("Watermelon/database.sqlite")
    }
}
