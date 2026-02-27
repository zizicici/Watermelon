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
        migrator.registerMigration("v3_dev_reset_schema") { db in
            let candidateTables = [
                ServerProfileRecord.databaseTableName,
                SyncStateRecord.databaseTableName,
                LocalAssetRecord.databaseTableName,
                LocalAssetResourceRecord.databaseTableName
            ]
            for tableName in candidateTables where try db.tableExists(tableName) {
                try db.drop(table: tableName)
            }

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

            try db.create(table: SyncStateRecord.databaseTableName) { table in
                table.column("stateKey", .text).notNull().primaryKey()
                table.column("stateValue", .text).notNull()
                table.column("updatedAt", .datetime).notNull()
            }

            try db.create(table: LocalAssetRecord.databaseTableName) { table in
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("assetFingerprint", .blob).notNull()
                table.column("resourceCount", .integer).notNull()
                table.column("updatedAt", .datetime).notNull()
                table.primaryKey(["assetLocalIdentifier"])
            }
            try db.create(index: "idx_local_assets_fingerprint", on: LocalAssetRecord.databaseTableName, columns: ["assetFingerprint"])

            try db.create(table: LocalAssetResourceRecord.databaseTableName) { table in
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("role", .integer).notNull()
                table.column("slot", .integer).notNull()
                table.column("contentHash", .blob).notNull()
                table.primaryKey(["assetLocalIdentifier", "role", "slot"])
            }
            try db.create(index: "idx_local_asset_resources_hash", on: LocalAssetResourceRecord.databaseTableName, columns: ["contentHash"])
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
