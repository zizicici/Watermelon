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
        migrator.registerMigration("v1_initial") { db in
            try db.create(table: ServerProfileRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("name", .text).notNull()
                table.column("storageType", .text).notNull().defaults(to: StorageType.smb.rawValue)
                table.column("connectionParams", .blob)
                table.column("sortOrder", .integer).notNull().defaults(to: 0)
                table.column("host", .text).notNull()
                table.column("port", .integer).notNull()
                table.column("shareName", .text).notNull()
                table.column("basePath", .text).notNull()
                table.column("username", .text).notNull()
                table.column("domain", .text)
                table.column("credentialRef", .text).notNull()
                table.column("backgroundBackupEnabled", .integer).notNull().defaults(to: 1)
                table.column("createdAt", .datetime).notNull()
                table.column("updatedAt", .datetime).notNull()
            }
            try db.execute(
                sql: """
                CREATE UNIQUE INDEX idx_server_profiles_unique_smb
                ON \(ServerProfileRecord.databaseTableName)(host, port, shareName, basePath, username, IFNULL(domain, ''))
                WHERE storageType = 'smb'
                """
            )

            try db.create(table: SyncStateRecord.databaseTableName) { table in
                table.column("stateKey", .text).notNull().primaryKey()
                table.column("stateValue", .text).notNull()
                table.column("updatedAt", .datetime).notNull()
            }

            try db.create(table: LocalAssetRecord.databaseTableName) { table in
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("assetFingerprint", .blob)
                table.column("resourceCount", .integer).notNull().defaults(to: 0)
                table.column("totalFileSizeBytes", .integer).notNull().defaults(to: 0)
                table.column("modificationDateNs", .integer)
                table.column("updatedAt", .datetime).notNull()
                table.primaryKey(["assetLocalIdentifier"])
            }
            try db.execute(
                sql: """
                CREATE INDEX idx_local_assets_has_fingerprint
                ON \(LocalAssetRecord.databaseTableName)(assetLocalIdentifier)
                WHERE assetFingerprint IS NOT NULL
                """
            )

            try db.create(table: LocalAssetResourceRecord.databaseTableName) { table in
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("role", .integer).notNull()
                table.column("slot", .integer).notNull()
                table.column("contentHash", .blob).notNull()
                table.column("fileSize", .integer).notNull().defaults(to: 0)
                table.primaryKey(["assetLocalIdentifier", "role", "slot"])
            }
            try db.create(index: "idx_local_asset_resources_hash", on: LocalAssetResourceRecord.databaseTableName, columns: ["contentHash"])
        }

        migrator.registerMigration("v2_ms_timestamps") { db in
            try db.execute(sql: "ALTER TABLE local_assets RENAME COLUMN modificationDateNs TO modificationDateMs")
            try db.execute(
                sql: """
                UPDATE local_assets
                SET modificationDateMs = modificationDateMs / 1000000
                WHERE modificationDateMs IS NOT NULL
                """
            )
        }

        return migrator
    }

    func read<T>(_ block: (Database) throws -> T) throws -> T {
        try dbQueue.read(block)
    }

    func write<T>(_ block: (Database) throws -> T) throws -> T {
        try dbQueue.write(block)
    }

    func fetchServerProfiles() throws -> [ServerProfileRecord] {
        try read { db in
            try ServerProfileRecord
                .order(Column("sortOrder").asc, Column("updatedAt").desc)
                .fetchAll(db)
        }
    }

    func setBackgroundBackupEnabled(_ enabled: Bool, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET backgroundBackupEnabled = ? WHERE id = ?
                """,
                arguments: [enabled, profileID]
            )
        }
    }

    func fetchBackgroundBackupEnabledProfiles() throws -> [ServerProfileRecord] {
        try read { db in
            try ServerProfileRecord
                .filter(Column("backgroundBackupEnabled") == true)
                .filter(Column("storageType") != StorageType.externalVolume.rawValue)
                .fetchAll(db)
        }
    }

    func findServerProfile(
        host: String,
        port: Int,
        shareName: String,
        basePath: String,
        username: String,
        domain: String?,
        storageType: String = StorageType.smb.rawValue
    ) throws -> ServerProfileRecord? {
        try read { db in
            let request = ServerProfileRecord
                .filter(Column("storageType") == storageType)
                .filter(Column("host") == host)
                .filter(Column("port") == port)
                .filter(Column("shareName") == shareName)
                .filter(Column("basePath") == basePath)
                .filter(Column("username") == username)
                .filter(sql: "IFNULL(domain, '') = ?", arguments: [domain ?? ""])
            return try request.fetchOne(db)
        }
    }

    func saveServerProfile(_ profile: inout ServerProfileRecord) throws {
        try write { db in
            let now = Date()
            profile.updatedAt = now
            if profile.id == nil {
                profile.createdAt = now
                let maxSortOrder = try Int.fetchOne(
                    db,
                    sql: "SELECT MAX(sortOrder) FROM \(ServerProfileRecord.databaseTableName)"
                ) ?? -1
                if profile.sortOrder <= maxSortOrder {
                    profile.sortOrder = maxSortOrder + 1
                }
            }
            try profile.save(db)
        }
    }

    func deleteServerProfile(id: Int64) throws {
        try write { db in
            _ = try ServerProfileRecord.deleteOne(db, key: id)
        }
    }

    func saveServerProfileSortOrder(profileIDs: [Int64]) throws {
        try write { db in
            for (index, id) in profileIDs.enumerated() {
                try db.execute(
                    sql: """
                    UPDATE \(ServerProfileRecord.databaseTableName)
                    SET sortOrder = ?
                    WHERE id = ?
                    """,
                    arguments: [index, id]
                )
            }
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

    func backgroundBackupLastCompletedAt(profileID: Int64) throws -> Date? {
        guard let value = try syncStateValue(for: backgroundBackupLastCompletedKey(profileID: profileID)),
              let timestamp = TimeInterval(value) else {
            return nil
        }
        return Date(timeIntervalSince1970: timestamp)
    }

    func setBackgroundBackupLastCompletedAt(_ date: Date, profileID: Int64) throws {
        try setSyncState(
            key: backgroundBackupLastCompletedKey(profileID: profileID),
            value: String(date.timeIntervalSince1970)
        )
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

    private func backgroundBackupLastCompletedKey(profileID: Int64) -> String {
        "background_backup_last_completed_at_profile_\(profileID)"
    }

    static func defaultDatabaseURL() -> URL {
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return support.appendingPathComponent("Watermelon/database.sqlite")
    }
}
