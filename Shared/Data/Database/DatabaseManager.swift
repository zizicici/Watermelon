import Foundation
import GRDB

final class DatabaseManager: @unchecked Sendable {
    let dbQueue: DatabaseQueue

    init(databaseURL: URL? = nil) throws {
        let url = databaseURL ?? Self.defaultDatabaseURL()
        try Self.prepareDatabaseLocation(at: url)
        // Foreground and background-task connections share one file; WAL + busy timeout avoid "database is locked" COMMIT failures on overlap.
        var config = Configuration()
        config.journalMode = .wal
        config.busyMode = .timeout(5)
        dbQueue = try DatabaseQueue(path: url.path, configuration: config)
        try migrator.migrate(dbQueue)
        Self.enableBackgroundAccessForDatabaseFiles(at: url)
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

        migrator.registerMigration("v3_writer_id") { db in
            try db.alter(table: ServerProfileRecord.databaseTableName) { table in
                table.add(column: "writerID", .text)
            }
        }

        migrator.registerMigration("v4_background_backup_node_options") { db in
            try db.alter(table: ServerProfileRecord.databaseTableName) { table in
                table.add(column: "backgroundBackupMinIntervalMinutes", .integer).defaults(to: 1440)
                table.add(column: "backgroundBackupRequiresWiFi", .boolean).defaults(to: true)
            }
        }

        // Default false so existing nodes are never silently opted into the extra thumbnail uploads.
        migrator.registerMigration("v5_generate_remote_thumbnails") { db in
            try db.alter(table: ServerProfileRecord.databaseTableName) { table in
                table.add(column: "generateRemoteThumbnails", .boolean).notNull().defaults(to: false)
            }
        }

        migrator.registerMigration("v6_default_resource_storage_codec") { db in
            try db.alter(table: ServerProfileRecord.databaseTableName) { table in
                table.add(column: "defaultResourceStorageCodec", .integer).notNull().defaults(to: 0)
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

    func setBackgroundBackupMinIntervalMinutes(_ minutes: Int, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET backgroundBackupMinIntervalMinutes = ? WHERE id = ?
                """,
                arguments: [minutes, profileID]
            )
        }
    }

    func setBackgroundBackupRequiresWiFi(_ requiresWiFi: Bool, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET backgroundBackupRequiresWiFi = ? WHERE id = ?
                """,
                arguments: [requiresWiFi, profileID]
            )
        }
    }

    func setGenerateRemoteThumbnails(_ enabled: Bool, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET generateRemoteThumbnails = ? WHERE id = ?
                """,
                arguments: [enabled, profileID]
            )
        }
    }

    func setDefaultResourceStorageCodec(_ codec: Int, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET defaultResourceStorageCodec = ? WHERE id = ?
                """,
                arguments: [codec, profileID]
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
            // Writer identity and storage codec are machine-owned; stale editor records must not rewrite them.
            if let id = profile.id,
               let liveRow = try Row.fetchOne(
                   db,
                   sql: "SELECT writerID, defaultResourceStorageCodec FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                   arguments: [id]
               ) {
                let liveWriterID: String? = liveRow["writerID"]
                if let liveWriterID, !liveWriterID.isEmpty {
                    profile.writerID = liveWriterID
                } else {
                    profile.writerID = UUID().uuidString.lowercased()
                }
                profile.defaultResourceStorageCodec = liveRow["defaultResourceStorageCodec"]
            } else {
                profile.writerID = UUID().uuidString.lowercased()
            }
            try profile.save(db)
        }
    }

    // Lazily persists a canonical writer ID for a saved profile and returns live machine-owned fields.
    // Mirrors saveServerProfile's guard: a non-empty live writer ID always wins; a live NULL/empty writer ID
    // is the upgrade case and mints+persists one. Missing rows are never minted an unpersistable identity.
    func profileWithBackfilledWriterID(_ profile: ServerProfileRecord) throws -> ServerProfileRecord {
        guard let id = profile.id else { return profile }
        var result = profile
        try write { db in
            let row = try Row.fetchOne(
                db,
                sql: "SELECT writerID, defaultResourceStorageCodec FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                arguments: [id]
            )
            // No live row: a deleted/absent profile. Never mint — minting would update zero rows and hand
            // back an identity anchored nowhere. Leave `result` as the caller passed it (nil stays nil).
            guard let row else { return }
            result.defaultResourceStorageCodec = row["defaultResourceStorageCodec"]
            let liveWriterID: String? = row["writerID"]
            if let liveWriterID, !liveWriterID.isEmpty {
                result.writerID = liveWriterID
            } else {
                let generated = UUID().uuidString.lowercased()
                try db.execute(
                    sql: "UPDATE \(ServerProfileRecord.databaseTableName) SET writerID = ? WHERE id = ?",
                    arguments: [generated, id]
                )
                result.writerID = generated
            }
        }
        return result
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

    func backgroundBackupLastRanAt(profileID: Int64) throws -> Date? {
        guard let value = try syncStateValue(for: backgroundBackupLastRanKey(profileID: profileID)),
              let timestamp = TimeInterval(value) else {
            return nil
        }
        return Date(timeIntervalSince1970: timestamp)
    }

    func setBackgroundBackupLastRanAt(_ date: Date, profileID: Int64) throws {
        try setSyncState(
            key: backgroundBackupLastRanKey(profileID: profileID),
            value: String(date.timeIntervalSince1970)
        )
    }

    // Background run markers describe the destination the profile pointed at when written; a same-id repoint must drop them.
    func clearBackgroundBackupRunMarkers(profileID: Int64) throws {
        try write { db in
            _ = try SyncStateRecord.deleteOne(db, key: backgroundBackupLastCompletedKey(profileID: profileID))
            _ = try SyncStateRecord.deleteOne(db, key: backgroundBackupLastRanKey(profileID: profileID))
        }
    }

    // Diagnostic-only: records that another writer's lock was seen during acquire for this profile.
    func setMultiDeviceObserved(_ date: Date, profileID: Int64) throws {
        try setSyncState(
            key: multiDeviceObservedKey(profileID: profileID),
            value: String(date.timeIntervalSince1970)
        )
    }

    func multiDeviceObservedAt(profileID: Int64) throws -> Date? {
        guard let value = try syncStateValue(for: multiDeviceObservedKey(profileID: profileID)),
              let timestamp = TimeInterval(value) else {
            return nil
        }
        return Date(timeIntervalSince1970: timestamp)
    }

    private func multiDeviceObservedKey(profileID: Int64) -> String {
        "multi_device_observed_at_profile_\(profileID)"
    }

    func remoteVerifiedAt(profileID: Int64) throws -> Date? {
        guard let value = try syncStateValue(for: remoteVerifiedAtKey(profileID: profileID)),
              let timestamp = TimeInterval(value) else {
            return nil
        }
        return Date(timeIntervalSince1970: timestamp)
    }

    func setRemoteVerifiedAt(_ date: Date, profileID: Int64) throws {
        try setSyncState(
            key: remoteVerifiedAtKey(profileID: profileID),
            value: String(date.timeIntervalSince1970)
        )
    }

    func clearRemoteVerifiedAt(profileID: Int64) throws {
        try write { db in
            _ = try SyncStateRecord.deleteOne(db, key: remoteVerifiedAtKey(profileID: profileID))
        }
    }

    private func remoteVerifiedAtKey(profileID: Int64) -> String {
        "remote_verified_at_\(profileID)"
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

    // Distinct from completed-at: advanced on any executed run (incl. partially-failed ones that still
    // committed months) so foreground refresh isn't gated behind full completion, while cooldown keeps using completed-at.
    private func backgroundBackupLastRanKey(profileID: Int64) -> String {
        "background_backup_last_ran_at_profile_\(profileID)"
    }

    static func defaultDatabaseURL() -> URL {
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        return support.appendingPathComponent("Watermelon/database.sqlite")
    }

    private static func prepareDatabaseLocation(at url: URL) throws {
        let directoryURL = url.deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: directoryURL,
            withIntermediateDirectories: true,
            attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        )
        try? FileProtection.enableBackgroundAccess(at: directoryURL)
    }

    private static func enableBackgroundAccessForDatabaseFiles(at url: URL) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            try? FileProtection.enableBackgroundAccess(at: URL(fileURLWithPath: url.path + suffix))
        }
    }
}
