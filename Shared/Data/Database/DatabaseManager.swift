import Foundation
import GRDB

final class DatabaseManager: @unchecked Sendable {
    let dbQueue: DatabaseQueue
    private let externalBookmarkRefreshLock = NSLock()
    private var externalBookmarkRefreshes: [Int64: (previous: Data?, current: Data)] = [:]

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

    func fetchServerProfile(id: Int64) throws -> ServerProfileRecord? {
        try read { db in
            try ServerProfileRecord.fetchOne(db, key: id)
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

    func setServerProfileName(_ name: String, profileID: Int64) throws {
        try write { db in
            try db.execute(
                sql: """
                UPDATE \(ServerProfileRecord.databaseTableName)
                SET name = ?, updatedAt = ? WHERE id = ?
                """,
                arguments: [name, Date(), profileID]
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
                .filter(Column("port") == port)
                .filter(Column("shareName") == shareName)
                .filter(Column("basePath") == basePath)
                .filter(Column("username") == username)
                .filter(sql: "IFNULL(domain, '') = ?", arguments: [domain ?? ""])
            return try request.fetchAll(db).first {
                if storageType == StorageType.smb.rawValue {
                    return RemoteHostIdentity.canonicalSMB($0.host) == RemoteHostIdentity.canonicalSMB(host)
                }
                return RemoteHostIdentity.canonical($0.host) == RemoteHostIdentity.canonical(host)
            }
        }
    }

    func refreshExternalVolumeConnectionParams(
        profileID: Int64,
        expectedConnectionParams: Data?,
        refreshedConnectionParams: Data
    ) throws -> Bool {
        let refreshed = try write { db in
            guard let liveProfile = try ServerProfileRecord.fetchOne(db, key: profileID),
                  liveProfile.resolvedStorageType == .externalVolume,
                  liveProfile.connectionParams == expectedConnectionParams else { return false }
            try db.execute(
                sql: "UPDATE \(ServerProfileRecord.databaseTableName) SET connectionParams = ? WHERE id = ?",
                arguments: [refreshedConnectionParams, profileID]
            )
            return true
        }
        if refreshed {
            externalBookmarkRefreshLock.withLock {
                externalBookmarkRefreshes[profileID] = (expectedConnectionParams, refreshedConnectionParams)
            }
        }
        return refreshed
    }

    func matchesAcceptedExternalBookmarkRefresh(
        profileID: Int64,
        previousConnectionParams: Data?,
        currentConnectionParams: Data?
    ) -> Bool {
        externalBookmarkRefreshLock.withLock {
            guard let currentConnectionParams,
                  let refresh = externalBookmarkRefreshes[profileID] else { return false }
            return refresh.previous == previousConnectionParams && refresh.current == currentConnectionParams
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
            // Writer identity is machine-owned: keep the live value or mint a fresh one; the in-memory value is never trusted.
            if let id = profile.id,
               let liveWriterID = try String.fetchOne(
                   db,
                   sql: "SELECT writerID FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                   arguments: [id]
               ) {
                profile.writerID = liveWriterID
            } else {
                profile.writerID = UUID().uuidString.lowercased()
            }
            try profile.save(db)
        }
    }

    func saveConnectionProfile(_ profile: inout ServerProfileRecord, editingProfileID: Int64?) throws {
        try write { db in
            let now = Date()
            if let editingProfileID {
                guard let liveProfile = try ServerProfileRecord.fetchOne(db, key: editingProfileID) else {
                    throw NSError(
                        domain: "DatabaseManager",
                        code: 404,
                        userInfo: [NSLocalizedDescriptionKey: "The storage profile no longer exists"]
                    )
                }

                profile.id = editingProfileID
                profile.name = liveProfile.name
                profile.sortOrder = liveProfile.sortOrder
                profile.backgroundBackupEnabled = liveProfile.backgroundBackupEnabled
                profile.backgroundBackupMinIntervalMinutes = liveProfile.backgroundBackupMinIntervalMinutes
                profile.backgroundBackupRequiresWiFi = liveProfile.backgroundBackupRequiresWiFi
                profile.generateRemoteThumbnails = liveProfile.generateRemoteThumbnails
                profile.createdAt = liveProfile.createdAt
                profile.writerID = liveProfile.writerID
                profile.updatedAt = now

                let destinationChanged = !liveProfile.hasSameRemoteDestination(as: profile)
                try profile.save(db)
                if destinationChanged {
                    _ = try SyncStateRecord.deleteOne(db, key: remoteVerifiedAtKey(profileID: editingProfileID))
                    _ = try SyncStateRecord.deleteOne(db, key: backgroundBackupLastCompletedKey(profileID: editingProfileID))
                    _ = try SyncStateRecord.deleteOne(db, key: backgroundBackupLastRanKey(profileID: editingProfileID))
                    if let active = try SyncStateRecord.fetchOne(db, key: "active_server_profile_id"),
                       Int64(active.stateValue) == editingProfileID {
                        _ = try SyncStateRecord.deleteOne(db, key: "active_server_profile_id")
                    }
                }
                return
            }

            profile.updatedAt = now
            profile.createdAt = now
            let maxSortOrder = try Int.fetchOne(
                db,
                sql: "SELECT MAX(sortOrder) FROM \(ServerProfileRecord.databaseTableName)"
            ) ?? -1
            if profile.sortOrder <= maxSortOrder {
                profile.sortOrder = maxSortOrder + 1
            }
            profile.writerID = UUID().uuidString.lowercased()
            try profile.save(db)
        }
    }

    // Lazily persists a canonical writer ID for a saved profile and returns it carrying the live value.
    // Mirrors saveServerProfile's machine-owned identity guard: in one write transaction read the live
    // row; a non-empty live value always wins (the stale in-memory value is never trusted over it); a
    // present row with a NULL/empty writer ID is the genuine upgrade case and mints+persists a lowercased
    // UUID. An unsaved profile (nil id) is returned unchanged. A stale profile whose row is missing
    // (deleted/absent) is never minted an unpersistable identity: it keeps a non-empty in-memory writer ID
    // if it already has one, otherwise stays nil so callers fail closed.
    func profileWithBackfilledWriterID(_ profile: ServerProfileRecord) throws -> ServerProfileRecord {
        guard let id = profile.id else { return profile }
        var result = profile
        try write { db in
            let row = try Row.fetchOne(
                db,
                sql: "SELECT writerID FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                arguments: [id]
            )
            // No live row: a deleted/absent profile. Never mint — minting would update zero rows and hand
            // back an identity anchored nowhere. Leave `result` as the caller passed it (nil stays nil).
            guard let row else { return }
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

    func deleteServerProfile(id: Int64, remainingProfileIDs: [Int64]? = nil) throws {
        try write { db in
            _ = try ServerProfileRecord.deleteOne(db, key: id)
            if let active = try SyncStateRecord.fetchOne(db, key: "active_server_profile_id"),
               Int64(active.stateValue) == id {
                _ = try SyncStateRecord.deleteOne(db, key: "active_server_profile_id")
            }
            if let remainingProfileIDs {
                for (index, remainingID) in remainingProfileIDs.enumerated() {
                    try db.execute(
                        sql: "UPDATE \(ServerProfileRecord.databaseTableName) SET sortOrder = ? WHERE id = ?",
                        arguments: [index, remainingID]
                    )
                }
            }
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
