import Foundation
import GRDB

extension MonthManifestStore {
    enum ManifestOrigin {
        case freshlyCreated
        case downloadedFromRemote
    }

    struct PreparedManifestQueue {
        let queue: DatabaseQueue
        let requiresRemoteSync: Bool
    }

    enum ManifestFileValidation: Equatable {
        case valid
        case invalid
        case inconclusive
    }

    private static let incompatibleManifestSchemaErrorCode = -41

    /// Single entry point for opening a local manifest file. Migrations
    /// applied here surface through `requiresRemoteSync`, so the caller's
    /// flush path carries them back to remote and new schema changes don't
    /// need per-loader wiring.
    // Opens a local month-manifest sqlite. These files are disposable temps (remote is the source of
    // truth; the flush export is quick_check'd before upload), so we skip journal fsyncs for far faster
    // bulk writes. journal_mode returns a row (read via fetchOne, mirroring GRDB's own setUpWALMode);
    // GRDB leaves journal_mode alone for a default (non-WAL) DatabaseQueue, so MEMORY sticks.
    static func makeManifestQueue(path: String) throws -> DatabaseQueue {
        var config = Configuration()
        config.prepareDatabase { db in
            _ = try String.fetchOne(db, sql: "PRAGMA journal_mode = MEMORY")
            try db.execute(sql: "PRAGMA synchronous = OFF")
        }
        return try DatabaseQueue(path: path, configuration: config)
    }

    static func prepareLocalManifest(
        localURL: URL,
        origin: ManifestOrigin
    ) throws -> PreparedManifestQueue {
        let queue = try makeManifestQueue(path: localURL.path)
        do {
            let requiresRemoteSync: Bool
            switch origin {
            case .freshlyCreated:
                try migrate(queue)
                requiresRemoteSync = true
            case .downloadedFromRemote:
                requiresRemoteSync = try prepareExistingManifest(queue)
            }
            return PreparedManifestQueue(queue: queue, requiresRemoteSync: requiresRemoteSync)
        } catch {
            try? queue.close()
            throw error
        }
    }

    static func migrate(_ queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("month_manifest_v1_initial") { db in
            try ensureSchemaBaseline(db)
        }
        try migrator.migrate(queue)
    }

    // Downloaded manifests may not carry this build's GRDB migration history,
    // so apply inline legacy migrations here instead of the destructive reset
    // migrator.
    static func prepareExistingManifest(_ queue: DatabaseQueue) throws -> Bool {
        try queue.write { db in
            let migrated = try migrateLegacyNsTimestamps(db)
            try validateExistingManifestSchema(db)
            try ensureSchemaIndexes(db)
            return migrated
        }
    }

    private static func migrateLegacyNsTimestamps(_ db: Database) throws -> Bool {
        let legacyToCurrent: [(legacy: String, current: String)] = [
            ("creationDateNs", "creationDateMs"),
            ("backedUpAtNs", "backedUpAtMs")
        ]
        var migrated = false
        for table in ["resources", "assets"] {
            let columns = try tableColumns(db, tableName: table)
            for pair in legacyToCurrent {
                guard columns.contains(pair.legacy), !columns.contains(pair.current) else { continue }
                try db.execute(
                    sql: "ALTER TABLE \(table) RENAME COLUMN \(pair.legacy) TO \(pair.current)"
                )
                try db.execute(
                    sql: """
                    UPDATE \(table)
                    SET \(pair.current) = \(pair.current) / 1000000
                    WHERE \(pair.current) IS NOT NULL
                    """
                )
                migrated = true
            }
        }
        return migrated
    }

    private static func tableColumns(_ db: Database, tableName: String) throws -> Set<String> {
        let rows = try Row.fetchAll(db, sql: "PRAGMA table_info(\(tableName))")
        return Set(rows.compactMap { $0["name"] as String? })
    }

    static func ensureSchemaBaseline(_ db: Database) throws {
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL
            )
            """
        )
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """
        )
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """
        )
        try ensureSchemaIndexes(db)
    }

    static func ensureSchemaIndexes(_ db: Database) throws {
        try db.execute(sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_resources_contentHash ON resources(contentHash)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_asset ON asset_resources(assetFingerprint)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_hash ON asset_resources(resourceHash)")
    }

    private static func validateExistingManifestSchema(_ db: Database) throws {
        try validateExistingManifestTable(
            db,
            tableName: "resources",
            requiredColumns: [
                "fileName",
                "contentHash",
                "fileSize",
                "resourceType",
                "creationDateMs",
                "backedUpAtMs"
            ]
        )
        try validateExistingManifestTable(
            db,
            tableName: "assets",
            requiredColumns: [
                "assetFingerprint",
                "creationDateMs",
                "backedUpAtMs",
                "resourceCount",
                "totalFileSizeBytes"
            ]
        )
        try validateExistingManifestTable(
            db,
            tableName: "asset_resources",
            requiredColumns: [
                "assetFingerprint",
                "resourceHash",
                "role",
                "slot"
            ]
        )
    }

    private static func validateExistingManifestTable(
        _ db: Database,
        tableName: String,
        requiredColumns: Set<String>
    ) throws {
        let existingColumns = try tableColumns(db, tableName: tableName)
        guard !existingColumns.isEmpty else {
            throw incompatibleManifestSchemaError(tableName: tableName, missingColumns: requiredColumns)
        }

        let missingColumns = requiredColumns.subtracting(existingColumns)
        guard missingColumns.isEmpty else {
            throw incompatibleManifestSchemaError(tableName: tableName, missingColumns: missingColumns)
        }
    }

    private static func incompatibleManifestSchemaError(
        tableName: String,
        missingColumns: Set<String>
    ) -> NSError {
        let missing = missingColumns.sorted().joined(separator: ", ")
        return NSError(
            domain: "MonthManifestStore",
            code: incompatibleManifestSchemaErrorCode,
            userInfo: [
                NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                    String(localized: "backup.manifest.error.schemaIncompatible"),
                    tableName,
                    missing
                )
            ]
        )
    }

    // Reuses the load path (migrate-then-validate), not a read-only schema check: a manifest still on the
    // legacy creationDateNs/backedUpAtNs schema is loadable after migration, and cleanup must not class it
    // as junk. Runs on a downloaded temp copy, so the in-place migration is safe.
    static func validateMonthManifestFile(at url: URL) -> ManifestFileValidation {
        let validationURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("manifest-validate-\(UUID().uuidString).sqlite")
        var queue: DatabaseQueue?
        defer {
            closeAndRemoveLocalManifest(at: validationURL, queue: queue)
        }
        do {
            let quickCheck = try RemoteSqliteValidator.quickCheckResults(at: url)
            guard quickCheck == ["ok"] else { return .invalid }
            try FileManager.default.copyItem(at: url, to: validationURL)
            let openedQueue = try makeManifestQueue(path: validationURL.path)
            queue = openedQueue
            _ = try prepareExistingManifest(openedQueue)
            return .valid
        } catch {
            return classifyManifestValidationError(error)
        }
    }

    static func validateMonthManifestFile(
        at url: URL,
        year: Int,
        month: Int,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        layout: ManifestLayout
    ) -> ManifestFileValidation {
        let validationURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("manifest-validate-\(UUID().uuidString).sqlite")
        var queue: DatabaseQueue?
        var store: MonthManifestStore?
        defer {
            if store != nil {
                store = nil
            } else {
                closeAndRemoveLocalManifest(at: validationURL, queue: queue)
            }
        }
        do {
            let quickCheck = try RemoteSqliteValidator.quickCheckResults(at: url)
            guard quickCheck == ["ok"] else { return .invalid }
            try FileManager.default.copyItem(at: url, to: validationURL)
            let prepared = try prepareLocalManifest(
                localURL: validationURL,
                origin: .downloadedFromRemote
            )
            queue = prepared.queue
            store = MonthManifestStore(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                localManifestURL: validationURL,
                dbQueue: prepared.queue,
                remoteFilesByName: [:],
                dirty: prepared.requiresRemoteSync,
                layout: layout
            )
            try store?.reloadCache()
            return .valid
        } catch {
            return classifyManifestValidationError(error)
        }
    }

    static func classifyManifestValidationError(_ error: Error) -> ManifestFileValidation {
        if isIncompatibleManifestSchemaError(error) { return .invalid }
        if let dbError = error as? DatabaseError {
            switch dbError.resultCode {
            case .SQLITE_CORRUPT, .SQLITE_NOTADB:
                return .invalid
            default:
                return .inconclusive
            }
        }
        return .inconclusive
    }

    static func isValidMonthManifestFile(at url: URL) -> Bool {
        validateMonthManifestFile(at: url) == .valid
    }

    private static func isIncompatibleManifestSchemaError(_ error: Error) -> Bool {
        let ns = error as NSError
        return ns.domain == "MonthManifestStore" && ns.code == incompatibleManifestSchemaErrorCode
    }

    static func closeAndRemoveLocalManifest(at localURL: URL, queue: DatabaseQueue?) {
        if let queue {
            do {
                try queue.close()
            } catch {
                NSLog("MonthManifestStore: failed to close local manifest db before cleanup: \(error.localizedDescription)")
                return
            }
        }
        try? FileManager.default.removeItem(at: localURL)
    }

    static func makeLocalManifestURL(year: Int, month: Int) -> URL {
        purgeStaleTempFilesIfNeeded()
        let fileName = "\(tempFilePrefix)\(year)_\(month)_\(UUID().uuidString).\(tempFileExtension)"
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(fileName)
        try? FileManager.default.removeItem(at: localURL)
        return localURL
    }

    static func purgeStaleTempFilesIfNeeded() {
        let shouldPurge = staleTempCleanupLock.withLock { () -> Bool in
            guard !hasPurgedStaleTempFiles else { return false }
            hasPurgedStaleTempFiles = true
            return true
        }
        guard shouldPurge else { return }

        let tmpURL = FileManager.default.temporaryDirectory
        guard let fileURLs = try? FileManager.default.contentsOfDirectory(
            at: tmpURL,
            includingPropertiesForKeys: [.contentModificationDateKey, .creationDateKey, .isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return
        }

        let now = Date()
        for fileURL in fileURLs {
            guard fileURL.pathExtension == tempFileExtension else { continue }
            guard fileURL.lastPathComponent.hasPrefix(tempFilePrefix) else { continue }
            guard let values = try? fileURL.resourceValues(forKeys: [.contentModificationDateKey, .creationDateKey, .isRegularFileKey]),
                  values.isRegularFile == true else {
                continue
            }
            let referenceDate = values.contentModificationDate ?? values.creationDate ?? now
            guard now.timeIntervalSince(referenceDate) >= staleTempFileAge else {
                continue
            }
            try? FileManager.default.removeItem(at: fileURL)
        }
    }

}
