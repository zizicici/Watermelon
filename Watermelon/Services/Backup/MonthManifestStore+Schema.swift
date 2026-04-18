import Foundation
import GRDB

extension MonthManifestStore {
    static func migrate(_ queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("month_manifest_v1_initial") { db in
            try ensureSchemaBaseline(db)
        }
        try migrator.migrate(queue)
    }

    // Downloaded manifests may not carry this build's GRDB migration history.
    // Validate them in place instead of applying the destructive reset migration.
    static func prepareExistingManifest(_ queue: DatabaseQueue) throws {
        try queue.write { db in
            try validateExistingManifestSchema(db)
            try ensureSchemaIndexes(db)
        }
    }

    static func ensureSchemaBaseline(_ db: Database) throws {
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL
            )
            """
        )
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL,
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
                "creationDateNs",
                "backedUpAtNs"
            ]
        )
        try validateExistingManifestTable(
            db,
            tableName: "assets",
            requiredColumns: [
                "assetFingerprint",
                "creationDateNs",
                "backedUpAtNs",
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
        let rows = try Row.fetchAll(db, sql: "PRAGMA table_info(\(tableName))")
        guard !rows.isEmpty else {
            throw incompatibleManifestSchemaError(tableName: tableName, missingColumns: requiredColumns)
        }

        let existingColumns = Set(rows.compactMap { row in
            row["name"] as String?
        })
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
            code: -41,
            userInfo: [
                NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                    String(localized: "backup.manifest.error.schemaIncompatible"),
                    tableName,
                    missing
                )
            ]
        )
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

    static func dateFromEpochNs(_ ns: Int64?) -> Date? {
        guard let ns else { return nil }
        return Date(nanosecondsSinceEpoch: ns)
    }
}
