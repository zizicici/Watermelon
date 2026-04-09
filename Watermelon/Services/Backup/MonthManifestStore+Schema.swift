import Foundation
import GRDB

extension MonthManifestStore {
    static func migrate(_ queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("month_manifest_v3_dev_schema_reset") { db in
            try db.execute(sql: "DROP INDEX IF EXISTS idx_asset_resources_hash")
            try db.execute(sql: "DROP INDEX IF EXISTS idx_asset_resources_asset")
            try db.execute(sql: "DROP INDEX IF EXISTS idx_resources_contentHash")
            try db.execute(sql: "DROP TABLE IF EXISTS asset_resources")
            try db.execute(sql: "DROP TABLE IF EXISTS assets")
            try db.execute(sql: "DROP TABLE IF EXISTS resources")
            try ensureSchemaBaseline(db)
        }
        try migrator.migrate(queue)
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
        try db.execute(sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_resources_contentHash ON resources(contentHash)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_asset ON asset_resources(assetFingerprint)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_hash ON asset_resources(resourceHash)")
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
