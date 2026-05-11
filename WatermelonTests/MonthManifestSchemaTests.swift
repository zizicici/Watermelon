import XCTest
import GRDB
@testable import Watermelon

/// Per-month manifest schema migrations run inline in `prepareExistingManifest`
/// (separate from GRDB's standard migrator since downloaded sqlites don't carry
/// this build's migration history). Tests pin: legacy *Ns rename+divide, both-
/// columns-present idempotency (else "duplicate column" on re-rename), schema
/// validation rejecting missing-column/missing-table manifests, and dbQueue
/// release on error so the temp file can be deleted.
final class MonthManifestSchemaTests: XCTestCase {
    private var tempDir: URL!

    override func setUpWithError() throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: tempDir)
    }

    // MARK: - Fresh manifest

    func testFreshlyCreated_setsUpFullSchemaAndIndexes() throws {
        let url = tempDir.appendingPathComponent("fresh.sqlite")
        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .freshlyCreated)
        defer { try? prepared.queue.close() }

        XCTAssertTrue(prepared.requiresRemoteSync,
                      "freshlyCreated manifests must signal remoteSync so the caller pushes baseline state")

        let columnsByTable = try prepared.queue.read { db -> [String: Set<String>] in
            var result: [String: Set<String>] = [:]
            for table in ["resources", "assets", "asset_resources"] {
                let cols = try Row.fetchAll(db, sql: "PRAGMA table_info(\(table))")
                    .compactMap { $0["name"] as String? }
                result[table] = Set(cols)
            }
            return result
        }
        XCTAssertEqual(columnsByTable["resources"], [
            "fileName", "contentHash", "fileSize", "resourceType", "creationDateMs", "backedUpAtMs"
        ])
        XCTAssertEqual(columnsByTable["assets"], [
            "assetFingerprint", "creationDateMs", "backedUpAtMs", "resourceCount", "totalFileSizeBytes"
        ])
        XCTAssertEqual(columnsByTable["asset_resources"], [
            "assetFingerprint", "resourceHash", "role", "slot"
        ])

        // Indexes are critical — flushToRemote relies on UNIQUE(contentHash) for dedup.
        let indexNames = try prepared.queue.read { db in
            try Set(String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL"))
        }
        XCTAssertTrue(indexNames.contains("idx_resources_contentHash"))
        XCTAssertTrue(indexNames.contains("idx_asset_resources_asset"))
        XCTAssertTrue(indexNames.contains("idx_asset_resources_hash"))
    }

    // MARK: - Legacy ns timestamp migration

    /// Real ns timestamps in legacy `creationDateNs` / `backedUpAtNs` columns must be
    /// renamed and divided by 1e6. Catches a future "rename only" or "divide only"
    /// regression that would either leave the column unrenamed (caller fails to read)
    /// or leave nanosecond values in a ms-named column (timestamps off by 1000×).
    func testLegacyNsManifest_isRenamedAndDivided() throws {
        let url = tempDir.appendingPathComponent("legacy-ns.sqlite")
        let creationNs: Int64 = 1_700_000_000_123_000_000
        let backedUpNs: Int64 = 1_700_000_001_456_000_000
        try buildLegacyNsManifest(at: url) { db in
            try db.execute(
                sql: """
                INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateNs, backedUpAtNs)
                VALUES ('photo.jpg', X'AA', 100, 1, ?, ?)
                """,
                arguments: [creationNs, backedUpNs]
            )
            try db.execute(
                sql: """
                INSERT INTO assets (assetFingerprint, creationDateNs, backedUpAtNs, resourceCount, totalFileSizeBytes)
                VALUES (X'BB', ?, ?, 1, 100)
                """,
                arguments: [creationNs, backedUpNs]
            )
        }

        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        defer { try? prepared.queue.close() }

        XCTAssertTrue(prepared.requiresRemoteSync,
                      "ns→ms migration must mark requiresRemoteSync so the caller pushes the upgraded manifest")

        let resCols = try prepared.queue.read { db in
            try Set(Row.fetchAll(db, sql: "PRAGMA table_info(resources)").compactMap { $0["name"] as String? })
        }
        XCTAssertTrue(resCols.contains("creationDateMs"), "rename must complete")
        XCTAssertTrue(resCols.contains("backedUpAtMs"))
        XCTAssertFalse(resCols.contains("creationDateNs"), "old name must be gone")
        XCTAssertFalse(resCols.contains("backedUpAtNs"))

        let row = try prepared.queue.read { db in
            try Row.fetchOne(db, sql: "SELECT creationDateMs, backedUpAtMs FROM resources WHERE fileName = 'photo.jpg'")
        }
        XCTAssertEqual(row?["creationDateMs"], creationNs / 1_000_000)
        XCTAssertEqual(row?["backedUpAtMs"], backedUpNs / 1_000_000)
    }

    /// NULL ns timestamps must remain NULL after migration. Without the
    /// `WHERE ... IS NOT NULL` guard the UPDATE would write `NULL / 1000000 = NULL`
    /// — but only because of SQLite's NULL propagation. If a future change
    /// initializes the column with a default or writes 0 for NULL, this test fires.
    func testLegacyNsManifest_nullValuesPreserved() throws {
        let url = tempDir.appendingPathComponent("legacy-null.sqlite")
        try buildLegacyNsManifest(at: url) { db in
            try db.execute(
                sql: """
                INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateNs, backedUpAtNs)
                VALUES ('photo.jpg', X'AA', 100, 1, NULL, ?)
                """,
                arguments: [Int64(1_700_000_000_000_000_000)]
            )
        }
        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        defer { try? prepared.queue.close() }

        let row = try prepared.queue.read { db in
            try Row.fetchOne(db, sql: "SELECT creationDateMs, backedUpAtMs FROM resources WHERE fileName = 'photo.jpg'")
        }
        XCTAssertNil(row?["creationDateMs"] as Int64?,
                     "NULL creationDateNs must remain NULL — never coerced to 0 or epoch")
        XCTAssertEqual(row?["backedUpAtMs"], 1_700_000_000_000)
    }

    /// `migrateLegacyNsTimestamps` guards on `legacy && !current`: if a manifest
    /// already has both column names (an unusual half-migrated state from a manual
    /// recovery, etc.), the rename would fail with "duplicate column name". The
    /// guard makes that case a no-op. Catches future code that drops the `!current`
    /// half of the guard.
    func testLegacyManifest_bothColumnNamesPresent_isNoOp() throws {
        let url = tempDir.appendingPathComponent("half-migrated.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        try queue.write { db in
            try db.execute(sql: """
            CREATE TABLE resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateNs INTEGER,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """)
            try db.execute(
                sql: """
                INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateNs, creationDateMs, backedUpAtMs)
                VALUES ('p', X'AA', 1, 1, 1000000, 1, 2)
                """
            )
        }
        try queue.close()

        // prepareLocalManifest must NOT throw "duplicate column name".
        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        defer { try? prepared.queue.close() }

        let row = try prepared.queue.read { db in
            try Row.fetchOne(db, sql: "SELECT creationDateMs, creationDateNs FROM resources")
        }
        // The current creationDateMs must be untouched (still 1, not overwritten to 1).
        XCTAssertEqual(row?["creationDateMs"], 1, "existing ms column must not be overwritten")
        XCTAssertEqual(row?["creationDateNs"], 1_000_000, "legacy column may stay; the guard only blocks rename")
    }

    // MARK: - Schema validation refuses incompatible manifests

    /// A manifest written by a future client that drops a required column must be
    /// refused with a clear error — not silently read with NULLs in missing columns.
    func testValidate_rejectsManifestMissingResourceColumn() throws {
        let url = tempDir.appendingPathComponent("missing-col.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        try queue.write { db in
            // Drop `contentHash` from the production schema.
            try db.execute(sql: """
            CREATE TABLE resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateMs INTEGER,
              backedUpAtMs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """)
        }
        try queue.close()

        do {
            _ = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
            XCTFail("expected schema-incompatible error")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "MonthManifestStore")
            XCTAssertEqual(error.code, -41)
            XCTAssertTrue(
                (error.localizedDescription).contains("contentHash"),
                "error must name the missing column so the user / log can diagnose"
            )
        }
    }

    /// A manifest without any of the required tables (e.g. opened a non-manifest
    /// sqlite by accident) must throw, not be silently treated as an empty
    /// manifest (which would tombstone every asset on subsequent reconcile).
    func testValidate_rejectsManifestMissingTable() throws {
        let url = tempDir.appendingPathComponent("missing-table.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        try queue.write { db in
            try db.execute(sql: "CREATE TABLE unrelated (a INTEGER)")
        }
        try queue.close()

        do {
            _ = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
            XCTFail("expected schema-incompatible error for missing tables")
        } catch let error as NSError {
            XCTAssertEqual(error.code, -41)
        }
    }

    /// On schema-validation failure, `prepareLocalManifest` must close the dbQueue
    /// (`try? queue.close()` in the catch) so the underlying file handle is
    /// released. Otherwise the temp file can't be deleted on the caller's cleanup
    /// path and we leak file handles every failed manifest open.
    func testValidate_failure_releasesDBQueueSoFileCanBeDeleted() throws {
        let url = tempDir.appendingPathComponent("missing-col.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        try queue.write { db in
            // Same incomplete schema as testValidate_rejectsManifestMissingResourceColumn.
            try db.execute(sql: "CREATE TABLE resources (fileName TEXT PRIMARY KEY)")
        }
        try queue.close()

        XCTAssertThrowsError(
            try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        )
        // If the queue inside prepareLocalManifest leaked, file deletion would fail
        // (or, on some platforms, succeed but leave WAL/SHM files behind).
        XCTAssertNoThrow(try FileManager.default.removeItem(at: url),
                         "file must be deletable after a validation failure (queue must have been closed)")
    }

    // MARK: - Idempotency

    /// `ensureSchemaIndexes` is called from both fresh and downloaded paths and uses
    /// `CREATE INDEX IF NOT EXISTS` — running it twice must not error and must
    /// leave the indexes in place.
    func testEnsureSchemaIndexes_idempotent() throws {
        let url = tempDir.appendingPathComponent("idempotent.sqlite")
        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .freshlyCreated)
        defer { try? prepared.queue.close() }

        // Running prepareExistingManifest now exercises the same ensureSchemaIndexes
        // path on an already-indexed db — must be a no-op.
        _ = try MonthManifestStore.prepareExistingManifest(prepared.queue)

        let indexCount = try prepared.queue.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%'") ?? 0
        }
        XCTAssertGreaterThanOrEqual(indexCount, 3,
                                    "all production indexes still present after a redundant ensureIndexes call")
    }

    /// A manifest that's already at HEAD schema (no ns columns) must be accepted
    /// AND `requiresRemoteSync` must be false — there's no upgrade work to flush
    /// back. Catches a future change that always returns true regardless of work.
    func testHeadSchemaManifest_doesNotRequireRemoteSync() throws {
        let url = tempDir.appendingPathComponent("head.sqlite")
        // First create a fresh manifest at HEAD.
        let initial = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .freshlyCreated)
        try initial.queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateMs, backedUpAtMs)
                VALUES ('p.jpg', X'AA', 100, 1, 1000, 2000)
                """
            )
        }
        try initial.queue.close()

        // Re-open as if downloaded — no migration work, no remote-sync requirement.
        let reopened = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        defer { try? reopened.queue.close() }
        XCTAssertFalse(reopened.requiresRemoteSync,
                       "a HEAD-schema manifest must not be re-flushed to remote; that signals churn")
    }

    // MARK: - Helpers

    /// Build a sqlite file matching v1's legacy `*Ns` schema. Caller supplies
    /// content via `seed` (executed inside a write transaction).
    private func buildLegacyNsManifest(at url: URL, seed: (Database) throws -> Void) throws {
        let queue = try DatabaseQueue(path: url.path)
        try queue.write { db in
            try db.execute(sql: """
            CREATE TABLE resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """)
            try seed(db)
        }
        try queue.close()
    }
}
