import XCTest
import GRDB
@testable import Watermelon

/// Each test pins a specific promise the migration code makes:
/// - v2 ns→ms: rename + divide; NULL preserved; 0 preserved
/// - v3 writerID: nullable for existing rows (NOT NULL would crash legacy data)
/// - v3 repo_state: composite PK on (profileID, repoID)
/// - v1 SMB unique: partial on storageType='smb'; IFNULL(domain,'') collapses NULL/""
/// - Re-open idempotency: re-running migrator preserves data and indexes
final class DatabaseMigratorTests: XCTestCase {
    private var tempDir: URL!

    override func setUpWithError() throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: tempDir)
    }


    /// Production data path: a real ns timestamp in `local_assets.modificationDateNs`
    /// must end up at the same wall-clock instant in `modificationDateMs`.
    func testV2_nsToMs_realTimestamp_convertedExactly() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        var migrator = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &migrator)
        try migrator.migrate(queue)

        let nsValue: Int64 = 1_700_000_000_123_000_000  // a real ns timestamp
        try queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets
                (assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, modificationDateNs, updatedAt)
                VALUES (?, NULL, 0, 0, ?, ?)
                """,
                arguments: ["asset-1", nsValue, Date()]
            )
        }

        // Continue with the production v2 migration registered.
        var v2Migrator = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v2Migrator)
        registerProductionV2(into: &v2Migrator)
        try v2Migrator.migrate(queue)

        let result = try queue.read { db in
            try Int64.fetchOne(db, sql: "SELECT modificationDateMs FROM local_assets WHERE assetLocalIdentifier = 'asset-1'")
        }
        XCTAssertEqual(result, nsValue / 1_000_000,
                       "ms value must equal ns value divided by 1e6 (integer division)")
    }

    /// Catches a future "remove WHERE IS NOT NULL" change that would set NULL → 0,
    /// silently corrupting "we never observed this asset's mtime" into "epoch zero".
    func testV2_nsToMs_nullTimestampPreserved() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        var v1 = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v1)
        try v1.migrate(queue)

        try queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets
                (assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, modificationDateNs, updatedAt)
                VALUES (?, NULL, 0, 0, NULL, ?)
                """,
                arguments: ["asset-no-mtime", Date()]
            )
        }

        var v2 = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v2)
        registerProductionV2(into: &v2)
        try v2.migrate(queue)

        let result = try queue.read { db in
            try Row.fetchOne(db, sql: "SELECT modificationDateMs FROM local_assets WHERE assetLocalIdentifier = 'asset-no-mtime'")
        }
        XCTAssertNotNil(result, "row must still exist")
        XCTAssertNil(result?["modificationDateMs"] as Int64?,
                     "NULL ns must remain NULL ms; without the WHERE IS NOT NULL guard it would become 0")
    }

    /// 0 ns is a real (if absurd) timestamp; integer division by 1e6 is 0 — but the
    /// WHERE clause includes 0 (not NULL), so it gets explicitly written. Lock the
    /// behavior so a future change to `WHERE modificationDateMs > 0` doesn't subtly
    /// break this case.
    func testV2_nsToMs_zeroPreservedAsZero() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        var v1 = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v1)
        try v1.migrate(queue)

        try queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets
                (assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, modificationDateNs, updatedAt)
                VALUES (?, NULL, 0, 0, 0, ?)
                """,
                arguments: ["asset-zero", Date()]
            )
        }

        var v2 = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v2)
        registerProductionV2(into: &v2)
        try v2.migrate(queue)

        let result = try queue.read { db in
            try Int64.fetchOne(db, sql: "SELECT modificationDateMs FROM local_assets WHERE assetLocalIdentifier = 'asset-zero'")
        }
        XCTAssertEqual(result, 0)
    }

    func testV2_columnRenamedAndOldNameGone() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        _ = try DatabaseManager(databaseURL: url)
        let queue = try DatabaseQueue(path: url.path)
        let columns = try queue.read { db in
            try Set(Row.fetchAll(db, sql: "PRAGMA table_info(local_assets)").compactMap { $0["name"] as String? })
        }
        XCTAssertTrue(columns.contains("modificationDateMs"))
        XCTAssertFalse(columns.contains("modificationDateNs"),
                       "v2 must rename the column; both names coexisting would mean the rename failed silently")
    }


    /// Existing v2 profiles must survive v3: the new writerID column must be NULL
    /// (defaulting to NOT NULL would crash on first read of pre-v3 rows).
    func testV3_writerIDColumnIsNullableForExistingProfiles() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        var migrator = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &migrator)
        registerProductionV2(into: &migrator)
        try migrator.migrate(queue)

        // Insert a v2 profile (no writerID column yet).
        try queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO server_profiles
                (name, storageType, sortOrder, host, port, shareName, basePath, username, domain, credentialRef, backgroundBackupEnabled, createdAt, updatedAt)
                VALUES ('legacy', 'smb', 0, 'h', 445, 's', '/', 'u', NULL, 'ref', 1, ?, ?)
                """,
                arguments: [Date(), Date()]
            )
        }

        // Now apply v3.
        var v3Migrator = DatabaseMigrator()
        registerProductionMigrationsUpToV1(into: &v3Migrator)
        registerProductionV2(into: &v3Migrator)
        registerProductionV3(into: &v3Migrator)
        try v3Migrator.migrate(queue)

        let writerID = try queue.read { db in
            try Row.fetchOne(db, sql: "SELECT writerID FROM server_profiles WHERE name = 'legacy'")
        }
        XCTAssertNotNil(writerID, "row exists")
        XCTAssertNil(writerID?["writerID"] as String?,
                     "writerID must be NULL for pre-v3 profiles; NOT NULL default would crash legacy data")
    }

    func testV3_repoState_compositePrimaryKey_uniquePerProfileRepoPair() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let dbm = try DatabaseManager(databaseURL: url)

        // (profileID=1, repoID="A"), (1, "B"), (2, "A") → all OK
        try dbm.write { db in
            try RepoStateRecord(profileID: 1, repoID: "A", writerID: "w", lastClock: 0, lastSeq: 0, migrationCompleted: 0).insert(db)
            try RepoStateRecord(profileID: 1, repoID: "B", writerID: "w", lastClock: 0, lastSeq: 0, migrationCompleted: 0).insert(db)
            try RepoStateRecord(profileID: 2, repoID: "A", writerID: "w", lastClock: 0, lastSeq: 0, migrationCompleted: 0).insert(db)
        }

        // Duplicate (1, "A") must fail.
        do {
            try dbm.write { db in
                try RepoStateRecord(profileID: 1, repoID: "A", writerID: "w2", lastClock: 1, lastSeq: 1, migrationCompleted: 1).insert(db)
            }
            XCTFail("expected unique constraint violation on (profileID, repoID)")
        } catch let error as DatabaseError {
            XCTAssertEqual(error.resultCode, .SQLITE_CONSTRAINT,
                           "duplicate composite key must produce a constraint error")
        }
    }


    /// The unique index is `WHERE storageType = 'smb'` on purpose: the same (host,
    /// port, share, basePath, user, domain) tuple may legitimately exist for SMB
    /// AND WebDAV (different protocols, same NAS). Loosening the WHERE would lock
    /// out cross-protocol setups.
    func testV1_smbUniqueIndex_doesNotApplyToNonSMBStorageTypes() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let dbm = try DatabaseManager(databaseURL: url)

        try dbm.write { db in
            try db.execute(
                sql: """
                INSERT INTO server_profiles
                (name, storageType, sortOrder, host, port, shareName, basePath, username, domain, credentialRef, backgroundBackupEnabled, createdAt, updatedAt)
                VALUES ('w1', 'webdav', 0, 'h', 0, '', '/', 'u', NULL, 'ref', 1, ?, ?)
                """,
                arguments: [Date(), Date()]
            )
            // Same tuple, different storage type, must NOT collide.
            try db.execute(
                sql: """
                INSERT INTO server_profiles
                (name, storageType, sortOrder, host, port, shareName, basePath, username, domain, credentialRef, backgroundBackupEnabled, createdAt, updatedAt)
                VALUES ('w2', 'webdav', 1, 'h', 0, '', '/', 'u', NULL, 'ref', 1, ?, ?)
                """,
                arguments: [Date(), Date()]
            )
        }

        let count = try dbm.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM server_profiles WHERE storageType = 'webdav'") ?? 0
        }
        XCTAssertEqual(count, 2, "non-SMB profiles must not be subject to the partial unique index")
    }

    func testV1_smbUniqueIndex_blocksDuplicateSMBProfiles() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let dbm = try DatabaseManager(databaseURL: url)
        try insertSMBProfile(dbm: dbm, name: "first", domain: "WORK")

        do {
            try insertSMBProfile(dbm: dbm, name: "second", domain: "WORK")
            XCTFail("duplicate SMB tuple must be rejected by the partial unique index")
        } catch let error as DatabaseError {
            XCTAssertEqual(error.resultCode, .SQLITE_CONSTRAINT)
        }
    }

    /// `IFNULL(domain, '')` in the index: a SMB profile with `domain=NULL` and one
    /// with `domain=""` are the SAME endpoint and must collide. Removing IFNULL
    /// would let them coexist as duplicates pointing at the same NAS.
    func testV1_smbUniqueIndex_treatsNullDomainEquivalentToEmptyString() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let dbm = try DatabaseManager(databaseURL: url)
        try insertSMBProfile(dbm: dbm, name: "null-domain", domain: nil)

        do {
            try insertSMBProfile(dbm: dbm, name: "empty-domain", domain: "")
            XCTFail("NULL domain and empty-string domain must be treated as the same key")
        } catch let error as DatabaseError {
            XCTAssertEqual(error.resultCode, .SQLITE_CONSTRAINT)
        }
    }

    /// Distinct domains must coexist (same host, different Windows workgroup is a
    /// distinct credential/auth scope).
    func testV1_smbUniqueIndex_allowsDistinctDomains() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        let dbm = try DatabaseManager(databaseURL: url)
        try insertSMBProfile(dbm: dbm, name: "work", domain: "WORK")
        try insertSMBProfile(dbm: dbm, name: "home", domain: "HOME")

        let count = try dbm.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM server_profiles") ?? 0
        }
        XCTAssertEqual(count, 2)
    }


    /// Re-opening the same DB file must be a no-op for already-applied migrations.
    /// Catches future migrations that don't gate on schema state (e.g. forgetting
    /// `IF NOT EXISTS` on a CREATE INDEX inside a non-GRDB-tracked migration).
    func testReopen_dataPreservedAcrossSecondInit() throws {
        let url = tempDir.appendingPathComponent("db.sqlite")
        do {
            let dbm = try DatabaseManager(databaseURL: url)
            try insertSMBProfile(dbm: dbm, name: "stable", domain: "WORK")
            try dbm.write { db in
                try RepoStateRecord(profileID: 1, repoID: "A", writerID: "w", lastClock: 5, lastSeq: 7, migrationCompleted: 1).insert(db)
            }
        }
        // Second open: migrator runs, must not corrupt or drop anything.
        let dbm2 = try DatabaseManager(databaseURL: url)
        let profiles = try dbm2.fetchServerProfiles()
        XCTAssertEqual(profiles.count, 1)
        XCTAssertEqual(profiles.first?.name, "stable")

        let repoState = try dbm2.read { db in
            try RepoStateRecord
                .filter(Column("profileID") == 1)
                .filter(Column("repoID") == "A")
                .fetchOne(db)
        }
        XCTAssertEqual(repoState?.lastSeq, 7,
                       "previously-persisted lastSeq must survive a re-open + re-migrate cycle")
    }

    /// The makeMigrator() factory returns the same migration list every call —
    /// state-free, no hidden ordering. Catches future code that captures `self` or
    /// reads instance state inside the migrator (would silently break across
    /// processes / threads).
    func testMakeMigrator_isPureFactoryAcrossCalls() throws {
        let url1 = tempDir.appendingPathComponent("db1.sqlite")
        let url2 = tempDir.appendingPathComponent("db2.sqlite")
        let queue1 = try DatabaseQueue(path: url1.path)
        let queue2 = try DatabaseQueue(path: url2.path)
        try DatabaseManager.makeMigrator().migrate(queue1)
        try DatabaseManager.makeMigrator().migrate(queue2)

        let names1 = try queue1.read { db in
            try Set(String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'grdb%' AND name NOT LIKE 'sqlite_%'"))
        }
        let names2 = try queue2.read { db in
            try Set(String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'grdb%' AND name NOT LIKE 'sqlite_%'"))
        }
        XCTAssertEqual(names1, names2, "two fresh DBs migrated through makeMigrator() must end up with identical schemas")
    }


    private func insertSMBProfile(dbm: DatabaseManager, name: String, domain: String?) throws {
        try dbm.write { db in
            try db.execute(
                sql: """
                INSERT INTO server_profiles
                (name, storageType, sortOrder, host, port, shareName, basePath, username, domain, credentialRef, backgroundBackupEnabled, createdAt, updatedAt)
                VALUES (?, 'smb', 0, 'host.local', 445, 'Share', '/path', 'user', ?, 'ref', 1, ?, ?)
                """,
                arguments: [name, domain, Date(), Date()]
            )
        }
    }

    /// Mirrors production v1's CREATE TABLE + index statements exactly. We can't
    /// stop the production migrator after v1 directly (the migrator is `let`-style
    /// configured up-front), so we register a structurally-identical v1 here for
    /// stepwise tests. *If production v1 changes, update this helper.*
    private func registerProductionMigrationsUpToV1(into migrator: inout DatabaseMigrator) {
        migrator.registerMigration("v1_initial") { db in
            try db.execute(sql: """
            CREATE TABLE server_profiles (
              id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
              name TEXT NOT NULL,
              storageType TEXT NOT NULL DEFAULT 'smb',
              connectionParams BLOB,
              sortOrder INTEGER NOT NULL DEFAULT 0,
              host TEXT NOT NULL,
              port INTEGER NOT NULL,
              shareName TEXT NOT NULL,
              basePath TEXT NOT NULL,
              username TEXT NOT NULL,
              domain TEXT,
              credentialRef TEXT NOT NULL,
              backgroundBackupEnabled INTEGER NOT NULL DEFAULT 1,
              createdAt DATETIME NOT NULL,
              updatedAt DATETIME NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE UNIQUE INDEX idx_server_profiles_unique_smb
            ON server_profiles(host, port, shareName, basePath, username, IFNULL(domain, ''))
            WHERE storageType = 'smb'
            """)
            try db.execute(sql: """
            CREATE TABLE sync_state (
              stateKey TEXT NOT NULL PRIMARY KEY,
              stateValue TEXT NOT NULL,
              updatedAt DATETIME NOT NULL
            )
            """)
            try db.execute(sql: """
            CREATE TABLE local_assets (
              assetLocalIdentifier TEXT NOT NULL,
              assetFingerprint BLOB,
              resourceCount INTEGER NOT NULL DEFAULT 0,
              totalFileSizeBytes INTEGER NOT NULL DEFAULT 0,
              modificationDateNs INTEGER,
              updatedAt DATETIME NOT NULL,
              PRIMARY KEY (assetLocalIdentifier)
            )
            """)
            try db.execute(sql: """
            CREATE INDEX idx_local_assets_has_fingerprint
            ON local_assets(assetLocalIdentifier)
            WHERE assetFingerprint IS NOT NULL
            """)
            try db.execute(sql: """
            CREATE TABLE local_asset_resources (
              assetLocalIdentifier TEXT NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (assetLocalIdentifier, role, slot)
            )
            """)
            try db.execute(sql: "CREATE INDEX idx_local_asset_resources_hash ON local_asset_resources(contentHash)")
        }
    }

    private func registerProductionV2(into migrator: inout DatabaseMigrator) {
        migrator.registerMigration("v2_ms_timestamps") { db in
            try db.execute(sql: "ALTER TABLE local_assets RENAME COLUMN modificationDateNs TO modificationDateMs")
            try db.execute(sql: """
            UPDATE local_assets
            SET modificationDateMs = modificationDateMs / 1000000
            WHERE modificationDateMs IS NOT NULL
            """)
        }
    }

    private func registerProductionV3(into migrator: inout DatabaseMigrator) {
        migrator.registerMigration("v3_repo_state") { db in
            try db.execute(sql: "ALTER TABLE server_profiles ADD COLUMN writerID TEXT")
            try db.execute(sql: """
            CREATE TABLE repo_state (
              profileID INTEGER NOT NULL,
              repoID TEXT NOT NULL,
              writerID TEXT NOT NULL,
              lastClock INTEGER NOT NULL DEFAULT 0,
              lastSeq INTEGER NOT NULL DEFAULT 0,
              migrationCompleted INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (profileID, repoID)
            )
            """)
        }
    }
}
