import XCTest
import GRDB
@testable import Watermelon

final class DatabaseManagerWriterIDTests: XCTestCase {
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    private func makeProfile(writerID: String? = nil) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "server",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host.local",
            port: 445,
            shareName: "share",
            basePath: "/photos",
            username: "user",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: writerID
        )
    }

    private func liveWriterID(id: Int64) throws -> String? {
        try databaseManager.read { db in
            try String.fetchOne(
                db,
                sql: "SELECT writerID FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                arguments: [id]
            )
        }
    }

    func testSaveGeneratesLowercasedUUIDWhenMissing() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)

        let writerID = try XCTUnwrap(profile.writerID, "save must populate a writer ID")
        XCTAssertNotNil(UUID(uuidString: writerID), "writer ID must be a UUID string")
        XCTAssertEqual(writerID, writerID.lowercased(), "writer ID must be lowercased")
        XCTAssertEqual(try liveWriterID(id: try XCTUnwrap(profile.id)), writerID)
    }

    func testDefaultResourceStorageCodecDefaultsPlainAndCanBeUpdated() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let id = try XCTUnwrap(profile.id)

        var fetched = try XCTUnwrap(try databaseManager.fetchServerProfiles().first { $0.id == id })
        XCTAssertEqual(fetched.defaultResourceStorageCodec, RemoteManifestResource.plaintextStorageCodec)
        XCTAssertFalse(fetched.defaultResourceStorageIsEncrypted)

        try databaseManager.setDefaultResourceStorageCodec(RemoteManifestResource.encryptedStorageCodec, profileID: id)

        fetched = try XCTUnwrap(try databaseManager.fetchServerProfiles().first { $0.id == id })
        XCTAssertEqual(fetched.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertTrue(fetched.defaultResourceStorageIsEncrypted)
    }

    func testResavePreservesLiveDefaultResourceStorageCodec() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let id = try XCTUnwrap(profile.id)
        try databaseManager.setDefaultResourceStorageCodec(RemoteManifestResource.encryptedStorageCodec, profileID: id)

        var stale = profile
        stale.name = "renamed"
        stale.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
        try databaseManager.saveServerProfile(&stale)

        let fetched = try XCTUnwrap(try databaseManager.fetchServerProfiles().first { $0.id == id })
        XCTAssertEqual(stale.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(fetched.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(fetched.name, "renamed")
    }

    func testResaveDoesNotRotateWriterID() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let original = try XCTUnwrap(profile.writerID)

        profile.name = "renamed"
        try databaseManager.saveServerProfile(&profile)

        XCTAssertEqual(profile.writerID, original, "re-saving must not rotate the writer ID")
        XCTAssertEqual(try liveWriterID(id: try XCTUnwrap(profile.id)), original)
    }

    func testNilInMemoryWriterIDPreservesLiveValue() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let live = try XCTUnwrap(profile.writerID)

        var stale = profile
        stale.writerID = nil
        try databaseManager.saveServerProfile(&stale)

        XCTAssertEqual(stale.writerID, live, "nil in-memory writer ID must be replaced by the live value")
        XCTAssertEqual(try liveWriterID(id: try XCTUnwrap(profile.id)), live)
    }

    func testStaleInMemoryWriterIDPreservesLiveValue() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let live = try XCTUnwrap(profile.writerID)

        var stale = profile
        stale.writerID = "00000000-0000-0000-0000-000000000000"
        try databaseManager.saveServerProfile(&stale)

        XCTAssertEqual(stale.writerID, live, "stale in-memory writer ID must not overwrite the live value")
        XCTAssertEqual(try liveWriterID(id: try XCTUnwrap(profile.id)), live)
    }

    func testDeletionLeavesNoProfileWithWriterID() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let writerID = try XCTUnwrap(profile.writerID)

        try databaseManager.deleteServerProfile(id: try XCTUnwrap(profile.id))

        let remaining = try databaseManager.read { db in
            try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM \(ServerProfileRecord.databaseTableName) WHERE writerID = ?",
                arguments: [writerID]
            ) ?? 0
        }
        XCTAssertEqual(remaining, 0, "deleting a profile must leave no row with its writer ID")
    }

    func testNewRecordWithPrefilledWriterIDStillGeneratesCanonicalValue() throws {
        var profile = makeProfile(writerID: "not-a-uuid")
        try databaseManager.saveServerProfile(&profile)

        let writerID = try XCTUnwrap(profile.writerID)
        XCTAssertNotEqual(writerID, "not-a-uuid", "a caller-supplied writer ID must not be persisted")
        XCTAssertNotNil(UUID(uuidString: writerID), "generated writer ID must be a UUID string")
        XCTAssertEqual(writerID, writerID.lowercased(), "generated writer ID must be lowercased")
        XCTAssertEqual(try liveWriterID(id: try XCTUnwrap(profile.id)), writerID)
    }

    func testLiveNullWriterIDGeneratesInsteadOfAcceptingPrefilledValue() throws {
        // Simulate a row carried over from a pre-v3 DB: writerID is still NULL.
        let id = try databaseManager.write { db -> Int64 in
            try db.execute(
                sql: """
                INSERT INTO \(ServerProfileRecord.databaseTableName)
                (name, storageType, sortOrder, host, port, shareName, basePath, username, credentialRef, backgroundBackupEnabled, createdAt, updatedAt, writerID)
                VALUES ('migrated', 'smb', 0, 'h', 445, 's', '/p', 'u', 'r', 0, '2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000', NULL)
                """
            )
            return db.lastInsertedRowID
        }
        XCTAssertNil(try liveWriterID(id: id), "precondition: live writer ID is NULL")

        let fetched = try databaseManager.read { db in
            try ServerProfileRecord.fetchOne(db, key: id)
        }
        var profile = try XCTUnwrap(fetched)
        profile.writerID = "stale-prefilled-value"
        try databaseManager.saveServerProfile(&profile)

        let writerID = try XCTUnwrap(profile.writerID)
        XCTAssertNotEqual(writerID, "stale-prefilled-value", "live-NULL save must not accept a prefilled value")
        XCTAssertNotNil(UUID(uuidString: writerID), "generated writer ID must be a UUID string")
        XCTAssertEqual(writerID, writerID.lowercased(), "generated writer ID must be lowercased")
        XCTAssertEqual(try liveWriterID(id: id), writerID)
    }

    // MARK: - Lazy backfill (P08 Phase 6 / F14)

    func testBackfillGeneratesAndPersistsWhenLiveWriterIDNull() throws {
        // A row carried over from a pre-v3 DB: writerID still NULL.
        let id = try databaseManager.write { db -> Int64 in
            try db.execute(
                sql: """
                INSERT INTO \(ServerProfileRecord.databaseTableName)
                (name, storageType, sortOrder, host, port, shareName, basePath, username, credentialRef, backgroundBackupEnabled, createdAt, updatedAt, writerID)
                VALUES ('migrated', 'smb', 0, 'h', 445, 's', '/p', 'u', 'r', 0, '2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000', NULL)
                """
            )
            return db.lastInsertedRowID
        }
        let fetched = try databaseManager.read { db in try ServerProfileRecord.fetchOne(db, key: id) }
        let profile = try XCTUnwrap(fetched)
        XCTAssertNil(profile.writerID, "precondition: upgraded profile carries no writer ID")

        let backfilled = try databaseManager.profileWithBackfilledWriterID(profile)
        let writerID = try XCTUnwrap(backfilled.writerID, "backfill must populate a writer ID")
        XCTAssertNotNil(UUID(uuidString: writerID), "writer ID must be a UUID string")
        XCTAssertEqual(writerID, writerID.lowercased(), "writer ID must be lowercased")
        XCTAssertEqual(try liveWriterID(id: id), writerID, "backfill must persist the minted writer ID")
    }

    func testBackfillPreservesLiveValueAndIgnitesNoRotation() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let live = try XCTUnwrap(profile.writerID)
        let id = try XCTUnwrap(profile.id)

        // Stale in-memory value must not overwrite the live DB value, and backfill must not rotate.
        var stale = profile
        stale.writerID = "00000000-0000-0000-0000-000000000000"
        let backfilled = try databaseManager.profileWithBackfilledWriterID(stale)
        XCTAssertEqual(backfilled.writerID, live, "backfill must return the live value, not the stale one")
        XCTAssertEqual(try liveWriterID(id: id), live, "backfill must not rotate the persisted writer ID")

        let again = try databaseManager.profileWithBackfilledWriterID(backfilled)
        XCTAssertEqual(again.writerID, live, "repeated backfill is stable")
    }

    func testBackfillPreservesLiveDefaultResourceStorageCodec() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let id = try XCTUnwrap(profile.id)
        try databaseManager.setDefaultResourceStorageCodec(RemoteManifestResource.encryptedStorageCodec, profileID: id)

        var stale = profile
        stale.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
        let backfilled = try databaseManager.profileWithBackfilledWriterID(stale)

        XCTAssertEqual(backfilled.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        let fetched = try XCTUnwrap(try databaseManager.fetchServerProfiles().first { $0.id == id })
        XCTAssertEqual(fetched.defaultResourceStorageCodec, RemoteManifestResource.encryptedStorageCodec)
    }

    func testBackfillUnsavedProfileReturnedUnchanged() throws {
        let unsaved = makeProfile(writerID: nil)   // id == nil
        XCTAssertNil(unsaved.id)
        let result = try databaseManager.profileWithBackfilledWriterID(unsaved)
        XCTAssertNil(result.writerID, "an unsaved profile has no row to backfill; nil identity is preserved")
    }

    // R02 regression (R01 Codex Medium): a saved-looking profile whose row was deleted must never receive a
    // freshly minted, unpersisted identity — nil must stay nil so the caller fails closed.
    func testBackfillMissingRowWithNilWriterIDDoesNotMint() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)            // mints a writer ID and inserts a row
        let id = try XCTUnwrap(profile.id)
        try databaseManager.deleteServerProfile(id: id)            // the row is now gone

        var stale = profile
        stale.writerID = nil                                       // stale in-memory: id set, identity lost
        let result = try databaseManager.profileWithBackfilledWriterID(stale)
        XCTAssertNil(result.writerID, "a missing row with nil identity must not be minted a writer ID")
        XCTAssertNil(try liveWriterID(id: id), "there is no row to persist a writer ID into")
    }

    // A missing row with an existing in-memory identity keeps it (acceptable) but resurrects no DB row.
    func testBackfillMissingRowPreservesInMemoryWriterIDWithoutPersisting() throws {
        var profile = makeProfile(writerID: nil)
        try databaseManager.saveServerProfile(&profile)
        let id = try XCTUnwrap(profile.id)
        let original = try XCTUnwrap(profile.writerID)
        try databaseManager.deleteServerProfile(id: id)

        let result = try databaseManager.profileWithBackfilledWriterID(profile)   // still carries `original`
        XCTAssertEqual(result.writerID, original, "a missing row preserves an existing in-memory identity")
        XCTAssertNil(try liveWriterID(id: id), "preserving the in-memory value must not resurrect a row")
    }

    func testServerProfilesSchemaHasWriterIDColumn() throws {
        let columns = try databaseManager.read { db in
            try db.columns(in: ServerProfileRecord.databaseTableName).map { $0.name }
        }
        XCTAssertTrue(columns.contains("writerID"), "server_profiles must expose a writerID column")
    }
}
