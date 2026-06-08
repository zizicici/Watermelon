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

    func testServerProfilesSchemaHasWriterIDColumn() throws {
        let columns = try databaseManager.read { db in
            try db.columns(in: ServerProfileRecord.databaseTableName).map { $0.name }
        }
        XCTAssertTrue(columns.contains("writerID"), "server_profiles must expose a writerID column")
    }
}
