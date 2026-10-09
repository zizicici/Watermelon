import GRDB
import XCTest
@testable import Watermelon

final class AssetContentFingerprintTests: XCTestCase {
    private var directory: URL!
    private var database: DatabaseManager!
    private var repository: ContentHashIndexRepository!

    override func setUpWithError() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: database)
    }

    override func tearDownWithError() throws {
        repository = nil
        database = nil
        try FileManager.default.removeItem(at: directory)
    }

    func testAllAdjustmentBytesAndPresenceAreExcludedFromIdentity() {
        let original = ContentIdentityFixtures.resources(adjustment: ContentIdentityFixtures.adjustment())
        let expected = AssetContentFingerprint.fingerprint(resources: original)
        for adjustment in [ContentIdentityFixtures.adjustment(timestamp: 2, encoding: .xml),
                           ContentIdentityFixtures.adjustment(payload: Data("different recipe".utf8)),
                           ContentIdentityFixtures.adjustment(flags: 32768, format: "other.editor", version: "2"),
                           Data("opaque adjustment".utf8), Data()] {
            let resources = ContentIdentityFixtures.resources(adjustment: adjustment)
            XCTAssertEqual(AssetContentFingerprint.fingerprint(resources: resources), expected)
        }
        XCTAssertEqual(AssetContentFingerprint.fingerprint(resources: original.filter { $0.role != 7 }), expected)
        let additional = original + [.init(role: 7, slot: 1, hash: Data(repeating: 42, count: 32))]
        XCTAssertEqual(AssetContentFingerprint.fingerprint(resources: additional), expected)
    }

    func testMediaBytesRolesAndSlotsStillDetermineIdentity() {
        let original = ContentIdentityFixtures.resources(adjustment: Data())
        let expected = AssetContentFingerprint.fingerprint(resources: original)
        for replacement in [AssetContentFingerprint.Resource(role: 2, slot: 0, hash: Data(repeating: 99, count: 32)),
                            .init(role: 1, slot: 0, hash: original[0].hash),
                            .init(role: 2, slot: 1, hash: original[0].hash)] {
            XCTAssertNotEqual(AssetContentFingerprint.fingerprint(resources: [replacement] + original.dropFirst()), expected)
        }
        XCTAssertEqual(AssetContentFingerprint.fingerprint(resources: original.reversed()), expected)
    }

    func testExcludingAdjustmentNeverBypassesFileIntegrityVerification() throws {
        let source = ContentIdentityFixtures.adjustment()
        let changed = ContentIdentityFixtures.adjustment(timestamp: 2)
        let url = directory.appendingPathComponent("adjustment.plist")
        try changed.write(to: url)
        let resource = RemoteAssetResourceInstance(role: 7, slot: 0, resourceHash: ContentIdentityFixtures.hash(source),
            fileName: "adjustment.plist", fileSize: Int64(source.count), remoteRelativePath: "2026/01/adjustment.plist", creationDateMs: 0)
        XCTAssertThrowsError(try RestoreService.verifyDownloadedResource(at: url, instance: resource))
        try source.write(to: url)
        XCTAssertNoThrow(try RestoreService.verifyDownloadedResource(at: url, instance: resource))
    }

    func testLocalUpgradeRekeysCachedResourcesWithoutReadingPhotosOrChangingCacheAge() throws {
        let edited = ContentIdentityFixtures.resources(adjustment: ContentIdentityFixtures.adjustment())
        let ordinary = [AssetContentFingerprint.Resource(role: 1, slot: 0, hash: Data(repeating: 1, count: 32))]
        try index(edited, id: "edited")
        try index(ordinary, id: "ordinary")
        let before = try repository.fetchAssetFingerprintRecords()
        try database.write { db in
            try db.execute(sql: "DELETE FROM sync_state WHERE stateKey = 'local_asset_fingerprint_version'")
        }
        let oldFingerprint = try XCTUnwrap(before["edited"]?.fingerprint)
        let expected = AssetContentFingerprint.fingerprint(resources: edited)
        XCTAssertNotEqual(oldFingerprint, expected)
        try reopen()
        let records = try repository.fetchAssetFingerprintRecords()
        XCTAssertEqual(records["edited"]?.fingerprint, expected)
        XCTAssertEqual(records["edited"]?.updatedAt, before["edited"]?.updatedAt)
        XCTAssertEqual(records["ordinary"], before["ordinary"])
        XCTAssertTrue(try repository.fetchInvalidatedFingerprintAssetIDs().isEmpty)
        let cached = try XCTUnwrap(repository.fetchAssetHashCaches(assetIDs: ["edited"])["edited"])
        XCTAssertEqual(cached.hashesByRoleSlot[.init(role: 7, slot: 0)], edited.last?.hash)
        try database.read { db in
            XCTAssertFalse(try db.tableExists("restore_origins"))
            XCTAssertFalse(try db.tableExists("asset_content_identities"))
            XCTAssertFalse(try db.tableExists("pending_restore_imports"))
            let migrations = try String.fetchAll(db, sql: "SELECT identifier FROM grdb_migrations ORDER BY identifier")
            XCTAssertEqual(migrations.count, 7)
            XCTAssertEqual(migrations.last, "v7_background_backup_node_settings")
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_assets"), 2)
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_asset_resources"), edited.count + ordinary.count)
            XCTAssertEqual(try String.fetchOne(db, sql: "PRAGMA integrity_check"), "ok")
        }
        try reopen()
        XCTAssertEqual(try repository.fetchAssetFingerprintRecords()["edited"], records["edited"])
    }

    func testIncompleteLocalCacheRemainsUnknownAndEligibleForPreflight() throws {
        let resources = ContentIdentityFixtures.resources(adjustment: Data())
        try index(resources, id: "partial")
        try database.write { db in
            try db.execute(sql: "DELETE FROM sync_state WHERE stateKey = 'local_asset_fingerprint_version'")
            try db.execute(sql: "DELETE FROM local_asset_resources WHERE assetLocalIdentifier = 'partial' AND role = 2")
        }
        try reopen()
        XCTAssertNil(try repository.fetchAssetFingerprintRecords()["partial"])
        XCTAssertEqual(try repository.fetchInvalidatedFingerprintAssetIDs(), ["partial"])
        XCTAssertEqual(HomeDataProcessingWorker.fingerprintValidationAssetIDs(
            snapshots: [TestFixtures.snapshot(id: "partial")], records: [:], invalidatedAssetIDs: ["partial"]), ["partial"])
    }

    private func reopen() throws {
        repository = nil
        database = nil
        database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: database)
    }

    private func index(_ resources: [AssetContentFingerprint.Resource], id: String) throws {
        let old = BackupAssetResourcePlanner.legacyAssetFingerprint(resourceRoleSlotHashes: resources.map { ($0.role, $0.slot, $0.hash) })
        try repository.upsertAssetHashSnapshot(assetLocalIdentifier: id, assetFingerprint: old,
            resources: resources.map { .init(role: $0.role, slot: $0.slot, contentHash: $0.hash, fileSize: 100) },
            totalFileSizeBytes: 400, modificationDateMs: 1_000)
    }
}
