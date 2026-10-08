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

    func testTimestampAndSerializationDoNotChangeIdentityButOriginalHashesDiffer() throws {
        let original = ContentIdentityFixtures.adjustment(timestamp: 1, encoding: .xml)
        let imported = ContentIdentityFixtures.adjustment(timestamp: 2, encoding: .binary)
        XCTAssertNotEqual(ContentIdentityFixtures.hash(original), ContentIdentityFixtures.hash(imported))
        XCTAssertEqual(AssetContentFingerprint.adjustmentHash(original, roles: [1, 5, 7]),
                       AssetContentFingerprint.adjustmentHash(imported, roles: [1, 5, 7]))
        let oldFields = try XCTUnwrap(try PropertyListSerialization.propertyList(from: original, format: nil) as? [String: Any])
        XCTAssertEqual(oldFields["adjustmentTimestamp"] as? Date, Date(timeIntervalSince1970: 1))
    }

    func testEditingRecipeFormatAndUnknownFieldsRemainPartOfIdentity() {
        let original = ContentIdentityFixtures.adjustment()
        let variants = [
            ContentIdentityFixtures.adjustment(payload: Data("different crop".utf8)),
            ContentIdentityFixtures.adjustment(format: "other.editor"),
            ContentIdentityFixtures.adjustment(version: "2"),
            ContentIdentityFixtures.adjustment(extra: ["adjustmentBaseVersion": 1]),
            ContentIdentityFixtures.adjustment(extra: ["futureSetting": ["enabled": true, "values": [1, 2]]])
        ]
        for other in variants {
            XCTAssertNotEqual(AssetContentFingerprint.adjustmentHash(original, roles: [1, 5, 7]),
                              AssetContentFingerprint.adjustmentHash(other, roles: [1, 5, 7]))
        }
    }

    func testOnlyVerifiedVideoRenderFlagPairIsNormalized() {
        let original = ContentIdentityFixtures.adjustment(flags: 16384)
        let imported = ContentIdentityFixtures.adjustment(timestamp: 2, flags: 18944)
        XCTAssertEqual(AssetContentFingerprint.adjustmentHash(original, roles: [2, 5, 6, 7]),
                       AssetContentFingerprint.adjustmentHash(imported, roles: [2, 5, 6, 7]))
        XCTAssertNotEqual(AssetContentFingerprint.adjustmentHash(original, roles: [1, 5, 7]),
                          AssetContentFingerprint.adjustmentHash(imported, roles: [1, 5, 7]))
        for flags in [0, 18945, 32768] {
            XCTAssertNotEqual(AssetContentFingerprint.adjustmentHash(original, roles: [2, 5, 6, 7]),
                              AssetContentFingerprint.adjustmentHash(ContentIdentityFixtures.adjustment(flags: flags), roles: [2, 5, 6, 7]))
        }
        XCTAssertNotEqual(AssetContentFingerprint.adjustmentHash(ContentIdentityFixtures.adjustment(flags: 16384, version: "2"), roles: [2, 5, 6, 7]),
                          AssetContentFingerprint.adjustmentHash(ContentIdentityFixtures.adjustment(flags: 18944, version: "2"), roles: [2, 5, 6, 7]))
    }

    func testUnrecognizedMetadataFallsBackToFullHashAndCorruptBytesAreRejected() throws {
        for data in [Data("opaque metadata".utf8), Data("<plist><dict></dict></plist>".utf8)] {
            XCTAssertEqual(AssetContentFingerprint.adjustmentHash(data, roles: [1, 5, 7]), ContentIdentityFixtures.hash(data))
        }
        let original = ContentIdentityFixtures.adjustment()
        let resources = ContentIdentityFixtures.resources(adjustment: original)
        XCTAssertThrowsError(try AssetContentFingerprint.fingerprint(resources: resources,
            adjustmentData: [ContentIdentityFixtures.hash(original): Data("corrupt".utf8)]))
    }

    func testNormalizedFingerprintSurvivesIdentifierChangeAndDetectsContentEdits() throws {
        let original = ContentIdentityFixtures.adjustment()
        let imported = ContentIdentityFixtures.adjustment(timestamp: 2, flags: 18944)
        func fingerprint(_ data: Data, media: UInt8 = 1) throws -> Data {
            try AssetContentFingerprint.fingerprint(resources: ContentIdentityFixtures.resources(adjustment: data, media: media),
                adjustmentData: [ContentIdentityFixtures.hash(data): data])
        }
        let expected = try fingerprint(original)
        XCTAssertEqual(try fingerprint(imported), expected)
        XCTAssertNotEqual(try fingerprint(imported, media: 9), expected)
        let resources = ContentIdentityFixtures.resources(adjustment: imported)
        for id in ["old-id", "new-id"] {
            try repository.clearLocalHashIndex()
            try repository.upsertAssetHashSnapshot(assetLocalIdentifier: id, assetFingerprint: fingerprint(imported),
                resources: resources.map { .init(role: $0.role, slot: $0.slot, contentHash: $0.hash, fileSize: 100) },
                totalFileSizeBytes: 400, modificationDateMs: 1_000)
            XCTAssertEqual(try repository.fetchAssetFingerprintRecords()[id]?.fingerprint, expected)
        }
    }

    func testMetadataChangesNeverBypassFileIntegrityVerification() throws {
        let source = ContentIdentityFixtures.adjustment()
        let other = ContentIdentityFixtures.adjustment(timestamp: 2)
        let url = directory.appendingPathComponent("adjustment.plist")
        try other.write(to: url)
        let resource = RemoteAssetResourceInstance(role: 7, slot: 0, resourceHash: ContentIdentityFixtures.hash(source),
            fileName: "adjustment.plist", fileSize: Int64(source.count), remoteRelativePath: "2026/01/adjustment.plist", creationDateMs: 0)
        XCTAssertThrowsError(try RestoreService.verifyDownloadedResource(at: url, instance: resource))
    }

    func testExistingSchemaPreservesRowsAndInvalidatesOnlyLegacyAdjustmentFingerprintsOnce() throws {
        let adjustment = ContentIdentityFixtures.adjustment()
        let editedResources = ContentIdentityFixtures.resources(adjustment: adjustment)
        let ordinaryResources = [AssetContentFingerprint.Resource(role: 1, slot: 0, hash: Data(repeating: 1, count: 32))]
        try index(editedResources, id: "edited")
        try index(ordinaryResources, id: "ordinary")
        try database.write { db in
            try db.execute(sql: "DELETE FROM sync_state WHERE stateKey = 'local_asset_fingerprint_version'")
        }
        repository = nil
        database = nil
        database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: database)
        let records = try repository.fetchAssetFingerprintRecords()
        XCTAssertNil(records["edited"])
        XCTAssertEqual(records["ordinary"]?.fingerprint, rawFingerprint(ordinaryResources))
        try database.read { db in
            XCTAssertFalse(try db.tableExists("restore_origins"))
            XCTAssertFalse(try db.tableExists("asset_content_identities"))
            XCTAssertFalse(try db.tableExists("pending_restore_imports"))
            let migrations = try String.fetchAll(db, sql: "SELECT identifier FROM grdb_migrations ORDER BY identifier")
            XCTAssertEqual(migrations.count, 7)
            XCTAssertEqual(migrations.last, "v7_background_backup_node_settings")
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_assets"), 2)
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_asset_resources"), editedResources.count + ordinaryResources.count)
            XCTAssertEqual(try String.fetchOne(db, sql: "PRAGMA integrity_check"), "ok")
        }
        try index(editedResources, id: "edited")
        let reopened = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        XCTAssertEqual(try reopened.fetchAssetFingerprintRecords()["edited"]?.fingerprint, rawFingerprint(editedResources))
    }

    private func rawFingerprint(_ resources: [AssetContentFingerprint.Resource]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map { ($0.role, $0.slot, $0.hash) })
    }

    private func index(_ resources: [AssetContentFingerprint.Resource], id: String) throws {
        try repository.upsertAssetHashSnapshot(assetLocalIdentifier: id, assetFingerprint: rawFingerprint(resources),
            resources: resources.map { .init(role: $0.role, slot: $0.slot, contentHash: $0.hash, fileSize: 100) },
            totalFileSizeBytes: 400, modificationDateMs: 1_000)
    }
}
