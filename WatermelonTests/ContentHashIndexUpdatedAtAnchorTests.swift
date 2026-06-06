import XCTest
@testable import Watermelon

/// The hash-index row's `updatedAt` is the trust anchor (`mtime > updatedAt` ⇒ stale). It must be the
/// content-capture time, not the DB-write time: a deferred batch-drain write would otherwise stamp a
/// time later than an edit made during the run, letting the stale fingerprint be trusted forever.
final class ContentHashIndexUpdatedAtAnchorTests: XCTestCase {
    private var tempDir: URL!
    private var databaseManager: DatabaseManager!
    private var repository: ContentHashIndexRepository!

    override func setUpWithError() throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        databaseManager = try DatabaseManager(databaseURL: tempDir.appendingPathComponent("db.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: databaseManager)
    }

    override func tearDownWithError() throws {
        repository = nil
        databaseManager = nil
        try? FileManager.default.removeItem(at: tempDir)
    }

    func testUpsertAssetHashSnapshotHonorsExplicitCaptureTime() throws {
        let captureTime = Date(timeIntervalSince1970: 1_700_000_000)
        let id = PhotoKitLocalIdentifier(rawValue: "asset-snapshot")
        let fp = AssetFingerprint(decoding: TestFixtures.fingerprint(0x21))!

        try repository.upsertAssetHashSnapshot(
            assetLocalIdentifier: id,
            assetFingerprint: fp,
            resources: [LocalAssetResourceHashRecord(role: 0, slot: 0, contentHash: Data([0x01]), fileSize: 1)],
            totalFileSizeBytes: 1,
            modificationDateMs: 0,
            selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion,
            resourceSignature: Data([0xAA]),
            updatedAt: captureTime
        )

        let record = try repository.fetchAssetFingerprintRecords(assetIDs: [id])[id]
        XCTAssertEqual(record?.updatedAt, captureTime,
                       "the row must carry the supplied capture time, not the wall-clock write time")
    }

    func testUpsertAssetFingerprintHonorsExplicitCaptureTime() throws {
        let captureTime = Date(timeIntervalSince1970: 1_690_000_000)
        let id = PhotoKitLocalIdentifier(rawValue: "asset-fingerprint")
        let fp = AssetFingerprint(decoding: TestFixtures.fingerprint(0x22))!

        try repository.upsertAssetFingerprint(
            assetLocalIdentifier: id,
            assetFingerprint: fp,
            resourceCount: 1,
            totalFileSizeBytes: 1,
            modificationDateMs: 0,
            updatedAt: captureTime
        )

        let record = try repository.fetchAssetFingerprintRecords(assetIDs: [id])[id]
        XCTAssertEqual(record?.updatedAt, captureTime)
    }

    /// The window the fix closes: an edit whose modificationDate falls after capture but before the
    /// (now irrelevant) write time must be rejected by trust because the anchor is the capture time.
    func testCaptureTimeAnchorRejectsEditLandingBeforeWriteTime() throws {
        let captureTime = Date(timeIntervalSince1970: 1_700_000_000)
        let editTime = captureTime.addingTimeInterval(30)       // edit during the run, after capture
        let id = PhotoKitLocalIdentifier(rawValue: "asset-window")
        let fp = AssetFingerprint(decoding: TestFixtures.fingerprint(0x23))!
        let signature = Data([0xAA])

        try repository.upsertAssetHashSnapshot(
            assetLocalIdentifier: id,
            assetFingerprint: fp,
            resources: [LocalAssetResourceHashRecord(role: 0, slot: 0, contentHash: Data([0x01]), fileSize: 1)],
            totalFileSizeBytes: 1,
            modificationDateMs: 0,
            selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion,
            resourceSignature: signature,
            updatedAt: captureTime
        )

        let record = try repository.fetchAssetFingerprintRecords(assetIDs: [id])[id]
        let cache = try XCTUnwrap(record).trustFields
        let editedShape = LocalHashIndexTrust.AssetShape(
            modificationDate: editTime,
            currentResourceSignature: signature
        )
        XCTAssertFalse(LocalHashIndexTrust.canTrust(cache, for: editedShape),
                       "an edit after the capture time must not be trusted even though it predates any later write")
    }
}
