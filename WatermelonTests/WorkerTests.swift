import XCTest
@testable import Watermelon

final class WorkerTests: XCTestCase {
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

    private func makeWorker() -> HomeDataProcessingWorker {
        HomeDataProcessingWorker(
            photoLibraryService: PhotoLibraryService(),
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteMonthSnapshot: { _ in nil }
        )
    }

    // MARK: - Scope-guard on read paths

    func testLocalAssetIDs_returnsEmpty_onScopeMismatch() {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .allPhotos,
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])]
        )

        XCTAssertEqual(worker.localAssetIDs(for: key, expectedScope: .allPhotos), ["a"])
        XCTAssertTrue(worker.localAssetIDs(for: key, expectedScope: .albums(["x"])).isEmpty)
    }

    func testRefreshLocalIndex_returnsEmpty_onScopeMismatch() async {
        // Worker's `remoteOnlyItems` ANDs scope with hasActiveConnection + remote data
        // presence so it can't isolate scope-mismatch alone — `refreshLocalIndex` can.
        let worker = makeWorker()
        worker._testSeed(
            scope: .allPhotos,
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])]
        )

        let result = await worker.refreshLocalIndex(
            forAssetIDs: ["a"],
            expectedScope: .albums(["x"])
        )
        XCTAssertTrue(result.isEmpty)
    }

    // MARK: - File-size scan write-back gate (Critical Invariant #2)

    func testWriteFileSizeIfScopeStable_writesWhenScopeUnchanged() async {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .allPhotos,
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])]
        )

        let didWrite = await worker.writeFileSizeIfScopeStable(
            12345,
            for: key,
            sampledScope: .allPhotos
        )
        XCTAssertTrue(didWrite)
        XCTAssertEqual(worker._testMonthFileSize(for: key), 12345)
    }

    // MARK: - syncRemoteSnapshot connection flip

    func testSyncRemoteSnapshot_disconnect_clearsRemoteSummary() async {
        // Critical Invariant: hasActiveConnection=false must wipe remote summaries
        // so `monthRow.remote` doesn't leak stale data to UI after disconnect.
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 1)
        let fp = Data([0xAA])
        let hash = Data([0xBB])
        let delta = TestFixtures.remoteMonthDelta(
            key,
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: 2024, month: 1, contentHash: hash)],
            links: [TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: hash)]
        )

        _ = await worker.syncRemoteSnapshot(
            state: TestFixtures.remoteSnapshotState(revision: 1, isFullSnapshot: true, deltas: [delta]),
            hasActiveConnection: true
        )
        XCTAssertNotNil(worker.monthRow(for: key).remote, "connected sync should populate remote summary")

        _ = await worker.syncRemoteSnapshot(
            state: TestFixtures.remoteSnapshotState(revision: 2, isFullSnapshot: false, deltas: []),
            hasActiveConnection: false
        )
        XCTAssertNil(worker.monthRow(for: key).remote, "disconnect must drop remote summary")
    }

    func testWriteFileSizeIfScopeStable_skipsWhenScopeChangedMidScan() async {
        // Critical Invariant #2: a reload landing between sample and write-back must
        // invalidate the write-back; otherwise pre-reload totals would land on a
        // freshly-wiped `monthFileSizes` and flash stale UI.
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .allPhotos,
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])]
        )

        let sample = await worker.sampleFileSizeScan(for: key)
        XCTAssertEqual(sample.scope, .allPhotos)
        XCTAssertEqual(sample.ids, ["a"])

        worker._testForceLoadedScope(.albums(["x"]))

        let didWrite = await worker.writeFileSizeIfScopeStable(
            999_999,
            for: key,
            sampledScope: sample.scope
        )
        XCTAssertFalse(didWrite, "write-back must be skipped when scope changed mid-scan")
        XCTAssertNil(worker._testMonthFileSize(for: key), "in-memory size must remain unset")
    }
}
