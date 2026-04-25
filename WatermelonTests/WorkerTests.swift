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

        // With matching scope, the IDs surface.
        XCTAssertEqual(worker.localAssetIDs(for: key, expectedScope: .allPhotos), ["a"])

        // With a mismatched scope, the worker must return [] regardless of engine state.
        XCTAssertTrue(worker.localAssetIDs(for: key, expectedScope: .albums(["x"])).isEmpty)
    }

    func testRefreshLocalIndex_returnsEmpty_onScopeMismatch() async {
        // Scope-mismatch guard on `refreshLocalIndex` is the cleanest scope-only gate
        // on the worker (`remoteOnlyItems` ANDs scope with hasActiveConnection + remote
        // data presence, so it cannot distinguish "scope mismatch" from "other reasons").
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

    func testWriteFileSizeIfScopeStable_skipsWhenScopeChangedMidScan() async {
        // Critical Invariant #2: a reload that lands between the scan's sample and its
        // write-back must invalidate the write-back. Otherwise pre-reload totals would
        // be re-applied to a freshly-wiped `monthFileSizes` map (and flash stale UI).
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .allPhotos,
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])]
        )

        // Sample took place under .allPhotos; meanwhile a reload landed and the worker
        // is now under .albums(["x"]).
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
