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

    func testFingerprintValidationCandidatesRequireNewerModificationDate() {
        let rowAt = Date(timeIntervalSince1970: 1_000)
        let records = [
            "older": TestFixtures.record(Data([0x01]), updatedAt: rowAt),
            "same": TestFixtures.record(Data([0x02]), updatedAt: rowAt),
            "newer": TestFixtures.record(Data([0x03]), updatedAt: rowAt),
        ]
        let snapshots = [
            TestFixtures.snapshot(id: "older", modificationDate: Date(timeIntervalSince1970: 999)),
            TestFixtures.snapshot(id: "same", modificationDate: rowAt),
            TestFixtures.snapshot(id: "newer", modificationDate: Date(timeIntervalSince1970: 1_001)),
            TestFixtures.snapshot(id: "unindexed", modificationDate: Date(timeIntervalSince1970: 1_001)),
        ]

        XCTAssertEqual(
            HomeDataProcessingWorker.fingerprintValidationAssetIDs(
                snapshots: snapshots,
                records: records
            ),
            ["newer"]
        )
    }

    // MARK: - Scope-guard on read paths

    func testLocalAssetIDs_returnsEmpty_onScopeMismatch() {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a", year: 2024, month: 5)]])
        )

        XCTAssertEqual(worker.localAssetIDs(for: key, expectedScope: .device(.all)), ["a"])
        XCTAssertTrue(worker.localAssetIDs(for: key, expectedScope: .albums(["x"])).isEmpty)
    }

    func testRefreshLocalIndex_returnsEmpty_onScopeMismatch() async {
        // Worker's `remoteOnlyItems` ANDs scope with hasActiveConnection + remote data
        // presence so it can't isolate scope-mismatch alone — `refreshLocalIndex` can.
        let worker = makeWorker()
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a", year: 2024, month: 5)]])
        )

        let result = await worker.refreshLocalIndex(
            forAssetIDs: ["a"],
            expectedScope: .albums(["x"])
        )
        XCTAssertTrue(result.isEmpty)
    }

    func testBrowserLocalSeedRequiresLoadedAllPhotosScope() async {
        let worker = makeWorker()
        let fingerprint = Data([0x01])
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a")]]),
            fingerprints: ["a": TestFixtures.record(fingerprint)]
        )

        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(seed?.localIDByFingerprint, [fingerprint: "a"])
        XCTAssertEqual(seed?.assets.map(\.localIdentifier), ["a"])
        XCTAssertEqual(seed?.assets.first?.creationDateMs, TestFixtures.date(2024, 1).millisecondsSinceEpoch)
        XCTAssertEqual(seed?.assets.first?.fingerprint, fingerprint)

        worker._testForceLoadedScope(.albums(["album"]))
        let staleAllPhotosSeed = await worker.browserLocalSeed(expectedScope: .device(.all))
        let albumSeed = await worker.browserLocalSeed(expectedScope: .albums(["album"]))
        XCTAssertNil(staleAllPhotosSeed)
        XCTAssertNil(albumSeed)
    }

    // MARK: - File-size scan write-back gate (Critical Invariant #2)

    func testWriteFileSizeIfIndexStable_writesWhenIndexUnchanged() async {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a", year: 2024, month: 5)]])
        )
        let sample = await worker.sampleFileSizeScan(for: key)

        let didWrite = await worker.writeFileSizeIfIndexStable(
            12345,
            for: key,
            sampledScope: sample.scope,
            sampledAssetIDs: sample.ids
        )
        XCTAssertTrue(didWrite)
        XCTAssertEqual(worker._testMonthFileSize(for: key), 12345)
    }

    func testFilteredDeviceIndexCannotSeedTheFullLibraryBrowser() async {
        let worker = makeWorker()
        for filter in [PhotoLibraryMediaFilter.photos, .videos] {
            worker._testSeed(
                scope: .device(filter),
                payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "filtered")]])
            )
            let scopedSeed = await worker.browserLocalSeed(expectedScope: .device(filter))
            let fullSeed = await worker.browserLocalSeed(expectedScope: .device(.all))
            XCTAssertNil(scopedSeed)
            XCTAssertNil(fullSeed)
        }
    }

    func testMediaFilterChangeRejectsOldAssetIDsAndFileSizeWrites() async {
        let worker = makeWorker()
        let month = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.photos),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "photo", year: 2024, month: 5)]])
        )
        let sample = await worker.sampleFileSizeScan(for: month)
        worker._testSeed(scope: .device(.videos), payload: TestFixtures.initialPayload([[]]))
        XCTAssertTrue(worker.localAssetIDs(for: month, expectedScope: .device(.photos)).isEmpty)
        let didWrite = await worker.writeFileSizeIfIndexStable(
            12345, for: month, sampledScope: sample.scope, sampledAssetIDs: sample.ids
        )
        XCTAssertFalse(didWrite)
        XCTAssertNil(worker._testMonthFileSize(for: month))
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

    func testDisconnectedSnapshotForcesFullBootstrapOnNextConnection() async {
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
            state: TestFixtures.remoteSnapshotState(revision: 7, isFullSnapshot: true, deltas: [delta]),
            hasActiveConnection: false
        )

        XCTAssertNil(
            worker.remoteSnapshotRevisionForQuery(hasActiveConnection: true),
            "a cancelled connect can feed a disconnected partial snapshot; the next successful connect must request a full remote bootstrap"
        )

        _ = await worker.syncRemoteSnapshot(
            state: TestFixtures.remoteSnapshotState(revision: 8, isFullSnapshot: true, deltas: [delta]),
            hasActiveConnection: true
        )

        XCTAssertEqual(
            worker.remoteSnapshotRevisionForQuery(hasActiveConnection: true),
            8,
            "after a full connected bootstrap, later connected refreshes may resume incremental revision queries"
        )
    }

    func testWriteFileSizeIfIndexStable_skipsWhenScopeChangedMidScan() async {
        // Critical Invariant #2: a reload landing between sample and write-back must
        // invalidate the write-back; otherwise pre-reload totals would land on a
        // freshly-wiped `monthFileSizes` and flash stale UI.
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a", year: 2024, month: 5)]])
        )

        let sample = await worker.sampleFileSizeScan(for: key)
        XCTAssertEqual(sample.scope, .device(.all))
        XCTAssertEqual(sample.ids, ["a"])

        worker._testForceLoadedScope(.albums(["x"]))

        let didWrite = await worker.writeFileSizeIfIndexStable(
            999_999,
            for: key,
            sampledScope: sample.scope,
            sampledAssetIDs: sample.ids
        )
        XCTAssertFalse(didWrite, "write-back must be skipped when scope changed mid-scan")
        XCTAssertNil(worker._testMonthFileSize(for: key), "in-memory size must remain unset")
    }

    func testWriteFileSizeIfIndexStable_writesAfterReloadWhenMonthMembershipUnchanged() async {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[
                TestFixtures.snapshot(id: "a", year: 2024, month: 5),
                TestFixtures.snapshot(id: "b", year: 2024, month: 6)
            ]])
        )

        let sample = await worker.sampleFileSizeScan(for: key)
        XCTAssertEqual(sample.scope, .device(.all))
        XCTAssertEqual(sample.ids, ["a"])

        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[
                TestFixtures.snapshot(id: "a", year: 2024, month: 5),
                TestFixtures.snapshot(id: "c", year: 2024, month: 6)
            ]])
        )

        let didWrite = await worker.writeFileSizeIfIndexStable(
            999_999,
            for: key,
            sampledScope: sample.scope,
            sampledAssetIDs: sample.ids
        )
        XCTAssertTrue(didWrite, "unrelated month changes must not drop a stable month write-back")
        XCTAssertEqual(worker._testMonthFileSize(for: key), 999_999)
    }

    func testWriteFileSizeIfIndexStable_skipsWhenMonthMembershipChangedMidScan() async {
        let worker = makeWorker()
        let key = LibraryMonthKey(year: 2024, month: 5)
        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "a", year: 2024, month: 5)]])
        )

        let sample = await worker.sampleFileSizeScan(for: key)
        XCTAssertEqual(sample.scope, .device(.all))
        XCTAssertEqual(sample.ids, ["a"])

        worker._testSeed(
            scope: .device(.all),
            payload: TestFixtures.initialPayload([[TestFixtures.snapshot(id: "b", year: 2024, month: 5)]])
        )

        let didWrite = await worker.writeFileSizeIfIndexStable(
            999_999,
            for: key,
            sampledScope: sample.scope,
            sampledAssetIDs: sample.ids
        )
        XCTAssertFalse(didWrite, "write-back must be skipped when this month's membership changed mid-scan")
        XCTAssertNil(worker._testMonthFileSize(for: key), "in-memory size must remain unset")
    }
}
