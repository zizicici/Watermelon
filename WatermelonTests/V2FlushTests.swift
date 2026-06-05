import XCTest
@testable import Watermelon

final class V2FlushTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-test-uuid"
    private let year = 2026
    private let month = 1
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
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

    func testFlushV2WritesCommitAndSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let store = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xBB),
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/photo.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])

        let delta = try await store.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [asset.assetFingerprint])
        XCTAssertTrue(delta.committedTombstoneFingerprints.isEmpty)

        // Commit file should exist at expected path.
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 1
        )
        let commitExists = await client.hasFile(commitPath)
        XCTAssertTrue(commitExists, "commit file must be written at seq 1")

        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 1)
        XCTAssertEqual(counts.snapshots, 0)

        // Cross-validate via materialize so we pin actual bytes, not just paths.
        // The existence + delta checks above would pass even if commit body was
        // empty, used the wrong fingerprint, or pointed at a wrong physical path.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])

        let materializedAsset = try XCTUnwrap(monthState.assets[asset.assetFingerprint],
            "asset must round-trip through the durable commit")
        XCTAssertEqual(materializedAsset.totalFileSizeBytes, asset.totalFileSizeBytes)
        XCTAssertEqual(materializedAsset.creationDateMs, asset.creationDateMs)
        XCTAssertEqual(materializedAsset.backedUpAtMs, asset.backedUpAtMs)
        XCTAssertEqual(materializedAsset.resourceCount, asset.resourceCount)

        let materializedResource = try XCTUnwrap(monthState.resources[RemotePhysicalPathKey("2026/01/photo.jpg")],
            "resource must be at the physical path we wrote")
        XCTAssertEqual(materializedResource.contentHash, hash,
            "content hash must round-trip exactly — a swap or truncation here means the commit body is wrong")
        XCTAssertEqual(materializedResource.fileSize, resource.fileSize)
        XCTAssertEqual(materializedResource.resourceType, ResourceTypeCode.photo)

        let arKey = AssetResourceKey(
            assetFingerprint: asset.assetFingerprint,
            role: ResourceTypeCode.photo,
            slot: 0
        )
        let materializedLink = try XCTUnwrap(monthState.assetResources[arKey],
            "asset → resource link must round-trip — a missing link here means commit's resources[] was empty")
        XCTAssertEqual(materializedLink.resourceHash, hash)
        XCTAssertEqual(materializedLink.logicalName, "photo.jpg")
    }

    func testCommitPendingAssetWritesCommitWithoutSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let rows = makeSingleAssetRows(assetByte: 0xA1, hashByte: 0xA2, name: "commit-only.jpg")
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])

        let delta = try await store.commitPendingAssetToRemote(ignoreCancellation: false)

        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [rows.asset.assetFingerprint])
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 1)
        XCTAssertEqual(counts.snapshots, 0)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertNotNil(output.state.months[monthKey]?.assets[rows.asset.assetFingerprint])
    }

    func testCommitPendingAssetNoOpWhenNoPendingOps() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let delta = try await store.commitPendingAssetToRemote(ignoreCancellation: false)

        XCTAssertFalse(delta.didFlush)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 0)
        XCTAssertEqual(counts.snapshots, 0)
        let lamportValue = await v2.lamport.value()
        let seqValue = await v2.seqAllocator.value()
        XCTAssertEqual(lamportValue, 0)
        XCTAssertEqual(seqValue, 0)
    }

    func testFlushIgnoreCancellationBoundaryDecisions() {
        // U01: pause-final ignores cancellation for both V1 and V2 — V2 batch pending may hold
        // up to BackupV2Constants.batchFlushInterval row-writes that would orphan their uploaded
        // resources if cancellation interrupted the final flush.
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: true,
            taskIsCancelled: false,
            hasV2Services: true
        ))
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: true,
            taskIsCancelled: false,
            hasV2Services: false
        ))
        // A pause/stop observed first at the EOM boundary leaves paused == false while the task is
        // already cancelled. The final flush must still commit the in-memory batch (its resources
        // are uploaded), not drop it — otherwise the same pause yields commit-or-drop on timing alone.
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: false,
            taskIsCancelled: true,
            hasV2Services: true
        ))
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: true,
            taskIsCancelled: true,
            hasV2Services: true
        ))
        // Normal completion (no pause, task live) still respects cancellation — there is no
        // cancellation to ignore, and the flush should observe a genuinely live task.
        XCTAssertFalse(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: false,
            taskIsCancelled: false,
            hasV2Services: false
        ))
        XCTAssertFalse(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(
            paused: false,
            taskIsCancelled: false,
            hasV2Services: true
        ))
        XCTAssertFalse(BackgroundBackupRunner.backgroundIntervalFlushIgnoresCancellation())
        XCTAssertTrue(BackgroundBackupRunner.backgroundFinalFlushIgnoresCancellation())
    }

    func testBackgroundEndOfMonthCancelledIsRealFailureOnlyWhenTaskLive() {
        // A `.cancelled`-classified EOM flush is silent only on genuine task teardown. With
        // ignoreCancellation, a transport NSURLErrorCancelled (CommitLogWriter maps it to a bare
        // CancellationError) on a live task is a real flush failure — the month must not be marked
        // complete (and put on cooldown) with pending V2 ops left uncommitted.
        XCTAssertTrue(BackgroundBackupRunner.backgroundEndOfMonthCancelledIsRealFailure(taskIsCancelled: false))
        XCTAssertFalse(BackgroundBackupRunner.backgroundEndOfMonthCancelledIsRealFailure(taskIsCancelled: true))
    }

    func testFlushWritesSnapshotOverPriorPerAssetCommits() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let first = makeSingleAssetRows(assetByte: 0xB1, hashByte: 0xB2, name: "first.jpg")
        _ = try store.upsertResource(first.resource)
        try store.upsertAsset(first.asset, links: [first.link])
        _ = try await store.commitPendingAssetToRemote(ignoreCancellation: false)

        let second = makeSingleAssetRows(assetByte: 0xB3, hashByte: 0xB4, name: "second.jpg")
        _ = try store.upsertResource(second.resource)
        try store.upsertAsset(second.asset, links: [second.link])
        _ = try await store.commitPendingAssetToRemote(ignoreCancellation: false)

        let delta = try await store.flushToRemote(ignoreCancellation: false)

        XCTAssertFalse(delta.didFlush)
        XCTAssertTrue(delta.committedAssetFingerprints.isEmpty)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 2)
        XCTAssertEqual(counts.snapshots, 0)
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[first.asset.assetFingerprint])
        XCTAssertNotNil(monthState.assets[second.asset.assetFingerprint])
        let covered = output.coveredByMonth[monthKey] ?? .empty
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 1))
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 2))
    }

    func testSnapshotRetryCoversCommitsInterleavedAfterFailedAttempt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let first = makeSingleAssetRows(assetByte: 0xD1, hashByte: 0xD2, name: "first.jpg")
        _ = try store.upsertResource(first.resource)
        try store.upsertAsset(first.asset, links: [first.link])
        _ = try await store.commitPendingAssetToRemote(ignoreCancellation: false)

        _ = try await store.flushToRemote(ignoreCancellation: false)

        let second = makeSingleAssetRows(assetByte: 0xD3, hashByte: 0xD4, name: "second.jpg")
        _ = try store.upsertResource(second.resource)
        try store.upsertAsset(second.asset, links: [second.link])
        _ = try await store.commitPendingAssetToRemote(ignoreCancellation: false)
        _ = try await store.flushToRemote(ignoreCancellation: false)

        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 2)
        XCTAssertEqual(counts.snapshots, 0)
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let covered = output.coveredByMonth[monthKey] ?? .empty
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 1))
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 2))
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertEqual(
            Set(monthState.assets.keys),
            [first.asset.assetFingerprint, second.asset.assetFingerprint]
        )
    }

    func testDefensiveFlushPublishesCommittedSnapshotAtCallSite() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let rows = makeSingleAssetRows(assetByte: 0xC1, hashByte: 0xC2, name: "defensive.jpg")
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])
        let remoteIndexService = RemoteIndexSyncService()

        let outcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store,
            month: monthKey,
            remoteIndexService: remoteIndexService,
            ignoreCancellation: false
        )

        guard case .completed(let delta) = outcome else {
            XCTFail("expected .completed outcome, got \(outcome)")
            return
        }
        XCTAssertEqual(delta.committedAssetFingerprints, [rows.asset.assetFingerprint])
        XCTAssertEqual(remoteIndexService.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], [rows.asset.assetFingerprint])
    }

    func testDefensiveFlushPublishesBeforeSnapshotFailurePropagation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let occupiedSnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 2, writerID: writerID, runID: runID
        )
        await client.injectFile(path: occupiedSnapshotPath, data: Data("occupied".utf8))
        let rows = makeSingleAssetRows(assetByte: 0xC3, hashByte: 0xC4, name: "snapshot-fail.jpg")
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])
        let remoteIndexService = RemoteIndexSyncService()

        let outcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store,
            month: monthKey,
            remoteIndexService: remoteIndexService,
            ignoreCancellation: false
        )
        guard case .completed(let delta) = outcome else {
            XCTFail("expected .completed outcome, got \(outcome)")
            return
        }
        XCTAssertEqual(delta.committedAssetFingerprints, [rows.asset.assetFingerprint])

        XCTAssertEqual(remoteIndexService.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], [rows.asset.assetFingerprint])
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertNotNil(output.state.months[monthKey]?.assets[rows.asset.assetFingerprint])

        let retryOutcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store,
            month: monthKey,
            remoteIndexService: remoteIndexService,
            ignoreCancellation: false
        )
        guard case .completed(let retryDelta) = retryOutcome else {
            XCTFail("expected no-op .completed outcome, got \(retryOutcome)")
            return
        }
        XCTAssertFalse(retryDelta.didFlush)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.snapshots, 1, "hot-path flush should not write a retry snapshot")
    }

    func testFlushV2ReturnsTombstoneFingerprintsInDelta() async throws {
        // Tombstone-only flush: applyDeletions adds to pending tombstones; flush
        // reports them in committedTombstoneFingerprints.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Materialize an existing asset so reconcile can tombstone it
        let fp = TestFixtures.assetFingerprint(0xCC)
        let hash = TestFixtures.fingerprint(0xDD)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: fp,
            resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0,
            logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link], replacingSubsetFingerprints: [])
        // Initial flush — asset committed
        _ = try await store.flushToRemote(ignoreCancellation: false)

        let newFP = TestFixtures.assetFingerprint(0xEE)
        let newAsset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [fp])

        let delta = try await store.flushToRemote(ignoreCancellation: false)
        XCTAssertTrue(delta.committedTombstoneFingerprints.contains(fp),
                      "tombstone fingerprint must be reported in BackupMonthFlushDelta for defensive publication")
        XCTAssertTrue(delta.committedAssetFingerprints.contains(newFP),
                      "superseding asset must be reported in BackupMonthFlushDelta")

        // Both ops must land in a single commit file (seq=2; seq=1 was the first flush).
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let secondCommitExists = await client.hasFile(commitPath)
        XCTAssertTrue(secondCommitExists, "second flush must be at seq 2")

        // Materialize and verify the tombstoned-then-superseded shape lands:
        // newFP present, fp absent, fp recorded as tombstoned with our stamp.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[newFP], "superseding asset must be present after replay")
        XCTAssertNil(monthState.assets[fp], "subset-replaced asset must not be present")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp),
                      "tombstone must survive snapshot baseline so LWW gate against stale adds keeps working")
        XCTAssertNotNil(monthState.deletedAssetStamps[fp],
                        "tombstone stamp must persist for cross-writer LWW comparison")
    }

    func testTombstone_coveredBySnapshot_carriesStampForLWW() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let fp = TestFixtures.assetFingerprint(0xCC)
        let hash = TestFixtures.fingerprint(0xDD)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        // Tombstone via subset replacement, then flush again — the tombstone now
        // lives in a commit AND its effect is in the snapshot baseline.
        let supersedingFP = TestFixtures.assetFingerprint(0xEE)
        let superseding = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: supersedingFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        try store.upsertAsset(superseding, links: [link], replacingSubsetFingerprints: [fp])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])

        XCTAssertNil(monthState.assets[fp])
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
        let stamp = try XCTUnwrap(monthState.deletedAssetStamps[fp])
        XCTAssertEqual(stamp.writerID, writerID)
        XCTAssertGreaterThan(stamp.clock, 0)
    }

    func testFlushV2_subsetReplacementTombstone_carriesObservedBasis() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldFP = TestFixtures.assetFingerprint(0xC1)
        let newFP = TestFixtures.assetFingerprint(0xC2)
        let hash = TestFixtures.fingerprint(0xD1)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let oldAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: oldFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: oldFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(oldAsset, links: [link])
        _ = try await store.flushToRemote()

        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [oldFP])
        _ = try await store.flushToRemote()

        // Read the second commit (seq=2) and assert tombstone has observedBasis.
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try await client.download(remotePath: commitPath, localURL: downloadURL)
        let parsed = try CommitLogReader.parse(localURL: downloadURL)
        let tombstone = parsed.ops.first { op in
            if case .tombstoneAsset = op.body { return true }
            return false
        }
        guard let op = tombstone, case let .tombstoneAsset(body) = op.body else {
            XCTFail("expected tombstone op in subset-replacement commit"); return
        }
        XCTAssertNotNil(body.observedBasis, "subset-replacement tombstone must carry observedBasis")
    }

    func testFlushV2_observedBasis_rollsForwardAcrossFlushes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldFP = TestFixtures.assetFingerprint(0xC1)
        let newFP = TestFixtures.assetFingerprint(0xC2)
        let hash = TestFixtures.fingerprint(0xD1)
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let oldAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: oldFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: oldFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )

        _ = try store.upsertResource(resource)
        try store.upsertAsset(oldAsset, links: [link])
        _ = try await store.flushToRemote()
        let lamportAfterFlush1 = await v2.lamport.value()
        XCTAssertGreaterThan(lamportAfterFlush1, 0)

        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [oldFP])
        _ = try await store.flushToRemote()

        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try await client.download(remotePath: commitPath, localURL: downloadURL)
        let parsed = try CommitLogReader.parse(localURL: downloadURL)
        let tombstone = parsed.ops.first { op in
            if case .tombstoneAsset = op.body { return true }
            return false
        }
        guard let op = tombstone, case let .tombstoneAsset(body) = op.body else {
            XCTFail("expected tombstone with basis"); return
        }
        let basis = body.observedBasis
        XCTAssertGreaterThanOrEqual(basis.lamportWatermark, lamportAfterFlush1,
                                     "basis must reflect lamport AFTER flush 1, not load-time only")
        XCTAssertGreaterThanOrEqual(basis.perWriterMaxSeq[writerID] ?? 0, 1,
                                    "basis perWriterMaxSeq must include flush 1's seq")
    }

    func testUpsertAsset_rejectsLinkToPhysicallyMissingHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xCC)
        let assetFP = TestFixtures.assetFingerprint(0xDD)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(resource)
        try store1.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Physical file disappears; reload sees the resource row but flags the hash missing.
        try await client.delete(path: "\(basePath)/\(physicalPath)")
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let newAssetFP = TestFixtures.assetFingerprint(0xEE)
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newAssetFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100
        )
        let newLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: newAssetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        XCTAssertThrowsError(try store2.upsertAsset(newAsset, links: [newLink])) { err in
            let nsError = err as NSError
            XCTAssertEqual(nsError.domain, "V2MonthSession")
            XCTAssertEqual(nsError.code, -11, "must throw fail-fast on physicallyMissing link")
        }
    }

    func testFlushV2SnapshotEmitsOnlyCommittedPathsPerHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let pathA = "2026/01/photo.jpg"
        let pathB = "2026/01/photo~widB.jpg"

        let resA = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: pathA, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let resB = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: pathB, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(resA)
        _ = try store.upsertResource(resB)

        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xBB),
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0,
            logicalName: "photo.jpg"
        )
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        // pathA is in the addAsset commit body (lex-min present path for hash).
        XCTAssertNotNil(monthState.resources[RemotePhysicalPathKey(pathA)], "committed path must survive")
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(pathA)]?.contentHash, hash)
        // pathB was upserted but never linked through any committed asset → orphan.
        XCTAssertNil(monthState.resources[RemotePhysicalPathKey(pathB)],
                     "orphan path (no commit body references it) must not be in snapshot — would break state == fold(commits)")
    }

    func testReconcileDropsResourcesWhosePhysicalFileIsMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: stage a real upload + flush so the snapshot has a resource row
        // with a corresponding file on remote.
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xAA)
        let assetFP = TestFixtures.assetFingerprint(0xBB)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        // Stage the physical bytes too so the directory listing sees the file.
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store.flushToRemote()

        // Sanity: a fresh session would see this resource as live.
        let baseline = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertNotNil(baseline.findResourceByHash(hash), "baseline session must see the resource")

        // Round 2: simulate someone deleting the physical file out-of-band (manual rm,
        // peer cleanup, anything). Snapshot still has the row; remote dir doesn't.
        try await client.delete(path: "\(basePath)/\(physicalPath)")

        let reloaded = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertNil(
            reloaded.findResourceByHash(hash),
            "missing physical file must drop the resource so re-upload isn't skipped as hash_exists"
        )
        XCTAssertTrue(
            reloaded.isAssetIncomplete(assetFP),
            "asset becomes incomplete after its only resource is filtered out → triggers full re-processing"
        )
    }

    func testSnapshotPreservesAssetsEvenWhenPhysicalFilesMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: set up an asset whose only resource gets uploaded, flush to bake into snapshot.
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xCC)
        let assetFP = TestFixtures.assetFingerprint(0xDD)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store.flushToRemote()

        // Round 2: physical file gets removed (manual rm or peer cleanup).
        try await client.delete(path: "\(basePath)/\(physicalPath)")

        // Round 3: a fresh session sees the orphan via materialize+filter, then writes
        // a new snapshot for some unrelated reason (different asset upserted + flushed).
        let session2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertTrue(session2.isAssetIncomplete(assetFP),
                      "asset must be flagged incomplete after its only resource is missing")

        // Trigger a flush by upserting an unrelated asset+resource.
        let otherHash = TestFixtures.fingerprint(0x11)
        let otherFP = TestFixtures.assetFingerprint(0x22)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: otherPath, contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try session2.upsertResource(otherResource)
        try session2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await session2.flushToRemote()

        // Re-materialize and verify: the asset row AND its link MUST appear in the
        // new snapshot — that's the covered-range invariant. Verify-month is the
        // path that surfaces it as `partiallyMissing`/`fullyMissing` to the user;
        // snapshot writer doesn't take action on its own.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let arKey = AssetResourceKey(
            assetFingerprint: assetFP, role: ResourceTypeCode.photo, slot: 0
        )
        XCTAssertNotNil(
            monthState.assetResources[arKey],
            "Step 5 contract: snapshot is faithful to commit log; orphan link survives"
        )
        XCTAssertNotNil(
            monthState.assets[assetFP],
            "Step 5 contract: asset row stays even when its resources are physically missing"
        )
        // Session view: still flags it incomplete so consumers don't act on it.
        let session3 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertTrue(session3.isAssetIncomplete(assetFP),
                      "session view filters via physicallyMissingHashes — incomplete remains observable")
    }

    func testFlushV2_orphanUpsertResourceWithoutUpsertAsset_isFilteredFromSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let orphanHash = TestFixtures.fingerprint(0xA1)
        let orphanPath = "2026/01/orphan.jpg"
        let orphanResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: orphanPath,
            contentHash: orphanHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        // Simulates "first resource of a multi-resource asset uploaded, then second
        // upload permanently failed → AssetProcessor.process returns .failed without
        // upsertAsset". The orphan resource is left dangling in indexes.
        _ = try store.upsertResource(orphanResource)

        // A legitimate asset with its resource succeeds and gets committed.
        let legitHash = TestFixtures.fingerprint(0xA2)
        let legitPath = "2026/01/legit.jpg"
        let legitFP = TestFixtures.assetFingerprint(0xB2)
        let legitResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: legitPath,
            contentHash: legitHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let legitAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: legitFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 50
        )
        let legitLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: legitFP, resourceHash: legitHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "legit.jpg"
        )
        _ = try store.upsertResource(legitResource)
        try store.upsertAsset(legitAsset, links: [legitLink])
        _ = try await store.flushToRemote()

        // Materialize: the legit resource is in the snapshot/commit body; the orphan
        // path appears in no commit body anywhere, so it must not show up either.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.resources[RemotePhysicalPathKey(legitPath)],
                        "committed resource must survive the materialize round-trip")
        XCTAssertNil(monthState.resources[RemotePhysicalPathKey(orphanPath)],
                     "orphan resource (upserted but never linked to a committed asset) must not be in snapshot — snapshot ≠ fold(commits)")
    }

    func testFlushV2_committedPathOverwrittenByUpsert_snapshotRetainsCommittedHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: commit asset A at (path, oldHash).
        let path = "2026/01/photo.jpg"
        let oldHash = TestFixtures.fingerprint(0x10)
        let assetAFP = TestFixtures.assetFingerprint(0x20)
        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let oldResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: oldHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let assetA = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetAFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let linkA = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetAFP, resourceHash: oldHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(oldResource)
        try store1.upsertAsset(assetA, links: [linkA])
        await client.injectFile(path: "\(basePath)/\(path)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Round 2: new session loads the snapshot. Simulate physical file deletion
        // followed by an upsertResource that repurposes the same path with a
        // different hash — but the corresponding upsertAsset never lands (asset
        // commit fails). Then commit a legitimate, unrelated asset to force a
        // snapshot flush.
        try await client.delete(path: "\(basePath)/\(path)")
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let newHash = TestFixtures.fingerprint(0x11)
        let newResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: newHash, fileSize: 200,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store2.upsertResource(newResource)

        let otherHash = TestFixtures.fingerprint(0x30)
        let otherFP = TestFixtures.assetFingerprint(0x40)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: otherPath,
            contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try store2.upsertResource(otherResource)
        try store2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await store2.flushToRemote()

        // Materialize and verify: the path retains oldHash from the original commit,
        // NOT newHash from the orphan upsertResource.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let retainedRow = try XCTUnwrap(monthState.resources[RemotePhysicalPathKey(path)],
            "originally committed path must remain in snapshot — covered range includes its addAsset commit")
        XCTAssertEqual(retainedRow.contentHash, oldHash,
            "snapshot resource row at path must reflect the COMMITTED hash, not the in-session upsert overwrite")
        XCTAssertEqual(retainedRow.fileSize, 100,
            "the full row (size, type, etc) must match the committed row, not the overwritten one")
    }

    func testV2MonthIndexes_seed_isFaithfulToMaterializedResources() throws {
        let tombstonedHash = TestFixtures.fingerprint(0x77)
        let livingHash = TestFixtures.fingerprint(0x88)
        let livingFP = TestFixtures.assetFingerprint(0xC1)
        let tombstonedFP = TestFixtures.assetFingerprint(0xC2)
        let tombstonedPath = "2026/01/tombstoned.jpg"
        let livingPath = "2026/01/living.jpg"

        // Mirror a post-tombstone fold(covered):
        //   resources = {tombstonedPath (orphan after tombstone), livingPath}
        //   assets = {livingFP only}; links reference only livingHash
        //   deletedAssetStamps contains tombstonedFP
        var materialized = RepoMonthState.empty
        materialized.assets[livingFP] = SnapshotAssetRow(
            assetFingerprint: livingFP,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 50,
            stamp: OpStamp(writerID: writerID, seq: 1, clock: 1)
        )
        materialized.resources[RemotePhysicalPathKey(livingPath)] = SnapshotResourceRow(
            physicalRemotePath: livingPath,
            contentHash: livingHash,
            fileSize: 50,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            crypto: nil
        )
        materialized.resources[RemotePhysicalPathKey(tombstonedPath)] = SnapshotResourceRow(
            physicalRemotePath: tombstonedPath,
            contentHash: tombstonedHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0,
            crypto: nil
        )
        let linkKey = AssetResourceKey(assetFingerprint: livingFP, role: ResourceTypeCode.photo, slot: 0)
        materialized.assetResources[linkKey] = SnapshotAssetResourceRow(
            assetFingerprint: livingFP,
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: livingHash,
            logicalName: "living.jpg"
        )
        materialized.deletedAssetStamps[tombstonedFP] = OpStamp(writerID: writerID, seq: 2, clock: 2)

        let indexes = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        let state = indexes.currentMaterializedState()
        XCTAssertNotNil(state.resources[RemotePhysicalPathKey(livingPath)],
                        "linked resource row must survive seed")
        XCTAssertNotNil(state.resources[RemotePhysicalPathKey(tombstonedPath)],
                        "post-tombstone orphan row must survive seed — RepoMaterializer leaves it in fold(covered), so dropping it here would break state == fold(covered)")
        XCTAssertEqual(state.resources[RemotePhysicalPathKey(tombstonedPath)]?.contentHash, tombstonedHash,
                        "the orphan row's content hash must round-trip exactly — drift would corrupt the snapshot baseline")
    }

    func testV2MonthIndexes_exactMatchBackend_nfdListingDoesNotMarkNfcResourcePresent() throws {
        // S3 (.caseSensitive, byte-exact): committed resource is at the NFC leaf, but only a
        // distinct same-size NFD-spelled object is listed. The exact committed key is absent, so
        // the resource must stay missing — collapsing NFC/NFD would bind dedup to absent bytes.
        let hash = TestFixtures.fingerprint(0x5A)
        let nfcPath = "2026/01/caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"

        var materialized = RepoMonthState.empty
        materialized.resources[RemotePhysicalPathKey(nfcPath)] = SnapshotResourceRow(
            physicalRemotePath: nfcPath,
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0,
            crypto: nil
        )

        let indexes = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: [nfdLeaf: MonthManifestStore.RemoteFileMetadata(size: 100)],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )

        XCTAssertNil(indexes.findResourceByHash(hash),
            "exact-match backend must not treat a same-size NFD object as the committed NFC key")
    }

    func testListedSizesByPresenceKey_exactMatchBackend_retainsBothNfcAndNfdTwins() throws {
        // Two genuinely-distinct objects whose leaves are NFC/NFD twins (a real S3/SFTP state).
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        XCTAssertNotEqual(Data(nfcLeaf.utf8), Data(nfdLeaf.utf8), "test premise: NFC and NFD bytes differ")
        let entries = [
            RemoteStorageEntry(path: "2026/01/\(nfcLeaf)", name: nfcLeaf, isDirectory: false, size: 111, creationDate: nil, modificationDate: nil),
            RemoteStorageEntry(path: "2026/01/\(nfdLeaf)", name: nfdLeaf, isDirectory: false, size: 222, creationDate: nil, modificationDate: nil),
        ]

        // The [String:…] listing folds the twins to one entry — the upstream collapse.
        XCTAssertEqual(
            MonthManifestStore.dedupedRemoteFilesByName(entries: entries, year: year, month: month).count, 1,
            "premise: [String:…] listing folds NFC/NFD twins to one entry")

        // Byte-exact keying keeps both twins, each with its own size.
        let exact = MonthManifestStore.listedSizesByPresenceKey(entries: entries, nameCase: .caseSensitive)
        XCTAssertEqual(exact.count, 2, "exact-name backend must keep byte-distinct NFC/NFD twins separate")
        XCTAssertEqual(exact[BackendNameCaseSensitivity.caseSensitive.presenceKey(for: nfcLeaf)], [111])
        XCTAssertEqual(exact[BackendNameCaseSensitivity.caseSensitive.presenceKey(for: nfdLeaf)], [222])

        // Case-insensitive backends genuinely fold NFC/NFD — capability truth, not a bug.
        XCTAssertEqual(
            MonthManifestStore.listedSizesByPresenceKey(entries: entries, nameCase: .caseInsensitive).count, 1,
            "case-insensitive backends fold canonically-equivalent leaves")
    }

    func testV2MonthIndexes_exactMatchBackend_bothNfcAndNfdTwinsListed_bothPresent() throws {
        // S3/SFTP (.caseSensitive): two distinct-content resources committed at NFC/NFD twin leaves,
        // both physically present. Both must compute present; the [String:…] listing collapse used to
        // mark one .missing → authoritative false absence + redundant repair upload.
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        let nfcPath = "2026/01/\(nfcLeaf)"
        let nfdPath = "2026/01/\(nfdLeaf)"
        let hashNFC = TestFixtures.fingerprint(0x6A)
        let hashNFD = TestFixtures.fingerprint(0x6B)

        var materialized = RepoMonthState.empty
        materialized.resources[RemotePhysicalPathKey(nfcPath)] = SnapshotResourceRow(
            physicalRemotePath: nfcPath, contentHash: hashNFC, fileSize: 111,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )
        materialized.resources[RemotePhysicalPathKey(nfdPath)] = SnapshotResourceRow(
            physicalRemotePath: nfdPath, contentHash: hashNFD, fileSize: 222,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )

        let entries = [
            RemoteStorageEntry(path: "/repo/\(nfcPath)", name: nfcLeaf, isDirectory: false, size: 111, creationDate: nil, modificationDate: nil),
            RemoteStorageEntry(path: "/repo/\(nfdPath)", name: nfdLeaf, isDirectory: false, size: 222, creationDate: nil, modificationDate: nil),
        ]
        let byteExact = MonthManifestStore.listedSizesByPresenceKey(entries: entries, nameCase: .caseSensitive)

        let fixed = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: MonthManifestStore.dedupedRemoteFilesByName(entries: entries, year: year, month: month),
            listedSizesByPresenceKey: byteExact,
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        XCTAssertNotNil(fixed.findResourceByHash(hashNFC), "present NFC twin must be findable")
        XCTAssertNotNil(fixed.findResourceByHash(hashNFD), "present NFD twin must be findable")
        XCTAssertTrue(fixed.physicallyMissingHashesSnapshot().isEmpty,
            "both twins are present — neither may be published physically missing")

        // Contrast: deriving presence sizes from the collapsed [String:…] listing (pre-fix path)
        // drops the NFD twin's size, marking a genuinely-present resource missing.
        let collapsed = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: MonthManifestStore.dedupedRemoteFilesByName(entries: entries, year: year, month: month),
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        XCTAssertEqual(collapsed.physicallyMissingHashesSnapshot(), [hashNFD],
            "regression guard: the folded listing falsely reports one present twin missing")
    }

    func testV2MonthIndexes_sameHashNfcAndNfdTwins_oneListed_hashNotMissingAndFindable() throws {
        // Bug-X P07 R07 F2: the SAME content hash committed at two byte-distinct NFC/NFD twin leaves
        // (multi-writer same-content upload); only the NFD twin's object is listed/present. A
        // `Set<String>` pathsByHash folds the twins to one spelling — if it keeps the missing NFC
        // twin, the shared hash is falsely published fully-missing and findResourceByHash returns nil
        // even though the NFD bytes are present. Byte-exact path keys keep both twins.
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        let nfcPath = "2026/01/\(nfcLeaf)"
        let nfdPath = "2026/01/\(nfdLeaf)"
        XCTAssertNotEqual(Data(nfcPath.utf8), Data(nfdPath.utf8), "premise: twin paths are byte-distinct")
        let hash = TestFixtures.fingerprint(0x7C)

        var materialized = RepoMonthState.empty
        materialized.resources[RemotePhysicalPathKey(nfcPath)] = SnapshotResourceRow(
            physicalRemotePath: nfcPath, contentHash: hash, fileSize: 321,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )
        materialized.resources[RemotePhysicalPathKey(nfdPath)] = SnapshotResourceRow(
            physicalRemotePath: nfdPath, contentHash: hash, fileSize: 321,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )

        // Only the NFD twin is physically listed.
        let entries = [
            RemoteStorageEntry(path: "/repo/\(nfdPath)", name: nfdLeaf, isDirectory: false, size: 321, creationDate: nil, modificationDate: nil),
        ]
        let byteExact = MonthManifestStore.listedSizesByPresenceKey(entries: entries, nameCase: .caseSensitive)

        let indexes = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: MonthManifestStore.dedupedRemoteFilesByName(entries: entries, year: year, month: month),
            listedSizesByPresenceKey: byteExact,
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )

        XCTAssertTrue(indexes.physicallyMissingHashesSnapshot().isEmpty,
            "shared hash has a present NFD twin — it must not be published physically missing")
        XCTAssertEqual(indexes.findResourceByHash(hash)?.physicalRemotePath, nfdPath,
            "findResourceByHash must resolve to the present twin, not nil")
    }

    func testFlushV2_postTombstoneOrphanResource_survivesAcrossFlushes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let assetFP = TestFixtures.assetFingerprint(0xA1)
        let hash = TestFixtures.fingerprint(0xB1)
        let physicalPath = "2026/01/photo.jpg"

        // Round 1: commit asset A with one resource, with physical bytes on remote.
        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000, backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(resource)
        try store1.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Round 2: subset-replace asset A with asset B (different fp, same resource).
        // This emits a tombstone for assetFP whose resource row remains in fold(covered).
        let supersedingFP = TestFixtures.assetFingerprint(0xA2)
        let superseding = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: supersedingFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100
        )
        try store1.upsertAsset(superseding, links: [link], replacingSubsetFingerprints: [assetFP])
        _ = try await store1.flushToRemote()

        // Reload + flush an unrelated asset; the resulting snapshot must still emit
        // the resource row for `physicalPath` because fold(covered) includes both
        // the addAsset(A) and the tombstone(A) commits, and the materializer's
        // tombstone handling preserves `state.resources[RemotePhysicalPathKey(physicalPath)]`.
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let otherFP = TestFixtures.assetFingerprint(0xC0)
        let otherHash = TestFixtures.fingerprint(0xD0)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: otherPath,
            contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 3, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try store2.upsertResource(otherResource)
        try store2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await store2.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.resources[RemotePhysicalPathKey(physicalPath)],
                        "post-tombstone orphan resource row must survive reload+flush — fold(covered) preserves it")
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(physicalPath)]?.contentHash, hash)
    }

    func testFlushV2_committedRowDates_matchAssetBodyNotResource() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let path = "2026/01/photo.jpg"
        // Resource has DIFFERENT dates than the asset, to surface any projection bug.
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: hash, fileSize: 100, resourceType: ResourceTypeCode.photo,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_005_000
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xBB),
            creationDateMs: 1_700_000_100_000,
            backedUpAtMs: 1_700_000_999_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        // Materialize: the resource row must carry the asset body's dates, matching
        // what RepoMaterializer would derive on replay.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let row = try XCTUnwrap(monthState.resources[RemotePhysicalPathKey(path)])
        XCTAssertEqual(row.creationDateMs, asset.creationDateMs,
                       "resource row's creationDateMs must come from asset body, not the live resource — replay derives it from body")
        XCTAssertEqual(row.backedUpAtMs, asset.backedUpAtMs,
                       "resource row's backedUpAtMs must come from asset body, not the live resource — replay derives it from body")
    }

    func testFlushV2_resourceRowStampPropagatesThroughProductionFlushPath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xCC)
        let path = "2026/01/stamped.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: hash, fileSize: 200,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xDD),
            creationDateMs: 1_700_000_100_000,
            backedUpAtMs: 1_700_000_200_000,
            resourceCount: 1, totalFileSizeBytes: 200
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "stamped.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let resourceRow = try XCTUnwrap(monthState.resources[RemotePhysicalPathKey(path)],
                                         "production flush must publish a resource row at the path")
        let resourceStamp = try XCTUnwrap(resourceRow.stamp,
                                           "production flush must stamp resource rows for path-level LWW")
        let assetRow = try XCTUnwrap(monthState.assets[asset.assetFingerprint])
        let assetStamp = try XCTUnwrap(assetRow.stamp,
                                        "production flush must stamp asset rows")
        XCTAssertEqual(resourceStamp.writerID, writerID,
                       "stamp.writerID must be the flusher's writerID")
        XCTAssertEqual(resourceStamp.writerID, assetStamp.writerID,
                       "resource and asset stamps share the producing op's writerID")
        XCTAssertEqual(resourceStamp.seq, assetStamp.seq,
                       "resource and asset stamps share the producing op's allocator seq")
        XCTAssertEqual(resourceStamp.clock, assetStamp.clock,
                       "resource stamp clock must match the producing addAsset op's clock")
        XCTAssertGreaterThan(resourceStamp.clock, 0)
        XCTAssertGreaterThan(resourceStamp.seq, 0)
    }

    func testUpsertResource_repurposingPath_dropsOldHashMapping() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldHash = TestFixtures.fingerprint(0xAA)
        let newHash = TestFixtures.fingerprint(0xBB)
        let path = "2026/01/photo.jpg"

        let oldResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: oldHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let newResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: newHash, fileSize: 200,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )

        _ = try store.upsertResource(oldResource)
        XCTAssertNotNil(store.findResourceByHash(oldHash), "baseline: old hash present after first upsert")

        _ = try store.upsertResource(newResource)
        XCTAssertNil(
            store.findResourceByHash(oldHash),
            "old hash must be unmapped after path is repurposed — otherwise lookup would serve new content under the old key"
        )
        XCTAssertEqual(store.findResourceByHash(newHash)?.contentHash, newHash)
    }

    func testFlushV2_retryOnAlreadyExists_reTicksLamportClockForFreshOrdering() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let seq1Path = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 1
        )
        let peerHeader = CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: repoID,
            writerID: writerID,
            seq: 1,
            runID: "peer-run",
            scope: CommitHeader.monthScope(monthKey),
            clockMin: 1,
            clockMax: 1,
            bodyKind: CommitHeader.bodyKindPlain
        )
        let peerOp = CommitOp(opSeq: 0, clock: 1, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: TestFixtures.assetFingerprint(0xFC),
            reason: .manifestOrphan
        )))
        let peerBytes = try encodeCommit(header: peerHeader, ops: [peerOp])
        await client.injectFile(path: seq1Path, data: peerBytes)
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xFE),
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let hash = TestFixtures.fingerprint(0xFD)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/retry-photo.jpg",
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "retry-photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])

        let delta = try await store.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [asset.assetFingerprint])

        let seq2Path = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let seq1Exists = await client.hasFile(seq1Path)
        XCTAssertTrue(seq1Exists)
        let filesAfterRetry = await client.snapshotFiles()
        XCTAssertEqual(filesAfterRetry[seq1Path], peerBytes)
        let seq2Exists = await client.hasFile(seq2Path)
        XCTAssertTrue(seq2Exists, "retry must succeed at seq=2 after seq=1 collision")

        // Parse the successful commit's header to verify clock advanced past the
        // first attempt's tick. Without re-tick, clockMin would stay at 1 even though
        // seq advanced to 2; with re-tick, clockMin is 2 (or higher).
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("retry-commit-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        try await client.download(remotePath: seq2Path, localURL: tempURL)
        let parsed = try CommitLogReader.parse(localURL: tempURL)
        XCTAssertGreaterThanOrEqual(parsed.header.clockMin, 2,
                                    "retry must re-tick Lamport so clockMin advances past the failed-attempt tick of 1")
    }

    func testFlushV2_snapshotRetryUsesFreshLamportFilename() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let firstSnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: monthKey,
            lamport: 2,
            writerID: writerID,
            runID: runID
        )
        await client.injectFile(path: firstSnapshotPath, data: Data("occupied snapshot".utf8))

        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.assetFingerprint(0xFA),
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let hash = TestFixtures.fingerprint(0xFB)
        let resource = RemoteManifestResource(
            year: year,
            month: month,
            physicalRemotePath: "2026/01/snapshot-retry.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "snapshot-retry.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])

        let delta = try await store.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [asset.assetFingerprint])
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 1)
        XCTAssertEqual(counts.snapshots, 1)

        let retrySnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: monthKey,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let retrySnapshotExists = await client.hasFile(retrySnapshotPath)
        XCTAssertFalse(retrySnapshotExists)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertNotNil(output.state.months[monthKey]?.assets[asset.assetFingerprint])
    }


    private func encodeCommit(header: CommitHeader, ops: [CommitOp]) throws -> Data {
        var integrity = IntegrityAccumulator()
        var lines: [String] = []
        let headerLine = try CommitOpMapper.encodeHeaderLine(header)
        integrity.absorbLine(headerLine)
        lines.append(headerLine)
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            integrity.absorbLine(line)
            lines.append(line)
        }
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: integrity.finalize(), rowCount: integrity.rowCount)
        lines.append(endLine)
        return Data((lines.joined(separator: "\n") + "\n").utf8)
    }

    private func makeV2Services(client: InMemoryRemoteStorageClient) async throws -> BackupV2RuntimeServices {
        let profileID = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
        )
    }

    private func makeSingleAssetRows(
        assetByte: UInt8,
        hashByte: UInt8,
        name: String
    ) -> (
        asset: RemoteManifestAsset,
        resource: RemoteManifestResource,
        link: RemoteAssetResourceLink
    ) {
        let hash = TestFixtures.fingerprint(hashByte)
        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let physicalPath = "2026/01/\(name)"
        let asset = RemoteManifestAsset(
            year: year,
            month: month,
            assetFingerprint: assetFingerprint,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year,
            month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: name
        )
        return (asset, resource, link)
    }

    private func strictSubsetData(_ value: Int) -> Data {
        var bytes = [UInt8](repeating: 0, count: 32)
        var remaining = UInt64(value)
        for index in 0 ..< 8 {
            bytes[31 - index] = UInt8(remaining & 0xff)
            remaining >>= 8
        }
        return Data(bytes)
    }

    private func strictSubsetFingerprint(_ value: Int) -> AssetFingerprint {
        AssetFingerprint(decoding: strictSubsetData(value))!
    }

    private func emptyV2Indexes() -> V2MonthIndexes {
        V2MonthIndexes(
            year: year,
            month: month,
            materializedState: .empty,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
    }

    @discardableResult
    private func addV2IndexedResource(
        _ indexes: V2MonthIndexes,
        hash: Data,
        name: String,
        resourceType: Int = ResourceTypeCode.photo
    ) throws -> RemoteManifestResource {
        let resource = RemoteManifestResource(
            year: year,
            month: month,
            physicalRemotePath: "2026/01/\(name)",
            contentHash: hash,
            fileSize: 1,
            resourceType: resourceType,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        return try indexes.upsertResource(resource)
    }

    private func strictSubsetAsset(
        fingerprint: AssetFingerprint,
        resourceCount: Int
    ) -> RemoteManifestAsset {
        RemoteManifestAsset(
            year: year,
            month: month,
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: resourceCount,
            totalFileSizeBytes: Int64(resourceCount)
        )
    }

    private func strictSubsetLink(
        fingerprint: AssetFingerprint,
        hash: Data,
        role: Int = ResourceTypeCode.photo,
        slot: Int = 0,
        name: String = "photo.jpg"
    ) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: fingerprint,
            resourceHash: hash,
            role: role,
            slot: slot,
            logicalName: name
        )
    }

    private func strictSubsetKey(
        hash: Data,
        role: Int = ResourceTypeCode.photo,
        slot: Int = 0
    ) -> AssetResourceLinkKey {
        AssetResourceLinkKey(role: role, slot: slot, hash: hash)
    }

    private func largeStrictSubsetState(
        unrelatedCount: Int,
        includePartial: Bool
    ) -> (state: RepoMonthState, partialFingerprint: AssetFingerprint, photoHash: Data, videoHash: Data) {
        var state = RepoMonthState.empty
        for index in 0 ..< unrelatedCount {
            let fingerprint = strictSubsetFingerprint(10_000 + index)
            let hash = strictSubsetData(20_000 + index)
            state.assets[fingerprint] = SnapshotAssetRow(
                assetFingerprint: fingerprint,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 1,
                totalFileSizeBytes: 1,
                stamp: TestFixtures.opStamp(seq: UInt64(index + 1), clock: UInt64(index + 1))
            )
            state.assetResources[AssetResourceKey(
                assetFingerprint: fingerprint,
                role: ResourceTypeCode.photo,
                slot: 0
            )] = SnapshotAssetResourceRow(
                assetFingerprint: fingerprint,
                role: ResourceTypeCode.photo,
                slot: 0,
                resourceHash: hash,
                logicalName: "unrelated-\(index).jpg"
            )
        }

        let partialFingerprint = strictSubsetFingerprint(90_001)
        let photoHash = strictSubsetData(90_002)
        let videoHash = strictSubsetData(90_003)
        if includePartial {
            state.assets[partialFingerprint] = SnapshotAssetRow(
                assetFingerprint: partialFingerprint,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 1,
                totalFileSizeBytes: 1,
                stamp: TestFixtures.opStamp(seq: 90_001, clock: 90_001)
            )
            state.assetResources[AssetResourceKey(
                assetFingerprint: partialFingerprint,
                role: ResourceTypeCode.photo,
                slot: 0
            )] = SnapshotAssetResourceRow(
                assetFingerprint: partialFingerprint,
                role: ResourceTypeCode.photo,
                slot: 0,
                resourceHash: photoHash,
                logicalName: "partial.jpg"
            )
        }
        return (state, partialFingerprint, photoHash, videoHash)
    }

    private func repoMetadataCounts(_ client: InMemoryRemoteStorageClient) async -> (commits: Int, snapshots: Int) {
        let files = await client.snapshotFiles().keys
        return (
            files.filter { $0.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/") }.count,
            files.filter { $0.hasPrefix(RepoLayout.snapshotsDirectoryPath(base: basePath) + "/") }.count
        )
    }

    func testLoadOrCreate_createDirectoryURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let monthRel = String(format: "%04d/%02d", year, month)
        let monthAbsPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRel)
        await client.injectCreateDirectoryURLErrorCancelled(for: monthAbsPath)

        do {
            _ = try await V2MonthSession.loadOrCreate(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                v2Services: v2
            )
            XCTFail("expected CancellationError from URL-shaped createDirectory cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testLoadOrCreate_listURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let monthRel = String(format: "%04d/%02d", year, month)
        let monthAbsPath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRel)
        await client.injectListURLErrorCancelled(for: monthAbsPath)

        do {
            _ = try await V2MonthSession.loadOrCreate(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                v2Services: v2
            )
            XCTFail("expected CancellationError from URL-shaped list cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    private func insertProfile() throws -> Int64 {
        try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
    }

    func testClassifierUnwrapsCommitLogWriterIOFailure() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let connectionError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet,
            userInfo: nil
        )
        let wrapped = CommitLogWriter.WriteError.ioFailure(connectionError)
        XCTAssertTrue(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(wrapped),
            "classifier must unwrap CommitLogWriter.WriteError.ioFailure to find the underlying connection error"
        )
    }

    func testClassifierSkipsCommitLogWriterAlreadyExists() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let wrapped = CommitLogWriter.WriteError.alreadyExists
        XCTAssertFalse(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(wrapped),
            "classifier must not treat alreadyExists as a connection error"
        )
    }

    func testClassifierUnwrapsMetadataCreateGateStagingVerificationFailed() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let connectionError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet
        )
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/test.json",
            underlying: connectionError
        )
        XCTAssertTrue(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(gateError),
            "classifier must unwrap MetadataCreateGate.Error.stagingVerificationFailed to find the underlying connection error"
        )
    }

    func testClassifierUnwrapsMetadataCreateGateFinalVerificationFailed() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let connectionError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet
        )
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(
            remotePath: "/test.json",
            underlying: connectionError
        )
        XCTAssertTrue(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(gateError),
            "classifier must unwrap MetadataCreateGate.Error.finalVerificationFailed to find the underlying connection error"
        )
    }

    func testClassifierSkipsMetadataCreateGateNilUnderlying() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/test.json",
            underlying: nil
        )
        XCTAssertFalse(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(gateError),
            "byte-mismatch gate error (nil underlying) must not be classified as connection-unavailable"
        )
    }

    func testClassifierSkipsMetadataCreateGateNonExclusiveFinalization() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let gateError = MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/test.json")
        XCTAssertFalse(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(gateError),
            "nonExclusiveFinalization must not be classified as connection-unavailable"
        )
    }

    func testClassifierUnwrapsMetadataCreateGateThroughCommitLogWriterChain() throws {
        let profileID = try insertProfile()
        let profile = try databaseManager.read { try ServerProfileRecord.fetchOne($0, key: profileID)! }
        let connectionError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet
        )
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/test.json",
            underlying: connectionError
        )
        let wrapped = CommitLogWriter.WriteError.ioFailure(gateError)
        XCTAssertTrue(
            profile.isConnectionUnavailableErrorIncludingFlushUnderlying(wrapped),
            "classifier must unwrap through CommitLogWriter.WriteError → MetadataCreateGate.Error → connection error"
        )
    }

    // MARK: - FlushError.cancellationCause through MetadataCreateGate.Error

    func testFlushErrorCancellationCauseUnwrapsMetadataCreateGateError_staging() {
        let gate = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/snapshot.jsonl",
            underlying: CancellationError()
        )
        let wrapped = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(gate))
        XCTAssertNotNil(wrapped.cancellationCause,
                        "cancellation BFS must unwrap FlushError → WriteError.finalizationFailed → MetadataCreateGate.Error.stagingVerificationFailed → CancellationError")
    }

    func testFlushErrorCancellationCauseUnwrapsMetadataCreateGateError_final() {
        let gate = MetadataCreateGate.Error.finalVerificationFailed(
            remotePath: "/commit.jsonl",
            underlying: CancellationError()
        )
        let wrapped = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.ioFailure(gate))
        XCTAssertNotNil(wrapped.cancellationCause,
                        "cancellation BFS must unwrap FlushError → WriteError.ioFailure → MetadataCreateGate.Error.finalVerificationFailed → CancellationError")
    }

    func testFlushErrorCancellationCause_nilUnderlyingGateIsNotCancellation() {
        let gate = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/snapshot.jsonl",
            underlying: nil
        )
        let wrapped = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(gate))
        XCTAssertNil(wrapped.cancellationCause,
                     "nil-underlying gate error (byte mismatch) must not be cancellation")
    }

    func testFlushErrorCancellationCause_nonExclusiveFinalizationIsNotCancellation() {
        let gate = MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/snapshot.jsonl")
        let wrapped = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(gate))
        XCTAssertNil(wrapped.cancellationCause,
                     "nonExclusiveFinalization must not be cancellation")
    }

    func testFlushErrorCancellationCause_recognisesNSURLErrorCancelledLeaf() {
        let urlCancel = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let gate = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/snapshot.jsonl",
            underlying: urlCancel
        )
        let wrapped = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(gate))
        XCTAssertNotNil(wrapped.cancellationCause,
                        "URLSession-level cancel (NSURLErrorDomain/-999) must be classified as cancellation so user-stop mid-flush doesn't surface as a non-cancel error")
    }

    // MARK: - Strict-subset finder (BackupMonthStore wiring for production uploads)

    func testFindStrictSubsetAssetFingerprints_returnsOnlySupersededAssets() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let photoPath = "2026/01/photo.jpg"
        let videoPath = "2026/01/photo.mov"

        let photoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: photoPath,
            contentHash: photoHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let videoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: videoPath,
            contentHash: videoHash, fileSize: 200,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(photoResource)
        _ = try store.upsertResource(videoResource)

        // Asset A — partial (photo only); models an older backup before paired-video support.
        let partialFP = TestFixtures.assetFingerprint(0xB1)
        let partial = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: partialFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let partialLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: partialFP, resourceHash: photoHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        try store.upsertAsset(partial, links: [partialLink])

        // Unrelated asset C — different hash; must NOT show up as a subset.
        let unrelatedHash = TestFixtures.fingerprint(0xCC)
        let unrelatedPath = "2026/01/other.jpg"
        let unrelatedResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: unrelatedPath,
            contentHash: unrelatedHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(unrelatedResource)
        let unrelatedFP = TestFixtures.assetFingerprint(0xB2)
        let unrelated = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: unrelatedFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 50
        )
        let unrelatedLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: unrelatedFP, resourceHash: unrelatedHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        try store.upsertAsset(unrelated, links: [unrelatedLink])

        // Incoming bundle (photo + paired video) — A's links are a strict subset; C's are not.
        let supersedingKeys: Set<AssetResourceLinkKey> = [
            AssetResourceLinkKey(role: ResourceTypeCode.photo, slot: 0, hash: photoHash),
            AssetResourceLinkKey(role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash)
        ]
        let subsets = store.findStrictSubsetAssetFingerprints(forResourceKeys: supersedingKeys)
        XCTAssertEqual(Set(subsets), [partialFP],
                       "strict-subset finder must report the partial asset and only it")
    }

    func testFindStrictSubsetAssetFingerprints_v1AndV2ReturnSameFingerprints() async throws {
        let v1Client = InMemoryRemoteStorageClient()
        try await v1Client.connect()
        let v1Store = try await MonthManifestStore.loadOrCreate(
            client: v1Client, basePath: basePath, year: year, month: month
        )

        let v2Client = InMemoryRemoteStorageClient()
        try await v2Client.connect()
        let v2 = try await makeV2Services(client: v2Client)
        let v2Store = try await V2MonthSession.loadOrCreate(
            client: v2Client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let unrelatedHash = TestFixtures.fingerprint(0xCC)
        let partialFP = TestFixtures.assetFingerprint(0xB1)
        let unrelatedFP = TestFixtures.assetFingerprint(0xB2)

        let photoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.jpg",
            contentHash: photoHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let videoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.mov",
            contentHash: videoHash, fileSize: 200,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        )
        let unrelatedResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/other.jpg",
            contentHash: unrelatedHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let partialAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: partialFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let partialLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: partialFP, resourceHash: photoHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        let unrelatedAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: unrelatedFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 50
        )
        let unrelatedLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: unrelatedFP, resourceHash: unrelatedHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )

        func seed(_ store: any BackupMonthStore) throws {
            _ = try store.upsertResource(photoResource)
            _ = try store.upsertResource(videoResource)
            _ = try store.upsertResource(unrelatedResource)
            try store.upsertAsset(partialAsset, links: [partialLink])
            try store.upsertAsset(unrelatedAsset, links: [unrelatedLink])
        }

        try seed(v1Store)
        try seed(v2Store)

        let supersedingKeys: Set<AssetResourceLinkKey> = [
            AssetResourceLinkKey(role: ResourceTypeCode.photo, slot: 0, hash: photoHash),
            AssetResourceLinkKey(role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash)
        ]

        XCTAssertEqual(
            Set(v1Store.findStrictSubsetAssetFingerprints(forResourceKeys: supersedingKeys)),
            [partialFP]
        )
        XCTAssertEqual(
            Set(v2Store.findStrictSubsetAssetFingerprints(forResourceKeys: supersedingKeys)),
            [partialFP]
        )
    }

    func testFindStrictSubsetAssetFingerprints_excludesEqualLinkSet() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xA1)
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/p.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(resource)
        let fp = TestFixtures.assetFingerprint(0xB1)
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "p.jpg"
        )
        try store.upsertAsset(asset, links: [link])

        // Equal link set — same (role, slot, hash); must not be flagged as a strict subset.
        let sameKeys: Set<AssetResourceLinkKey> = [
            AssetResourceLinkKey(role: ResourceTypeCode.photo, slot: 0, hash: hash)
        ]
        XCTAssertTrue(store.findStrictSubsetAssetFingerprints(forResourceKeys: sameKeys).isEmpty,
                      "equal link set is not a strict subset")
    }

    func testStrictSubsetFindAndHasParityAcrossV1AndV2EdgeCases() async throws {
        let v1Client = InMemoryRemoteStorageClient()
        try await v1Client.connect()
        let v1Store = try await MonthManifestStore.loadOrCreate(
            client: v1Client, basePath: basePath, year: year, month: month
        )

        let v2Client = InMemoryRemoteStorageClient()
        try await v2Client.connect()
        let v2 = try await makeV2Services(client: v2Client)
        let v2Store = try await V2MonthSession.loadOrCreate(
            client: v2Client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let properHash = TestFixtures.fingerprint(0xA1)
        let properExtraHash = TestFixtures.fingerprint(0xA2)
        let roleHash = TestFixtures.fingerprint(0xA3)
        let roleExtraHash = TestFixtures.fingerprint(0xA4)
        let slotHash = TestFixtures.fingerprint(0xA5)
        let slotExtraHash = TestFixtures.fingerprint(0xA6)
        let properFP = TestFixtures.assetFingerprint(0xB1)
        let roleFP = TestFixtures.assetFingerprint(0xB2)
        let slotFP = TestFixtures.assetFingerprint(0xB3)
        let emptyFP = TestFixtures.assetFingerprint(0xB4)

        let resources = [
            (properHash, "proper.jpg", ResourceTypeCode.photo),
            (properExtraHash, "proper.mov", ResourceTypeCode.pairedVideo),
            (roleHash, "role.jpg", ResourceTypeCode.photo),
            (roleExtraHash, "role.mov", ResourceTypeCode.pairedVideo),
            (slotHash, "slot.jpg", ResourceTypeCode.photo),
            (slotExtraHash, "slot.mov", ResourceTypeCode.pairedVideo)
        ]

        func seed(_ store: any BackupMonthStore) throws {
            for (hash, name, type) in resources {
                _ = try store.upsertResource(RemoteManifestResource(
                    year: year,
                    month: month,
                    physicalRemotePath: "2026/01/\(name)",
                    contentHash: hash,
                    fileSize: 1,
                    resourceType: type,
                    creationDateMs: nil,
                    backedUpAtMs: 0
                ))
            }

            let properAsset = strictSubsetAsset(fingerprint: properFP, resourceCount: 1)
            try store.upsertAsset(properAsset, links: [
                strictSubsetLink(fingerprint: properFP, hash: properHash, name: "proper.jpg")
            ])

            let roleAsset = strictSubsetAsset(fingerprint: roleFP, resourceCount: 1)
            try store.upsertAsset(roleAsset, links: [
                strictSubsetLink(
                    fingerprint: roleFP,
                    hash: roleHash,
                    role: ResourceTypeCode.pairedVideo,
                    name: "role.jpg"
                )
            ])

            let slotAsset = strictSubsetAsset(fingerprint: slotFP, resourceCount: 1)
            try store.upsertAsset(slotAsset, links: [
                strictSubsetLink(
                    fingerprint: slotFP,
                    hash: slotHash,
                    slot: 1,
                    name: "slot.jpg"
                )
            ])

            try store.upsertAsset(
                strictSubsetAsset(fingerprint: emptyFP, resourceCount: 0),
                links: []
            )
        }

        try seed(v1Store)
        try seed(v2Store)

        let cases: [(name: String, keys: Set<AssetResourceLinkKey>, expected: Set<AssetFingerprint>)] = [
            (
                name: "proper strict subset",
                keys: [
                    strictSubsetKey(hash: properHash),
                    strictSubsetKey(hash: properExtraHash, role: ResourceTypeCode.pairedVideo)
                ],
                expected: [properFP]
            ),
            (
                name: "equal set",
                keys: [strictSubsetKey(hash: properHash)],
                expected: []
            ),
            (
                name: "empty candidate",
                keys: [
                    strictSubsetKey(hash: TestFixtures.fingerprint(0xD1)),
                    strictSubsetKey(hash: TestFixtures.fingerprint(0xD2), role: ResourceTypeCode.pairedVideo)
                ],
                expected: []
            ),
            (
                name: "same hash different role",
                keys: [
                    strictSubsetKey(hash: roleHash),
                    strictSubsetKey(hash: roleExtraHash, role: ResourceTypeCode.pairedVideo)
                ],
                expected: []
            ),
            (
                name: "same hash different slot",
                keys: [
                    strictSubsetKey(hash: slotHash),
                    strictSubsetKey(hash: slotExtraHash, role: ResourceTypeCode.pairedVideo)
                ],
                expected: []
            ),
            (
                name: "no overlap",
                keys: [
                    strictSubsetKey(hash: TestFixtures.fingerprint(0xD3)),
                    strictSubsetKey(hash: TestFixtures.fingerprint(0xD4), role: ResourceTypeCode.pairedVideo)
                ],
                expected: []
            )
        ]

        func assertStore(
            _ store: any BackupMonthStore,
            label: String,
            file: StaticString = #filePath,
            line: UInt = #line
        ) {
            for item in cases {
                let found = Set(store.findStrictSubsetAssetFingerprints(forResourceKeys: item.keys))
                XCTAssertEqual(found, item.expected, "\(label): \(item.name)", file: file, line: line)
                XCTAssertEqual(
                    store.hasStrictSubsetAssetFingerprint(forResourceKeys: item.keys),
                    !found.isEmpty,
                    "\(label): \(item.name)",
                    file: file,
                    line: line
                )
            }
        }

        assertStore(v1Store, label: "v1")
        assertStore(v2Store, label: "v2")
    }

    func testV2StrictSubsetIndexDedupesDuplicateLinks() throws {
        let indexes = emptyV2Indexes()
        let hash = TestFixtures.fingerprint(0xE1)
        let extraHash = TestFixtures.fingerprint(0xE2)
        let fingerprint = TestFixtures.assetFingerprint(0xE3)
        _ = try addV2IndexedResource(indexes, hash: hash, name: "duplicate.jpg")
        let asset = strictSubsetAsset(fingerprint: fingerprint, resourceCount: 2)
        let duplicateLink = strictSubsetLink(fingerprint: fingerprint, hash: hash, name: "duplicate.jpg")
        try indexes.upsertAsset(asset, links: [duplicateLink, duplicateLink], replacingSubsetFingerprints: [])

        let supersedingKeys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: hash),
            strictSubsetKey(hash: extraHash, role: ResourceTypeCode.pairedVideo)
        ]
        XCTAssertEqual(
            Set(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: supersedingKeys)),
            [fingerprint]
        )
        XCTAssertTrue(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: [strictSubsetKey(hash: hash)]).isEmpty)
        let stats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: supersedingKeys)
        XCTAssertEqual(stats.candidateCount, 1)
        XCTAssertEqual(stats.predicateChecks, 1)
    }

    func testV2StrictSubsetIndexUpdatesOnSameFingerprintReplacement() throws {
        let indexes = emptyV2Indexes()
        let oldHash = TestFixtures.fingerprint(0xE4)
        let newHash = TestFixtures.fingerprint(0xE5)
        let extraHash = TestFixtures.fingerprint(0xE6)
        let fingerprint = TestFixtures.assetFingerprint(0xE7)
        _ = try addV2IndexedResource(indexes, hash: oldHash, name: "old.jpg")
        _ = try addV2IndexedResource(indexes, hash: newHash, name: "new.jpg")

        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: fingerprint, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: fingerprint, hash: oldHash, name: "old.jpg")],
            replacingSubsetFingerprints: []
        )
        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: fingerprint, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: fingerprint, hash: newHash, name: "new.jpg")],
            replacingSubsetFingerprints: []
        )

        let oldKeys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: oldHash),
            strictSubsetKey(hash: extraHash, role: ResourceTypeCode.pairedVideo)
        ]
        XCTAssertTrue(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: oldKeys).isEmpty)
        let oldStats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: oldKeys)
        XCTAssertEqual(oldStats.candidateCount, 0)
        XCTAssertEqual(oldStats.predicateChecks, 0)

        let newKeys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: newHash),
            strictSubsetKey(hash: extraHash, role: ResourceTypeCode.pairedVideo)
        ]
        XCTAssertEqual(
            Set(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: newKeys)),
            [fingerprint]
        )
    }

    func testV2StrictSubsetIndexRemovesTombstonedSubsetsAndCleansEmptyBuckets() throws {
        let indexes = emptyV2Indexes()
        let partialHash = TestFixtures.fingerprint(0xE8)
        let videoHash = TestFixtures.fingerprint(0xE9)
        let oldOnlyHash = TestFixtures.fingerprint(0xEA)
        let replacementHash = TestFixtures.fingerprint(0xEB)
        let queryExtraHash = TestFixtures.fingerprint(0xEC)
        let partialFP = TestFixtures.assetFingerprint(0xF1)
        let fullFP = TestFixtures.assetFingerprint(0xF2)
        let oldOnlyFP = TestFixtures.assetFingerprint(0xF3)
        let replacementFP = TestFixtures.assetFingerprint(0xF4)

        for (hash, name, type) in [
            (partialHash, "partial.jpg", ResourceTypeCode.photo),
            (videoHash, "partial.mov", ResourceTypeCode.pairedVideo),
            (oldOnlyHash, "old-only.jpg", ResourceTypeCode.photo),
            (replacementHash, "replacement.jpg", ResourceTypeCode.photo)
        ] {
            _ = try addV2IndexedResource(indexes, hash: hash, name: name, resourceType: type)
        }

        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: partialFP, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: partialFP, hash: partialHash, name: "partial.jpg")],
            replacingSubsetFingerprints: []
        )
        let fullLinks = [
            strictSubsetLink(fingerprint: fullFP, hash: partialHash, name: "partial.jpg"),
            strictSubsetLink(
                fingerprint: fullFP,
                hash: videoHash,
                role: ResourceTypeCode.pairedVideo,
                name: "partial.mov"
            )
        ]
        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: fullFP, resourceCount: 2),
            links: fullLinks,
            replacingSubsetFingerprints: [partialFP]
        )
        let fullKeys = AssetResourceLinkSetPredicate.keys(fromLinks: fullLinks)
        XCTAssertTrue(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: fullKeys).isEmpty)
        XCTAssertFalse(indexes.hasStrictSubsetAssetFingerprint(forResourceKeys: fullKeys))

        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: oldOnlyFP, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: oldOnlyFP, hash: oldOnlyHash, name: "old-only.jpg")],
            replacingSubsetFingerprints: []
        )
        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: replacementFP, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: replacementFP, hash: replacementHash, name: "replacement.jpg")],
            replacingSubsetFingerprints: [oldOnlyFP]
        )
        let oldOnlyQuery: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: oldOnlyHash),
            strictSubsetKey(hash: queryExtraHash, role: ResourceTypeCode.pairedVideo)
        ]
        XCTAssertTrue(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: oldOnlyQuery).isEmpty)
        let oldOnlyStats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: oldOnlyQuery)
        XCTAssertEqual(oldOnlyStats.candidateCount, 0)
        XCTAssertEqual(oldOnlyStats.predicateChecks, 0)
    }

    func testV2StrictSubsetIndexRecordCommitDoesNotChangeQuery() throws {
        let indexes = emptyV2Indexes()
        let photoHash = TestFixtures.fingerprint(0xED)
        let videoHash = TestFixtures.fingerprint(0xEE)
        let fingerprint = TestFixtures.assetFingerprint(0xEF)
        _ = try addV2IndexedResource(indexes, hash: photoHash, name: "photo.jpg")
        try indexes.upsertAsset(
            strictSubsetAsset(fingerprint: fingerprint, resourceCount: 1),
            links: [strictSubsetLink(fingerprint: fingerprint, hash: photoHash, name: "photo.jpg")],
            replacingSubsetFingerprints: []
        )

        let keys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: photoHash),
            strictSubsetKey(hash: videoHash, role: ResourceTypeCode.pairedVideo)
        ]
        let before = Set(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys))
        let beforeStats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: keys)
        indexes.recordCommit(
            assetClocks: [fingerprint: 1],
            tombstoneClocks: [:],
            committedResources: [:],
            committedResourceClocks: [:],
            writerID: writerID,
            seq: 1
        )
        XCTAssertEqual(Set(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys)), before)
        XCTAssertEqual(indexes.strictSubsetQueryStatsForTesting(forResourceKeys: keys), beforeStats)
    }

    func testV2StrictSubsetQueryNarrowsCandidatesInLargeMonth() throws {
        let unrelatedCount = 5_000
        let fixture = largeStrictSubsetState(unrelatedCount: unrelatedCount, includePartial: true)
        let indexes = V2MonthIndexes(
            year: year,
            month: month,
            materializedState: fixture.state,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        let keys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: fixture.photoHash),
            strictSubsetKey(hash: fixture.videoHash, role: ResourceTypeCode.pairedVideo)
        ]

        XCTAssertEqual(
            Set(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys)),
            [fixture.partialFingerprint]
        )
        XCTAssertTrue(indexes.hasStrictSubsetAssetFingerprint(forResourceKeys: keys))
        let stats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: keys)
        XCTAssertEqual(stats.incomingKeyCount, 2)
        XCTAssertEqual(stats.hashBucketLookups, 2)
        XCTAssertEqual(stats.candidateCount, 1)
        XCTAssertEqual(stats.predicateChecks, 1)
        XCTAssertLessThan(stats.candidateCount, unrelatedCount / 100)
    }

    func testV2StrictSubsetQueryAvoidsPredicateChecksWhenNoHashesOverlap() throws {
        let fixture = largeStrictSubsetState(unrelatedCount: 5_000, includePartial: false)
        let indexes = V2MonthIndexes(
            year: year,
            month: month,
            materializedState: fixture.state,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        let keys: Set<AssetResourceLinkKey> = [
            strictSubsetKey(hash: fixture.photoHash),
            strictSubsetKey(hash: fixture.videoHash, role: ResourceTypeCode.pairedVideo)
        ]

        XCTAssertTrue(indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys).isEmpty)
        XCTAssertFalse(indexes.hasStrictSubsetAssetFingerprint(forResourceKeys: keys))
        let stats = indexes.strictSubsetQueryStatsForTesting(forResourceKeys: keys)
        XCTAssertEqual(stats.incomingKeyCount, 2)
        XCTAssertEqual(stats.hashBucketLookups, 2)
        XCTAssertEqual(stats.candidateCount, 0)
        XCTAssertEqual(stats.predicateChecks, 0)
    }

    /// Heal round-trip: pre-fix manifest carries both partial `A` and full `B`. A later
    /// session running the fixed strict-subset wiring (findStrictSubset → upsertAsset)
    /// must self-heal — replay the same wiring AssetProcessor uses and verify `A` is
    /// tombstoned in the materialized state after flush, even though `B` was already
    /// present before the upsert.
    func testStrictSubsetHealRoundTrip_priorPartialIsTombstonedOnSubsequentUpsert() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let photoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.jpg",
            contentHash: photoHash, fileSize: 1,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let videoResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.mov",
            contentHash: videoHash, fileSize: 2,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(photoResource)
        _ = try store.upsertResource(videoResource)

        // Partial A (pre-fix backup: photo only). Committed in the initial flush so the
        // heal step sees A as a baseline asset, not a same-session pending one.
        let partialFP = TestFixtures.assetFingerprint(0xB1)
        let partialAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: partialFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let partialLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: partialFP, resourceHash: photoHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        try store.upsertAsset(partialAsset, links: [partialLink])

        // Full B (post-fix backup: photo + paired video) — committed in the same flush.
        let fullFP = TestFixtures.assetFingerprint(0xB2)
        let fullAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fullFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 2, totalFileSizeBytes: 3
        )
        let fullLinks = [
            RemoteAssetResourceLink(
                year: year, month: month, assetFingerprint: fullFP, resourceHash: photoHash,
                role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
            ),
            RemoteAssetResourceLink(
                year: year, month: month, assetFingerprint: fullFP, resourceHash: videoHash,
                role: ResourceTypeCode.pairedVideo, slot: 0, logicalName: "photo.mov"
            )
        ]
        try store.upsertAsset(fullAsset, links: fullLinks)
        _ = try await store.flushToRemote(ignoreCancellation: false)

        XCTAssertTrue(store.containsAssetFingerprint(partialFP),
                      "precondition: partial A must be in the manifest before the heal upsert")
        XCTAssertTrue(store.containsAssetFingerprint(fullFP),
                      "precondition: full B must be in the manifest before the heal upsert")

        // Replay AssetProcessor's wiring: compute strict subsets from B's links and pass
        // them into upsertAsset. With the fix, this tombstones A even though the caller
        // is just re-asserting B against the existing baseline.
        let fullKeys = AssetResourceLinkSetPredicate.keys(fromLinks: fullLinks)
        let subsets = Set(store.findStrictSubsetAssetFingerprints(forResourceKeys: fullKeys))
        XCTAssertEqual(subsets, [partialFP],
                       "strict-subset finder must surface A given B's full link set")
        try store.upsertAsset(fullAsset, links: fullLinks, replacingSubsetFingerprints: subsets)
        let healDelta = try await store.flushToRemote(ignoreCancellation: false)
        XCTAssertTrue(healDelta.committedTombstoneFingerprints.contains(partialFP),
                      "heal flush must emit a tombstone for A")

        // Materialized state must reflect A being tombstoned and B remaining.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNil(monthState.assets[partialFP],
                     "strict-subset heal: partial A must be gone from materialized state after flush")
        XCTAssertNotNil(monthState.assets[fullFP],
                        "strict-subset heal: full B must remain after flush")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(partialFP),
                      "strict-subset heal: A must appear as tombstoned for LWW gating against stale peer adds")
    }

    // MARK: - External-volume regression for connection-unavailable through V2 wrappers

    func testIsConnectionUnavailableEnumeratesEveryWrapperNodeForExternalVolume() throws {
        // Pin invariant #4 from the unit-003 plan: for external-volume profiles, the per-node
        // predicate `isConnectionUnavailableError` itself walks wrappers via
        // `isLikelyExternalStorageUnavailable`. The new BackupErrorChain-based outer walk
        // must still surface the external-storage-unavailable cause when buried under
        // FlushError → WriteError → CommitWriteError → RemoteStorageClientError chains.
        let externalProfile = TestFixtures.makeServerProfile(storageType: .externalVolume)
        let cause = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(
                CommitLogWriter.WriteError.ioFailure(
                    RemoteStorageClientError.externalStorageUnavailable
                )
            ))
        XCTAssertTrue(
            externalProfile.isConnectionUnavailableErrorIncludingFlushUnderlying(cause),
            "external-volume profile must detect connection-unavailable through V2 flush wrappers"
        )
    }

    // MARK: - P10-W2 MonthIndexSplit: published-snapshot equivalence + boundaries

    /// Equivalent V1 and V2 month contents expose the same resource/asset/link sets where V1
    /// semantics apply, via the shared `SnapshotProjection` export the split now routes through.
    func testPublishedSnapshotEquivalence_v1AndV2_sameResourceAssetLinkSets() async throws {
        let v1Client = InMemoryRemoteStorageClient()
        try await v1Client.connect()
        let v1Store = try await MonthManifestStore.loadOrCreate(
            client: v1Client, basePath: basePath, year: year, month: month
        )

        let v2Client = InMemoryRemoteStorageClient()
        try await v2Client.connect()
        let v2 = try await makeV2Services(client: v2Client)
        let v2Store = try await V2MonthSession.loadOrCreate(
            client: v2Client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let photoHash = TestFixtures.fingerprint(0x11)
        let videoHash = TestFixtures.fingerprint(0x12)
        let soloHash = TestFixtures.fingerprint(0x13)
        let pairFP = TestFixtures.assetFingerprint(0x21)
        let soloFP = TestFixtures.assetFingerprint(0x22)

        func seed(_ store: any BackupMonthStore) throws {
            _ = try store.upsertResource(RemoteManifestResource(
                year: year, month: month, physicalRemotePath: "2026/01/pair.jpg",
                contentHash: photoHash, fileSize: 100,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
            ))
            _ = try store.upsertResource(RemoteManifestResource(
                year: year, month: month, physicalRemotePath: "2026/01/pair.mov",
                contentHash: videoHash, fileSize: 200,
                resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
            ))
            _ = try store.upsertResource(RemoteManifestResource(
                year: year, month: month, physicalRemotePath: "2026/01/solo.jpg",
                contentHash: soloHash, fileSize: 50,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
            ))
            try store.upsertAsset(
                RemoteManifestAsset(year: year, month: month, assetFingerprint: pairFP,
                                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 2, totalFileSizeBytes: 300),
                links: [
                    RemoteAssetResourceLink(year: year, month: month, assetFingerprint: pairFP, resourceHash: photoHash,
                                            role: ResourceTypeCode.photo, slot: 0, logicalName: "pair.jpg"),
                    RemoteAssetResourceLink(year: year, month: month, assetFingerprint: pairFP, resourceHash: videoHash,
                                            role: ResourceTypeCode.pairedVideo, slot: 0, logicalName: "pair.mov")
                ]
            )
            try store.upsertAsset(
                RemoteManifestAsset(year: year, month: month, assetFingerprint: soloFP,
                                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 50),
                links: [RemoteAssetResourceLink(year: year, month: month, assetFingerprint: soloFP, resourceHash: soloHash,
                                                role: ResourceTypeCode.photo, slot: 0, logicalName: "solo.jpg")]
            )
        }

        try seed(v1Store)
        try seed(v2Store)

        XCTAssertEqual(
            SnapshotProjection.normalize(v1Store),
            SnapshotProjection.normalize(v2Store),
            "equivalent V1 and V2 month contents must expose the same resource/asset/link sets"
        )
    }

    /// Subset replacement removes the replaced partial from the published set and keeps the V2
    /// tombstone state correct after commit/materialize.
    func testSubsetReplacement_dropsReplacedPartialFromPublishedSetAndTombstonesAfterMaterialize() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let partialFP = TestFixtures.assetFingerprint(0xB1)
        let fullFP = TestFixtures.assetFingerprint(0xB2)

        _ = try store.upsertResource(RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.jpg",
            contentHash: photoHash, fileSize: 1,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        ))
        _ = try store.upsertResource(RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.mov",
            contentHash: videoHash, fileSize: 2,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        ))

        // Commit partial A (photo only) on its own flush so it becomes a durable baseline asset.
        let partialAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: partialFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        try store.upsertAsset(partialAsset, links: [RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: partialFP, resourceHash: photoHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )])
        _ = try await store.flushToRemote()

        // Full B (photo + paired video) supersedes A.
        let fullAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fullFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 2, totalFileSizeBytes: 3
        )
        let fullLinks = [
            RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fullFP, resourceHash: photoHash,
                                    role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"),
            RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fullFP, resourceHash: videoHash,
                                    role: ResourceTypeCode.pairedVideo, slot: 0, logicalName: "photo.mov")
        ]
        let subsets = Set(store.findStrictSubsetAssetFingerprints(
            forResourceKeys: AssetResourceLinkSetPredicate.keys(fromLinks: fullLinks)
        ))
        XCTAssertEqual(subsets, [partialFP])
        try store.upsertAsset(fullAsset, links: fullLinks, replacingSubsetFingerprints: subsets)

        XCTAssertEqual(Set(store.unsortedSnapshot().assets.map(\.assetFingerprint)), [fullFP],
                       "published asset set must drop the replaced partial before flush")

        let healDelta = try await store.flushToRemote()
        XCTAssertTrue(healDelta.committedTombstoneFingerprints.contains(partialFP))

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNil(monthState.assets[partialFP], "replaced partial must be gone from materialized state")
        XCTAssertNotNil(monthState.assets[fullFP], "superseding asset must remain")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(partialFP),
                      "replaced partial must be tombstoned for LWW gating")
    }

    /// Publishing equivalent V1 and V2 stores through `RemoteIndexSyncService.publishMonthSnapshot`
    /// yields equivalent `remoteMonthRawData`, `currentState`, and resume safe-to-skip sets.
    func testPublishMonthSnapshot_v1AndV2_yieldEquivalentRemoteDataAndResumeSets() async throws {
        let v1Client = InMemoryRemoteStorageClient()
        try await v1Client.connect()
        let v1Store = try await MonthManifestStore.loadOrCreate(
            client: v1Client, basePath: basePath, year: year, month: month
        )
        let v2Client = InMemoryRemoteStorageClient()
        try await v2Client.connect()
        let v2 = try await makeV2Services(client: v2Client)
        let v2Store = try await V2MonthSession.loadOrCreate(
            client: v2Client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hashA = TestFixtures.fingerprint(0x31)
        let hashB = TestFixtures.fingerprint(0x32)
        // Resume classification recomputes the fingerprint from links, so use real fingerprints.
        let fpA = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hashA)]
        )
        let fpB = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hashB)]
        )

        func seed(_ store: any BackupMonthStore) throws {
            _ = try store.upsertResource(RemoteManifestResource(
                year: year, month: month, physicalRemotePath: "2026/01/a.jpg",
                contentHash: hashA, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
            ))
            _ = try store.upsertResource(RemoteManifestResource(
                year: year, month: month, physicalRemotePath: "2026/01/b.jpg",
                contentHash: hashB, fileSize: 20,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
            ))
            try store.upsertAsset(
                RemoteManifestAsset(year: year, month: month, assetFingerprint: fpA,
                                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 10),
                links: [RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fpA, resourceHash: hashA,
                                                role: ResourceTypeCode.photo, slot: 0, logicalName: "a.jpg")]
            )
            try store.upsertAsset(
                RemoteManifestAsset(year: year, month: month, assetFingerprint: fpB,
                                    creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 20),
                links: [RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fpB, resourceHash: hashB,
                                                role: ResourceTypeCode.photo, slot: 0, logicalName: "b.jpg")]
            )
        }
        try seed(v1Store)
        try seed(v2Store)

        let v1Service = RemoteIndexSyncService()
        let v2Service = RemoteIndexSyncService()
        v1Service.publishMonthSnapshot(of: v1Store, for: monthKey)
        v2Service.publishMonthSnapshot(of: v2Store, for: monthKey)

        let v1Raw = try XCTUnwrap(v1Service.remoteMonthRawData(for: monthKey))
        let v2Raw = try XCTUnwrap(v2Service.remoteMonthRawData(for: monthKey))
        XCTAssertEqual(
            SnapshotProjection.normalize((resources: v1Raw.resources, assets: v1Raw.assets, links: v1Raw.assetResourceLinks)),
            SnapshotProjection.normalize((resources: v2Raw.resources, assets: v2Raw.assets, links: v2Raw.assetResourceLinks)),
            "remoteMonthRawData row sets must match across V1 and V2 publish"
        )

        func monthDeltaNormalized(_ service: RemoteIndexSyncService) -> SnapshotProjection.Normalized {
            let delta = service.currentState(since: nil).monthDeltas.first { $0.month == monthKey }
            return SnapshotProjection.normalize((
                resources: delta?.resources ?? [],
                assets: delta?.assets ?? [],
                links: delta?.assetResourceLinks ?? []
            ))
        }
        XCTAssertEqual(monthDeltaNormalized(v1Service), monthDeltaNormalized(v2Service),
                       "currentState month delta row sets must match across V1 and V2 publish")

        XCTAssertEqual(
            v1Service.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            v2Service.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            "resume safe-to-skip sets must match across V1 and V2 publish"
        )
        XCTAssertEqual(v2Service.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], [fpA, fpB],
                       "both healthy assets are safe to skip")
    }

    /// Pending V2 adds are not durable until their batch commit lands; publish is withheld while
    /// uncommitted ops remain. (Multi-chunk partial durability is covered in V2BatchCommitTests.)
    func testContainsDurableAssetFingerprint_pendingV2AddNotDurableUntilFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xC1)
        let fp = TestFixtures.assetFingerprint(0xC2)
        _ = try store.upsertResource(RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/photo.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        ))
        try store.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: fp,
                                creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1),
            links: [RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash,
                                            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg")]
        )

        XCTAssertTrue(store.containsAssetFingerprint(fp))
        XCTAssertFalse(store.containsDurableAssetFingerprint(fp),
                       "pending V2 add must not be reported durable before its commit lands")
        XCTAssertTrue(store.hasUncommittedV2Ops)

        _ = try await store.flushToRemote()

        XCTAssertTrue(store.containsDurableAssetFingerprint(fp),
                      "after flush the committed add is durable")
        XCTAssertFalse(store.hasUncommittedV2Ops)
    }
}
