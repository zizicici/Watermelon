import XCTest
@testable import Watermelon

/// W1 `MonthDurableTransaction` state-machine + cancellation/resume matrix:
///   pending -> commitDurable -> sideEffectsDrained -> published, plus hard-abort.
/// Illegal/missing transitions surface as `LifecycleError`; partial-durable advances only the
/// committed delta and withholds publish while uncommitted V2 ops remain.
final class MonthDurableTransactionTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-durable-tx-test"
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

    // MARK: - State-machine invariants

    func testDrainSideEffects_beforeDurableCommit_throws() async {
        let transaction = makeForegroundTransaction(remoteIndex: RemoteIndexSyncService(), aggregator: ParallelBackupProgressAggregator(total: 1))
        XCTAssertEqual(transaction.state, .pending)
        do {
            _ = try await transaction.drainSideEffects()
            XCTFail("drain from .pending must throw")
        } catch let error as MonthDurableTransaction.LifecycleError {
            XCTAssertEqual(error, .drainRequiresDurableCommit)
        } catch {
            XCTFail("unexpected error \(error)")
        }
        XCTAssertEqual(transaction.state, .pending, "illegal drain must not advance the state")
    }

    func testPublish_beforeDrain_throws() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let transaction = makeForegroundTransaction(remoteIndex: remoteIndex, aggregator: ParallelBackupProgressAggregator(total: 1))
        // From .pending.
        XCTAssertThrowsError(try transaction.publishCommittedView(monthStore: store)) { error in
            XCTAssertEqual(error as? MonthDurableTransaction.LifecycleError, .publishRequiresDrainedSideEffects)
        }
        // From .commitDurable (recorded but not drained).
        transaction.beginCommitDurable(outcome: .completed(.none))
        XCTAssertEqual(transaction.state, .commitDurable)
        XCTAssertThrowsError(try transaction.publishCommittedView(monthStore: store)) { error in
            XCTAssertEqual(error as? MonthDurableTransaction.LifecycleError, .publishRequiresDrainedSideEffects)
        }
        XCTAssertEqual(transaction.state, .commitDurable, "illegal publish must not advance the state")
    }

    func testCanonicalCycle_advancesThroughEveryState() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let transaction = makeForegroundTransaction(
            remoteIndex: remoteIndex,
            aggregator: ParallelBackupProgressAggregator(total: 3),
            processor: makeProcessor(remoteIndex: remoteIndex)
        )
        for index in 0 ..< 3 {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }

        let outcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: false)
        XCTAssertEqual(transaction.state, .pending)
        transaction.beginCommitDurable(outcome: outcome)
        XCTAssertEqual(transaction.state, .commitDurable)
        _ = try await transaction.drainSideEffects()
        XCTAssertEqual(transaction.state, .sideEffectsDrained)
        let didPublish = try transaction.publishCommittedView(monthStore: store)
        XCTAssertEqual(transaction.state, .published)
        XCTAssertTrue(didPublish, "full durable flush must publish")
        XCTAssertEqual(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey]?.count, 3,
                       "full durable flush becomes visible through RemoteIndexSyncService in the same session")
    }

    // Publish is blocked while hasUncommittedV2Ops is true after a partial durable result.
    func testPublish_partialDurableWithUncommittedOps_emitsNothing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let cap = BackupV2Constants.batchFlushInterval
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }
        let chunk2Path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 2)
        await client.injectUploadError(.transport, for: chunk2Path)

        let remoteIndex = RemoteIndexSyncService()
        let transaction = makeForegroundTransaction(
            remoteIndex: remoteIndex,
            aggregator: ParallelBackupProgressAggregator(total: cap + 1),
            processor: makeProcessor(remoteIndex: remoteIndex)
        )
        let outcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: false)
        guard case .commitDurablePartial = outcome else {
            XCTFail("expected commitDurablePartial, got \(outcome)")
            return
        }
        transaction.beginCommitDurable(outcome: outcome)
        _ = try await transaction.drainSideEffects()
        let didPublish = try transaction.publishCommittedView(monthStore: store)
        XCTAssertFalse(didPublish, "publish must be blocked while hasUncommittedV2Ops is true")
        XCTAssertNil(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
                     "no non-durable rows may reach the committed view on a partial result")
        XCTAssertTrue(store.hasUncommittedV2Ops)
    }

    // MARK: - Hard abort

    func testHardAbort_fromPending_rollsBackNonDurableStateAndOverlay() async throws {
        let remoteIndex = RemoteIndexSyncService()
        let processor = makeProcessor(remoteIndex: remoteIndex)
        let aggregator = ParallelBackupProgressAggregator(total: 1)
        let rows = makeAssetRows(index: 0)
        let writer = remoteIndex.makeOptimisticAssetWriter()
        writer.appendResource(rows.resource)
        writer.appendAsset(rows.asset, links: [rows.link])
        await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "local-0", assetFingerprint: rows.asset.assetFingerprint,
            totalFileSizeBytes: 1, modificationDateMs: nil, body: .fingerprintOnly(resourceCount: 1)
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "asset-0", assetFingerprint: rows.asset.assetFingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        await aggregator.recordProvisional(month: monthKey, fingerprint: rows.asset.assetFingerprint, assetLocalIdentifier: "local-0", status: .success)

        let transaction = makeForegroundTransaction(remoteIndex: remoteIndex, aggregator: aggregator, processor: processor)
        await transaction.abort()

        let intentsAfter = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfter = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfter, 0)
        XCTAssertEqual(provisionalAfter, 0)
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 0, "provisional success reverted on abort")
        XCTAssertEqual(state.failed, 1)
        XCTAssertNil(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], "optimistic overlay dropped")
        XCTAssertEqual(transaction.state, .pending)
    }

    func testHardAbort_afterPublished_isNoOpForDurableRows() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let transaction = makeForegroundTransaction(
            remoteIndex: remoteIndex,
            aggregator: ParallelBackupProgressAggregator(total: 2),
            processor: makeProcessor(remoteIndex: remoteIndex)
        )
        var fingerprints: Set<AssetFingerprint> = []
        for index in 0 ..< 2 {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
            fingerprints.insert(rows.asset.assetFingerprint)
        }
        let outcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: false)
        transaction.beginCommitDurable(outcome: outcome)
        _ = try await transaction.drainSideEffects()
        XCTAssertTrue(try transaction.publishCommittedView(monthStore: store))
        XCTAssertEqual(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], fingerprints)

        // Hard abort after publish must leave the durable (published) rows intact.
        await transaction.abort()
        XCTAssertEqual(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey], fingerprints,
                       "hard abort after published is a no-op for durable rows")
    }

    // MARK: - Cancellation / interruption / resume matrix

    // Interruption before any durable commit: no drained hash-index row, provisional rolled back,
    // overlay dropped, and the pending asset does not materialize after reload.
    func testInterruptionBeforeDurableCommit_leavesNothingDurable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let hashRepo = ContentHashIndexRepository(databaseManager: databaseManager)
        let processor = AssetProcessor(photoLibraryService: PhotoLibraryService(), hashIndexRepository: hashRepo, remoteIndexService: remoteIndex)
        let aggregator = ParallelBackupProgressAggregator(total: 1)

        // Asset is upserted into the session (pending) but never flushed — the optimistic overlay +
        // intent + provisional record exist, nothing is durable on remote.
        let rows = makeAssetRows(index: 0)
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])
        processor.optimisticWriter.appendResource(rows.resource)
        processor.optimisticWriter.appendAsset(rows.asset, links: [rows.link])
        await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "local-0", assetFingerprint: rows.asset.assetFingerprint,
            totalFileSizeBytes: 1, modificationDateMs: nil, body: .fingerprintOnly(resourceCount: 1)
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "asset-0", assetFingerprint: rows.asset.assetFingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        await aggregator.recordProvisional(month: monthKey, fingerprint: rows.asset.assetFingerprint, assetLocalIdentifier: "local-0", status: .success)

        let transaction = makeForegroundTransaction(remoteIndex: remoteIndex, aggregator: aggregator, processor: processor)
        await transaction.abort()

        let intentsAfter = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfter = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfter, 0)
        XCTAssertEqual(provisionalAfter, 0)
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 0)
        XCTAssertEqual(state.failed, 1)
        XCTAssertNil(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey])
        // No hash-index row was ever drained — drain runs only after a durable commit.
        let caches = try hashRepo.fetchAssetHashCaches(assetIDs: ["local-0"])
        XCTAssertTrue(caches.isEmpty, "no hash-index row may exist for an asset that never committed durably")
        // After reload from durable commit files, the pending asset is absent.
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertNil(output.state.months[monthKey]?.assets[rows.asset.assetFingerprint])
    }

    // Chunk 1 durable then chunk 2 interruption, then retry/resume commits chunk 2 and publishes
    // the full durable set. Exercises the deferred-then-recovered cycle end-to-end via the transaction.
    func testChunk1DurableThenChunk2Interruption_thenRetryCommitsAndPublishesFullSet() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let processor = makeProcessor(remoteIndex: remoteIndex)
        let cap = BackupV2Constants.batchFlushInterval
        let aggregator = ParallelBackupProgressAggregator(total: cap + 1)
        var allFingerprints: [AssetFingerprint] = []
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
            allFingerprints.append(rows.asset.assetFingerprint)
            await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(index)"), assetFingerprint: rows.asset.assetFingerprint,
                totalFileSizeBytes: 1, modificationDateMs: nil, body: .fingerprintOnly(resourceCount: 1)
            ))
            _ = await aggregator.record(result: AssetProcessResult(
                status: .success, reason: nil, displayName: "asset-\(index)", assetFingerprint: rows.asset.assetFingerprint,
                timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
            ))
            await aggregator.recordProvisional(month: monthKey, fingerprint: rows.asset.assetFingerprint,
                                                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(index)"), status: .success)
        }
        let chunk2Path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 2)
        await client.injectUploadError(.transport, for: chunk2Path)

        let transaction = makeForegroundTransaction(remoteIndex: remoteIndex, aggregator: aggregator, processor: processor)
        // First (interval) cycle: chunk 1 durable, chunk 2 interrupted. Drain chunk 1, withhold publish.
        let firstOutcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: false)
        guard case .commitDurablePartial(let firstDelta, _) = firstOutcome else {
            XCTFail("expected partial after chunk-2 interruption")
            return
        }
        transaction.beginCommitDurable(outcome: firstOutcome)
        _ = try await transaction.drainSideEffects()
        XCTAssertFalse(try transaction.publishCommittedView(monthStore: store), "publish withheld for chunk 2")
        let intentsAfterFirst = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterFirst, (cap + 1) - firstDelta.committedAssetFingerprints.count,
                       "only chunk-1 intents drained; chunk 2 stays queued")
        XCTAssertTrue(store.hasUncommittedV2Ops)
        // Materialize sees only chunk 1.
        let partialState = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(partialState.state.months[monthKey]?.assets.count, cap)

        // Retry/resume: the leftover commits at a fresh seq (seq=2's injected error does not block
        // seq=3), so drain it and publish the full set.
        let retryOutcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: true)
        guard case .completed = retryOutcome else {
            XCTFail("retry must complete cleanly, got \(retryOutcome)")
            return
        }
        transaction.beginCommitDurable(outcome: retryOutcome)
        _ = try await transaction.drainSideEffects()
        XCTAssertTrue(try transaction.publishCommittedView(monthStore: store), "full durable set publishes on retry")
        let intentsAfterRetry = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfterRetry = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterRetry, 0)
        XCTAssertEqual(provisionalAfterRetry, 0)
        XCTAssertFalse(store.hasUncommittedV2Ops)
        XCTAssertEqual(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey]?.count, cap + 1)
        let finalState = await aggregator.snapshot()
        XCTAssertEqual(finalState.succeeded, cap + 1, "deferred reconciliation prevented spurious failures")
        XCTAssertEqual(finalState.failed, 0)
    }

    // End-of-month cancellation with ignoreCancellation preserves the final-flush-can-still-commit
    // behavior: a paused/cancelled EOM opts into ignoring cancellation, and the commit-only flush
    // still drains and commits the in-memory batch rather than dropping it.
    func testEndOfMonthCancellation_ignoreCancellation_finalFlushStillCommits() async throws {
        // The paused / cancelled-at-boundary cases both opt into ignoring cancellation.
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(paused: true, taskIsCancelled: false, hasV2Services: true))
        XCTAssertTrue(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(paused: false, taskIsCancelled: true, hasV2Services: true))
        // A genuinely live, unpaused completion still respects cancellation.
        XCTAssertFalse(BackupParallelExecutor.foregroundFinalFlushIgnoresCancellation(paused: false, taskIsCancelled: false, hasV2Services: true))

        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let rows = makeAssetRows(index: 0)
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])

        // The final flush runs with ignoreCancellation=true and must still commit the batch.
        let outcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: true)
        guard case .completed(let delta) = outcome else {
            XCTFail("final flush under ignoreCancellation must complete, got \(outcome)")
            return
        }
        XCTAssertEqual(delta.committedAssetFingerprints, [rows.asset.assetFingerprint])
        XCTAssertFalse(store.hasUncommittedV2Ops, "pending op committed by the ignore-cancellation final flush")
    }

    // Background variant: nil aggregator. Drain still advances the lifecycle and drains intents;
    // abort clears the month's intents and drops the overlay without touching progress counters.
    func testBackgroundVariant_drainsIntents_andAbortClearsIntentsAndOverlay() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(client: client, basePath: basePath, year: year, month: month, v2Services: v2)
        let remoteIndex = RemoteIndexSyncService()
        let processor = makeProcessor(remoteIndex: remoteIndex)
        for index in 0 ..< 2 {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
            processor.optimisticWriter.appendAsset(rows.asset, links: [rows.link])
            await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(index)"), assetFingerprint: rows.asset.assetFingerprint,
                totalFileSizeBytes: 1, modificationDateMs: nil, body: .fingerprintOnly(resourceCount: 1)
            ))
        }

        let transaction = MonthDurableTransaction(
            aggregator: nil, assetProcessor: processor, eventStream: BackupEventStream(),
            profile: TestFixtures.makeServerProfile(storageType: .webdav), month: monthKey, workerID: 1
        )
        let outcome = try await BackupParallelExecutor.commitMonthStoreDefensively(monthStore: store, ignoreCancellation: false)
        transaction.beginCommitDurable(outcome: outcome)
        let drainOutcome = try await transaction.drainSideEffects()
        guard case .allDrained(let count)? = drainOutcome else {
            XCTFail("background drain must return the HashIndexDrainOutcome, got \(String(describing: drainOutcome))")
            return
        }
        XCTAssertEqual(count, 2, "background drain writes the durable intents")
        let intentsAfterDrain = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterDrain, 0)
        XCTAssertTrue(try transaction.publishCommittedView(monthStore: store))
        XCTAssertEqual(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey]?.count, 2)

        // Now enqueue a fresh (non-durable) intent + overlay and abort: background abort clears the
        // month's intents and drops the overlay (no aggregator counters involved).
        let extra = makeAssetRows(index: 99)
        processor.optimisticWriter.appendAsset(extra.asset, links: [extra.link])
        await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "local-99", assetFingerprint: extra.asset.assetFingerprint,
            totalFileSizeBytes: 1, modificationDateMs: nil, body: .fingerprintOnly(resourceCount: 1)
        ))
        await transaction.abort()
        let intentsAfterAbort = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterAbort, 0, "background abort clears the month's queued intents")
    }

    // MARK: - Helpers

    private func makeProcessor(remoteIndex: RemoteIndexSyncService) -> AssetProcessor {
        AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndex
        )
    }

    private func makeForegroundTransaction(
        remoteIndex: RemoteIndexSyncService,
        aggregator: ParallelBackupProgressAggregator,
        processor: AssetProcessor? = nil
    ) -> MonthDurableTransaction {
        MonthDurableTransaction(
            aggregator: aggregator,
            assetProcessor: processor ?? makeProcessor(remoteIndex: remoteIndex),
            eventStream: BackupEventStream(),
            profile: TestFixtures.makeServerProfile(storageType: .webdav),
            month: monthKey,
            workerID: 1
        )
    }

    private func makeV2Services(client: InMemoryRemoteStorageClient) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
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
            initialMaterializeOutput: InitialMaterializeOutputBox(nil)
        )
    }

    private func makeAssetRows(index: Int) -> (asset: RemoteManifestAsset, resource: RemoteManifestResource, link: RemoteAssetResourceLink) {
        var hashBytes = [UInt8](repeating: 0, count: 32)
        hashBytes[0] = UInt8((index >> 8) & 0xff)
        hashBytes[1] = UInt8(index & 0xff)
        hashBytes[2] = 0xAA
        let hash = Data(hashBytes)
        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let physicalPath = "2026/01/asset-\(index).jpg"
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFingerprint,
            creationDateMs: 1_700_000_000_000 + Int64(index),
            backedUpAtMs: 1_700_000_001_000 + Int64(index),
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFingerprint, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "asset-\(index).jpg"
        )
        return (asset, resource, link)
    }
}
