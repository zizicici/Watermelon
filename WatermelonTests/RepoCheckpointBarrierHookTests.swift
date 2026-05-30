import Foundation
import XCTest
@testable import Watermelon

final class RepoCheckpointBarrierHookTests: XCTestCase {
    func testRuntimeDefaultsAdvertiseFullRetentionCapability() {
        XCTAssertEqual(
            RepoRetentionRuntimeDefaults.peerCapability,
            RetentionPeerCapability(barrierAwareSessionRefresh: true, checkpointBarrierHook: true)
        )
    }

    func testDirectHookSkipsWithoutRetentionWrite() async throws {
        let empty = try await makeClient()
        let emptyResult = try await RepoCheckpointBarrierHook(
            services: try await makeServices(client: empty, policy: policy()),
            month: month
        ).run()
        XCTAssertEqual(emptyResult.outcome, .skippedEmptyFold)
        let emptyRetentionCount = await retentionFiles(empty).count
        XCTAssertEqual(emptyRetentionCount, 0)

        let below = try await makeClient()
        try await writeAddCommit(client: below, seq: 1, clock: 1, assetByte: 0xA1)
        let belowResult = try await RepoCheckpointBarrierHook(
            services: try await makeServices(
                client: below,
                policy: policy(checkpointCommitThreshold: 10)
            ),
            month: month
        ).run()
        XCTAssertEqual(belowResult.outcome, .skippedBelowThreshold)
        XCTAssertNil(belowResult.barrier)
        guard case .preflightBlocked(let blockers, _)? = belowResult.deleteResult else {
            return XCTFail("expected empty retention preflight block, got \(String(describing: belowResult.deleteResult))")
        }
        XCTAssertTrue(blockers.contains(.emptyBarrierSet))
        let belowRetentionCount = await retentionFiles(below).count
        XCTAssertEqual(belowRetentionCount, 0)
    }

    func testDirectHookWritesAcceptedCheckpointAndVerifiedBarrier() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB1)

        let result = try await RepoCheckpointBarrierHook(
            services: try await makeServices(
                client: client,
                policy: policy(checkpointCommitThreshold: 1)
            ),
            month: month
        ).run()

        XCTAssertEqual(result.outcome, .checkpointWrittenBarrierPublished)
        let checkpointName = try XCTUnwrap(result.checkpoint.snapshotName)
        let barrier = try XCTUnwrap(result.barrier)
        XCTAssertEqual(barrier.manifest.checkpointSnapshotName, checkpointName)
        XCTAssertEqual(barrier.manifest.coveredRanges, result.checkpoint.covered)
        XCTAssertEqual(barrier.manifest.repoID, repoID)
        XCTAssertEqual(barrier.manifest.createdByWriterID, writerID)
        XCTAssertEqual(barrier.manifest.runID, UUID(uuidString: runID))
        XCTAssertEqual(barrier.manifest.deletePrefixByWriter, policy().conservativeDeletePrefixByWriter(covered: result.checkpoint.covered))
        let retentionCount = await retentionFiles(inner).count
        XCTAssertEqual(retentionCount, 1)
        XCTAssertEqual(client.deleteCount(), 0)
        guard case .preflightBlocked(let blockers, _)? = result.deleteResult else {
            return XCTFail("expected fresh barrier preflight block, got \(String(describing: result.deleteResult))")
        }
        XCTAssertTrue(blockers.contains {
            if case .barrierTooYoung = $0 { return true }
            return false
        })

        let materialized = try await RepoMaterializer(client: inner, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)
        XCTAssertEqual(materialized.acceptedSnapshotBaselinesByMonth[month]?.filename, checkpointName)
        let loaded = try await RetentionManifestRemoteStore(client: inner, basePath: basePath)
            .loadBarrierSet(expectedRepoID: repoID, month: month)
        XCTAssertTrue(loaded.isComplete)
        XCTAssertTrue(loaded.barrierSet.unionCovered.superset(of: result.checkpoint.covered))
    }

    func testDirectHookMapsAlreadyExistingBarrierManifestOutcome() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        client.reportExistingMetadata(containing: RepoLayout.retentionDirectoryPath(base: basePath))
        client.alreadyExistAtomicCreate(containing: RepoLayout.retentionDirectoryPath(base: basePath))
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB2)

        let result = try await RepoCheckpointBarrierHook(
            services: try await makeServices(
                client: client,
                policy: policy(checkpointCommitThreshold: 1)
            ),
            month: month
        ).run()

        XCTAssertEqual(result.outcome, .checkpointWrittenBarrierAlreadyExisted)
        XCTAssertEqual(result.barrier?.writeOutcome, .alreadyExistedSameBytes)
        let retentionCount = await retentionFiles(inner).count
        XCTAssertEqual(retentionCount, 1)
    }

    func testProductionDefaultHookDeletesAuthorizedCommitPrefixAfterBarrier() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB3)

        let result = try await RepoCheckpointBarrierHook(
            services: try await makeServices(
                client: client,
                policy: policy(
                    checkpointCommitThreshold: 0,
                    retentionStalenessThresholdSeconds: 0
                )
            ),
            month: month
        ).run()

        XCTAssertEqual(result.outcome, .checkpointWrittenBarrierPublished)
        guard case .completed(let summary, _, .passed(_))? = result.deleteResult else {
            return XCTFail("expected completed commit-prefix deletion, got \(String(describing: result.deleteResult))")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        let commitExists = await client.hasFile(commitPath)
        XCTAssertFalse(commitExists)
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)
        XCTAssertEqual(materialized.acceptedSnapshotBaselinesByMonth[month]?.filename, result.checkpoint.snapshotName)
    }

    func testProductionDefaultHookTreatsPreflightBlockedDeletionAsNonFatal() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB4)

        let result = try await RepoCheckpointBarrierHook(
            services: try await makeServices(
                client: client,
                policy: policy(checkpointCommitThreshold: 0)
            ),
            month: month
        ).run()

        XCTAssertEqual(result.outcome, .checkpointWrittenBarrierPublished)
        guard case .preflightBlocked(let blockers, _)? = result.deleteResult else {
            return XCTFail("expected preflight blocked deletion, got \(String(describing: result.deleteResult))")
        }
        XCTAssertTrue(blockers.contains {
            if case .barrierTooYoung = $0 { return true }
            return false
        })
        XCTAssertEqual(client.deleteCount(), 0)
    }

    func testHookRetriesAgedBarrierDeletionWhenCheckpointBelowThreshold() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB5)
        let services = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let result = try await RepoCheckpointBarrierHook(services: services, month: month).run()

        XCTAssertEqual(result.outcome, .skippedBelowThreshold)
        guard case .completed(let summary, _, .passed(_))? = result.deleteResult else {
            return XCTFail("expected aged barrier deletion on skipped checkpoint, got \(String(describing: result.deleteResult))")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        let commitExists = await client.hasFile(commitPath)
        XCTAssertFalse(commitExists)
    }

    func testStartupMaintenanceDeletesInactiveMonthWithAgedBarrier() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB6)
        let services = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let results = try await RepoRetentionStartupMaintenance(
            services: services,
            nowMs: { 120_000 }
        ).run()

        guard case .completed(let summary, _, .passed(_))? = results[month] else {
            return XCTFail("expected startup maintenance deletion, got \(String(describing: results[month]))")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        let commitExists = await client.hasFile(commitPath)
        XCTAssertFalse(commitExists)
    }

    func testDirectHookPropagatesCheckpointFailureWithoutBarrier() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xC1)
        let client = HookTestRemoteClient(inner: inner)
        client.failAtomicCreate(containing: RepoLayout.snapshotsDirectoryPath(base: basePath), afterMatches: 0)

        do {
            _ = try await RepoCheckpointBarrierHook(
                services: try await makeServices(
                    client: client,
                    policy: policy(checkpointCommitThreshold: 1)
                ),
                month: month
            ).run()
            XCTFail("expected checkpoint write failure")
        } catch {
            XCTAssertFalse(error is CancellationError)
        }
        let retentionCount = await retentionFiles(inner).count
        XCTAssertEqual(retentionCount, 0)
    }

    func testDirectHookPropagatesBarrierPublishFailureAfterCheckpoint() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xC2)
        client.failAtomicCreate(containing: RepoLayout.retentionDirectoryPath(base: basePath), afterMatches: 0)

        do {
            _ = try await RepoCheckpointBarrierHook(
                services: try await makeServices(
                    client: client,
                    policy: policy(checkpointCommitThreshold: 1)
                ),
                month: month
            ).run()
            XCTFail("expected barrier publish failure")
        } catch {
            XCTAssertFalse(error is CancellationError)
        }

        let retentionCount = await retentionFiles(inner).count
        let snapshotCount = await snapshotFiles(inner).count
        XCTAssertEqual(retentionCount, 0)
        XCTAssertEqual(snapshotCount, 1)
    }

    func testDirectHookPropagatesMetadataReadRaceAndCancellationAsTypedErrors() async throws {
        let raceInner = try await makeClient()
        let raceClient = HookTestRemoteClient(inner: raceInner)
        let fakePath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 9)
        raceClient.setListHook(path: RepoLayout.commitsDirectoryPath(base: basePath)) { path, inner in
            var entries = try await inner.list(path: path)
            entries.append(RemoteStorageEntry(
                path: fakePath,
                name: (fakePath as NSString).lastPathComponent,
                isDirectory: false,
                size: 12,
                creationDate: nil,
                modificationDate: nil
            ))
            return entries
        }

        do {
            _ = try await RepoCheckpointBarrierHook(
                services: try await makeServices(
                    client: raceClient,
                    policy: policy(checkpointCommitThreshold: 0)
                ),
                month: month
            ).run()
            XCTFail("expected MetadataReadRaceError")
        } catch RepoMaterializer.MetadataReadRaceError.requiredCommitVanished(_, let raceMonth, let raceWriter, let seq) {
            XCTAssertEqual(raceMonth, month)
            XCTAssertEqual(raceWriter, writerID)
            XCTAssertEqual(seq, 9)
        }

        let cancelInner = try await makeClient()
        try await writeAddCommit(client: cancelInner, seq: 1, clock: 1, assetByte: 0xD1)
        let cancelClient = HookTestRemoteClient(inner: cancelInner)
        cancelClient.cancelNextAtomicCreate()
        do {
            _ = try await RepoCheckpointBarrierHook(
                services: try await makeServices(
                    client: cancelClient,
                    policy: policy(checkpointCommitThreshold: 1)
                ),
                month: month
            ).run()
            XCTFail("expected CancellationError")
        } catch is CancellationError {
        }
    }

    func testRuntimeHookDefaultOnAndCheckpointPolicyControlsBarrierWrites() async throws {
        let skippedInner = try await makeClient()
        let skippedClient = HookTestRemoteClient(inner: skippedInner)
        let skippedSession = try await makeDirtySession(
            client: skippedClient,
            policy: policy(checkpointCommitThreshold: 1),
            assetByte: 0xE2
        )
        let skippedDelta = try await skippedSession.flushToRemote()
        XCTAssertTrue(skippedDelta.didFlush)
        let skippedRetentionCount = await retentionFiles(skippedInner).count
        let skippedSnapshotCount = await snapshotFiles(skippedInner).count
        XCTAssertEqual(skippedRetentionCount, 0)
        XCTAssertEqual(skippedSnapshotCount, 1)

        let enabledInner = try await makeClient()
        let enabledClient = HookTestRemoteClient(inner: enabledInner)
        let enabledSession = try await makeDirtySession(
            client: enabledClient,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0xE3
        )
        let enabledDelta = try await enabledSession.flushToRemote()
        XCTAssertTrue(enabledDelta.didFlush)
        let enabledRetentionCount = await retentionFiles(enabledInner).count
        let enabledSnapshotCount = await snapshotFiles(enabledInner).count
        XCTAssertEqual(enabledRetentionCount, 1)
        XCTAssertEqual(enabledSnapshotCount, 2)
    }

    func testRuntimeHookSkipsIgnoreCancellationAndCommitOnlyFlush() async throws {
        let ignoreInner = try await makeClient()
        let ignoreClient = HookTestRemoteClient(inner: ignoreInner)
        let ignoreSession = try await makeDirtySession(
            client: ignoreClient,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0xF1
        )
        _ = try await ignoreSession.flushToRemote(ignoreCancellation: true)
        let ignoreRetentionCount = await retentionFiles(ignoreInner).count
        let ignoreSnapshotCount = await snapshotFiles(ignoreInner).count
        XCTAssertEqual(ignoreRetentionCount, 0)
        XCTAssertEqual(ignoreSnapshotCount, 1)

        let commitOnlyInner = try await makeClient()
        let commitOnlyClient = HookTestRemoteClient(inner: commitOnlyInner)
        let commitOnlySession = try await makeDirtySession(
            client: commitOnlyClient,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0xF2
        )
        _ = try await commitOnlySession.commitPendingAssetToRemote(ignoreCancellation: false)
        let commitOnlyRetentionCount = await retentionFiles(commitOnlyInner).count
        let commitOnlySnapshotCount = await snapshotFiles(commitOnlyInner).count
        XCTAssertEqual(commitOnlyRetentionCount, 0)
        XCTAssertEqual(commitOnlySnapshotCount, 0)
    }

    func testRuntimeSessionSnapshotFailurePreventsHook() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        let logger: MonthManifestStepLogger = { [client] line in
            client.logLines(line)
        }
        let (session, fingerprint) = try await makeDirtySessionWithAssetFingerprint(
            client: client,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0x63,
            logLines: logger
        )
        client.failAtomicCreate(containing: RepoLayout.snapshotsDirectoryPath(base: basePath), afterMatches: 0)

        do {
            _ = try await session.flushToRemote()
            XCTFail("expected snapshotWriteFailed")
        } catch let deferred as V2MonthSession.MonthDurableSnapshotDeferred {
            XCTAssertEqual(deferred.delta.committedAssetFingerprints, [fingerprint])
            XCTAssertTrue(deferred.delta.committedTombstoneFingerprints.isEmpty)
            guard case .snapshotWriteFailed = deferred.flushError else {
                return XCTFail("expected snapshotWriteFailed, got \(deferred.flushError)")
            }
        }

        let retentionCount = await retentionFiles(inner).count
        let snapshotCount = await snapshotFiles(inner).count
        let maintenanceLogCount = client.logLines().filter { $0.contains("checkpoint barrier maintenance failed") }.count
        XCTAssertEqual(retentionCount, 0)
        XCTAssertEqual(snapshotCount, 0)
        XCTAssertEqual(maintenanceLogCount, 0)
    }

    func testRuntimeHookFailureLogsAndDoesNotFailFlushButCancellationPropagates() async throws {
        let failingInner = try await makeClient()
        let failingClient = HookTestRemoteClient(inner: failingInner)
        let logger: MonthManifestStepLogger = { [failingClient] line in
            failingClient.logLines(line)
        }
        let failingSession = try await makeDirtySession(
            client: failingClient,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0x61,
            logLines: logger
        )
        failingClient.failAtomicCreate(containing: RepoLayout.snapshotsDirectoryPath(base: basePath), afterMatches: 1)

        let delta = try await failingSession.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        let failingRetentionCount = await retentionFiles(failingInner).count
        let maintenanceLogCount = failingClient.logLines().filter { $0.contains("checkpoint barrier maintenance failed") }.count
        XCTAssertEqual(failingRetentionCount, 0)
        XCTAssertEqual(maintenanceLogCount, 1)

        let cancelInner = try await makeClient()
        let cancelClient = HookTestRemoteClient(inner: cancelInner)
        let cancelSession = try await makeDirtySession(
            client: cancelClient,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0x62
        )
        cancelClient.cancelAtomicCreate(containing: RepoLayout.snapshotsDirectoryPath(base: basePath), afterMatches: 1)

        // U01 R04: cancellation in the post-commit checkpoint barrier now surfaces through
        // `FlushError.snapshotWriteFailed` carrying the durable delta, instead of escaping as
        // a raw `CancellationError`. This lets `flushMonthStorePublishingDefensiveCommits` map it
        // to `.commitDurableSnapshotDeferred` and the executor still runs
        // `applyDurableBatchSideEffects` (intent drain + provisional mark-durable) before the
        // cancellation routes to pause/abort. `FlushError.cancellationCause` still walks the
        // underlying chain and matches the wrapped `CancellationError`, so cancellation is still
        // honored downstream.
        do {
            _ = try await cancelSession.flushToRemote()
            XCTFail("expected MonthDurableSnapshotDeferred wrapping the barrier cancellation")
        } catch let deferred as V2MonthSession.MonthDurableSnapshotDeferred {
            guard case .snapshotWriteFailed(let underlying) = deferred.flushError else {
                XCTFail("expected snapshotWriteFailed, got \(deferred.flushError)")
                return
            }
            XCTAssertTrue(underlying is CancellationError,
                          "underlying of barrier cancellation must remain a CancellationError so downstream cancellationCause walker recognizes it")
            XCTAssertNotNil(deferred.flushError.cancellationCause,
                            "FlushError.cancellationCause must surface the cancellation via the underlying-chain walker")
        }
    }

    func testRuntimeBarrierPublishFailureFailsOpenAfterCheckpoint() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        let logger: MonthManifestStepLogger = { [client] line in
            client.logLines(line)
        }
        let (session, fingerprint) = try await makeDirtySessionWithAssetFingerprint(
            client: client,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0x64,
            logLines: logger
        )
        client.failAtomicCreate(containing: RepoLayout.retentionDirectoryPath(base: basePath), afterMatches: 0)

        let delta = try await session.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [fingerprint])
        let cleanDelta = try await session.flushToRemote()
        XCTAssertFalse(cleanDelta.didFlush)
        let retentionCount = await retentionFiles(inner).count
        let snapshotCount = await snapshotFiles(inner).count
        let maintenanceLogCount = client.logLines().filter { $0.contains("checkpoint barrier maintenance failed") }.count
        XCTAssertEqual(retentionCount, 0)
        XCTAssertEqual(snapshotCount, 2)
        XCTAssertEqual(maintenanceLogCount, 1)
    }

    func testRuntimeHookBarrierFeedsSubsequentBarrierAwareRefresh() async throws {
        let inner = try await makeClient()
        let client = HookTestRemoteClient(inner: inner)
        let firstSession = try await makeDirtySession(
            client: client,
            policy: policy(checkpointCommitThreshold: 0),
            assetByte: 0x65
        )
        _ = try await firstSession.flushToRemote()
        let loaded = try await RetentionManifestRemoteStore(client: inner, basePath: basePath)
            .loadBarrierSet(expectedRepoID: repoID, month: month)
        let barrier = try XCTUnwrap(loaded.valid.first)

        let secondServices = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99)
        )
        let secondSession = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: secondServices
        )
        let rows = makeAssetRows(hashByte: 0x66, name: "after-barrier.jpg")
        _ = try secondSession.upsertResource(rows.resource)
        try secondSession.upsertAsset(rows.asset, links: [rows.link], replacingSubsetFingerprints: [])

        _ = try await secondSession.flushToRemote()

        let commit = try await readCommit(client: inner, seq: 2)
        XCTAssertGreaterThan(commit.header.clockMin, barrier.barrierLamport)
        XCTAssertGreaterThan(client.listCount(path: RepoLayout.retentionDirectoryPath(base: basePath)), 0)
    }

    private func makeDirtySession(
        client: any RemoteStorageClientProtocol,
        policy: RepoCompactionPolicy,
        assetByte: UInt8,
        logLines: MonthManifestStepLogger? = nil
    ) async throws -> V2MonthSession {
        try await makeDirtySessionWithAssetFingerprint(
            client: client,
            policy: policy,
            assetByte: assetByte,
            logLines: logLines
        ).session
    }

    private func makeDirtySessionWithAssetFingerprint(
        client: any RemoteStorageClientProtocol,
        policy: RepoCompactionPolicy,
        assetByte: UInt8,
        logLines: MonthManifestStepLogger? = nil
    ) async throws -> (session: V2MonthSession, fingerprint: AssetFingerprint) {
        let services = try await makeServices(client: client, policy: policy)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services,
            stepLogger: logLines
        )
        let rows = makeAssetRows(hashByte: assetByte, name: "asset-\(assetByte).jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link], replacingSubsetFingerprints: [])
        return (session, rows.asset.assetFingerprint)
    }

    private func makeServices(
        client: any RemoteStorageClientProtocol,
        policy: RepoCompactionPolicy
    ) async throws -> BackupV2RuntimeServices {
        let dbURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("checkpoint-barrier-\(UUID().uuidString).sqlite")
        let database = try DatabaseManager(databaseURL: dbURL)
        let profileID = try TestFixtures.insertServerProfile(
            in: database,
            writerID: writerID,
            basePath: basePath,
            storageType: .webdav
        )
        let identity = RepoIdentity(database: database)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: 0)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: database,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: CommitLogWriter(client: client, basePath: basePath),
            snapshotWriter: SnapshotWriter(client: client, basePath: basePath),
            liveness: LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true),
            compactionPolicy: policy,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func writeAddCommit(
        client: any RemoteStorageClientProtocol,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let hash = TestFixtures.fingerprint(assetByte)
        let body = CommitAddAssetBody(
            assetFingerprint: assetFingerprint(hash: hash),
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: []
        )
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: month,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: true
        )
    }

    private func makeAssetRows(hashByte: UInt8, name: String) -> (
        asset: RemoteManifestAsset,
        resource: RemoteManifestResource,
        link: RemoteAssetResourceLink
    ) {
        let hash = TestFixtures.fingerprint(hashByte)
        let fp = assetFingerprint(hash: hash)
        let path = "2026/05/\(name)"
        return (
            RemoteManifestAsset(
                year: month.year,
                month: month.month,
                assetFingerprint: fp,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 1,
                totalFileSizeBytes: 100
            ),
            RemoteManifestResource(
                year: month.year,
                month: month.month,
                physicalRemotePath: path,
                contentHash: hash,
                fileSize: 100,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 0
            ),
            RemoteAssetResourceLink(
                year: month.year,
                month: month.month,
                assetFingerprint: fp,
                resourceHash: hash,
                role: ResourceTypeCode.photo,
                slot: 0,
                logicalName: name
            )
        )
    }

    private func assetFingerprint(hash: Data) -> AssetFingerprint {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
    }

    private func retentionFiles(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter { $0.key.contains("/.watermelon/retention/") }
    }

    private func snapshotFiles(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter { $0.key.contains("/.watermelon/snapshots/") }
    }

    private func readCommit(client: InMemoryRemoteStorageClient, seq: UInt64) async throws -> CommitFile {
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: seq)
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: path, localURL: temp)
        return try CommitLogReader.parse(localURL: temp)
    }

    @discardableResult
    private func writeCheckpointBarrier(
        client: InMemoryRemoteStorageClient,
        services: BackupV2RuntimeServices,
        createdAtMs: Int64
    ) async throws -> RepoRetentionBarrierPublishResult {
        let checkpoint = try await RepoCheckpointService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            clock: services.lamport,
            policy: services.compactionPolicy
        ).checkpointMonth(month, mode: .force, respectTaskCancellation: true)
        return try await RepoRetentionBarrierService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            policy: services.compactionPolicy,
            nowMs: { createdAtMs }
        ).publishBarrier(for: checkpoint, respectTaskCancellation: true)
    }

    private func policy(
        checkpointCommitThreshold: Int = 1,
        retentionStalenessThresholdSeconds: Int = 86_400
    ) -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: checkpointCommitThreshold,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: retentionStalenessThresholdSeconds,
            snapshotFallbackKeepCount: 2
        )
    }

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
}

private final class HookTestRemoteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias ListHook = @Sendable (_ path: String, _ inner: InMemoryRemoteStorageClient) async throws -> [RemoteStorageEntry]
    typealias DownloadHook = @Sendable (_ path: String, _ localURL: URL, _ inner: InMemoryRemoteStorageClient) async throws -> Void

    private enum Injected: Error {
        case nonCancellation
    }

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var listHooks: [String: ListHook] = [:]
    private var downloadHooks: [String: DownloadHook] = [:]
    private var downloadSubstringHooks: [(substring: String, hook: DownloadHook)] = []
    private var listCounts: [String: Int] = [:]
    private var deleteCalls = 0
    private var cancelNextCreate = false
    private var failingAtomicCreateSubstring: (substring: String, afterMatches: Int, cancellation: Bool)?
    private var alreadyExistsAtomicCreateSubstrings: [String] = []
    private var existingMetadataSubstrings: [String] = []
    private var logs: [String] = []

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { false }
    nonisolated var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func setListHook(path: String, hook: @escaping ListHook) {
        lock.withLock { listHooks[Self.normalize(path)] = hook }
    }

    func setDownloadHook(path: String, hook: @escaping DownloadHook) {
        lock.withLock { downloadHooks[Self.normalize(path)] = hook }
    }

    func setDownloadHook(containing substring: String, hook: @escaping DownloadHook) {
        lock.withLock { downloadSubstringHooks.append((substring, hook)) }
    }

    func cancelNextAtomicCreate() {
        lock.withLock { cancelNextCreate = true }
    }

    func failAtomicCreate(containing substring: String, afterMatches: Int) {
        lock.withLock {
            failingAtomicCreateSubstring = (substring, afterMatches, false)
        }
    }

    func cancelAtomicCreate(containing substring: String, afterMatches: Int) {
        lock.withLock {
            failingAtomicCreateSubstring = (substring, afterMatches, true)
        }
    }

    func alreadyExistAtomicCreate(containing substring: String) {
        lock.withLock {
            alreadyExistsAtomicCreateSubstrings.append(Self.normalize(substring))
        }
    }

    func reportExistingMetadata(containing substring: String) {
        lock.withLock {
            existingMetadataSubstrings.append(Self.normalize(substring))
        }
    }

    func listCount(path: String) -> Int {
        lock.withLock { listCounts[Self.normalize(path)] ?? 0 }
    }

    func deleteCount() -> Int {
        lock.withLock { deleteCalls }
    }

    func logLines(_ line: String) {
        lock.withLock { logs.append(line) }
    }

    func logLines() -> [String] {
        lock.withLock { logs }
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let key = Self.normalize(path)
        let hook = lock.withLock { () -> ListHook? in
            listCounts[key, default: 0] += 1
            return listHooks.removeValue(forKey: key)
        }
        if let hook {
            return try await hook(key, inner)
        }
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let key = Self.normalize(path)
        let shouldReportExisting = lock.withLock { () -> Bool in
            guard let index = existingMetadataSubstrings.firstIndex(where: { key.contains($0) }) else {
                return false
            }
            existingMetadataSubstrings.remove(at: index)
            return true
        }
        if shouldReportExisting {
            return RemoteStorageEntry(
                path: key,
                name: (key as NSString).lastPathComponent,
                isDirectory: false,
                size: 1,
                creationDate: nil,
                modificationDate: nil
            )
        }
        return try await inner.metadata(path: path)
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        let key = Self.normalize(remotePath)
        let action = lock.withLock { () -> Bool? in
            if cancelNextCreate {
                cancelNextCreate = false
                return true
            }
            guard var failing = failingAtomicCreateSubstring, key.contains(failing.substring) else {
                return nil
            }
            if failing.afterMatches > 0 {
                failing.afterMatches -= 1
                failingAtomicCreateSubstring = failing
                return nil
            }
            failingAtomicCreateSubstring = nil
            return failing.cancellation
        }
        if action == true {
            throw CancellationError()
        } else if action == false {
            throw Injected.nonCancellation
        }
        let shouldAlreadyExist = lock.withLock { () -> Bool in
            guard let index = alreadyExistsAtomicCreateSubstrings.firstIndex(where: { key.contains($0) }) else {
                return false
            }
            alreadyExistsAtomicCreateSubstrings.remove(at: index)
            return true
        }
        if shouldAlreadyExist {
            let data = try Data(contentsOf: localURL)
            await inner.injectFile(path: key, data: data)
            return .alreadyExists
        }
        return try await inner.atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let hook = lock.withLock { () -> DownloadHook? in
            if let exact = downloadHooks.removeValue(forKey: key) {
                return exact
            }
            guard let index = downloadSubstringHooks.firstIndex(where: { key.contains($0.substring) }) else {
                return nil
            }
            return downloadSubstringHooks.remove(at: index).hook
        }
        if let hook {
            try await hook(key, localURL, inner)
            return
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }

    func delete(path: String) async throws {
        lock.withLock { deleteCalls += 1 }
        throw Injected.nonCancellation
    }

    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}
