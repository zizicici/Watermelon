import XCTest
@testable import Watermelon

final class RepoCompactionServiceTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-compaction-test"
    private let year = 2026
    private let monthValue = 5
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: monthValue) }
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

    // MARK: - Snapshot GC disposition tests

    func testStartupCompactionSkipsSnapshotGC() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactStartupMonths()

        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "startup compaction must process the month with checkpoint-eligible commits")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedDisabled))
    }

    func testReportOnlyEntryReturnsSkippedReportOnly() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .reportSnapshotGC()

        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "report-only entry must process the month with checkpoint-eligible commits")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedReportOnly))
    }

    func testUserMaintenanceEntryDoesNotSkipDisabled() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        // User maintenance uses .always, so it should NOT be .skippedDisabled.
        if case .skipped(.skippedDisabled) = result.snapshotGC {
            XCTFail("user maintenance entry must not use .skippedDisabled")
        }
    }

    // MARK: - Commit GC authority tests

    func testCommitGCRunsAfterCheckpointWithPostCheckpointAccepted() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // Checkpoint must succeed and commit GC must run (non-nil).
        XCTAssertEqual(result.outcome, .checkpointWritten)
        XCTAssertNotNil(result.commitCleanup,
            "commit GC must run when post-checkpoint accepted is present")
    }

    func testCompactMonthWithoutCheckpointRecommendationStillAttemptsCommitGC() async throws {
        // Baseline covers seq 1-3; only 1 replay commit — below default threshold of 5.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 1)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // R7-2: commit GC is no longer gated on a fresh checkpoint — it runs off the existing
        // accepted baseline. Here there are no deletable commit files, so it blocks rather than skips.
        XCTAssertNotEqual(result.outcome, .checkpointWritten)
        let cleanup = try XCTUnwrap(result.commitCleanup,
            "commit GC must run even when checkpoint is not recommended")
        guard case .preflightBlocked = cleanup else {
            return XCTFail("expected commit GC to find no deletable candidates, got \(cleanup)")
        }
    }

    func testStartupCompactionRunsCommitGCOnResidualPrefixWithoutFreshCheckpoint() async throws {
        // Phase 1: 6 replay commits cross the threshold, writing a checkpoint covering [1,9].
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let service = RepoCompactionService(services: services)
        _ = try await service.compactMonth(monthKey)

        // Phase 2: no new commits, so no fresh checkpoint is recommended, but the accepted
        // snapshot now dominates residual commit files. R7-2 must still select the month via
        // checkpointCoveredPrefixCandidateCount and delete the residual prefix.
        let result = try await service.compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "startup compaction must select the month via covered-prefix candidates without a fresh checkpoint")
        XCTAssertNotEqual(monthResult.outcome, .checkpointWritten,
            "phase 2 must not write a fresh checkpoint")
        let cleanup = try XCTUnwrap(monthResult.commitCleanup,
            "commit GC must run on the residual deletable prefix")
        guard case .completed(let summary, _, _) = cleanup else {
            return XCTFail("expected residual commit GC to complete, got \(cleanup)")
        }
        XCTAssertFalse(summary.deleted.isEmpty, "residual commits must actually be deleted")
    }

    // MARK: - Materialize reuse

    func testStartupCompactionReusesInitialMaterializeOutput() async throws {
        // Accepted baseline [1,3] + one below-threshold commit outside the delete prefix ⇒ zero
        // candidate months, so candidate selection is the only materialize work in startup.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 1)
        let snapshotsDir = RepoLayout.snapshotsDirectoryPath(base: basePath)

        // Without reuse: the planner runs its own full materialize on top of its own listing.
        let servicesNoBox = try await makeServices(client: client)
        let before0 = await client.listAttemptCount(for: snapshotsDir)
        _ = try await RepoCompactionService(services: servicesNoBox).compactStartupMonths()
        let listsWithoutReuse = await client.listAttemptCount(for: snapshotsDir) - before0

        // With reuse: the box-supplied materialize is passed through, so only the planner's own
        // single listing touches the snapshots directory.
        let preMaterialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        let servicesWithBox = try await makeServices(client: client, initialMaterialize: preMaterialized)
        let before1 = await client.listAttemptCount(for: snapshotsDir)
        _ = try await RepoCompactionService(services: servicesWithBox).compactStartupMonths()
        let listsWithReuse = await client.listAttemptCount(for: snapshotsDir) - before1

        XCTAssertEqual(listsWithReuse, 1,
            "candidate selection must list snapshots once (planner only, no second full materialize)")
        XCTAssertLessThan(listsWithReuse, listsWithoutReuse,
            "reusing initialMaterializeOutput must avoid the planner's second full materialize")
    }

    func testCorruptBaselineSkipsCompactionAndCommitGC() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Write a corrupt snapshot (not valid jsonl) so materialize outcome is .corrupt.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))

        // Write commits so the month has content.
        for i in 0..<6 {
            let seq = UInt64(1 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // Corrupt outcome → compaction skipped entirely, commit GC did not run.
        XCTAssertNotEqual(result.outcome, .checkpointWritten)
        XCTAssertNil(result.commitCleanup,
            "commit GC must not run on corrupt baseline")
    }

    // MARK: - Helpers

    /// Writes an accepted snapshot covering seq [1,3] then `replayCount` additional commits
    /// at seq 4..(4+replayCount-1). Default threshold is 5, so replayCount >= 5 triggers recommendation.
    private func makeClientWithBaselineAndCommits(replayCount: Int) async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Post-delete verification reads the remote identity, so a realistic repo needs both files.
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // Accepted baseline snapshot covering seq [1,3].
        let covered = CoveredRanges(rangesByWriter: [
            writerID: [ClosedSeqRange(low: 1, high: 3)]
        ])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: .empty)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: true
        )

        // Additional commits beyond the baseline covered range.
        for i in 0..<replayCount {
            let seq = UInt64(4 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        return client
    }

    private func writeAddCommit(
        client: InMemoryRemoteStorageClient,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let assetFP = TestFixtures.assetFingerprint(assetByte)
        let hash = TestFixtures.fingerprint(assetByte &+ 1)
        let resources = [CommitResourceEntry(
            physicalRemotePath: String(format: "%04d/%02d/asset-%02x.jpg", year, monthValue, assetByte),
            logicalName: "asset.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )]
        let op = CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: assetFP,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: resources
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: monthKey,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [op],
            month: monthKey,
            respectTaskCancellation: true
        )
    }

    private static let testPolicy = RepoCompactionPolicy(
        checkpointCommitThreshold: 5,
        checkpointByteThreshold: Int64.max,
        snapshotFallbackKeepCount: 2
    )

    private func makeServices(
        client: InMemoryRemoteStorageClient,
        initialMaterialize: RepoMaterializer.MaterializeOutput? = nil
    ) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
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
            compactionPolicy: Self.testPolicy,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(initialMaterialize),
        )
    }
}
