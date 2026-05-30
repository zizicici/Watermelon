import XCTest
@testable import Watermelon

/// U01-BatchCommit200 contract tests:
///   - hard cap on commit-file size (≤ BackupV2Constants.batchFlushInterval ops per file)
///   - per-chunk `recordCommitted(seq:)` so snapshot covered ranges include every chunk
///   - `containsDurableAssetFingerprint` rejects in-session pending V2 rows
///   - `V2MonthIndexes.recordCommit` clears only the stamped pending fingerprints
final class V2BatchCommitTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-batch-test"
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

    // MARK: - B3-1 (Battle Revision 3) — snapshot covered must include every chunk seq

    func testChunkedDrain_201Pending_ProducesTwoCommitFiles_AndSnapshotCoversBoth() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval

        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }

        _ = try await store.flushToRemote(ignoreCancellation: false)

        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 2,
                       "cap+1 pending must split into two commit files (cap + 1)")
        XCTAssertGreaterThanOrEqual(counts.snapshots, 1, "snapshot must land at least once after drain")

        // Materialized state must include every fingerprint, and covered must span both chunks.
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertEqual(monthState.assets.count, cap + 1)
        let covered = output.coveredByMonth[monthKey] ?? .empty
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 1))
        XCTAssertTrue(covered.contains(writerID: writerID, seq: 2))
    }

    func testChunkedDrain_AdjacentSeqsMergeIntoContiguousCoveredRange() async throws {
        // After a 3-chunk drain (cap=200, 600 pending), the resulting commit-file seqs (1,2,3)
        // must merge into a single covered range so snapshot/retention reasoning is correct.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval
        let total = cap * 3
        for index in 0 ..< total {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }

        _ = try await store.flushToRemote(ignoreCancellation: false)

        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 3, "3 * cap pending must split into exactly 3 commit files")

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let covered = output.coveredByMonth[monthKey] ?? .empty
        for seq in 1...3 {
            XCTAssertTrue(covered.contains(writerID: writerID, seq: UInt64(seq)),
                          "covered must include every chunk seq (got missing seq \(seq))")
        }
    }

    // MARK: - B2-1 — recordCommit clears only stamped pending fingerprints

    func testRecordCommit_chunkedFlush_leftoverPendingSurvivesFirstCommit() async throws {
        // Direct V2MonthCommitFlusher loop with `limit:` to verify the indexes' pending set
        // shrinks chunk-by-chunk, not all-at-once. Past behaviour (`pending*.removeAll()`)
        // would drop the cap-th entry from memory after the first commit and corrupt this test.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }

        // First chunk: commit `cap` ops. flushToRemote loops internally; we verify with the side
        // effect (commit-file count + materialized asset count).
        _ = try await store.flushToRemote(ignoreCancellation: false)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 2,
                       "201 pending must produce 2 commit files (cap + 1) — if recordCommit clears all pending after chunk 1, the 201st row would be lost")
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertEqual(monthState.assets.count, cap + 1,
                       "all 201 fingerprints must be present in materialized state")
    }

    // MARK: - R1 — `containsDurableAssetFingerprint`

    func testContainsDurableAssetFingerprint_pendingV2RowIsNotDurable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let rows = makeAssetRows(index: 0)
        _ = try store.upsertResource(rows.resource)
        try store.upsertAsset(rows.asset, links: [rows.link])

        XCTAssertTrue(store.containsAssetFingerprint(rows.asset.assetFingerprint),
                      "containsAssetFingerprint reflects in-memory pending state")
        XCTAssertFalse(store.containsDurableAssetFingerprint(rows.asset.assetFingerprint),
                       "containsDurableAssetFingerprint must reject the pending row until commit lands")

        _ = try await store.flushToRemote(ignoreCancellation: false)

        XCTAssertTrue(store.containsDurableAssetFingerprint(rows.asset.assetFingerprint),
                      "after flush, the fingerprint is durable")
    }

    // MARK: - Hash-index intent queue shape (B2-2)

    func testIntentQueue_DuplicateFingerprintAcrossLocalIdentifiers_BothDrained() async throws {
        let queue = PendingHashIndexIntentQueue()
        let fp = TestFixtures.assetFingerprint(0xCC)
        let intentA = HashIndexUpsertIntent(
            assetLocalIdentifier: "local-A",
            assetFingerprint: fp,
            totalFileSizeBytes: 1,
            modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        )
        let intentB = HashIndexUpsertIntent(
            assetLocalIdentifier: "local-B",
            assetFingerprint: fp,
            totalFileSizeBytes: 1,
            modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        )
        await queue.enqueue(month: monthKey, intent: intentA)
        await queue.enqueue(month: monthKey, intent: intentB)
        let before = await queue.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(before, 2,
                       "queue must hold one intent per (fingerprint, localIdentifier) — flat keying loses one")

        let drained = await queue.drain(month: monthKey, durableAssetFingerprints: [fp])
        let drainedLocalIDs = Set(drained.map(\.assetLocalIdentifier))
        XCTAssertEqual(drainedLocalIDs, ["local-A", "local-B"],
                       "drain must surface every local-identifier intent under a durable fingerprint")
        let after = await queue.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(after, 0)
    }

    func testIntentQueue_ReenqueueSameLocalIdentifier_KeepsLatest() async throws {
        let queue = PendingHashIndexIntentQueue()
        let fp = TestFixtures.assetFingerprint(0xDD)
        let first = HashIndexUpsertIntent(
            assetLocalIdentifier: "local-X",
            assetFingerprint: fp,
            totalFileSizeBytes: 1,
            modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        )
        let second = HashIndexUpsertIntent(
            assetLocalIdentifier: "local-X",
            assetFingerprint: fp,
            totalFileSizeBytes: 2,
            modificationDateMs: 42,
            body: .fingerprintOnly(resourceCount: 2)
        )
        await queue.enqueue(month: monthKey, intent: first)
        await queue.enqueue(month: monthKey, intent: second)
        let count = await queue.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(count, 1, "same (fp, localID) must collapse to latest")

        let drained = await queue.drain(month: monthKey, durableAssetFingerprints: [fp])
        XCTAssertEqual(drained.count, 1)
        XCTAssertEqual(drained.first?.totalFileSizeBytes, 2,
                       "second enqueue wins")
    }

    func testIntentQueue_RollBackDiscardsForFingerprints() async throws {
        let queue = PendingHashIndexIntentQueue()
        let fp1 = TestFixtures.assetFingerprint(0xE1)
        let fp2 = TestFixtures.assetFingerprint(0xE2)
        await queue.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "a", assetFingerprint: fp1,
            totalFileSizeBytes: 1, modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        ))
        await queue.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "b", assetFingerprint: fp2,
            totalFileSizeBytes: 1, modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        ))

        await queue.rollBack(month: monthKey, fingerprints: [fp1])
        let after = await queue.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(after, 1, "rollBack must drop fp1's bucket and keep fp2")

        let remainingDrained = await queue.drain(month: monthKey, durableAssetFingerprints: [fp1, fp2])
        XCTAssertEqual(remainingDrained.count, 1, "fp1 was rolled back so only fp2 remains for drain")
        XCTAssertEqual(remainingDrained.first?.assetFingerprint, fp2)
    }

    // MARK: - Aggregator status-aware reconciliation (B2-3)

    func testAggregator_RecordProvisional_ThenMarkBatchDurable_LeavesCountersUnchanged() async {
        let aggregator = ParallelBackupProgressAggregator(total: 3)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "a",
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .skipped, reason: nil, displayName: "b",
            assetFingerprint: TestFixtures.assetFingerprint(0x02),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: TestFixtures.assetFingerprint(0x01),
            assetLocalIdentifier: "local-a", status: .success
        )
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: TestFixtures.assetFingerprint(0x02),
            assetLocalIdentifier: "local-b", status: .skipped
        )
        await aggregator.markBatchDurable(
            month: monthKey,
            committedAssetFingerprints: [TestFixtures.assetFingerprint(0x01), TestFixtures.assetFingerprint(0x02)]
        )

        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 1)
        XCTAssertEqual(state.skipped, 1)
        XCTAssertEqual(state.failed, 0,
                       "markBatchDurable must not change counters; reconciliation is for rollback only")
        let pending = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(pending, 0, "markBatchDurable empties the matched provisional entries")
    }

    func testAggregator_RollBackProvisionalBatch_DecrementsPerRecordedStatus() async {
        let aggregator = ParallelBackupProgressAggregator(total: 3)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "a",
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .skipped, reason: nil, displayName: "b",
            assetFingerprint: TestFixtures.assetFingerprint(0x02),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: TestFixtures.assetFingerprint(0x01),
            assetLocalIdentifier: "local-a", status: .success
        )
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: TestFixtures.assetFingerprint(0x02),
            assetLocalIdentifier: "local-b", status: .skipped
        )

        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertEqual(rolledBack, [TestFixtures.assetFingerprint(0x01), TestFixtures.assetFingerprint(0x02)])

        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 0, "succeeded count must revert for rolled-back success")
        XCTAssertEqual(state.skipped, 0, "skipped count must revert for rolled-back skip")
        XCTAssertEqual(state.failed, 2, "rollback bumps failed by the size of the in-flight batch")
        let pending = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(pending, 0, "buffer cleared after rollback")
    }

    // U01 fixer R02: rollback must decrement once per (fingerprint, assetLocalIdentifier) cell.
    // The aggregator's prior `[Data: Status]` shape collapsed duplicate-fingerprint local assets
    // into one entry, so the rollback would under-revert by one. The hash-index intent queue
    // already preserves both rows (B2-2); this test pins the matching shape on the aggregator.
    func testAggregator_RollBackProvisionalBatch_DuplicateFingerprintDistinctLocalIdentifiers_DecrementsBoth() async {
        let aggregator = ParallelBackupProgressAggregator(total: 4)
        let sharedFingerprint = TestFixtures.assetFingerprint(0xAB)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "dup-a",
            assetFingerprint: sharedFingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .skipped, reason: nil, displayName: "dup-b",
            assetFingerprint: sharedFingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: sharedFingerprint,
            assetLocalIdentifier: "local-dup-a", status: .success
        )
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: sharedFingerprint,
            assetLocalIdentifier: "local-dup-b", status: .skipped
        )

        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertEqual(rolledBack, [sharedFingerprint],
                       "return value still groups by fingerprint — hash-index intent rollback dedupes per fp")

        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 0,
                       "succeeded must decrement for the success-status local asset under the shared fingerprint")
        XCTAssertEqual(state.skipped, 0,
                       "skipped must decrement for the skipped-status local asset under the shared fingerprint")
        XCTAssertEqual(state.failed, 2,
                       "failed must bump by the number of provisional cells rolled back (2), not by unique fingerprints (1)")
    }

    // Bug-IX P01 R01 Finding 1: when a same-batch subset-tombstone lands durably (commit op
    // `tombstoneAsset(F_A)` co-resident with `addAsset(F_B)`), the aggregator buffer must clear
    // F_A as well as F_B. Without this, a later hard-abort would revert F_A's provisional success
    // into a spurious `failed` even though F_A's durable outcome is "row absent, tombstone stamped,
    // bytes preserved via F_B".
    func testAggregator_MarkBatchDurable_WithTombstones_ClearsTombstonedBuffer() async {
        let aggregator = ParallelBackupProgressAggregator(total: 2)
        let fpA = TestFixtures.assetFingerprint(0xAA)
        let fpB = TestFixtures.assetFingerprint(0xBB)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "a",
            assetFingerprint: fpA,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "b",
            assetFingerprint: fpB,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpA,
            assetLocalIdentifier: "local-a", status: .success
        )
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpB,
            assetLocalIdentifier: "local-b", status: .success
        )
        // Commit lands with addAsset(F_B) and tombstoneAsset(F_A).
        await aggregator.markBatchDurable(
            month: monthKey,
            committedAssetFingerprints: [fpB],
            committedTombstoneFingerprints: [fpA]
        )

        let pending = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(pending, 0,
                       "tombstoned fingerprint F_A must be cleared from the buffer once its tombstone commit lands")

        // A later hard-abort must NOT revert F_A or F_B — both are durable.
        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertTrue(rolledBack.isEmpty,
                      "no provisional cells should survive past the durable commit of F_A's tombstone + F_B's add")
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 2,
                       "succeeded counters must stay; durable tombstone is a valid outcome for F_A")
        XCTAssertEqual(state.failed, 0,
                       "tombstoned-but-durable F_A must not flip into failed on a later rollback")
    }

    // Bug-IX P01 R04 Claude B Finding 1: when an upsertAsset(B, replacingSubsetFingerprints={F_A})
    // tombstones F_A in-memory, F_A's earlier provisional success record (for A_localID) must be
    // cleared once F_B's `addAsset` op commits durably — even if F_A's `tombstoneAsset` op never
    // reaches a durable commit (e.g. multi-chunk drain whose tombstone-only chunk fails on
    // connection loss). Without the carrier→subsets cascade, a subsequent hard-abort rollback
    // would revert A_localID's provisional success into a spurious `failed` despite F_B carrying
    // A's bytes durably.
    func testAggregator_MarkBatchDurable_TombstoneOpDeferred_CarrierCascadesToSubset() async {
        let aggregator = ParallelBackupProgressAggregator(total: 2)
        let fpA = TestFixtures.assetFingerprint(0xA1)
        let fpB = TestFixtures.assetFingerprint(0xB2)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "a",
            assetFingerprint: fpA,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "b",
            assetFingerprint: fpB,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        // Asset A processed first with no subset replacement.
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpA,
            assetLocalIdentifier: "local-a", status: .success
        )
        // Asset B's `upsertAsset(B, replacingSubsetFingerprints={fpA})` tombstones fpA in-memory.
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpB,
            assetLocalIdentifier: "local-b", status: .success,
            tombstonedSubsets: [fpA]
        )

        // The same-batch tombstone op falls into a later chunk that fails — only the chunk
        // carrying B's addAsset commits durably. Outcome shape: adds={fpB}, tombstones=[].
        await aggregator.markBatchDurable(
            month: monthKey,
            committedAssetFingerprints: [fpB],
            committedTombstoneFingerprints: []
        )

        let pending = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(pending, 0,
                       "carrier→subsets cascade must clear fpA even when its tombstone op didn't land durably")

        // A subsequent hard-abort must NOT revert A_localID — F_B carries A's bytes durably.
        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertTrue(rolledBack.isEmpty,
                      "no provisional cells should survive past the carrier's durable commit")
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 2,
                       "succeeded counters stay put; A_localID's bytes are reachable via F_B")
        XCTAssertEqual(state.failed, 0,
                       "A_localID must not flip into failed on a hard-abort after F_B's durable commit")
    }

    // Bug-IX P01 R04 Claude B Finding 1 — companion: if the carrier itself fails (no chunk lands),
    // BOTH F_A and F_B must roll back. The carrier→subsets map must not pre-clear F_A in the
    // all-fail path.
    func testAggregator_AllChunksFail_RollBackRevertsBothCarrierAndSubset() async {
        let aggregator = ParallelBackupProgressAggregator(total: 2)
        let fpA = TestFixtures.assetFingerprint(0xA3)
        let fpB = TestFixtures.assetFingerprint(0xB4)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "a",
            assetFingerprint: fpA,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "b",
            assetFingerprint: fpB,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpA,
            assetLocalIdentifier: "local-a", status: .success
        )
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: fpB,
            assetLocalIdentifier: "local-b", status: .success,
            tombstonedSubsets: [fpA]
        )

        // No durable commit at all (e.g. the single-chunk flush failed entirely before any op
        // reached the remote).
        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertEqual(rolledBack, [fpA, fpB],
                       "both carrier and its subset must roll back when nothing committed")
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, 0,
                       "succeeded counters must revert when no commit landed for either fingerprint")
        XCTAssertEqual(state.failed, 2,
                       "both A_localID and B_localID must flip into failed under the all-fail path")
    }

    // U01 fixer R01: multi-chunk partial-fail must surface chunk-1 durability so downstream can
    // drain intents + publish the durable sweep. Without surfacing, the executor's hard-abort
    // catch would roll back chunk-1's fingerprints and discard their hash-index intents despite
    // chunk-1 being durable on the remote.
    func testMultiChunkDrain_Chunk2Fails_SurfacesChunk1AsSnapshotWriteFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Exclusive guarantee writes directly to the final commit-file path (no staging move), so
        // an upload-error injection on the final path is sufficient.
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }

        // Inject upload failure for chunk 2's commit-file path (seq=2). The flusher allocates one
        // seq per chunk so chunk 1 lands at seq=1 (the failure is keyed by the chunk-2 path).
        let chunk2Path = RepoLayout.commitFilePath(
            base: basePath,
            month: monthKey,
            writerID: writerID,
            seq: 2
        )
        await client.injectUploadError(.transport, for: chunk2Path)

        do {
            _ = try await store.flushToRemote(ignoreCancellation: false)
            XCTFail("flush must surface a snapshotWriteFailed once chunk 2 fails")
        } catch let deferred as V2MonthSession.MonthDurableSnapshotDeferred {
            guard case .snapshotWriteFailed = deferred.flushError else {
                XCTFail("expected FlushError.snapshotWriteFailed, got \(deferred.flushError)")
                return
            }
            let assets = deferred.delta.committedAssetFingerprints
            let tombstones = deferred.delta.committedTombstoneFingerprints
            XCTAssertEqual(assets.count, cap,
                           "deferred delta must carry chunk 1's fingerprints (\(cap)) — \(assets.count) seen")
            XCTAssertTrue(tombstones.isEmpty, "no tombstones in this scenario")
        } catch {
            XCTFail("expected V2MonthSession.MonthDurableSnapshotDeferred, got \(type(of: error)): \(error)")
            return
        }

        // Chunk 1 is durable on the remote — replay must show its fingerprints.
        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = output.state.months[monthKey] ?? .empty
        XCTAssertEqual(monthState.assets.count, cap,
                       "chunk 1's \(cap) fingerprints must be materializable from the durable commit")
    }

    // U01 fixer R02-1: after a partial multi-chunk failure, the in-memory state still carries the
    // uncommitted chunk-N+1 rows. `publishDefensiveFlushSnapshotIfNeeded` must NOT publish that
    // state into the committed view — only durable rows should reach `RemoteIndexSyncService`.
    func testFlushMonthStorePublishingDefensiveCommits_PartialMultiChunk_DoesNotPublishUncommittedRows() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval
        // Seed pending so chunk 1 fits exactly the cap, chunk 2 carries 1 leftover.
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }
        let chunk2Path = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        await client.injectUploadError(.transport, for: chunk2Path)

        let remoteIndex = RemoteIndexSyncService()
        let outcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store,
            month: monthKey,
            remoteIndexService: remoteIndex,
            ignoreCancellation: false
        )
        guard case .commitDurableSnapshotDeferred(let delta, _) = outcome else {
            XCTFail("expected commitDurableSnapshotDeferred outcome, got \(outcome)")
            return
        }
        XCTAssertEqual(delta.committedAssetFingerprints.count, cap,
                       "delta must carry exactly chunk 1's fingerprints (cap=\(cap))")
        // The conservative R02 fix skips publish entirely when V2 still has pending ops. The
        // committed view therefore stays at its initial (empty) state — chunk-2's fingerprint
        // must NOT appear, and chunk-1's fingerprints likewise stay absent until a subsequent
        // successful flush publishes the now-durable state.
        let safeToSkip = remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()
        XCTAssertNil(safeToSkip[monthKey],
                     "publishDefensiveFlushSnapshotIfNeeded must NOT publish month state when V2 has uncommitted ops; the committed view has no entry for this month")
        XCTAssertTrue(store.hasUncommittedV2Ops,
                      "after partial multi-chunk failure, hasUncommittedV2Ops must report the chunk-N+1 remainder")
    }

    // U01 fixer R02-1 negative path: a full-drain flush (no leftover pending) must continue to
    // publish via the existing `publishDefensiveFlushSnapshotIfNeeded` route, so consumers can see
    // the now-durable rows in the same session. Pins that the R02 gate doesn't break the happy path.
    func testFlushMonthStorePublishingDefensiveCommits_NoPartial_PublishesCommittedSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        for index in 0 ..< 3 {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
        }
        let remoteIndex = RemoteIndexSyncService()
        _ = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store,
            month: monthKey,
            remoteIndexService: remoteIndex,
            ignoreCancellation: false
        )
        XCTAssertFalse(store.hasUncommittedV2Ops, "all pending was drained")
        let safeToSkip = remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()
        XCTAssertEqual(safeToSkip[monthKey]?.count, 3,
                       "non-partial success must publish the full durable snapshot")
    }

    // U01 fixer R02-2: aggregator-level shape — after marking chunk 1's fingerprints durable and
    // accumulating one extra chunk-2 record, rollBackProvisionalBatch must revert ONLY chunk 2's
    // record (chunk 1 was already cleared by markBatchDurable). Pins the executor-side contract
    // that the rollback after partial multi-chunk reverts only the leftover provisional cells.
    func testAggregator_MarkBatchDurableThenRollBack_OnlyChunk2RecordReverts() async {
        let aggregator = ParallelBackupProgressAggregator(total: 5)
        let chunk1Fingerprints: [AssetFingerprint] = (0..<4).map { TestFixtures.assetFingerprint(UInt8($0 + 0x40)) }
        // Per-asset progress + per-asset provisional record for chunk 1.
        for fp in chunk1Fingerprints {
            _ = await aggregator.record(result: AssetProcessResult(
                status: .success, reason: nil, displayName: "asset-\(fp)",
                assetFingerprint: fp,
                timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
            ))
            await aggregator.recordProvisional(
                month: monthKey,
                fingerprint: fp,
                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(fp)"),
                status: .success
            )
        }
        // Chunk 1's batch commit lands — markBatchDurable clears those entries; counters unchanged.
        await aggregator.markBatchDurable(
            month: monthKey,
            committedAssetFingerprints: Set(chunk1Fingerprints)
        )
        // Now chunk 2 starts: one extra provisional record.
        let chunk2Fingerprint = TestFixtures.assetFingerprint(0x99)
        _ = await aggregator.record(result: AssetProcessResult(
            status: .skipped, reason: nil, displayName: "chunk2",
            assetFingerprint: chunk2Fingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: chunk2Fingerprint,
            assetLocalIdentifier: "local-chunk2", status: .skipped
        )

        let rolledBack = await aggregator.rollBackProvisionalBatch(month: monthKey)
        XCTAssertEqual(rolledBack, [chunk2Fingerprint],
                       "rollback after partial multi-chunk must revert ONLY chunk 2's leftover")
        let state = await aggregator.snapshot()
        XCTAssertEqual(state.succeeded, chunk1Fingerprints.count,
                       "chunk 1's succeeded counter must remain — its commit landed")
        XCTAssertEqual(state.skipped, 0, "chunk 2's skipped record was reverted")
        XCTAssertEqual(state.failed, 1, "rollback bumps failed by chunk 2's one record")
    }

    // U01 fixer R03 (checker finding): the durable cached-skip short-circuit
    // (`reason == "asset_exists_cached"`) writes its hash-index row inline, does NOT call
    // `finalizeRowWritingAsset`, and does NOT enqueue a V2 intent. The executor must therefore
    // skip `recordProvisional` for that result so a later batch rollback can't revert it.
    // Pins the typed flag `wroteProvisionalV2Row` differentiates these results.
    func testAssetProcessResult_CachedDurableSkip_DoesNotMarkProvisionalRow() {
        // Mirror the `asset_exists_cached` construction site in AssetProcessor.processWithLocalCache.
        let cachedSkip = AssetProcessResult(
            status: .skipped, reason: "asset_exists_cached", displayName: "x",
            assetFingerprint: TestFixtures.assetFingerprint(0xC1),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0
        )
        XCTAssertFalse(cachedSkip.wroteProvisionalV2Row,
                       "asset_exists_cached must default to wroteProvisionalV2Row=false")
        // Mirror the cache-reuse construction site (resources_reused_cached) which DOES call
        // finalizeRowWritingAsset and DOES enqueue a V2 intent.
        let cacheReuse = AssetProcessResult(
            status: .skipped, reason: "resources_reused_cached", displayName: "y",
            assetFingerprint: TestFixtures.assetFingerprint(0xC2),
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 0,
            wroteProvisionalV2Row: true
        )
        XCTAssertTrue(cacheReuse.wroteProvisionalV2Row,
                      "resources_reused_cached must opt in to wroteProvisionalV2Row=true")
    }

    // U01 fixer R03-1 (reviewer A + B): the interval-fail path's rollback was deferred to allow
    // the trailing forced final flush (paused → ignoreCancellation=true) to still commit pending
    // V2 ops. Pin the contract by simulating the executor's intent + provisional bookkeeping
    // around a partial multi-chunk failure followed by a recovering retry: chunk-2's intent and
    // provisional record MUST survive the first flush's `snapshotWriteFailed` outcome and only
    // be reconciled when the retry commits them durably.
    func testPartialMultiChunkOutcome_DoesNotDiscardChunk2Intent_RetryDrainsIt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let cap = BackupV2Constants.batchFlushInterval

        // Seed (cap + 1) pending V2 ops — first chunk fits cap, second chunk carries 1 leftover.
        var allFingerprints: [AssetFingerprint] = []
        for index in 0 ..< (cap + 1) {
            let rows = makeAssetRows(index: index)
            _ = try store.upsertResource(rows.resource)
            try store.upsertAsset(rows.asset, links: [rows.link])
            allFingerprints.append(rows.asset.assetFingerprint)
        }

        // Simulate the executor recording intents + provisional rows for every asset before the
        // flush. Use a fresh AssetProcessor's intent queue + an aggregator initialized to total =
        // cap + 1.
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService()
        )
        let aggregator = ParallelBackupProgressAggregator(total: cap + 1)
        for (index, fp) in allFingerprints.enumerated() {
            await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(index)"), assetFingerprint: fp,
                totalFileSizeBytes: 1, modificationDateMs: nil,
                body: .fingerprintOnly(resourceCount: 1)
            ))
            _ = await aggregator.record(result: AssetProcessResult(
                status: .success, reason: nil, displayName: "asset-\(index)",
                assetFingerprint: fp,
                timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
            ))
            await aggregator.recordProvisional(
                month: monthKey, fingerprint: fp,
                assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: "local-\(index)"), status: .success
            )
        }
        let initialIntents = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        XCTAssertEqual(initialIntents, cap + 1, "intents primed for every asset")

        // First flush fails chunk 2 — `flushMonthStorePublishingDefensiveCommits` returns
        // `commitDurableSnapshotDeferred` with chunk-1 delta. R03: this must NOT discard chunk-2's
        // intent or provisional record. The executor's `applyDurableBatchSideEffects` only drains
        // intents for the outcome.delta (chunk 1).
        let chunk2Path = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        await client.injectUploadError(.transport, for: chunk2Path)

        let remoteIndex = RemoteIndexSyncService()
        let firstOutcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store, month: monthKey, remoteIndexService: remoteIndex, ignoreCancellation: false
        )
        guard case .commitDurableSnapshotDeferred(let firstDelta, _) = firstOutcome else {
            XCTFail("expected commitDurableSnapshotDeferred after chunk-2 upload error")
            return
        }
        await BackupParallelExecutor.applyDurableBatchSideEffects(
            aggregator: aggregator,
            assetProcessor: processor,
            month: monthKey,
            outcome: firstOutcome,
            eventStream: BackupEventStream(),
            profile: TestFixtures.makeServerProfile(storageType: .webdav),
            workerID: 1
        )
        // Chunk 1 drained from intent queue + provisional buffer; chunk 2 still queued.
        let intentsAfterFirst = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfterFirst = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterFirst, (cap + 1) - firstDelta.committedAssetFingerprints.count,
                       "applyDurableBatchSideEffects drains only chunk-1's intents — chunk 2 must remain queued (R03 invariant)")
        XCTAssertEqual(provisionalAfterFirst, intentsAfterFirst,
                       "provisional buffer shrinks by the same amount as the intent queue")
        XCTAssertTrue(store.hasUncommittedV2Ops, "chunk 2 is still pending in V2MonthIndexes")

        // The retry (simulating the paused EOM flush with ignoreCancellation=true) clears the
        // chunk-2 upload error and commits the leftover. `applyDurableBatchSideEffects` for the
        // second outcome drains the chunk-2 intent, completing the reconciliation.
        let secondOutcome = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
            monthStore: store, month: monthKey, remoteIndexService: remoteIndex, ignoreCancellation: true
        )
        guard case .completed(let secondDelta) = secondOutcome else {
            XCTFail("expected the retry to complete cleanly, got \(secondOutcome)")
            return
        }
        XCTAssertFalse(store.hasUncommittedV2Ops, "retry drained the chunk-2 leftover")
        await BackupParallelExecutor.applyDurableBatchSideEffects(
            aggregator: aggregator,
            assetProcessor: processor,
            month: monthKey,
            outcome: secondOutcome,
            eventStream: BackupEventStream(),
            profile: TestFixtures.makeServerProfile(storageType: .webdav),
            workerID: 1
        )
        let intentsAfterRetry = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfterRetry = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfterRetry, 0, "retry drained chunk 2's intent")
        XCTAssertEqual(provisionalAfterRetry, 0, "retry cleared chunk 2 from provisional buffer")
        XCTAssertEqual(secondDelta.committedAssetFingerprints.count, 1,
                       "retry committed the cap+1th fingerprint as its single chunk")

        // Final counters: all (cap + 1) succeeded; no rollback, no failed bump.
        let finalState = await aggregator.snapshot()
        XCTAssertEqual(finalState.succeeded, cap + 1,
                       "all assets stay counted as succeeded — deferred rollback prevented spurious failures")
        XCTAssertEqual(finalState.failed, 0, "no rollback ran, no failed bump")
    }

    // U01 fixer R05: V2 `finalizeRowWritingAsset` calls `optimisticWriter.appendAsset` BEFORE the
    // batch commit lands, mutating `RemoteIndexSyncService.committedView` for same-session worker
    // visibility. On hard abort (no further flush will commit those fingerprints), the optimistic
    // rows must not keep surfacing through `remoteMonthRawData(for:)` /
    // `resumeSafeToSkipAssetFingerprintsByMonth()`. The fix wires
    // `RemoteIndexSyncService.dropOptimisticMonthIfStale(month:)` into
    // `rollBackProvisionalAndIntentsForHardAbort` (and the background runner's intent-clear
    // sites), so the in-process committed view drops the failed month's stale rows.
    func testHardAbortRollback_DropsUncommittedOptimisticMonthFromCommittedView() async throws {
        let remoteIndex = RemoteIndexSyncService()
        // Seed an optimistic resource + asset (mirrors what `AssetProcessor.uploadResource` +
        // `finalizeRowWritingAsset` do per-asset under U01, before the batch commit lands).
        let rows = makeAssetRows(index: 0)
        let writer = remoteIndex.makeOptimisticAssetWriter()
        writer.appendResource(rows.resource)
        writer.appendAsset(rows.asset, links: [rows.link])

        XCTAssertEqual(
            remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            [rows.asset.assetFingerprint],
            "the per-asset optimistic append must surface to in-session resume-skip predicates"
        )

        // Simulate the executor's hard-abort rollback. The R05 fix calls
        // dropOptimisticMonthIfStale from inside `rollBackProvisionalAndIntentsForHardAbort`;
        // here we hit the public surface directly to keep the test focused.
        remoteIndex.dropOptimisticMonthIfStale(month: monthKey)

        XCTAssertNil(
            remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            "after a hard abort, the uncommitted optimistic asset rows must be cleared from committedView; consumers must NOT keep seeing the non-durable fingerprint"
        )
        XCTAssertNil(
            remoteIndex.remoteMonthRawData(for: monthKey),
            "remoteMonthRawData should be empty for the dropped month (or the month entirely absent)"
        )
    }

    // Arch-VII A-II B3: `MonthOverlayCoordinator.onHardAbort(month:)` must produce the exact same
    // observable overlay state as a bare `dropOptimisticMonthIfStale(month:)` — a thin
    // behavior-preserving indirection, not a new operation.
    func testMonthOverlayCoordinator_onHardAbort_dropsUncommittedOptimisticMonth() async throws {
        let remoteIndex = RemoteIndexSyncService()
        let rows = makeAssetRows(index: 0)
        let writer = remoteIndex.makeOptimisticAssetWriter()
        writer.appendResource(rows.resource)
        writer.appendAsset(rows.asset, links: [rows.link])
        XCTAssertEqual(
            remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            [rows.asset.assetFingerprint],
            "precondition: optimistic append visible before onHardAbort"
        )

        MonthOverlayCoordinator(remoteIndexService: remoteIndex).onHardAbort(month: monthKey)

        XCTAssertNil(
            remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
            "onHardAbort must drop the month exactly like dropOptimisticMonthIfStale"
        )
        XCTAssertNil(
            remoteIndex.remoteMonthRawData(for: monthKey),
            "onHardAbort must leave the dropped month absent from remoteMonthRawData"
        )
    }

    // U01 fixer R05 — executor wiring: `rollBackProvisionalAndIntentsForHardAbort` MUST call
    // `dropOptimisticMonthIfStale` alongside the existing aggregator + intent-queue cleanup.
    // This pins that the helper is the single reconciliation point so all hard-abort callers
    // benefit without per-call-site editing.
    func testRollBackProvisionalAndIntentsForHardAbort_AlsoDropsOptimisticMonth() async throws {
        // Build the same shape the executor maintains: aggregator with a provisional record,
        // intent queue with one queued intent, optimistic view holding the appendAsset row.
        let remoteIndex = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndex
        )
        let aggregator = ParallelBackupProgressAggregator(total: 1)
        let rows = makeAssetRows(index: 0)
        let writer = remoteIndex.makeOptimisticAssetWriter()
        writer.appendResource(rows.resource)
        writer.appendAsset(rows.asset, links: [rows.link])
        await processor.pendingHashIndexIntents.enqueue(month: monthKey, intent: HashIndexUpsertIntent(
            assetLocalIdentifier: "local-0", assetFingerprint: rows.asset.assetFingerprint,
            totalFileSizeBytes: 1, modificationDateMs: nil,
            body: .fingerprintOnly(resourceCount: 1)
        ))
        _ = await aggregator.record(result: AssetProcessResult(
            status: .success, reason: nil, displayName: "asset-0",
            assetFingerprint: rows.asset.assetFingerprint,
            timing: AssetProcessTiming(), totalFileSizeBytes: 1, uploadedFileSizeBytes: 1
        ))
        await aggregator.recordProvisional(
            month: monthKey, fingerprint: rows.asset.assetFingerprint,
            assetLocalIdentifier: "local-0", status: .success
        )
        XCTAssertNotNil(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
                        "preconditions: optimistic asset visible before rollback")

        await BackupParallelExecutor.rollBackProvisionalAndIntentsForHardAbort(
            aggregator: aggregator,
            assetProcessor: processor,
            month: monthKey
        )

        let intentsAfter = await processor.pendingHashIndexIntents.pendingFingerprintCountForTest(month: monthKey)
        let provisionalAfter = await aggregator.provisionalCountForTest(month: monthKey)
        XCTAssertEqual(intentsAfter, 0, "hash-index intents rolled back")
        XCTAssertEqual(provisionalAfter, 0, "provisional buffer rolled back")
        XCTAssertNil(remoteIndex.resumeSafeToSkipAssetFingerprintsByMonth()[monthKey],
                     "R05: optimistic month overlay also dropped — the helper is the single reconciliation point")
    }

    // MARK: - Helpers

    private func makeV2Services(client: InMemoryRemoteStorageClient) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let liveness = LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true)
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
            liveness: liveness,
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private func makeAssetRows(index: Int) -> (
        asset: RemoteManifestAsset,
        resource: RemoteManifestResource,
        link: RemoteAssetResourceLink
    ) {
        // Distinct fingerprints + paths per index so subset-collapse cannot interfere.
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
            year: year, month: month,
            assetFingerprint: assetFingerprint,
            creationDateMs: 1_700_000_000_000 + Int64(index),
            backedUpAtMs: 1_700_000_001_000 + Int64(index),
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0,
            logicalName: "asset-\(index).jpg"
        )
        return (asset, resource, link)
    }

    private func repoMetadataCounts(_ client: InMemoryRemoteStorageClient) async -> (commits: Int, snapshots: Int) {
        let files = await client.snapshotFiles().keys
        return (
            files.filter { $0.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/") }.count,
            files.filter { $0.hasPrefix(RepoLayout.snapshotsDirectoryPath(base: basePath) + "/") }.count
        )
    }
}
