import Foundation
import XCTest
@testable import Watermelon

final class RepoSnapshotDeletePreflightTests: XCTestCase {

    // MARK: - Early-stage preflight blockers

    func testBlocksWhenVersionMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.missingVersion))
    }

    func testBlocksWhenBarrierSetEmpty() async throws {
        let client = try await makeReadyClient()

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.emptyBarrierSet))
    }

    // MARK: - Pre-delete barrier checkpoint evidence (R02 reviewer finding)

    /// Tampered/missing retained checkpoint snapshot must block BEFORE planning deletion.
    func testBlocksWhenRetainedBarrierCheckpointHasMismatchedSHA() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        // Publish a barrier with a wrong checkpoint SHA — preflight must catch this
        // before any snapshot delete candidate is considered.
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: String(repeating: "f", count: 64),
            createdAtMs: 0,
            snapshotName: nil
        )

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .barrierCheckpointMismatch(_, .sha256) = blocker { return true }
            return false
        }, "expected barrierCheckpointMismatch(.sha256), got \(blockers)")
    }

    /// Missing retained checkpoint snapshot file must block pre-delete.
    func testBlocksWhenRetainedBarrierCheckpointFileMissing() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        // Publish a barrier whose checkpoint file we then delete from the remote.
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )
        let snapshotPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: snapshotLamport,
            writerID: writerID,
            runID: runID
        )
        try await client.delete(path: snapshotPath)

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .barrierCheckpointReadFailed = blocker { return true }
            return false
        }, "expected barrierCheckpointReadFailed, got \(blockers)")
    }

    // MARK: - Barrier-attested observed-seq regression (R02 reviewer finding)

    /// Materializer observed seq must not be below any retained barrier's
    /// `observedSeqHighByWriter[writer]`; otherwise snapshot GC blocks.
    func testBlocksWhenBarrierObservedSeqExceedsMaterializerView() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        // Barrier claims writer observed seq 99, but no commits exist with that seq → materializer
        // observed seq is 0, so preflight must block on regression.
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil,
            observedSeqHighByWriter: [writerID: 99]
        )

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .barrierObservedSeqRegression(_, 99, _) = blocker { return true }
            return false
        }, "expected barrierObservedSeqRegression, got \(blockers)")
    }

    // MARK: - Cross-repo / no-baseline blockers (planner-required coverage)

    func testBlocksWhenNoAcceptedPerMonthSnapshotAndNoCrossRepoBaseline() async throws {
        let client = try await makeReadyClient()
        // No snapshot, no commits → materialize returns empty per-month accepted baseline.
        // Publish a barrier so we get past the empty-barrier-set blocker.
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )
        // Use a different month for the preflight target — no baseline exists for it.
        let otherMonth = LibraryMonthKey(year: 2026, month: 6)

        let result = try await preflight(client: client).makePlan(
            month: otherMonth,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        // No barrier for that month → empty barrier set short-circuits first.
        XCTAssertTrue(blockers.contains(.emptyBarrierSet))
    }

    // MARK: - Cross-repo index baseline wins (R04 reviewer finding)

    /// When a cross-repo index file at a higher lamport wins the materializer's
    /// tiebreak, snapshot GC must block with `.crossRepoIndexBaselineActive` —
    /// cross-repo-winner months are explicitly out of U04 scope.
    func testBlocksWhenCrossRepoIndexBaselineWinsForTargetMonth() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )
        // Materialize so we can hand a real MaterializeOutput to the cross-repo writer.
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        // Cross-repo index at lamport=10 — strictly newer than the per-month snapshot
        // at lamport=5, so it wins the materializer's lex-max tiebreak.
        try await client.createDirectory(path: RepoLayout.indexDirectoryPath(base: basePath))
        _ = try await RepoCrossRepoIndexWriter(client: client, basePath: basePath).write(
            materialized: materialized,
            expectedRepoID: repoID,
            writerID: writerID,
            runID: runID,
            lamport: snapshotLamport + 5,
            respectTaskCancellation: false
        )

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .crossRepoIndexBaselineActive(let m) = blocker, m == month { return true }
            return false
        }, "expected .crossRepoIndexBaselineActive, got \(blockers)")
    }

    // MARK: - No accepted per-month snapshot (R04 reviewer finding)

    /// Barrier evidence passes (SHA matches), but the materializer rejects the
    /// referenced snapshot due to a poisoned row stamp. Preflight must reach the
    /// `.noAcceptedPerMonthSnapshot` branch.
    func testBlocksWhenMaterializerRejectsBarrierSnapshotLeavingNoAcceptedBaseline() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // Snapshot at lamport=5 with a row stamp whose clock (999) exceeds the
        // filename lamport — materializer rejects via `snapshotHasUnworkableRowStamp`,
        // but `SnapshotReader.read` succeeds so barrier-checkpoint-evidence passes.
        let poisoned = try await writeSnapshotWithCustomResources(
            client: client,
            covered: covered,
            lamport: snapshotLamport,
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: "2026/05/in-month-but-poisoned.bin",
                    contentHash: TestFixtures.fingerprint(0xCD),
                    fileSize: 1,
                    resourceType: 0,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    crypto: nil,
                    stamp: OpStamp(writerID: writerID, seq: 1, clock: 999)
                )
            ]
        )
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: poisoned.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .noAcceptedPerMonthSnapshot(let m) = blocker, m == month { return true }
            return false
        }, "expected .noAcceptedPerMonthSnapshot, got \(blockers)")
    }

    // MARK: - Post-delete verifier: per-month → cross-repo transition (R04 reviewer)

    /// If the active baseline transitions from per-month-snapshot (pre-delete contract)
    /// to cross-repo-index (post-delete view), the verifier must fail with the typed
    /// `.crossRepoIndexBaselineActivated` reason.
    func testPostDeleteVerifierFailsOnPerMonthToCrossRepoBaselineTransition() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        try await client.createDirectory(path: RepoLayout.indexDirectoryPath(base: basePath))
        _ = try await RepoCrossRepoIndexWriter(client: client, basePath: basePath).write(
            materialized: materialized,
            expectedRepoID: repoID,
            writerID: writerID,
            runID: runID,
            lamport: snapshotLamport + 5,
            respectTaskCancellation: false
        )

        // Pre-delete contract pretends the per-month snapshot was the active baseline.
        let contract = RepoSnapshotPostDeleteEquivalenceContract(
            acceptedSnapshotFilename: RepoLayout.snapshotFileName(
                month: month,
                lamport: snapshotLamport,
                writerID: writerID,
                runID: runID
            ),
            acceptedSnapshotLamport: snapshotLamport,
            acceptedSnapshotSHA256Hex: snapshot.sha256Hex.lowercased(),
            acceptedSnapshotCovered: covered,
            retainedBarrierUnionCovered: covered,
            retainedManifestCheckpointSHA256ByFilename: [:],
            requiredObservedSeqByWriter: [:],
            preDeleteCovered: covered,
            preDeleteState: .empty,
            preDeleteObservedClock: 0
        )

        let result = await RepoSnapshotPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: month, expectedRepoID: repoID, contract: contract)

        guard case .failed(let reason, _) = result else {
            return XCTFail("expected failed verification, got \(result)")
        }
        if case .crossRepoIndexBaselineActivated(let m) = reason {
            XCTAssertEqual(m, month)
        } else {
            XCTFail("expected .crossRepoIndexBaselineActivated, got \(reason)")
        }
    }

    // MARK: - Materializer-equivalent body trust (R03 reviewer finding)

    /// Snapshot bodies whose rows the materializer would reject must block the whole
    /// month rather than become delete candidates. Out-of-month resource path is the
    /// canonical materializer-rejection case: header matches but a resource row's
    /// `physicalRemotePath` belongs to another month.
    func testBlocksWhenOlderCandidateHasOutOfMonthResourcePath() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // Accepted baseline at lamport=5 (matches the aged barrier).
        let newer = try await writeSnapshot(client: client, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )
        // Older snapshot at lamport=3 — header validates, but its resource row's
        // physicalRemotePath belongs to month 2026/06 instead of the target 2026/05.
        try await writeSnapshotWithCustomResources(
            client: client,
            covered: covered,
            lamport: 3,
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: "2026/06/out-of-month-file.bin",
                    contentHash: TestFixtures.fingerprint(0xAB),
                    fileSize: 1,
                    resourceType: 0,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    crypto: nil,
                    stamp: OpStamp(writerID: writerID, seq: 1, clock: 1)
                )
            ]
        )

        let executor = RepoSnapshotDeleteExecutor(
            client: client,
            basePath: basePath,
            policy: policy(),
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )

        let result = try await executor.execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .preflightBlocked(let blockers, _) = result else {
            return XCTFail("expected preflightBlocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .candidateCorruptOrUntrusted = blocker { return true }
            return false
        }, "expected candidateCorruptOrUntrusted blocker, got \(blockers)")
        // No deletion happened.
        let olderPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let olderExists = await client.hasFile(olderPath)
        XCTAssertTrue(olderExists, "older untrusted snapshot must NOT be deleted")
    }

    // MARK: - Happy path (executor + verifier integration)

    /// End-to-end: with two snapshots (older lamport=3, newer lamport=5 = accepted baseline)
    /// and a published aged barrier referencing the newer one, snapshot GC must delete the
    /// older snapshot and verification must pass.
    func testExecutorDeletesOlderSnapshotAndVerificationPasses() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // Older snapshot at lamport=3.
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 3)
        // Newer snapshot at lamport=5 (matches `snapshotLamport`, which the barrier references).
        let newer = try await writeSnapshot(client: client, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil
        )

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            minimumCheckpointIntervalSeconds: 0,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: client,
            basePath: basePath,
            policy: executorPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )

        let result = try await executor.execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .completed(let summary, _, .passed) = result else {
            return XCTFail("expected completed snapshot GC, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 1)
        XCTAssertEqual(summary.deleted.first?.lamport, 3)
        let olderPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let newerPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: snapshotLamport,
            writerID: writerID,
            runID: runID
        )
        let olderExists = await client.hasFile(olderPath)
        let newerExists = await client.hasFile(newerPath)
        XCTAssertFalse(olderExists)
        XCTAssertTrue(newerExists)
    }

    // MARK: - Fixtures

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let nowMs: Int64 = 120_000
    private let snapshotLamport: UInt64 = 5

    private func policy() -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            minimumCheckpointIntervalSeconds: 0,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 2
        )
    }

    private func preflight(client: any RemoteStorageClientProtocol) -> RepoSnapshotDeletePreflightService {
        RepoSnapshotDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: policy(),
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )
    }

    private func makeReadyClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.retentionDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        return client
    }

    private func makeCovered(seqs: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            writerID: seqs.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    @discardableResult
    private func writeSnapshot(
        client: any RemoteStorageClientProtocol,
        covered: CoveredRanges,
        lamport: UInt64? = nil
    ) async throws -> SnapshotFile {
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
        let rows = RepoSnapshotBuilder.build(header: header, state: .empty)
        return try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: rows.assets,
            resources: rows.resources,
            assetResources: rows.assetResources,
            deletedKeys: rows.deletedKeys,
            month: month,
            lamport: lamport ?? snapshotLamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    /// Build a snapshot file with caller-supplied resource rows (so callers can
    /// inject materializer-rejecting shapes like out-of-month resource paths).
    @discardableResult
    private func writeSnapshotWithCustomResources(
        client: any RemoteStorageClientProtocol,
        covered: CoveredRanges,
        lamport: UInt64,
        resources: [SnapshotResourceRow]
    ) async throws -> SnapshotFile {
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
        return try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: [],
            resources: resources,
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private func writeBarrier(
        client: InMemoryRemoteStorageClient,
        covered: CoveredRanges,
        checkpointSHA256Hex: String,
        createdAtMs: Int64,
        snapshotName: String? = nil,
        observedSeqHighByWriter: [String: UInt64]? = nil
    ) async throws {
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerID,
            runID: UUID(uuidString: runID)!,
            createdAtMs: createdAtMs,
            barrierLamport: snapshotLamport,
            checkpointSnapshotName: snapshotName ?? RepoLayout.snapshotFileName(
                month: month,
                lamport: snapshotLamport,
                writerID: writerID,
                runID: runID
            ),
            checkpointSHA256Hex: checkpointSHA256Hex,
            coveredRanges: covered,
            deletePrefixByWriter: policy().conservativeDeletePrefixByWriter(covered: covered),
            observedSeqHighByWriter: observedSeqHighByWriter ?? covered.rangesByWriter.mapValues { ranges in
                ranges.map(\.high).max() ?? 0
            },
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: policy().snapshotFallbackKeepCount
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 0
            )
        )
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref),
            data: try RetentionManifestStore.encode(manifest)
        )
    }
}
