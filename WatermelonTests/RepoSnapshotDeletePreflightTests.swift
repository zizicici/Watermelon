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

    // Bug-IX P01 R01 Codex A Finding 1: month-local partial-migration marker must block snapshot
    // GC even after the central marker has been cleared by phase-3 cleanup.
    func testBlocksWhenMonthPartialMigrationMarkerPresent() async throws {
        let client = try await makeReadyClient()
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, V1MigrationResidueFileNames.partialMigrationMarkerFileName)
        )
        await client.injectFile(path: markerPath, contents: "{}")

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.migrationResiduePresent(month: month)),
                      "month-local partial migration marker must block snapshot GC after central markers are cleared")
    }

    // Bug-IX P01 R04 Codex A Finding 1: a directory squatting at the reserved month-local
    // partial-migration marker path is damaged remote state, not proof that the marker is absent.
    // Snapshot GC must fail-closed before any irreversible snapshot deletion runs.
    func testMonthPartialMigrationMarkerDirectorySentinelFailsClosed() async throws {
        let client = try await makeReadyClient()
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, V1MigrationResidueFileNames.partialMigrationMarkerFileName)
        )
        try await client.createDirectory(path: markerPath)

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.migrationResidueCheckFailed(month: month)),
                      "directory-shaped month-local marker must fail-closed via migrationResidueCheckFailed")
        XCTAssertFalse(blockers.contains(.migrationResiduePresent(month: month)),
                       "directory shape isn't a marker present claim; it's uncertain corrupt state")
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
            additionalProtectedSnapshotSHA256ByFilename: [:],
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

    // Bug-IX P01 R08 Codex A F2: a directory squatting at a target-month snapshot filename is
    // damaged remote state, not proof that the snapshot is absent. Snapshot GC must fail-closed
    // via `.unparseableSnapshotPresent(filename:)` before deleting any other older snapshot for
    // the same month — parallel to the file-shaped unparseable handling.
    func testDirectoryAtTargetMonthSnapshotFilenameFailsClosed() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // Accepted baseline + barrier so deletion could otherwise proceed.
        let accepted = try await writeSnapshot(client: client, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: accepted.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil,
            snapshotKeepCount: 0
        )
        // An older deletable snapshot the GC plan would otherwise schedule for delete.
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 3)
        // A directory entry whose name parses as a target-month snapshot — damaged remote state.
        let damagedName = RepoLayout.snapshotFileName(
            month: month,
            lamport: 4,
            writerID: writerID,
            runID: "44444444-4444-4444-4444-444444444444"
        )
        let damagedPath = RemotePathBuilder.absolutePath(
            basePath: RepoLayout.snapshotsDirectoryPath(base: basePath),
            remoteRelativePath: damagedName
        )
        try await client.createDirectory(path: damagedPath)

        let result = try await preflight(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked, got \(result)")
        }
        XCTAssertTrue(
            blockers.contains(.unparseableSnapshotPresent(filename: damagedName)),
            "snapshot GC must fail-closed on a directory entry at a target-month snapshot filename"
        )
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
        // Barrier policy.snapshotKeepCount=1 must agree with the executor's
        // snapshotFallbackKeepCount=1; the preflight uses max() of the two so a barrier with a
        // higher keep count would protect the older snapshot.
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotName: nil,
            snapshotKeepCount: 1
        )

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
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

    // Bug-IX P04 R01 CodexReviewerA F2: delete reports success but snapshot remains → inconclusive
    func testDeleteNoOpReturnsInconclusiveWhenSnapshotStillPresent() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 3)
        let newer = try await writeSnapshot(client: inner, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 1
        )
        let olderPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let noOp = SnapshotNoOpDeleteClient(inner: inner, pathsToNoOp: [olderPath])

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: noOp,
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

        guard case .verificationInconclusive(let summary, _, _, let verification) = result else {
            return XCTFail("expected verificationInconclusive when deleted snapshot still present, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 1)
        guard case .inconclusive(let reason) = verification else {
            return XCTFail("expected inconclusive verification, got \(verification)")
        }
        if case .deleteTargetStillPresent(let path) = reason {
            XCTAssertEqual(path, olderPath)
        } else {
            XCTFail("expected .deleteTargetStillPresent, got \(reason)")
        }
        let olderExists = await inner.hasFile(olderPath)
        XCTAssertTrue(olderExists)
    }

    // Bug-IX P04 R22 CodexChecker F2: snapshot GC no-op delete followed by partial failure
    // must still probe absence for the earlier "deleted" candidate.
    func testNoOpDeleteFollowedByFailureStillProbesAbsence() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 3)
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 4)
        let newest = try await writeSnapshot(client: inner, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: newest.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 1
        )
        let l3Path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 3, writerID: writerID, runID: runID
        )
        let l4Path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 4, writerID: writerID, runID: runID
        )
        let wrapper = SnapshotNoOpAndFailClient(
            inner: inner,
            noOpPaths: [l3Path],
            failPaths: [l4Path]
        )

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: wrapper,
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

        guard case .verificationInconclusive(
            let summary,
            let stopReason?,
            _,
            .inconclusive(reason: .deleteTargetStillPresent(let foundPath))
        ) = result else {
            return XCTFail("expected verificationInconclusive with still-present snapshot, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 1)
        XCTAssertEqual(foundPath, l3Path)
        if case .deleteFailed(let candidate, .other) = stopReason {
            XCTAssertEqual(candidate.lamport, 4)
        } else {
            XCTFail("unexpected stop reason \(stopReason)")
        }
        let l3Exists = await inner.hasFile(l3Path)
        XCTAssertTrue(l3Exists)
    }

    // Bug-IX P04 R03 CodexReviewerA F2: delete throws not-found but snapshot remains →
    // candidate goes to alreadyMissing, absence probe now covers it.
    func testDeleteNotFoundWhileSnapshotStillPresent_returnsInconclusive() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 3)
        let newer = try await writeSnapshot(client: inner, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 1
        )
        let olderPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let notFound = SnapshotNotFoundDeleteClient(inner: inner, pathsToThrowNotFound: [olderPath])

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: notFound,
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

        guard case .verificationInconclusive(let summary, _, _, let verification) = result else {
            return XCTFail("expected verificationInconclusive when not-found delete leaves snapshot present, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 0)
        XCTAssertTrue(summary.alreadyMissingCount > 0)
        guard case .inconclusive(let reason) = verification else {
            return XCTFail("expected inconclusive verification, got \(verification)")
        }
        if case .deleteTargetStillPresent(let path) = reason {
            XCTAssertEqual(path, olderPath)
        } else {
            XCTFail("expected .deleteTargetStillPresent, got \(reason)")
        }
        let olderExists = await inner.hasFile(olderPath)
        XCTAssertTrue(olderExists)
    }

    // Bug-IX P04 R06 CodexReviewerA F1: metadata throws classified not-found for a
    // successfully deleted snapshot → post-delete probe must treat it as confirmed absence.
    func testDeleteSuccess_metadataThrowsNotFound_returnsCompleted() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 3)
        let newer = try await writeSnapshot(client: inner, covered: covered, lamport: snapshotLamport)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: newer.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 1
        )
        let olderPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 3,
            writerID: writerID,
            runID: runID
        )
        let wrapper = SnapshotMetadataNotFoundThrowClient(inner: inner)

        let executorPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: wrapper,
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

        guard case .completed(let summary, _, let verification) = result else {
            return XCTFail("expected completed, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 1)
        guard case .passed = verification else {
            return XCTFail("expected passed verification, got \(verification)")
        }
        let olderExists = await inner.hasFile(olderPath)
        XCTAssertFalse(olderExists)
    }

    // Bug-IX P01 R02 Codex A Finding 1: a fresh too-young barrier supersedes an older eligible
    // barrier in the full valid set but is not yet age-eligible to authorize deletion. The fresh
    // barrier's checkpoint snapshot must still be protected from snapshot GC, otherwise the next
    // time that barrier becomes age-eligible its evidence is gone.
    func testFreshTooYoungBarrierCheckpointIsProtectedFromSnapshotGC() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // S5 — referenced by an age-eligible barrier (createdAtMs=0).
        let s5 = try await writeSnapshot(client: client, covered: covered, lamport: 5)
        // S8 — referenced by a fresh too-young barrier (createdAtMs=110_000, nowMs=120_000, minAgeMs=60_000).
        let s8 = try await writeSnapshot(client: client, covered: covered, lamport: 8)
        // S10 — accepted baseline (no barrier referencing it; lamport > both barriers).
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 10)

        // Old eligible barrier at lamport=5 referencing S5.
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: s5.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 0,
            barrierLamport: 5
        )
        // Fresh too-young barrier at lamport=8 referencing S8 — same coveredRanges + higher ref,
        // so it supersedes the old barrier in the full valid set. Eligibility filter still keeps the
        // old barrier as the authorization barrier (because the fresh one is below the age threshold).
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: s8.sha256Hex,
            createdAtMs: 110_000,
            snapshotKeepCount: 0,
            barrierLamport: 8
        )

        let mixedPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 60,
            snapshotFallbackKeepCount: 0
        )
        let preflightService = RepoSnapshotDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: mixedPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )
        let result = try await preflightService.makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        // With the fix in place, every snapshot candidate is now protected (S5 by old barrier,
        // S8 by fresh-but-superseding barrier, S10 is the accepted baseline outside candidates),
        // so the preflight blocks with noDeleteCandidates and the protected set still includes S8.
        let s8Filename = RepoLayout.snapshotFileName(
            month: month,
            lamport: 8,
            writerID: writerID,
            runID: runID
        )
        switch result {
        case .blocked(let blockers, let report):
            XCTAssertTrue(blockers.contains(.noDeleteCandidates),
                          "expected .noDeleteCandidates because all delete candidates are now barrier-protected, got \(blockers)")
            XCTAssertTrue(report.protectedFilenames.contains(s8Filename),
                          "S8 (referenced by the fresh too-young barrier) must be in the protection set")
        case .planned(let plan, _):
            XCTAssertFalse(plan.snapshotsToDelete.contains(where: { $0.filename == s8Filename }),
                           "S8 must never appear in snapshotsToDelete; it is the fresh barrier's authoritative checkpoint")
            XCTAssertTrue(plan.protectedFilenames.contains(s8Filename),
                          "S8 must be in protectedFilenames even when its barrier hasn't aged into eligibility")
        }
    }

    // Bug-IX P01 R03 Codex A Finding 1: a fresh too-young barrier is in the retained-union
    // protection set, but pre-fix the pre-delete evidence gate (`barrierCheckpointEvidenceBlockers`)
    // only validated the age-eligible barriers' checkpoints. If the fresh barrier's checkpoint
    // file is missing or has a mismatched SHA, preflight must fail closed BEFORE deleting any
    // other older snapshot — not leave the failure to the post-delete verifier.
    func testFreshTooYoungBarrierWithMismatchedCheckpointBlocksPreDelete() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // S3 — additional deletable older snapshot, not referenced by any barrier. Ensures the
        // pre-fix path has at least one candidate to delete (so the bug surfaces as a `.planned`
        // outcome rather than `.noDeleteCandidates`).
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 3)
        // S5 — referenced by an age-eligible barrier (createdAtMs=0).
        let s5 = try await writeSnapshot(client: client, covered: covered, lamport: 5)
        // S8 — referenced by a fresh too-young barrier; the barrier will claim a wrong SHA.
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 8)
        // S10 — accepted baseline.
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 10)

        // Old eligible barrier referencing S5 (authorizes the deletion).
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: s5.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 0,
            barrierLamport: 5
        )
        // Fresh too-young barrier referencing S8 but with a tampered checkpoint SHA. F5 already
        // protects S8 from deletion; F7 now also requires the SHA mismatch to surface BEFORE
        // any mutation rather than only at post-delete verification.
        let s8Name = RepoLayout.snapshotFileName(
            month: month,
            lamport: 8,
            writerID: writerID,
            runID: runID
        )
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: String(repeating: "f", count: 64),
            createdAtMs: 110_000,
            snapshotName: s8Name,
            snapshotKeepCount: 0,
            barrierLamport: 8
        )

        let mixedPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 60,
            snapshotFallbackKeepCount: 0
        )
        let preflightService = RepoSnapshotDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: mixedPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )
        let result = try await preflightService.makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .blocked(let blockers, _) = result else {
            return XCTFail("expected blocked before any delete due to fresh barrier SHA mismatch, got \(result)")
        }
        XCTAssertTrue(blockers.contains { blocker in
            if case .barrierCheckpointMismatch(let filename, .sha256) = blocker,
               filename == s8Name { return true }
            return false
        }, "expected barrierCheckpointMismatch(.sha256) on the fresh too-young barrier, got \(blockers)")
    }

    // Bug-IX P01 R01 Codex A continuation Finding 1: retained barriers stamp the deletion
    // authorization contract; their `policy.snapshotKeepCount` is the floor for fallback
    // retention. A later runtime that lowered `snapshotFallbackKeepCount` must not delete
    // snapshots the retained barrier authorized to keep.
    // Bug-IX P04 R25 CodexReviewerA: fallback-protected snapshots must be checked
    // by the post-delete verifier. If a storage fault removes a non-barrier protected
    // snapshot during GC, the verifier must fail closed rather than report .completed.
    func testFallbackProtectedSnapshotMissingDuringDelete_failsClosed() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let acceptedLamport: UInt64 = 10
        let accepted = try await writeSnapshot(client: inner, covered: covered, lamport: acceptedLamport)
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 7)
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 5)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: accepted.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 2,
            barrierLamport: acceptedLamport
        )

        let l5Path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerID, runID: runID
        )
        let l7Path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 7, writerID: writerID, runID: runID
        )
        // Storage fault: delete(L5) also removes the fallback-protected L7.
        let wrapper = SnapshotCollateralDeleteClient(
            inner: inner,
            collateralDeletePaths: [l5Path: [l7Path]]
        )

        let narrowedPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 0
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: wrapper,
            basePath: basePath,
            policy: narrowedPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )

        let result = try await executor.execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationFailed(_, _, _, let verification) = result else {
            return XCTFail("expected verificationFailed, got \(result)")
        }
        guard case .failed(let reason, _) = verification else {
            return XCTFail("expected failed verification, got \(verification)")
        }
        guard case .protectedSnapshotMissingOrTampered(let filename, _, _) = reason else {
            return XCTFail("expected protectedSnapshotMissingOrTampered, got \(reason)")
        }
        let l7Filename = RepoLayout.snapshotFileName(month: month, lamport: 7, writerID: writerID, runID: runID)
        XCTAssertEqual(filename, l7Filename, "verifier must detect fallback-protected L7 gone")
    }

    // Bug-IX P04 R26 CodexReviewerA F2: pre-delete accepted baseline SHA must be checked
    // even when a newer post-delete baseline supersedes it. Otherwise a remote-fault that
    // removes the pre-delete baseline during GC silently passes when the new baseline can
    // still materialize the month.
    func testPreDeleteBaselineLossWithSupersede_failsClosed() async throws {
        let inner = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        let baselineLamport: UInt64 = 10
        let baseline = try await writeSnapshot(client: inner, covered: covered, lamport: baselineLamport)
        // Older candidate, eligible for deletion.
        _ = try await writeSnapshot(client: inner, covered: covered, lamport: 5)
        try await writeBarrier(
            client: inner,
            covered: covered,
            checkpointSHA256Hex: baseline.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 1,
            barrierLamport: baselineLamport
        )

        // Pre-build a supersede snapshot (L20) to be injected during the delete hook so
        // the post-delete materializer accepts it instead of the (now-missing) L10 baseline.
        let supersedeLamport: UInt64 = 20
        let supersedeHeader = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
        let supersedeRows = RepoSnapshotBuilder.build(header: supersedeHeader, state: .empty)
        let supersedePath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: supersedeLamport, writerID: writerID, runID: runID
        )

        let l5Path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerID, runID: runID
        )
        let baselinePath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: baselineLamport, writerID: writerID, runID: runID
        )

        let capturedBasePath = basePath
        let capturedMonth = month
        let capturedRunID = runID
        let wrapper = SnapshotSupersedeDeleteClient(
            inner: inner,
            triggerPath: l5Path,
            collateralDeletePaths: [baselinePath],
            supersedeWriter: { [supersedeHeader, supersedeRows] in
                _ = try await SnapshotWriter(client: inner, basePath: capturedBasePath).write(
                    header: supersedeHeader,
                    assets: supersedeRows.assets,
                    resources: supersedeRows.resources,
                    assetResources: supersedeRows.assetResources,
                    deletedKeys: supersedeRows.deletedKeys,
                    month: capturedMonth,
                    lamport: supersedeLamport,
                    runID: capturedRunID,
                    respectTaskCancellation: false
                )
            }
        )

        let narrowedPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 1
        )
        let executor = RepoSnapshotDeleteExecutor(
            client: wrapper,
            basePath: basePath,
            policy: narrowedPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )

        let result = try await executor.execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationFailed(_, _, _, let verification) = result else {
            return XCTFail("expected verificationFailed, got \(result)")
        }
        guard case .failed(let reason, _) = verification else {
            return XCTFail("expected failed verification, got \(verification)")
        }
        guard case .protectedSnapshotMissingOrTampered(let filename, _, _) = reason else {
            return XCTFail("expected protectedSnapshotMissingOrTampered, got \(reason)")
        }
        let baselineFilename = RepoLayout.snapshotFileName(
            month: month, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        XCTAssertEqual(filename, baselineFilename,
                       "verifier must always re-check pre-delete baseline SHA, even when superseded; supersede path: \(supersedePath)")
    }

    func testRetainedBarrierKeepCountFloorsSnapshotGC() async throws {
        let client = try await makeReadyClient()
        let covered = makeCovered(seqs: [(1, 1)])
        // Accepted baseline at the barrier's lamport (10).
        let acceptedLamport: UInt64 = 10
        let accepted = try await writeSnapshot(client: client, covered: covered, lamport: acceptedLamport)
        // Two older parseable snapshots. With keepCount=2 from the retained barrier they should
        // be protected even when the runtime policy says snapshotFallbackKeepCount=0.
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 7)
        _ = try await writeSnapshot(client: client, covered: covered, lamport: 5)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: accepted.sha256Hex,
            createdAtMs: 0,
            snapshotKeepCount: 2,
            barrierLamport: acceptedLamport
        )

        let narrowedPolicy = RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 0,
            snapshotFallbackKeepCount: 0
        )
        let preflightService = RepoSnapshotDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: narrowedPolicy,
            isLocalVolume: true,
            peerStatusProvider: { .empty }
        )
        let result = try await preflightService.makePlan(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .planned(let plan, _) = result else {
            return XCTFail("expected planned, got \(result)")
        }
        // keepCount=2 takes the top-2 by (lamport desc) which are accepted=10 and lamport=7.
        // Without the fix, runtime policy.snapshotFallbackKeepCount=0 would leave protected={10}
        // and lamport=7 would land in the delete list.
        let kept7Filename = RepoLayout.snapshotFileName(month: month, lamport: 7, writerID: writerID, runID: runID)
        XCTAssertTrue(plan.protectedFilenames.contains(kept7Filename),
                      "retained barrier policy.snapshotKeepCount=2 must protect lamport=7 even when runtime says 0")
        XCTAssertFalse(plan.snapshotsToDelete.contains(where: { $0.filename == kept7Filename }),
                       "snapshots authorized to be kept by the retained barrier must not appear in the delete list")
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
        observedSeqHighByWriter: [String: UInt64]? = nil,
        snapshotKeepCount: Int? = nil,
        barrierLamport: UInt64? = nil
    ) async throws {
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerID,
            runID: UUID(uuidString: runID)!,
            createdAtMs: createdAtMs,
            barrierLamport: barrierLamport ?? snapshotLamport,
            checkpointSnapshotName: snapshotName ?? RepoLayout.snapshotFileName(
                month: month,
                lamport: barrierLamport ?? snapshotLamport,
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
                snapshotKeepCount: snapshotKeepCount ?? policy().snapshotFallbackKeepCount
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

/// Client whose `delete` throws not-found for specified paths without removing them.
private final class SnapshotNotFoundDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let pathsToThrowNotFound: Set<String>

    init(inner: InMemoryRemoteStorageClient, pathsToThrowNotFound: [String]) {
        self.inner = inner
        self.pathsToThrowNotFound = Set(pathsToThrowNotFound.map(Self.normalize))
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if pathsToThrowNotFound.contains(Self.normalize(path)) {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

/// Client whose `delete` reports success but leaves specified files in place.
private final class SnapshotNoOpDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let pathsToNoOp: Set<String>

    init(inner: InMemoryRemoteStorageClient, pathsToNoOp: [String]) {
        self.inner = inner
        self.pathsToNoOp = Set(pathsToNoOp.map(Self.normalize))
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if pathsToNoOp.contains(Self.normalize(path)) { return }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

/// Client whose `metadata` throws classified not-found for absent entries instead of returning nil.
private final class SnapshotMetadataNotFoundThrowClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let result = try await inner.metadata(path: path)
        if result == nil {
            throw NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError)
        }
        return result
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}

private final class SnapshotNoOpAndFailClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let noOpPaths: Set<String>
    private let failPaths: Set<String>

    init(inner: InMemoryRemoteStorageClient, noOpPaths: [String], failPaths: [String]) {
        self.inner = inner
        self.noOpPaths = Set(noOpPaths.map(Self.normalize))
        self.failPaths = Set(failPaths.map(Self.normalize))
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        let key = Self.normalize(path)
        if noOpPaths.contains(key) { return }
        if failPaths.contains(key) {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut,
                          userInfo: [NSLocalizedDescriptionKey: "transport failure"])
        }
        try await inner.delete(path: key)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

/// Client that, when asked to delete a primary path, also deletes the configured
/// collateral paths from the inner store — models a storage fault that removes a
/// non-target file during a delete.
private final class SnapshotCollateralDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let collateralByPrimary: [String: [String]]

    init(inner: InMemoryRemoteStorageClient, collateralDeletePaths: [String: [String]]) {
        self.inner = inner
        var normalized: [String: [String]] = [:]
        for (primary, others) in collateralDeletePaths {
            normalized[Self.normalize(primary)] = others.map(Self.normalize)
        }
        self.collateralByPrimary = normalized
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        let key = Self.normalize(path)
        try await inner.delete(path: key)
        if let collateral = collateralByPrimary[key] {
            for other in collateral {
                try? await inner.delete(path: other)
            }
        }
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

/// Client that, when asked to delete `triggerPath`, also deletes the configured collateral
/// paths and then runs `supersedeWriter` to inject a newer snapshot into the inner store —
/// models a remote-fault window where a peer publishes a newer baseline during delete and
/// the pre-delete baseline is collaterally removed.
private final class SnapshotSupersedeDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let triggerPath: String
    private let collateralDeletePaths: [String]
    private let supersedeWriter: @Sendable () async throws -> Void

    init(
        inner: InMemoryRemoteStorageClient,
        triggerPath: String,
        collateralDeletePaths: [String],
        supersedeWriter: @escaping @Sendable () async throws -> Void
    ) {
        self.inner = inner
        self.triggerPath = Self.normalize(triggerPath)
        self.collateralDeletePaths = collateralDeletePaths.map(Self.normalize)
        self.supersedeWriter = supersedeWriter
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .overwritePossible }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        let key = Self.normalize(path)
        try await inner.delete(path: key)
        if key == triggerPath {
            for other in collateralDeletePaths {
                try? await inner.delete(path: other)
            }
            try await supersedeWriter()
        }
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}
