import XCTest
@testable import Watermelon

final class RepoRetentionDeletePreflightTests: XCTestCase {
    func testHappyPathProducesExactDryRunPlanAndPostDeleteContract() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...4)
        let covered = coveredRanges([(1, 3)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        await client.injectFile(
            path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(month.text)--not-a-writer--0000000000000005.jsonl",
            contents: "not trusted"
        )
        try await writeCommits(client: client, month: otherMonth, seqs: 1...1)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        if case .planned(_, let report) = result {
            XCTAssertEqual(report.candidateScan?.readConcurrencyLimit, 1)
        }
        let plan = try requirePlan(result)

        XCTAssertEqual(plan.repoID, repoID)
        XCTAssertEqual(plan.deletePrefixByWriter, [writerA: 3])
        XCTAssertEqual(plan.commitFiles.map(\.filename), (1...3).map {
            RepoLayout.commitFileName(month: month, writerID: writerA, seq: UInt64($0))
        })
        XCTAssertEqual(plan.commitFiles.map(\.path), (1...3).map {
            RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: UInt64($0))
        })
        XCTAssertTrue(plan.commitFiles.allSatisfy { !$0.sha256Hex.isEmpty && $0.rowCount > 0 })
        XCTAssertEqual(plan.protectedSummary.outOfPrefixCommitFileCount, 1)
        XCTAssertEqual(plan.protectedSummary.targetMonthUnparseableFilenameCount, 1)
        XCTAssertEqual(plan.protectedSummary.crossMonthCommitFileCount, 1)
        XCTAssertEqual(plan.livenessDecision.blockers, [])
        XCTAssertEqual(plan.acceptedSnapshot.filename, RepoLayout.snapshotFileName(
            month: month,
            lamport: snapshotLamport,
            writerID: writerA,
            runID: runID
        ))

        let contract = plan.preDeleteEvidence.postDeleteEquivalenceContract
        XCTAssertEqual(contract.mode, .retentionSuperset)
        XCTAssertEqual(contract.acceptedSnapshotFilename, plan.acceptedSnapshot.filename)
        XCTAssertEqual(contract.acceptedSnapshotCovered, covered)
        XCTAssertEqual(contract.retainedBarrierUnionCovered, covered)
        XCTAssertEqual(contract.requiredObservedSeqByWriter[writerA], 4)
        XCTAssertEqual(contract.expectedDeletePrefixByWriter, [writerA: 3])
        XCTAssertEqual(contract.preDeleteState, plan.preDeleteEvidence.materializedState)
    }

    func testMissingUnsupportedUnreadableVersionAndIdentityMismatchFailClosed() async throws {
        let missingVersion = try await makeClient(injectVersion: false)
        var result = try await service(client: missingVersion).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.missingVersion))

        let unsupported = try await makeClient(injectVersion: false)
        try await TestFixtures.injectVersionJSON(unsupported, basePath: basePath, formatVersion: 99, writerID: writerA)
        result = try await service(client: unsupported).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.unsupportedVersion(formatVersion: 99)))

        let unreadable = try await makeClient(injectVersion: false)
        await unreadable.injectFile(path: RepoLayout.versionFilePath(base: basePath), contents: "{not-json")
        result = try await service(client: unreadable).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.unreadableVersion))

        let noAcceptedForExpectedRepo = try await makeClient()
        try await writeCommits(client: noAcceptedForExpectedRepo, seqs: 1...1)
        let expectedRepoSnapshot = try await writeSnapshot(client: noAcceptedForExpectedRepo, covered: coveredRanges([(1, 1)]))
        try await writeBarrier(
            client: noAcceptedForExpectedRepo,
            repoID: foreignRepoID,
            covered: coveredRanges([(1, 1)]),
            checkpointSHA256Hex: expectedRepoSnapshot.sha256Hex
        )
        result = try await service(client: noAcceptedForExpectedRepo).makePlan(
            month: month,
            expectedRepoID: foreignRepoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.barrierCheckpointMismatch(
            filename: RepoLayout.snapshotFileName(month: month, lamport: snapshotLamport, writerID: writerA, runID: runID),
            reason: .repoID(expected: foreignRepoID, actual: repoID)
        )))
        XCTAssertFalse(blockers(in: result).contains { blocker in
            if case .repoIdentityMismatch = blocker { return true }
            return false
        })
    }

    func testMigrationMarkerInvalidBarrierAndEmptyBarrierBlock() async throws {
        let migration = try await makeClient()
        await migration.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerA),
            contents: "{}"
        )
        var result = try await service(client: migration).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.migrationInProgress))

        let invalid = try await makeClient()
        await invalid.injectFile(
            path: "\(RepoLayout.retentionDirectoryPath(base: basePath))/\(month.text)--bad.json",
            contents: "not-json"
        )
        result = try await service(client: invalid).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains { blocker in
            if case .invalidBarrierSet(let entries) = blocker {
                return entries.map(\.reason).contains(.filenameMalformed)
            }
            return false
        })

        let sameMonthForeign = try await makeClient()
        try await writeBarrier(
            client: sameMonthForeign,
            repoID: foreignRepoID,
            covered: coveredRanges([(1, 1)])
        )
        result = try await service(client: sameMonthForeign).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains { blocker in
            if case .invalidBarrierSet(let entries) = blocker {
                return entries.map(\.reason).contains(.foreignRepoID(foreignRepoID))
            }
            return false
        })

        let empty = try await makeClient()
        result = try await service(client: empty).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.emptyBarrierSet))

        let differentMonthForeign = try await makeReadyClient()
        try await writeBarrier(
            client: differentMonthForeign,
            month: otherMonth,
            repoID: foreignRepoID,
            covered: coveredRanges([(1, 1)])
        )
        result = try await service(client: differentMonthForeign).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        _ = try requirePlan(result)
    }

    func testMigrationMarkerDirectorySentinelBlocksPreflight() async throws {
        let client = try await makeReadyClient()
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerA))

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.migrationInProgress))
    }

    // Bug-IX P01 R01 Codex A Finding 1: phase-3 sweep preserves a month's partial-migration
    // marker + legacy residue when not every V1 asset could migrate, then deletes the central
    // marker. A subsequent retention preflight must still treat that month as migration-unresolved
    // and refuse irreversible commit-prefix deletion.
    func testMonthPartialMigrationMarkerBlocksPreflight() async throws {
        let client = try await makeReadyClient()
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, V1MigrationResidueFileNames.partialMigrationMarkerFileName)
        )
        await client.injectFile(path: markerPath, contents: "{}")

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.migrationResiduePresent(month: month)),
                      "month-local partial migration marker must block retention deletion even after the central marker is gone")
    }

    // Bug-IX P01 R04 Codex A Finding 1: a directory squatting at the month-local partial-migration
    // marker path is damaged remote state, not proof that the marker is absent. The retention
    // preflight must fail-closed before any irreversible commit-prefix deletion runs.
    func testMonthPartialMigrationMarkerDirectorySentinelFailsClosed() async throws {
        let client = try await makeReadyClient()
        let markerPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, V1MigrationResidueFileNames.partialMigrationMarkerFileName)
        )
        try await client.createDirectory(path: markerPath)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.migrationResidueCheckFailed(month: month)),
                      "directory-shaped month-local marker must fail-closed via migrationResidueCheckFailed")
        XCTAssertFalse(blockers(in: result).contains(.migrationResiduePresent(month: month)),
                       "directory shape isn't a marker present claim; it's uncertain corrupt state")
    }

    func testBarrierAgeAndLivenessCapabilityBlockers() async throws {
        let tooYoung = try await makeClient()
        try await writeCommits(client: tooYoung, seqs: 1...1)
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: tooYoung, covered: covered)
        try await writeBarrier(
            client: tooYoung,
            covered: covered,
            createdAtMs: nowMs - 1_000,
            checkpointSHA256Hex: snapshot.sha256Hex
        )

        var result = try await service(client: tooYoung).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains { blocker in
            if case .barrierTooYoung = blocker { return true }
            return false
        })

        let future = try await makeClient()
        try await writeCommits(client: future, seqs: 1...1)
        let futureSnapshot = try await writeSnapshot(client: future, covered: covered)
        try await writeBarrier(
            client: future,
            covered: covered,
            createdAtMs: nowMs + 10 * 60 * 1_000,
            checkpointSHA256Hex: futureSnapshot.sha256Hex
        )
        result = try await service(client: future).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains { blocker in
            if case .barrierCreatedInFuture = blocker { return true }
            return false
        })

        let agedMaskedByFresh = try await makeClient()
        try await writeCommits(client: agedMaskedByFresh, seqs: 1...2)
        let oldCovered = coveredRanges([(1, 1)])
        let oldSnapshot = try await writeSnapshot(client: agedMaskedByFresh, covered: oldCovered, lamport: 10)
        try await writeBarrier(
            client: agedMaskedByFresh,
            covered: oldCovered,
            lamport: 10,
            checkpointSHA256Hex: oldSnapshot.sha256Hex
        )
        let freshCovered = coveredRanges([(1, 2)])
        let freshSnapshot = try await writeSnapshot(client: agedMaskedByFresh, covered: freshCovered, lamport: 11)
        try await writeBarrier(
            client: agedMaskedByFresh,
            covered: freshCovered,
            lamport: 11,
            createdAtMs: nowMs - 1_000,
            checkpointSHA256Hex: freshSnapshot.sha256Hex
        )

        result = try await service(client: agedMaskedByFresh).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        let plan = try requirePlan(result)
        XCTAssertEqual(plan.deletePrefixByWriter, [writerA: 1])
        XCTAssertEqual(plan.preDeleteEvidence.retainedBarrierUnionCovered, oldCovered)
        XCTAssertEqual(plan.commitFiles.map(\.seq), [1])

        let agedMaskedByFuture = try await makeClient()
        try await writeCommits(client: agedMaskedByFuture, seqs: 1...2)
        let oldFutureSnapshot = try await writeSnapshot(client: agedMaskedByFuture, covered: oldCovered, lamport: 10)
        try await writeBarrier(
            client: agedMaskedByFuture,
            covered: oldCovered,
            lamport: 10,
            checkpointSHA256Hex: oldFutureSnapshot.sha256Hex
        )
        let futureMaskedSnapshot = try await writeSnapshot(client: agedMaskedByFuture, covered: freshCovered, lamport: 11)
        try await writeBarrier(
            client: agedMaskedByFuture,
            covered: freshCovered,
            lamport: 11,
            createdAtMs: nowMs + 10 * 60 * 1_000,
            checkpointSHA256Hex: futureMaskedSnapshot.sha256Hex
        )

        result = try await service(client: agedMaskedByFuture).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains { blocker in
            if case .barrierCreatedInFuture = blocker { return true }
            return false
        })

        let activePeer = try await makeClient()
        try await writeCommits(client: activePeer, seqs: 1...1)
        let activeSnapshot = try await writeSnapshot(client: activePeer, covered: covered)
        try await writeBarrier(client: activePeer, covered: covered, checkpointSHA256Hex: activeSnapshot.sha256Hex)
        result = try await service(
            client: activePeer,
            peerStatusView: RetentionPeerStatusView(peers: [
                RetentionPeerStatus(
                    writerID: writerB,
                    status: .active(lastSeenMs: nowMs - 1_000),
                    capability: RetentionPeerCapability(
                        barrierAwareSessionRefresh: true,
                        checkpointBarrierHook: true
                    )
                )
            ], listComplete: true)
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
        XCTAssertTrue(blockers(in: result).contains(.retentionLivenessBlocked([
            .activePeer(writerID: writerB)
        ])))

        let localVolumeResult = try await service(
            client: activePeer,
            peerStatusView: RetentionPeerStatusView(peers: [
                RetentionPeerStatus(
                    writerID: writerB,
                    status: .active(lastSeenMs: nowMs - 1_000),
                    capability: nil
                )
            ], listComplete: true),
            isLocalVolume: true
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
        _ = try requirePlan(localVolumeResult)
    }

    func testLivenessSnapshotFailureThrowsTypedAndCancellationIsPreserved() async throws {
        let failing = try await makeReadyClient()
        do {
            _ = try await service(
                client: failing,
                peerStatusProvider: { throw InMemoryRemoteStorageClient.InjectedError.transport }
            ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
            XCTFail("expected typed liveness snapshot error")
        } catch RepoRetentionDeletePreflightError.livenessSnapshotUnavailable {
        }

        let cancelling = try await makeReadyClient()
        do {
            _ = try await service(
                client: cancelling,
                peerStatusProvider: { throw CancellationError() }
            ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
    }

    func testAuthoritativeRepoIdentityMismatch_blocksAfterValidBarriers() async throws {
        let client = try await makeReadyClient()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: foreignRepoID, writerID: writerA)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.repoIdentityMismatch(
            expected: repoID,
            observed: foreignRepoID
        )))
    }

    func testUnknownLegacyAndSelfOnlyPeerViewSemantics() async throws {
        let unknown = try await makeReadyClient()
        var result = try await service(
            client: unknown,
            peerStatusView: RetentionPeerStatusView(peers: [
                RetentionPeerStatus(
                    writerID: writerB,
                    status: .unknown(reason: .readFailed),
                    capability: nil
                )
            ], listComplete: true)
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
        XCTAssertTrue(blockers(in: result).contains(.retentionLivenessBlocked([
            .incompleteView,
            .unknownPeer(writerID: writerB)
        ])))

        let legacy = try await makeReadyClient()
        result = try await service(
            client: legacy,
            peerStatusView: RetentionPeerStatusView(peers: [
                RetentionPeerStatus(
                    writerID: writerB,
                    status: .stale(lastSeenMs: nowMs - 1_000),
                    capability: nil
                )
            ], listComplete: true)
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
        XCTAssertTrue(blockers(in: result).contains(.retentionLivenessBlocked([
            .legacyPeer(writerID: writerB, lastSeenMs: nowMs - 1_000)
        ])))

        let selfOnly = try await makeReadyClient()
        result = try await service(client: selfOnly, peerStatusView: .empty).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        _ = try requirePlan(result)
    }

    func testAcceptedSnapshotMustDominateBarrierUnion() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...3)
        let barrierSnapshot = try await writeSnapshot(client: client, covered: coveredRanges([(1, 3)]))
        try await writeBarrier(
            client: client,
            covered: coveredRanges([(1, 3)]),
            checkpointSHA256Hex: barrierSnapshot.sha256Hex
        )
        _ = try await writeSnapshot(client: client, covered: coveredRanges([(1, 2)]), lamport: snapshotLamport + 1)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.acceptedSnapshotMissingBarrierCoverage))
    }

    func testCoveredGapsPlanOnlyContiguousPrefix() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...5)
        let covered = coveredRanges([(1, 2), (4, 5)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        let plan = try requirePlan(result)

        XCTAssertEqual(plan.deletePrefixByWriter, [writerA: 2])
        XCTAssertEqual(plan.commitFiles.map(\.seq), [1, 2])
        XCTAssertEqual(plan.protectedSummary.outOfPrefixCommitFileCount, 3)
    }

    func testPublishedDeletePrefixCapsWiderBarrierCoverage() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...3)
        let covered = coveredRanges([(1, 3)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(
            client: client,
            covered: covered,
            deletePrefixByWriter: [writerA: 2],
            checkpointSHA256Hex: snapshot.sha256Hex
        )

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        let plan = try requirePlan(result)

        XCTAssertEqual(plan.deletePrefixByWriter, [writerA: 2])
        XCTAssertEqual(plan.commitFiles.map(\.seq), [1, 2])
        XCTAssertEqual(plan.protectedSummary.outOfPrefixCommitFileCount, 1)
    }

    func testCandidateHeaderRepoAndScopeMismatchBlock() async throws {
        let repoMismatch = try await makeClient()
        _ = try await CommitLogWriter(client: repoMismatch, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: foreignRepoID,
                writerID: writerA,
                seq: 1,
                runID: runID,
                month: month
            ),
            ops: [],
            month: month,
            respectTaskCancellation: false
        )
        let covered = coveredRanges([(1, 1)])
        let repoMismatchSnapshot = try await writeSnapshot(client: repoMismatch, covered: covered)
        try await writeBarrier(client: repoMismatch, covered: covered, checkpointSHA256Hex: repoMismatchSnapshot.sha256Hex)

        var result = try await service(client: repoMismatch).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.candidateHeaderMismatch(
            filename: RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1),
            reason: .repoID(expected: repoID, actual: foreignRepoID)
        )))

        let scopeMismatch = try await makeClient()
        _ = try await CommitLogWriter(client: scopeMismatch, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerA,
                seq: 1,
                runID: runID,
                month: otherMonth
            ),
            ops: [],
            month: month,
            respectTaskCancellation: false
        )
        let scopeMismatchSnapshot = try await writeSnapshot(client: scopeMismatch, covered: covered)
        try await writeBarrier(client: scopeMismatch, covered: covered, checkpointSHA256Hex: scopeMismatchSnapshot.sha256Hex)
        result = try await service(client: scopeMismatch).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.candidateHeaderMismatch(
            filename: RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1),
            reason: .month(expected: month, actual: otherMonth)
        )))
    }

    func testCandidateCorruptionAndVanishedReadBlock() async throws {
        let corrupt = try await makeClient()
        try await writeCommits(client: corrupt, seqs: 1...1)
        let covered = coveredRanges([(1, 1)])
        let corruptSnapshot = try await writeSnapshot(client: corrupt, covered: covered)
        try await writeBarrier(client: corrupt, covered: covered, checkpointSHA256Hex: corruptSnapshot.sha256Hex)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await corrupt.corrupt(path: commitPath, with: Data("bad\n".utf8))

        var result = try await service(client: corrupt).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.candidateCorruptOrUntrusted(
            filename: RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        )))

        let vanishedInner = try await makeClient()
        try await writeCommits(client: vanishedInner, seqs: 1...1)
        let vanishedSnapshot = try await writeSnapshot(client: vanishedInner, covered: covered)
        try await writeBarrier(client: vanishedInner, covered: covered, checkpointSHA256Hex: vanishedSnapshot.sha256Hex)
        let vanished = VanishingCandidateReadClient(inner: vanishedInner, vanishPath: commitPath)
        result = try await service(client: vanished).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.candidateReadFailed(
            filename: RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        )))
    }

    func testBarrierCheckpointEvidenceMustMatchSnapshot() async throws {
        let filename = RepoLayout.snapshotFileName(month: month, lamport: snapshotLamport, writerID: writerA, runID: runID)

        let badSHA = try await makeClient()
        try await writeCommits(client: badSHA, seqs: 1...1)
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: badSHA, covered: covered)
        let wrongSHA = String(repeating: "b", count: 64)
        try await writeBarrier(client: badSHA, covered: covered, checkpointSHA256Hex: wrongSHA)
        var result = try await service(client: badSHA).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.barrierCheckpointMismatch(
            filename: filename,
            reason: .sha256(expected: wrongSHA, actual: snapshot.sha256Hex)
        )))

        let missing = try await makeClient()
        try await writeCommits(client: missing, seqs: 1...1)
        let missingSnapshot = try await writeSnapshot(client: missing, covered: covered)
        try await writeBarrier(client: missing, covered: covered, checkpointSHA256Hex: missingSnapshot.sha256Hex)
        try await missing.delete(path: RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: snapshotLamport,
            writerID: writerA,
            runID: runID
        ))
        result = try await service(client: missing).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.barrierCheckpointReadFailed(filename: filename)))

        let coveredMismatch = try await makeClient()
        try await writeCommits(client: coveredMismatch, seqs: 1...2)
        let narrowerSnapshot = try await writeSnapshot(client: coveredMismatch, covered: covered)
        try await writeBarrier(
            client: coveredMismatch,
            covered: coveredRanges([(1, 2)]),
            checkpointSHA256Hex: narrowerSnapshot.sha256Hex
        )
        result = try await service(client: coveredMismatch).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        XCTAssertTrue(blockers(in: result).contains(.barrierCheckpointMismatch(
            filename: filename,
            reason: .coveredRanges
        )))
    }

    func testBarrierObservedHighWaterMustNotRegress() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...1)
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(
            client: client,
            covered: covered,
            checkpointSHA256Hex: snapshot.sha256Hex,
            observedSeqHighByWriter: [writerA: 2]
        )

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(blockers(in: result).contains(.barrierObservedSeqRegression(
            writerID: writerA,
            expectedAtLeast: 2,
            observed: 1
        )))
    }

    func testSeqZeroAndNonCanonicalCommitAreProtectedAndCandidateListFailureIsTyped() async throws {
        let seqZero = try await makeClient()
        try await writeCommits(client: seqZero, seqs: 0...1)
        let canonicalFilename = RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        let canonicalPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let files = await seqZero.snapshotFiles()
        let canonicalBytes = try XCTUnwrap(files[canonicalPath])
        let nonCanonicalFilename = "\(month.text)--\(writerA)--1.jsonl"
        await seqZero.injectFile(
            path: RemotePathBuilder.absolutePath(
                basePath: RepoLayout.commitsDirectoryPath(base: basePath),
                remoteRelativePath: nonCanonicalFilename
            ),
            data: canonicalBytes
        )
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: seqZero, covered: covered)
        try await writeBarrier(client: seqZero, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)

        var result = try await service(client: seqZero).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        let plan = try requirePlan(result)
        XCTAssertEqual(plan.commitFiles.map(\.filename), [canonicalFilename])
        XCTAssertEqual(plan.commitFiles.map(\.path), [RemotePathBuilder.absolutePath(
            basePath: RepoLayout.commitsDirectoryPath(base: basePath),
            remoteRelativePath: canonicalFilename
        )])
        XCTAssertEqual(plan.commitFiles.map(\.seq), [1])
        XCTAssertEqual(plan.protectedSummary.outOfPrefixCommitFileCount, 0)
        XCTAssertEqual(plan.protectedSummary.targetMonthUnparseableFilenameCount, 2)
        XCTAssertFalse(plan.commitFiles.contains { $0.filename == nonCanonicalFilename })

        let listFailureInner = try await makeReadyClient()
        let listFailure = CommitListFailingClient(
            inner: listFailureInner,
            commitsDirectoryPath: RepoLayout.commitsDirectoryPath(base: basePath),
            failOnListCall: 3
        )
        result = try await service(client: listFailure).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        if case .blocked(let blockers, let report) = result {
            XCTAssertEqual(blockers, [.candidateListFailed])
            XCTAssertEqual(report.candidateScan?.blockers, [.candidateListFailed])
        } else {
            XCTFail("expected candidate list failure blocker")
        }

        let cancellationInner = try await makeReadyClient()
        let cancellation = CommitListFailingClient(
            inner: cancellationInner,
            commitsDirectoryPath: RepoLayout.commitsDirectoryPath(base: basePath),
            failOnListCall: 3,
            error: CancellationError()
        )
        do {
            _ = try await service(client: cancellation).makePlan(
                month: month,
                expectedRepoID: repoID,
                mode: .dryRun,
                nowMs: nowMs
            )
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
    }

    // A not-found listing the commits namespace (after a valid accepted snapshot + aged barrier
    // make retention otherwise eligible) is damaged/ambiguous metadata, not proof of no candidates.
    // It must fail closed via `.candidateListFailed`, mirroring the snapshot scanner's missing-dir
    // handling, rather than silently downgrading to `.noDeleteCandidates`.
    func testCommitsDirectoryNotFoundFailsClosedAsCandidateListFailed() async throws {
        let inner = try await makeReadyClient()
        let notFound = CommitListFailingClient(
            inner: inner,
            commitsDirectoryPath: RepoLayout.commitsDirectoryPath(base: basePath),
            failOnListCall: 3,
            error: NSError(domain: "WebDAVClient", code: 404)
        )
        let result = try await service(client: notFound).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        if case .blocked(let blockers, let report) = result {
            XCTAssertEqual(blockers, [.candidateListFailed])
            XCTAssertEqual(report.candidateScan?.blockers, [.candidateListFailed])
        } else {
            XCTFail("expected .candidateListFailed for a missing commits namespace, got \(result)")
        }
    }

    // Bug-IX P01 R08 Codex A F1: a commit whose body contains an op at or above
    // `LamportClock.maxAdoptableValue` is rejected by the materializer's commit-trust pipeline.
    // Retention preflight must mirror that predicate so the file is not classified as a delete
    // candidate even when the accepted snapshot/barrier's covered range nominally includes its seq.
    // Bug-IX P01 R09 Codex A F1: a directory squatting at a canonical target-month commit
    // filename inside the delete prefix is damaged remote metadata in the exact namespace
    // retention is about to prune. Preflight must fail-closed via `.candidateCorruptOrUntrusted`
    // before any sibling commit in the same prefix is removed. Parallel to F20's snapshot
    // directory-shape fix in `RepoSnapshotDeletePreflightService`.
    func testDirectoryAtTargetMonthCommitFilenameInPrefixFailsClosed() async throws {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...2)
        let covered = coveredRanges([(1, 3)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)

        let damagedName = RepoLayout.commitFileName(month: month, writerID: writerA, seq: 3)
        let damagedPath = RemotePathBuilder.absolutePath(
            basePath: RepoLayout.commitsDirectoryPath(base: basePath),
            remoteRelativePath: damagedName
        )
        try await client.createDirectory(path: damagedPath)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        XCTAssertTrue(
            blockers(in: result).contains(.candidateCorruptOrUntrusted(filename: damagedName)),
            "retention preflight must fail-closed when a directory squats at a canonical in-prefix commit filename"
        )
        // The sibling commit files must NOT have been promoted to planned candidates.
        if case .planned = result {
            XCTFail("expected .blocked, got \(result)")
        }
    }

    func testCommitWithUnworkableOpClockIsKeptAsCorruptOrUntrusted() async throws {
        let client = try await makeClient()
        let fingerprint = TestFixtures.assetFingerprint(0xC1)
        try await writeCommit(
            client: client,
            writerID: writerA,
            seq: 1,
            ops: [addAssetOp(fingerprint: fingerprint, clock: LamportClock.maxAdoptableValue, resources: [])]
        )
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        let filename = RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        XCTAssertTrue(
            blockers(in: result).contains(.candidateCorruptOrUntrusted(filename: filename)),
            "retention preflight must keep a materializer-rejected commit (op.clock >= maxAdoptableValue) instead of deleting it"
        )
    }

    func testMultiWriterNonEmptyStatePlanAndContract() async throws {
        let client = try await makeClient()
        let fpA = TestFixtures.assetFingerprint(0xA1)
        let fpB = TestFixtures.assetFingerprint(0xB1)
        let hashA = TestFixtures.fingerprint(0xA2)
        let resourceA = CommitResourceEntry(
            physicalRemotePath: "2026/05/a.jpg",
            logicalName: "a.jpg",
            contentHash: hashA,
            fileSize: 11,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )
        try await writeCommit(
            client: client,
            writerID: writerA,
            seq: 1,
            ops: [addAssetOp(fingerprint: fpA, clock: 1, resources: [resourceA])]
        )
        try await writeCommit(client: client, writerID: writerA, seq: 2, ops: [])
        try await writeCommit(
            client: client,
            writerID: writerB,
            seq: 1,
            ops: [addAssetOp(fingerprint: fpB, clock: 2, resources: [])]
        )

        var state = RepoMonthState.empty
        state.assets[fpA] = SnapshotAssetRow(
            assetFingerprint: fpA,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 11,
            stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
        )
        state.resources[resourceA.physicalRemotePath] = SnapshotResourceRow(
            physicalRemotePath: resourceA.physicalRemotePath,
            contentHash: resourceA.contentHash,
            fileSize: resourceA.fileSize,
            resourceType: resourceA.resourceType,
            creationDateMs: nil,
            backedUpAtMs: 1,
            crypto: nil,
            stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
        )
        state.assetResources[AssetResourceKey(assetFingerprint: fpA, role: resourceA.role, slot: resourceA.slot)] =
            SnapshotAssetResourceRow(
                assetFingerprint: fpA,
                role: resourceA.role,
                slot: resourceA.slot,
                resourceHash: resourceA.contentHash,
                logicalName: resourceA.logicalName
            )
        state.assets[fpB] = SnapshotAssetRow(
            assetFingerprint: fpB,
            creationDateMs: nil,
            backedUpAtMs: 2,
            resourceCount: 0,
            totalFileSizeBytes: 0,
            stamp: OpStamp(writerID: writerB, seq: 1, clock: 2)
        )

        let covered = coveredRanges([
            writerA: [(1, 2)],
            writerB: [(1, 1)]
        ])
        let snapshot = try await writeSnapshot(client: client, covered: covered, state: state)
        try await writeBarrier(
            client: client,
            covered: covered,
            deletePrefixByWriter: [writerA: 2, writerB: 1],
            checkpointSHA256Hex: snapshot.sha256Hex,
            observedSeqHighByWriter: [writerA: 2, writerB: 1]
        )

        let result = try await service(client: client).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )
        let plan = try requirePlan(result)
        let contract = plan.preDeleteEvidence.postDeleteEquivalenceContract

        XCTAssertEqual(plan.deletePrefixByWriter, [writerA: 2, writerB: 1])
        XCTAssertEqual(plan.commitFiles.map(\.seq), [1, 2, 1])
        XCTAssertEqual(contract.requiredObservedSeqByWriter, [writerA: 2, writerB: 1])
        XCTAssertEqual(contract.expectedDeletePrefixByWriter, [writerA: 2, writerB: 1])
        XCTAssertEqual(contract.preDeleteState.months[month]?.assets[fpA], state.assets[fpA])
        XCTAssertEqual(contract.preDeleteState.months[month]?.assets[fpB], state.assets[fpB])
        XCTAssertEqual(contract.preDeleteState.months[month]?.resources[resourceA.physicalRemotePath], state.resources[resourceA.physicalRemotePath])
    }

    func testReadOnlyClientSeesNoRemoteMutation() async throws {
        let inner = try await makeClient()
        try await writeCommits(client: inner, seqs: 1...2)
        let covered = coveredRanges([(1, 2)])
        let snapshot = try await writeSnapshot(client: inner, covered: covered)
        try await writeBarrier(client: inner, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        let before = await inner.snapshotFiles()
        let readOnly = ReadOnlyAssertingClient(inner: inner)

        let result = try await service(client: readOnly).makePlan(
            month: month,
            expectedRepoID: repoID,
            mode: .dryRun,
            nowMs: nowMs
        )

        _ = try requirePlan(result)
        let after = await inner.snapshotFiles()
        XCTAssertEqual(after, before)
        XCTAssertEqual(readOnly.mutatingCallCount, 0)
    }

    func testNoDeletePrimitiveOrRuntimeWiring() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let preflightSource = try source(root, "Shared/Services/Repo/RepoRetentionDeletePreflightService.swift")
        XCTAssertFalse(preflightSource.contains("client.delete("))
        XCTAssertFalse(preflightSource.contains(".delete("))
        XCTAssertFalse(preflightSource.contains("atomicCreate("))
        XCTAssertFalse(preflightSource.contains("upload("))
        XCTAssertFalse(preflightSource.contains("createDirectory("))
        XCTAssertFalse(preflightSource.contains("moveIfAbsent("))
        XCTAssertFalse(preflightSource.contains("move("))
        XCTAssertFalse(preflightSource.contains("copy("))

        for path in [
            "Shared/Services/Repo/BackupV2RuntimeBuilder.swift",
            "Shared/Services/Repo/RepoCheckpointBarrierHook.swift",
            "Shared/Services/Backup/V2MonthSession.swift",
            "Shared/Services/Backup/V2RetentionBarrierRefresh.swift"
        ] {
            let text = try source(root, path)
            XCTAssertFalse(text.contains("RepoRetentionDeletePreflightService"), "preflight unexpectedly wired in \(path)")
            XCTAssertFalse(text.contains("RepoRetentionDeletePreflightMode"), "preflight mode unexpectedly wired in \(path)")
        }

        let allowedPaths = Set([
            root.appendingPathComponent("Shared/Services/Repo/RepoRetentionDeletePreflightService.swift").path,
            root.appendingPathComponent("Shared/Services/Repo/RepoRetentionDeleteExecutor.swift").path
        ])
        let productionFiles = try swiftSources(root: root, under: "Shared") + swiftSources(root: root, under: "Watermelon")
        let callSites = try productionFiles.filter { !allowedPaths.contains($0.path) }.compactMap { url -> String? in
            let text = try String(contentsOf: url, encoding: .utf8)
            return text.contains("RepoRetentionDeletePreflightService")
                ? relativePath(root: root, url: url)
                : nil
        }
        XCTAssertEqual(callSites, [])
    }

    private func makeReadyClient() async throws -> InMemoryRemoteStorageClient {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: 1...1)
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        return client
    }

    private func makeClient(injectVersion: Bool = true) async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerA)
        if injectVersion {
            try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        }
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.retentionDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        return client
    }

    private func service(
        client: any RemoteStorageClientProtocol,
        peerStatusView: RetentionPeerStatusView = .empty,
        isLocalVolume: Bool = false,
        peerStatusProvider: RepoRetentionDeletePreflightService.PeerStatusProvider? = nil
    ) -> RepoRetentionDeletePreflightService {
        RepoRetentionDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: policy,
            isLocalVolume: isLocalVolume,
            peerStatusProvider: peerStatusProvider ?? { peerStatusView }
        )
    }

    private func writeCommits(
        client: any RemoteStorageClientProtocol,
        month: LibraryMonthKey? = nil,
        writerID: String? = nil,
        seqs: ClosedRange<UInt64>
    ) async throws {
        let targetMonth = month ?? self.month
        for seq in seqs {
            try await writeCommit(
                client: client,
                month: targetMonth,
                writerID: writerID ?? writerA,
                seq: seq,
                ops: []
            )
        }
    }

    private func writeCommit(
        client: any RemoteStorageClientProtocol,
        month: LibraryMonthKey? = nil,
        writerID: String,
        seq: UInt64,
        ops: [CommitOp]
    ) async throws {
        let targetMonth = month ?? self.month
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: targetMonth
            ),
            ops: ops,
            month: targetMonth,
            respectTaskCancellation: false
        )
    }

    @discardableResult
    private func writeSnapshot(
        client: any RemoteStorageClientProtocol,
        covered: CoveredRanges,
        lamport: UInt64? = nil,
        state: RepoMonthState = .empty
    ) async throws -> SnapshotFile {
        let targetLamport = lamport ?? snapshotLamport
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered
        )
        let rows = RepoSnapshotBuilder.build(header: header, state: state)
        return try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: rows.assets,
            resources: rows.resources,
            assetResources: rows.assetResources,
            deletedKeys: rows.deletedKeys,
            month: month,
            lamport: targetLamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private func writeBarrier(
        client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey? = nil,
        repoID: String? = nil,
        covered: CoveredRanges,
        lamport: UInt64? = nil,
        deletePrefixByWriter: [String: UInt64]? = nil,
        createdAtMs: Int64? = nil,
        checkpointSHA256Hex: String = String(repeating: "a", count: 64),
        observedSeqHighByWriter: [String: UInt64]? = nil,
        livenessGate: RetentionLivenessGate? = nil
    ) async throws {
        let targetMonth = month ?? self.month
        let targetRepoID = repoID ?? self.repoID
        let targetLamport = lamport ?? snapshotLamport
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: targetRepoID,
            month: targetMonth,
            createdByWriterID: writerA,
            runID: UUID(uuidString: runID)!,
            createdAtMs: createdAtMs ?? oldBarrierCreatedAtMs,
            barrierLamport: targetLamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(
                month: targetMonth,
                lamport: targetLamport,
                writerID: writerA,
                runID: runID
            ),
            checkpointSHA256Hex: checkpointSHA256Hex,
            coveredRanges: covered,
            deletePrefixByWriter: deletePrefixByWriter ?? policy.conservativeDeletePrefixByWriter(covered: covered),
            observedSeqHighByWriter: observedSeqHighByWriter ?? covered.rangesByWriter.mapValues { ranges in
                ranges.map(\.high).max() ?? 0
            },
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: policy.snapshotFallbackKeepCount
            ),
            livenessGate: livenessGate ?? RetentionLivenessGate(
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

    private func coveredRanges(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            writerA: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    private func coveredRanges(_ rangesByWriter: [String: [(UInt64, UInt64)]]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: rangesByWriter.mapValues { ranges in
            ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        })
    }

    private func addAssetOp(
        fingerprint: AssetFingerprint,
        clock: UInt64,
        resources: [CommitResourceEntry]
    ) -> CommitOp {
        CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: resources
        )))
    }

    private func requirePlan(_ result: RepoRetentionDeletePreflightResult) throws -> RepoRetentionDeletePreflightPlan {
        switch result {
        case .planned(let plan, _):
            return plan
        case .blocked(let blockers, _):
            XCTFail("expected plan, got blockers \(blockers)")
            throw NSError(domain: "RepoRetentionDeletePreflightTests", code: 1)
        }
    }

    private func blockers(in result: RepoRetentionDeletePreflightResult) -> [RepoRetentionDeletePreflightBlocker] {
        switch result {
        case .planned:
            XCTFail("expected blockers, got plan")
            return []
        case .blocked(let blockers, _):
            return blockers
        }
    }

    private func source(_ root: URL, _ path: String) throws -> String {
        try String(contentsOf: root.appendingPathComponent(path), encoding: .utf8)
    }

    private func swiftSources(root: URL, under relativePath: String) throws -> [URL] {
        let directory = root.appendingPathComponent(relativePath)
        guard let enumerator = FileManager.default.enumerator(
            at: directory,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }
        return try enumerator.compactMap { item in
            guard let url = item as? URL, url.pathExtension == "swift" else { return nil }
            let values = try url.resourceValues(forKeys: [.isRegularFileKey])
            return values.isRegularFile == true ? url : nil
        }
    }

    private func relativePath(root: URL, url: URL) -> String {
        let rootPath = root.path.hasSuffix("/") ? root.path : root.path + "/"
        return url.path.hasPrefix(rootPath) ? String(url.path.dropFirst(rootPath.count)) : url.path
    }

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let foreignRepoID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    private let writerA = "11111111-1111-1111-1111-111111111111"
    private let writerB = "22222222-2222-2222-2222-222222222222"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let otherMonth = LibraryMonthKey(year: 2026, month: 6)
    private let snapshotLamport: UInt64 = 10
    private let nowMs: Int64 = 1_800_000_000_000
    private var oldBarrierCreatedAtMs: Int64 { nowMs - Int64(policy.retentionStalenessThresholdSeconds + 1) * 1_000 }
    private let policy = RepoCompactionPolicy(
        checkpointCommitThreshold: 1,
        checkpointByteThreshold: 1,
        retentionStalenessThresholdSeconds: 60,
        snapshotFallbackKeepCount: 2
    )
}

private final class VanishingCandidateReadClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let vanishPath: String
    private let lock = NSLock()
    private var armed = true

    init(inner: InMemoryRemoteStorageClient, vanishPath: String) {
        self.inner = inner
        self.vanishPath = Self.normalize(vanishPath)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { true }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }
    var isSerialized: Bool { false }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
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
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        let normalized = Self.normalize(remotePath)
        let shouldVanish = lock.withLock { () -> Bool in
            guard armed, normalized == vanishPath else { return false }
            armed = false
            return true
        }
        if shouldVanish {
            try await inner.delete(path: remotePath)
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
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
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

private final class CommitListFailingClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let commitsDirectoryPath: String
    let failOnListCall: Int
    let error: Error
    private let lock = NSLock()
    private var commitListCalls = 0

    init(
        inner: InMemoryRemoteStorageClient,
        commitsDirectoryPath: String,
        failOnListCall: Int,
        error: Error = InMemoryRemoteStorageClient.InjectedError.transport
    ) {
        self.inner = inner
        self.commitsDirectoryPath = Self.normalize(commitsDirectoryPath)
        self.failOnListCall = failOnListCall
        self.error = error
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { true }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }
    var isSerialized: Bool { false }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        if Self.normalize(path) == commitsDirectoryPath {
            let shouldFail = lock.withLock { () -> Bool in
                commitListCalls += 1
                return commitListCalls == failOnListCall
            }
            if shouldFail {
                throw error
            }
        }
        return try await inner.list(path: path)
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
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
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

private final class ReadOnlyAssertingClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var mutations = 0

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    var mutatingCallCount: Int {
        lock.withLock { mutations }
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { true }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }
    var isSerialized: Bool { false }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try failMutation("upload")
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try failMutation("atomicCreate")
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try failMutation("setModificationDate")
    }
    func delete(path: String) async throws {
        try failMutation("delete")
    }
    func createDirectory(path: String) async throws {
        try failMutation("createDirectory")
    }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try failMutation("move")
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try failMutation("moveIfAbsent")
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try failMutation("copy")
    }

    private func failMutation(_ operation: String) throws -> Never {
        lock.withLock { mutations += 1 }
        XCTFail("preflight called mutating operation \(operation)")
        throw NSError(domain: "ReadOnlyAssertingClient", code: 1)
    }
}
