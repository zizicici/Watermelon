import XCTest
@testable import Watermelon

final class RepoPostDeleteLightweightVerificationTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-verify-test"
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: 2026, month: 5) }

    // MARK: - Commit GC lightweight verifier

    func testCommitGC_LightweightPassesWhenAcceptedSnapshotUnchanged() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = result {
            // expected
        } else {
            XCTFail("expected .passed, got \(result)")
        }
    }

    func testCommitGC_LightweightMatchesFullVerifier_HappyPath() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        let lightweight = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)
        let full = await RepoRetentionPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = lightweight { /* ok */ } else {
            XCTFail("lightweight should pass, got \(lightweight)")
        }
        if case .passed = full { /* ok */ } else {
            XCTFail("full should pass, got \(full)")
        }
    }

    func testCommitGC_BothFailOnMissingAcceptedSnapshot() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        let snapshotFilename = RepoLayout.snapshotFileName(
            month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        let snapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        try await client.delete(path: snapshotPath)

        let lightweight = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)
        let full = await RepoRetentionPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed = lightweight { /* expected */ } else {
            XCTFail("lightweight should fail on missing snapshot, got \(lightweight)")
        }
        if case .failed = full { /* expected */ } else {
            XCTFail("full should fail on missing snapshot, got \(full)")
        }
    }

    func testCommitGC_BothFailOnTamperedAcceptedSnapshot() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        let snapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        await client.corrupt(path: snapshotPath, with: Data("tampered\n".utf8))

        let lightweight = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)
        let full = await RepoRetentionPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed = lightweight { /* expected */ } else {
            XCTFail("lightweight should fail on tampered snapshot, got \(lightweight)")
        }
        if case .failed = full { /* expected */ } else {
            XCTFail("full should fail on tampered snapshot, got \(full)")
        }
    }

    func testCommitGC_LightweightFailsOnIdentityMismatch() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        let wrongRepoID = "00000000-0000-0000-0000-000000000000"
        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: wrongRepoID, contract: contract)

        if case .failed(reason: .repoIdentityMismatch(_, _), _) = result {
            // expected
        } else {
            XCTFail("expected .failed(.repoIdentityMismatch), got \(result)")
        }
    }

    func testCommitGC_LightweightFailsOnMissingIdentity() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // No identity file injected.
        let contract = RepoRetentionPostDeleteEquivalenceContract(
            mode: .retentionSuperset,
            acceptedSnapshotFilename: "nonexistent.jsonl",
            acceptedSnapshotSHA256Hex: "",
            acceptedSnapshotCovered: .empty,
            requiredObservedSeqByWriter: [:],
            expectedDeletePrefixByWriter: [:],
            preDeleteCovered: .empty,
            preDeleteState: RepoSnapshotState(months: [:], observedClock: 0)
        )
        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed(reason: .missingRepoIdentity(_), _) = result {
            // expected - absent identity is a definitive failure
        } else {
            XCTFail("expected .failed(.missingRepoIdentity), got \(result)")
        }
    }

    func testCommitGC_LightweightFailsOnCoveredRegression() async throws {
        let client = try await makeRepoWithBaseline()
        // Contract claims accepted covered was wider than it actually is.
        let inflatedCovered = CoveredRanges(rangesByWriter: [
            writerID: [ClosedSeqRange(low: 1, high: 100)]
        ])
        let contract = try await makeCommitGCContract(client: client, acceptedSnapshotCovered: inflatedCovered)

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed(reason: .acceptedSnapshotCoverageRegression(_), _) = result {
            // expected
        } else {
            XCTFail("expected .failed(.acceptedSnapshotCoverageRegression), got \(result)")
        }
    }

    func testCommitGC_LightweightFailsOnDeletePrefixBeyondCoverage() async throws {
        let client = try await makeRepoWithBaseline()
        // Delete prefix extends beyond accepted coverage.
        let beyondCovered = [writerID: UInt64(100)]
        let contract = try await makeCommitGCContract(client: client, deletePrefixByWriter: beyondCovered)

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed(reason: .deletePrefixCoverageRegression(_), _) = result {
            // expected
        } else {
            XCTFail("expected .failed(.deletePrefixCoverageRegression), got \(result)")
        }
    }

    // MARK: - Snapshot GC lightweight verifier

    func testSnapshotGC_LightweightPassesWhenAllProtectedUnchanged() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = result {
            // expected
        } else {
            XCTFail("expected .passed, got \(result)")
        }
    }

    func testSnapshotGC_FailsOnMissingProtectedSnapshot() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        // Delete a protected snapshot
        let protectedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 8, writerID: writerID, runID: runID
        )
        try await client.delete(path: protectedPath)

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed = result {
            // expected
        } else {
            XCTFail("expected .failed on missing protected snapshot, got \(result)")
        }
    }

    func testSnapshotGC_FailsOnTamperedAcceptedSnapshot() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        let acceptedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        await client.corrupt(path: acceptedPath, with: Data("tampered\n".utf8))

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed = result {
            // expected
        } else {
            XCTFail("expected .failed on tampered accepted, got \(result)")
        }
    }

    func testSnapshotGC_InconclusiveOnAcceptedReadFailure() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        let acceptedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        await client.injectDownloadError(.transport, for: acceptedPath)

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .inconclusive = result {
            // expected - read failure is inconclusive, not a definitive failure
        } else {
            XCTFail("expected .inconclusive on read failure, got \(result)")
        }
    }

    func testSnapshotGC_LightweightMatchesFull_HappyPath() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        let lightweight = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)
        let full = await RepoSnapshotPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = lightweight { /* ok */ } else {
            XCTFail("lightweight should pass, got \(lightweight)")
        }
        if case .passed = full { /* ok */ } else {
            XCTFail("full should pass, got \(full)")
        }
    }

    // MARK: - Equivalence: both verifiers agree on failure cases

    func testSnapshotGC_BothFailOnMissingAccepted() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        let acceptedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        try await client.delete(path: acceptedPath)

        let lightweight = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)
        let full = await RepoSnapshotPostDeleteVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .failed = lightweight { /* expected */ } else {
            XCTFail("lightweight should fail, got \(lightweight)")
        }
        if case .failed = full { /* expected */ } else {
            XCTFail("full should fail, got \(full)")
        }
    }

    // MARK: - Divergence: incomparable snapshot appears after preflight

    func testCommitGC_LightweightInconclusiveWhenIncomparableSnapshotAppears() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        // Inject an incomparable snapshot from a different writer with covered
        // that is NOT a subset of the accepted snapshot's covered.
        let otherWriterID = "33333333-3333-3333-3333-cccccccccccc"
        _ = try await writeSnapshot(
            client: client,
            lamport: 20,
            covered: CoveredRanges(rangesByWriter: [otherWriterID: [ClosedSeqRange(low: 1, high: 5)]]),
            writerID: otherWriterID,
            runID: "run-other"
        )

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .inconclusive = result {
            // expected - incomparable candidate detected
        } else {
            XCTFail("expected .inconclusive when incomparable snapshot appears, got \(result)")
        }
    }

    func testCommitGC_LightweightInconclusiveWhenHigherCoveredCandidateAppears() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        // Inject a snapshot from the same writer but with covered extending beyond accepted.
        _ = try await writeSnapshot(
            client: client,
            lamport: 20,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 10)]]),
            writerID: writerID,
            runID: "run-higher"
        )

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .inconclusive = result {
            // expected - higher covered candidate detected
        } else {
            XCTFail("expected .inconclusive when higher covered snapshot appears, got \(result)")
        }
    }

    func testCommitGC_ExecutorFallbackCatchesAmbiguity() async throws {
        // Verify the executor falls back to full verify when lightweight is inconclusive.
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        // Create a plan with the contract's evidence
        let plan = RepoRetentionDeletePreflightPlan(
            month: monthKey,
            repoID: repoID,
            acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo(
                filename: contract.acceptedSnapshotFilename,
                month: monthKey,
                lamport: baselineLamport,
                writerID: writerID,
                runIDPrefix: runID,
                covered: baselineCovered
            ),
            deletePrefixByWriter: [writerID: 3],
            commitFiles: [],
            protectedSummary: RepoRetentionProtectedSummary(),
            preDeleteEvidence: RepoRetentionPreDeleteEvidence(
                materializedState: RepoSnapshotState(months: [:], observedClock: baselineLamport),
                materializedCovered: baselineCovered,
                observedSeqByWriter: [writerID: 5],
                acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo(
                    filename: contract.acceptedSnapshotFilename,
                    month: monthKey,
                    lamport: baselineLamport,
                    writerID: writerID,
                    runIDPrefix: runID,
                    covered: baselineCovered
                ),
                postDeleteEquivalenceContract: contract
            )
        )

        // Inject incomparable snapshot AFTER plan creation (simulates race)
        let otherWriterID = "33333333-3333-3333-3333-cccccccccccc"
        _ = try await writeSnapshot(
            client: client,
            lamport: 20,
            covered: CoveredRanges(rangesByWriter: [otherWriterID: [ClosedSeqRange(low: 1, high: 5)]]),
            writerID: otherWriterID,
            runID: "run-race"
        )

        let report = RepoRetentionDeletePreflightReport(
            month: monthKey,
            repoID: repoID,
            mode: .dryRun,
            evaluatedAtMs: 0
        )

        let executor = RepoRetentionCommitDeleteExecutor(
            client: client,
            basePath: basePath,
            isLocalVolume: true
        )
        let result = try await executor.execute(plan: plan, report: report)

        // Executor should fall back to full verify which will see the incomparable snapshot.
        // The result should NOT be .completed (which lightweight alone would have returned).
        if case .completed = result {
            XCTFail("executor should not return .completed when incomparable snapshot appears during race")
        }
        // Either verificationFailed, verificationInconclusive, or stopped — all are safe.
    }

    func testSnapshotGC_LightweightInconclusiveWhenIncomparableSnapshotAppears() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        // Inject incomparable snapshot from different writer
        let otherWriterID = "33333333-3333-3333-3333-cccccccccccc"
        _ = try await writeSnapshot(
            client: client,
            lamport: 20,
            covered: CoveredRanges(rangesByWriter: [otherWriterID: [ClosedSeqRange(low: 1, high: 5)]]),
            writerID: otherWriterID,
            runID: "run-other"
        )

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .inconclusive = result {
            // expected - incomparable candidate detected
        } else {
            XCTFail("expected .inconclusive when incomparable snapshot appears, got \(result)")
        }
    }

    func testSnapshotGC_LightweightInconclusiveWhenHigherCoveredCandidateAppears() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        // Inject snapshot with higher covered from same writer
        _ = try await writeSnapshot(
            client: client,
            lamport: 20,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 10)]]),
            writerID: writerID,
            runID: "run-higher"
        )

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .inconclusive = result {
            // expected - higher covered candidate detected
        } else {
            XCTFail("expected .inconclusive when higher covered snapshot appears, got \(result)")
        }
    }

    // MARK: - Subset candidates are OK (no incomparability)

    func testCommitGC_LightweightPassesWithSubsetCandidate() async throws {
        let client = try await makeRepoWithBaseline()
        let contract = try await makeCommitGCContract(client: client)

        // Inject a snapshot whose covered is a proper subset of accepted covered.
        _ = try await writeSnapshot(
            client: client,
            lamport: 7,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]]),
            writerID: writerID,
            runID: "run-subset"
        )

        let result = await RepoRetentionPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = result {
            // expected - subset candidate doesn't challenge accepted authority
        } else {
            XCTFail("expected .passed with subset candidate, got \(result)")
        }
    }

    func testSnapshotGC_LightweightPassesWithSubsetCandidate() async throws {
        let client = try await makeRepoWithBaselineAndProtected()
        let contract = try await makeSnapshotGCContract(client: client)

        // Inject a snapshot whose covered is a proper subset of accepted covered.
        _ = try await writeSnapshot(
            client: client,
            lamport: 7,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]]),
            writerID: writerID,
            runID: "run-subset"
        )

        let result = await RepoSnapshotPostDeleteLightweightVerifier(client: client, basePath: basePath)
            .verify(month: monthKey, expectedRepoID: repoID, contract: contract)

        if case .passed = result {
            // expected - subset candidate doesn't challenge accepted authority
        } else {
            XCTFail("expected .passed with subset candidate, got \(result)")
        }
    }

    // MARK: - Helpers

    private let baselineLamport: UInt64 = 10

    private var baselineCovered: CoveredRanges {
        CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 5)]])
    }

    /// Sets up repo with identity, version, and one baseline snapshot covering seq [1,5].
    private func makeRepoWithBaseline() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        _ = try await writeSnapshot(client: client, lamport: baselineLamport, covered: baselineCovered)
        return client
    }

    /// Sets up repo with identity, version, a baseline snapshot, and a second protected snapshot.
    private func makeRepoWithBaselineAndProtected() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // Protected snapshot with lower covered
        _ = try await writeSnapshot(
            client: client,
            lamport: 8,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]])
        )

        // Accepted baseline with higher covered
        _ = try await writeSnapshot(client: client, lamport: baselineLamport, covered: baselineCovered)

        return client
    }

    @discardableResult
    private func writeSnapshot(
        client: InMemoryRemoteStorageClient,
        lamport: UInt64,
        covered: CoveredRanges,
        writerID: String? = nil,
        runID: String? = nil
    ) async throws -> SnapshotFile {
        let wID = writerID ?? self.writerID
        let rID = runID ?? self.runID
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: wID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: .empty)
        return try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: lamport,
            runID: rID,
            respectTaskCancellation: true
        )
    }

    private func makeCommitGCContract(
        client: InMemoryRemoteStorageClient,
        acceptedSnapshotCovered: CoveredRanges? = nil,
        deletePrefixByWriter: [String: UInt64]? = nil
    ) async throws -> RepoRetentionPostDeleteEquivalenceContract {
        let snapshotFilename = RepoLayout.snapshotFileName(
            month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        let snapshotFile = try await SnapshotReader(client: client, basePath: basePath)
            .read(filename: snapshotFilename)
        let covered = acceptedSnapshotCovered ?? baselineCovered
        return RepoRetentionPostDeleteEquivalenceContract(
            mode: .retentionSuperset,
            acceptedSnapshotFilename: snapshotFilename,
            acceptedSnapshotSHA256Hex: snapshotFile.sha256Hex.lowercased(),
            acceptedSnapshotCovered: covered,
            requiredObservedSeqByWriter: [writerID: 5],
            expectedDeletePrefixByWriter: deletePrefixByWriter ?? [writerID: 3],
            preDeleteCovered: baselineCovered,
            preDeleteState: RepoSnapshotState(months: [:], observedClock: baselineLamport)
        )
    }

    private func makeSnapshotGCContract(
        client: InMemoryRemoteStorageClient
    ) async throws -> RepoSnapshotPostDeleteEquivalenceContract {
        let acceptedFilename = RepoLayout.snapshotFileName(
            month: monthKey, lamport: baselineLamport, writerID: writerID, runID: runID
        )
        let acceptedFile = try await SnapshotReader(client: client, basePath: basePath)
            .read(filename: acceptedFilename)

        let protectedFilename = RepoLayout.snapshotFileName(
            month: monthKey, lamport: 8, writerID: writerID, runID: runID
        )
        let protectedFile = try await SnapshotReader(client: client, basePath: basePath)
            .read(filename: protectedFilename)

        return RepoSnapshotPostDeleteEquivalenceContract(
            acceptedSnapshotFilename: acceptedFilename,
            acceptedSnapshotLamport: baselineLamport,
            acceptedSnapshotSHA256Hex: acceptedFile.sha256Hex.lowercased(),
            acceptedSnapshotCovered: baselineCovered,
            additionalProtectedSnapshotSHA256ByFilename: [
                protectedFilename: protectedFile.sha256Hex.lowercased()
            ],
            requiredObservedSeqByWriter: [writerID: 5],
            preDeleteCovered: baselineCovered,
            preDeleteState: RepoSnapshotState(months: [:], observedClock: baselineLamport),
            preDeleteObservedClock: baselineLamport
        )
    }
}
