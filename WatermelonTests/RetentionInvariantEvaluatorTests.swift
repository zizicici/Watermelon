import XCTest
@testable import Watermelon

final class RetentionInvariantEvaluatorTests: XCTestCase {

    // MARK: - Happy path

    func testHappyPath_AllSevenGuardsPass_ReturnsPassedOutcome() {
        let evidence = makeEvidence()
        let contract = makeContract()
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .passed)
    }

    // MARK: - Per-guard failure cases

    func testGuard1_AcceptedSnapshotMissingContractCoverage_ReturnsAcceptedSnapshotCoverageRegression() {
        let evidence = makeEvidence()
        // Contract demands a covered range the accepted snapshot does not cover.
        let contract = makeContract(acceptedSnapshotCovered: covered([(1, 9)]))
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .acceptedSnapshotCoverageRegression(filename: acceptedSnapshotFilename)))
    }

    func testGuard2_AcceptedSnapshotMissingRetainedBarrierCoverage_ReturnsRetainedBarrierCoverageRegression() {
        let evidence = makeEvidence()
        let contract = makeContract(retainedBarrierUnionCovered: covered([(1, 9)]))
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .retainedBarrierCoverageRegression(filename: acceptedSnapshotFilename)))
    }

    func testGuard3_AcceptedSnapshotMissingDeletePrefixCoverage_ReturnsDeletePrefixCoverageRegression() {
        let evidence = makeEvidence()
        // Delete-prefix 9 → covered range [1, 9], which the accepted snapshot ([1, 5]) does not cover.
        let contract = makeContract(expectedDeletePrefixByWriter: [writerA: 9])
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .deletePrefixCoverageRegression(filename: acceptedSnapshotFilename)))
    }

    func testGuard4_StateNotRetentionSuperset_AssetRemovedAfter_ReturnsStateNotRetentionSuperset() {
        let evidence = makeEvidence()
        let contract = makeContract()
        // After state has an empty asset dictionary while pre-delete contained `fp`.
        var afterMonth = monthStateWithAsset()
        afterMonth.assets = [:]
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: RepoSnapshotState(months: [month: afterMonth], observedClock: preDeleteObservedClock),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .stateNotRetentionSuperset))
    }

    func testGuard4_StateNotRetentionSuperset_DeletedFingerprintsShrink_ReturnsStateNotRetentionSuperset() {
        let evidence = makeEvidence()
        let contract = makeContract()
        var afterMonth = monthStateWithAsset()
        afterMonth.deletedAssetStamps = [:]
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: RepoSnapshotState(months: [month: afterMonth], observedClock: preDeleteObservedClock),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .stateNotRetentionSuperset))
    }

    func testGuard5_MaterializedCoveredShrunkFromPreDelete_ReturnsCoveredRangeRegression() {
        // evidence.materializedCovered is [1, 4]; pre-delete is [1, 5] → guard 5 violation.
        let evidence = makeEvidence(materializedCovered: covered([(1, 4)]))
        let contract = makeContract()
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .coveredRangeRegression))
    }

    func testGuard6_ObservedSeqRegressionForFirstSortedWriter_ReturnsTypedPayload() {
        // Two writers regress; sorted iteration must pick writerA first (alphabetic).
        let evidence = makeEvidence(observedSeqByWriter: [writerA: 3, writerB: 1])
        let contract = makeContract(requiredObservedSeqByWriter: [writerA: 5, writerB: 2])
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .observedSeqRegression(writerID: writerA, expectedAtLeast: 5, observed: 3)))
    }

    func testGuard7_ObservedClockRegression_ReturnsTypedPayload() {
        // afterState clock drops below pre-delete; evidence.observedClock mirrors afterState
        // to make guards 1–6 pass before guard 7 fires.
        let evidence = makeEvidence(observedClock: preDeleteObservedClock - 1)
        let contract = makeContract()
        var afterMonth = monthStateWithAsset()
        afterMonth.assets = preDeleteMonthAssets
        let after = RepoSnapshotState(months: [month: afterMonth], observedClock: preDeleteObservedClock - 1)
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: after,
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .observedClockRegression(
            expectedAtLeast: preDeleteObservedClock,
            observed: preDeleteObservedClock - 1
        )))
    }

    // MARK: - Check-order pins (4 tests covering 1v2, 1v3, 4v5, 6v7)

    func testCheckOrderPin_Guard1BeatsGuard2_BothAcceptedSnapshotCovered() {
        let evidence = makeEvidence()
        // Both guard 1 and guard 2 are violated; guard 1 case must win.
        let contract = makeContract(
            acceptedSnapshotCovered: covered([(1, 9)]),
            retainedBarrierUnionCovered: covered([(1, 9)])
        )
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .acceptedSnapshotCoverageRegression(filename: acceptedSnapshotFilename)))
    }

    func testCheckOrderPin_Guard1BeatsGuard3_AcceptedSnapshotCoverageBeatsDeletePrefix() {
        let evidence = makeEvidence()
        // Guards 1 and 3 both violated; guard 1 must win.
        let contract = makeContract(
            acceptedSnapshotCovered: covered([(1, 9)]),
            expectedDeletePrefixByWriter: [writerA: 9]
        )
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: afterStateMatchingPreDelete(),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .acceptedSnapshotCoverageRegression(filename: acceptedSnapshotFilename)))
    }

    func testCheckOrderPin_Guard4BeatsGuard5_StateBeatsCoveredRegression() {
        // Guard 4 (state regression: empty assets) and guard 5 (materializedCovered shrunk).
        // Guard 4 must win.
        let evidence = makeEvidence(materializedCovered: covered([(1, 4)]))
        let contract = makeContract()
        var afterMonth = monthStateWithAsset()
        afterMonth.assets = [:]
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: RepoSnapshotState(months: [month: afterMonth], observedClock: preDeleteObservedClock),
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .stateNotRetentionSuperset))
    }

    func testCheckOrderPin_Guard6BeatsGuard7_SeqBeatsClockRegression() {
        // Guard 6 (seq regression) and guard 7 (clock regression). Guard 6 must win.
        let evidence = makeEvidence(
            observedSeqByWriter: [writerA: 3],
            observedClock: preDeleteObservedClock - 1
        )
        let contract = makeContract(requiredObservedSeqByWriter: [writerA: 5])
        var afterMonth = monthStateWithAsset()
        afterMonth.assets = preDeleteMonthAssets
        let after = RepoSnapshotState(months: [month: afterMonth], observedClock: preDeleteObservedClock - 1)
        let outcome = RetentionInvariantEvaluator.evaluatePostDeleteContract(
            evidence: evidence,
            afterState: after,
            month: month,
            contract: contract
        )
        XCTAssertEqual(outcome, .failed(reason: .observedSeqRegression(writerID: writerA, expectedAtLeast: 5, observed: 3)))
    }

    // MARK: - Fixtures

    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let acceptedSnapshotFilename = "snapshot-fixture.jsonl"
    private let preDeleteObservedClock: UInt64 = 100
    private var assetFP: AssetFingerprint { TestFixtures.assetFingerprint(0xA1) }

    private var preDeleteMonthAssets: [AssetFingerprint: SnapshotAssetRow] {
        [assetFP: SnapshotAssetRow(
            assetFingerprint: assetFP,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 0,
            totalFileSizeBytes: 0,
            stamp: OpStamp(writerID: writerA, seq: 5, clock: 100)
        )]
    }

    private func monthStateWithAsset() -> RepoMonthState {
        RepoMonthState(
            assets: preDeleteMonthAssets,
            resources: [:],
            assetResources: [:],
            deletedAssetStamps: [
                TestFixtures.assetFingerprint(0xA2): OpStamp(writerID: writerA, seq: 4, clock: 90)
            ]
        )
    }

    private func afterStateMatchingPreDelete() -> RepoSnapshotState {
        RepoSnapshotState(months: [month: monthStateWithAsset()], observedClock: preDeleteObservedClock)
    }

    private func covered(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [writerA: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }])
    }

    private func makeAcceptedSnapshot(covered: CoveredRanges? = nil) -> RepoMaterializer.AcceptedSnapshotBaselineInfo {
        RepoMaterializer.AcceptedSnapshotBaselineInfo(
            filename: acceptedSnapshotFilename,
            month: month,
            lamport: 100,
            writerID: writerA,
            runIDPrefix: String(repoID.prefix(8)),
            covered: covered ?? self.covered([(1, 5)])
        )
    }

    private func makeEvidence(
        acceptedSnapshotCovered: CoveredRanges? = nil,
        materializedCovered: CoveredRanges? = nil,
        observedSeqByWriter: [String: UInt64]? = nil,
        observedClock: UInt64? = nil
    ) -> RepoRetentionPostDeleteVerificationEvidence {
        RepoRetentionPostDeleteVerificationEvidence(
            acceptedSnapshot: makeAcceptedSnapshot(covered: acceptedSnapshotCovered),
            materializedCovered: materializedCovered ?? covered([(1, 5)]),
            observedSeqByWriter: observedSeqByWriter ?? [writerA: 5],
            observedClock: observedClock ?? preDeleteObservedClock
        )
    }

    private func makeContract(
        acceptedSnapshotCovered: CoveredRanges? = nil,
        retainedBarrierUnionCovered: CoveredRanges? = nil,
        expectedDeletePrefixByWriter: [String: UInt64]? = nil,
        requiredObservedSeqByWriter: [String: UInt64]? = nil,
        preDeleteCovered: CoveredRanges? = nil,
        preDeleteState: RepoSnapshotState? = nil
    ) -> RepoRetentionPostDeleteEquivalenceContract {
        RepoRetentionPostDeleteEquivalenceContract(
            mode: .retentionSuperset,
            acceptedSnapshotFilename: acceptedSnapshotFilename,
            acceptedSnapshotCovered: acceptedSnapshotCovered ?? covered([(1, 5)]),
            retainedBarrierUnionCovered: retainedBarrierUnionCovered ?? covered([(1, 5)]),
            requiredObservedSeqByWriter: requiredObservedSeqByWriter ?? [writerA: 5],
            expectedDeletePrefixByWriter: expectedDeletePrefixByWriter ?? [writerA: 3],
            preDeleteCovered: preDeleteCovered ?? covered([(1, 5)]),
            preDeleteState: preDeleteState ?? RepoSnapshotState(
                months: [month: monthStateWithAsset()],
                observedClock: preDeleteObservedClock
            )
        )
    }
}
