import XCTest
@testable import Watermelon

final class RepoCompactionPolicyTests: XCTestCase {
    func testDefaultPolicyUsesBackupV2Constants() {
        let policy = RepoCompactionPolicy.default
        XCTAssertEqual(policy.checkpointCommitThreshold, 5_000)
        XCTAssertEqual(policy.checkpointByteThreshold, 16 * 1024 * 1024)
        XCTAssertEqual(policy.minimumCheckpointIntervalSeconds, 6 * 60 * 60)
        XCTAssertEqual(policy.retentionStalenessThresholdSeconds, 24 * 60 * 60)
        XCTAssertEqual(policy.snapshotFallbackKeepCount, 2)
    }

    func testRetentionStalenessThresholdExceedsLivenessStaleThreshold() {
        XCTAssertGreaterThan(
            TimeInterval(RepoCompactionPolicy.default.retentionStalenessThresholdSeconds),
            LivenessTracker.staleThreshold
        )
    }

    func testConservativePrefixEmptyInput() {
        XCTAssertEqual(CoveredRanges.empty.conservativeContiguousPrefixByWriter(), [:])
    }

    func testConservativePrefixSingleRangeFromOne() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 1, high: 1_000)]
        ])
        XCTAssertEqual(covered.conservativeContiguousPrefixByWriter(), [writerA: 1_000])
    }

    func testConservativePrefixStopsAtGap() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [
                ClosedSeqRange(low: 1, high: 500),
                ClosedSeqRange(low: 700, high: 800)
            ]
        ])
        XCTAssertEqual(covered.conservativeContiguousPrefixByWriter(), [writerA: 500])
    }

    func testConservativePrefixOmitsWriterWithoutSeqOne() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 5, high: 10)]
        ])
        XCTAssertEqual(covered.conservativeContiguousPrefixByWriter(), [:])
    }

    func testConservativePrefixAllowsSingleSeqOne() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 1, high: 1)]
        ])
        XCTAssertEqual(covered.conservativeContiguousPrefixByWriter(), [writerA: 1])
    }

    func testConservativePrefixHandlesWritersIndependently() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [
                ClosedSeqRange(low: 1, high: 5),
                ClosedSeqRange(low: 9, high: 10)
            ],
            writerB: [ClosedSeqRange(low: 1, high: 20)]
        ])
        XCTAssertEqual(covered.conservativeContiguousPrefixByWriter(), [
            writerA: 5,
            writerB: 20
        ])
    }

    func testConservativePrefixDoesNotIncreaseForNonExtendingRanges() {
        let original = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 1, high: 5)]
        ])
        let added = original.merging(CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 10, high: 20)]
        ]))
        XCTAssertEqual(original.conservativeContiguousPrefixByWriter()[writerA], 5)
        XCTAssertEqual(added.conservativeContiguousPrefixByWriter()[writerA], 5)
    }

    func testPolicyDeletePrefixUsesAbsenceForNoPrefix() {
        let covered = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 2, high: 8)]
        ])
        XCTAssertNil(RepoCompactionPolicy.default.conservativeDeletePrefixByWriter(covered: covered)[writerA])
    }

    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
}
