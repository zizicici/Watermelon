import Foundation
import XCTest
@testable import Watermelon

final class RepoSnapshotProtectionSetTests: XCTestCase {

    // MARK: - Protected set composition

    func testAcceptedBaselineAlwaysProtected() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 10)]),
            parseableSnapshotsForMonth: [],
            snapshotKeepCount: 0
        ))
        XCTAssertTrue(result.protectedFilenames.contains("baseline.jsonl"))
        XCTAssertTrue(result.deleteCandidateFilenames.isEmpty)
    }

    func testSnapshotKeepCountRetainsMostRecentParseable() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "s400.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 400)]),
            parseableSnapshotsForMonth: [
                .init(filename: "s100.jsonl", lamport: 100, writerID: writer, covered: covered(writer: writer, ranges: [(1, 100)])),
                .init(filename: "s200.jsonl", lamport: 200, writerID: writer, covered: covered(writer: writer, ranges: [(1, 200)])),
                .init(filename: "s300.jsonl", lamport: 300, writerID: writer, covered: covered(writer: writer, ranges: [(1, 300)])),
                .init(filename: "s400.jsonl", lamport: 400, writerID: writer, covered: covered(writer: writer, ranges: [(1, 400)]))
            ],
            snapshotKeepCount: 2
        ))
        // Top 2 lamports = 400, 300 — both protected.
        XCTAssertTrue(result.protectedFilenames.contains("s400.jsonl"))
        XCTAssertTrue(result.protectedFilenames.contains("s300.jsonl"))
        // Older ones (100, 200) are subsets of baseline coverage and not protected → candidates.
        XCTAssertEqual(result.deleteCandidateFilenames, ["s100.jsonl", "s200.jsonl"])
    }

    func testSnapshotKeepCountZeroProtectsOnlyBaseline() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 100)]),
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer, covered: covered(writer: writer, ranges: [(1, 10)])),
                .init(filename: "b.jsonl", lamport: 20, writerID: writer, covered: covered(writer: writer, ranges: [(1, 20)]))
            ],
            snapshotKeepCount: 0
        ))
        XCTAssertEqual(result.protectedFilenames, ["baseline.jsonl"])
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl", "b.jsonl"])
    }

    func testNegativeSnapshotKeepCountClampedToZero() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 100)]),
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer, covered: covered(writer: writer, ranges: [(1, 10)]))
            ],
            snapshotKeepCount: -5
        ))
        XCTAssertEqual(result.protectedFilenames, ["baseline.jsonl"])
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl"])
    }

    // MARK: - Candidate eligibility

    func testNonSubsetCoverageIsNotCandidate() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 50)]),
            parseableSnapshotsForMonth: [
                .init(filename: "wider.jsonl", lamport: 10, writerID: writer, covered: covered(writer: writer, ranges: [(1, 100)])),
                .init(filename: "subset.jsonl", lamport: 20, writerID: writer, covered: covered(writer: writer, ranges: [(1, 30)]))
            ],
            snapshotKeepCount: 0
        ))
        // wider.jsonl extends beyond baseline → not a candidate.
        XCTAssertFalse(result.deleteCandidateFilenames.contains("wider.jsonl"))
        // subset.jsonl is fully within baseline → candidate.
        XCTAssertEqual(result.deleteCandidateFilenames, ["subset.jsonl"])
    }

    func testCandidateOrderingDeterministicLamportThenFilename() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "z.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 1000)]),
            parseableSnapshotsForMonth: [
                .init(filename: "b.jsonl", lamport: 50, writerID: writer, covered: covered(writer: writer, ranges: [(1, 50)])),
                .init(filename: "a.jsonl", lamport: 50, writerID: writer, covered: covered(writer: writer, ranges: [(1, 50)])),
                .init(filename: "c.jsonl", lamport: 10, writerID: writer, covered: covered(writer: writer, ranges: [(1, 10)]))
            ],
            snapshotKeepCount: 0
        ))
        // Lamport asc, filename asc within ties.
        XCTAssertEqual(result.deleteCandidateFilenames, ["c.jsonl", "a.jsonl", "b.jsonl"])
    }

    // MARK: - Combined

    func testProtectedOverridesEligibility() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineCovered: covered(writer: writer, ranges: [(1, 100)]),
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer, covered: covered(writer: writer, ranges: [(1, 10)])),
                .init(filename: "b.jsonl", lamport: 30, writerID: writer, covered: covered(writer: writer, ranges: [(1, 30)]))
            ],
            snapshotKeepCount: 1
        ))
        // KeepN=1 protects the newest parseable (b.jsonl, lamport 30).
        // baseline.jsonl protected always.
        // a.jsonl is the only remaining candidate.
        XCTAssertTrue(result.protectedFilenames.contains("baseline.jsonl"))
        XCTAssertTrue(result.protectedFilenames.contains("b.jsonl"))
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl"])
    }

    private let writer = "11111111-1111-1111-1111-111111111111"

    private func covered(writer: String, ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [writer: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }])
    }
}
