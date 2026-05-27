import Foundation
import XCTest
@testable import Watermelon

final class RepoSnapshotProtectionSetTests: XCTestCase {

    // MARK: - Protected set composition

    func testAcceptedBaselineAlwaysProtected() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineLamport: 100,
            barrierReferencedFilenames: [],
            parseableSnapshotsForMonth: [],
            snapshotKeepCount: 0
        ))
        XCTAssertTrue(result.protectedFilenames.contains("baseline.jsonl"))
        XCTAssertTrue(result.deleteCandidateFilenames.isEmpty)
    }

    func testBarrierReferencedSnapshotsAlwaysProtectedEvenIfBehindBaseline() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "newest.jsonl",
            acceptedBaselineLamport: 200,
            barrierReferencedFilenames: ["old-but-pinned.jsonl"],
            parseableSnapshotsForMonth: [
                .init(filename: "old-but-pinned.jsonl", lamport: 50, writerID: writer)
            ],
            snapshotKeepCount: 0
        ))
        XCTAssertTrue(result.protectedFilenames.contains("old-but-pinned.jsonl"))
        XCTAssertFalse(result.deleteCandidateFilenames.contains("old-but-pinned.jsonl"))
    }

    func testSnapshotKeepCountRetainsMostRecentParseable() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "s400.jsonl",
            acceptedBaselineLamport: 400,
            barrierReferencedFilenames: [],
            parseableSnapshotsForMonth: [
                .init(filename: "s100.jsonl", lamport: 100, writerID: writer),
                .init(filename: "s200.jsonl", lamport: 200, writerID: writer),
                .init(filename: "s300.jsonl", lamport: 300, writerID: writer),
                .init(filename: "s400.jsonl", lamport: 400, writerID: writer)
            ],
            snapshotKeepCount: 2
        ))
        // Top 2 lamports = 400, 300 — both protected.
        XCTAssertTrue(result.protectedFilenames.contains("s400.jsonl"))
        XCTAssertTrue(result.protectedFilenames.contains("s300.jsonl"))
        // Older ones (100, 200) are strictly behind baseline and not protected → candidates.
        XCTAssertEqual(result.deleteCandidateFilenames, ["s100.jsonl", "s200.jsonl"])
    }

    func testSnapshotKeepCountZeroProtectsOnlyBaselineAndBarrierRefs() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineLamport: 100,
            barrierReferencedFilenames: ["pinned.jsonl"],
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer),
                .init(filename: "b.jsonl", lamport: 20, writerID: writer),
                .init(filename: "pinned.jsonl", lamport: 30, writerID: writer)
            ],
            snapshotKeepCount: 0
        ))
        XCTAssertEqual(result.protectedFilenames, ["baseline.jsonl", "pinned.jsonl"])
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl", "b.jsonl"])
    }

    func testNegativeSnapshotKeepCountClampedToZero() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "baseline.jsonl",
            acceptedBaselineLamport: 100,
            barrierReferencedFilenames: [],
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer)
            ],
            snapshotKeepCount: -5
        ))
        XCTAssertEqual(result.protectedFilenames, ["baseline.jsonl"])
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl"])
    }

    // MARK: - Candidate eligibility

    func testCandidatesMustBeStrictlyBehindBaselineLamport() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "s100.jsonl",
            acceptedBaselineLamport: 100,
            barrierReferencedFilenames: [],
            parseableSnapshotsForMonth: [
                .init(filename: "s50.jsonl", lamport: 50, writerID: writer),
                .init(filename: "s100.jsonl", lamport: 100, writerID: writer),
                // Hypothetical concurrent baseline at same lamport: never delete.
                .init(filename: "s100b.jsonl", lamport: 100, writerID: writer),
                // Future baseline (would be the new accepted): never delete.
                .init(filename: "s150.jsonl", lamport: 150, writerID: writer)
            ],
            snapshotKeepCount: 0
        ))
        // Only strictly < 100 may be deleted; s100/s100b/s150 are not candidates.
        XCTAssertEqual(result.deleteCandidateFilenames, ["s50.jsonl"])
    }

    func testCandidateOrderingDeterministicLamportThenFilename() {
        let result = RepoSnapshotProtectionSet.compute(.init(
            acceptedBaselineFilename: "z.jsonl",
            acceptedBaselineLamport: 1000,
            barrierReferencedFilenames: [],
            parseableSnapshotsForMonth: [
                .init(filename: "b.jsonl", lamport: 50, writerID: writer),
                .init(filename: "a.jsonl", lamport: 50, writerID: writer),
                .init(filename: "c.jsonl", lamport: 10, writerID: writer)
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
            acceptedBaselineLamport: 100,
            barrierReferencedFilenames: ["pinned.jsonl"],
            parseableSnapshotsForMonth: [
                .init(filename: "a.jsonl", lamport: 10, writerID: writer),
                .init(filename: "pinned.jsonl", lamport: 20, writerID: writer),
                .init(filename: "b.jsonl", lamport: 30, writerID: writer)
            ],
            snapshotKeepCount: 1
        ))
        // KeepN=1 protects the newest parseable (b.jsonl, lamport 30).
        // pinned.jsonl protected by barrier-ref.
        // baseline.jsonl protected always.
        // a.jsonl is the only remaining candidate.
        XCTAssertTrue(result.protectedFilenames.contains("baseline.jsonl"))
        XCTAssertTrue(result.protectedFilenames.contains("pinned.jsonl"))
        XCTAssertTrue(result.protectedFilenames.contains("b.jsonl"))
        XCTAssertEqual(result.deleteCandidateFilenames, ["a.jsonl"])
    }

    private let writer = "11111111-1111-1111-1111-111111111111"
}
