import XCTest
@testable import Watermelon

final class CoveredRangesTests: XCTestCase {
    func testContainsSeqWithinSingleRange() {
        var r = CoveredRanges()
        r.add(writerID: "A", range: ClosedSeqRange(low: 1, high: 10))
        XCTAssertTrue(r.contains(writerID: "A", seq: 1))
        XCTAssertTrue(r.contains(writerID: "A", seq: 5))
        XCTAssertTrue(r.contains(writerID: "A", seq: 10))
        XCTAssertFalse(r.contains(writerID: "A", seq: 11))
        XCTAssertFalse(r.contains(writerID: "A", seq: 0))
        XCTAssertFalse(r.contains(writerID: "B", seq: 5))
    }

    func testGapMembershipNotMisreadAsBeyondMax() {
        var r = CoveredRanges()
        r.add(writerID: "A", range: ClosedSeqRange(low: 1, high: 5))
        r.add(writerID: "A", range: ClosedSeqRange(low: 12, high: 15))
        XCTAssertFalse(r.contains(writerID: "A", seq: 6))
        XCTAssertFalse(r.contains(writerID: "A", seq: 10))
        XCTAssertFalse(r.contains(writerID: "A", seq: 11))
        XCTAssertTrue(r.contains(writerID: "A", seq: 12))
    }

    func testMergeAdjacentRanges() {
        var r = CoveredRanges()
        r.add(writerID: "A", range: ClosedSeqRange(low: 1, high: 5))
        r.add(writerID: "A", range: ClosedSeqRange(low: 6, high: 10))
        XCTAssertEqual(r.rangesByWriter["A"], [ClosedSeqRange(low: 1, high: 10)])
    }

    func testMergeOverlappingRanges() {
        var r = CoveredRanges()
        r.add(writerID: "A", range: ClosedSeqRange(low: 1, high: 5))
        r.add(writerID: "A", range: ClosedSeqRange(low: 4, high: 10))
        XCTAssertEqual(r.rangesByWriter["A"], [ClosedSeqRange(low: 1, high: 10)])
    }

    func testSupersetSameWriter() {
        let outer = CoveredRanges(rangesByWriter: ["A": [ClosedSeqRange(low: 1, high: 100)]])
        let inner = CoveredRanges(rangesByWriter: ["A": [ClosedSeqRange(low: 5, high: 10)]])
        XCTAssertTrue(outer.superset(of: inner))
        XCTAssertFalse(inner.superset(of: outer))
    }

    func testSupersetMultipleWriters() {
        let outer = CoveredRanges(rangesByWriter: [
            "A": [ClosedSeqRange(low: 1, high: 100)],
            "B": [ClosedSeqRange(low: 1, high: 50)]
        ])
        let inner = CoveredRanges(rangesByWriter: [
            "A": [ClosedSeqRange(low: 10, high: 20)],
            "B": [ClosedSeqRange(low: 1, high: 1)]
        ])
        XCTAssertTrue(outer.superset(of: inner))
    }

    func testSupersetMissingWriter() {
        let outer = CoveredRanges(rangesByWriter: ["A": [ClosedSeqRange(low: 1, high: 100)]])
        let inner = CoveredRanges(rangesByWriter: ["B": [ClosedSeqRange(low: 1, high: 1)]])
        XCTAssertFalse(outer.superset(of: inner))
    }

    func testEncodeDecodeRoundTrip() {
        let original = CoveredRanges(rangesByWriter: [
            "writer-A": [ClosedSeqRange(low: 1, high: 5), ClosedSeqRange(low: 10, high: 12)],
            "writer-B": [ClosedSeqRange(low: 100, high: 200)]
        ])
        let encoded = original.encodedAsRangeArrayMap()
        let decoded = CoveredRanges.decode(encoded)
        XCTAssertEqual(decoded.rangesByWriter, original.rangesByWriter)
    }

    func testMergingTwoCoveredRanges() {
        let lhs = CoveredRanges(rangesByWriter: ["A": [ClosedSeqRange(low: 1, high: 5)]])
        let rhs = CoveredRanges(rangesByWriter: ["A": [ClosedSeqRange(low: 10, high: 15)], "B": [ClosedSeqRange(low: 1, high: 1)]])
        let merged = lhs.merging(rhs)
        XCTAssertEqual(merged.rangesByWriter["A"], [ClosedSeqRange(low: 1, high: 5), ClosedSeqRange(low: 10, high: 15)])
        XCTAssertEqual(merged.rangesByWriter["B"], [ClosedSeqRange(low: 1, high: 1)])
    }
}
