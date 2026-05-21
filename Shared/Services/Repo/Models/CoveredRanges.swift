import Foundation

struct CoveredRanges: Equatable, Sendable {
    typealias Range = ClosedSeqRange

    private(set) var rangesByWriter: [String: [Range]]

    init(rangesByWriter: [String: [Range]] = [:]) {
        var normalized: [String: [Range]] = [:]
        for (writer, ranges) in rangesByWriter {
            normalized[writer] = Self.merge(ranges)
        }
        self.rangesByWriter = normalized
    }

    static var empty: CoveredRanges { CoveredRanges() }

    func contains(writerID: String, seq: UInt64) -> Bool {
        guard let ranges = rangesByWriter[writerID] else { return false }
        return Self.contains(ranges: ranges, seq: seq)
    }

    mutating func add(writerID: String, range: Range) {
        var ranges = rangesByWriter[writerID] ?? []
        ranges.append(range)
        rangesByWriter[writerID] = Self.merge(ranges)
    }

    mutating func add(writerID: String, seq: UInt64) {
        add(writerID: writerID, range: Range(low: seq, high: seq))
    }

    func merging(_ other: CoveredRanges) -> CoveredRanges {
        var combined = rangesByWriter
        for (writer, ranges) in other.rangesByWriter {
            let existing = combined[writer] ?? []
            combined[writer] = Self.merge(existing + ranges)
        }
        return CoveredRanges(rangesByWriter: combined)
    }

    func superset(of other: CoveredRanges) -> Bool {
        for (writer, otherRanges) in other.rangesByWriter {
            guard let selfRanges = rangesByWriter[writer] else { return false }
            for range in otherRanges {
                if !Self.containsRange(ranges: selfRanges, candidate: range) {
                    return false
                }
            }
        }
        return true
    }

    func encodedAsRangeArrayMap() -> [String: [[UInt64]]] {
        var result: [String: [[UInt64]]] = [:]
        for (writer, ranges) in rangesByWriter {
            result[writer] = ranges.map { [$0.low, $0.high] }
        }
        return result
    }

    static func decode(_ raw: [String: [[UInt64]]]) -> CoveredRanges {
        var result: [String: [Range]] = [:]
        for (writer, pairs) in raw {
            var collected: [Range] = []
            for pair in pairs where pair.count == 2 {
                let low = pair[0]
                let high = pair[1]
                if low <= high {
                    collected.append(Range(low: low, high: high))
                }
            }
            result[writer] = merge(collected)
        }
        return CoveredRanges(rangesByWriter: result)
    }

    static func merge(_ ranges: [Range]) -> [Range] {
        guard !ranges.isEmpty else { return [] }
        let sorted = ranges.sorted { lhs, rhs in
            if lhs.low != rhs.low { return lhs.low < rhs.low }
            return lhs.high < rhs.high
        }
        var output: [Range] = []
        for range in sorted {
            if let last = output.last, isContiguousOrOverlapping(last: last, next: range) {
                output[output.count - 1] = Range(low: last.low, high: max(last.high, range.high))
            } else {
                output.append(range)
            }
        }
        return output
    }

    private static func isContiguousOrOverlapping(last: Range, next: Range) -> Bool {
        if next.low <= last.high { return true }
        // Adjacent ranges (next.low == last.high + 1) — guard the UInt64 overflow before adding.
        if last.high == UInt64.max { return false }
        return next.low <= last.high &+ 1
    }

    private static func contains(ranges: [Range], seq: UInt64) -> Bool {
        var lo = 0
        var hi = ranges.count - 1
        while lo <= hi {
            let mid = (lo + hi) / 2
            let r = ranges[mid]
            if seq < r.low {
                hi = mid - 1
            } else if seq > r.high {
                lo = mid + 1
            } else {
                return true
            }
        }
        return false
    }

    private static func containsRange(ranges: [Range], candidate: Range) -> Bool {
        for r in ranges where r.low <= candidate.low && r.high >= candidate.high {
            return true
        }
        return false
    }
}

struct ClosedSeqRange: Equatable, Hashable, Sendable {
    let low: UInt64
    let high: UInt64
}
