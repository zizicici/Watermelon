import Foundation

struct PiPProgressAccumulator {
    private(set) var displayedFraction: Double?
    private var segmentBaseFraction = 0.0

    mutating func reset() {
        displayedFraction = nil
        segmentBaseFraction = 0
    }

    mutating func beginRemainingSegment() {
        segmentBaseFraction = displayedFraction ?? 0
    }

    mutating func update(_ rawFraction: Double?) {
        guard let rawFraction, rawFraction.isFinite else { return }
        let clamped = min(max(rawFraction, 0), 1)
        let mapped = segmentBaseFraction + (1 - segmentBaseFraction) * clamped
        displayedFraction = min(mapped, 0.99)
    }

    mutating func complete() {
        displayedFraction = 1
    }

    mutating func freeze() {
        displayedFraction = displayedFraction ?? 0
    }
}
