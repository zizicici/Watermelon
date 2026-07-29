import XCTest
@testable import Watermelon

final class PiPProgressAccumulatorTests: XCTestCase {
    func testProgressReflectsEstimateGrowth() {
        var accumulator = PiPProgressAccumulator()

        accumulator.update(0.6)
        accumulator.update(0.4)

        XCTAssertEqual(accumulator.displayedFraction ?? 0, 0.4, accuracy: 0.001)
    }

    func testResumeMapsRemainingPlanOntoUnfinishedProgress() {
        var accumulator = PiPProgressAccumulator()
        accumulator.update(0.6)

        accumulator.beginRemainingSegment()
        accumulator.update(0.5)

        XCTAssertEqual(accumulator.displayedFraction ?? 0, 0.8, accuracy: 0.001)
    }

    func testActiveProgressStopsShortOfCompletion() {
        var accumulator = PiPProgressAccumulator()

        accumulator.update(1)

        XCTAssertEqual(accumulator.displayedFraction ?? 0, 0.99, accuracy: 0.001)
    }

    func testInvalidProgressDoesNotReachRenderingState() {
        var accumulator = PiPProgressAccumulator()

        accumulator.update(.nan)
        accumulator.update(.infinity)

        XCTAssertNil(accumulator.displayedFraction)
    }

    func testTerminalStatesResolveProgress() {
        var completed = PiPProgressAccumulator()
        completed.complete()

        var failed = PiPProgressAccumulator()
        failed.freeze()

        XCTAssertEqual(completed.displayedFraction ?? 0, 1, accuracy: 0.001)
        XCTAssertEqual(failed.displayedFraction ?? -1, 0, accuracy: 0.001)
    }
}
