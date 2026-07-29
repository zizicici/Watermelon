import XCTest
@testable import Watermelon

final class PiPElapsedTimeTrackerTests: XCTestCase {
    func testPausedIntervalIsExcludedFromElapsedTime() {
        var tracker = PiPElapsedTimeTracker()
        tracker.start(at: 100)

        XCTAssertEqual(tracker.elapsed(at: 112), 12)

        tracker.setPaused(true, at: 112)
        XCTAssertEqual(tracker.elapsed(at: 200), 12)

        tracker.setPaused(false, at: 200)
        XCTAssertEqual(tracker.elapsed(at: 205), 17)
    }

    func testRepeatedPauseAndResumeEventsAreIdempotent() {
        var tracker = PiPElapsedTimeTracker()
        tracker.start(at: 100)

        tracker.setPaused(true, at: 110)
        tracker.setPaused(true, at: 150)
        XCTAssertEqual(tracker.elapsed(at: 200), 10)

        tracker.setPaused(false, at: 200)
        tracker.setPaused(false, at: 250)
        XCTAssertEqual(tracker.elapsed(at: 260), 70)
    }

    func testStopFreezesElapsedTime() {
        var tracker = PiPElapsedTimeTracker()
        tracker.start(at: 100)

        XCTAssertEqual(tracker.stop(at: 125), 25)
        XCTAssertEqual(tracker.elapsed(at: 300), 25)
    }
}
