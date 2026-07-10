import XCTest
@testable import Watermelon

final class MediaBrowserShareMaterializationTrackerTests: XCTestCase {
    func testOnlyOneShareCanMaterializeAtATime() throws {
        var tracker = MediaBrowserShareMaterializationTracker()

        let first = try XCTUnwrap(tracker.begin())

        XCTAssertNil(tracker.begin())
        XCTAssertTrue(tracker.finish(first))
        XCTAssertNotNil(tracker.begin())
    }

    func testCancelledAttemptCannotFinishOverNewAttempt() throws {
        var tracker = MediaBrowserShareMaterializationTracker()
        let first = try XCTUnwrap(tracker.begin())

        XCTAssertTrue(tracker.cancel(first))
        let second = try XCTUnwrap(tracker.begin())

        XCTAssertFalse(tracker.finish(first))
        XCTAssertEqual(tracker.activeToken, second)
        XCTAssertTrue(tracker.finish(second))
    }
}
