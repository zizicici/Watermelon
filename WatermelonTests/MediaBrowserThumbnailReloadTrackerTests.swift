import XCTest
@testable import Watermelon

final class MediaBrowserThumbnailReloadTrackerTests: XCTestCase {
    func testNewRequestSurvivesOlderReloadCompletion() {
        var tracker = MediaBrowserThumbnailReloadTracker()
        tracker.requestReload()
        let firstLoad = tracker.requestedGeneration

        tracker.requestReload()
        let secondLoad = tracker.requestedGeneration
        tracker.markApplied(firstLoad)

        XCTAssertFalse(tracker.shouldApply(firstLoad))
        XCTAssertTrue(tracker.shouldApply(secondLoad))

        tracker.markApplied(secondLoad)
        XCTAssertFalse(tracker.shouldApply(secondLoad))
    }
}
