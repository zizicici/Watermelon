import XCTest
@testable import WatermelonMac

final class MacRemoteThumbnailPurgeOutcomeTests: XCTestCase {
    func testCancellationErrorIsCancelled() {
        XCTAssertEqual(
            MacRemoteThumbnailPurgeOutcome.from(
                error: CancellationError(),
                taskIsCancelled: false
            ),
            .cancelled
        )
    }

    func testCancelledTaskOverridesConnectionError() {
        XCTAssertEqual(
            MacRemoteThumbnailPurgeOutcome.from(
                error: NSError(
                    domain: "MacRemoteThumbnailPurgeOutcomeTests",
                    code: 1
                ),
                taskIsCancelled: true
            ),
            .cancelled
        )
    }

    func testConnectionErrorIsFailed() {
        XCTAssertEqual(
            MacRemoteThumbnailPurgeOutcome.from(
                error: NSError(
                    domain: "MacRemoteThumbnailPurgeOutcomeTests",
                    code: 1
                ),
                taskIsCancelled: false
            ),
            .failed
        )
    }
}
