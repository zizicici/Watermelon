import XCTest
@testable import WatermelonMac

final class MacRunningTaskWindowClosePolicyTests: XCTestCase {
    func testCancelKeepsWindowOpen() {
        XCTAssertEqual(
            MacRunningTaskWindowClosePolicy.action(
                stopConfirmed: false,
                isTaskRunning: true
            ),
            .keepOpen
        )
    }

    func testLateConfirmationClosesAfterTaskAlreadyFinished() {
        XCTAssertEqual(
            MacRunningTaskWindowClosePolicy.action(
                stopConfirmed: true,
                isTaskRunning: false
            ),
            .close
        )
    }

    func testConfirmedStopWaitsForRunningTask() {
        XCTAssertEqual(
            MacRunningTaskWindowClosePolicy.action(
                stopConfirmed: true,
                isTaskRunning: true
            ),
            .stopThenClose
        )
    }
}
