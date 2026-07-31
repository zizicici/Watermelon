import XCTest
@testable import WatermelonMac

final class MacRepositoryMaintenanceClosePolicyTests: XCTestCase {
    func testCloseRequestWinsOverCancelledDelete() {
        XCTAssertEqual(
            MacRepositoryMaintenanceClosePolicy.terminalAction(
                closeRequested: true,
                cancelledDelete: true
            ),
            .close
        )
    }

    func testCancelledDeleteRescansWhenWindowRemainsOpen() {
        XCTAssertEqual(
            MacRepositoryMaintenanceClosePolicy.terminalAction(
                closeRequested: false,
                cancelledDelete: true
            ),
            .rescan
        )
    }

    func testOrdinaryCompletionKeepsWindowOpen() {
        XCTAssertEqual(
            MacRepositoryMaintenanceClosePolicy.terminalAction(
                closeRequested: false,
                cancelledDelete: false
            ),
            .stay
        )
    }
}
