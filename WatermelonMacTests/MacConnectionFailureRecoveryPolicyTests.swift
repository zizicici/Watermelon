import AppKit
import XCTest
@testable import WatermelonMac

final class MacConnectionFailureRecoveryPolicyTests: XCTestCase {
    func testEditButtonReturnsFailedProfileID() {
        XCTAssertEqual(
            MacConnectionFailureRecoveryPolicy.editProfileID(
                profileID: 42,
                response: .alertFirstButtonReturn
            ),
            42
        )
    }

    func testDismissAndUnsavedProfileDoNotOpenEditor() {
        XCTAssertNil(
            MacConnectionFailureRecoveryPolicy.editProfileID(
                profileID: 42,
                response: .alertSecondButtonReturn
            )
        )
        XCTAssertNil(
            MacConnectionFailureRecoveryPolicy.editProfileID(
                profileID: nil,
                response: .alertFirstButtonReturn
            )
        )
    }
}
