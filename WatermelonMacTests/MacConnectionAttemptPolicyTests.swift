import XCTest
@testable import WatermelonMac

final class MacConnectionAttemptPolicyTests: XCTestCase {
    func testMatchingProfileAndEpochRemainCurrent() {
        XCTAssertTrue(
            isCurrent(
                profileID: 7,
                selectedProfileID: 7,
                attemptEpoch: 3,
                currentEpoch: 3
            )
        )
    }

    func testCancelledAttemptRejectsLateCredentialPrompt() {
        XCTAssertFalse(
            isCurrent(
                profileID: 7,
                selectedProfileID: 7,
                attemptEpoch: 3,
                currentEpoch: 4
            )
        )
    }

    func testProfileSwitchRejectsLateCredentialPrompt() {
        XCTAssertFalse(
            isCurrent(
                profileID: 7,
                selectedProfileID: 8,
                attemptEpoch: 3,
                currentEpoch: 3
            )
        )
    }

    func testUnsavedOrDeselectedProfileCannotContinue() {
        XCTAssertFalse(
            isCurrent(
                profileID: nil,
                selectedProfileID: nil,
                attemptEpoch: 3,
                currentEpoch: 3
            )
        )
        XCTAssertFalse(
            isCurrent(
                profileID: 7,
                selectedProfileID: nil,
                attemptEpoch: 3,
                currentEpoch: 3
            )
        )
    }

    private func isCurrent(
        profileID: Int64?,
        selectedProfileID: Int64?,
        attemptEpoch: UInt64,
        currentEpoch: UInt64
    ) -> Bool {
        MacConnectionAttemptPolicy.isCurrent(
            profileID: profileID,
            selectedProfileID: selectedProfileID,
            attemptEpoch: attemptEpoch,
            currentEpoch: currentEpoch
        )
    }
}
