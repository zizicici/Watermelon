import XCTest
@testable import WatermelonMac

final class MacConnectionCredentialPromptPolicyTests: XCTestCase {
    func testCancelledPromptIsCancellation() {
        XCTAssertThrowsError(
            try MacConnectionCredentialPromptPolicy
                .submittedCredential(from: nil)
        ) {
            XCTAssertTrue($0 is CancellationError)
        }
    }

    func testEmptySubmissionIsInvalidConfiguration() {
        XCTAssertThrowsError(
            try MacConnectionCredentialPromptPolicy
                .submittedCredential(from: "")
        ) {
            guard case RemoteStorageClientError
                .invalidConfiguration = $0 else {
                return XCTFail("Expected invalid configuration")
            }
        }
    }

    func testSubmittedCredentialIsPreserved() throws {
        XCTAssertEqual(
            try MacConnectionCredentialPromptPolicy
                .submittedCredential(from: "secret"),
            "secret"
        )
    }
}
