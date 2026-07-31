import XCTest
@testable import WatermelonMac

final class MacConnectionCredentialPersistencePolicyTests:
    XCTestCase
{
    func testPromptedCredentialUsesValidatedLiveAccount() {
        XCTAssertEqual(
            MacConnectionCredentialPersistencePolicy
                .accountAfterValidation(
                    requiresPersistence: true,
                    attemptedCredentialRef: "credential:one",
                    liveCredentialRef: "credential:one"
                ),
            "credential:one"
        )
    }

    func testExistingCredentialDoesNotNeedAnotherWrite() {
        XCTAssertNil(
            MacConnectionCredentialPersistencePolicy
                .accountAfterValidation(
                    requiresPersistence: false,
                    attemptedCredentialRef: "credential:one",
                    liveCredentialRef: "credential:one"
                )
        )
    }

    func testChangedCredentialReferenceRejectsLateWrite() {
        XCTAssertNil(
            MacConnectionCredentialPersistencePolicy
                .accountAfterValidation(
                    requiresPersistence: true,
                    attemptedCredentialRef: "credential:one",
                    liveCredentialRef: "credential:two"
                )
        )
    }

    func testEmptyCredentialReferenceRejectsWrite() {
        XCTAssertNil(
            MacConnectionCredentialPersistencePolicy
                .accountAfterValidation(
                    requiresPersistence: true,
                    attemptedCredentialRef: "",
                    liveCredentialRef: ""
                )
        )
    }
}
