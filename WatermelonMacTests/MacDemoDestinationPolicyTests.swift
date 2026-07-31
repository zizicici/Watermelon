import XCTest
@testable import WatermelonMac

final class MacDemoDestinationPolicyTests: XCTestCase {
    func testConnectedDemoProvidesDestination() {
        XCTAssertTrue(
            MacDemoDestinationPolicy.shouldProvideDestination(
                arguments: ["WatermelonMac", "--demo-connected"]
            )
        )
    }

    func testConnectingDemoProvidesDestination() {
        XCTAssertTrue(
            MacDemoDestinationPolicy.shouldProvideDestination(
                arguments: ["WatermelonMac", "--demo-connecting"]
            )
        )
    }

    func testUnrelatedDemoDoesNotInventDestination() {
        XCTAssertFalse(
            MacDemoDestinationPolicy.shouldProvideDestination(
                arguments: ["WatermelonMac", "--demo-onboarding"]
            )
        )
    }

    func testHomeStateDemosUseSyntheticPhotoLibrary() {
        for argument in [
            "--demo-photo-library",
            "--demo-no-destination",
            "--demo-connecting",
            "--demo-connected",
        ] {
            XCTAssertTrue(
                MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
                    arguments: ["WatermelonMac", argument]
                ),
                argument
            )
        }
    }

    func testUnrelatedDemoDoesNotUseSyntheticPhotoLibrary() {
        XCTAssertFalse(
            MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
                arguments: ["WatermelonMac", "--demo-onboarding"]
            )
        )
    }
}
