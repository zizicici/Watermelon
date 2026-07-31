import XCTest
@testable import WatermelonMac

final class MacProfileConnectionEditorModeTests: XCTestCase {
    func testCreateModeShowsNameField() {
        XCTAssertTrue(
            MacProfileConnectionEditorMode(
                hasEditingProfile: false
            ).showsNameField
        )
    }

    func testEditModeHidesNameField() {
        XCTAssertFalse(
            MacProfileConnectionEditorMode(
                hasEditingProfile: true
            ).showsNameField
        )
    }
}
