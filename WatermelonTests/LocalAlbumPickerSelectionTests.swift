import XCTest
@testable import Watermelon

final class LocalAlbumPickerSelectionTests: XCTestCase {
    func testDeselectingTheLastAlbumCanBeConfirmed() {
        var selection = LocalAlbumPickerSelection(identifiers: ["a", "b"])
        selection.toggle("a")
        XCTAssertEqual(selection.identifiers, ["b"])
        selection.toggle("b")
        XCTAssertTrue(selection.identifiers.isEmpty)
        XCTAssertTrue(selection.canComplete)
        XCTAssertFalse(selection.canClear)
    }

    func testAlbumsDisappearingDoesNotBecomeAnExplicitClear() {
        var selection = LocalAlbumPickerSelection(identifiers: ["a", "b"])
        selection.retainAvailable([])
        XCTAssertTrue(selection.identifiers.isEmpty)
        XCTAssertFalse(selection.canComplete)
        XCTAssertTrue(selection.canClear)
        selection.clear()
        XCTAssertTrue(selection.canComplete)
        XCTAssertFalse(selection.canClear)
    }

    func testLosingRemainingAlbumAfterUserDeselectsAnotherDoesNotResetScope() {
        var selection = LocalAlbumPickerSelection(identifiers: ["a", "b"])
        selection.toggle("a")
        selection.retainAvailable([])
        XCTAssertFalse(selection.canComplete)
    }

    func testNewSelectionAfterClearingDoesNotKeepPermissionToSubmitAnEmptyResult() {
        var selection = LocalAlbumPickerSelection(identifiers: ["a"])
        selection.clear()
        selection.toggle("b")
        XCTAssertEqual(selection.identifiers, ["b"])
        selection.retainAvailable([])
        XCTAssertFalse(selection.canComplete)
    }

    func testOpeningAnEmptyPickerDoesNotCommitAReset() {
        let selection = LocalAlbumPickerSelection(identifiers: [])
        XCTAssertFalse(selection.canComplete)
        XCTAssertFalse(selection.canClear)
    }
}
