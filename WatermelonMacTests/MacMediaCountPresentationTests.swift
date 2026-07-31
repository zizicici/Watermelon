import XCTest
@testable import WatermelonMac

final class MacMediaCountPresentationTests: XCTestCase {
    func testItemsMatchIOSMediaOrderAndSymbols() {
        XCTAssertEqual(
            MacMediaCountPresentation.items(
                photoCount: 12,
                videoCount: 3
            ),
            [
                MacMediaCountItem(symbolName: "photo", count: 12),
                MacMediaCountItem(symbolName: "video", count: 3),
            ]
        )
    }

    func testItemsKeepZeroCountsForMissingLibrarySide() {
        XCTAssertEqual(
            MacMediaCountPresentation.items(
                photoCount: 0,
                videoCount: 0
            ),
            [
                MacMediaCountItem(symbolName: "photo", count: 0),
                MacMediaCountItem(symbolName: "video", count: 0),
            ]
        )
    }
}
