import XCTest
@testable import Watermelon

final class ThumbnailSizingTests: XCTestCase {
    func testTargetLongSideIsHalfOfOriginalCappedAt400() {
        XCTAssertEqual(ThumbnailSizing.targetLongSide(originalWidth: 4_000, originalHeight: 3_000), 400)
        XCTAssertEqual(ThumbnailSizing.targetLongSide(originalWidth: 600, originalHeight: 400), 300)
        XCTAssertEqual(ThumbnailSizing.targetLongSide(originalWidth: 101, originalHeight: 51), 50)
    }

    func testFittedSizePreservesAspectRatioWithoutApplyingHalfRuleAgain() {
        XCTAssertEqual(
            ThumbnailSizing.fittedSize(width: 800, height: 600, maximumLongSide: 400),
            CGSize(width: 400, height: 300)
        )
        XCTAssertEqual(
            ThumbnailSizing.fittedSize(width: 600, height: 800, maximumLongSide: 400),
            CGSize(width: 300, height: 400)
        )
        XCTAssertEqual(
            ThumbnailSizing.fittedSize(width: 225, height: 300, maximumLongSide: 400),
            CGSize(width: 225, height: 300)
        )
    }

    func testTinyImagesStayAtLeastOnePixel() {
        XCTAssertEqual(ThumbnailSizing.targetLongSide(originalWidth: 1, originalHeight: 1), 1)
        XCTAssertEqual(
            ThumbnailSizing.fittedSize(width: 1, height: 1, maximumLongSide: 400),
            CGSize(width: 1, height: 1)
        )
    }

    func testInvalidOriginalSizeReturnsNil() {
        XCTAssertNil(ThumbnailSizing.targetLongSide(originalWidth: 0, originalHeight: 100))
        XCTAssertNil(ThumbnailSizing.fittedSize(width: 0, height: 100, maximumLongSide: 400))
    }
}
