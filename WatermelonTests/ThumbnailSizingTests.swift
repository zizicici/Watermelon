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

    func testFittedImageUsesNoAlphaBackingBitmap() throws {
        let source = UIGraphicsImageRenderer(size: CGSize(width: 600, height: 800)).image { context in
            UIColor.systemRed.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 600, height: 800))
        }

        let fitted = try XCTUnwrap(ThumbnailSizing.fittedImage(source, maximumLongSide: 400))

        XCTAssertEqual(fitted.size, CGSize(width: 300, height: 400))
        XCTAssertEqual(fitted.scale, 1)
        XCTAssertEqual(fitted.cgImage?.alphaInfo, .noneSkipLast)
    }

    func testFittedImageKeepsUIKitOrientation() throws {
        let source = UIGraphicsImageRenderer(size: CGSize(width: 100, height: 200)).image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 100, height: 100))
            UIColor.blue.setFill()
            context.fill(CGRect(x: 0, y: 100, width: 100, height: 100))
        }

        let fitted = try XCTUnwrap(ThumbnailSizing.fittedImage(source, maximumLongSide: 200))
        let top = try XCTUnwrap(pixel(atX: 50, y: 25, in: fitted))
        let bottom = try XCTUnwrap(pixel(atX: 50, y: 175, in: fitted))

        XCTAssertGreaterThan(top.red, top.blue)
        XCTAssertGreaterThan(bottom.blue, bottom.red)
    }

    func testJPEGDataReturnsJPEGBytes() throws {
        let source = UIGraphicsImageRenderer(size: CGSize(width: 300, height: 400)).image { context in
            UIColor.systemBlue.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 300, height: 400))
        }

        let data = try XCTUnwrap(ThumbnailSizing.jpegData(from: source))

        XCTAssertEqual(data.prefix(2), Data([0xff, 0xd8]))
        XCTAssertEqual(data.suffix(2), Data([0xff, 0xd9]))
    }

    private func pixel(atX x: Int, y: Int, in image: UIImage) -> (red: UInt8, green: UInt8, blue: UInt8)? {
        guard let cgImage = image.cgImage,
              x >= 0, x < cgImage.width,
              y >= 0, y < cgImage.height,
              let providerData = cgImage.dataProvider?.data else { return nil }
        let data = providerData as Data
        let offset = y * cgImage.bytesPerRow + x * 4
        guard offset + 2 < data.count else { return nil }
        return (data[offset], data[offset + 1], data[offset + 2])
    }
}
