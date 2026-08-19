import ImageIO
import Photos
import UIKit
import XCTest
@testable import Watermelon

final class InboxTransferMetadataSanitizerTests: XCTestCase {
    func testPhotoSanitizerRemovesGPSDictionary() async throws {
        let sourceURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension("jpg")
        let image = UIGraphicsImageRenderer(size: CGSize(width: 2, height: 2)).image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 2, height: 2))
        }
        let cgImage = try XCTUnwrap(image.cgImage)
        let destination = try XCTUnwrap(CGImageDestinationCreateWithURL(
            sourceURL as CFURL,
            "public.jpeg" as CFString,
            1,
            nil
        ))
        let gps: [CFString: Any] = [
            kCGImagePropertyGPSLatitude: 37.3349,
            kCGImagePropertyGPSLatitudeRef: "N",
            kCGImagePropertyGPSLongitude: 122.0090,
            kCGImagePropertyGPSLongitudeRef: "W",
        ]
        CGImageDestinationAddImage(
            destination,
            cgImage,
            [kCGImagePropertyGPSDictionary: gps] as CFDictionary
        )
        XCTAssertTrue(CGImageDestinationFinalize(destination))
        defer { try? FileManager.default.removeItem(at: sourceURL) }

        let sanitized = try await InboxTransferMetadataSanitizer.removingLocationMetadata(
            from: sourceURL,
            resourceType: .photo
        )
        defer { try? FileManager.default.removeItem(at: sanitized.url) }

        let sanitizedSource = try XCTUnwrap(CGImageSourceCreateWithURL(sanitized.url as CFURL, nil))
        let properties = try XCTUnwrap(
            CGImageSourceCopyPropertiesAtIndex(sanitizedSource, 0, nil) as? [CFString: Any]
        )
        XCTAssertNil(properties[kCGImagePropertyGPSDictionary])
    }

    func testVideoOutputChangesUnknownContainerExtensionWithContainer() {
        let output = InboxTransferMetadataSanitizer.videoOutput(
            sourceExtension: "3gp",
            supported: [.mov]
        )

        XCTAssertEqual(output?.fileType, .mov)
        XCTAssertEqual(output?.filenameExtension, "mov")
        XCTAssertEqual(
            InboxTransferService.replacingFilenameExtension("clip.3gp", with: output?.filenameExtension),
            "clip.mov"
        )
    }

    func testVideoOutputPreservesSupportedMP4Container() {
        let output = InboxTransferMetadataSanitizer.videoOutput(
            sourceExtension: "mp4",
            supported: [.mov, .mp4]
        )

        XCTAssertEqual(output?.fileType, .mp4)
        XCTAssertEqual(output?.filenameExtension, "mp4")
    }

    func testLivePhotoStillImageTimeTrackIsAllowed() {
        XCTAssertTrue(InboxTransferMetadataSanitizer.timedMetadataIdentifiersAreAllowed([
            "mdta/com.apple.quicktime.still-image-time"
        ]))
        XCTAssertFalse(InboxTransferMetadataSanitizer.timedMetadataIdentifiersAreAllowed([]))
        XCTAssertFalse(InboxTransferMetadataSanitizer.timedMetadataIdentifiersAreAllowed([
            "mdta/com.apple.quicktime.location.ISO6709"
        ]))
    }
}
