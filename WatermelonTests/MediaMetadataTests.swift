import ImageIO
import Photos
import UIKit
import XCTest
@testable import Watermelon

final class MediaMetadataTests: XCTestCase {
    func testImageMetadataIsGroupedAndNestedTagsAreFlattened() throws {
        let properties: [String: Any] = [
            kCGImagePropertyPixelWidth as String: 4_032,
            kCGImagePropertyPixelHeight as String: 3_024,
            kCGImagePropertyExifDictionary as String: [
                kCGImagePropertyExifExposureTime as String: 1.0 / 125.0,
                kCGImagePropertyExifFNumber as String: 1.8,
                kCGImagePropertyExifISOSpeedRatings as String: [100],
            ],
            kCGImagePropertyTIFFDictionary as String: [
                kCGImagePropertyTIFFModel as String: "Camera",
                "Vendor": ["Serial": "1234"],
            ],
        ]

        let document = MediaMetadataParser.document(
            imageProperties: properties,
            summary: summary()
        )

        XCTAssertEqual(document.detailLevel, .original)
        XCTAssertEqual(document.sections.first?.title, "File")
        XCTAssertNotNil(document.sections.first { $0.title == "Image" })
        let exif = try XCTUnwrap(document.sections.first { $0.title == "EXIF" })
        XCTAssertEqual(exif.rows.first { $0.label == kCGImagePropertyExifExposureTime as String }?.value, "1/125 s")
        XCTAssertTrue(exif.rows.first { $0.label == kCGImagePropertyExifFNumber as String }?.value.hasPrefix("ƒ/") == true)
        XCTAssertEqual(exif.rows.first { $0.label == kCGImagePropertyExifISOSpeedRatings as String }?.value, "100")
        let tiff = try XCTUnwrap(document.sections.first { $0.title == "TIFF" })
        XCTAssertEqual(tiff.rows.first { $0.label == "Vendor.Serial" }?.value, "1234")
    }

    func testBinaryMetadataIsSummarizedInsteadOfDumped() {
        let rows = MediaMetadataParser.flattenedRows([
            "Profile": Data(repeating: 7, count: 2_048),
        ])

        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].label, "Profile")
        XCTAssertFalse(rows[0].value.contains("0707"))
        XCTAssertTrue(rows[0].value.contains("2"))
    }

    func testSummaryIncludesBrowserLevelFactsWithoutEmbeddedExif() {
        let document = MediaMetadataParser.summaryDocument(summary())
        let rows = Dictionary(
            uniqueKeysWithValues: document.sections[0].rows.map { ($0.label, $0.value) }
        )

        XCTAssertTrue(document.isSummaryOnly)
        XCTAssertEqual(rows["FileName"], "IMG_0001.HEIC")
        XCTAssertEqual(rows["FileType"], "HEIC")
        XCTAssertEqual(rows["ImageSize"], "4032 × 3024")
        XCTAssertEqual(rows["MediaType"], "Photo")
        XCTAssertNotNil(rows["GPSPosition"])
    }

    @MainActor
    func testMetadataControllerReleaseCancelsInFlightLoad() async {
        let started = expectation(description: "metadata load started")
        let cancelled = expectation(description: "metadata load cancelled")
        let source = BlockingMetadataSource(started: started, cancelled: cancelled)
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1_700_000_000_000,
            localIdentifier: "local",
            fingerprint: nil,
            isBackedUp: false
        )
        var controller: MediaMetadataViewController? = MediaMetadataViewController(
            item: item,
            source: source
        )
        weak let weakController = controller

        controller?.loadViewIfNeeded()
        await fulfillment(of: [started], timeout: 1)
        controller = nil
        await fulfillment(of: [cancelled], timeout: 1)

        XCTAssertNil(weakController)
    }

    private func summary() -> MediaMetadataSummary {
        MediaMetadataSummary(
            fileName: "IMG_0001.HEIC",
            fileType: "HEIC",
            fileSize: 3_000_000,
            pixelWidth: 4_032,
            pixelHeight: 3_024,
            creationDate: Date(timeIntervalSince1970: 1_700_000_000),
            duration: nil,
            latitude: 1.3521,
            longitude: 103.8198,
            altitude: 15,
            mediaType: "Photo"
        )
    }
}

private final class BlockingMetadataSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .local

    private let started: XCTestExpectation
    private let cancelled: XCTestExpectation

    init(started: XCTestExpectation, cancelled: XCTestExpectation) {
        self.started = started
        self.cancelled = cancelled
    }

    func load() async -> MediaBrowserLoadResult {
        .loaded(.empty)
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        nil
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        nil
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        nil
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        nil
    }

    func metadata(for item: MediaBrowserItem) async -> MediaMetadataDocument? {
        started.fulfill()
        do {
            try await Task.sleep(for: .seconds(30))
        } catch is CancellationError {
            cancelled.fulfill()
        } catch {}
        return nil
    }
}
