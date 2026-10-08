import XCTest
@testable import Watermelon

final class HomeDownloadRunProgressTests: XCTestCase {
    func testResumeExcludesRestoredAndSkippedItemsWhenLocalScopeStillReportsThemRemoteOnly() {
        let month = LibraryMonthKey(year: 2026, month: 6)
        let items = [makeItem(1), makeItem(2), makeItem(3)]
        var progress = HomeDownloadRunProgress()

        progress.recordRestored(items[0].assetFingerprint, in: month)
        progress.recordSkipped(items[1].assetFingerprint, displayName: "second.jpg", in: month)

        XCTAssertEqual(progress.pendingItems(items, in: month).map(\.id), [items[2].id])
        XCTAssertEqual(progress.skippedItems(in: month)[items[1].assetFingerprint], "second.jpg")

        progress.recordRestored(items[2].assetFingerprint, in: month)
        XCTAssertTrue(progress.pendingItems(items, in: month).isEmpty)
        XCTAssertEqual(progress.pendingItems(items, in: LibraryMonthKey(year: 2026, month: 7)).count, 3)
    }

    private func makeItem(_ number: UInt8) -> RemoteAlbumItem {
        let name = "item-\(number).jpg"
        let hash = Data([number])
        let resource = RemoteManifestResource(
            year: 2026,
            month: 6,
            fileName: name,
            contentHash: hash,
            fileSize: 1,
            resourceType: 1,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        return RemoteAlbumItem(
            id: name,
            assetFingerprint: hash,
            creationDate: Date(timeIntervalSince1970: 0),
            resources: [resource],
            instances: [],
            representative: resource,
            mediaKind: .photo,
            contentHashes: [hash],
            isIncomplete: false,
            missingResourceCount: 0
        )
    }
}
