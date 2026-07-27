import XCTest
@testable import Watermelon

extension MediaBrowserItem {
    var testLabel: String {
        if let relativePath = photoRemoteRelativePath ?? videoRemoteRelativePath {
            return URL(fileURLWithPath: relativePath).deletingPathExtension().lastPathComponent
        }
        return localIdentifier ?? ""
    }
}

final class MediaBrowserModelsTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2024, month: 1)

    func testLocalBackingCannotBecomeRemoteDeletable() {
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: "local",
            fingerprint: Data([1]),
            isBackedUp: true
        )

        XCTAssertEqual(item.presence, .both)
        XCTAssertTrue(item.isDeviceDeletable)
        XCTAssertFalse(item.isRemoteDeletable)
    }

    func testRemoteBackingOwnsLocalAttachmentTransition() {
        var item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: nil,
            remote: RemoteMediaReference(
                fingerprint: Data([2]),
                photoRelativePath: "2024/01/a.jpg",
                videoRelativePath: nil,
                photoContentHash: nil,
                videoContentHash: nil,
                storageMonth: month,
                isIncomplete: false
            )
        )

        XCTAssertEqual(item.presence, .remoteOnly)
        XCTAssertTrue(item.isRemoteDeletable)
        item.attachLocalIdentifier("local")
        XCTAssertEqual(item.presence, .both)
        XCTAssertEqual(item.localIdentifier, "local")
        item.removeLocalIdentifier()
        XCTAssertEqual(item.presence, .remoteOnly)
        XCTAssertNil(item.localIdentifier)
    }

    func testTypedIDsKeepLocalAndRemoteNamespacesSeparate() {
        let fingerprint = Data([3])
        XCTAssertNotEqual(
            MediaBrowserItemID.local(fingerprint.hexString + "#path"),
            MediaBrowserItemID.remote(
                fingerprint: fingerprint,
                storageMonth: month
            )
        )
    }

    func testRemoteIDIncludesStorageMonthForGroupingTwins() {
        let fingerprint = Data([3])
        let january = MediaBrowserItemID.remote(
            fingerprint: fingerprint,
            storageMonth: LibraryMonthKey(year: 2024, month: 1)
        )
        let february = MediaBrowserItemID.remote(
            fingerprint: fingerprint,
            storageMonth: LibraryMonthKey(year: 2024, month: 2)
        )

        XCTAssertNotEqual(january, february)
    }

    @MainActor
    func testSnapshotAndSessionPublishOneVersionedView() {
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: "local",
            fingerprint: nil,
            isBackedUp: false
        )
        let snapshot = MediaBrowserSnapshot(
            sections: [MediaBrowserSection(month: month, items: [item])]
        )
        let session = MediaBrowserSession()

        session.replace(with: snapshot)

        XCTAssertEqual(session.revision, 1)
        XCTAssertEqual(session.snapshot.item(section: 0, item: 0)?.id, item.id)
        XCTAssertEqual(session.snapshot.item(id: item.id), item)
        XCTAssertEqual(session.snapshot.item(at: 0), item)
    }

    func testSnapshotIndexesAcrossSegmentedSections() {
        let first = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 2,
            localIdentifier: "first",
            fingerprint: nil,
            isBackedUp: false
        )
        let second = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: "second",
            fingerprint: Data([2]),
            isBackedUp: true
        )
        let february = LibraryMonthKey(year: 2024, month: 2)
        let snapshot = MediaBrowserSnapshot(sections: [
            MediaBrowserSection(month: february, items: [first]),
            MediaBrowserSection(month: month, items: [second]),
        ])

        XCTAssertEqual(snapshot.months, [february, month])
        XCTAssertEqual(snapshot.itemCount, 2)
        XCTAssertEqual(snapshot.item(at: 0), first)
        XCTAssertEqual(snapshot.item(at: 1), second)
        XCTAssertEqual(snapshot.itemIDs(inSection: 0), [first.id])
        XCTAssertEqual(snapshot.item(section: 1, item: 0), second)
        XCTAssertEqual(snapshot.item(id: .local("second")), second)
        XCTAssertEqual(snapshot.index(of: second.id), 1)
    }
}
