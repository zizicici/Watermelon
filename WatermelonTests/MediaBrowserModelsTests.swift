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

    func testSnapshotReportsOnlyExistingItemsWhosePresentationChanged() {
        let original = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 2,
            localIdentifier: "existing",
            fingerprint: nil,
            isBackedUp: false
        )
        let updated = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 2,
            localIdentifier: "existing",
            fingerprint: Data([1]),
            isBackedUp: true
        )
        let inserted = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: "inserted",
            fingerprint: nil,
            isBackedUp: false
        )
        let previous = MediaBrowserSnapshot(sections: [
            MediaBrowserSection(month: month, items: [original]),
        ])
        let current = MediaBrowserSnapshot(sections: [
            MediaBrowserSection(month: month, items: [updated, inserted]),
        ])

        XCTAssertEqual(current.changedItemIDs(comparedTo: previous), [updated.id])
    }

    func testSnapshotReportsNoChangesForEquivalentReload() {
        let item = MediaBrowserItem(
            kind: .video,
            creationDateMs: 1,
            localIdentifier: "existing",
            fingerprint: nil,
            isBackedUp: false
        )
        let previous = MediaBrowserSnapshot(sections: [
            MediaBrowserSection(month: month, items: [item]),
        ])
        let current = MediaBrowserSnapshot(sections: [
            MediaBrowserSection(month: month, items: [item]),
        ])

        XCTAssertTrue(current.changedItemIDs(comparedTo: previous).isEmpty)
    }

    func testLocalOnlyFilterPreservesOrderAndDropsBackedUpItemsAndEmptyMonths() {
        let photo = localItem("photo", isBackedUp: false)
        let unindexed = localItem("unindexed", fingerprint: nil, isBackedUp: false)
        let backedUp = localItem("backed-up", isBackedUp: true)
        let remote = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        let merged = remoteItem(fingerprint: Data([3]), localIdentifier: "remote-local-twin")
        let february = LibraryMonthKey(year: 2024, month: 2)
        let march = LibraryMonthKey(year: 2024, month: 3)
        let sections = [
            MediaBrowserSection(month: march, items: [backedUp]),
            MediaBrowserSection(month: february, items: [photo, remote]),
            MediaBrowserSection(month: month, items: [merged, unindexed]),
        ]

        let filtered = MediaBrowserSnapshot(sections: sections, filter: .localOnly)

        XCTAssertEqual(filtered.months, [february, month])
        XCTAssertEqual(filtered.itemCount, 2)
        XCTAssertEqual(filtered.item(at: 0), photo)
        XCTAssertEqual(filtered.item(at: 1), unindexed)
        XCTAssertEqual(filtered.index(of: unindexed.id), 1)
        XCTAssertEqual(filtered.itemIDs(inSection: 0), [photo.id])
        XCTAssertNil(filtered.item(id: backedUp.id))
        XCTAssertNil(filtered.item(id: remote.id))
        XCTAssertTrue(BatchActionResolver.resolve([photo, unindexed]).showsUpload)

        let all = MediaBrowserSnapshot(sections: sections, filter: .all)
        XCTAssertEqual(all.months, [march, february, month])
        XCTAssertEqual(all.itemCount, 5)
        XCTAssertEqual(all.item(section: 0, item: 0), backedUp)
    }

    func testLocalBackupFiltersReconcileAfterBackupAndRemoteRemoval() {
        func snapshot(isBackedUp: Bool, filter: MediaBrowserFilter) -> MediaBrowserSnapshot {
            MediaBrowserSnapshot(sections: [
                MediaBrowserSection(month: month, items: [localItem("asset", isBackedUp: isBackedUp)]),
            ], filter: filter)
        }

        for isBackedUp in [false, true, false] {
            let localOnly = snapshot(isBackedUp: isBackedUp, filter: .localOnly)
            let backedUp = snapshot(isBackedUp: isBackedUp, filter: .backedUp)
            let visible = isBackedUp ? backedUp : localOnly
            let hidden = isBackedUp ? localOnly : backedUp

            XCTAssertEqual(visible.itemIDs(inSection: 0), [.local("asset")])
            XCTAssertEqual(visible.months, [month])
            XCTAssertTrue(hidden.isEmpty)
            XCTAssertTrue(hidden.months.isEmpty)
        }
    }

    func testRemoteOnlyFilterExcludesLocalItemsAndReconcilesDownloadedItems() {
        let local = localItem("local", isBackedUp: false)
        let backedUp = localItem("backed-up", isBackedUp: true)
        var remote = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        let merged = remoteItem(fingerprint: Data([3]), localIdentifier: "remote-local-twin")
        let february = LibraryMonthKey(year: 2024, month: 2)
        func snapshot() -> MediaBrowserSnapshot {
            MediaBrowserSnapshot(sections: [
                MediaBrowserSection(month: february, items: [local, backedUp]),
                MediaBrowserSection(month: month, items: [remote, merged]),
            ], filter: .remoteOnly)
        }

        let beforeDownload = snapshot()
        XCTAssertEqual(beforeDownload.months, [month])
        XCTAssertEqual(beforeDownload.itemCount, 1)
        XCTAssertEqual(beforeDownload.item(at: 0), remote)
        XCTAssertEqual(beforeDownload.index(of: remote.id), 0)
        XCTAssertNil(beforeDownload.item(id: local.id))
        XCTAssertNil(beforeDownload.item(id: merged.id))

        remote.attachLocalIdentifier("downloaded")
        XCTAssertTrue(snapshot().isEmpty)
        XCTAssertTrue(snapshot().months.isEmpty)

        remote.removeLocalIdentifier()
        XCTAssertEqual(snapshot().itemIDs(inSection: 0), [remote.id])
    }

    private func localItem(
        _ identifier: String,
        fingerprint: Data? = Data([1]),
        isBackedUp: Bool
    ) -> MediaBrowserItem {
        MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: identifier,
            fingerprint: fingerprint,
            isBackedUp: isBackedUp
        )
    }

    private func remoteItem(fingerprint: Data, localIdentifier: String?) -> MediaBrowserItem {
        MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: localIdentifier,
            remote: RemoteMediaReference(
                fingerprint: fingerprint,
                photoRelativePath: "2024/01/remote.jpg",
                videoRelativePath: nil,
                photoContentHash: nil,
                videoContentHash: nil,
                storageMonth: month,
                isIncomplete: false
            )
        )
    }
}
