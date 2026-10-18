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

    func testFileSizeSortSpansMonthsAndUsesStableTiesWithUnknownSizesLast() {
        func item(_ id: String, size: Int64?, date: Int64) -> MediaBrowserItem {
            MediaBrowserItem(kind: .photo, creationDateMs: date, localIdentifier: id,
                             fingerprint: nil, isBackedUp: false, fileSize: size)
        }
        let unknown = item("unknown", size: nil, date: 10)
        let small = item("small", size: 10, date: 9)
        let tieB = item("b", size: 100, date: 5)
        let tieA = item("a", size: 100, date: 5)
        let recent = item("recent", size: 100, date: 6)
        let large = item("large", size: 1_000, date: 1)
        let sections = [
            MediaBrowserSection(month: .init(year: 2024, month: 2), items: [unknown, small, tieB]),
            MediaBrowserSection(month: month, items: [tieA, recent, large]),
        ]
        let sorted = MediaBrowserSnapshot(sections: sections, sortOrder: .fileSize)
        let expected = [large, recent, tieA, tieB, small, unknown]

        XCTAssertEqual(sorted.sectionIDs, [.fileSize(.below1MB), .fileSize(.unknown)])
        XCTAssertEqual(sorted.itemCount, expected.count)
        XCTAssertEqual(sorted.itemIDs(inSection: 0), expected.dropLast().map(\.id))
        XCTAssertEqual(sorted.itemIDs(inSection: 1), [unknown.id])
        XCTAssertEqual(sorted.items(inDisplayOrderFor: Set(expected.map(\.id))), expected)
        for (index, item) in expected.enumerated() {
            XCTAssertEqual(sorted.item(at: index), item)
            XCTAssertEqual(sorted.index(of: item.id), index)
        }
        let restored = MediaBrowserSnapshot(sections: sections)
        XCTAssertEqual(restored.months, sections.map(\.month))
        XCTAssertEqual(restored.item(at: 0), unknown)
    }

    func testFileSizeSortCombinesWithPresenceFilterAndEmptyResults() {
        let unbacked = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "unbacked",
                                       fingerprint: nil, isBackedUp: false, fileSize: 100)
        let backed = MediaBrowserItem(kind: .photo, creationDateMs: 2, localIdentifier: "backed",
                                     fingerprint: Data([1]), isBackedUp: true, fileSize: 1_000)
        let sections = [MediaBrowserSection(month: month, items: [backed, unbacked])]
        let filtered = MediaBrowserSnapshot(sections: sections, filter: .localOnly, sortOrder: .fileSize)
        XCTAssertEqual(filtered.itemIDs(inSection: 0), [unbacked.id])
        XCTAssertNil(filtered.item(id: backed.id))
        let empty = MediaBrowserSnapshot(sections: sections, filter: .remoteOnly, sortOrder: .fileSize)
        XCTAssertTrue(empty.isEmpty)
        XCTAssertTrue(empty.sectionIDs.isEmpty)
    }

    func testSizeSectionBoundariesUseDecimalBytesAndKeepUnknownLast() {
        let cases: [(Int64?, MediaBrowserSizeRange)] = [
            (nil, .unknown), (-1, .unknown), (0, .below1MB), (999_999, .below1MB),
            (1_000_000, .from1MBTo10MB), (9_999_999, .from1MBTo10MB),
            (10_000_000, .from10MBTo100MB), (99_999_999, .from10MBTo100MB),
            (100_000_000, .from100MBTo1GB), (999_999_999, .from100MBTo1GB),
            (1_000_000_000, .atLeast1GB), (Int64.max, .atLeast1GB),
        ]
        let items = cases.enumerated().map { index, value in
            MediaBrowserItem(kind: .photo, creationDateMs: Int64(index), localIdentifier: "asset-\(index)",
                             fingerprint: nil, isBackedUp: false, fileSize: value.0)
        }
        for (size, expected) in cases {
            XCTAssertEqual(MediaBrowserSizeRange.containing(size), expected)
        }
        let snapshot = MediaBrowserSnapshot(sections: [
            .init(month: month, items: items),
        ], sortOrder: .fileSize)
        XCTAssertEqual(snapshot.sectionIDs, MediaBrowserSizeRange.allCases.map { .fileSize($0) })
        let expectedOrder = [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0].map { items[$0] }
        XCTAssertEqual(snapshot.items(inDisplayOrderFor: Set(items.map(\.id))), expectedOrder)
        for (index, item) in expectedOrder.enumerated() {
            XCTAssertEqual(snapshot.item(at: index), item)
            XCTAssertEqual(snapshot.index(of: item.id), index)
        }
    }

    func testMediaFilterIncludesLivePhotosAndCombinesWithPresenceAndBothSortOrders() {
        let photo = MediaBrowserItem(kind: .photo, creationDateMs: 4, localIdentifier: "photo",
                                     fingerprint: nil, isBackedUp: false, fileSize: 2_000_000)
        let live = MediaBrowserItem(kind: .livePhoto, creationDateMs: 3, localIdentifier: "live",
                                    fingerprint: nil, isBackedUp: true, fileSize: 12_000_000)
        let video = MediaBrowserItem(kind: .video, creationDateMs: 2, localIdentifier: "video",
                                     fingerprint: nil, isBackedUp: false, fileSize: 200_000_000)
        let remoteVideo = MediaBrowserItem(kind: .video, creationDateMs: 1, localIdentifier: nil,
            remote: .init(fingerprint: Data([4]), photoRelativePath: nil, videoRelativePath: "video.mov",
                          photoContentHash: nil, videoContentHash: nil, storageMonth: month, isIncomplete: false),
            fileSize: 1_000_000_000)
        let sections = [
            MediaBrowserSection(month: .init(year: 2024, month: 2), items: [photo, live]),
            MediaBrowserSection(month: month, items: [video, remoteVideo]),
        ]
        for order in MediaBrowserSortOrder.allCases {
            let photos = MediaBrowserSnapshot(sections: sections, mediaFilter: .photos, sortOrder: order)
            XCTAssertEqual(Set(photos.itemIDs), [photo.id, live.id])
            let videos = MediaBrowserSnapshot(sections: sections, mediaFilter: .videos, sortOrder: order)
            XCTAssertEqual(Set(videos.itemIDs), [video.id, remoteVideo.id])
            let backedPhotos = MediaBrowserSnapshot(sections: sections, filter: .backedUp, mediaFilter: .photos, sortOrder: order)
            XCTAssertEqual(Set(backedPhotos.itemIDs), [live.id])
            let localVideos = MediaBrowserSnapshot(sections: sections, filter: .localOnly, mediaFilter: .videos, sortOrder: order)
            XCTAssertEqual(Set(localVideos.itemIDs), [video.id])
            let remoteVideos = MediaBrowserSnapshot(sections: sections, filter: .remoteOnly, mediaFilter: .videos, sortOrder: order)
            XCTAssertEqual(Set(remoteVideos.itemIDs), [remoteVideo.id])
            let empty = MediaBrowserSnapshot(sections: sections, filter: .remoteOnly, mediaFilter: .photos, sortOrder: order)
            XCTAssertTrue(empty.isEmpty)
            XCTAssertTrue(empty.sectionIDs.isEmpty)
        }
        let photosByMonth = MediaBrowserSnapshot(sections: sections, mediaFilter: .photos)
        XCTAssertEqual(photosByMonth.months, [.init(year: 2024, month: 2)])
        let photosBySize = MediaBrowserSnapshot(sections: sections, mediaFilter: .photos, sortOrder: .fileSize)
        XCTAssertEqual(photosBySize.sectionIDs, [.fileSize(.from10MBTo100MB), .fileSize(.from1MBTo10MB)])
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

    func testSelectAllKeepsDisplayOrderAcrossMonthsAndRemoteTwins() {
        let first = localItem("z", isBackedUp: false)
        let last = localItem("a", isBackedUp: true)
        let januaryRemote = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        let february = LibraryMonthKey(year: 2024, month: 2)
        let februaryRemote = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: nil,
            remote: .init(fingerprint: Data([2]), photoRelativePath: "2024/02/remote.jpg",
                videoRelativePath: nil, photoContentHash: nil, videoContentHash: nil,
                storageMonth: february, isIncomplete: false))
        let snapshot = MediaBrowserSnapshot(sections: [
            .init(month: february, items: [first, februaryRemote]),
            .init(month: month, items: [januaryRemote, last]),
        ])

        XCTAssertEqual(snapshot.items(inDisplayOrderFor: Set(snapshot.itemIDs)),
            [first, februaryRemote, januaryRemote, last])
    }

    func testSparseSelectionKeepsDisplayOrderAndDropsStaleIDs() {
        let items = (0..<12).map { localItem("item-\($0)", isBackedUp: false) }
        let snapshot = MediaBrowserSnapshot(sections: [
            .init(month: .init(year: 2024, month: 3), items: Array(items.prefix(6))),
            .init(month: .init(year: 2024, month: 2), items: []),
            .init(month: month, items: Array(items.suffix(6))),
        ])

        XCTAssertEqual(snapshot.items(inDisplayOrderFor: [items[10].id, .local("removed"), items[2].id]),
            [items[2], items[10]])
    }

    func testDenseSelectionAfterReloadKeepsOnlySelectedVisibleItems() {
        let removed = localItem("removed", isBackedUp: false)
        let changed = localItem("changed", isBackedUp: false)
        let retained = localItem("retained", isBackedUp: false)
        let previous = MediaBrowserSnapshot(sections: [
            .init(month: month, items: [removed, changed, retained]),
        ])
        let updated = localItem("changed", isBackedUp: true)
        let inserted = localItem("inserted", isBackedUp: false)
        let current = MediaBrowserSnapshot(sections: [
            .init(month: month, items: [retained, updated, inserted]),
        ])
        let selectedIDs = Set(previous.itemIDs)

        XCTAssertEqual(current.items(inDisplayOrderFor: selectedIDs), [retained, updated])
        let filtered = MediaBrowserSnapshot(sections: [
            .init(month: month, items: [retained, updated, inserted]),
        ], filter: .localOnly)
        XCTAssertEqual(filtered.items(inDisplayOrderFor: selectedIDs), [retained])
        XCTAssertTrue(current.items(inDisplayOrderFor: []).isEmpty)
        XCTAssertTrue(MediaBrowserSnapshot.empty.items(inDisplayOrderFor: selectedIDs).isEmpty)
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
