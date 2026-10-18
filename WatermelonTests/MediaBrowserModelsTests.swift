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

    func testLocalBackingCanDeleteRemoteByFingerprintWithoutRemoteMonth() {
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1,
            localIdentifier: "local",
            fingerprint: Data([1]),
            isBackedUp: true
        )

        XCTAssertEqual(item.presence, .both)
        XCTAssertTrue(item.isDeviceDeletable)
        XCTAssertTrue(item.isRemoteDeletable)
        XCTAssertNil(item.remoteMonth)
    }

    func testRemoteBackingDerivesPresenceFromLocalReference() {
        let remote = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        let downloaded = remoteItem(fingerprint: Data([2]), localIdentifier: "local")

        XCTAssertEqual(remote.presence, .remoteOnly)
        XCTAssertTrue(remote.isRemoteDeletable)
        XCTAssertNil(remote.localIdentifier)
        XCTAssertEqual(downloaded.presence, .both)
        XCTAssertTrue(downloaded.isRemoteDeletable)
        XCTAssertEqual(downloaded.localIdentifier, "local")
        XCTAssertEqual(remote.id, downloaded.id)
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
        XCTAssertEqual(snapshot.sectionIndex(of: first.id), 0)
        XCTAssertEqual(snapshot.sectionIndex(of: second.id), 1)
        XCTAssertNil(snapshot.sectionIndex(of: .local("missing")))
        XCTAssertEqual(snapshot.itemCount(inSection: 0), 1)
        XCTAssertEqual(snapshot.itemCount(inSection: -1), 0)
        XCTAssertEqual(snapshot.itemCount(inSection: 2), 0)
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
            for snapshot in [photos, videos, backedPhotos, localVideos, remoteVideos, empty] {
                let pages = MediaBrowserPageOrder(snapshot: snapshot)
                XCTAssertEqual(pages.count, snapshot.itemCount)
                XCTAssertEqual((0..<pages.count).compactMap { pages.item(at: $0, in: snapshot)?.id }, snapshot.orderedItemIDs)
                XCTAssertNil(pages.item(at: -1, in: snapshot))
                XCTAssertNil(pages.item(at: pages.count, in: snapshot))
            }
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

        remote = remoteItem(fingerprint: Data([2]), localIdentifier: "downloaded")
        XCTAssertTrue(snapshot().isEmpty)
        XCTAssertTrue(snapshot().months.isEmpty)

        remote = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        XCTAssertEqual(snapshot().itemIDs(inSection: 0), [remote.id])
    }

    @MainActor
    func testViewerResolvesDownloadedItemOutsideFilteredGridWithoutAnOverride() throws {
        let original = remoteItem(fingerprint: Data([2]), localIdentifier: nil)
        let session = MediaBrowserSession()
        session.replace(with: MediaBrowserSnapshot(sections: [.init(month: month, items: [original])], filter: .remoteOnly))
        let pageIDs = session.snapshot.orderedItemIDs
        session.invalidate()
        XCTAssertFalse(session.isCurrent)
        let downloaded = remoteItem(fingerprint: Data([2]), localIdentifier: "downloaded")
        session.replace(with: MediaBrowserSnapshot(sections: [.init(month: month, items: [downloaded])], filter: .remoteOnly))

        XCTAssertTrue(session.isCurrent)
        XCTAssertTrue(session.snapshot.isEmpty)
        let resolved = try XCTUnwrap(session.snapshot.resolve(pageIDs[0]))
        XCTAssertEqual(resolved, downloaded)
        XCTAssertEqual(MediaBrowserActionPolicy.actions(for: resolved, scope: .unified), [.share, .deleteLocal, .deleteRemote])
        session.replace(with: MediaBrowserSnapshot(sections: [.init(month: month, items: [original])], filter: .remoteOnly))
        XCTAssertNil(session.snapshot.resolve(pageIDs[0])?.localIdentifier)
        session.reset()
        XCTAssertFalse(session.isCurrent)
        XCTAssertNil(session.snapshot.resolve(pageIDs[0]))
    }

    func testViewerResolvesLocalToRemoteIdentityAfterBackupAndKeepsPageOrder() {
        let first = localItem("first", isBackedUp: false)
        let second = localItem("second", isBackedUp: false)
        let original = MediaBrowserSnapshot(sections: [.init(month: month, items: [first, second])])
        let remote = remoteItem(fingerprint: Data([8]), localIdentifier: "first")
        let updated = MediaBrowserSnapshot(sections: [.init(month: month, items: [second, remote])], filter: .localOnly)

        XCTAssertEqual(updated.orderedItemIDs, [second.id])
        XCTAssertEqual(original.orderedItemIDs.compactMap { updated.resolve($0) }, [remote, second])
        XCTAssertEqual(updated.resolve(first.id)?.remoteMonth, month)
    }

    func testViewerDoesNotResolveDeletedOrEditedContentAsItsOldRemoteIdentity() {
        let original = remoteItem(fingerprint: Data([1]), localIdentifier: "edited")
        let edited = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "edited",
                                      fingerprint: Data([2]), isBackedUp: false)
        let updated = MediaBrowserSnapshot(sections: [.init(month: month, items: [edited])])
        XCTAssertNil(updated.resolve(original.id))
        XCTAssertEqual(updated.resolve(.local("edited")), edited)
        XCTAssertNil(MediaBrowserSnapshot.empty.resolve(.local("edited")))
    }

    func testViewerResolvesRemainingLocalBytesWhenRemoteCopyIsRemoved() {
        let original = remoteItem(fingerprint: Data([1]), localIdentifier: "retained")
        let local = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "retained",
                                    fingerprint: Data([1]), isBackedUp: false)
        let updated = MediaBrowserSnapshot(sections: [.init(month: month, items: [local])], filter: .remoteOnly)
        XCTAssertTrue(updated.isEmpty)
        XCTAssertEqual(updated.resolve(original.id), local)
        XCTAssertFalse(updated.resolve(original.id)?.isRemoteDeletable ?? true)
    }

    func testViewerDoesNotSubstituteAnotherRemoteMonthAndChoosesAliasesDeterministically() {
        let january = remoteItem(fingerprint: Data([1]), localIdentifier: "local")
        let february = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "local",
            remote: .init(fingerprint: Data([1]), photoRelativePath: "2024/02/a.jpg", videoRelativePath: nil,
                          photoContentHash: nil, videoContentHash: nil, storageMonth: .init(year: 2024, month: 2), isIncomplete: false))
        for items in [[january, february], [february, january]] {
            let updated = MediaBrowserSnapshot(sections: [.init(month: month, items: items)])
            XCTAssertEqual(updated.resolve(.local("local")), january)
            XCTAssertEqual(updated.resolve(february.id), february)
        }
        let remaining = MediaBrowserSnapshot(sections: [.init(month: month, items: [february])])
        XCTAssertNil(remaining.resolve(january.id))
    }

    func testViewerResolutionIsIndependentOfMediaFilterAndSizeSort() {
        let video = MediaBrowserItem(kind: .video, creationDateMs: 1, localIdentifier: "video",
                                    fingerprint: nil, isBackedUp: false, fileSize: 20_000_000)
        let snapshot = MediaBrowserSnapshot(sections: [.init(month: month, items: [video])],
                                            mediaFilter: .photos, sortOrder: .fileSize)
        XCTAssertTrue(snapshot.isEmpty)
        XCTAssertEqual(snapshot.resolve(video.id), video)
    }

    func testViewerKeepsConfirmedRemoteIdentityAfterDeletingOriginalLocalCopy() {
        let fingerprint = Data([1])
        let representative = remoteItem(fingerprint: fingerprint, localIdentifier: "B")
        let remoteOnly = remoteItem(fingerprint: fingerprint, localIdentifier: nil)
        var page = MediaBrowserPageReference(.local("A"))
        let backedUp = MediaBrowserSnapshot(sections: [.init(month: month, items: [representative])],
            localFingerprintAliases: ["A": fingerprint, "B": fingerprint], filter: .localOnly)
        XCTAssertEqual(page.resolve(in: backedUp)?.localIdentifier, "A")
        XCTAssertEqual(page.resolve(in: backedUp)?.id, representative.id)
        let removedLocal = MediaBrowserSnapshot(sections: [.init(month: month, items: [remoteOnly])], filter: .localOnly)
        XCTAssertEqual(page.resolve(in: removedLocal), remoteOnly)
        XCTAssertNil(page.resolve(in: .empty))
    }

    func testLocalPageForgetsPriorBackupIdentityAfterAnEdit() {
        for fingerprint: Data? in [Data([2]), nil] {
            var page = MediaBrowserPageReference(.local("A"))
            let remote = remoteItem(fingerprint: Data([1]), localIdentifier: "A")
            XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [remote])])), remote)
            let edited = localItem("A", fingerprint: fingerprint, isBackedUp: false)
            let oldRemote = remoteItem(fingerprint: Data([1]), localIdentifier: nil)
            XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [edited, oldRemote])])), edited)
            XCTAssertNil(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [oldRemote])])))
        }
    }

    func testRemotePageDoesNotFollowEditsAfterFallingBackToLocalBytes() {
        let remote = remoteItem(fingerprint: Data([1]), localIdentifier: "A")
        var page = MediaBrowserPageReference(remote.id)
        let local = localItem("A", fingerprint: Data([1]), isBackedUp: false)
        XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [local])])), local)
        let edited = localItem("A", fingerprint: Data([2]), isBackedUp: false)
        XCTAssertNil(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [edited])])))
    }

    func testLocalPagePreservesConfirmedMonthWhenAnotherRemoteTwinAppears() {
        let january = remoteItem(fingerprint: Data([1]), localIdentifier: "A")
        let february = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "A",
            remote: .init(fingerprint: Data([1]), photoRelativePath: "2024/02/a.jpg", videoRelativePath: nil,
                          photoContentHash: nil, videoContentHash: nil, storageMonth: .init(year: 2024, month: 2), isIncomplete: false))
        var page = MediaBrowserPageReference(.local("A"))
        XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [february])])), february)
        XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [january, february])])), february)
    }

    func testLocalPageDoesNotAdoptAnotherDuplicatesIdentityAfterFallback() {
        let remote = remoteItem(fingerprint: Data([1]), localIdentifier: "A")
        var page = MediaBrowserPageReference(.local("A"))
        XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [remote])])), remote)
        let duplicate = localItem("B", fingerprint: Data([1]), isBackedUp: false)
        XCTAssertEqual(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [duplicate])])), duplicate)
        let edited = localItem("B", fingerprint: Data([2]), isBackedUp: false)
        XCTAssertNil(page.resolve(in: MediaBrowserSnapshot(sections: [.init(month: month, items: [edited])])))
    }

    func testPageOrderConsumesIntermediateVersionsWithoutRenderingAnyPage() {
        let local = localItem("A", fingerprint: nil, isBackedUp: false)
        let neighbour = localItem("B", fingerprint: nil, isBackedUp: false)
        let initial = MediaBrowserSnapshot(sections: [.init(month: month, items: [local, neighbour])])
        var pages = MediaBrowserPageOrder(snapshot: initial)
        let originalPages = pages
        let independentPages = MediaBrowserPageOrder(snapshot: initial)
        XCTAssertNotEqual(pages.id, independentPages.id)
        let remote = remoteItem(fingerprint: Data([1]), localIdentifier: "A")
        pages = pages.reconciling(with: MediaBrowserSnapshot(sections: [.init(month: month, items: [remote, neighbour])]))
        let remoteOnly = remoteItem(fingerprint: Data([1]), localIdentifier: nil)
        let removed = MediaBrowserSnapshot(sections: [.init(month: month, items: [remoteOnly, neighbour])])
        pages = pages.reconciling(with: removed)
        XCTAssertEqual(pages.item(at: 0, in: removed), remoteOnly)
        XCTAssertEqual(pages.id, originalPages.id)
        XCTAssertNil(originalPages.item(at: 0, in: removed))
        XCTAssertNil(independentPages.item(at: 0, in: removed))
        XCTAssertNil(MediaBrowserPageOrder(snapshot: initial).item(at: 0, in: removed))
        let edited = localItem("A", fingerprint: Data([2]), isBackedUp: false)
        pages = pages.reconciling(with: MediaBrowserSnapshot(sections: [.init(month: month, items: [edited, remoteOnly, neighbour])]))
        pages = pages.reconciling(with: removed)
        XCTAssertNil(pages.item(at: 0, in: removed))
        XCTAssertEqual(pages.item(at: 1, in: removed), neighbour)
        XCTAssertEqual(pages.count, 2)
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
