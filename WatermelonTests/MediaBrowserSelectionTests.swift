import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserSelectionTests: XCTestCase {
    func testSectionToggleCompletesPartialSelectionAndPreservesOtherSections() {
        let selection = makeSelection(count: 8)
        selection.setSelected(true, for: item(0).id)
        selection.setSelected(true, for: item(7).id)
        XCTAssertFalse(selection.isSectionSelected(0))
        let revision = selection.revision
        XCTAssertTrue(selection.toggleSection(0))
        XCTAssertEqual(selection.revision, revision + 1)
        XCTAssertTrue(selection.isSectionSelected(0))
        XCTAssertFalse(selection.isSectionSelected(1))
        XCTAssertEqual(selection.selectedIDs, Set([0, 1, 2, 3, 7].map { item($0).id }))
        XCTAssertTrue(selection.toggleSection(0))
        XCTAssertFalse(selection.isSectionSelected(0))
        XCTAssertEqual(selection.selectedIDs, [item(7).id])
        assertConsistent(selection)
        selection.selectAll()
        XCTAssertTrue(selection.isSectionSelected(0))
        XCTAssertTrue(selection.isSectionSelected(1))
        selection.clear()
        XCTAssertFalse(selection.isSectionSelected(0))
        XCTAssertFalse(selection.isSectionSelected(1))
        assertConsistent(selection)
    }

    func testSectionToggleUsesFilteredSizeGroupsAcrossMonths() {
        var first = item(0)
        first.fileSize = 20_000_000
        var second = item(2)
        second.fileSize = 30_000_000
        var small = item(4)
        small.fileSize = 100_000
        var hiddenVideo = item(1)
        hiddenVideo.fileSize = 40_000_000
        var hiddenBackup = item(6, backedUp: true)
        hiddenBackup.fileSize = 50_000_000
        let selection = MediaBrowserSelection()
        selection.replaceSnapshot(.init(sections: [
            .init(month: .init(year: 2026, month: 2), items: [first, hiddenVideo]),
            .init(month: .init(year: 2026, month: 1), items: [second, small, hiddenBackup]),
        ], filter: .localOnly, mediaFilter: .photos, sortOrder: .fileSize))
        selection.enter()
        XCTAssertTrue(selection.toggleSection(0))
        XCTAssertEqual(selection.selectedIDs, [first.id, second.id])
        XCTAssertEqual(selection.orderedItems.map(\.id), [second.id, first.id])
        XCTAssertTrue(selection.isSectionSelected(0))
        XCTAssertFalse(selection.isSectionSelected(1))
        assertConsistent(selection)
    }

    func testSectionStateRebuildsWhenRefreshMovesItemsAndAddsUnselectedItems() {
        let selection = makeSelection(count: 4)
        selection.toggleSection(0)
        selection.replaceSnapshot(.init(sections: [
            .init(month: .init(year: 2026, month: 2), items: [item(2), item(3)]),
            .init(month: .init(year: 2026, month: 1), items: [item(1), item(0, backedUp: true), item(4)]),
        ]))
        XCTAssertEqual(selection.selectedIDs, [item(0).id, item(1).id])
        XCTAssertFalse(selection.isSectionSelected(0))
        XCTAssertFalse(selection.isSectionSelected(1))
        selection.toggleSection(1)
        XCTAssertTrue(selection.isSectionSelected(1))
        XCTAssertFalse(selection.summary.actions.showsUpload)
        selection.remove([item(1).id])
        XCTAssertFalse(selection.isSectionSelected(1))
        assertConsistent(selection)
        selection.exit()
        XCTAssertFalse(selection.isSectionSelected(1))
    }

    func testSectionToggleRequiresSelectionAndValidNonemptySection() {
        let selection = MediaBrowserSelection()
        selection.replaceSnapshot(snapshot([item(0)]))
        XCTAssertFalse(selection.toggleSection(0))
        selection.enter()
        let revision = selection.revision
        XCTAssertFalse(selection.toggleSection(-1))
        XCTAssertFalse(selection.toggleSection(1))
        XCTAssertFalse(selection.isSectionSelected(1))
        XCTAssertEqual(selection.revision, revision)
        selection.replaceSnapshot(snapshot([]))
        XCTAssertFalse(selection.toggleSection(0))
        XCTAssertFalse(selection.isSectionSelected(0))
    }

    func testSelectAllAndSingleChangesKeepSummaryAndOrderedSelectionConsistent() {
        let selection = makeSelection(count: 50_000)
        XCTAssertTrue(selection.selectAll())
        XCTAssertEqual(selection.summary.count, 50_000)
        XCTAssertEqual(selection.summary.actions.deviceCount, 50_000)
        XCTAssertTrue(selection.summary.actions.showsUpload)
        let revision = selection.revision
        XCTAssertFalse(selection.selectAll())
        XCTAssertEqual(selection.revision, revision)
        for index in 0..<500 { XCTAssertTrue(selection.setSelected(false, for: .local("asset-\(index)"))) }
        XCTAssertEqual(selection.summary.count, 49_500)
        XCTAssertEqual(selection.orderedItems.count, 49_500)
        XCTAssertEqual(selection.orderedItems.first?.id, .local("asset-500"))
        assertConsistent(selection)
    }

    func testRefreshPreservesIDsButUpdatesCapabilitiesAndOrder() {
        let session = MediaBrowserSession()
        let first = item(0)
        let second = item(1)
        session.replace(with: snapshot([first, second, item(2)]))
        let selection = session.selection
        selection.enter()
        selection.selectAll()
        XCTAssertEqual(selection.orderedItems.map(\.id), [first.id, second.id, item(2).id])
        session.replace(with: snapshot([item(9), item(1, backedUp: true), first]))
        XCTAssertEqual(selection.selectedIDs, [first.id, second.id])
        XCTAssertEqual(selection.orderedItems.map(\.id), [second.id, first.id])
        XCTAssertFalse(selection.summary.actions.showsUpload)
        XCTAssertEqual(selection.summary.actions.deviceCount, 2)
        XCTAssertEqual(selection.summary.actions.remoteCount, 0)
        assertConsistent(selection)
        session.reset()
        XCTAssertFalse(selection.isSelecting)
        XCTAssertTrue(selection.selectedIDs.isEmpty)
        XCTAssertTrue(selection.orderedItems.isEmpty)
    }

    func testFilteredSnapshotOnlySelectsVisibleScopeAndRemovesStaleIDs() {
        let selection = MediaBrowserSelection()
        selection.replaceSnapshot(snapshot([item(0), item(1, backedUp: true), item(2)], filter: .localOnly))
        selection.enter()
        selection.selectAll()
        XCTAssertEqual(selection.selectedIDs, [item(0).id, item(2).id])
        selection.replaceSnapshot(snapshot([item(0, backedUp: true), item(1, backedUp: true), item(2)], filter: .localOnly))
        XCTAssertEqual(selection.selectedIDs, [item(2).id])
        XCTAssertFalse(selection.setSelected(true, for: item(0).id))
        XCTAssertFalse(selection.setSelected(true, for: .local("missing")))
        assertConsistent(selection)
    }

    func testLocalAndRemoteCopiesRetainDifferentDeletionScopes() {
        let fingerprint = Data([1, 2, 3])
        let local = item(0, backedUp: true)
        let remote = (1...2).map { month in
            MediaBrowserItem(kind: .photo, creationDateMs: 0, localIdentifier: "asset-0",
                remote: .init(fingerprint: fingerprint, photoRelativePath: nil, videoRelativePath: nil,
                    photoContentHash: nil, videoContentHash: nil, storageMonth: .init(year: 2026, month: month), isIncomplete: false))
        }
        let selection = MediaBrowserSelection()
        selection.replaceSnapshot(snapshot([local] + remote))
        selection.enter()
        selection.selectAll()
        XCTAssertEqual(selection.selectedIDs.count, 3)
        XCTAssertEqual(selection.summary.actions.remoteCount, 2)
        selection.remove(remote.map(\.id))
        XCTAssertEqual(selection.selectedIDs, [local.id])
        XCTAssertEqual(selection.summary.actions.remoteCount, 0)
        XCTAssertEqual(selection.summary.actions.deviceCount, 1)
        assertConsistent(selection)
    }

    func testDragReversalRestoresMixedBaselineAcrossMonths() {
        let selection = makeSelection(count: 80)
        for index in stride(from: 0, to: 80, by: 3) { selection.setSelected(true, for: item(index).id) }
        let baseline = selection.selectedIDs
        for anchor in [31, 30] {
            XCTAssertTrue(selection.beginDrag(at: item(anchor).id))
            let selects = !baseline.contains(item(anchor).id)
            for endpoint in [anchor, 79, 79, 35, 0, 28, 70, anchor] {
                selection.updateDrag(through: item(endpoint).id)
                let range = Set((min(anchor, endpoint)...max(anchor, endpoint)).map { item($0).id })
                let expected = selects ? baseline.union(range) : baseline.subtracting(range)
                XCTAssertEqual(selection.selectedIDs, expected)
                assertConsistent(selection)
            }
            selection.endDrag()
            selection.setSelected(baseline.contains(item(anchor).id), for: item(anchor).id)
        }
    }

    func testRepeatedDragEndpointDoesNotPublishChangesAndOtherMutationsWait() {
        let selection = makeSelection(count: 20)
        selection.beginDrag(at: item(3).id)
        XCTAssertTrue(selection.updateDrag(through: item(8).id))
        let revision = selection.revision
        XCTAssertFalse(selection.updateDrag(through: item(8).id))
        XCTAssertFalse(selection.selectAll())
        XCTAssertFalse(selection.toggleSection(0))
        XCTAssertFalse(selection.setSelected(true, for: item(10).id))
        XCTAssertFalse(selection.remove([item(3).id]))
        XCTAssertEqual(selection.revision, revision)
        selection.endDrag()
        XCTAssertTrue(selection.setSelected(true, for: item(10).id))
        assertConsistent(selection)
    }

    func testExitAndNewSnapshotCannotCarryAnOldDragRange() {
        let selection = makeSelection(count: 20)
        selection.beginDrag(at: item(3).id)
        selection.updateDrag(through: item(8).id)
        selection.exit()
        XCTAssertFalse(selection.isSelecting)
        XCTAssertFalse(selection.isDragging)
        XCTAssertFalse(selection.setSelected(true, for: item(10).id))
        XCTAssertFalse(selection.updateDrag(through: item(18).id))
        selection.enter()
        selection.beginDrag(at: item(3).id)
        selection.updateDrag(through: item(5).id)
        selection.replaceSnapshot(snapshot([item(4), item(3)]))
        XCTAssertFalse(selection.isDragging)
        XCTAssertEqual(selection.orderedItems.map(\.id), [item(4).id, item(3).id])
        assertConsistent(selection)
    }

    private func makeSelection(count: Int) -> MediaBrowserSelection {
        let selection = MediaBrowserSelection()
        let split = count / 2
        selection.replaceSnapshot(.init(sections: [
            .init(month: .init(year: 2026, month: 2), items: (0..<split).map { item($0) }),
            .init(month: .init(year: 2026, month: 1), items: (split..<count).map { item($0) })
        ]))
        selection.enter()
        return selection
    }

    private func item(_ index: Int, backedUp: Bool = false) -> MediaBrowserItem {
        .init(kind: index.isMultiple(of: 2) ? .photo : .video, creationDateMs: Int64(index),
              localIdentifier: "asset-\(index)", fingerprint: nil, isBackedUp: backedUp)
    }

    private func snapshot(_ items: [MediaBrowserItem], filter: MediaBrowserFilter = .all) -> MediaBrowserSnapshot {
        .init(sections: [.init(month: .init(year: 2026, month: 1), items: items)], filter: filter)
    }

    private func assertConsistent(_ selection: MediaBrowserSelection, file: StaticString = #filePath, line: UInt = #line) {
        let items = selection.orderedItems
        XCTAssertEqual(Set(items.map(\.id)), selection.selectedIDs, file: file, line: line)
        XCTAssertEqual(selection.summary.count, items.count, file: file, line: line)
        XCTAssertEqual(selection.summary.photoCount, items.filter { !$0.isVideo }.count, file: file, line: line)
        XCTAssertEqual(selection.summary.videoCount, items.filter(\.isVideo).count, file: file, line: line)
        XCTAssertEqual(selection.summary.actions, BatchActionResolver.resolve(items), file: file, line: line)
        for section in selection.snapshot.sectionIDs.indices {
            let ids = selection.snapshot.itemIDs(inSection: section)
            XCTAssertEqual(selection.isSectionSelected(section), !ids.isEmpty && ids.allSatisfy(selection.selectedIDs.contains), file: file, line: line)
        }
    }
}
