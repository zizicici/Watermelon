import Photos
import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserGridSelectionTests: XCTestCase {
    func testSectionButtonsTrackMonthAndSizeSelectionsAndRefreshedContents() async throws {
        let items = [20_000_000, 1_000_000, 30_000_000, 2_000_000].enumerated().map { index, size in
            MediaBrowserItem(kind: .photo, creationDateMs: Int64(index), localIdentifier: "section-\(index)",
                fingerprint: nil, isBackedUp: false, fileSize: Int64(size))
        }
        let source = SelectionSource(sections: [
            .init(month: .init(year: 2026, month: 2), items: Array(items.prefix(2))),
            .init(month: .init(year: 2026, month: 1), items: Array(items.suffix(2))),
        ])
        let fixture = try MediaBrowserTestFixture(source: source)
        defer { fixture.close() }
        let controller = fixture.controller
        let collection = fixture.collection
        let dataSource = fixture.dataSource
        let window = fixture.window
        let presence = fixture.presence
        let runner = fixture.runner
        try await waitUntil {
            controller.view.layoutIfNeeded()
            return collection.numberOfSections == 2 && collection.numberOfItems(inSection: 1) == 2
        }
        let navigation = try XCTUnwrap(controller.navigationController)
        let initialBottomInset = controller.view.safeAreaInsets.bottom
        XCTAssertTrue(navigation.isToolbarHidden)
        let firstHeader = try XCTUnwrap(collection.supplementaryView(forElementKind: "month-header", at: IndexPath(item: 0, section: 0)))
        let secondHeader = try XCTUnwrap(collection.supplementaryView(forElementKind: "month-header", at: IndexPath(item: 0, section: 1)))
        let firstButton = try checkbox(in: firstHeader)
        let secondButton = try checkbox(in: secondHeader)
        XCTAssertTrue(firstButton.isHidden)
        XCTAssertTrue(secondButton.isHidden)
        controller.perform(NSSelectorFromString("enterSelection"))
        XCTAssertTrue(navigation.isToolbarHidden)
        XCTAssertFalse(firstButton.isHidden)
        XCTAssertTrue(firstButton.isEnabled)
        XCTAssertGreaterThanOrEqual(firstButton.bounds.width, 44)
        XCTAssertGreaterThanOrEqual(firstButton.bounds.height, 44)
        firstButton.sendActions(for: .touchUpInside)
        XCTAssertTrue(firstButton.isSelected)
        XCTAssertFalse(secondButton.isSelected)
        XCTAssertFalse(navigation.isToolbarHidden)
        let deleteItem = try XCTUnwrap(controller.toolbarItems?.first { $0.accessibilityIdentifier == "mediaBrowser.batch.delete" })
        XCTAssertNil(deleteItem.customView)
        XCTAssertNil(deleteItem.image)
        XCTAssertEqual(deleteItem.title, String(localized: "mediaBrowser.action.delete"))
        XCTAssertEqual(deleteItem.accessibilityLabel, String(localized: "mediaBrowser.action.delete"))
        XCTAssertEqual(deleteItem.tintColor, .systemRed)
        XCTAssertTrue(deleteItem.isEnabled)
        XCTAssertEqual(controller.toolbarItems?.compactMap(\.accessibilityIdentifier), ["mediaBrowser.batch.delete"])
        try await waitUntil {
            window.layoutIfNeeded()
            return controller.view.safeAreaInsets.bottom > initialBottomInset
        }
        XCTAssertEqual(collection.contentInset.bottom, 0)
        let firstCell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: 0, section: 0)))
        let secondCell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: 1, section: 0)))
        XCTAssertTrue(try checkbox(in: firstCell).isSelected)
        XCTAssertTrue(try checkbox(in: secondCell).isSelected)
        try checkbox(in: firstCell).sendActions(for: .touchUpInside)
        XCTAssertFalse(firstButton.isSelected)
        firstButton.sendActions(for: .touchUpInside)
        XCTAssertTrue(firstButton.isSelected)
        XCTAssertTrue(try checkbox(in: firstCell).isSelected)
        firstButton.sendActions(for: .touchUpInside)
        XCTAssertFalse(firstButton.isSelected)
        XCTAssertFalse(try checkbox(in: secondCell).isSelected)
        XCTAssertTrue(navigation.isToolbarHidden)
        XCTAssertFalse(deleteItem.isEnabled)
        controller.perform(NSSelectorFromString("selectAllItems"))
        XCTAssertTrue(controller.toolbarItems?.contains { $0 === deleteItem } ?? false)
        XCTAssertTrue(firstButton.isSelected)
        XCTAssertTrue(secondButton.isSelected)
        controller.collectionView(collection, didBeginMultipleSelectionInteractionAt: IndexPath(item: 0, section: 0))
        XCTAssertFalse(firstButton.isEnabled)
        XCTAssertFalse(deleteItem.isEnabled)
        firstButton.sendActions(for: .touchUpInside)
        XCTAssertTrue(firstButton.isSelected)
        controller.collectionViewDidEndMultipleSelectionInteraction(collection)
        XCTAssertTrue(firstButton.isEnabled)
        XCTAssertTrue(deleteItem.isEnabled)
        firstButton.sendActions(for: .touchUpInside)
        XCTAssertFalse(firstButton.isSelected)
        XCTAssertTrue(secondButton.isSelected)

        try await Task.sleep(for: .milliseconds(300))
        let nextController = UIViewController()
        let loadsBeforeHiding = source.loadRequests.count
        navigation.pushViewController(nextController, animated: false)
        try await waitUntil { navigation.topViewController === nextController && navigation.isToolbarHidden }
        source.replace(items: Array(items.reversed()))
        for _ in 0..<20 {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        }
        try await waitUntil {
            source.loadRequests.count == loadsBeforeHiding + 1 && dataSource.snapshot().numberOfSections == 1
        }
        XCTAssertTrue(navigation.isToolbarHidden)
        navigation.popViewController(animated: false)
        try await waitUntil {
            navigation.topViewController === controller && !navigation.isToolbarHidden
                && dataSource.snapshot().numberOfSections == 1
        }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(source.loadRequests.count, loadsBeforeHiding + 1)
        XCTAssertTrue(controller.toolbarItems?.contains { $0 === deleteItem } ?? false)

        controller.setSortOrder(.fileSize)
        XCTAssertTrue(navigation.isToolbarHidden)
        try await waitUntil {
            dataSource.snapshot().sectionIdentifiers == [.fileSize(.from10MBTo100MB), .fileSize(.from1MBTo10MB)]
        }
        controller.view.layoutIfNeeded()
        controller.perform(NSSelectorFromString("enterSelection"))
        let sizeHeader = try XCTUnwrap(collection.supplementaryView(forElementKind: "month-header", at: IndexPath(item: 0, section: 0)))
        let sizeButton = try checkbox(in: sizeHeader)
        XCTAssertFalse(sizeButton.isSelected)
        sizeButton.sendActions(for: .touchUpInside)
        for index in 0..<2 {
            let cell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: index, section: 0)))
            XCTAssertTrue(try checkbox(in: cell).isSelected)
        }
        let newItem = MediaBrowserItem(kind: .photo, creationDateMs: 5, localIdentifier: "new-section-item",
                                      fingerprint: nil, isBackedUp: false, fileSize: 25_000_000)
        source.replace(items: items + [newItem])
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await waitUntil { dataSource.snapshot().numberOfItems == 5 }
        controller.view.layoutIfNeeded()
        let refreshedHeader = try XCTUnwrap(collection.supplementaryView(forElementKind: "month-header", at: IndexPath(item: 0, section: 0)))
        let refreshedButton = try checkbox(in: refreshedHeader)
        XCTAssertFalse(refreshedButton.isSelected)
        refreshedButton.sendActions(for: .touchUpInside)
        XCTAssertTrue(refreshedButton.isSelected)
        let newPath = try XCTUnwrap(dataSource.indexPath(for: newItem.id))
        let newCell = try XCTUnwrap(collection.cellForItem(at: newPath))
        XCTAssertTrue(try checkbox(in: newCell).isSelected)
        controller.perform(NSSelectorFromString("cancelSelection"))
        XCTAssertTrue(refreshedButton.isHidden)
        XCTAssertFalse(try checkbox(in: newCell).isSelected)
        XCTAssertTrue(navigation.isToolbarHidden)
        XCTAssertTrue(controller.toolbarItems?.isEmpty ?? true)
        let staleAction = try XCTUnwrap(deleteItem.action)
        UIApplication.shared.sendAction(staleAction, to: deleteItem.target, from: deleteItem, for: nil)
        XCTAssertFalse(runner.isActionRunning)
    }

    func testSizeSortMenuSelectionLabelsAndSwitchBack() async throws {
        let small = MediaBrowserItem(kind: .photo, creationDateMs: 2, localIdentifier: "small",
                                     fingerprint: nil, isBackedUp: false, fileSize: 1_000_000)
        let large = MediaBrowserItem(kind: .livePhoto, creationDateMs: 1, localIdentifier: "large",
                                     fingerprint: nil, isBackedUp: true, fileSize: 100_000_000)
        let unknown = MediaBrowserItem(kind: .video, creationDateMs: 0, localIdentifier: "unknown",
                                       fingerprint: nil, isBackedUp: false)
        let source = SelectionSource(items: [small, large, unknown])
        let fixture = try MediaBrowserTestFixture(source: source, remoteSource: source)
        defer { fixture.close() }
        let controller = fixture.controller
        let collection = fixture.collection
        let dataSource = fixture.dataSource
        let presence = fixture.presence
        try await waitUntil {
            controller.view.layoutIfNeeded()
            return collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == 3
        }
        XCTAssertEqual(source.loadRequests, [false])
        let filterButton = try XCTUnwrap(controller.navigationItem.trailingItemGroups.first?.barButtonItems.first)
        let groups = try XCTUnwrap(filterButton.menu).children.compactMap { $0 as? UIMenu }
        XCTAssertEqual(groups.count, 3)
        XCTAssertTrue(groups.allSatisfy { $0.options.contains(.displayInline) && $0.options.contains(.singleSelection) })
        XCTAssertEqual(groups.map(\.title), [String(localized: "mediaBrowser.sort"), String(localized: "mediaBrowser.filter.mediaType"), MediaBrowserMode.local.filterGroupTitle])
        XCTAssertEqual(groups[0].children.count, 2)
        XCTAssertEqual(groups[1].children.count, 3)
        XCTAssertEqual(groups[2].children.count, 3)
        XCTAssertEqual(groups[2].children.compactMap { ($0 as? UIAction)?.title }, MediaBrowserMode.local.availableFilters.map { $0.title(in: .local) })

        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        controller.setSortOrder(.fileSize)
        let sizeSections: [MediaBrowserSectionID] = [.fileSize(.from100MBTo1GB), .fileSize(.from1MBTo10MB), .fileSize(.unknown)]
        try await waitUntil { dataSource.snapshot().sectionIdentifiers == sizeSections }
        XCTAssertEqual(source.loadRequests, [false, true])
        collection.layoutIfNeeded()
        XCTAssertEqual(dataSource.snapshot().itemIdentifiers, [large.id, small.id, unknown.id])
        XCTAssertTrue(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        XCTAssertFalse(collection.visibleSupplementaryViews(ofKind: "month-header").isEmpty)
        for (index, expected) in [large, small, unknown].enumerated() {
            let cell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: 0, section: index)))
            let label = try XCTUnwrap(cell.contentView.subviews.compactMap { $0 as? UILabel }.first)
            XCTAssertFalse(label.isHidden)
            XCTAssertEqual(label.text, expected.fileSize.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) } ?? "—")
            XCTAssertEqual(label.frame.maxX, cell.contentView.bounds.maxX - 6, accuracy: 0.5)
            let icons = cell.contentView.subviews.compactMap { $0 as? UIImageView }.filter {
                !$0.isHidden && abs($0.frame.minX - 6) < 0.5 && $0.frame.midY > cell.contentView.bounds.midY
            }
            if expected.isVideo || expected.isLivePhoto {
                XCTAssertEqual(icons.count, 1)
                XCTAssertEqual(label.frame.midY, try XCTUnwrap(icons.first).frame.midY, accuracy: 0.5)
            }
        }
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        controller.setMediaFilter(.videos)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [unknown.id] }
        XCTAssertEqual(dataSource.snapshot().sectionIdentifiers, [.fileSize(.unknown)])
        XCTAssertTrue(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        controller.setFilter(.backedUp)
        try await waitUntil { dataSource.snapshot().sectionIdentifiers.isEmpty }
        XCTAssertTrue(filterButton.isEnabled)
        controller.setMediaFilter(.photos)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [large.id] }
        controller.setFilter(.localOnly)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [small.id] }
        XCTAssertEqual(source.loadRequests, [false, true])
        let switcher = try XCTUnwrap(controller.navigationItem.titleView as? UISegmentedControl)
        switcher.selectedSegmentIndex = 1
        switcher.sendActions(for: .valueChanged)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [large.id, small.id] }
        XCTAssertEqual(source.loadRequests, [false, true, true])
        XCTAssertEqual(dataSource.snapshot().sectionIdentifiers, Array(sizeSections.prefix(2)))
        XCTAssertEqual(filterButton.menu?.children.last?.title, MediaBrowserMode.remote.filterGroupTitle)
        switcher.selectedSegmentIndex = 0
        switcher.sendActions(for: .valueChanged)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [small.id] }
        XCTAssertEqual(source.loadRequests, [false, true, true, true])
        controller.setSortOrder(.month)
        try await waitUntil { dataSource.snapshot().sectionIdentifiers == [.month(.init(year: 2026, month: 1))] }
        collection.layoutIfNeeded()
        for cell in collection.visibleCells {
            XCTAssertTrue(try XCTUnwrap(cell.contentView.subviews.compactMap { $0 as? UILabel }.first).isHidden)
        }
        XCTAssertFalse(collection.visibleSupplementaryViews(ofKind: "month-header").isEmpty)
        controller.setFilter(.all)
        controller.setMediaFilter(.all)
        controller.setSortOrder(.fileSize)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [large.id, small.id, unknown.id] }
        XCTAssertEqual(source.loadRequests, [false, true, true, true])
        var resized = large
        resized.fileSize = 500_000
        source.replace(items: [small, resized, unknown])
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [small.id, large.id, unknown.id] }
        collection.layoutIfNeeded()
        XCTAssertEqual(dataSource.snapshot().sectionIdentifiers, [.fileSize(.from1MBTo10MB), .fileSize(.below1MB), .fileSize(.unknown)])
        let resizedCell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: 0, section: 1)))
        let sizeLabel = try XCTUnwrap(resizedCell.contentView.subviews.compactMap { $0 as? UILabel }.first)
        XCTAssertEqual(sizeLabel.text, ByteCountFormatter.string(fromByteCount: 500_000, countStyle: .file))
        XCTAssertEqual(source.loadRequests, [false, true, true, true, true])

        let refreshGate = LoadGate()
        defer { Task { await refreshGate.finish() } }
        source.holdNextLoad(refreshGate)
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await waitUntil { source.loadRequests.count == 6 }
        let freshVideo = MediaBrowserItem(kind: .video, creationDateMs: 3, localIdentifier: "fresh-video",
                                          fingerprint: nil, isBackedUp: false, fileSize: 20_000_000)
        source.replace(items: [freshVideo])
        controller.setMediaFilter(.videos)
        try await waitUntil { dataSource.snapshot().itemIdentifiers == [freshVideo.id] }
        XCTAssertEqual(source.loadRequests.count, 7)
        await refreshGate.finish()
        try await Task.sleep(for: .milliseconds(30))
        controller.setMediaFilter(.all)
        try await Task.sleep(for: .milliseconds(30))
        XCTAssertEqual(dataSource.snapshot().itemIdentifiers, [freshVideo.id])
        XCTAssertEqual(source.loadRequests.count, 7)
    }

    func testLargeSelectAllSurvivesIdleReloadScrollAndNativeDeselection() async throws {
        let items = (0..<50_000).map { index in
            MediaBrowserItem(kind: .photo, creationDateMs: Int64(index),
                localIdentifier: "asset-\(index)", fingerprint: nil, isBackedUp: index.isMultiple(of: 2))
        }
        let source = SelectionSource(items: items)
        let remoteSource = SelectionSource(items: Array(items.prefix(4)))
        let fixture = try MediaBrowserTestFixture(source: source, remoteSource: remoteSource)
        defer { fixture.close() }
        let controller = fixture.controller
        let collection = fixture.collection
        let dataSource = fixture.dataSource
        let window = fixture.window
        let presence = fixture.presence
        try await waitUntil {
            controller.view.layoutIfNeeded()
            return collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == items.count
                && !collection.indexPathsForVisibleItems.isEmpty
        }
        let firstPath = IndexPath(item: 0, section: 0)
        let firstCell = try XCTUnwrap(collection.cellForItem(at: firstPath))
        let firstCheckbox = try checkbox(in: firstCell)
        firstCheckbox.sendActions(for: .touchUpInside)
        XCTAssertTrue(firstCheckbox.isSelected)
        XCTAssertEqual(collection.indexPathsForSelectedItems, [firstPath])
        firstCheckbox.sendActions(for: .touchUpInside)
        XCTAssertFalse(firstCheckbox.isSelected)
        XCTAssertTrue(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        let selectionStarted = ProcessInfo.processInfo.systemUptime
        controller.perform(NSSelectorFromString("selectAllItems"))
        let selectionMilliseconds = (ProcessInfo.processInfo.systemUptime - selectionStarted) * 1_000
        assertVisibleSelection(collection, selected: true)

        let reloadStarted = ProcessInfo.processInfo.systemUptime
        for round in 0..<3 {
            let marker = MediaBrowserItem(kind: .photo, creationDateMs: 0,
                localIdentifier: "refresh-\(round)", fingerprint: nil, isBackedUp: false)
            source.replace(items: items.dropLast() + [marker])
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
            try await waitUntil {
                dataSource.itemIdentifier(for: IndexPath(item: items.count - 1, section: 0)) == marker.id
            }
            await Task.yield()
            collection.layoutIfNeeded()
            assertVisibleSelection(collection, selected: true)
        }
        print("GRID_SELECTION count=\(items.count) nativeSelected=\(collection.indexPathsForSelectedItems?.count ?? 0) visible=\(collection.indexPathsForVisibleItems.count) selectAllMs=\(selectionMilliseconds) threeReloadsMs=\((ProcessInfo.processInfo.systemUptime - reloadStarted) * 1_000)")

        let lastPath = IndexPath(item: items.count - 1, section: 0)
        collection.scrollToItem(at: lastPath, at: .bottom, animated: false)
        window.layoutIfNeeded()
        let lastCell = try XCTUnwrap(collection.cellForItem(at: lastPath))
        XCTAssertLessThanOrEqual(lastCell.frame.maxY, collection.bounds.maxY - collection.adjustedContentInset.bottom + 0.5)
        XCTAssertEqual(collection.contentInset.bottom, 0)

        collection.scrollToItem(at: IndexPath(item: 40_000, section: 0), at: .top, animated: false)
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: true)
        collection.scrollToItem(at: IndexPath(item: 0, section: 0), at: .top, animated: false)
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: true)

        let path = try XCTUnwrap(collection.indexPathsForVisibleItems.first)
        controller.collectionView(collection, didBeginMultipleSelectionInteractionAt: path)
        collection.deselectItem(at: path, animated: false)
        controller.collectionView(collection, didDeselectItemAt: path)
        controller.collectionViewDidEndMultipleSelectionInteraction(collection)
        XCTAssertFalse(collection.indexPathsForSelectedItems?.contains(path) ?? false)
        collection.scrollToItem(at: IndexPath(item: 40_000, section: 0), at: .top, animated: false)
        collection.layoutIfNeeded()
        collection.scrollToItem(at: path, at: .top, animated: false)
        collection.layoutIfNeeded()
        XCTAssertFalse(collection.indexPathsForSelectedItems?.contains(path) ?? false)

        controller.perform(NSSelectorFromString("selectAllItems"))
        collection.scrollToItem(at: firstPath, at: .top, animated: false)
        collection.layoutIfNeeded()
        let dragCell = try XCTUnwrap(collection.cellForItem(at: firstPath))
        let drag = SelectionDragGesture()
        drag.testState = .began
        dragCell.perform(NSSelectorFromString("selectionButtonDragged:"), with: drag)
        drag.testState = .changed
        drag.testLocation = try XCTUnwrap(collection.layoutAttributesForItem(at: IndexPath(item: 8, section: 0))).center
        dragCell.perform(NSSelectorFromString("selectionButtonDragged:"), with: drag)
        for index in 0...8 { XCTAssertFalse(collection.indexPathsForSelectedItems?.contains(IndexPath(item: index, section: 0)) ?? false) }
        drag.testLocation = try XCTUnwrap(collection.layoutAttributesForItem(at: IndexPath(item: 3, section: 0))).center
        dragCell.perform(NSSelectorFromString("selectionButtonDragged:"), with: drag)
        for index in 4...8 { XCTAssertTrue(collection.indexPathsForSelectedItems?.contains(IndexPath(item: index, section: 0)) ?? false) }
        drag.testState = .ended
        dragCell.perform(NSSelectorFromString("selectionButtonDragged:"), with: drag)
        controller.perform(NSSelectorFromString("selectAllItems"))
        assertVisibleSelection(collection, selected: true)

        let inserted = MediaBrowserItem(kind: .photo, creationDateMs: 0,
            localIdentifier: "inserted", fingerprint: nil, isBackedUp: false)
        source.replace(items: [inserted] + items.dropFirst())
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await waitUntil { dataSource.itemIdentifier(for: IndexPath(item: 0, section: 0)) == inserted.id }
        collection.layoutIfNeeded()
        try await Task.sleep(for: .milliseconds(100))
        XCTAssertFalse(collection.indexPathsForSelectedItems?.contains(IndexPath(item: 0, section: 0)) ?? false)
        XCTAssertLessThanOrEqual(collection.indexPathsForSelectedItems?.count ?? 0, collection.indexPathsForVisibleItems.count)

        let switcher = try XCTUnwrap(controller.navigationItem.titleView as? UISegmentedControl)
        switcher.selectedSegmentIndex = 1
        switcher.sendActions(for: .valueChanged)
        try await waitUntil { collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == 4 }
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: false)
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        assertVisibleSelection(collection, selected: true)
        switcher.selectedSegmentIndex = 0
        switcher.sendActions(for: .valueChanged)
        try await waitUntil { collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == items.count }
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: false)
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        controller.setFilter(.backedUp)
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        XCTAssertTrue(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        try await waitUntil { collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == 24_999 }
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: false)
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        assertVisibleSelection(collection, selected: true)
        controller.setFilter(.all)
        try await waitUntil { collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == items.count }
        collection.layoutIfNeeded()
        assertVisibleSelection(collection, selected: false)
        controller.perform(NSSelectorFromString("enterSelection"))
        controller.perform(NSSelectorFromString("selectAllItems"))
        let checkbox = try checkbox(in: XCTUnwrap(collection.cellForItem(at: firstPath)))
        checkbox.sendActions(for: .touchUpInside)
        XCTAssertFalse(checkbox.isSelected)

        let gate = ActionGate()
        let deleteItem = try XCTUnwrap(controller.toolbarItems?.first { $0.accessibilityIdentifier == "mediaBrowser.batch.delete" })
        XCTAssertTrue(controller.runExternalSelectionAction { _ in await gate.wait(); return false })
        XCTAssertFalse(deleteItem.isEnabled)
        try await waitUntil { gate.continuation != nil }
        checkbox.sendActions(for: .touchUpInside)
        controller.perform(NSSelectorFromString("selectAllItems"))
        controller.perform(NSSelectorFromString("cancelSelection"))
        XCTAssertFalse(checkbox.isSelected)
        XCTAssertFalse(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        XCTAssertFalse(controller.collectionView(collection, shouldBeginMultipleSelectionInteractionAt: firstPath))
        switcher.selectedSegmentIndex = 1
        switcher.sendActions(for: .valueChanged)
        XCTAssertEqual(switcher.selectedSegmentIndex, 0)
        source.replace(items: [])
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await Task.sleep(for: .milliseconds(50))
        XCTAssertEqual(collection.numberOfItems(inSection: 0), items.count)
        gate.finish()
        try await waitUntil { collection.numberOfSections == 0 || collection.numberOfItems(inSection: 0) == 0 }
        XCTAssertTrue(collection.indexPathsForSelectedItems?.isEmpty ?? true)
        controller.perform(NSSelectorFromString("exitSelection"))
    }

    func testCommittedChangesDuringActionRefreshOnceWithoutCompletionInvalidation() async throws {
        let original = lifecycleItem("original")
        let updated = lifecycleItem("committed")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        try await waitUntil { fixture.itemIDs == [original.id] }
        let initialLoads = fixture.source.loadRequests.count
        let gate = ActionGate()
        defer { gate.finish() }
        XCTAssertTrue(fixture.controller.runExternalSelectionAction { _ in
            await gate.wait()
            return false
        })
        try await waitUntil { gate.continuation != nil }
        fixture.source.replace(items: [updated])
        let presence = fixture.presence
        let delivered = expectation(description: "Commits invalidate while the action is still running")
        delivered.expectedFulfillmentCount = 3
        let observer = NotificationCenter.default.addObserver(forName: .LibraryPresenceDidChange,
            object: nil, queue: .main) { notification in
                if notification.object as? LibraryPresenceIndex === presence { delivered.fulfill() }
            }
        defer { NotificationCenter.default.removeObserver(observer) }
        for index in 0..<3 {
            let instance = RemoteAssetResourceInstance(role: 1, slot: 0,
                resourceHash: Data(repeating: UInt8(index), count: 32), fileName: "photo.jpg", fileSize: 10,
                remoteRelativePath: "2026/10/photo.jpg", creationDateMs: 1)
            try fixture.dependencies.hashIndexRepository.writeHashIndex(assetLocalIdentifier: "committed-\(index)", instances: [instance])
            try await Task.sleep(for: .milliseconds(30))
        }
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads)
        XCTAssertEqual(fixture.itemIDs, [original.id])
        gate.finish()
        try await waitUntil { fixture.itemIDs == [updated.id] }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
    }

    func testFullScreenCoverAllowsUpdatesWithoutReloadingOnDismissal() async throws {
        let original = lifecycleItem("original")
        let updated = lifecycleItem("updated")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        try await waitUntil { fixture.itemIDs == [original.id] }
        let initialLoads = fixture.source.loadRequests.count
        let cover = UIViewController()
        cover.modalPresentationStyle = .fullScreen
        await withCheckedContinuation { continuation in
            fixture.controller.present(cover, animated: false) { continuation.resume() }
        }
        fixture.source.replace(items: [updated])
        fixture.postPresenceBurst()
        try await waitUntil { fixture.itemIDs == [updated.id] }
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
        await withCheckedContinuation { continuation in cover.dismiss(animated: false) { continuation.resume() } }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
    }

    func testGlobalExecutionRefreshesPeriodicallyAndBackgroundWaitsForForeground() async throws {
        let original = lifecycleItem("original")
        let executed = lifecycleItem("executed")
        let foreground = lifecycleItem("foreground")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        try await waitUntil { fixture.itemIDs == [original.id] }
        let initialLoads = fixture.source.loadRequests.count

        let otherRuntime = AppRuntimeFlags()
        var executionClaim: AppRuntimeFlags.ExecutionClaim? = try XCTUnwrap(otherRuntime.tryEnterExecution())
        defer { if let executionClaim { otherRuntime.exitExecution(executionClaim) } }
        XCTAssertTrue(fixture.dependencies.appRuntimeFlags.isExecuting)
        fixture.source.replace(items: [executed])
        fixture.postPresenceBurst()
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads)
        XCTAssertEqual(fixture.itemIDs, [original.id])

        try await waitUntil { fixture.itemIDs == [executed.id] }
        XCTAssertTrue(fixture.dependencies.appRuntimeFlags.isExecuting)
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)

        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        var isBackgrounded = true
        defer {
            if isBackgrounded { NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil) }
        }
        fixture.source.replace(items: [foreground])
        fixture.postPresenceBurst()
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
        XCTAssertEqual(fixture.itemIDs, [executed.id])

        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        isBackgrounded = false
        try await waitUntil { fixture.itemIDs == [foreground.id] }
        XCTAssertTrue(fixture.dependencies.appRuntimeFlags.isExecuting)
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 2)
        otherRuntime.exitExecution(try XCTUnwrap(executionClaim))
        executionClaim = nil
    }

    func testNoncancellingLoadCannotCommitWhileBackgrounded() async throws {
        let original = lifecycleItem("original")
        let stale = lifecycleItem("stale")
        let fresh = lifecycleItem("fresh")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        try await waitUntil { fixture.itemIDs == [original.id] }
        let initialLoads = fixture.source.loadRequests.count
        let gate = LoadGate()
        defer { Task { await gate.finish() } }
        fixture.source.replace(items: [stale])
        fixture.source.holdNextLoad(gate)
        fixture.postPresenceBurst()
        try await waitUntil { fixture.source.loadRequests.count == initialLoads + 1 }

        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        var isBackgrounded = true
        defer {
            if isBackgrounded { NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil) }
        }
        fixture.source.replace(items: [fresh])
        fixture.postPresenceBurst()
        await gate.finish()
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
        XCTAssertEqual(fixture.itemIDs, [original.id])

        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        isBackgrounded = false
        try await waitUntil { fixture.itemIDs == [fresh.id] }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 2)
    }

    func testInitialAndTrailingLoadsFinishDuringExecutionWithContinuousCommits() async throws {
        let otherRuntime = AppRuntimeFlags()
        var executionClaim: AppRuntimeFlags.ExecutionClaim? = try XCTUnwrap(otherRuntime.tryEnterExecution())
        defer { if let executionClaim { otherRuntime.exitExecution(executionClaim) } }
        let original = lifecycleItem("original")
        let fresh = lifecycleItem("fresh")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        let gate = LoadGate()
        defer { Task { await gate.finish() } }
        fixture.source.holdNextLoad(gate)
        try await waitUntil { fixture.source.loadRequests.count == 1 }
        XCTAssertTrue(fixture.itemIDs.isEmpty)

        fixture.source.replace(items: [fresh])
        for _ in 0..<6 {
            fixture.postPresenceBurst()
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTAssertEqual(fixture.source.loadRequests.count, 1)
        await gate.finish()
        try await waitUntil { fixture.itemIDs == [original.id] }
        var updatedDuringCommits = false
        for _ in 0..<30 {
            fixture.postPresenceBurst()
            try await Task.sleep(for: .milliseconds(50))
            updatedDuringCommits = updatedDuringCommits || fixture.itemIDs == [fresh.id]
        }
        XCTAssertTrue(fixture.dependencies.appRuntimeFlags.isExecuting)
        XCTAssertTrue(updatedDuringCommits)
        XCTAssertEqual(fixture.itemIDs, [fresh.id])
        XCTAssertEqual(fixture.source.loadRequests.count, 2)

        otherRuntime.exitExecution(try XCTUnwrap(executionClaim))
        executionClaim = nil
    }

    func testAutomaticChangesPublishCompletedRoundBeforeTrailingRefreshFinishes() async throws {
        let original = lifecycleItem("original")
        let intermediate = lifecycleItem("intermediate")
        let latest = lifecycleItem("latest")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        try await waitUntil { fixture.itemIDs == [original.id] }
        let first = LoadGate()
        let trailing = LoadGate()
        defer { Task { await first.finish(); await trailing.finish() } }
        fixture.source.replace(items: [intermediate])
        fixture.source.holdNextLoad(first)
        fixture.postPresenceBurst()
        try await waitUntil { fixture.source.loadRequests.count == 2 }

        fixture.source.replace(items: [latest])
        fixture.source.holdNextLoad(trailing)
        for _ in 0..<6 {
            fixture.postPresenceBurst()
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTAssertEqual(fixture.source.loadRequests.count, 2)
        await first.finish()
        try await waitUntil { fixture.itemIDs == [intermediate.id] && fixture.source.loadRequests.count == 3 }
        await trailing.finish()
        try await waitUntil { fixture.itemIDs == [latest.id] }
        XCTAssertEqual(fixture.source.loadRequests.count, 3)
    }

    func testTimeZoneChangeRejectsSlowResultsAndCachedFilterAndSortContent() async throws {
        let previousTimeZone = MonthGroupingTimeZonePreference.current
        defer { MonthGroupingTimeZonePreference.setCurrent(previousTimeZone) }
        MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 0))
        let original = lifecycleItem("original")
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        fixture.controller.setSortOrder(.fileSize)
        try await waitUntil { fixture.itemIDs == [original.id] }
        let initialLoads = fixture.source.loadRequests.count
        let obsolete = LoadGate()
        let replacement = LoadGate()
        defer { Task { await obsolete.finish(); await replacement.finish() } }
        fixture.source.replace(items: [lifecycleItem("obsolete")])
        fixture.source.holdNextLoad(obsolete)
        fixture.postPresenceBurst()
        try await waitUntil { fixture.source.loadRequests.count == initialLoads + 1 }

        let current = lifecycleItem("current")
        fixture.source.replace(items: [current])
        fixture.source.holdNextLoad(replacement)
        MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 3_600))
        await obsolete.finish()
        try await waitUntil { fixture.source.loadRequests.count == initialLoads + 2 }
        XCTAssertEqual(fixture.itemIDs, [original.id])
        await replacement.finish()
        try await waitUntil { fixture.itemIDs == [current.id] }

        let filtered = lifecycleItem("filtered")
        fixture.source.replace(items: [filtered])
        MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 7_200))
        fixture.controller.setFilter(.localOnly)
        try await waitUntil { fixture.itemIDs == [filtered.id] }
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 3)

        let sorted = lifecycleItem("sorted")
        fixture.source.replace(items: [sorted])
        MonthGroupingTimeZonePreference.setCurrent(.init(mode: .fixedOffset, offsetSeconds: 10_800))
        fixture.controller.setSortOrder(.month)
        try await waitUntil { fixture.itemIDs == [sorted.id] }
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 4)
    }

    func testForegroundRefreshUpdatesGridWhileViewerIsPresented() async throws {
        var original = lifecycleItem("same-asset")
        original.fileSize = 20_000
        let firstImage = testThumbnail(.red)
        let nextImage = testThumbnail(.blue)
        let fixture = try MediaBrowserTestFixture(source: SelectionSource(items: [original]))
        defer { fixture.close() }
        fixture.source.setThumbnail(firstImage)
        fixture.controller.setSortOrder(.fileSize)
        try await waitUntil {
            fixture.controller.view.layoutIfNeeded()
            return fixture.itemIDs == [original.id]
                && fixture.controller.heroSource(forItemID: original.id)?.image === firstImage
        }
        let indexPath = IndexPath(item: 0, section: 0)
        let initialCell = try XCTUnwrap(fixture.collection.cellForItem(at: indexPath))
        let initialThumbnailRequests = fixture.source.thumbnailRequestCount
        XCTAssertGreaterThan(initialThumbnailRequests, 0)
        fixture.controller.collectionView(fixture.collection, didSelectItemAt: indexPath)
        let viewer = try XCTUnwrap(fixture.controller.presentedViewController as? MediaBrowserViewerViewController)
        try await waitUntil { viewer.isDisplayed && viewer.heroDestination() != nil }

        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        var updated = original
        updated.fileSize = 30_000
        fixture.source.setThumbnail(nextImage)
        fixture.source.replace(items: [updated])
        let initialLoads = fixture.source.loadRequests.count
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        let expectedSize = ByteCountFormatter.string(fromByteCount: 30_000, countStyle: .file)
        try await waitUntil {
            fixture.collection.cellForItem(at: indexPath)?.contentView.subviews.compactMap { $0 as? UILabel }
                .contains { $0.accessibilityIdentifier == "mediaBrowser.fileSize" && $0.text == expectedSize } == true
        }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(fixture.source.loadRequests.count, initialLoads + 1)
        XCTAssertTrue(fixture.collection.cellForItem(at: indexPath) === initialCell)
        try await waitUntil { fixture.controller.heroSource(forItemID: original.id)?.image === nextImage }
        XCTAssertEqual(fixture.source.thumbnailRequestCount, initialThumbnailRequests + 1)

        viewer.dismiss(animated: false)
        try await waitUntil {
            fixture.controller.presentedViewController == nil
                && fixture.controller.heroSource(forItemID: original.id)?.image === nextImage
        }
        XCTAssertEqual(fixture.source.thumbnailRequestCount, initialThumbnailRequests + 1)
    }

    private func testThumbnail(_ color: UIColor) -> UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 20, height: 20)).image { context in
            color.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 20, height: 20))
        }
    }

    private func lifecycleItem(_ identifier: String) -> MediaBrowserItem {
        MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: identifier,
                         fingerprint: nil, isBackedUp: false)
    }

    private func assertVisibleSelection(_ collection: UICollectionView, selected: Bool, file: StaticString = #filePath, line: UInt = #line) {
        let visible = Set(collection.indexPathsForVisibleItems)
        XCTAssertFalse(visible.isEmpty, file: file, line: line)
        XCTAssertEqual(Set(collection.indexPathsForSelectedItems ?? []), selected ? visible : [], file: file, line: line)
        for cell in collection.visibleCells {
            XCTAssertEqual(cell.isSelected, selected, file: file, line: line)
            XCTAssertEqual(try checkbox(in: cell).isSelected, selected, file: file, line: line)
        }
    }

    private func checkbox(in view: UIView) throws -> UIButton {
        if let button = view as? UIButton { return button }
        for child in view.subviews {
            if let button = try? checkbox(in: child) { return button }
        }
        throw NSError(domain: "SelectionCheckboxMissing", code: 1)
    }

    private final class SelectionDragGesture: UILongPressGestureRecognizer {
        var testState = UIGestureRecognizer.State.possible
        var testLocation = CGPoint.zero
        override var state: UIGestureRecognizer.State { get { testState } set { testState = newValue } }
        override func location(in view: UIView?) -> CGPoint { testLocation }
    }

    private final class ActionGate {
        var continuation: CheckedContinuation<Void, Never>?
        func wait() async { await withCheckedContinuation { continuation = $0 } }
        func finish() { continuation?.resume(); continuation = nil }
        deinit { continuation?.resume() }
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        for _ in 0..<500 {
            if condition() { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        XCTFail("Grid did not reach the expected state")
        throw NSError(domain: "SelectionTestTimeout", code: 1)
    }

    private actor LoadGate {
        private var isFinished = false
        private var continuation: CheckedContinuation<Void, Never>?

        func wait() async {
            guard !isFinished else { return }
            await withCheckedContinuation { continuation = $0 }
        }

        func finish() {
            isFinished = true
            continuation?.resume()
            continuation = nil
        }
    }

    private final class SelectionSource: MediaBrowserSource, @unchecked Sendable {
        let mode = MediaBrowserMode.local
        private let lock = NSLock()
        private var sections: [MediaBrowserSection]
        private var requests: [Bool] = []
        private var nextLoadGate: LoadGate?
        private var thumbnailImage: UIImage?
        private var thumbnailRequests = 0

        var loadRequests: [Bool] { lock.withLock { requests } }
        var thumbnailRequestCount: Int { lock.withLock { thumbnailRequests } }

        init(items: [MediaBrowserItem]) {
            sections = [.init(month: .init(year: 2026, month: 1), items: items)]
        }
        init(sections: [MediaBrowserSection]) { self.sections = sections }
        func replace(items: [MediaBrowserItem]) {
            lock.withLock { sections = [.init(month: .init(year: 2026, month: 1), items: items)] }
        }
        func holdNextLoad(_ gate: LoadGate) { lock.withLock { nextLoadGate = gate } }
        func setThumbnail(_ image: UIImage) { lock.withLock { thumbnailImage = image } }
        func load() async -> MediaBrowserLoadResult {
            await load(includeFileSizes: false)
        }
        func load(includeFileSizes: Bool) async -> MediaBrowserLoadResult {
            let (content, gate): (MediaBrowserContent, LoadGate?) = lock.withLock {
                requests.append(includeFileSizes)
                let gate = nextLoadGate
                nextLoadGate = nil
                return (.init(sections: sections), gate)
            }
            await gate?.wait()
            return .loaded(content)
        }
        func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
            lock.withLock {
                thumbnailRequests += 1
                return thumbnailImage
            }
        }
        func photoImage(for item: MediaBrowserItem) async -> UIImage? { lock.withLock { thumbnailImage } }
        func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? { nil }
        func video(for item: MediaBrowserItem) async -> MaterializedVideo? { nil }
    }
}
