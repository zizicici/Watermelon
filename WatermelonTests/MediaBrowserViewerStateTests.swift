import Photos
import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserViewerStateTests: XCTestCase {
    func testOpenViewerUsesUnfilteredGridContentAndClearsRemovedMedia() async throws {
        let month = LibraryMonthKey(year: 2026, month: 1)
        let remote = RemoteMediaReference(fingerprint: Data([1]), photoRelativePath: "2026/01/photo.jpg",
            videoRelativePath: nil, photoContentHash: nil, videoContentHash: nil, storageMonth: month, isIncomplete: false)
        let original = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: nil, remote: remote)
        let downloaded = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "downloaded", remote: remote)
        let source = Source(sections: [.init(month: month, items: [original])])
        let fixture = try MediaBrowserTestFixture(source: source)
        defer { fixture.close() }
        let grid = fixture.controller
        let presence = fixture.presence
        grid.setFilter(.remoteOnly)
        let collection = fixture.collection
        try await waitUntil {
            grid.view.layoutIfNeeded()
            return collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == 1
        }
        grid.collectionView(collection, didSelectItemAt: IndexPath(item: 0, section: 0))
        let viewer = try XCTUnwrap(grid.presentedViewController as? MediaBrowserViewerViewController)
        try await waitUntil { viewer.view.window != nil && viewer.heroDestination() != nil }
        let pager = try XCTUnwrap(viewer.view.subviews.compactMap { $0 as? UICollectionView }.first)
        let bar = try XCTUnwrap(descendants(of: viewer.view).compactMap { $0 as? MediaActionBar }.first)
        XCTAssertEqual(viewer.heroCurrentItemID, original.id)
        XCTAssertFalse(hasDeleteButton(in: bar))

        let loadsBeforeBackgrounding = source.loadCount
        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        var isBackgrounded = true
        defer {
            if isBackgrounded { NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil) }
        }
        XCTAssertFalse(viewer.isDisplayed)
        source.replace(sections: [.init(month: month, items: [downloaded])])
        for _ in 0..<20 {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        }
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(source.loadCount, loadsBeforeBackgrounding)
        XCTAssertEqual(collection.numberOfSections, 1)
        XCTAssertFalse(hasDeleteButton(in: bar))
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        isBackgrounded = false
        XCTAssertTrue(viewer.isDisplayed)
        XCTAssertFalse(bar.isHidden)
        XCTAssertFalse(hasDeleteButton(in: bar))
        try await waitUntil { collection.numberOfSections == 0 && !bar.isHidden && self.hasDeleteButton(in: bar) }
        let deleteButton = try button(titled: String(localized: "mediaBrowser.action.delete"), in: bar)
        XCTAssertTrue(deleteButton.showsMenuAsPrimaryAction)
        XCTAssertEqual((deleteButton.menu?.children.first as? UIMenu)?.children.compactMap { ($0 as? UIAction)?.title }, [MediaBrowserActionKind.deleteLocal.title])
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(source.loadCount, loadsBeforeBackgrounding + 1)
        XCTAssertEqual(viewer.heroCurrentItemID, original.id)
        XCTAssertEqual(pager.numberOfItems(inSection: 0), 1)

        source.replace(sections: [])
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try await waitUntil { viewer.heroCurrentItemID == nil }
        XCTAssertTrue(bar.isHidden)
        XCTAssertNil(viewer.heroDestination())
        XCTAssertEqual(pager.numberOfItems(inSection: 0), 1)
        XCTAssertTrue(descendants(of: viewer.view).compactMap { $0 as? UILabel }.contains {
            !$0.isHidden && $0.text == String(localized: "mediaBrowser.viewer.unavailable")
        })
        viewer.dismiss(animated: false)
        try await waitUntil { grid.presentedViewController == nil }
    }

    func testReadOnlyActionsRemainUsableDuringExecutionAndContinuousInvalidation() async throws {
        let month = LibraryMonthKey(year: 2026, month: 1)
        let item = MediaBrowserItem(kind: .photo, creationDateMs: 1, localIdentifier: "local", fingerprint: nil, isBackedUp: false)
        let source = Source(sections: [.init(month: month, items: [item])])
        let fixture = try MediaBrowserTestFixture(source: source)
        defer { fixture.close() }
        let grid = fixture.controller
        let presence = fixture.presence
        let runner = fixture.runner
        let collection = fixture.collection
        try await waitUntil {
            grid.view.layoutIfNeeded()
            return collection.numberOfSections == 1 && collection.numberOfItems(inSection: 0) == 1
        }
        grid.collectionView(collection, didSelectItemAt: IndexPath(item: 0, section: 0))
        let viewer = try XCTUnwrap(grid.presentedViewController as? MediaBrowserViewerViewController)
        try await waitUntil { viewer.isDisplayed && viewer.heroDestination() != nil }
        let bar = try XCTUnwrap(descendants(of: viewer.view).compactMap { $0 as? MediaActionBar }.first)
        let oldDeleteButton = try button(titled: String(localized: "mediaBrowser.action.delete"), in: bar)
        XCTAssertTrue(oldDeleteButton.showsMenuAsPrimaryAction)
        XCTAssertEqual((oldDeleteButton.menu?.children.first as? UIMenu)?.children.compactMap { ($0 as? UIAction)?.title }, [MediaBrowserActionKind.deleteLocal.title])
        let initialLoads = source.loadCount
        let runtime = AppRuntimeFlags()
        var claim: AppRuntimeFlags.ExecutionClaim? = try XCTUnwrap(runtime.tryEnterExecution())
        defer { if let claim { runtime.exitExecution(claim) } }
        var updated = item
        updated.fileSize = 321
        source.replace(sections: [.init(month: month, items: [updated])])
        let readOnlyTitles: Set<String> = [String(localized: "mediaBrowser.info.title"), String(localized: "mediaBrowser.action.share")]
        for _ in 0..<6 {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
            try await Task.sleep(for: .milliseconds(50))
            XCTAssertFalse(bar.isHidden)
            XCTAssertEqual(actionTitles(in: bar), readOnlyTitles)
            XCTAssertEqual(source.loadCount, initialLoads)
        }
        try await waitUntil { source.loadCount == initialLoads + 1 }
        XCTAssertTrue(runtime.isExecuting)
        XCTAssertEqual(actionTitles(in: bar), readOnlyTitles)
        XCTAssertFalse(runner.canRun(.deleteLocal))
        oldDeleteButton.sendActions(for: .touchUpInside)
        XCTAssertNil(viewer.presentedViewController)
        XCTAssertFalse(runner.isActionRunning)

        try button(titled: String(localized: "mediaBrowser.info.title"), in: bar).sendActions(for: .touchUpInside)
        try await waitUntil {
            source.metadataRequests == [item.id] && viewer.presentedViewController is UINavigationController
                && viewer.presentedViewController?.isBeingPresented == false
        }
        let metadata = try XCTUnwrap(viewer.presentedViewController)
        await withCheckedContinuation { continuation in
            metadata.dismiss(animated: false) { continuation.resume() }
        }
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        try button(titled: String(localized: "mediaBrowser.action.share"), in: bar).sendActions(for: .touchUpInside)
        try await waitUntil {
            source.shareRequests == [item.id] && viewer.presentedViewController is UIActivityViewController
                && viewer.presentedViewController?.isBeingPresented == false
        }
        let shareSheet = try XCTUnwrap(viewer.presentedViewController)
        await withCheckedContinuation { continuation in
            shareSheet.dismiss(animated: false) { continuation.resume() }
        }
        XCTAssertGreaterThanOrEqual(source.loadCount, initialLoads + 1)
        XCTAssertEqual(actionTitles(in: bar), readOnlyTitles)

        runtime.exitExecution(try XCTUnwrap(claim))
        claim = nil
        try await waitUntil { self.hasDeleteButton(in: bar) }
        let finalLoads = source.loadCount
        try await Task.sleep(for: .milliseconds(350))
        XCTAssertEqual(source.loadCount, finalLoads)
        XCTAssertEqual(viewer.heroCurrentItemID, item.id)
        viewer.dismiss(animated: false)
    }

    private func button(titled title: String, in view: UIView) throws -> UIButton {
        try XCTUnwrap(descendants(of: view).compactMap { $0 as? UIButton }.first { $0.configuration?.title == title })
    }

    private func actionTitles(in view: UIView) -> Set<String> {
        Set(descendants(of: view).compactMap { ($0 as? UIButton)?.configuration?.title })
    }

    private func hasDeleteButton(in view: UIView) -> Bool {
        descendants(of: view).compactMap { $0 as? UIButton }.contains {
            $0.configuration?.title == String(localized: "mediaBrowser.action.delete")
        }
    }

    private func descendants(of view: UIView) -> [UIView] {
        view.subviews.flatMap { [$0] + descendants(of: $0) }
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(5)
        while !condition(), Date() < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        XCTAssertTrue(condition())
    }

    private final class Source: MediaBrowserSource, @unchecked Sendable {
        let mode: MediaBrowserMode = .merged
        private let lock = NSLock()
        private var sections: [MediaBrowserSection]
        private var requests = 0
        private var sharedIDs: [MediaBrowserItemID] = []
        private var metadataIDs: [MediaBrowserItemID] = []

        var loadCount: Int { lock.withLock { requests } }
        var shareRequests: [MediaBrowserItemID] { lock.withLock { sharedIDs } }
        var metadataRequests: [MediaBrowserItemID] { lock.withLock { metadataIDs } }

        init(sections: [MediaBrowserSection]) { self.sections = sections }
        func replace(sections: [MediaBrowserSection]) { lock.withLock { self.sections = sections } }
        func load() async -> MediaBrowserLoadResult {
            .loaded(lock.withLock {
                requests += 1
                return MediaBrowserContent(sections: sections)
            })
        }
        func thumbnail(for item: MediaBrowserItem) async -> UIImage? { await photoImage(for: item) }
        func photoImage(for item: MediaBrowserItem) async -> UIImage? {
            await MainActor.run {
                UIGraphicsImageRenderer(size: CGSize(width: 20, height: 20)).image { context in
                    UIColor.red.setFill()
                    context.fill(CGRect(x: 0, y: 0, width: 20, height: 20))
                }
            }
        }
        func video(for item: MediaBrowserItem) async -> MaterializedVideo? { nil }
        func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? { nil }
        func shareItems(for item: MediaBrowserItem) async -> [Any] {
            lock.withLock { sharedIDs.append(item.id) }
            guard let image = await photoImage(for: item) else { return [] }
            return [image]
        }
        func metadata(for item: MediaBrowserItem) async -> MediaMetadataDocument? {
            lock.withLock { metadataIDs.append(item.id) }
            return nil
        }
    }
}
