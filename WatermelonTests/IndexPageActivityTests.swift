import Photos
import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class IndexPageActivityTests: XCTestCase {
    func testLocalIndexLoadsBeforeShowingAndCommitsAfterNavigationPush() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let source = IndexSource()
        defer { Task { await source.firstLoad.open() } }
        let page = LocalIndexViewController(coordinator: fixture.dependencies.localIndexBuildCoordinator,
            photoLibraryService: fixture.dependencies.photoLibraryService,
            hashIndexRepository: fixture.dependencies.hashIndexRepository, snapshotLoader: { await source.load() })
        page.loadViewIfNeeded()
        try await waitUntil { source.count == 1 }
        let navigation = fixture.show(page)
        navigation.pushViewController(UIViewController(), animated: false)
        await source.firstLoad.open()
        let table = try XCTUnwrap(page.view.subviews.compactMap { $0 as? UITableView }.first)
        try await waitUntil {
            table.layoutIfNeeded()
            return self.indexedText(table)?.contains("1") == true
        }
        XCTAssertFalse(source.oldLoadWasCancelled)
        navigation.popViewController(animated: false)
        try await Task.sleep(for: .milliseconds(100))
        XCTAssertEqual(source.count, 1)
    }

    func testLocalIndexBackgroundCancelsOldLoadAndForegroundCatchesUp() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let source = IndexSource()
        defer { Task { await source.firstLoad.open() } }
        let page = LocalIndexViewController(coordinator: fixture.dependencies.localIndexBuildCoordinator,
            photoLibraryService: fixture.dependencies.photoLibraryService,
            hashIndexRepository: fixture.dependencies.hashIndexRepository, snapshotLoader: { await source.load() })
        _ = fixture.show(page)
        try await waitUntil { source.count == 1 }
        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        for _ in 0 ..< 10 {
            NotificationCenter.default.post(name: .ExecutionLifecycleDidChange, object: nil)
        }
        await Task.yield()
        XCTAssertEqual(source.count, 1)
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        let table = try XCTUnwrap(page.view.subviews.compactMap { $0 as? UITableView }.first)
        try await waitUntil { source.count == 2 && self.indexedText(table)?.contains("99") == true }
        await source.firstLoad.open()
        try await waitUntil { source.oldLoadWasCancelled }
        XCTAssertTrue(indexedText(table)?.contains("99") == true)
    }

    func testDuplicatesLoadBeforeShowingAndForegroundRefreshPreservesKeep() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let source = DuplicateSource()
        let page = DuplicatesViewController(coordinator: fixture.dependencies.localIndexBuildCoordinator,
            hashIndexRepository: fixture.dependencies.hashIndexRepository,
            photoLibraryService: fixture.dependencies.photoLibraryService, snapshotLoader: { source.load() })
        page.loadViewIfNeeded()
        try await waitUntil { source.count == 1 }
        _ = fixture.show(page)
        let table = try XCTUnwrap(page.view.subviews.compactMap { $0 as? UITableView }.first)
        try await waitUntil { source.count == 1 && table.numberOfSections == 1 && table.numberOfRows(inSection: 0) == 3 }
        page.tableView(table, didSelectRowAt: IndexPath(row: 2, section: 0))
        try await waitUntil { self.isKept(table, row: 2) }

        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        try await waitUntil { source.count == 2 && !table.isHidden && self.isKept(table, row: 2) }
        NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        source.removeKeptAsset()
        XCTAssertEqual(source.count, 2)
        NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        try await waitUntil { source.count == 3 && !table.isHidden && self.isKept(table, row: 1) }
        XCTAssertFalse(isKept(table, row: 2))
    }

    func testRebuildConfirmationReleasesAfterDismissal() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let page = LocalIndexViewController(coordinator: fixture.dependencies.localIndexBuildCoordinator,
            photoLibraryService: fixture.dependencies.photoLibraryService,
            hashIndexRepository: fixture.dependencies.hashIndexRepository,
            snapshotLoader: { .init(total: 0, indexed: 0, totalSizeBytes: 0, lastUpdatedAt: nil) })
        _ = fixture.show(page)
        let table = try XCTUnwrap(page.view.subviews.compactMap { $0 as? UITableView }.first)
        try await waitUntil { table.numberOfSections == 3 }
        page.tableView(table, didSelectRowAt: IndexPath(row: 0, section: 2))
        try await waitUntil { page.presentedViewController?.viewIfLoaded?.window != nil }
        weak var confirmation = page.presentedViewController
        XCTAssertEqual(confirmation?.title, String(localized: "home.localIndex.confirmRebuildTitle"))
        await withCheckedContinuation { continuation in
            page.dismiss(animated: false) { continuation.resume() }
        }
        try await waitUntil { confirmation == nil }
    }

    private func indexedText(_ table: UITableView) -> String? {
        guard table.numberOfSections > 0, table.numberOfRows(inSection: 0) > 0 else { return nil }
        let indexPath = IndexPath(row: 0, section: 0)
        let cell = table.cellForRow(at: indexPath) ?? table.dataSource?.tableView(table, cellForRowAt: indexPath)
        return (cell?.contentConfiguration as? UIListContentConfiguration)?.secondaryText
    }

    private func isKept(_ table: UITableView, row: Int) -> Bool {
        table.layoutIfNeeded()
        return table.cellForRow(at: IndexPath(row: row, section: 0))?.contentView.subviews
            .compactMap { $0 as? UILabel }.contains { !$0.isHidden && $0.text == String(localized: "home.duplicates.statusKeep") } == true
    }

    private func waitUntil(_ predicate: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(5)
        while !predicate(), Date() < deadline { try await Task.sleep(for: .milliseconds(10)) }
        XCTAssertTrue(predicate())
    }

    @MainActor
    private final class Fixture {
        let dependencies: DependencyContainer
        let window: UIWindow
        let previousKeyWindow: UIWindow?
        let directory: URL

        init() throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            dependencies = DependencyContainer(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")),
                reconcileOneDriveAccounts: false)
            let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
            previousKeyWindow = scene.keyWindow
            window = UIWindow(windowScene: scene)
        }

        func show(_ controller: UIViewController) -> UINavigationController {
            let navigation = UINavigationController(rootViewController: controller)
            window.rootViewController = navigation
            window.makeKeyAndVisible()
            return navigation
        }

        func close() {
            window.isHidden = true
            window.rootViewController = nil
            previousKeyWindow?.makeKey()
            try? dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }

    private final class IndexSource: @unchecked Sendable {
        let firstLoad = RestoreTestLatch()
        private let lock = NSLock()
        private var requests = 0
        private var cancelled = false
        var count: Int { lock.withLock { requests } }
        var oldLoadWasCancelled: Bool { lock.withLock { cancelled } }

        func load() async -> LocalIndexViewController.ScopeSnapshot? {
            let number = lock.withLock { requests += 1; return requests }
            if number == 1 {
                await firstLoad.wait()
                lock.withLock { cancelled = Task.isCancelled }
            }
            return .init(total: number == 1 ? 1 : 99, indexed: number == 1 ? 1 : 99, totalSizeBytes: 10, lastUpdatedAt: nil)
        }
    }

    private final class DuplicateSource: @unchecked Sendable {
        private let lock = NSLock()
        private var requests = 0
        private var removeKept = false
        var count: Int { lock.withLock { requests } }
        func removeKeptAsset() { lock.withLock { removeKept = true } }

        func load() -> DuplicatesViewController.DuplicatesData? {
            let identifiers = lock.withLock {
                requests += 1
                return removeKept ? ["a", "c"] : ["a", "b"]
            }
            return .init(scopeTotal: 2, scopeIndexed: 2, groups: [(Data([1]), identifiers.map {
                .init(assetLocalIdentifier: $0, creationDate: nil, mediaType: .image)
            })])
        }
    }
}
