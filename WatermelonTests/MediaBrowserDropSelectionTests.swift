import Photos
import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserDropSelectionTests: XCTestCase {
    func testSectionButtonIsDisabledWhileDropTaskRunsAndRestoredAfterwards() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.show()
        let button = try XCTUnwrap(fixture.collection.visibleSupplementaryViews(ofKind: "month-header")
            .flatMap { $0.subviews.compactMap { $0 as? UIButton } }
            .first { $0.accessibilityIdentifier == "mediaBrowser.sectionSelection" })
        XCTAssertFalse(button.isHidden)
        button.sendActions(for: .touchUpInside)
        XCTAssertTrue(button.isSelected)
        XCTAssertTrue(fixture.controller.runExternalSelectionAction { _ in
            try? await Task.sleep(for: .milliseconds(100))
            return false
        })
        XCTAssertFalse(button.isEnabled)
        button.sendActions(for: .touchUpInside)
        XCTAssertTrue(button.isSelected)
        try await waitUntil { button.isEnabled }
        button.sendActions(for: .touchUpInside)
        XCTAssertFalse(button.isSelected)
    }

    func testGestureEndAndCancellationRestoreSendAndLibraryMenu() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.show()
        try fixture.toggle(0)
        try fixture.toggle(1)
        try await Task.sleep(for: .milliseconds(300))
        fixture.assertAvailability(send: true, scope: true)

        fixture.beginNative(at: 0)
        fixture.assertAvailability(send: false, scope: false)
        fixture.endNative()
        fixture.assertAvailability(send: true, scope: true)

        fixture.beginNative(at: 0)
        fixture.deselectNative(at: 0)
        try await Task.sleep(for: .milliseconds(300))
        fixture.assertAvailability(send: false, scope: false)
        fixture.endNative()
        fixture.assertAvailability(send: true, scope: true)

        for ending: UIGestureRecognizer.State in [.ended, .cancelled, .failed] {
            try fixture.toggle(0)
            let drag = try fixture.beginDrag(at: 0)
            try await Task.sleep(for: .milliseconds(300))
            fixture.assertAvailability(send: false, scope: false)
            try fixture.finishDrag(drag, at: 0, state: ending)
            fixture.assertAvailability(send: true, scope: true)
        }
    }

    func testDeselectingLastPhotoLeavesFileSendableAfterBothGestures() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.show()
        let file = fixture.directory.appendingPathComponent("selected.txt")
        try Data("selection-test".utf8).write(to: file)
        fixture.controller.documentPicker(UIDocumentPickerViewController(forOpeningContentTypes: [.data]),
            didPickDocumentsAt: [file])
        try await waitUntil { fixture.sendButton.isUserInteractionEnabled && fixture.scopeButton.isEnabled }

        try fixture.toggle(0)
        fixture.beginNative(at: 0)
        fixture.deselectNative(at: 0)
        fixture.assertAvailability(send: false, scope: false)
        fixture.endNative()
        XCTAssertTrue(fixture.collection.indexPathsForSelectedItems?.isEmpty ?? true)
        fixture.assertAvailability(send: true, scope: true)

        try fixture.toggle(0)
        let drag = try fixture.beginDrag(at: 0)
        fixture.assertAvailability(send: false, scope: false)
        try fixture.finishDrag(drag, at: 0, state: .ended)
        XCTAssertTrue(fixture.collection.indexPathsForSelectedItems?.isEmpty ?? true)
        fixture.assertAvailability(send: true, scope: true)
    }

    func testGestureCompletionStillHonorsEmptySelectionDestinationAndQuota() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.show()
        try fixture.toggle(0)
        fixture.gates.destinationAvailable = false
        fixture.beginNative(at: 0)
        fixture.endNative()
        fixture.assertAvailability(send: false, scope: true)

        fixture.gates.destinationAvailable = true
        fixture.gates.isPro = false
        for index in 1...3 { try fixture.toggle(index) }
        fixture.beginNative(at: 0)
        fixture.endNative()
        fixture.assertAvailability(send: false, scope: true)
        fixture.beginNative(at: 3)
        fixture.deselectNative(at: 3)
        fixture.endNative()
        fixture.assertAvailability(send: true, scope: true)

        fixture.beginNative(at: 0)
        for index in 0...2 { fixture.deselectNative(at: index) }
        fixture.endNative()
        fixture.assertAvailability(send: false, scope: true)
    }

    func testDeactivationEndsNativeInteractionBeforeReturningToDrop() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.show()
        try fixture.toggle(0)
        fixture.beginNative(at: 0)
        fixture.assertAvailability(send: false, scope: false)
        fixture.controller.setMediaDropActive(false)
        fixture.controller.setMediaDropActive(true)
        try await waitUntil { fixture.sendButton.isUserInteractionEnabled && fixture.scopeButton.isEnabled }
        XCTAssertTrue(fixture.collection.indexPathsForSelectedItems?.contains(IndexPath(item: 0, section: 0)) ?? false)
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        for _ in 0..<500 {
            if condition() { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        XCTFail("Drop did not reach the expected state")
        throw NSError(domain: "DropSelectionTimeout", code: 1)
    }

    @MainActor
    private final class Gates {
        var isPro = true
        var destinationAvailable = true
    }

    @MainActor
    private final class Fixture {
        let gates = Gates()
        let directory: URL
        let database: DatabaseManager
        let library: MediaDropLocalLibraryController
        let controller: MediaBrowserGridViewController
        let collection: UICollectionView
        private let window: UIWindow
        private let previousKeyWindow: UIWindow?

        init() throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
            let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
            let presence = LibraryPresenceIndex(hashIndexRepository: dependencies.hashIndexRepository,
                coordinator: dependencies.backupCoordinator, profileKey: { nil })
            let runner = MediaBrowserActionRunner(env: .init(appSession: dependencies.appSession,
                backupCoordinator: dependencies.backupCoordinator, restoreService: dependencies.restoreService,
                photoLibraryService: dependencies.photoLibraryService, hashIndexRepository: dependencies.hashIndexRepository,
                localHashIndexBuildService: dependencies.localHashIndexBuildService,
                localIndexChangePublisher: dependencies.localIndexChangePublisher, allAccessibleAssetIDs: { [] },
                presenceIndex: presence, appRuntimeFlags: dependencies.appRuntimeFlags, isTaskActive: { false },
                iCloudPhotoBackupMode: { .disable }, monthGroupingTimeZone: { .frozenCurrent() }))
            library = MediaDropLocalLibraryController(photoLibraryService: dependencies.photoLibraryService,
                initialMediaFilter: .all, makeAlbumBrowser: { _ in nil })
            let source = SelectionSource()
            let gates = gates
            controller = MediaBrowserGridViewController(
                specs: [.init(mode: .local, isAvailable: { true }, makeSource: { source })],
                initialMode: .local, initialMonth: nil, remoteStorageImage: { nil }, sessionToken: { nil },
                actionRunner: runner, presenceIndex: presence, title: "Drop selection test", selectionAction: .init(
                    buttonTitle: "TestSend", symbolName: "paperplane", initialOptions: .defaultOption,
                    accessPolicy: { .init(isPro: gates.isPro) }, canChooseDestination: { gates.destinationAvailable },
                    makeMenu: { _ in UIMenu(children: []) }, perform: { _, _, _, _, _ in false }),
                transferLocalLibrary: library)
            let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
            previousKeyWindow = scene.keyWindow
            window = UIWindow(windowScene: scene)
            window.rootViewController = UINavigationController(rootViewController: controller)
            window.makeKeyAndVisible()
            controller.loadViewIfNeeded()
            controller.setMediaDropActive(true)
            collection = try XCTUnwrap(controller.view.subviews.compactMap { $0 as? UICollectionView }.first)
        }

        var sendButton: UIButton { buttons(in: controller.view).first { $0.accessibilityLabel == "TestSend" }! }
        var scopeButton: UIButton { buttons(in: controller.view).first { $0.accessibilityLabel == library.title }! }

        func show() async throws {
            for _ in 0..<500 {
                controller.view.layoutIfNeeded()
                if collection.numberOfSections == 1, collection.numberOfItems(inSection: 0) == 12,
                   collection.cellForItem(at: IndexPath(item: 0, section: 0)) != nil { return }
                try await Task.sleep(for: .milliseconds(10))
            }
            throw NSError(domain: "DropSelectionLoadTimeout", code: 1)
        }

        func close() {
            controller.setMediaDropActive(false)
            window.isHidden = true
            window.rootViewController = nil
            previousKeyWindow?.makeKey()
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        func toggle(_ index: Int) throws {
            let cell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: index, section: 0)))
            try XCTUnwrap(buttons(in: cell).first).sendActions(for: .touchUpInside)
        }

        func beginNative(at index: Int) {
            controller.collectionView(collection, didBeginMultipleSelectionInteractionAt: IndexPath(item: index, section: 0))
        }

        func deselectNative(at index: Int) {
            let path = IndexPath(item: index, section: 0)
            collection.deselectItem(at: path, animated: false)
            controller.collectionView(collection, didDeselectItemAt: path)
        }

        func endNative() { controller.collectionViewDidEndMultipleSelectionInteraction(collection) }

        func beginDrag(at index: Int) throws -> SelectionDragGesture {
            let path = IndexPath(item: index, section: 0)
            let cell = try XCTUnwrap(collection.cellForItem(at: path))
            let gesture = SelectionDragGesture()
            gesture.testLocation = try XCTUnwrap(collection.layoutAttributesForItem(at: path)).center
            gesture.testState = .began
            cell.perform(NSSelectorFromString("selectionButtonDragged:"), with: gesture)
            return gesture
        }

        func finishDrag(_ gesture: SelectionDragGesture, at index: Int, state: UIGestureRecognizer.State) throws {
            gesture.testState = state
            let cell = try XCTUnwrap(collection.cellForItem(at: IndexPath(item: index, section: 0)))
            cell.perform(NSSelectorFromString("selectionButtonDragged:"), with: gesture)
        }

        func assertAvailability(send: Bool, scope: Bool, file: StaticString = #filePath, line: UInt = #line) {
            XCTAssertEqual(sendButton.isUserInteractionEnabled, send, file: file, line: line)
            XCTAssertEqual(scopeButton.isEnabled, scope, file: file, line: line)
            XCTAssertEqual(library.canChangeScope(), scope, file: file, line: line)
        }

        private func buttons(in view: UIView) -> [UIButton] {
            (view as? UIButton).map { [$0] } ?? view.subviews.flatMap { buttons(in: $0) }
        }
    }

    private final class SelectionDragGesture: UILongPressGestureRecognizer {
        var testState = UIGestureRecognizer.State.possible
        var testLocation = CGPoint.zero
        override var state: UIGestureRecognizer.State { get { testState } set { testState = newValue } }
        override func location(in view: UIView?) -> CGPoint { testLocation }
    }

    private final class SelectionSource: MediaBrowserSource, Sendable {
        let mode = MediaBrowserMode.local
        func load() async -> MediaBrowserLoadResult {
            .loaded(.init(sections: [.init(month: .init(year: 2026, month: 1), items: (0..<12).map {
                MediaBrowserItem(kind: .photo, creationDateMs: Int64($0),
                    localIdentifier: "drop-test-\($0)", fingerprint: nil, isBackedUp: false)
            })]))
        }
        func thumbnail(for item: MediaBrowserItem) async -> UIImage? { nil }
        func photoImage(for item: MediaBrowserItem) async -> UIImage? { nil }
        func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? { nil }
        func video(for item: MediaBrowserItem) async -> MaterializedVideo? { nil }
    }
}
