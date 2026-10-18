import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserTestFixture<Source: MediaBrowserSource> {
    let source: Source
    let dependencies: DependencyContainer
    let presence: LibraryPresenceIndex
    let runner: MediaBrowserActionRunner
    let controller: MediaBrowserGridViewController
    let window: UIWindow
    let collection: UICollectionView
    let dataSource: UICollectionViewDiffableDataSource<MediaBrowserSectionID, MediaBrowserItemID>
    private let directory: URL
    private let previousWindow: UIWindow?

    var itemIDs: [MediaBrowserItemID] { dataSource.snapshot().itemIdentifiers }

    init(source: Source, remoteSource: MediaBrowserSource? = nil) throws {
        self.source = source
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        dependencies = DependencyContainer(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")),
            reconcileOneDriveAccounts: false)
        presence = LibraryPresenceIndex(hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: dependencies.backupCoordinator, profileKey: { nil })
        runner = MediaBrowserActionRunner(env: .init(appSession: dependencies.appSession,
            backupCoordinator: dependencies.backupCoordinator, restoreService: dependencies.restoreService,
            photoLibraryService: dependencies.photoLibraryService, hashIndexRepository: dependencies.hashIndexRepository,
            localHashIndexBuildService: dependencies.localHashIndexBuildService, allAccessibleAssetIDs: { [] },
            presenceIndex: presence, appRuntimeFlags: dependencies.appRuntimeFlags,
            isTaskActive: { [runtime = dependencies.appRuntimeFlags] in runtime.isExecuting },
            iCloudPhotoBackupMode: { .disable }, monthGroupingTimeZone: { .frozenCurrent() }))
        var specs = [MediaBrowserGridViewController.ModeSpec(mode: source.mode, isAvailable: { true }, makeSource: { source })]
        if let remoteSource {
            specs.append(.init(mode: .remote, isAvailable: { true }, makeSource: { remoteSource }))
        }
        controller = MediaBrowserGridViewController(specs: specs, initialMode: source.mode,
            initialMonth: nil, remoteStorageImage: { nil }, sessionToken: { nil },
            actionRunner: runner, presenceIndex: presence, title: "Browser")
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        previousWindow = scene.keyWindow
        window = UIWindow(windowScene: scene)
        window.rootViewController = UINavigationController(rootViewController: controller)
        window.makeKeyAndVisible()
        controller.loadViewIfNeeded()
        collection = try XCTUnwrap(controller.view.subviews.compactMap { $0 as? UICollectionView }.first)
        dataSource = try XCTUnwrap(collection.dataSource as? UICollectionViewDiffableDataSource<MediaBrowserSectionID, MediaBrowserItemID>)
    }

    func postPresenceBurst() {
        for _ in 0..<20 {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: presence)
        }
    }

    func close() {
        window.isHidden = true
        window.rootViewController = nil
        previousWindow?.makeKey()
        try? dependencies.databaseManager.dbQueue.close()
        try? FileManager.default.removeItem(at: directory)
    }
}
