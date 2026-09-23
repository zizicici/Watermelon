import MoreKit
import Photos
import UIKit
import XCTest
@testable import Watermelon

final class LocalLibraryMediaFilterTests: XCTestCase {
    func testPhotoKitPredicatesSelectOnlyRequestedMediaType() {
        let mediaTypes: [PHAssetMediaType] = [.image, .video, .audio, .unknown]
        for type in mediaTypes {
            let asset = ["mediaType": NSNumber(value: type.rawValue)]
            XCTAssertEqual(PhotoLibraryMediaFilter.photos.predicate?.evaluate(with: asset), type == .image)
            XCTAssertEqual(PhotoLibraryMediaFilter.videos.predicate?.evaluate(with: asset), type == .video)
        }
        XCTAssertNil(PhotoLibraryMediaFilter.all.predicate)
    }

    func testPhotosIncludeLivePhotosAndVideosExcludeThem() {
        XCTAssertTrue(PhotoLibraryMediaFilter.photos.includes(.photo))
        XCTAssertTrue(PhotoLibraryMediaFilter.photos.includes(.livePhoto))
        XCTAssertFalse(PhotoLibraryMediaFilter.photos.includes(.video))
        XCTAssertFalse(PhotoLibraryMediaFilter.videos.includes(.photo))
        XCTAssertFalse(PhotoLibraryMediaFilter.videos.includes(.livePhoto))
        XCTAssertTrue(PhotoLibraryMediaFilter.videos.includes(.video))
        for kind: AlbumMediaKind in [.photo, .livePhoto, .video] {
            XCTAssertTrue(PhotoLibraryMediaFilter.all.includes(kind))
        }
    }

    func testFilteredScopesCannotRepresentTheEntireLibrary() {
        XCTAssertTrue(HomeLocalLibraryScope.device(.all).isEntireLibrary)
        for filter in [PhotoLibraryMediaFilter.photos, .videos] {
            let scope = HomeLocalLibraryScope.device(filter)
            XCTAssertFalse(scope.isEntireLibrary)
            XCTAssertEqual(scope.photoLibraryQuery, .library(filter))
            XCTAssertTrue(scope.selectedAlbumIdentifiers.isEmpty)
        }
        let albumScope = HomeLocalLibraryScope.albums(["album-a"])
        XCTAssertFalse(albumScope.isEntireLibrary)
        XCTAssertNil(albumScope.deviceMediaFilter)
        XCTAssertEqual(albumScope.photoLibraryQuery, .albums(["album-a"]))
    }

    @MainActor
    func testDeviceMenuChecksOnlyTheActiveMediaFilter() {
        let filters: [PhotoLibraryMediaFilter] = [.all, .photos, .videos]
        XCTAssertEqual(PhotoLibraryMediaFilter.allCases, filters)
        XCTAssertEqual(DefaultDeviceMediaScopeSetting.getOptions().map(\.mediaFilter), filters)
        for isPad in [false, true] {
            for (index, filter) in filters.enumerated() {
                let menu = HomeLocalLibraryMenu.deviceMenu(
                    scope: .device(filter), isPad: isPad, attributes: [], onSelect: { _ in }
                )
                let actions = menu.children.compactMap { $0 as? UIAction }
                XCTAssertEqual(menu.title, isPad ? "iPad" : "iPhone")
                XCTAssertEqual(actions.count, 3)
                XCTAssertEqual(actions.map(\.title), filters.map(\.localizedTitle))
                XCTAssertEqual(actions.indices.filter { actions[$0].state == .on }, [index])
            }
        }
    }

    @MainActor
    func testAlbumScopeDoesNotCheckAnyDeviceFilterAndBusyMenuIsDisabled() {
        let menu = HomeLocalLibraryMenu.deviceMenu(
            scope: .albums(["album-a"]), isPad: false, attributes: .disabled, onSelect: { _ in }
        )
        let actions = menu.children.compactMap { $0 as? UIAction }
        XCTAssertEqual(actions.count, 3)
        XCTAssertTrue(actions.allSatisfy { $0.state == .off && $0.attributes.contains(.disabled) })
    }

    @MainActor
    func testSavedDefaultAppliesLiveToBackupButNotToAnOpenDropSession() throws {
        let defaults = DefaultDeviceMediaScopeSetting.userDefaults
        let key = DefaultDeviceMediaScopeSetting.getKey()
        let previousValue = defaults.object(forKey: key)
        let previousSource = defaults.object(forKey: LocalDataSourceStore.storageKey)
        defaults.removeObject(forKey: LocalDataSourceStore.storageKey)
        defer {
            if let previousSource {
                defaults.set(previousSource, forKey: LocalDataSourceStore.storageKey)
            } else {
                defaults.removeObject(forKey: LocalDataSourceStore.storageKey)
            }
            if let previousValue {
                defaults.set(previousValue, forKey: key)
            } else {
                defaults.removeObject(forKey: key)
            }
        }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("scope.sqlite"))
        defer { try? database.dbQueue.close() }
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)

        try DefaultDeviceMediaScopeSetting.setCurrent(.photos)
        let backup = HomeScreenStore(dependencies: dependencies)
        let drop = MediaDropLocalLibraryController(
            photoLibraryService: dependencies.photoLibraryService, makeAlbumBrowser: { _ in nil }
        )
        XCTAssertEqual(backup.localLibraryScope, .device(.photos))
        XCTAssertEqual(drop.scope, .device(.photos))

        try DefaultDeviceMediaScopeSetting.setCurrent(.videos)
        // Backup follows a changed default live; Drop only picks it up when its browser reapplies it.
        XCTAssertEqual(backup.localLibraryScope, .device(.videos))
        XCTAssertEqual(drop.scope, .device(.photos))
        drop.applyDefaultSourceIfNeeded()
        XCTAssertEqual(drop.scope, .device(.videos))
        let nextBackup = HomeScreenStore(dependencies: dependencies)
        let nextDrop = MediaDropLocalLibraryController(
            photoLibraryService: dependencies.photoLibraryService, makeAlbumBrowser: { _ in nil }
        )
        XCTAssertEqual(nextBackup.localLibraryScope, .device(.videos))
        XCTAssertEqual(nextDrop.scope, .device(.videos))

        nextDrop.setScope(.device(.all))
        XCTAssertEqual(DefaultDeviceMediaScopeSetting.getValue(), .videos)
        XCTAssertEqual(nextBackup.localLibraryScope, .device(.videos))
    }
}
