import MoreKit
import Photos
import XCTest
@testable import Watermelon

final class LocalDataSourceTests: XCTestCase {
    private var suiteName: String!
    private var defaults: UserDefaults!
    private var store: LocalDataSourceStore!
    private let travel = LocalAlbumReference(id: "travel", name: "Travel")
    private let favorites = LocalAlbumReference(id: "favorites", name: "Favorites")

    override func setUp() {
        super.setUp()
        suiteName = "LocalDataSourceTests.\(UUID().uuidString)"
        defaults = UserDefaults(suiteName: suiteName)
        store = LocalDataSourceStore(defaults: defaults)
    }

    override func tearDown() {
        defaults.removePersistentDomain(forName: suiteName)
        store = nil
        defaults = nil
        super.tearDown()
    }

    func testLegacyDefaultsMigrateWithoutChangingSelectedMedia() throws {
        let key = DefaultDeviceMediaScopeSetting.getKey()
        XCTAssertEqual(store.defaultSource.scope, .device(.all))
        for (value, kind) in [(0, LocalDataSource.Kind.all), (1, .photos), (2, .videos)] {
            defaults.set(value, forKey: key)
            XCTAssertEqual(store.defaultSource.kind, kind)
        }
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel]))
        defaults.set(0, forKey: key)
        XCTAssertEqual(store.defaultSource.scope, .albums([travel.id]))
    }

    func testAlbumSelectionPersistsTogetherAndDeduplicatesIdentifiers() throws {
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel, favorites, travel]))
        let reloaded = LocalDataSourceStore(defaults: defaults!)
        XCTAssertEqual(reloaded.defaultSource.albums, [travel, favorites])
        XCTAssertEqual(reloaded.defaultSource.scope, .albums([travel.id, favorites.id]))
    }

    func testChoosingDeviceSourcePreservesSavedAlbums() throws {
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel, favorites]))
        try store.setDefault(LocalDataSource(kind: .videos))
        XCTAssertEqual(store.defaultSource.scope, .device(.videos))
        XCTAssertTrue(store.defaultSource.albums.isEmpty)
        XCTAssertEqual(store.source(for: .albums).albums, [travel, favorites])
    }

    func testRepairReplacesWholeSelectionWithoutChangingDefaultKind() throws {
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel, favorites]))
        let originalID = store.defaultSource.id
        try store.setDefault(LocalDataSource(kind: .photos))
        try store.replaceAlbumSelection([favorites, favorites])
        XCTAssertEqual(store.defaultSource.kind, .photos)
        XCTAssertEqual(store.source(for: .albums).id, originalID)
        XCTAssertEqual(store.albumReferences, [favorites])
    }

    func testExplicitlyClearingSavedAlbumsResetsDefaultToAllMedia() throws {
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel]))
        try store.setDefault(LocalDataSource(kind: .albums))
        let reloaded = LocalDataSourceStore(defaults: defaults!)
        XCTAssertEqual(reloaded.defaultSource.scope, .device(.all))
        XCTAssertTrue(reloaded.albumReferences.isEmpty)
        XCTAssertEqual(reloaded.source(for: .albums).subtitle, String(localized: "common.none"))
    }

    func testClearingAlbumReferencesResetsOnlyAnActiveAlbumDefault() throws {
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel]))
        try store.replaceAlbumSelection([])
        XCTAssertEqual(store.defaultSource.scope, .device(.all))
        XCTAssertTrue(store.albumReferences.isEmpty)

        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel]))
        try store.setDefault(LocalDataSource(kind: .photos))
        try store.replaceAlbumSelection([])
        XCTAssertEqual(store.defaultSource.scope, .device(.photos))
        XCTAssertTrue(store.albumReferences.isEmpty)
    }

    func testCorruptPersistedDataNeverExpandsToWholeLibrary() {
        for invalid: Any in [Data("invalid".utf8), "invalid", Data("{\"kind\":\"unknown\",\"albums\":[]}".utf8)] {
            defaults.set(invalid, forKey: LocalDataSourceStore.storageKey)
            XCTAssertEqual(store.defaultSource.scope, .albums([]))
            XCTAssertFalse(store.defaultSource.scope.isEntireLibrary)
        }
    }

    func testMissingAlbumsThrowWithAllMissingNamesAndNoPartialResult() {
        XCTAssertThrowsError(try LocalDataSourceError.validateAlbums(
            ["a", "b", "c"], authorization: .authorized, existing: { ["a"] },
            names: ["a": "Available", "b": "Travel", "c": "Favorites"]
        )) { error in
            XCTAssertEqual(error as? LocalDataSourceError, .unavailableAlbums(["Travel", "Favorites"]))
            XCTAssertTrue(error.localizedDescription.contains("Travel"))
            XCTAssertTrue(error.localizedDescription.contains("Favorites"))
        }
    }

    func testPermissionFailureDoesNotAttemptAlbumLookup() {
        for status: PHAuthorizationStatus in [.limited, .denied, .restricted, .notDetermined] {
            XCTAssertThrowsError(try LocalDataSourceError.validateAlbums(
                [travel.id], authorization: status,
                existing: { XCTFail("Permission errors must be reported before resolving albums"); return [] }, names: [:]
            )) { error in
                XCTAssertEqual(error as? LocalDataSourceError, .fullPhotoAccessRequired)
            }
        }
    }

    func testExistingEmptyAlbumIsValidButEmptySelectionIsNot() {
        XCTAssertNoThrow(try LocalDataSourceError.validateAlbums(
            [travel.id], authorization: .authorized, existing: { [self.travel.id] }, names: [:]
        ))
        XCTAssertThrowsError(try LocalDataSourceError.validateAlbums(
            [], authorization: .authorized, existing: { [] }, names: [:]
        )) { error in
            XCTAssertEqual(error as? LocalDataSourceError, .emptyAlbums)
        }
    }

    @MainActor
    func testIntentRequiresItsOwnAlbumsAndDeduplicatesByIdentifier() throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        try store.setDefault(LocalDataSource(kind: .albums, albums: [travel]))
        let entity = LocalDataSourceEntity(kind: .albums)
        for selection: [LocalAlbumEntity]? in [nil, []] {
            XCTAssertThrowsError(try entity.resolve(albums: selection)) { error in
                XCTAssertEqual(error as? LocalDataSourceError, .emptyAlbums)
            }
        }
        let albums = [LocalAlbumEntity(travel), LocalAlbumEntity(favorites), LocalAlbumEntity(travel)]
        XCTAssertEqual(try entity.resolve(albums: albums).albums, [travel, favorites])
        for kind: LocalDataSource.Kind in [.all, .photos, .videos] {
            XCTAssertEqual(try LocalDataSourceEntity(kind: kind).resolve(albums: albums), LocalDataSource(kind: kind))
        }
    }

    @MainActor
    func testAppDefaultsInitializeBothModesWhileShortcutsKeepIndependentAlbumCombinations() async throws {
        let sharedDefaults = DefaultDeviceMediaScopeSetting.userDefaults
        let previous = sharedDefaults.object(forKey: LocalDataSourceStore.storageKey)
        defer {
            if let previous { sharedDefaults.set(previous, forKey: LocalDataSourceStore.storageKey) }
            else { sharedDefaults.removeObject(forKey: LocalDataSourceStore.storageKey) }
        }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("scope.sqlite"))
        defer { try? database.dbQueue.close() }
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
        let saved = LocalDataSource(kind: .albums, albums: [travel, favorites])
        try LocalDataSourceStore.shared.setDefault(saved)
        let backup = HomeScreenStore(dependencies: dependencies)
        let drop = MediaDropLocalLibraryController(photoLibraryService: dependencies.photoLibraryService, makeAlbumBrowser: { _ in nil })
        XCTAssertEqual(backup.localLibraryScope, saved.scope)
        XCTAssertEqual(drop.scope, saved.scope)
        if #available(iOS 27.0, *) {
            let entity = await LocalDataSourceQuery().defaultResult()
            XCTAssertEqual(entity?.id, "albums")
            XCTAssertEqual(entity?.title, String(localized: "home.localSource.specificAlbums"))
            XCTAssertThrowsError(try entity?.resolve(albums: nil))
            let firstShortcut = RunBackupIntent()
            firstShortcut.dataSource = LocalDataSourceEntity(kind: .albums)
            firstShortcut.albums = [LocalAlbumEntity(travel), LocalAlbumEntity(favorites)]
            let secondShortcut = RunBackupIntent()
            secondShortcut.dataSource = LocalDataSourceEntity(kind: .albums)
            secondShortcut.albums = [LocalAlbumEntity(favorites)]
            try LocalDataSourceStore.shared.setDefault(LocalDataSource(kind: .photos))
            try LocalDataSourceStore.shared.replaceAlbumSelection([])
            XCTAssertEqual(try firstShortcut.dataSource.resolve(albums: firstShortcut.albums).albums, [travel, favorites])
            XCTAssertEqual(try secondShortcut.dataSource.resolve(albums: secondShortcut.albums).albums, [favorites])
            let suggestions = try await LocalDataSourceQuery().suggestedEntities()
            XCTAssertEqual(suggestions.map(\.id), ["all", "photos", "videos", "albums"])
            try LocalDataSourceStore.shared.setDefault(saved)
        }
        backup.setLocalLibraryScope(.device(.photos))
        drop.setScope(.device(.videos))
        XCTAssertEqual(LocalDataSourceStore.shared.defaultSource, saved)
        XCTAssertEqual(backup.localLibraryScope, .device(.photos))
        XCTAssertEqual(drop.scope, .device(.videos))
    }
}
