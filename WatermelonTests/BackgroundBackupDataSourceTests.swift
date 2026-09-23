import GRDB
import XCTest
@testable import Watermelon

final class BackgroundBackupDataSourceTests: XCTestCase {
    private var directory: URL!
    private var databaseURL: URL!
    private var database: DatabaseManager!
    private let travel = LocalAlbumReference(id: "travel", name: "Travel")
    private let family = LocalAlbumReference(id: "family", name: "Family")

    override func setUpWithError() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        databaseURL = directory.appendingPathComponent("test.sqlite")
        database = try DatabaseManager(databaseURL: databaseURL)
    }

    override func tearDownWithError() throws {
        database = nil
        try FileManager.default.removeItem(at: directory)
    }

    func testMigrationMakesBackupInheritAppDefault() throws {
        let profile = try makeProfile(name: "Existing")
        try database.write { db in
            try db.execute(sql: "ALTER TABLE server_profiles DROP COLUMN backgroundBackupDataSourceJSON")
            try db.execute(sql: "ALTER TABLE server_profiles DROP COLUMN backgroundBackupNotifyOnSuccess")
            try db.execute(sql: "ALTER TABLE server_profiles DROP COLUMN backgroundBackupNotifyOnFailure")
            try db.execute(sql: "DELETE FROM grdb_migrations WHERE identifier = 'v7_background_backup_node_settings'")
        }
        database = nil
        database = try DatabaseManager(databaseURL: databaseURL)

        let migrated = try XCTUnwrap(database.fetchServerProfile(id: XCTUnwrap(profile.id)))
        XCTAssertEqual(migrated.name, "Existing")
        XCTAssertNil(migrated.backgroundBackupDataSourceJSON)
        XCTAssertNil(migrated.backupDataSourceOverride)
        XCTAssertEqual(migrated.defaultBackupDataSource(appDefault: LocalDataSource(kind: .photos)).scope, .device(.photos))
        XCTAssertTrue(migrated.backgroundBackupNotifyOnSuccess)
        XCTAssertTrue(migrated.backgroundBackupNotifyOnFailure)
    }

    func testNodesPersistIndependentSelectionsWithoutChangingAppDefault() throws {
        let suite = "BackgroundBackupDataSourceTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let store = LocalDataSourceStore(defaults: defaults)
        try store.setDefault(LocalDataSource(kind: .videos))
        let first = try makeProfile(name: "First")
        let second = try makeProfile(name: "Second")
        XCTAssertEqual(first.defaultBackupDataSource(appDefault: store.defaultSource).scope, .device(.videos))
        try select(LocalDataSource(kind: .photos), for: first)
        try select(LocalDataSource(kind: .albums, albums: [travel, family, travel]), for: second)
        let reopened = try DatabaseManager(databaseURL: databaseURL)
        let savedFirst = try XCTUnwrap(reopened.fetchServerProfile(id: XCTUnwrap(first.id)))
        let savedSecond = try XCTUnwrap(reopened.fetchServerProfile(id: XCTUnwrap(second.id)))

        XCTAssertEqual(savedFirst.defaultBackupDataSource().scope, .device(.photos))
        XCTAssertEqual(savedSecond.defaultBackupDataSource().albums, [travel, family])
        XCTAssertEqual(savedSecond.defaultBackupDataSource().scope, .albums([travel.id, family.id]))
        XCTAssertEqual(store.defaultSource.scope, .device(.videos))
    }

    func testSwitchingMediaPreservesAlbumsButExplicitlyClearingThemResetsToAll() throws {
        var profile = try makeProfile(name: "NAS")
        profile = try select(LocalDataSource(kind: .albums, albums: [travel, family]), for: profile)
        profile = try select(LocalDataSource(kind: .videos), for: profile)
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .device(.videos))
        XCTAssertEqual(profile.defaultBackupDataSource().albums, [travel, family])

        profile = try select(LocalDataSource(kind: .albums), for: profile)
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .device(.all))
        XCTAssertTrue(profile.defaultBackupDataSource().albums.isEmpty)
    }

    func testInvalidStoredSelectionNeverExpandsToAllMedia() throws {
        var profile = try makeProfile(name: "NAS")
        for invalid in ["invalid", "{\"kind\":\"unknown\",\"albums\":[]}"] {
            profile.backgroundBackupDataSourceJSON = Data(invalid.utf8)
            XCTAssertEqual(profile.defaultBackupDataSource().scope, .albums([]))
        }
        profile.backgroundBackupDataSourceJSON = try JSONEncoder().encode(LocalDataSource(kind: .albums))
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .albums([]))
    }

    func testChangingSourceClearsCooldownButPreservesRemoteRefreshMarker() throws {
        let profile = try makeProfile(name: "NAS")
        let id = try XCTUnwrap(profile.id)
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        try database.setBackgroundBackupLastCompletedAt(date, profileID: id)
        try database.setBackgroundBackupLastRanAt(date, profileID: id)
        let saved = try select(LocalDataSource(kind: .photos), for: profile)
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: id))
        XCTAssertEqual(try database.backgroundBackupLastRanAt(profileID: id), date)

        try database.setBackgroundBackupLastCompletedAt(date, profileID: id)
        try select(LocalDataSource(kind: .photos), for: saved)
        XCTAssertEqual(try database.backgroundBackupLastCompletedAt(profileID: id), date)
    }

    func testAlbumWarningsRefreshPerNodeAndClearAfterChangingSource() throws {
        let service = PhotoLibraryService()
        var first = try makeProfile(name: "First")
        var second = try makeProfile(name: "Second")
        let unaffected = try select(LocalDataSource(kind: .all), for: makeProfile(name: "All Media"))
        first = try select(LocalDataSource(kind: .albums, albums: [travel]), for: first)
        second = try select(LocalDataSource(kind: .albums, albums: [family]), for: second)
        let firstID = try XCTUnwrap(first.id)
        let secondID = try XCTUnwrap(second.id)

        let before = service.nodeDataSourceErrors(for: [first, second, unaffected])
        XCTAssertEqual(Set(before.keys), [firstID, secondID])

        first = try select(LocalDataSource(kind: .photos), for: first)
        let after = service.nodeDataSourceErrors(for: [first, second, unaffected])
        XCTAssertEqual(Set(after.keys), [secondID])
        XCTAssertEqual(after[secondID], before[secondID])
        XCTAssertEqual(first.defaultBackupDataSource().albums, [travel])
    }

    func testBackupOverrideCanReturnToAppDefault() throws {
        var profile = try makeProfile(name: "NAS")
        let appDefault = LocalDataSource(kind: .videos)
        profile = try select(LocalDataSource(kind: .photos), for: profile)
        let reopened = try DatabaseManager(databaseURL: databaseURL)
        profile = try XCTUnwrap(reopened.fetchServerProfile(id: XCTUnwrap(profile.id)))
        XCTAssertEqual(profile.defaultBackupDataSource(appDefault: appDefault).scope, .device(.photos))

        let id = try XCTUnwrap(profile.id)
        try database.setBackgroundBackupLastCompletedAt(Date(), profileID: id)
        profile = try select(nil, for: profile)
        XCTAssertNil(profile.backupDataSourceOverride)
        XCTAssertNil(profile.backgroundBackupDataSourceJSON)
        XCTAssertNil(try database.backgroundBackupLastCompletedAt(profileID: id))
        XCTAssertEqual(profile.defaultBackupDataSource(appDefault: appDefault), appDefault)
    }

    func testExistingBackgroundSelectionIsPreservedAsBackupDefault() throws {
        var profile = try makeProfile(name: "Legacy")
        let source = LocalDataSource(kind: .albums, albums: [travel])
        try database.setNodeBackupDataSourceJSON(try JSONEncoder().encode(source), profileID: XCTUnwrap(profile.id))
        profile = try XCTUnwrap(database.fetchServerProfile(id: XCTUnwrap(profile.id)))
        XCTAssertEqual(profile.backupDataSourceOverride, source)
        profile = try select(LocalDataSource(kind: .videos), for: profile)
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .device(.videos))
        XCTAssertEqual(profile.defaultBackupDataSource().albums, [travel])
    }

    func testUnknownPayloadKeysAreIgnoredAndDroppedWhenSaving() throws {
        var profile = try makeProfile(name: "NAS")
        let stored = Data(#"{"version":1,"backup":{"kind":"photos","albums":[]},"drop":{"kind":"videos","albums":[]}}"#.utf8)
        try database.setNodeBackupDataSourceJSON(stored, profileID: XCTUnwrap(profile.id))
        profile = try XCTUnwrap(database.fetchServerProfile(id: XCTUnwrap(profile.id)))
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .device(.photos))
        profile = try select(LocalDataSource(kind: .photos), for: profile)
        let saved = try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(profile.backgroundBackupDataSourceJSON)) as? [String: Any])
        XCTAssertNil(saved["drop"])
        XCTAssertEqual(profile.defaultBackupDataSource().scope, .device(.photos))

        profile.backgroundBackupDataSourceJSON = Data(#"{"version":1,"drop":{"kind":"videos","albums":[]}}"#.utf8)
        XCTAssertNil(profile.backupDataSourceOverride)
        XCTAssertEqual(profile.defaultBackupDataSource(appDefault: LocalDataSource(kind: .all)).scope, .device(.all))
    }

    func testDefaultTrackerOnlyResetsSelectionWhenNodeOrRelevantDefaultChanges() throws {
        var first = try select(LocalDataSource(kind: .photos), for: makeProfile(name: "First"))
        let second = try select(LocalDataSource(kind: .photos), for: makeProfile(name: "Second"))
        var tracker = BackupDataSourceDefaultsTracker(profile: first, source: first.defaultBackupDataSource())
        XCTAssertNil(tracker.changedSource(profile: first))
        XCTAssertEqual(tracker.changedSource(profile: second)?.scope, .device(.photos))
        XCTAssertNil(tracker.changedSource(profile: second))
        first = try select(LocalDataSource(kind: .videos), for: first)
        XCTAssertEqual(tracker.changedSource(profile: first)?.scope, .device(.videos))
    }

    @MainActor
    func testInheritedDefaultChangeWaitsForExecutionAndReplacesPendingAlbumNormalization() throws {
        let defaults = DefaultDeviceMediaScopeSetting.userDefaults
        let previous = defaults.object(forKey: LocalDataSourceStore.storageKey)
        defer {
            if let previous { defaults.set(previous, forKey: LocalDataSourceStore.storageKey) }
            else { defaults.removeObject(forKey: LocalDataSourceStore.storageKey) }
        }
        let albums = LocalDataSource(kind: .albums, albums: [travel])
        try LocalDataSourceStore.shared.setDefault(albums)
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
        let backup = HomeScreenStore(dependencies: dependencies)
        let drop = MediaDropLocalLibraryController(photoLibraryService: dependencies.photoLibraryService, makeAlbumBrowser: { _ in nil })
        let claim = try XCTUnwrap(dependencies.appRuntimeFlags.tryEnterExecution())
        defer { dependencies.appRuntimeFlags.exitExecution(claim) }
        drop.canChangeScope = { false }
        backup.refreshLocalPhotoAccessIfNeeded()
        try LocalDataSourceStore.shared.setDefault(LocalDataSource(kind: .photos))
        backup.reloadProfiles()
        drop.applyDefaultSourceIfNeeded()
        XCTAssertEqual(backup.localLibraryScope, albums.scope)
        XCTAssertEqual(drop.scope, albums.scope)

        dependencies.appRuntimeFlags.exitExecution(claim)
        drop.canChangeScope = { true }
        drop.applyDefaultSourceIfNeeded()
        XCTAssertEqual(backup.localLibraryScope, .device(.photos))
        XCTAssertEqual(drop.scope, .device(.photos))
    }

    @MainActor
    func testBackupUsesNodeDefaultWhileDropUsesAppDefaultAndKeepsTemporaryChoices() throws {
        let defaults = DefaultDeviceMediaScopeSetting.userDefaults
        let previous = defaults.object(forKey: LocalDataSourceStore.storageKey)
        defer {
            if let previous { defaults.set(previous, forKey: LocalDataSourceStore.storageKey) }
            else { defaults.removeObject(forKey: LocalDataSourceStore.storageKey) }
        }
        try LocalDataSourceStore.shared.setDefault(LocalDataSource(kind: .all))
        let profile = try select(LocalDataSource(kind: .photos), for: makeProfile(name: "NAS"))
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
        dependencies.appSession.activate(profile: profile, password: "")
        let backup = HomeScreenStore(dependencies: dependencies)
        let drop = MediaDropLocalLibraryController(
            photoLibraryService: dependencies.photoLibraryService,
            makeAlbumBrowser: { _ in nil }
        )
        XCTAssertEqual(backup.localLibraryScope, .device(.photos))
        XCTAssertEqual(drop.scope, .device(.all))
        backup.setLocalLibraryScope(.device(.videos))
        drop.setScope(.device(.photos))
        drop.applyDefaultSourceIfNeeded()
        backup.reloadProfiles()
        XCTAssertEqual(backup.localLibraryScope, .device(.videos))
        XCTAssertEqual(drop.scope, .device(.photos))
        XCTAssertEqual(LocalDataSourceStore.shared.defaultSource.kind, .all)

        let next = try select(LocalDataSource(kind: .all), for: makeProfile(name: "Next"))
        dependencies.appSession.activate(profile: next, password: "")
        drop.applyDefaultSourceIfNeeded()
        XCTAssertEqual(backup.localLibraryScope, .device(.all))
        XCTAssertEqual(drop.scope, .device(.photos))
    }

    @discardableResult
    private func select(_ source: LocalDataSource?, for profile: ServerProfileRecord) throws -> ServerProfileRecord {
        let id = try XCTUnwrap(profile.id)
        try database.setNodeBackupDataSourceJSON(
            profile.encodedBackupDataSource(selecting: source),
            profileID: id
        )
        return try XCTUnwrap(database.fetchServerProfile(id: id))
    }

    private func makeProfile(name: String) throws -> ServerProfileRecord {
        var profile = ServerProfileRecord(
            name: name, storageType: StorageType.smb.rawValue, sortOrder: 0,
            host: "nas.local", port: 445, shareName: "Photos", basePath: "/\(name)",
            username: "user", credentialRef: "test", createdAt: Date(), updatedAt: Date()
        )
        try database.saveServerProfile(&profile)
        return profile
    }
}
