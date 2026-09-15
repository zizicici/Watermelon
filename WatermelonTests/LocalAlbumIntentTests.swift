import Photos
import XCTest
@testable import Watermelon

final class LocalAlbumIntentTests: XCTestCase {
    private var suiteName: String!
    private var defaults: UserDefaults!
    private var names: LocalAlbumNameCache!

    override func setUp() {
        super.setUp()
        suiteName = "LocalAlbumIntentTests.\(UUID().uuidString)"
        defaults = UserDefaults(suiteName: suiteName)
        names = LocalAlbumNameCache(defaults: defaults)
    }

    override func tearDown() {
        defaults.removePersistentDomain(forName: suiteName)
        names = nil
        defaults = nil
        super.tearDown()
    }

    func testAlbumPickerQueriesTheLibraryAndSupportsSearchWithoutAppSelection() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let albums = [LocalAlbumReference(id: "a", name: "Travel"), LocalAlbumReference(id: "b", name: "Favorites")]
        let query = LocalAlbumQuery(names: names, authorizationStatus: { .authorized }) { ids in
            XCTAssertNil(ids)
            return albums
        }
        let suggestions = try await query.suggestedEntities()
        XCTAssertEqual(suggestions.map(\.reference), albums)
        let matches = try await query.entities(matching: "travel")
        XCTAssertEqual(matches.map(\.id), ["a"])
        let defaults = await query.defaultResult()
        XCTAssertNil(defaults)
    }

    func testDeletedAlbumsRemainInResolvedSelectionAndCauseWholeSelectionFailure() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let travel = LocalAlbumReference(id: "a", name: "Travel")
        let favorites = LocalAlbumReference(id: "b", name: "Favorites")
        names.remember([travel, favorites])
        let query = LocalAlbumQuery(names: names, authorizationStatus: { .authorized }) { ids in
            XCTAssertEqual(ids, ["a", "b"])
            return [travel]
        }
        let resolved = try await query.entities(for: ["b", "a"])
        XCTAssertEqual(resolved.map(\.reference), [favorites, travel])
        let source = try LocalDataSourceEntity(kind: .albums).resolve(albums: resolved)
        XCTAssertEqual(source.scope, .albums(["a", "b"]))
        XCTAssertThrowsError(try LocalDataSourceError.validateAlbums(
            source.scope.selectedAlbumIdentifiers, authorization: .authorized,
            existing: { ["a"] }, names: Dictionary(source.albums.map { ($0.id, $0.name) }, uniquingKeysWith: { _, new in new })
        )) { error in
            XCTAssertEqual(error as? LocalDataSourceError, .unavailableAlbums(["Favorites"]))
        }
    }

    func testAlbumNamesSurviveCacheReloadAndFollowRenamesWithoutChangingIdentity() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        names.remember([LocalAlbumReference(id: "a", name: "Before")])
        let query = LocalAlbumQuery(names: names, authorizationStatus: { .authorized }) { _ in
            [LocalAlbumReference(id: "a", name: "After")]
        }
        let refreshed = try await query.entities(for: ["a"])
        XCTAssertEqual(refreshed.map(\.id), ["a"])
        XCTAssertEqual(refreshed.map(\.title), ["After"])
        let reloaded = LocalAlbumNameCache(defaults: defaults)
        let missingQuery = LocalAlbumQuery(names: reloaded, authorizationStatus: { .authorized }, fetchAlbums: { _ in [] })
        let missing = try await missingQuery.entities(for: ["a"])
        XCTAssertEqual(missing.map(\.title), ["After"])
    }

    func testSameNamedAlbumsKeepDistinctIdentifiers() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let query = LocalAlbumQuery(names: names, authorizationStatus: { .authorized }) { _ in
            [LocalAlbumReference(id: "a", name: "Travel"), LocalAlbumReference(id: "b", name: "Travel")]
        }
        let albums = try await query.suggestedEntities()
        XCTAssertEqual(try LocalDataSourceEntity(kind: .albums).resolve(albums: albums).scope, .albums(["a", "b"]))
    }

    func testPermissionLossPreservesSavedIdentifiersButDoesNotListAlbums() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        names.remember([LocalAlbumReference(id: "a", name: "Travel")])
        for status: PHAuthorizationStatus in [.limited, .denied, .restricted, .notDetermined] {
            let query = LocalAlbumQuery(names: names, authorizationStatus: { status }) { _ in
                XCTFail("Do not fetch albums without full photo access")
                return []
            }
            let selected = try await query.entities(for: ["a"])
            XCTAssertEqual(selected.map(\.id), ["a"])
            XCTAssertEqual(selected.map(\.title), ["Travel"])
            do {
                _ = try await query.suggestedEntities()
                XCTFail("Album selection must require full photo access")
            } catch {
                XCTAssertEqual(error as? LocalDataSourceError, .fullPhotoAccessRequired)
            }
        }
    }

    func testUnknownAlbumIsPreservedForValidationEvenWithoutCachedName() async throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let query = LocalAlbumQuery(names: names, authorizationStatus: { .authorized }, fetchAlbums: { _ in [] })
        let selected = try await query.entities(for: ["missing"])
        XCTAssertEqual(selected.map(\.id), ["missing"])
        XCTAssertEqual(selected.map(\.title), [String(localized: "home.localAlbums.untitled")])
    }
}
