import XCTest
@preconcurrency import Photos
@testable import Watermelon

@MainActor
final class ScopeNormalizerTests: XCTestCase {
    private final class Source {
        var status: PHAuthorizationStatus = .authorized
        var existing: Set<String> = []
    }

    private func makeNormalizer(source: Source) -> HomeScopeNormalizer {
        HomeScopeNormalizer(hooks: HomeScopeNormalizer.Hooks(
            authorizationStatus: { source.status },
            existingUserAlbumIdentifiers: { ids in source.existing.intersection(ids) }
        ))
    }

    func testNormalize_allPhotos_passthrough() {
        let normalizer = makeNormalizer(source: Source())
        for filter in PhotoLibraryMediaFilter.allCases {
            let result = normalizer.normalize(.device(filter))
            XCTAssertEqual(result.scope, .device(filter))
            XCTAssertNil(result.alert)
        }
    }

    func testNormalize_emptyAlbums_preservesScopeAndRequiresSelection() {
        let normalizer = makeNormalizer(source: Source())
        let result = normalizer.normalize(.albums([]))
        XCTAssertEqual(result.scope, .albums([]))
        XCTAssertEqual(result.alert, .emptyAlbums)
    }

    func testNormalize_unauthorized_passthrough() {
        let source = Source()
        source.status = .denied
        let normalizer = makeNormalizer(source: source)
        let scope = HomeLocalLibraryScope.albums(["a", "b"])
        let result = normalizer.normalize(scope)
        XCTAssertEqual(result.scope, scope, "unauthorized state defers normalization to the alert flow")
        XCTAssertNil(result.alert)
    }

    func testNormalize_allAlbumsExisting_passthrough() {
        let source = Source()
        source.existing = ["a", "b"]
        let normalizer = makeNormalizer(source: source)
        let scope = HomeLocalLibraryScope.albums(["a", "b"])
        let result = normalizer.normalize(scope)
        XCTAssertEqual(result.scope, scope)
        XCTAssertNil(result.alert)
    }

    func testNormalize_allAlbumsDeleted_preservesSelectionAndNames() {
        let source = Source()
        source.existing = []
        let normalizer = makeNormalizer(source: source)
        normalizer.albumNames = ["a": "Travel", "b": "Favorites"]
        let result = normalizer.normalize(.albums(["a", "b"]))
        XCTAssertEqual(result.scope, .albums(["a", "b"]))
        XCTAssertEqual(result.alert, .unavailableAlbums(["Travel", "Favorites"]))
    }

    func testNormalize_someAlbumsDeleted_preservesWholeSelectionAndNamesMissingAlbums() {
        let source = Source()
        source.existing = ["a"]
        let normalizer = makeNormalizer(source: source)
        normalizer.albumNames = ["a": "Travel", "b": "Favorites"]
        let result = normalizer.normalize(.albums(["a", "b"]))
        XCTAssertEqual(result.scope, .albums(["a", "b"]))
        XCTAssertEqual(result.alert, .unavailableAlbums(["Favorites"]))
        source.existing = ["a", "b"]
        XCTAssertNil(normalizer.normalize(result.scope).alert)
    }

    func testLimitedAccessRequiresPermissionInsteadOfReportingDeletedAlbums() {
        let source = Source()
        source.status = .limited
        let normalizer = makeNormalizer(source: source)
        let result = normalizer.normalize(.albums(["a", "b"]))
        XCTAssertEqual(result.scope, .albums(["a", "b"]))
        XCTAssertEqual(result.alert, .fullPhotoAccessRequired)
    }

    func testEmitAlert_dedupsRepeatCallsWithinDebounceWindow() {
        let normalizer = makeNormalizer(source: Source())
        var fireCount = 0
        normalizer.onAlert = { _ in fireCount += 1 }

        normalizer.emitAlertIfNotDebounced(.unavailableAlbums(["Travel"]))
        normalizer.emitAlertIfNotDebounced(.unavailableAlbums(["Travel"]))
        normalizer.emitAlertIfNotDebounced(.emptyAlbums)

        XCTAssertEqual(fireCount, 1, "burst alerts within the 2s window collapse to one emission")
    }
}
