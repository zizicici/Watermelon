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
        let result = normalizer.normalize(.allPhotos)
        XCTAssertEqual(result.scope, .allPhotos)
        XCTAssertNil(result.alert)
    }

    func testNormalize_emptyAlbums_degradesToAllPhotos_noAlert() {
        let normalizer = makeNormalizer(source: Source())
        let result = normalizer.normalize(.albums([]))
        XCTAssertEqual(result.scope, .allPhotos)
        XCTAssertNil(result.alert, "empty album set is a UI bug, not a user-visible loss")
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

    func testNormalize_allAlbumsDeleted_fallsBackToAllPhotos_withUnavailableAlert() {
        let source = Source()
        source.existing = []
        let normalizer = makeNormalizer(source: source)
        let result = normalizer.normalize(.albums(["a", "b"]))
        XCTAssertEqual(result.scope, .allPhotos)
        XCTAssertEqual(result.alert, .albumsUnavailable)
    }

    func testNormalize_someAlbumsDeleted_keepsRemaining_withUpdatedAlert() {
        let source = Source()
        source.existing = ["a"]
        let normalizer = makeNormalizer(source: source)
        let result = normalizer.normalize(.albums(["a", "b"]))
        XCTAssertEqual(result.scope, .albums(["a"]))
        XCTAssertEqual(result.alert, .albumsUpdated)
    }

    func testEmitAlert_dedupsRepeatCallsWithinDebounceWindow() {
        let normalizer = makeNormalizer(source: Source())
        var fireCount = 0
        normalizer.onAlert = { _, _ in fireCount += 1 }

        normalizer.emitAlertIfNotDebounced(.albumsUpdated)
        normalizer.emitAlertIfNotDebounced(.albumsUpdated)
        normalizer.emitAlertIfNotDebounced(.albumsUnavailable)

        XCTAssertEqual(fireCount, 1, "burst alerts within the 2s window collapse to one emission")
    }
}
