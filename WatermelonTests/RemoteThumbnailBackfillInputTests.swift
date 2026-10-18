import XCTest
@testable import Watermelon

final class RemoteThumbnailBackfillInputTests: XCTestCase {
    private let profileKey = "thumbnail-profile"
    private let month = LibraryMonthKey(year: 2024, month: 3)
    private let fingerprint = Data([1, 2, 3])

    private func cache(owner: String? = nil, hasAsset: Bool = false) -> RemoteLibrarySnapshotCache {
        let cache = RemoteLibrarySnapshotCache()
        if let owner { cache.setProfileKey(owner) }
        if hasAsset {
            let asset = TestFixtures.remoteAsset(year: month.year, month: month.month, fingerprint: fingerprint)
            cache.replaceMonth(month, resources: [], assets: [asset], assetResourceLinks: [])
        }
        return cache
    }

    private func resolve(_ cache: RemoteLibrarySnapshotCache) -> [Data]? {
        RemoteThumbnailSettingsViewController.backfillFingerprints(
            from: cache.state(since: nil),
            expectedProfileKey: profileKey
        )
    }

    func testUncertainSnapshotIsUnavailableEvenWhenItRetainsAssets() {
        let cache = cache(owner: profileKey, hasAsset: true)
        cache.invalidateIfOwned(by: profileKey)

        XCTAssertFalse(cache.state(since: nil).monthDeltas.isEmpty)
        XCTAssertNil(resolve(cache))
    }

    func testUncertainEmptySnapshotIsNotReportedAsNothingToGenerate() {
        let cache = cache(owner: profileKey)
        cache.invalidateIfOwned(by: profileKey)

        XCTAssertNil(resolve(cache))
    }

    func testAuthoritativeOwnedEmptySnapshotHasNothingToGenerate() {
        XCTAssertEqual(resolve(cache(owner: profileKey)), [])
    }

    func testForeignSnapshotIsUnavailableWhetherPopulatedOrEmpty() {
        XCTAssertNil(resolve(cache(owner: "other-profile", hasAsset: true)))
        XCTAssertNil(resolve(cache(owner: "other-profile")))
    }

    func testUnownedSnapshotIsUnavailable() {
        XCTAssertNil(resolve(cache()))
    }

    func testAuthoritativeOwnedSnapshotSuppliesBackfillFingerprints() {
        XCTAssertEqual(resolve(cache(owner: profileKey, hasAsset: true)), [fingerprint])
    }
}
