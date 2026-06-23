import XCTest
@testable import Watermelon

final class LocalHashIndexResultTests: XCTestCase {

    // A cache-valid asset whose bytes are offloaded to iCloud is fingerprint-complete: it must not count
    // toward the download/complement abort gate (incompleteAssetIDs), only as an upload network hint.
    func testNetworkPendingIsNotIncomplete() {
        let result = LocalHashIndexBuildResult(
            requestedAssetIDs: ["a"],
            readyAssetIDs: ["a"],
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: [],
            networkPendingAssetIDs: ["a"]
        )

        XCTAssertTrue(result.incompleteAssetIDs.isEmpty)
        XCTAssertTrue(result.networkPendingAssetIDs.contains("a"))
    }

    // Genuinely-unknown fingerprints (no cache) still gate the run closed.
    func testUnavailableAndFailedStillIncomplete() {
        let result = LocalHashIndexBuildResult(
            requestedAssetIDs: ["a", "b", "c"],
            readyAssetIDs: ["c"],
            unavailableAssetIDs: ["a"],
            failedAssetIDs: ["b"],
            missingAssetIDs: [],
            networkPendingAssetIDs: ["c"]
        )

        XCTAssertEqual(result.incompleteAssetIDs, ["a", "b"])
    }
}
