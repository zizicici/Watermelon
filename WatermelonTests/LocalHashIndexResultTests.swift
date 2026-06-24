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

    // An asset that vanished mid-scan or carries no backup-eligible resources is classified .missing (not
    // .failed); like the prepare step and the upload-path skip, it must never arm the completeness abort.
    func testMissingIsNotIncomplete() {
        let result = LocalHashIndexBuildResult(
            requestedAssetIDs: ["a", "b"],
            readyAssetIDs: ["a"],
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: ["b"],
            networkPendingAssetIDs: []
        )

        XCTAssertTrue(result.incompleteAssetIDs.isEmpty)
        XCTAssertTrue(result.missingAssetIDs.contains("b"))
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
