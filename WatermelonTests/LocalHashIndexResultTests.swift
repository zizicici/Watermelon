import XCTest
@testable import Watermelon

final class LocalHashIndexResultTests: XCTestCase {

    // An asset that vanished mid-scan or carries no backup-eligible resources is classified .missing (not
    // .failed); like the prepare step and the upload-path skip, it must never arm the completeness abort.
    func testMissingIsNotIncomplete() {
        let result = LocalHashIndexBuildResult(
            requestedAssetIDs: ["a", "b"],
            readyAssetIDs: ["a"],
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: ["b"]
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
            missingAssetIDs: []
        )

        XCTAssertEqual(result.incompleteAssetIDs, ["a", "b"])
    }
}
