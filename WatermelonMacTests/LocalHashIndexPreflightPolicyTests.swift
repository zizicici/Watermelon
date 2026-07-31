import XCTest
@testable import WatermelonMac

final class LocalHashIndexPreflightPolicyTests: XCTestCase {
    func testICloudUploadPendingForcesSingleWorker() {
        let result = makeResult(
            ready: ["ready"],
            unavailable: ["unavailable"],
            networkPending: ["offloaded"]
        )

        XCTAssertTrue(
            LocalHashIndexPreflightPolicy
                .requiresSingleUploadWorker(
                    initialResult: result,
                    uploadAssetIDs: ["offloaded"],
                    iCloudMode: .enable
                )
        )
        XCTAssertTrue(
            LocalHashIndexPreflightPolicy
                .requiresSingleUploadWorker(
                    initialResult: result,
                    uploadAssetIDs: ["unavailable"],
                    iCloudMode: .enable
                )
        )
    }

    func testNonUploadOrDisabledICloudDoesNotForceSingleWorker() {
        let result = makeResult(
            unavailable: ["unavailable"],
            networkPending: ["offloaded"]
        )

        XCTAssertFalse(
            LocalHashIndexPreflightPolicy
                .requiresSingleUploadWorker(
                    initialResult: result,
                    uploadAssetIDs: ["other"],
                    iCloudMode: .enable
                )
        )
        XCTAssertFalse(
            LocalHashIndexPreflightPolicy
                .requiresSingleUploadWorker(
                    initialResult: result,
                    uploadAssetIDs: ["offloaded"],
                    iCloudMode: .disable
                )
        )
    }

    func testRecoveryMergePreservesAccumulatedOutcomes() {
        let initial = makeResult(
            requested: ["a", "b", "c", "d"],
            ready: ["a"],
            unavailable: ["b", "c"],
            failed: ["d"],
            missing: ["missing-initial"],
            networkPending: ["a"]
        )
        let recovery = makeResult(
            requested: ["b", "c"],
            ready: ["b"],
            unavailable: ["c"],
            failed: ["failed-recovery"],
            missing: ["missing-recovery"],
            networkPending: ["b"]
        )

        let merged = LocalHashIndexPreflightPolicy.merging(
            initial,
            recovery: recovery
        )

        XCTAssertEqual(merged.requestedAssetIDs, ["a", "b", "c", "d"])
        XCTAssertEqual(merged.readyAssetIDs, ["a", "b"])
        XCTAssertEqual(merged.unavailableAssetIDs, ["c"])
        XCTAssertEqual(
            merged.failedAssetIDs,
            ["d", "failed-recovery"]
        )
        XCTAssertEqual(
            merged.missingAssetIDs,
            ["missing-initial", "missing-recovery"]
        )
        XCTAssertEqual(merged.networkPendingAssetIDs, ["a", "b"])
    }

    private func makeResult(
        requested: Set<String> = [],
        ready: Set<String> = [],
        unavailable: Set<String> = [],
        failed: Set<String> = [],
        missing: Set<String> = [],
        networkPending: Set<String> = []
    ) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: requested,
            readyAssetIDs: ready,
            unavailableAssetIDs: unavailable,
            failedAssetIDs: failed,
            missingAssetIDs: missing,
            networkPendingAssetIDs: networkPending
        )
    }
}
