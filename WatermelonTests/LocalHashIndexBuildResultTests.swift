import XCTest
@testable import Watermelon

// The fail-closed download/complement gate consumes `incompleteAssetIDs`. A cache-valid
// asset (fingerprint already durable) whose bytes are offloaded or whose offline probe
// errors must land in `cachedBytesUnavailableAssetIDs`, never in the gating buckets.
final class LocalHashIndexBuildResultTests: XCTestCase {

    private func result(
        ready: Set<PhotoKitLocalIdentifier> = [],
        cachedBytesUnavailable: Set<PhotoKitLocalIdentifier> = [],
        unavailable: Set<PhotoKitLocalIdentifier> = [],
        failed: Set<PhotoKitLocalIdentifier> = []
    ) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: ready.union(cachedBytesUnavailable).union(unavailable).union(failed),
            readyAssetIDs: ready,
            cachedBytesUnavailableAssetIDs: cachedBytesUnavailable,
            unavailableAssetIDs: unavailable,
            failedAssetIDs: failed,
            missingAssetIDs: []
        )
    }

    // F1: offloaded bytes on an already-indexed asset must not gate the run.
    func testCachedBytesUnavailableDoesNotCountAsIncomplete() {
        let r = result(ready: ["a"], cachedBytesUnavailable: ["b", "c"])
        XCTAssertTrue(r.incompleteAssetIDs.isEmpty)
    }

    // F2: a probe error on a cache-valid asset is classified the same way, so it also
    // never gates the run regardless of iCloud setting.
    func testMixedReadyAndCachedBytesUnavailableNeverGates() {
        let r = result(ready: ["a", "b"], cachedBytesUnavailable: ["c"])
        XCTAssertTrue(r.incompleteAssetIDs.isEmpty)
    }

    // A never-indexed offloaded asset (genuine index incompleteness) still gates.
    func testGenuineUnavailableCountsAsIncomplete() {
        let r = result(ready: ["a"], cachedBytesUnavailable: ["b"], unavailable: ["c"])
        XCTAssertEqual(r.incompleteAssetIDs, ["c"])
    }

    func testGenuineFailedCountsAsIncomplete() {
        let r = result(ready: ["a"], cachedBytesUnavailable: ["b"], failed: ["d"])
        XCTAssertEqual(r.incompleteAssetIDs, ["d"])
    }

    // Worker-count downgrade still sees cache-valid offloaded assets alongside genuine
    // unavailables, preserving the "any iCloud-only asset in upload scope forces 1 worker"
    // behavior.
    func testBytesUnavailableForUploadUnionsBothBuckets() {
        let r = result(cachedBytesUnavailable: ["b"], unavailable: ["c"])
        XCTAssertEqual(r.bytesUnavailableForUploadAssetIDs, ["b", "c"])
    }

    func testBytesUnavailableForUploadExcludesReadyAndFailed() {
        let r = result(ready: ["a"], cachedBytesUnavailable: ["b"], failed: ["d"])
        XCTAssertEqual(r.bytesUnavailableForUploadAssetIDs, ["b"])
    }
}
