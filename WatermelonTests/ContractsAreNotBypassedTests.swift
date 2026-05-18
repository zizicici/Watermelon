import XCTest
@testable import Watermelon

final class ContractsAreNotBypassedTests: XCTestCase {
    func testRepoTestBuilder_freshRepo_works() async throws {
        let builder = try await RepoTestBuilder.freshRepo()
        let output = try await builder.materialize()
        XCTAssertTrue(output.state.months.isEmpty, "fresh repo has no months")
    }

    func testCanonicalWriteAPIShape() {
        // RepoCommittedView's mutation entry points
        let _: (RepoMaterializer.MaterializeOutput) -> [LibraryMonthKey: Set<Data>] = RepoCommittedView().loadFromMaterialize
        let _: (LibraryMonthKey) -> Bool = RepoCommittedView().removeMonth

        // OptimisticInflightTracker's mutation entry points
        let tracker = OptimisticInflightTracker()
        let _: (LibraryMonthKey, Set<Data>) -> Void = tracker.markUncommittedAssets
        let _: () -> Set<LibraryMonthKey> = tracker.reset

        // Optimistic-cache writes go through the writer handle so cache + inflight
        // can't drift apart. RemoteIndexSyncService no longer exposes raw
        // upsertCachedAsset/upsertCachedResource publicly.
        let writer = RemoteIndexSyncService().makeOptimisticAssetWriter()
        let _: (RemoteManifestAsset, [RemoteAssetResourceLink]?, Bool) -> Void = writer.appendAsset
        let _: (RemoteManifestResource) -> Void = writer.appendResource
        let _: (LibraryMonthKey, Set<Data>) -> Void = writer.markUncommitted
    }
}
