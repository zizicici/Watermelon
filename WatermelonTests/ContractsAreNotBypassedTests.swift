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

        // Optimistic-cache writes go through the writer handle. Asset appends are
        // post-commit now, so there is no markUncommitted parameter to clear later.
        let writer = RemoteIndexSyncService().makeOptimisticAssetWriter()
        let _: (RemoteManifestAsset, [RemoteAssetResourceLink]?) -> Void = writer.appendAsset
        let _: (RemoteManifestResource) -> Void = writer.appendResource
        let _: (any BackupMonthStore, Bool) async throws -> MonthManifestStore.FlushDelta = { store, ignoreCancellation in
            try await store.commitPendingAssetToRemote(ignoreCancellation: ignoreCancellation)
        }
    }
}
