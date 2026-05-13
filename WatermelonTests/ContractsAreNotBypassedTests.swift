import XCTest
@testable import Watermelon

/// Compile-time + runtime guard against tests bypassing production write paths.
///
/// The "tests bypass contracts" failure mode is silent: a test that constructs
/// `RemoteLibrarySnapshotCache` state directly will pass even when the
/// production write path it represents is broken. Past reviews surfaced this
/// repeatedly — `service.upsertCachedAsset(...)` with a phantom asset row,
/// then a follow-up bug in production that direct-mutation tests never
/// reached. Step 9's lockdown asserts the typed boundaries are respected.
///
/// This test does NOT scan source files (impractical from inside an iOS test
/// target) — instead it pins the legitimate write path constants. If a future
/// change adds a new public mutation API on `RemoteIndexSyncService` /
/// `RepoCommittedView`, the test here doesn't fail automatically; the
/// requirement is documented in the type's doc-comment + Step 9's plan-file
/// section.
final class ContractsAreNotBypassedTests: XCTestCase {
    /// Sanity check: `RepoTestBuilder` exists and produces a fresh repo via
    /// real production paths. Tests have no excuse to construct fake state
    /// when the builder is one call away.
    func testRepoTestBuilder_freshRepo_works() async throws {
        let builder = try await RepoTestBuilder.freshRepo()
        let output = try await builder.materialize()
        XCTAssertTrue(output.state.months.isEmpty, "fresh repo has no months")
    }

    /// Lock in the canonical write API names. If a refactor renames or splits
    /// these, the test fails — the maintainer must consciously decide whether
    /// the new shape is safe for tests to call directly. Compile-time check.
    func testCanonicalWriteAPIShape() {
        // RepoCommittedView's mutation entry points
        let _: (RepoMaterializer.MaterializeOutput) -> [LibraryMonthKey: Set<Data>] = RepoCommittedView().loadFromMaterialize
        let _: (LibraryMonthKey) -> Bool = RepoCommittedView().removeMonth

        // OptimisticInflightTracker's mutation entry points
        let tracker = OptimisticInflightTracker()
        let _: (LibraryMonthKey, Set<Data>) -> Void = tracker.markUncommittedAssets
        let _: () -> Void = tracker.reset

        // Optimistic-cache writes go through the writer handle so cache + inflight
        // can't drift apart. RemoteIndexSyncService no longer exposes raw
        // upsertCachedAsset/upsertCachedResource publicly.
        let writer = RemoteIndexSyncService().makeOptimisticAssetWriter()
        let _: (RemoteManifestAsset, [RemoteAssetResourceLink]?, Bool) -> Void = writer.appendAsset
        let _: (RemoteManifestResource) -> Void = writer.appendResource
        let _: (LibraryMonthKey, Set<Data>) -> Void = writer.markUncommitted
    }
}
