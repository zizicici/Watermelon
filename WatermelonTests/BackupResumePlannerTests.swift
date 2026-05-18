import XCTest
@testable import Watermelon

/// Unit scope stops before PHAsset-derived per-month dedup.
final class BackupResumePlannerTests: XCTestCase {
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    private var hashIndex: ContentHashIndexRepository!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
        hashIndex = ContentHashIndexRepository(databaseManager: databaseManager)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        hashIndex = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    func testRetryMode_completedAssetIDsAreFiltered() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b", "c"]),
            completedAssetIDs: ["a"],
            dedupMode: .v1CompletedIDs
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["b", "c"])
    }

    private func freshHandle(_ committed: PerMonth<Set<Data>>) -> RemoteViewHandle {
        RemoteViewHandle(
            revision: 1,
            committedAssetFingerprintsByMonth: committed,
            overlayFreshness: .fresh,
            producedAt: Date()
        )
    }

    private func staleHandle() -> RemoteViewHandle {
        RemoteViewHandle(
            revision: 0,
            committedAssetFingerprintsByMonth: PerMonth<Set<Data>>(),
            overlayFreshness: .stale,
            producedAt: Date()
        )
    }

    /// Without PHAsset.creationDate (unit-test environment), per-month dedup can't
    /// derive the asset's month and conservatively keeps the asset pending. The
    /// production skip path requires real PHAssets and is covered in integration tests.
    func testRetryMode_perMonthDedup_keepsPendingWhenMonthUnknown() async throws {
        let fp1 = TestFixtures.fingerprint(0xAA)
        try hashIndex.upsertAssetFingerprint(
            assetLocalIdentifier: "a", assetFingerprint: fp1,
            resourceCount: 1, totalFileSizeBytes: 100, modificationDateMs: nil
        )

        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        var byMonth = PerMonth<Set<Data>>()
        byMonth.insert(fp1, for: LibraryMonthKey(year: 2026, month: 5))
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: [],
            dedupMode: .v2(freshHandle(byMonth))
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        // Both kept: PHAsset.fetchAssets returns empty in unit tests → no creationDate
        // → can't derive month → conservative no-skip. Production has real PHAssets.
        XCTAssertEqual(pending, ["a", "b"])
    }

    /// Empty V2 committed view must ignore optimistic reducer completions.
    func testRetryMode_v2EmptyCommitted_ignoresCompletedAssetIDs() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: ["a"],
            dedupMode: .v2(freshHandle(PerMonth<Set<Data>>()))
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["a", "b"])
    }

    func testRetryMode_v2StaleOverlay_ignoresCompletedAssetIDs() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: ["a"],
            dedupMode: .v2(staleHandle())
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["a", "b"])
    }
}
