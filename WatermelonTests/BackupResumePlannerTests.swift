import XCTest
@testable import Watermelon

/// `BackupResumePlanner.makePlan` skips assets whose local fingerprint is in the
/// V2-committed set FOR THE ASSET'S OWN MONTH so resume doesn't re-upload them. The
/// per-month scoping is load-bearing — a flat set lets two PHAssets in different
/// months share a fingerprint and silently mark each other as committed.
///
/// Per-month dedup requires `PHAsset.creationDate` to derive the asset's month, and
/// `PHAsset.fetchAssets` returns empty in unit tests. So the unit-test contract is
/// just: completedAssetIDs filtering + the empty-result early-return paths. The
/// per-month skip path lives in integration tests where PHAsset is real.
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
            completedAssetIDs: ["a"]
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["b", "c"])
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
            alreadyBackedUpFingerprintsByMonth: byMonth
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        // Both kept: PHAsset.fetchAssets returns empty in unit tests → no creationDate
        // → can't derive month → conservative no-skip. Production has real PHAssets.
        XCTAssertEqual(pending, ["a", "b"])
    }

    /// Empty per-month dedup map → all-pass early return (no records lookup, no PHAsset
    /// fetch). This unit-test path covers both the "no fingerprints provided" case and
    /// the production fast path when V2 cache is empty.
    func testRetryMode_emptyByMonth_keepsAllPending() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: [],
            alreadyBackedUpFingerprintsByMonth: PerMonth<Set<Data>>()
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["a", "b"])
    }
}
