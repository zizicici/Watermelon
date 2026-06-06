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

    private func freshHandle(_ safeToSkip: PerMonth<Set<AssetFingerprint>>) -> RemoteViewHandle {
        RemoteViewHandle(
            revision: 1,
            resumeCoverage: RemoteResumeCoverage(safeToSkipAssetFingerprintsByMonth: safeToSkip),
            overlayFreshness: .fresh,
            producedAt: Date()
        )
    }

    private func staleHandle() -> RemoteViewHandle {
        RemoteViewHandle(
            revision: 0,
            resumeCoverage: RemoteResumeCoverage(),
            overlayFreshness: .stale,
            producedAt: Date()
        )
    }

    /// Without PHAsset.creationDate (unit-test environment), per-month dedup can't
    /// derive the asset's month and conservatively keeps the asset pending. The
    /// production skip path requires real PHAssets and is covered in integration tests.
    func testRetryMode_perMonthDedup_keepsPendingWhenMonthUnknown() async throws {
        let fp1 = TestFixtures.assetFingerprint(0xAA)
        try hashIndex.upsertAssetFingerprint(
            assetLocalIdentifier: "a", assetFingerprint: fp1,
            resourceCount: 1, totalFileSizeBytes: 100, modificationDateMs: nil
        )

        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        var byMonth = PerMonth<Set<AssetFingerprint>>()
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

    /// Empty V2 safe-to-skip coverage must ignore optimistic reducer completions.
    func testRetryMode_v2EmptySafeToSkip_ignoresCompletedAssetIDs() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: ["a"],
            dedupMode: .v2(freshHandle(PerMonth<Set<AssetFingerprint>>()))
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

    /// The month-agnostic safe-to-skip union covers a fingerprint backed up complete under any month (so a
    /// restored asset whose local creation-date month diverges from its manifest month still dedups), while
    /// excluding fingerprints healing-required anywhere so a month genuinely needing a re-upload is not skipped.
    func testGlobalSafeToSkip_unionsAcrossMonthsAndExcludesHealing() {
        let janKey = LibraryMonthKey(year: 1970, month: 1)
        let mayKey = LibraryMonthKey(year: 2026, month: 5)
        let fSafeJan = TestFixtures.assetFingerprint(0x11)
        let fSafeMay = TestFixtures.assetFingerprint(0x22)
        let fHealing = TestFixtures.assetFingerprint(0x33)

        var safe = PerMonth<Set<AssetFingerprint>>()
        safe.insert(fSafeJan, for: janKey)
        safe.insert(fSafeMay, for: mayKey)
        safe.insert(fHealing, for: janKey)        // safe under jan...
        var healing = PerMonth<Set<AssetFingerprint>>()
        healing.insert(fHealing, for: mayKey)     // ...but healing-required under may

        let handle = RemoteViewHandle(
            revision: 1,
            resumeCoverage: RemoteResumeCoverage(
                safeToSkipAssetFingerprintsByMonth: safe,
                healingRequiredAssetFingerprintsByMonth: healing
            ),
            overlayFreshness: .fresh,
            producedAt: Date()
        )

        let global = BackupResumePlanner.globalSafeToSkip(handle)
        XCTAssertTrue(global.contains(fSafeJan), "a fingerprint safe under any month must be globally skippable")
        XCTAssertTrue(global.contains(fSafeMay), "a fingerprint safe under any month must be globally skippable")
        XCTAssertFalse(global.contains(fHealing),
            "a fingerprint healing-required anywhere must be excluded from the global skip set")
    }

    /// A non-clean month's best-effort healthy fingerprint must NOT enter the global safe-to-skip set: its
    /// content cannot be downloaded (non-clean downloads fail closed), so trusting it to cover a cross-month
    /// pending asset would skip the only re-uploadable local source. Clean months still contribute.
    func testGlobalSafeToSkip_excludesNonCleanMonthFingerprints() {
        let cleanKey = LibraryMonthKey(year: 1970, month: 1)
        let nonCleanKey = LibraryMonthKey(year: 2026, month: 5)
        let fClean = TestFixtures.assetFingerprint(0x44)
        let fNonClean = TestFixtures.assetFingerprint(0x55)

        var safe = PerMonth<Set<AssetFingerprint>>()
        safe.insert(fClean, for: cleanKey)
        safe.insert(fNonClean, for: nonCleanKey)

        let handle = RemoteViewHandle(
            revision: 1,
            resumeCoverage: RemoteResumeCoverage(safeToSkipAssetFingerprintsByMonth: safe),
            overlayFreshness: .fresh,
            producedAt: Date(),
            nonCleanMonths: [nonCleanKey]
        )

        let global = BackupResumePlanner.globalSafeToSkip(handle)
        XCTAssertTrue(global.contains(fClean),
            "a clean month's fingerprint must remain globally skippable")
        XCTAssertFalse(global.contains(fNonClean),
            "a non-clean month's best-effort fingerprint must not cover a cross-month asset")
    }

    // MARK: - Non-clean routing

    private let mayDirty = LibraryMonthKey(year: 2026, month: 5)
    private let juneDirty = LibraryMonthKey(year: 2026, month: 6)
    private let aprClean = LibraryMonthKey(year: 2026, month: 4)

    func testNonCleanRouter_excludesKnownNonCleanMonthAssets() {
        let routing = BackupResumeNonCleanRouter.route(
            monthsByAssetID: ["a": mayDirty, "b": mayDirty],
            nonCleanMonths: [mayDirty]
        )
        XCTAssertEqual(routing.routedAssetIDs, ["a", "b"])
        XCTAssertEqual(routing.blockedMonths, [mayDirty])
    }

    /// Mixed input: clean-month assets stay pending while non-clean-month assets are surfaced.
    func testNonCleanRouter_mixed_routesOnlyNonCleanMonthAssets() {
        let routing = BackupResumeNonCleanRouter.route(
            monthsByAssetID: ["a": aprClean, "b": mayDirty, "c": aprClean],
            nonCleanMonths: [mayDirty]
        )
        XCTAssertEqual(routing.routedAssetIDs, ["b"])
        XCTAssertEqual(routing.blockedMonths, [mayDirty])
    }

    /// All-blocked input: every asset routes out and its month surfaces as repair-required.
    func testNonCleanRouter_allBlocked_routesAllAndSurfacesMonths() {
        let routing = BackupResumeNonCleanRouter.route(
            monthsByAssetID: ["a": mayDirty, "b": juneDirty],
            nonCleanMonths: [mayDirty, juneDirty]
        )
        XCTAssertEqual(routing.routedAssetIDs, ["a", "b"])
        XCTAssertEqual(routing.blockedMonths, [mayDirty, juneDirty])
    }

    func testNonCleanRouter_emptyNonCleanSet_routesNothing() {
        let routing = BackupResumeNonCleanRouter.route(
            monthsByAssetID: ["a": mayDirty],
            nonCleanMonths: []
        )
        XCTAssertEqual(routing, BackupResumeNonCleanRouting())
    }

    /// An asset with no resolved month (no PHAsset) is absent from the map and must never route.
    func testNonCleanRouter_unknownMonthAsset_staysConservative() {
        let routing = BackupResumeNonCleanRouter.route(
            monthsByAssetID: ["a": mayDirty],
            nonCleanMonths: [mayDirty]
        )
        XCTAssertEqual(routing.routedAssetIDs, ["a"])
        XCTAssertFalse(routing.routedAssetIDs.contains("b"),
                       "an asset with no resolvable month must not be routed")
    }

    /// A mixed plan carries both a clean execution mode and repair-required months. The controller
    /// launches the clean subset and stashes the repair-required months so the terminal stays explicit.
    func testPlan_mixed_carriesBothExecutionAndRepairRequired() {
        let mixed = BackupResumePlan(
            resumedExecutionMode: .scoped(assetIDs: ["clean"]),
            repairRequiredMonths: [mayDirty]
        )
        XCTAssertNotNil(mixed.resumedExecutionMode)
        XCTAssertTrue(mixed.hasRepairRequiredWork)
        XCTAssertEqual(mixed.repairRequiredMonths, [mayDirty])
    }

    /// All-blocked plan must stay distinguishable from a genuine no-pending-work plan so the
    /// controller never reports resume complete-as-done while blocked work exists.
    func testPlan_allBlocked_distinctFromNoPendingWork() {
        let blocked = BackupResumePlan(resumedExecutionMode: nil, repairRequiredMonths: [mayDirty])
        XCTAssertNil(blocked.resumedExecutionMode)
        XCTAssertTrue(blocked.hasRepairRequiredWork)

        let noWork = BackupResumePlan(resumedExecutionMode: nil)
        XCTAssertNil(noWork.resumedExecutionMode)
        XCTAssertFalse(noWork.hasRepairRequiredWork)
    }

    /// Differential: V1 completed-ID filtering is unchanged and never surfaces repair-required.
    func testV1Mode_filtersCompletedIDs_noRepairRequired() async throws {
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: ["a"],
            dedupMode: .v1CompletedIDs
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["b"])
        XCTAssertFalse(plan.hasRepairRequiredWork)
    }

    /// A handle carrying non-clean months stays conservative in the unit environment: PHAsset.fetchAssets
    /// returns empty, so no month is derived, nothing is routed, and no repair-required state is surfaced.
    /// (Real month routing requires real PHAssets and is exercised on the pure router above.)
    func testV2Mode_nonCleanHandle_unitEnvConservative_keepsPendingNoRepair() async throws {
        let handle = RemoteViewHandle(
            revision: 1,
            resumeCoverage: RemoteResumeCoverage(),
            overlayFreshness: .fresh,
            producedAt: Date(),
            nonCleanMonths: [mayDirty]
        )
        let planner = BackupResumePlanner(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashIndex
        )
        let plan = try await planner.makePlan(
            pausedMode: .retry(assetIDs: ["a", "b"]),
            completedAssetIDs: [],
            dedupMode: .v2(handle)
        )
        guard case .retry(let pending) = plan.resumedExecutionMode else {
            XCTFail("expected .retry, got \(String(describing: plan.resumedExecutionMode))")
            return
        }
        XCTAssertEqual(pending, ["a", "b"])
        XCTAssertFalse(plan.hasRepairRequiredWork)
    }
}
