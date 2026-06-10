import XCTest
@testable import Watermelon

// Phase 0 (P02-TrackB-Phase0-BaselineProtectionNet): a baseline + protection net that locks the
// current default-off behaviour Track B must preserve, and pins named, non-red entry points for the
// behaviour each later Track B phase will introduce. The lit tests run today; the `Phase*` entries are
// skipped skeletons whose doc comments are the per-phase target spec — un-skip and assert the target
// when that phase lands. Finer-grained current-behaviour coverage already lives in PrepareRunCutoverTests,
// MonthManifestRelocateTests, OrphanCleanupLiteTests, WriteLockServiceTests, and DatabaseManagerWriterIDTests.
final class BaselineProtectionNetTests: XCTestCase {
    private let basePath = "/photos"
    private var keepAlive: [AnyObject] = []

    override func tearDown() {
        keepAlive.removeAll()
        super.tearDown()
    }

    // MARK: - Current-behaviour baseline (lit)

    // Flag-off read of a legacy V1 repo must stay pure V1: no version.json, no relocated Lite month
    // manifest, and nothing written into the .watermelon control tree (months / locks).
    func testFlagOffV1ReloadCreatesNoLiteVersionMonthsOrLocks() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()

        let service = try makePrepService(liteRepoEnabled: false)
        _ = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "flag-off must never create a Lite version.json")
        let liteMonth = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNil(liteMonth, "flag-off must not relocate the V1 month manifest into .watermelon/months")

        let listed = await client.listedPaths
        XCTAssertFalse(
            listed.contains(RepoLayoutLite.monthsDirectoryPath(basePath: basePath)),
            "flag-off must not touch the Lite months directory"
        )
        let created = await client.createdDirectories
        XCTAssertFalse(
            created.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
            "flag-off must not create the Lite locks directory"
        )
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(
            uploaded.contains { $0.contains("/.watermelon/") },
            "flag-off must write nothing into the Lite control tree"
        )
    }

    // A V1 manifest flush success writes only the legacy in-place manifest; it must not create a Lite
    // month manifest, a version.json, or any .watermelon control-tree artifact.
    func testV1ManifestFlushWritesOnlyLegacyManifestNoLiteTree() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        let flushed = try await store.flushToRemote()
        XCTAssertTrue(flushed)

        let v1Path = MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        let litePath = MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        let v1Data = await client.fileData(path: v1Path)
        let liteData = await client.fileData(path: litePath)
        XCTAssertNotNil(v1Data, "the legacy V1 month manifest must be written")
        XCTAssertNil(liteData, "a V1 flush must not write a Lite month manifest")

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "a V1 flush must not commit a Lite version.json")
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(
            uploaded.contains { $0.contains("/.watermelon/") },
            "a V1 flush must not upload into the Lite control tree"
        )
    }

    // MARK: - Later-phase target entries (skipped — un-skip when the phase lands)

    // Phase 1 (F02): an in-run upload finalizer must verify a month by REUSING the run's outer write
    // lease, not by acquiring+releasing an independent same-writer maintenance session — whose release
    // deletes the shared lock file out from under the still-active outer lease. The first assertion
    // pins the fix (reuse keeps the lock); the contrast documents the exact hazard reuse avoids.
    func testPhase1_F02_FinalizerVerifyReusesOuterLeaseWithoutDroppingLock() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = UUID().uuidString.lowercased()
        let monthKey = LibraryMonthKey(year: 2024, month: 3)

        // Outer run lease on a fresh Lite repo: commits version.json and holds the foreground lock.
        let outer = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        let lockedAtStart = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(lockedAtStart)

        // A committed Lite month whose only resource has no data file → verify must reconcile + flush.
        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        try seedStore.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")
        )
        _ = try await seedStore.flushToRemote()

        let service = try makePrepService(liteRepoEnabled: true)

        // Reuse path: verify through the OUTER session (no maintenance acquire/release). Its dirty
        // reconcile flush is gated by the outer lease, and the shared lock is left intact.
        try await service.verifyMonth(
            client: client, basePath: basePath, month: monthKey,
            plan: LiteRepoGateway.MaintenancePlan(layout: .lite, session: outer.session)
        )
        let lockedAfterReuse = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(
            lockedAfterReuse,
            "reusing the outer lease for finalizer verification must not delete the active outer lock"
        )

        // Contrast: an independent same-writer maintenance session releases — and thus DELETES — the
        // shared lock, dropping the still-active outer lease. This is the F02 hazard reuse avoids.
        let plan = try await service.makeMaintenancePlan(
            client: client, profile: makeProfile(writerID: writerID)
        )
        try await service.verifyMonth(client: client, basePath: basePath, month: monthKey, plan: plan)
        await plan.session?.stopAndRelease()
        let lockedAfterIndependent = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(
            lockedAfterIndependent,
            "an independent same-writer maintenance release deletes the shared lock — the exact F02 hazard"
        )

        await outer.session.stopAndRelease()
    }

    /// Phase 2 target (F05/F06/F09/F10): revise the lock/lease semantics in `WriteLockService`. Current
    /// behaviour, locked by WriteLockServiceTests (`testRefreshDoesNotRestoreConfidenceAfterNoteConfidenceLoss`,
    /// `testRefreshDoesNotRestoreConfidenceAfterInternalFault`, `testAssertStillOwnedWriteFailureBlocksConfidenceRestoration`):
    /// once confidence is latched off, a plain `refresh()` never restores it without a full
    /// `assertStillOwned`. Phase 2 may change when a refresh is allowed to recover confidence after the
    /// latch; assert the revised recovery rule here.
    func testPhase2_F05F06F09F10_LockLeaseSemantics() throws {
        throw XCTSkip("Phase 2 target entry (F05/F06/F09/F10) — un-skip when revised lock/lease semantics land.")
    }

    /// Phase 3 target (F03): reclassify an ambiguous month-data-dir `notFound` and guard the destructive
    /// prune. Current behaviour, locked by PrepareRunCutoverTests.testLoadSeededLiteTreatsMissingDataDirectoryAsEmpty:
    /// a `.lite` load treats a notFound LIST of `<YYYY>/<MM>` as an empty listing, so
    /// `reconcileWithRemoteListing` prunes every seed resource with no matching data file. Phase 3 must
    /// stop a transient/whole-directory absence from wiping a non-empty month.
    func testPhase3_F03_NotFoundReclassificationAndPruneGuards() throws {
        throw XCTSkip("Phase 3 target entry (F03) — un-skip when notFound reclassification + prune guards land.")
    }

    /// Phase 4 target (F04): repair-first maintenance cleanup. Current behaviour, locked by
    /// OrphanCleanupLiteTests: `OrphanCleanupLite.run` only best-effort deletes the fixed whitelist
    /// (months `.tmp`/`.bak` scratch, relocated V1 manifests, foreground expired locks) and never repairs.
    /// Phase 4 must attempt repair before (or instead of) deleting; assert the repair-first ordering here.
    func testPhase4_F04_RepairFirstCleanup() throws {
        throw XCTSkip("Phase 4 target entry (F04) — un-skip when repair-first cleanup lands.")
    }

    /// Phase 6 target (F14): lazily backfill and persist a canonical `writerID` on the write path. Current
    /// behaviour, locked by PrepareRunCutoverTests.testForegroundMissingWriterIdentityFailsClosed:
    /// `LiteRepoGateway.prepareForegroundWrite` throws `.writerIdentityUnavailable` when `profile.writerID`
    /// is nil, and `DatabaseManager.saveServerProfile` only mints a writerID on save. Phase 6 must backfill
    /// instead of failing closed; assert the lazy backfill + persistence here.
    func testPhase6_F14_WriterIDLazyBackfill() throws {
        throw XCTSkip("Phase 6 target entry (F14) — un-skip when writerID lazy backfill lands.")
    }

    // MARK: - Helpers

    private func makeProfile(writerID: String?) -> ServerProfileRecord {
        ServerProfileRecord(
            id: 1,
            name: "server",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host.local",
            port: 445,
            shareName: "share",
            basePath: basePath,
            username: "user",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: writerID
        )
    }

    private func makePrepService(liteRepoEnabled: Bool) throws -> BackupRunPreparationService {
        let dbm = try makeDatabaseManager()
        return BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(databaseManager: dbm),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: dbm),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: dbm,
            liteRepoEnabled: liteRepoEnabled
        )
    }

    private func makeDatabaseManager() throws -> DatabaseManager {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-db-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let dbm = try DatabaseManager(databaseURL: dir.appendingPathComponent("test.sqlite"))
        keepAlive.append(dbm)
        return dbm
    }
}
