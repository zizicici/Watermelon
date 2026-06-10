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

    /// Phase 1 target (F02): define the nested maintenance-lock lifetime so a per-month
    /// `BackupRunPreparation.verifyMonth(profile:password:month:)` running inside a full-sweep lease
    /// cannot let its inner `stopAndRelease` drop the outer lease that `BackupCoordinator.verifyAllMonths`
    /// holds for the whole sweep. Current behaviour: each verify entry point owns a fresh
    /// `makeMaintenancePlan` lease for its own scope, with no nesting protection.
    func testPhase1_F02_NestedMaintenanceLockLifetime() throws {
        throw XCTSkip("Phase 1 target entry (F02) — un-skip when nested maintenance-lock lifetime lands.")
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
