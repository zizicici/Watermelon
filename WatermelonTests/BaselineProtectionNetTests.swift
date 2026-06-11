import XCTest
@testable import Watermelon

// Phase 0 (P02-TrackB-Phase0-BaselineProtectionNet): named protection entries for the Repo Lite cutover.
// The lit tests run today; skipped `Phase*` entries document later-phase target specs.
final class BaselineProtectionNetTests: XCTestCase {
    private let basePath = "/photos"
    private var keepAlive: [AnyObject] = []

    override func tearDown() {
        keepAlive.removeAll()
        super.tearDown()
    }

    // MARK: - Current-behaviour baseline (lit)

    // A connection-time reload of a legacy V1 repo upgrades it before publishing the remote snapshot.
    func testV1ReloadMigratesToLite() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()

        let service = try makePrepService()
        _ = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: UUID().uuidString.lowercased()))

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData, "reload must commit a Lite version.json")
        let liteMonth = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNotNil(liteMonth, "reload must relocate the V1 month manifest into .watermelon/months")
        let created = await client.createdDirectories
        XCTAssertTrue(
            created.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
            "reload must take a Lite lock while upgrading"
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
            client: client,
                lockClient: client, basePath: basePath, writerID: writerID
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

        let service = try makePrepService()

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

    /// Phase 3 (F03): an ambiguous month-data-dir `notFound` is reclassified and the destructive prune is
    /// guarded. A transient share-down LIST no longer reads as object absence (so a non-empty month is not
    /// wiped), while a genuinely-absent data directory still prunes stale seed entries by design.
    func testPhase3_F03_NotFoundReclassificationAndPruneGuards() async throws {
        // Reclassification: a transient share/redirector outage is retryable, never object absence.
        XCTAssertEqual(RemoteFaultLite.classify(RemoteErrorFixtures.smbBadNetworkName), .retryable)
        XCTAssertNotEqual(RemoteFaultLite.classify(RemoteErrorFixtures.smbBadNetworkName), .notFound)

        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")],
            assets: [],
            assetResourceLinks: []
        )

        // Prune guard: a transient share-down during a non-empty Lite seeded load surfaces, never flushing
        // an emptied manifest.
        let blinking = InMemoryRemoteStorageClient()
        await blinking.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: blinking, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: { true }
            )
            XCTFail("a transient share-down must not prune a non-empty Lite month")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
        let uploaded = await blinking.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "a transient probe failure must not flush an emptied manifest")

        // By design: a confirmed-missing data directory still prunes stale seed entries (after confirmation
        // and the ownership gate).
        let deleted = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadSeeded(
            client: deleted, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: { true }
        )
        XCTAssertNil(store.findByFileName("a.jpg"),
                     "a confirmed-missing data directory still prunes by design after confirmation + ownership")
    }

    /// Phase 4 target (F04): repair-first maintenance cleanup. Current behaviour, locked by
    /// OrphanCleanupLiteTests: `OrphanCleanupLite.run` only best-effort deletes the fixed whitelist
    /// (months `.tmp`/`.bak` scratch, relocated V1 manifests, foreground expired locks) and never repairs.
    /// Phase 4 must attempt repair before (or instead of) deleting; assert the repair-first ordering here.
    func testPhase4_F04_RepairFirstCleanup() throws {
        throw XCTSkip("Phase 4 target entry (F04) — un-skip when repair-first cleanup lands.")
    }

    /// Phase 6 (F14): lazily backfill and persist a canonical `writerID` on the write path. An upgraded
    /// saved profile with a NULL writerID is backfilled (mint + persist a lowercased UUID) so foreground
    /// prepare acquires the lock instead of failing closed — while a direct unsaved/nil identity still
    /// fails closed.
    func testPhase6_F14_WriterIDLazyBackfill() async throws {
        let dbm = try makeDatabaseManager()
        // A pre-v3 saved profile whose writerID column is still NULL.
        let id = try dbm.write { db -> Int64 in
            try db.execute(
                sql: """
                INSERT INTO \(ServerProfileRecord.databaseTableName)
                (name, storageType, sortOrder, host, port, shareName, basePath, username, credentialRef, backgroundBackupEnabled, createdAt, updatedAt, writerID)
                VALUES ('migrated', 'smb', 0, 'h', 445, 's', '\(basePath)', 'u', 'r', 0, '2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000', NULL)
                """
            )
            return db.lastInsertedRowID
        }
        let saved = try XCTUnwrap(try dbm.read { db in try ServerProfileRecord.fetchOne(db, key: id) })
        XCTAssertNil(saved.writerID, "precondition: upgraded profile has no writer ID")

        let backfilled = try dbm.profileWithBackfilledWriterID(saved)
        let writerID = try XCTUnwrap(backfilled.writerID, "backfill must populate a writer ID")
        XCTAssertNotNil(UUID(uuidString: writerID), "writer ID must be a UUID string")
        XCTAssertEqual(writerID, writerID.lowercased(), "writer ID must be lowercased")
        let persisted = try dbm.read { db in
            try String.fetchOne(db, sql: "SELECT writerID FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?", arguments: [id])
        }
        XCTAssertEqual(persisted, writerID, "backfill must persist the minted writer ID")

        // The backfilled identity lets foreground prepare acquire the lock instead of failing closed.
        let client = InMemoryRemoteStorageClient()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
                lockClient: client, basePath: basePath, writerID: writerID
        )
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, "a backfilled writer ID must let foreground prepare take the lock")
        await plan.session.stopAndRelease()

        // A direct nil identity still fails closed.
        let bare = InMemoryRemoteStorageClient()
        do {
            _ = try await LiteRepoGateway.prepareForegroundWrite(client: bare, lockClient: bare, basePath: basePath, writerID: nil)
            XCTFail("a direct nil writer identity must still fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .writerIdentityUnavailable)
        }
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

    private func makePrepService() throws -> BackupRunPreparationService {
        let dbm = try makeDatabaseManager()
        return BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(databaseManager: dbm),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: dbm),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: dbm
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
