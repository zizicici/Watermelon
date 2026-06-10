import XCTest
import GRDB
@testable import Watermelon

// Step 6A (P06-PrepareRunCutover): default-off internal Lite prepare-run cutover. Exercises the routing
// gateway, the lease/ownership gates, the executor release lifecycle, read/verify routing, the flag-off
// V1 differential, and a real on-disk fresh-backup artifact layout.
final class PrepareRunCutoverTests: XCTestCase {
    private let basePath = "/photos"
    private var keepAlive: [AnyObject] = []

    override func tearDown() {
        keepAlive.removeAll()
        super.tearDown()
    }

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

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

    private func seedCommittedVersion(_ client: InMemoryRemoteStorageClient) async throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: "2026-01-01T00:00:00Z", createdBy: "seed")
        let data = try VersionManifestLite.encode(manifest)
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: data)
    }

    private func seedV1Manifest(_ client: InMemoryRemoteStorageClient) async {
        await client.seedFile(path: "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)", data: Data([0x01]))
    }

    // MARK: - Foreground write routing (fresh / current / version / layout / release)

    func testForegroundFreshAcquiresLockCommitsVersionAndUsesLiteLayout() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )

        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, "fresh route must acquire the foreground lock")

        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        XCTAssertEqual(manifest.layout, VersionManifestLite.layout)
        XCTAssertEqual(manifest.createdBy, writerID)

        await plan.session.stopAndRelease()
        let afterRelease = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(afterRelease, "release must delete the lock")
    }

    func testForegroundCurrentAcquiresLockWithoutRewritingVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )

        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(
            uploaded.contains(RepoLayoutLite.versionPath(basePath: basePath)),
            ".current must not re-commit version.json"
        )
        await plan.session.stopAndRelease()
    }

    // MARK: - Foreground whitelisted cleanup integration (P08)

    func testForegroundCurrentRunsWhitelistedCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let scratchPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath) + "/manifest_x.tmp"
        await client.seedFile(path: scratchPath, data: Data([0x01]))
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        let scratchGone = await client.fileData(path: scratchPath)
        XCTAssertNil(scratchGone, ".current foreground prepare must clean months scratch under its lock")
        await plan.session.stopAndRelease()
    }

    func testForegroundV1MigrateCleansOldV1ManifestAfterCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()
        let v1ManifestPath = "\(basePath)/2024/03/\(MonthManifestStore.manifestFileName)"
        let beforeMigrate = await client.fileData(path: v1ManifestPath)
        XCTAssertNotNil(beforeMigrate, "precondition: the legacy V1 manifest exists")

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: newWriterID()
        )
        let oldV1Gone = await client.fileData(path: v1ManifestPath)
        let liteManifest = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNil(oldV1Gone, "after migrating + committing, the old V1 manifest is cleaned")
        XCTAssertNotNil(liteManifest, "the relocated Lite month manifest must remain")
        await plan.session.stopAndRelease()
    }

    // MARK: - Fail-closed routing (.v1Migrate / damaged / unsupported / probe fault / contention / id)

    // Foreground .v1Migrate now migrates rather than failing closed (see V1ToLiteMigrationTests for the
    // full copy/validate/commit coverage); here we only confirm the route is accepted and ends committed.
    func testForegroundV1MigrateMigratesAndCommitsVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData, ".v1Migrate must commit version.json after migrating")
        let liteData = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNotNil(liteData, "the V1 month manifest must be relocated under .watermelon/months")
        await plan.session.stopAndRelease()
    }

    func testForegroundDamagedFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testForegroundUnsupportedFutureFormatFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        let future = WatermelonRemoteVersionManifest(
            formatVersion: 3, layout: "lite-month-sqlite", minAppVersion: "9.9.9",
            createdAt: "x", createdBy: "y"
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(future))

        await assertThrowsLiteError(.repoUnsupported) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testForegroundProbeFaultFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // base-path probe blinks

        await assertThrowsLiteError(.probeFault(.retryable)) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testForegroundLockContentionFailsClosedWithoutOwnLock() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let other = newWriterID()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: now)   // fresh foreign lock
        let writerID = newWriterID()

        await assertThrowsLiteError(.lockConflict) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: writerID, now: now
            )
        }
        let ownLock = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(ownLock, "a contended foreground acquire must not leave our own lock behind")
        let foreignLock = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(foreignLock, "foreground must not delete a fresh foreign lock")
    }

    func testForegroundMissingWriterIdentityFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        await assertThrowsLiteError(.writerIdentityUnavailable) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: nil
            )
        }
    }

    func testForegroundVersionCommitFailureReleasesLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound) // under-lock classify readVersion: no version.json
        await client.enqueueDownloadData(Data([0x00]))   // version read-back returns wrong bytes
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a prep error after lock acquire must release the lock")
    }

    // MARK: - Read routing (no lock)

    func testResolveReadLayoutCurrentReturnsLiteAndTakesNoLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)

        let layout = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: basePath)
        XCTAssertEqual(layout, .lite)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "a pure read must never write a lock")
    }

    func testResolveReadLayoutFreshReturnsLite() async throws {
        let client = InMemoryRemoteStorageClient()
        let layout = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: basePath)
        XCTAssertEqual(layout, .lite)
    }

    func testResolveReadLayoutV1ReturnsV1() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedV1Manifest(client)
        let layout = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: basePath)
        XCTAssertEqual(layout, .v1)
    }

    func testResolveReadLayoutDamagedThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: self.basePath)
        }
    }

    // MARK: - Maintenance (verify) routing

    func testMaintenanceCurrentAcquiresLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        XCTAssertNotNil(plan.session)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        // verify must not initialize a repo: no version.json committed here.
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(uploaded.contains(RepoLayoutLite.versionPath(basePath: basePath)))
        await plan.session?.stopAndRelease()
    }

    func testMaintenanceCurrentRunsWhitelistedCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let scratchPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath) + "/manifest_x.tmp"
        await client.seedFile(path: scratchPath, data: Data([0x01]))
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client, basePath: basePath, writerID: writerID
        )
        let scratchGone = await client.fileData(path: scratchPath)
        XCTAssertNil(scratchGone, ".current maintenance still cleans whitelisted scratch under its lock")
        await plan.session?.stopAndRelease()
    }

    func testMaintenanceFreshDoesNotRunCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        // Fresh route: month scratch under a `.watermelon` dir with no committed version.json, no V1
        // manifest, and no Lite month sqlite. Verify never commits version.json, so there is no
        // committed/current Lite repo to maintain and cleanup must not run.
        let scratchPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath) + "/manifest_x.tmp"
        await client.seedFile(path: scratchPath, data: Data([0x01]))
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let scratchSurvives = await client.fileData(path: scratchPath)
        XCTAssertNotNil(scratchSurvives, ".fresh maintenance must not clean — no committed/current Lite repo")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "verify must not initialize a fresh repo")
        await plan.session?.stopAndRelease()
    }

    func testMaintenanceV1IsLockFree() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedV1Manifest(client)
        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client, basePath: basePath, writerID: newWriterID()
        )
        XCTAssertEqual(plan.layout, .v1)
        XCTAssertNil(plan.session)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "V1 verify must take no lock")
    }

    func testMaintenanceDamagedThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testVerifyMonthFailsClosedWhenOwnershipLostBeforeFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        // Seed a Lite month manifest containing a phantom asset (no links) so reconcile must delete it.
        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        try seedStore.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")
        )
        try seedStore.upsertAsset(
            TestFixtures.remoteAsset(year: 2024, month: 3, fingerprint: Data([0xBB]), resourceCount: 0),
            links: []
        )
        _ = try await seedStore.flushToRemote()

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2024, month: 3),
                layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("verify must fail closed when ownership cannot be re-asserted before flush")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    // MARK: - Load-time reconcile flush ownership gate (R02 finding 1)

    /// Seeds a Lite month manifest whose only resource has no matching data file, so a *fresh* load's
    /// `reconcileWithRemoteListing` prunes it → store dirty → the first remote manifest write fires
    /// during load. This is the path that must now be ownership-gated.
    private func seedDirtyAtLoadLiteMonth(_ client: InMemoryRemoteStorageClient) async throws {
        await client.seedDirectory("\(basePath)/2024/03")
        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        try seedStore.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")
        )
        _ = try await seedStore.flushToRemote()
    }

    func testLoadOrCreateLiteReconcileFlushFailsClosedWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedDirtyAtLoadLiteMonth(client)

        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("a dirty load-time reconcile flush must fail closed when ownership is lost/foreign")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testLoadOrCreateLiteReconcileFlushProceedsWhenOwned() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedDirtyAtLoadLiteMonth(client)

        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        XCTAssertNil(store.findByFileName("a.jpg"), "owned reconcile should prune the resource missing from the listing")
        XCTAssertFalse(store.dirty, "owned reconcile should have flushed the pruned manifest")
    }

    func testLoadSeededLiteReconcileFlushFailsClosedWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        // A seed whose resource has no matching data file ⇒ reconcile prunes it on load ⇒ dirty flush.
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("a dirty seeded-load reconcile flush must fail closed when ownership is lost/foreign")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testLoadSeededLiteCleanReconcileFailsClosedWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        // Seed a resource that matches a data file, so reconcile is clean and dirty stays false.
        await client.seedFile(path: "\(basePath)/2024/03/b.jpg", data: Data([0xCC]))
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("a clean Lite seeded load must fail closed when ownership is lost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testLoadOrCreateV1ReconcileFlushUngatedByDefault() async throws {
        // Flag-off / V1 default (no assertOwnership) must keep flushing on load with no gate.
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try seedStore.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")
        )
        _ = try await seedStore.flushToRemote()

        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        XCTAssertNil(store.findByFileName("a.jpg"), "V1 load reconcile prunes and flushes with no ownership gate")
        XCTAssertFalse(store.dirty)
    }

    // MARK: - Lite missing data directory (F-02)

    func testLoadSeededLiteTreatsMissingDataDirectoryAsEmpty() async throws {
        let client = InMemoryRemoteStorageClient()
        // No YYYY/MM directory seeded — it's missing on the remote.
        // But the Lite manifest sqlite exists under .watermelon/months.
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: { true }
        )
        // Seed resource a.jpg has no matching data file (directory is gone → empty listing).
        // Reconcile should prune it since the data file is absent.
        XCTAssertNil(store.findByFileName("a.jpg"), "missing data dir should be treated as empty, pruning stale seed entries")
    }

    func testLoadSeededLiteRecreatesMissingDataDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        // No YYYY/MM directory seeded — it's missing on the remote.
        let seed = MonthManifestStore.Seed(
            resources: [],
            assets: [],
            assetResourceLinks: []
        )
        _ = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: { true }
        )
        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/2024/03"),
                      "loadSeeded must recreate the missing YYYY/MM data directory for directory-backed backends")
    }

    func testLoadSeededLiteMissingDataDirectoryDoesNotCreateDirWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        // No YYYY/MM directory seeded — it's missing on the remote.
        // Ownership is lost: the directory must not be created.
        let seed = MonthManifestStore.Seed(
            resources: [],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("loadSeeded must fail closed when ownership is lost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
        let created = await client.createdDirectories
        XCTAssertFalse(created.contains("/photos/2024/03"),
                      "loadSeeded must not create the data directory before confirming ownership")
    }

    // MARK: - Unseeded Lite loadOrCreate ownership / missing data directory (R09)

    func testLoadOrCreateLiteMissingDataDirectoryDoesNotCreateDirWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        // No YYYY/MM directory seeded. Unseeded path, ownership lost.
        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
                assertOwnership: { false }
            )
            XCTFail("loadOrCreate must fail closed when ownership is lost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
        let created = await client.createdDirectories
        XCTAssertFalse(created.contains("/photos/2024/03"),
                      "loadOrCreate must not create the data directory before confirming ownership")
    }

    func testLoadOrCreateLiteMissingDataDirectoryCreatesDirWhenOwned() async throws {
        let client = InMemoryRemoteStorageClient()
        // No YYYY/MM directory seeded. Unseeded path, ownership confirmed.
        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/2024/03"),
                     "loadOrCreate must create the missing YYYY/MM data directory after confirming ownership")
    }

    func testLoadOrCreateLiteExistingDirectoryDoesNotRecreateDir() async throws {
        let client = InMemoryRemoteStorageClient()
        // YYYY/MM directory already exists. Unseeded path.
        await client.seedDirectory("/photos/2024/03")
        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        let created = await client.createdDirectories
        XCTAssertFalse(created.contains("/photos/2024/03"),
                      "loadOrCreate must not recreate an existing data directory")
    }

    func testLoadOrCreateV1StillCreatesDirectoryUpfront() async throws {
        let client = InMemoryRemoteStorageClient()
        // V1 path must still create the directory upfront (no ownership gate).
        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/2024/03"),
                      "V1 loadOrCreate must still create the directory upfront")
    }

    func testLoadSeededLiteSurfacesNonNotFoundListError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: { true }
            )
            XCTFail("a retryable list error must surface, not be treated as empty")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
    }

    func testVerifyMonthLiteTreatsMissingDataDirectoryAsEmpty() async throws {
        let client = InMemoryRemoteStorageClient()
        // Seed a committed Lite month manifest with a resource.
        let litePath = MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(
            basePath: basePath, year: 2024, month: 3
        )
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xBB]), fileName: "b.jpg")
        )
        _ = try await store.flushToRemote()

        // The data directory 2024/03 was created by loadOrCreate. Remove all data files
        // to simulate external deletion of the data directory contents while the sqlite survives.
        // For this test, delete the seeded data directory so the list call gets not-found.
        // We'll use a fresh client with the same manifest but no data directory.
        let client2 = InMemoryRemoteStorageClient()
        await client2.seedFile(path: litePath, data: try await client.fileData(path: litePath) ?? Data())
        // Seed .watermelon/months directory so the manifest is discoverable, but NOT 2024/03.

        let service = RemoteIndexSyncService()
        // Prime the cache with the month so verifyMonth has something to verify.
        let digests = try await service.scanManifestDigests(
            client: client2, basePath: basePath, layout: .lite
        )
        XCTAssertEqual(digests.count, 1, "scan should find the Lite manifest")

        // verifyMonth should succeed — missing data dir treated as empty, stale entries pruned.
        try await service.verifyMonth(
            client: client2,
            basePath: basePath,
            month: LibraryMonthKey(year: 2024, month: 3),
            layout: .lite,
            assertOwnership: { true }
        )
    }

    // MARK: - Lease-confidence gate

    func testLeaseGatePassesWhileConfident() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        try await LiteWriteGuard.assertLeaseConfidence(session, now: now)   // must not throw
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterConfidenceLoss() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await session.lock.noteConfidenceLoss(.foregroundBackgroundTransition)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterLifecycleSuspend() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await session.lock.noteConfidenceLoss(.appLifecycleSuspend)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterRefreshFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        _ = await session.lock.refresh(now: now)   // degrades confidence
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterMissedConfidenceWindow() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let stale = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: stale)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterListFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        _ = await session.lock.assertStillOwned(mode: .foreground, now: now)   // LIST fault drops confidence
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateNoOpWhenSessionNil() async throws {
        try await LiteWriteGuard.assertLeaseConfidence(nil)   // V1 / read path: no gating
    }

    // MARK: - Flush ownership gate

    func testFlushOwnershipGatePassesWhenOwned() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)   // must not throw
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateTripsOnForeignWriter() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: now)   // fresh foreign
        await assertThrowsLiteError(.ownershipLost) {
            try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateTripsWhenOwnLockDeleted() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: now)
        await client.removeLock(basePath: basePath, writerID: writerID)
        await assertThrowsLiteError(.ownershipLost) {
            try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateTripsOnListFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.ownershipLost) {
            try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    // MARK: - Session release / refresh-stop

    func testStopAndReleaseIsIdempotent() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: Date())
        await session.stopAndRelease()
        await session.stopAndRelease()   // second call must be a safe no-op
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked)
    }

    // MARK: - Executor release lifecycle

    func testExecuteZeroAssetSuccessReleasesLease() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        let prepared = makePreparedRun(
            client: client, monthPlans: [], totalAssetCount: 0, session: plan.session
        )
        let result = try await makeExecutor().execute(
            preparedRun: prepared,
            profile: makeProfile(writerID: writerID),
            workerCountOverride: nil,
            iCloudPhotoBackupMode: .disable,
            eventStream: BackupEventStream()
        )
        XCTAssertEqual(result.total, 0)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "zero-asset success must release the Lite lease")
    }

    func testExecuteZeroAssetReleasesLockBeforeFinished() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRejectDeleteAfterDisconnect(true)
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        let prepared = makePreparedRun(
            client: client, monthPlans: [], totalAssetCount: 0, session: plan.session
        )
        let eventStream = BackupEventStream()

        // Consumer that disconnects the client when .finished arrives, simulating
        // BSC clearing state. If the lock is not yet released, the subsequent
        // stopAndRelease delete fails because the client is disconnected and
        // rejectDeleteAfterDisconnect is true.
        Task {
            for await event in eventStream.stream {
                if case .finished = event {
                    await client.disconnect()
                    break
                }
            }
        }

        _ = try await makeExecutor().execute(
            preparedRun: prepared,
            profile: makeProfile(writerID: writerID),
            workerCountOverride: nil,
            iCloudPhotoBackupMode: .disable,
            eventStream: eventStream
        )

        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "zero-asset path must release lock before emitting .finished")
    }

    func testExecuteExecutionErrorReleasesLease() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        // The first month load createDirectory now blows up, surfacing an execution error.
        await client.enqueueCreateDirectoryError(RemoteErrorFixtures.terminal)
        let prepared = makePreparedRun(
            client: client,
            monthPlans: [MonthWorkItem(month: LibraryMonthKey(year: 2024, month: 3), assetLocalIdentifiers: ["a"], estimatedBytes: 0)],
            totalAssetCount: 1,
            session: plan.session
        )
        do {
            _ = try await makeExecutor().execute(
                preparedRun: prepared,
                profile: makeProfile(writerID: writerID),
                workerCountOverride: nil,
                iCloudPhotoBackupMode: .disable,
                eventStream: BackupEventStream()
            )
            XCTFail("execution should surface the createDirectory fault")
        } catch {
            // expected
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "an execution error must release the Lite lease")
    }

    func testForegroundLeaseReleasedWhileClientStillConnected() async throws {
        // A client that rejects delete once disconnected (like real WebDAV/SFTP): the lease must be
        // released before the executor disconnects it, otherwise the lock leaks on the remote.
        let client = InMemoryRemoteStorageClient()
        await client.setRejectDeleteAfterDisconnect(true)
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        let prepared = makePreparedRun(
            client: client, monthPlans: [], totalAssetCount: 0, session: plan.session
        )
        _ = try await makeExecutor().execute(
            preparedRun: prepared,
            profile: makeProfile(writerID: writerID),
            workerCountOverride: nil,
            iCloudPhotoBackupMode: .disable,
            eventStream: BackupEventStream()
        )
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "lease must be released while the client is connected (delete-after-disconnect leaks the lock)")
        let connected = await client.connected
        XCTAssertFalse(connected, "execute must still disconnect the client after releasing the lease")
    }

    // MARK: - Background routing (skip / no-takeover / flush interval)

    func testBackgroundProceedFreshAcquiresAndCommits() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let outcome = await LiteRepoGateway.prepareBackgroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )
        guard case .proceed(let plan) = outcome else {
            return XCTFail("fresh background repo should proceed")
        }
        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData)
        await plan.session.stopAndRelease()
    }

    func testBackgroundSkipsOnFreshForeignLockWithoutTakeover() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let other = newWriterID()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: now)
        let outcome = await LiteRepoGateway.prepareBackgroundWrite(
            client: client, basePath: basePath, writerID: newWriterID(), now: now
        )
        guard case .skip = outcome else { return XCTFail("a fresh foreign lock must make background skip") }
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(foreignStillThere, "background must not take over a foreign lock")
    }

    func testBackgroundSkipsStaleForeignLockWithoutTakeover() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let other = newWriterID()
        let stale = now.addingTimeInterval(-(WriteLockService.expiry + 60))
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale)
        let outcome = await LiteRepoGateway.prepareBackgroundWrite(
            client: client, basePath: basePath, writerID: newWriterID(), now: now
        )
        guard case .skip = outcome else { return XCTFail("background never reclaims a stranger's stale lock") }
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(foreignStillThere, "background must not delete a stale foreign lock")
    }

    func testBackgroundSkipsV1MigrateWithoutWrites() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedV1Manifest(client)
        let outcome = await LiteRepoGateway.prepareBackgroundWrite(
            client: client, basePath: basePath, writerID: newWriterID()
        )
        guard case .skip = outcome else { return XCTFail(".v1Migrate must be skipped in background") }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testBackgroundFlushIntervalPreserved() {
        XCTAssertEqual(BackgroundBackupRunner.flushInterval, 10)
    }

    // MARK: - Flag-off V1 differential

    func testFlagOffReloadRejectsWatermelonRepo() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon")
        let service = try makePrepService(liteRepoEnabled: false)
        do {
            _ = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))
            XCTFail("flag-off reload must keep the V1 compatibility refusal of a V2 repo")
        } catch let error as BackupCompatibilityError {
            if case .remoteFormatUnsupported = error {} else {
                XCTFail("unexpected compatibility error: \(error)")
            }
        }
    }

    func testFlagOffV1RepoStaysV1AndDoesNotMigrate() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()
        let service = try makePrepService(liteRepoEnabled: false)

        let digest = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))
        XCTAssertEqual(digest.resourceCount, 1, "flag-off reads the V1 month as-is")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "flag-off must never create a Lite version.json")
        let monthsListed = await client.listedPaths.contains(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        XCTAssertFalse(monthsListed, "flag-off must not touch the Lite months directory")
    }

    func testFlagOnReloadAcceptsLiteRepoWithoutLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        let service = try makePrepService(liteRepoEnabled: true)

        let digest = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))
        XCTAssertEqual(digest.assetCount, 0)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "reload routing must not acquire a lock")
    }

    func testMakeMaintenancePlanFlagOffIsV1LockFree() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let service = try makePrepService(liteRepoEnabled: false)
        let plan = try await service.makeMaintenancePlan(client: client, profile: makeProfile(writerID: newWriterID()))
        XCTAssertEqual(plan.layout, .v1)
        XCTAssertNil(plan.session)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testMakeMaintenancePlanFlagOnLiteAcquiresLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()
        let service = try makePrepService(liteRepoEnabled: true)
        let plan = try await service.makeMaintenancePlan(client: client, profile: makeProfile(writerID: writerID))
        XCTAssertEqual(plan.layout, .lite)
        XCTAssertNotNil(plan.session)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        await plan.session?.stopAndRelease()
    }

    // MARK: - Data naming unchanged

    func testLiteLayoutKeepsYearMonthDataPaths() {
        let resource = TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01]), fileName: "IMG_0001.JPG")
        XCTAssertEqual(resource.remoteRelativePath, "2024/03/IMG_0001.JPG")
        XCTAssertEqual(
            MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3),
            "/photos/.watermelon/months/2024-03.sqlite"
        )
    }

    func testResolveNextAvailableNameUnchanged() {
        let next = RemoteFileNaming.resolveNextAvailableName(
            baseName: "IMG_0001.JPG", occupiedNames: ["IMG_0001.JPG"]
        )
        XCTAssertEqual(next, "IMG_0001_1.JPG", "Lite cutover must not redesign data naming")
    }

    // MARK: - Local-volume fresh backup artifacts (real on-disk)

    func testLocalVolumeFreshBackupProducesLiteArtifacts() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-localvol-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let client = DiskBackedRemoteStorageClient(rootURL: root)
        let writerID = newWriterID()

        // Fresh route: lock + version.json committed on a real volume.
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: writerID
        )

        // A month: manifest under .watermelon/months and a data resource under <YYYY>/<MM>.
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: { true }
        )
        let dataURL = root.appendingPathComponent("photos/IMG_0001.JPG")
        try FileManager.default.createDirectory(at: dataURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data([0xDE, 0xAD]).write(to: dataURL)
        try await client.upload(
            localURL: dataURL,
            remotePath: "\(basePath)/2024/03/IMG_0001.JPG",
            respectTaskCancellation: false,
            onProgress: nil
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01, 0x02]), fileName: "IMG_0001.JPG")
        )
        _ = try await store.flushToRemote()

        let fm = FileManager.default
        func exists(_ rel: String) -> Bool { fm.fileExists(atPath: root.appendingPathComponent(rel).path) }
        XCTAssertTrue(exists("photos/.watermelon/version.json"), "version.json")
        XCTAssertTrue(exists("photos/.watermelon/locks/\(writerID).lock"), "locks/<writerID>.lock")
        XCTAssertTrue(exists("photos/.watermelon/months/2024-03.sqlite"), "months/<YYYY-MM>.sqlite")
        XCTAssertTrue(exists("photos/2024/03/IMG_0001.JPG"), "photo resource under <YYYY>/<MM>/")

        await plan.session.stopAndRelease()
        XCTAssertFalse(exists("photos/.watermelon/locks/\(writerID).lock"), "release removes the lock")
    }

    // MARK: - Helpers

    private func acquiredSession(
        client: InMemoryRemoteStorageClient,
        writerID: String? = nil,
        now: Date
    ) async throws -> LiteWriteSession {
        let id = writerID ?? newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client, basePath: basePath, writerID: id, now: now
        )
        return plan.session
    }

    private func makePreparedRun(
        client: any RemoteStorageClientProtocol,
        monthPlans: [MonthWorkItem],
        totalAssetCount: Int,
        session: LiteWriteSession?
    ) -> BackupPreparedRun {
        BackupPreparedRun(
            initialClient: client,
            snapshotSeedLookup: nil,
            monthPlans: monthPlans,
            workerCount: 1,
            connectionPoolSize: 1,
            totalAssetCount: totalAssetCount,
            makeClient: { client },
            manifestLayout: .lite,
            liteSession: session
        )
    }

    private func makeExecutor() throws -> BackupParallelExecutor {
        let dbm = try makeDatabaseManager()
        let remoteIndexService = RemoteIndexSyncService()
        let repo = ContentHashIndexRepository(databaseManager: dbm)
        let assetProcessor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: repo,
            remoteIndexService: remoteIndexService
        )
        return BackupParallelExecutor(
            hashIndexRepository: repo,
            assetProcessor: assetProcessor,
            remoteIndexService: remoteIndexService
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

    private func assertThrowsLiteError(
        _ expected: LiteRepoError,
        file: StaticString = #filePath,
        line: UInt = #line,
        _ body: () async throws -> Void
    ) async {
        do {
            try await body()
            XCTFail("expected LiteRepoError.\(expected)", file: file, line: line)
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, expected, file: file, line: line)
        } catch {
            XCTFail("expected LiteRepoError.\(expected) but got \(error)", file: file, line: line)
        }
    }

    // MARK: - Unanchored optimistic cache eviction (R02 Fix A)

    private func makeEmptyMonthSqliteData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-month-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        let dbURL = tmpDir.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(queue)
        try queue.close()
        let data = try Data(contentsOf: dbURL)
        try? FileManager.default.removeItem(at: tmpDir)
        return data
    }

    func testUnanchoredCacheEvictedDuringNonFastPathSync() async throws {
        let client = InMemoryRemoteStorageClient()
        let service = RemoteIndexSyncService()
        let profile = makeProfile(writerID: nil)

        try await seedCommittedVersion(client)
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))

        let monthA = LibraryMonthKey(year: 2024, month: 1)
        let monthB = LibraryMonthKey(year: 2024, month: 2)
        let monthC = LibraryMonthKey(year: 2024, month: 3)

        let sqliteData = try makeEmptyMonthSqliteData()
        await client.seedFile(
            path: RepoLayoutLite.monthPath(basePath: basePath, month: monthA),
            data: sqliteData,
            modificationDate: Date(timeIntervalSince1970: 1000)
        )

        // First sync: establishes previous digests with month A.
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)

        // Optimistic entry for month B (no remote sqlite — unanchored).
        service.upsertCachedResource(RemoteManifestResource(
            year: monthB.year, month: monthB.month,
            fileName: "test.jpg",
            contentHash: Data([0x01]),
            fileSize: 100,
            resourceType: 0,
            creationDateMs: nil,
            backedUpAtMs: 1000
        ))
        XCTAssertTrue(service.allKnownMonths().contains(monthB),
                       "optimistic upsert should add month B to cache")

        // Add month C on remote — forces non-fast-path (changedMonths non-empty).
        await client.seedFile(
            path: RepoLayoutLite.monthPath(basePath: basePath, month: monthC),
            data: sqliteData,
            modificationDate: Date(timeIntervalSince1970: 2000)
        )

        // Second sync: must evict unanchored month B even though the fast path is skipped.
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)

        let months = service.allKnownMonths()
        XCTAssertFalse(months.contains(monthB),
                       "unanchored optimistic month B must be evicted when a real month changes")
    }

    func testUnanchoredCacheEvictedOnUnchangedFastPathSync() async throws {
        let client = InMemoryRemoteStorageClient()
        let service = RemoteIndexSyncService()
        let profile = makeProfile(writerID: nil)

        try await seedCommittedVersion(client)
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))

        let monthA = LibraryMonthKey(year: 2024, month: 1)
        let monthB = LibraryMonthKey(year: 2024, month: 2)

        let sqliteData = try makeEmptyMonthSqliteData()
        await client.seedFile(
            path: RepoLayoutLite.monthPath(basePath: basePath, month: monthA),
            data: sqliteData,
            modificationDate: Date(timeIntervalSince1970: 1000)
        )

        // First sync: establishes previous digests with month A.
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)

        // Optimistic entry for month B (no remote sqlite — unanchored).
        service.upsertCachedResource(RemoteManifestResource(
            year: monthB.year, month: monthB.month,
            fileName: "test.jpg",
            contentHash: Data([0x01]),
            fileSize: 100,
            resourceType: 0,
            creationDateMs: nil,
            backedUpAtMs: 1000
        ))
        XCTAssertTrue(service.allKnownMonths().contains(monthB),
                       "optimistic upsert should add month B to cache")

        // Second sync: remote unchanged → fast path. Must still evict month B.
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)

        XCTAssertFalse(service.allKnownMonths().contains(monthB),
                       "unanchored optimistic month B must be evicted on unchanged fast-path sync")
    }
}
