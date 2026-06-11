import XCTest
import GRDB
@testable import Watermelon

// Step 6A (P06-PrepareRunCutover): always-on Lite prepare-run routing. Exercises the gateway,
// lease/ownership gates, executor release lifecycle, read/verify routing, and a real on-disk fresh-backup
// artifact layout.
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

    private func seedV1Manifest(_ client: InMemoryRemoteStorageClient) async throws {
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await store.flushToRemote()
    }

    // MARK: - Foreground write routing (fresh / current / version / layout / release)

    func testForegroundFreshAcquiresLockCommitsVersionAndUsesLiteLayout() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        let scratchGone = await client.fileData(path: scratchPath)
        XCTAssertNil(scratchGone, ".current foreground prepare must clean months scratch under its lock")
        await plan.session.stopAndRelease()
    }

    func testForegroundV1MigrateRetainsOldV1ManifestAfterCommit() async throws {
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
            client: client,
            lockClient: client, basePath: basePath, writerID: newWriterID()
        )
        let oldV1Manifest = await client.fileData(path: v1ManifestPath)
        let liteManifest = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNotNil(oldV1Manifest, "after migrating + committing, the old V1 manifest is retained")
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
                client: client,
            lockClient: client, basePath: self.basePath, writerID: self.newWriterID()
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
                client: client,
            lockClient: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testForegroundProbeFaultFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // base-path probe blinks

        await assertThrowsLiteError(.probeFault(.retryable)) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: self.newWriterID()
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
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID, now: now
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
                client: client,
            lockClient: client, basePath: self.basePath, writerID: nil
            )
        }
    }

    func testForegroundVersionCommitFailureReleasesLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a prep error after lock acquire must release the lock")
    }

    // MARK: - Version-commit cancellation passthrough (M01 — commitVersionUnderLock)

    // Fresh-init: a cancelled version.json publish must surface as cancellation, never relabeled as
    // versionCommitFailed. The lock is still released (same as a non-cancellation commit failure).
    func testForegroundFreshVersionCommitCancellationIsNotVersionCommitFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.cancelled)   // publish move temp→version.json cancelled
        let writerID = newWriterID()

        do {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: basePath, writerID: writerID
            )
            XCTFail("a cancelled version commit must surface as cancellation, not versionCommitFailed")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled,
                           "cancellation must not be wrapped as versionCommitFailed")
            XCTAssertNil(error as? LiteRepoError, "cancellation must not surface as a LiteRepoError")
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a cancelled commit must still release the lock")
    }

    // Malformed-version repair: a cancelled repair commit must also surface as cancellation. The publish
    // takes the backup path (the malformed final exists), so both moves are scripted cancelled.
    func testForegroundMalformedVersionRepairCommitCancellationIsNotVersionCommitFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        await client.enqueueMoveError(RemoteErrorFixtures.cancelled)   // direct replace move cancelled
        await client.enqueueMoveError(RemoteErrorFixtures.cancelled)   // backup move cancelled
        let writerID = newWriterID()

        do {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: basePath, writerID: writerID
            )
            XCTFail("a cancelled malformed-version repair commit must surface as cancellation")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled,
                           "cancellation must not be wrapped as versionCommitFailed")
            XCTAssertNil(error as? LiteRepoError, "cancellation must not surface as a LiteRepoError")
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a cancelled repair commit must still release the lock")
    }

    // MARK: - Malformed-version recovery routing (P06 Phase 4)

    private func seedMalformedVersion(_ client: InMemoryRemoteStorageClient) async {
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: Data("not json".utf8))
    }

    func testForegroundMalformedVersionRepairsUnderLockAndUsesLiteLayout() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, "malformed-version repair must hold the foreground lock")

        // version.json rewritten to canonical bytes under the lock.
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        XCTAssertEqual(manifest.layout, VersionManifestLite.layout)
        XCTAssertEqual(manifest.createdBy, writerID)
        await plan.session.stopAndRelease()
    }

    func testForegroundMalformedVersionRepairFailureReleasesLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        // Publish move fails twice (direct replace, then the backup move) → repair commit fails.
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a failed malformed-version repair must release the lock")
    }

    func testMaintenanceMalformedVersionRepairsUnderLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        XCTAssertNotNil(plan.session)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        XCTAssertEqual(manifest.layout, VersionManifestLite.layout)
        await plan.session?.stopAndRelease()
    }

    func testResolveReadLayoutMalformedVersionThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: self.basePath)
        }
    }

    func testBackgroundRepairsMalformedVersionUnderLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        guard case .proceed(let plan) = outcome else { return XCTFail("malformed version should be repaired in background") }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        await plan.session.stopAndRelease()
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

    func testResolveReadLayoutV1ThrowsUntilWriterMigrates() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedV1Manifest(client)
        await assertThrowsLiteError(.repoMaintenanceUnavailable) {
            _ = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: self.basePath)
        }
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        let scratchGone = await client.fileData(path: scratchPath)
        XCTAssertNil(scratchGone, ".current maintenance still cleans whitelisted scratch under its lock")
        await plan.session?.stopAndRelease()
    }

    func testMaintenanceFreshRejectsWithoutWrites() async throws {
        let client = InMemoryRemoteStorageClient()
        // Fresh route: month scratch under a `.watermelon` dir with no committed version.json, no V1
        // manifest, and no Lite month sqlite. Verify never initializes, so a `.fresh` repo is rejected
        // without a lock, a version commit, or any control-tree write.
        let scratchPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath) + "/manifest_x.tmp"
        await client.seedFile(path: scratchPath, data: Data([0x01]))
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoMaintenanceUnavailable) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, ".fresh maintenance must not acquire a lock")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, ".fresh maintenance must not commit version.json")
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(
            uploaded.contains { $0.contains("/.watermelon/") },
            ".fresh maintenance must leave no control-tree bytes behind"
        )
        let scratchSurvives = await client.fileData(path: scratchPath)
        XCTAssertNotNil(scratchSurvives, ".fresh maintenance must not clean — there is no committed repo to maintain")
    }

    func testMaintenanceV1MigrateMigratesUnderLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedV1Manifest(client)
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareMaintenance(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, ".v1Migrate maintenance must hold the migration lock")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData, ".v1Migrate maintenance must commit version.json after migrating")
        await plan.session?.stopAndRelease()
    }

    // Maintenance `.current` reclassifies under the lock and, if the under-lock state is no longer
    // current, releases the lock and fails closed rather than maintaining a drifted repo.
    func testMaintenanceCurrentReleasesLockOnUnderLockMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        // Initial classify reads a committed version; the under-lock reclassify finds none (the file was
        // never seeded, only scripted for the first read) → `.fresh` → mismatch.
        let committed = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed")
        )
        await client.enqueueDownloadData(committed)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "maintenance .current must release the lock when the under-lock state is no longer current")
    }

    // Maintenance `.malformedVersion` reclassifies under the lock and releases it on a probe fault.
    func testMaintenanceMalformedVersionReleasesLockOnUnderLockFault() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()
        let ownLockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        await client.setOnDownload { path in
            if path == ownLockPath {
                await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
            }
        }

        await assertThrowsLiteError(.probeFault(.retryable)) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a probe fault under the lock must release the maintenance lock")
    }

    func testMaintenanceDamagedThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
    }

    func testVerifyMonthFailsClosedWhenOwnershipLostBeforeFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        // Seed a Lite month manifest containing a phantom asset (no links) so reconcile must delete it.
        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: {}
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
            )
            XCTFail("verify must fail closed when ownership cannot be re-asserted before flush")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testOwnedVerifyMonthFailsClosedWhenManifestIsMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        let service = RemoteIndexSyncService()

        do {
            try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2024, month: 3),
                layout: .lite,
                assertOwnership: {}
            )
            XCTFail("owned verify must fail when the Lite month manifest is missing")
        } catch let error as LiteRepoError {
            XCTFail("owned verify should not report ownership loss while ownership assertion is true: \(error)")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
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
            assertOwnership: {}
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
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
            assertOwnership: {}
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
            )
            XCTFail("a clean Lite seeded load must fail closed when ownership is lost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testLoadOrCreateV1ReconcileFlushUngatedByDefault() async throws {
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
            assertOwnership: {}
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
            assertOwnership: {}
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
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
                assertOwnership: { throw LiteRepoError.ownershipLost }
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
            assertOwnership: {}
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
            assertOwnership: {}
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
                assertOwnership: {}
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
            assertOwnership: {}
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
        await client2.seedFile(path: litePath, data: await client.fileData(path: litePath) ?? Data())
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
            assertOwnership: {}
        )
    }

    // MARK: - Transient fault / destructive-prune gate (P05 Phase 3)

    private func dataEntry(_ path: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: path,
            name: (path as NSString).lastPathComponent,
            isDirectory: false,
            size: 1,
            creationDate: nil,
            modificationDate: nil
        )
    }

    // A transient share-down LIST during a non-empty Lite seeded load must surface, never prune to empty.
    func testLoadSeededLiteTransientListFailureSurfacesAndDoesNotFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAA]), fileName: "a.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a transient share-down LIST must surface, not prune a non-empty seed to empty")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "a transient probe failure must not flush an emptied manifest")
    }

    // First LIST returns an empty view; the confirmation LIST faults → cannot confirm → no prune, no flush.
    func testLoadSeededLiteUnconfirmedEmptyListingDoesNotPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.enqueueListResult([])
        await client.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        XCTAssertNotNil(store.findByFileName("b.jpg"), "an unconfirmed empty listing must not prune the seed resource")
        XCTAssertFalse(store.dirty, "skipping the destructive prune must leave the manifest clean (no flush)")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no manifest flush after a skipped destructive prune")
    }

    // First LIST returns empty, but the confirmation LIST reads the real (non-empty) tree → disagree → skip.
    func testLoadSeededLiteDisagreeingConfirmationDoesNotPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.seedFile(path: "\(basePath)/2024/03/b.jpg", data: Data([0xCC]))
        await client.enqueueListResult([])   // first LIST: a transient empty view
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        XCTAssertNotNil(store.findByFileName("b.jpg"), "a disagreeing confirmation must not prune the present resource")
        XCTAssertFalse(store.dirty)
    }

    // A large-ratio (>= 50%) prune that the confirmation LIST does not reproduce must be skipped.
    func testLoadSeededLiteLargeRatioPruneRequiresConfirmation() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        let names = ["a.jpg", "b.jpg", "c.jpg", "d.jpg"]
        for name in names {
            await client.seedFile(path: "\(basePath)/2024/03/\(name)", data: Data([0x01]))
        }
        // First LIST shows only a.jpg → would prune 3/4. Confirmation reads the real tree (all four) → skip.
        await client.enqueueListResult([dataEntry("\(basePath)/2024/03/a.jpg")])
        let seed = MonthManifestStore.Seed(
            resources: names.enumerated().map { idx, name in
                TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([UInt8(idx)]), fileName: name)
            },
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        for name in names {
            XCTAssertNotNil(store.findByFileName(name), "\(name) must survive an unconfirmed large-ratio prune")
        }
        XCTAssertFalse(store.dirty)
    }

    func testLoadSeededLitePruneRatioIgnoresOrphanListingNames() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        let manifestNames = ["a.jpg", "b.jpg", "c.jpg", "d.jpg"]
        let orphanNames = ["orphan1.dat", "orphan2.dat", "orphan3.dat", "orphan4.dat"]
        for name in manifestNames + orphanNames {
            await client.seedFile(path: "\(basePath)/2024/03/\(name)", data: Data([0x01]))
        }
        await client.enqueueListResult(
            ["a.jpg"].map { dataEntry("\(basePath)/2024/03/\($0)") }
                + orphanNames.map { dataEntry("\(basePath)/2024/03/\($0)") }
        )
        let seed = MonthManifestStore.Seed(
            resources: manifestNames.enumerated().map { idx, name in
                TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([UInt8(idx)]), fileName: name)
            },
            assets: [],
            assetResourceLinks: []
        )

        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )

        for name in manifestNames {
            XCTAssertNotNil(store.findByFileName(name), "\(name) must survive an orphan-inflated partial listing")
        }
        XCTAssertFalse(store.dirty)
    }

    // Unseeded loadOrCreate: a transient probe fault must surface and must not be read as a missing dir.
    func testLoadOrCreateLiteTransientListFailureSurfacesAndDoesNotCreateDir() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a transient data-dir LIST fault must surface")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
        let created = await client.createdDirectories
        XCTAssertFalse(created.contains("/photos/2024/03"),
                       "a probe fault must not be read as a missing dir and create it")
    }

    // verifyMonth(.lite): a transient data-dir LIST fault must surface, never flush a pruned manifest.
    func testVerifyMonthLiteTransientListFailureDoesNotFlushPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: {}
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xBB]), fileName: "b.jpg")
        )
        _ = try await store.flushToRemote()
        await client.seedFile(path: "\(basePath)/2024/03/b.jpg", data: Data([0xBB]))

        let uploadsBefore = await client.uploadedPaths.count
        await client.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: LibraryMonthKey(year: 2024, month: 3),
                layout: .lite, assertOwnership: {}
            )
            XCTFail("a transient data-dir LIST fault during verify must surface, not flush a pruned manifest")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
        let uploadsAfter = await client.uploadedPaths.count
        XCTAssertEqual(uploadsAfter, uploadsBefore, "verify must not write the manifest after a transient probe fault")
    }

    // `.watermelon/months` digest scan: empty only for a true missing dir; any other fault must surface.
    func testScanLiteManifestDigestsReturnsEmptyOnlyForTrueNotFound() async throws {
        let client = InMemoryRemoteStorageClient()
        let service = RemoteIndexSyncService()
        let digests = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertTrue(digests.isEmpty, "a genuinely absent months directory scans as zero months")
    }

    func testScanLiteManifestDigestsThrowsOnTransientListFault() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.smbBadNetworkName)
        let service = RemoteIndexSyncService()
        do {
            _ = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
            XCTFail("a transient months-dir LIST fault must surface, not read as zero months")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
    }

    // MARK: - Lease-confidence gate

    func testLeaseGatePassesWhileConfident() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        try await LiteWriteGuard.assertLeaseConfidence(session, now: Date())   // must not throw
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterConfidenceLoss() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await session.lock.noteConfidenceLoss()
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testLeaseGateRecoversAfterTransientRefreshFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let faultTime = Date()

        // A transient refresh fault degrades confidence; the lease gate re-proves ownership.
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        _ = await session.lock.refresh(now: faultTime)
        try await LiteWriteGuard.assertLeaseConfidence(session, now: faultTime)

        // A successful in-window refresh re-proves ownership; the gate can pass again.
        let later = faultTime.addingTimeInterval(60)
        let refresh = await session.lock.refresh(now: later)
        XCTAssertEqual(refresh, .refreshed)
        try await LiteWriteGuard.assertLeaseConfidence(session, now: later)   // must not throw
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterRefreshFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let faultTime = Date().addingTimeInterval(1)
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        _ = await session.lock.refresh(now: faultTime)   // degrades confidence
        try await LiteWriteGuard.assertLeaseConfidence(session, now: faultTime)
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterMissedConfidenceWindow() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let stale = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        try await LiteWriteGuard.assertLeaseConfidence(session, now: stale)
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterListFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        _ = await session.lock.assertStillOwned(mode: .foreground, now: now)   // LIST fault drops confidence
        try await LiteWriteGuard.assertLeaseConfidence(session, now: now)
        await session.stopAndRelease()
    }

    func testLeaseGateNoOpWhenSessionNil() async throws {
        try await LiteWriteGuard.assertLeaseConfidence(nil)   // no write session: no gating
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
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    // A transient *confirmation* LIST fault during the flush re-assertion trips the gate for this attempt
    // but must not delete the own lock — so it is recoverable, not a permanent loss.
    func testFlushGateConfirmationFaultRetainsOwnLockAndRecovers() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: now)

        // Initial LIST sees our lock; write succeeds; the confirmation LIST faults transiently.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: writerID, modificationDate: now)
        ])
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)
        }
        let lockStillThere = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(lockStillThere, "a transient confirmation fault must not delete the own lock")

        // The lock survived, so a subsequent owned re-assert (clean LIST) passes.
        try await LiteWriteGuard.assertOwnedBeforeFlush(session, now: now)   // must not throw
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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

    func testInlineFinalizerFailureContributesToExecutionResult() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        let month = LibraryMonthKey(year: 2024, month: 3)
        let prepared = makePreparedRun(
            client: client,
            monthPlans: [MonthWorkItem(month: month, assetLocalIdentifiers: ["missing-asset"], estimatedBytes: 0)],
            totalAssetCount: 1,
            session: plan.session
        )

        let result = try await makeExecutor().execute(
            preparedRun: prepared,
            profile: makeProfile(writerID: writerID),
            workerCountOverride: nil,
            iCloudPhotoBackupMode: .disable,
            eventStream: BackupEventStream(),
            onMonthUploaded: { _, _ in .failed("verify failed") }
        )

        XCTAssertEqual(result.failed, 1, "inline verify/download failure must make the run partial")
        XCTAssertEqual(result.total, 1)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked)
    }

    func testForegroundLeaseReleasedWhileClientStillConnected() async throws {
        // A client that rejects delete once disconnected (like real WebDAV/SFTP): the lease must be
        // released before the executor disconnects it, otherwise the lock leaks on the remote.
        let client = InMemoryRemoteStorageClient()
        await client.setRejectDeleteAfterDisconnect(true)
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
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
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: newWriterID(), now: now
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
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: newWriterID(), now: now
        )
        guard case .skip = outcome else { return XCTFail("background never reclaims a stranger's stale lock") }
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(foreignStillThere, "background must not delete a stale foreign lock")
    }

    func testBackgroundInitialProbeCancellationIsNotSkip() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.cancelled)

        do {
            _ = try await LiteRepoGateway.prepareBackgroundWrite(
                client: client,
            lockClient: client,
                basePath: basePath,
                writerID: newWriterID()
            )
            XCTFail("background cancellation must surface, not return .skip")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
            XCTAssertNil(error as? LiteRepoError)
        }
    }

    func testBackgroundVersionCommitCancellationIsNotSkip() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.cancelled)
        let writerID = newWriterID()

        do {
            _ = try await LiteRepoGateway.prepareBackgroundWrite(
                client: client,
            lockClient: client,
                basePath: basePath,
                writerID: writerID
            )
            XCTFail("background commit cancellation must surface, not return .skip")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
            XCTAssertNil(error as? LiteRepoError)
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a cancelled background commit must still release the lock")
    }

    func testBackgroundUnderLockProbeFaultSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListResult([])                            // initial base probe: .fresh
        await client.enqueueListResult([])                            // acquire: locks list
        await client.enqueueListResult([])                            // acquire confirmation
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // under-lock base probe
        let writerID = newWriterID()

        await assertThrowsLiteError(.probeFault(.retryable)) {
            _ = try await LiteRepoGateway.prepareBackgroundWrite(
                client: client,
            lockClient: client,
                basePath: basePath,
                writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "under-lock probe fault must release the acquired background lock")
    }

    func testBackgroundV1MigrateMigratesWhenLockAcquired() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedV1Manifest(client)
        let writerID = newWriterID()
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        guard case .proceed(let plan) = outcome else { return XCTFail(".v1Migrate should migrate in background when the lock is acquired") }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        await plan.session.stopAndRelease()
    }

    // Initial probe reads `.fresh`, but under the lock V1 data is visible → background migrates rather
    // than initializing Lite over a V1 tree.
    func testBackgroundMigratesWhenFreshBecomesV1MigrateUnderLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedV1Manifest(client)                 // the tree is actually V1...
        await client.enqueueListResult([])           // ...but the initial base probe sees it empty → .fresh
        let writerID = newWriterID()

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        guard case .proceed(let plan) = outcome else {
            return XCTFail("a fresh probe that reclassifies to .v1Migrate under the lock must migrate")
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData)
        await plan.session.stopAndRelease()
    }

    func testBackgroundFlushIntervalPreserved() {
        XCTAssertEqual(BackgroundBackupRunner.flushInterval, 10)
    }

    // MARK: - Read/maintenance routing

    func testReloadToleratesBareWatermelonMarkerAsFreshLite() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon")
        let service = try makePrepService()
        let digest = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))
        XCTAssertEqual(digest.resourceCount, 0, "a bare marker over an empty V1 tree reads as empty, not rejected")
    }

    func testReloadV1RepoMigratesThenReadsLite() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1 = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        try v1.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await v1.flushToRemote()
        let service = try makePrepService()

        let digest = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: newWriterID()))
        XCTAssertEqual(digest.resourceCount, 1, "reload migrates V1 then reads the Lite month")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData, "reload must commit version.json after migrating")
        let liteData = await client.fileData(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNotNil(liteData, "reload must relocate the V1 month manifest into Lite")
    }

    func testReloadAcceptsLiteRepoWithoutLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        let service = try makePrepService()

        let digest = try await service.reloadRemoteIndex(client: client, profile: makeProfile(writerID: nil))
        XCTAssertEqual(digest.assetCount, 0)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "reload routing must not acquire a lock")
    }

    // MARK: - Verify-sweep format-probe dedup (M04)

    // Reusing the maintenance plan's already-resolved layout for the index sync must not run a second
    // pure-read classify (which would re-download version.json).
    func testReloadReusingMaintenancePlanSkipsSecondFormatProbe() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        let writerID = newWriterID()
        let service = try makePrepService()
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)

        let plan = try await service.makeMaintenancePlan(client: client, profile: makeProfile(writerID: writerID))
        let probesAfterPlan = (await client.downloadAttemptPaths).filter { $0 == versionPath }.count
        XCTAssertGreaterThanOrEqual(probesAfterPlan, 2, "the maintenance plan classifies twice (initial + under-lock)")

        let digest = try await service.reloadRemoteIndex(
            client: client, profile: makeProfile(writerID: writerID), reusing: plan
        )
        let probesAfterReload = (await client.downloadAttemptPaths).filter { $0 == versionPath }.count

        XCTAssertEqual(probesAfterReload, probesAfterPlan,
                       "reusing the maintenance plan must not run a second pure-read format classify")
        XCTAssertEqual(plan.layout, .lite, "the plan resolved the Lite layout the sync reused")
        XCTAssertEqual(digest.assetCount, 0)
        await plan.session?.stopAndRelease()
    }

    func testMakeMaintenancePlanAcquiresLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()
        let service = try makePrepService()
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
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )

        // A month: manifest under .watermelon/months and a data resource under <YYYY>/<MM>.
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: {}
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

    // MARK: - Writer ID lazy backfill on the prepare path (P08 / F14)

    // Inserts a pre-v3-style saved profile whose writerID column is NULL, returning (dbm, profile).
    private func insertNullWriterIDProfile() throws -> (DatabaseManager, ServerProfileRecord) {
        let dbm = try makeDatabaseManager()
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
        let profile = try XCTUnwrap(try dbm.read { db in try ServerProfileRecord.fetchOne(db, key: id) })
        XCTAssertNil(profile.writerID, "precondition: saved profile carries no writer ID")
        return (dbm, profile)
    }

    private func liveWriterID(_ dbm: DatabaseManager, id: Int64) throws -> String? {
        try dbm.read { db in
            try String.fetchOne(
                db,
                sql: "SELECT writerID FROM \(ServerProfileRecord.databaseTableName) WHERE id = ?",
                arguments: [id]
            )
        }
    }

    // Maintenance prepare backfills a saved nil writer ID and acquires the lock instead of failing closed.
    func testMaintenancePrepBackfillsNullWriterIDAndAcquiresLock() async throws {
        let (dbm, profile) = try insertNullWriterIDProfile()
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(databaseManager: dbm),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: dbm),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: dbm
        )
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)

        let plan = try await service.makeMaintenancePlan(client: client, profile: profile)
        XCTAssertEqual(plan.layout, .lite)
        XCTAssertNotNil(plan.session, "a backfilled identity must take the maintenance lock, not fail closed")

        let persisted = try XCTUnwrap(liveWriterID(dbm, id: try XCTUnwrap(profile.id)))
        XCTAssertNotNil(UUID(uuidString: persisted), "backfill persists a canonical UUID writer ID")
        let locked = await client.lockExists(basePath: basePath, writerID: persisted)
        XCTAssertTrue(locked, "the lock is held under the backfilled writer ID")
        await plan.session?.stopAndRelease()
    }

    // R02 regression (R01 Codex Medium): a stale saved-looking profile whose row was deleted carries a nil
    // identity through backfill, so maintenance prepare must fail closed — no lock, no Lite marker write.
    func testMaintenancePrepMissingRowStaleNilIdentityDoesNotAcquireLock() async throws {
        let (dbm, profile) = try insertNullWriterIDProfile()
        try dbm.deleteServerProfile(id: try XCTUnwrap(profile.id))   // the row is now gone
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(databaseManager: dbm),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: dbm),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: dbm
        )
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)   // committed repo so classify reaches the writer-ID gate

        do {
            _ = try await service.makeMaintenancePlan(client: client, profile: profile)
            XCTFail("a missing-row stale profile with nil identity must not produce a maintenance plan")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .writerIdentityUnavailable)
        }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no lock or Lite marker is written for a missing-row stale identity")
        let createdLocks = await client.createdDirectories.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath))
        XCTAssertFalse(createdLocks, "maintenance must not create the locks directory without a persisted identity")
    }

    // Foreground composition: backfill then prepareForegroundWrite acquires the lock for a saved nil identity.
    func testForegroundBackfillCompositionAcquiresLock() async throws {
        let (dbm, profile) = try insertNullWriterIDProfile()
        let backfilled = try dbm.profileWithBackfilledWriterID(profile)
        let writerID = try XCTUnwrap(backfilled.writerID)

        let client = InMemoryRemoteStorageClient()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, "a backfilled writer ID must let foreground prepare acquire the lock")
        await plan.session.stopAndRelease()
    }

    // Direct unsaved/nil identity still fails closed (foreground) and skips (background).
    func testBackgroundDirectNilWriterIdentitySkips() async throws {
        let client = InMemoryRemoteStorageClient()
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: nil
        )
        guard case .skip = outcome else { return XCTFail("a nil writer identity must make background skip") }
    }

    // MARK: - Prepare-failure marker unwind (P08)

    func testForegroundFreshCommitFailureUnwindsEmptyMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // publish move temp→version.json fails
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }

        let version = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(version, "commit failed before publishing version.json")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "the lock is released on commit failure")
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                      "an empty uncommitted marker is unwound")
        XCTAssertTrue(deleted.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
                      "the empty locks directory is unwound")
    }

    func testMarkerUnwindKeepsMarkerWhenVersionPresent() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        // Both the publish move and the backup move fail → repair commit fails with the version.json present.
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let version = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(version, "an existing version.json must survive a failed repair")
        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "unwind must never delete a marker that still holds version.json")
    }

    func testMarkerUnwindKeepsMarkerWithMonthSqlite() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(basePath)/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await client.enqueueListResult([])   // initial base probe sees empty → .fresh; under-lock sees the damaged tree
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "unwind must not delete a marker that contains a month sqlite")
        let month = await client.fileData(path: "\(basePath)/.watermelon/months/2024-03.sqlite")
        XCTAssertNotNil(month, "the month sqlite must survive")
    }

    func testMarkerUnwindKeepsMarkerWithDevMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon/commits")
        await client.enqueueListResult([])   // initial base probe sees empty → .fresh; under-lock sees the dev marker
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoUnsupported) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "unwind must not delete a marker that contains a dev/v2 marker dir")
    }

    func testMarkerUnwindFailureDoesNotMaskOriginalError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // commit fails
        // Every subsequent cleanup delete fails; the original prepare error must still surface.
        for _ in 0 ..< 4 { await client.enqueueDeleteError(RemoteErrorFixtures.terminal) }
        let writerID = newWriterID()

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
    }

    func testBackgroundFreshCommitFailureUnwindsEmptyMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // commit publish fails
        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareBackgroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: newWriterID()
            )
        }
        let version = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(version, "background commit failed before publishing version.json")
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                      "background also unwinds its empty uncommitted marker")
    }

    func testBackgroundUnderLockSkipUnwindsEmptyMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        let committed = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed")
        )
        await client.enqueueListResult([
            RemoteStorageEntry(
                path: RepoLayoutLite.repoDirectoryPath(basePath: basePath),
                name: RepoLayoutLite.repoDirectoryName,
                isDirectory: true,
                size: 0,
                creationDate: nil,
                modificationDate: nil
            )
        ])
        await client.enqueueDownloadData(committed)

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )
        guard case .skip = outcome else { return XCTFail("background current→fresh drift should skip") }

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
                      "background skip after acquire unwinds the empty locks directory")
        XCTAssertTrue(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                      "background skip after acquire unwinds the empty marker")
    }

    // MARK: - Helpers

    private func acquiredSession(
        client: InMemoryRemoteStorageClient,
        writerID: String? = nil,
        now: Date
    ) async throws -> LiteWriteSession {
        let id = writerID ?? newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: id, now: now
        )
        return plan.session
    }

    private func makePreparedRun(
        client: any RemoteStorageClientProtocol,
        monthPlans: [MonthWorkItem],
        totalAssetCount: Int,
        session: LiteWriteSession
    ) -> BackupPreparedRun {
        BackupPreparedRun(
            initialClient: client,
            snapshotSeedLookup: nil,
            monthPlans: monthPlans,
            workerCount: 1,
            connectionPoolSize: 1,
            totalAssetCount: totalAssetCount,
            makeClient: { client },
            writeMode: .lite(session)
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
