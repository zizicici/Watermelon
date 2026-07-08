import XCTest
import GRDB
@testable import Watermelon

private actor LockClientProviderSpy {
    private var handles: [LiteLockClientHandle]
    private var errors: [Error]
    private(set) var callCount = 0

    init(handles: [LiteLockClientHandle] = [], errors: [Error] = []) {
        self.handles = handles
        self.errors = errors
    }

    func make() async throws -> LiteLockClientHandle {
        callCount += 1
        if !errors.isEmpty {
            throw errors.removeFirst()
        }
        if !handles.isEmpty {
            return handles.removeFirst()
        }
        throw RemoteErrorFixtures.retryable
    }
}

private actor CopyingLockClientProviderSpy {
    private let source: InMemoryRemoteStorageClient
    private let destination: InMemoryRemoteStorageClient
    private let lockPath: String
    private let modificationDate: Date?
    private(set) var callCount = 0

    init(
        source: InMemoryRemoteStorageClient,
        destination: InMemoryRemoteStorageClient,
        lockPath: String,
        modificationDate: Date?
    ) {
        self.source = source
        self.destination = destination
        self.lockPath = lockPath
        self.modificationDate = modificationDate
    }

    func make() async throws -> LiteLockClientHandle {
        callCount += 1
        guard let data = await source.fileData(path: lockPath) else {
            throw RemoteErrorFixtures.notFound
        }
        await destination.seedFile(path: lockPath, data: data, modificationDate: modificationDate)
        return LiteLockClientHandle(client: destination)
    }
}

private final class RemoteSyncProgressRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [RemoteSyncProgress] = []

    func append(_ progress: RemoteSyncProgress) {
        lock.lock()
        values.append(progress)
        lock.unlock()
    }

    func snapshots() -> [RemoteSyncProgress] {
        lock.lock()
        let snapshot = values
        lock.unlock()
        return snapshot
    }
}

private final class CallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func bump() { lock.lock(); value += 1; lock.unlock() }
    var count: Int { lock.lock(); defer { lock.unlock() }; return value }
}

// Returns the seeded client on the first call and fails every later call, atomically — models a remote
// that accepts one extra pooled connection but refuses the rest.
private final class OneShotClientFactory: @unchecked Sendable {
    struct ConnectFailure: Error {}
    private let lock = NSLock()
    private var used = false
    private let client: InMemoryRemoteStorageClient
    init(_ client: InMemoryRemoteStorageClient) { self.client = client }
    func make() throws -> any RemoteStorageClientProtocol {
        lock.lock(); defer { lock.unlock() }
        if used { throw ConnectFailure() }
        used = true
        return client
    }
}

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

    private func seedPopulatedLiteMonth(
        _ client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey,
        hashByte: UInt8
    ) async throws {
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: month.year, month: month.month, layout: .lite,
            assertOwnership: {}
        )
        try store.upsertResource(
            TestFixtures.remoteResource(
                year: month.year, month: month.month, contentHash: Data([hashByte]), fileName: "f\(hashByte).jpg"
            )
        )
        _ = try await store.flushToRemote()
    }

    private func seedPopulatedV1Month(
        _ client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey,
        hashByte: UInt8
    ) async throws {
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: month.year, month: month.month, layout: .v1
        )
        try store.upsertResource(
            TestFixtures.remoteResource(
                year: month.year, month: month.month, contentHash: Data([hashByte]), fileName: "v\(hashByte).jpg"
            )
        )
        _ = try await store.flushToRemote()
    }

    private func makeMonthSqliteData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-bg-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(queue)
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    private func makeLegacyPruneMarkerData(month: LibraryMonthKey, manifestPath: String, data: Data) throws -> Data {
        try JSONEncoder().encode(
            LegacyV1PruneMarker(
                sources: [
                    LegacyV1PruneMarker.Source(
                        year: month.year,
                        month: month.month,
                        manifestPath: manifestPath,
                        sha256Hex: S3SigV4Signer.sha256Hex(data: data)
                    )
                ]
            )
        )
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
        XCTAssertEqual(manifest.createdBy, writerID)

        await plan.session.stopAndRelease()
        let afterRelease = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(afterRelease, "release must delete the lock")
    }

    func testForegroundFreshVersionCommitReconnectsLockClientForOwnershipAssertion() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClientA = InMemoryRemoteStorageClient()
        let lockClientB = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        await lockClientA.setPendingUploadModificationDate(now)
        await lockClientA.enqueueListResult([])
        await lockClientA.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: writerID, modificationDate: now)
        ])
        await lockClientA.enqueueListError(RemoteErrorFixtures.retryable)
        let provider = CopyingLockClientProviderSpy(
            source: lockClientA,
            destination: lockClientB,
            lockPath: RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!,
            modificationDate: now
        )

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: dataClient,
            lockClient: lockClientA,
            ownsLockClient: true,
            basePath: basePath,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )

        let versionData = await dataClient.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        let callCount = await provider.callCount
        let oldConnected = await lockClientA.connected
        let newConnected = await lockClientB.connected
        XCTAssertEqual(callCount, 1)
        XCTAssertFalse(oldConnected, "replaced lock clients are disconnected immediately after a successful reconnect")
        XCTAssertTrue(newConnected)

        await plan.session.stopAndRelease()
        let oldConnectedAfterRelease = await lockClientA.connected
        let newConnectedAfterRelease = await lockClientB.connected
        XCTAssertFalse(oldConnectedAfterRelease)
        XCTAssertFalse(newConnectedAfterRelease)
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

    func testForegroundCurrentCleanupPrunesResidualV1ManifestFromPriorInterruptedPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let manifest = try makeMonthSqliteData()
        let v1Path = MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        let litePath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        await client.seedFile(
            path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath),
            data: try makeLegacyPruneMarkerData(month: month, manifestPath: v1Path, data: manifest)
        )
        await client.seedFile(path: v1Path, data: manifest)
        await client.seedFile(path: litePath, data: manifest)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )

        let v1Data = await client.fileData(path: v1Path)
        let liteData = await client.fileData(path: litePath)
        XCTAssertNil(v1Data, "a later .current write must compensate if the post-commit V1 prune was interrupted")
        XCTAssertNotNil(liteData)
        await plan.session.stopAndRelease()
    }

    func testForegroundCurrentReusesUnderLockProbeListingsForCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let monthsDir = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        await client.seedFile(path: monthsDir + "/manifest_x.tmp", data: Data([0x01]))

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let listed = await client.listedPaths
        XCTAssertEqual(
            listed.filter { $0 == basePath }.count,
            2,
            "current cleanup without a pending V1 prune marker must not add a legacy V1 scan"
        )
        XCTAssertEqual(
            listed.filter { $0 == RepoLayoutLite.repoDirectoryPath(basePath: basePath) }.count,
            1,
            "repo dir should be listed only by the under-lock detailed probe"
        )
        XCTAssertEqual(
            listed.filter { $0 == monthsDir }.count,
            1,
            "months dir listing from the under-lock probe should be reused by cleanup"
        )
        let metadataAttempts = await client.metadataAttemptPaths
        XCTAssertFalse(
            metadataAttempts.contains(RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath)),
            "current cleanup without a pending marker should use the under-lock repo listing, not add a marker HEAD"
        )
        await plan.session.stopAndRelease()
    }

    func testForegroundCurrentDoesNotSeedEmptyMonthsSnapshotWhenRepoListingOmitsMonths() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()
        let monthsDir = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        await client.seedFile(path: monthPath, data: try makeMonthSqliteData())

        let baseEntries = [directoryEntry(RepoLayoutLite.repoDirectoryPath(basePath: basePath))]
        let repoEntriesWithoutMonths = [dataEntry(RepoLayoutLite.versionPath(basePath: basePath))]
        await client.enqueueListResult(baseEntries)
        await client.enqueueListResult([])
        await client.enqueueListResult([makeLockEntry(basePath: basePath, writerID: writerID, modificationDate: nil)])
        await client.enqueueListResult(baseEntries)
        await client.enqueueListResult(repoEntriesWithoutMonths)

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: writerID
        )

        let entries = try await plan.monthsListing.entries(client: client, basePath: basePath)
        XCTAssertTrue(
            entries.contains { $0.path == monthPath },
            "omitting months from the parent repo listing must not seed an empty months snapshot"
        )
        let listed = await client.listedPaths
        XCTAssertTrue(
            listed.contains(monthsDir),
            "months must be listed on demand when the detailed probe did not actually list it"
        )
        await plan.session.stopAndRelease()
    }

    func testForegroundCurrentMonthsProbeRetryableFaultDoesNotFailPrepare() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()
        let monthsDir = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        await client.seedDirectory(monthsDir)

        let baseEntries = [directoryEntry(RepoLayoutLite.repoDirectoryPath(basePath: basePath))]
        let repoEntries = [
            dataEntry(RepoLayoutLite.versionPath(basePath: basePath)),
            directoryEntry(monthsDir)
        ]
        await client.enqueueListResult(baseEntries)
        await client.enqueueListResult([])
        await client.enqueueListResult([makeLockEntry(basePath: basePath, writerID: writerID, modificationDate: nil)])
        await client.enqueueListResult(baseEntries)
        await client.enqueueListResult(repoEntries)
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: writerID
        )

        await plan.session.stopAndRelease()
    }

    func testForegroundV1MigrateDeletesOldV1ManifestAfterCommit() async throws {
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
        XCTAssertNil(oldV1Manifest, "after migrating + committing, the old V1 manifest is pruned")
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

    // A directory-only V1 manifest candidate (YYYY/MM/.watermelon_manifest.sqlite/) must not let a write path
    // commit a Lite version.json over damaged V1 control state: the router routes it .damaged, so foreground
    // fails closed before acquiring the lock, with no version commit.
    func testForegroundDirectoryOnlyV1CandidateFailsClosedWithoutVersionCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1ManifestPath = MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 2)
        await client.seedDirectory(v1ManifestPath)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client, lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "no Lite version.json may commit over a directory-valued V1 candidate")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "a directory-only V1 candidate must fail before acquiring the write lock")
    }

    func testBackgroundDirectoryOnlyV1CandidateSkipsWithoutVersionCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1ManifestPath = MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 2)
        await client.seedDirectory(v1ManifestPath)
        let writerID = newWriterID()

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client, lockClient: client, basePath: basePath, writerID: writerID
        )
        guard case .skip = outcome else { return XCTFail("background must skip a damaged directory-only V1 candidate") }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "background must not commit version.json over a directory-valued V1 candidate")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "background skip must not leave a lock behind")
    }

    func testReadAndReloadDirectoryOnlyV1CandidateFailClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1ManifestPath = MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 2)
        await client.seedDirectory(v1ManifestPath)

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: self.basePath)
        }
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareReload(
                client: client,
                basePath: self.basePath,
                writerID: self.newWriterID(),
                makeLockClient: { LiteLockClientHandle(client: client, ownsClient: false) }
            )
        }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNil(versionData, "read/reload must not commit version.json over a directory-valued V1 candidate")
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
            formatVersion: 3, minAppVersion: "9.9.9",
            createdAt: "x", createdBy: "y"
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(future))

        await assertThrowsLiteError(.repoUnsupported(minAppVersion: "9.9.9")) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: self.newWriterID()
            )
        }
        XCTAssertTrue(LiteRepoError.repoUnsupported(minAppVersion: "9.9.9").localizedDescription.contains("9.9.9"))
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

    func testForegroundOwnFreshLockReportsRetryableSelfLock() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: now)

        do {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
                lockClient: client,
                basePath: self.basePath,
                writerID: writerID,
                now: now
            )
            XCTFail("expected ownLockConflict")
        } catch let error as LiteRepoError {
            guard case .ownLockConflict(let block?) = error else {
                return XCTFail("expected ownLockConflict with retryAfter, got \(error)")
            }
            XCTAssertEqual(block.reason, .stillFresh)
            let retryAfter = try XCTUnwrap(block.retryAfter)
            let expectedRetryAfter = now.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance)
            XCTAssertEqual(retryAfter.timeIntervalSince1970, expectedRetryAfter.timeIntervalSince1970, accuracy: 0.001)
            let message = error.localizedDescription
            XCTAssertFalse(message.contains("lockedByAnotherDevice"))
            XCTAssertFalse(message.contains("backup.repo.ownLockConflict"))
            XCTAssertTrue(message.contains(LiteRepoError.ownLockConflictReasonText(.stillFresh)))
            XCTAssertTrue(message.contains(LiteRepoError.ownLockConflictRetryTimeText(expectedRetryAfter)))
        } catch {
            XCTFail("expected ownLockConflict, got \(error)")
        }
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

    func testForegroundCanonicalMalformedVersionFailsClosedBeforeRepairCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
                lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "canonical malformed version must not acquire the foreground lock")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertEqual(versionData, Data("not json".utf8), "canonical malformed version must not be silently rewritten")
    }

    // MARK: - Malformed-version routing

    private func seedMalformedVersion(_ client: InMemoryRemoteStorageClient) async {
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: Data("not json".utf8))
    }

    func testForegroundMalformedVersionThrowsRepoDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
                lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "canonical malformed version must fail before lock acquisition")
    }

    func testMalformedVersionWithRecoverableCurrentScratchRepairsUnderLock() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        await client.seedFile(path: RepoLayoutLite.monthPath(basePath: basePath, month: month), data: try makeMonthSqliteData())
        await client.seedFile(
            path: RepoLayoutLite.repoDirectoryPath(basePath: basePath) + "/version_11111111-1111-1111-1111-111111111111.json.tmp",
            data: try VersionManifestLite.encode(VersionManifestLite.makeManifest(createdAt: "t", createdBy: "scratch"))
        )
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        XCTAssertEqual(plan.layout, .lite)
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        let manifest = try VersionManifestLite.decode(try XCTUnwrap(versionData))
        XCTAssertEqual(manifest.formatVersion, VersionManifestLite.formatVersion)
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(locked, "recoverable current scratch repair must hold the foreground lock")
        await plan.session.stopAndRelease()
    }

    func testMaintenanceMalformedVersionThrowsRepoDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
                lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "canonical malformed version must not acquire the maintenance lock")
    }

    func testResolveReadLayoutMalformedVersionThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: self.basePath)
        }
    }

    func testBackgroundPrepareTakesOverStaleForeignLockEvenWithStaleOwnLock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let me = newWriterID()
        let other = newWriterID()
        let now = Date()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: now.addingTimeInterval(-600))      // stale own
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: now.addingTimeInterval(-600))   // stale foreign

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client, lockClient: client, basePath: basePath, writerID: me, now: now
        )
        guard case .proceed = outcome else {
            return XCTFail("background prepare must proceed after clearing stale locks")
        }
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertFalse(foreignExists, "background clears the stale foreign lock")
        let deleted = await client.deletedPaths
        let foreignLockPath = RepoLayoutLite.lockPath(basePath: basePath, writerID: other)!
        let ownLockPath = RepoLayoutLite.lockPath(basePath: basePath, writerID: me)!
        XCTAssertTrue(deleted.contains(foreignLockPath), "background takes over the stale foreign lock")
        XCTAssertTrue(deleted.contains(ownLockPath), "background reclaims its stale own lock before upload")
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(versionData, "the committed version must remain unchanged")
    }

    func testBackgroundSkipsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: writerID
        )
        guard case .skip = outcome else { return XCTFail("malformed version should skip in background") }
        let versionData = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertEqual(versionData, Data("not json".utf8), "background must not rewrite canonical malformed version")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "background canonical malformed skip must not keep a lock")
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
        // Initial classify reads a committed version; the under-lock reclassify finds none after the hook
        // removes it following the first read → `.fresh` → mismatch.
        let committed = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed")
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: committed)
        await client.setOnDownload { path in
            if path == RepoLayoutLite.versionPath(basePath: self.basePath) {
                try? await client.delete(path: path)
            }
        }
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

    // Maintenance must surface an under-lock unsupported (future/foreign committed format) as repoUnsupported,
    // preserving the upgrade-version signal the foreground/pre-lock/read routes already emit — not collapse it
    // to repoDamaged like the other under-lock failures.
    func testMaintenanceUnderLockUnsupportedSurfacesRepoUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        // The committed version is a future format (unsupported); the pre-lock classify is scripted to read a
        // current version so the lock is acquired before the under-lock reclassify sees the unsupported state.
        let future = WatermelonRemoteVersionManifest(
            formatVersion: 3, minAppVersion: "9.9.9",
            createdAt: "x", createdBy: "y"
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(future))
        let committed = try VersionManifestLite.encode(VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed"))
        await client.enqueueDownloadData(committed)   // pre-lock version read sees .current
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoUnsupported(minAppVersion: "9.9.9")) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
                lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "maintenance must release the lock when the under-lock state is unsupported")
    }

    func testMaintenanceCanonicalMalformedVersionDoesNotAcquireLock() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked, "canonical malformed version must fail before maintenance lock acquisition")
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

    // A seed is cache, not authority: if the canonical month sqlite is absent behind it, a clean (data-matching)
    // seeded load must not certify the month complete over missing month truth — it republishes the canonical
    // under the lease instead.
    func testLoadSeededLiteAbsentCanonicalRepublishesUnderOwnership() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        // Data file present and matching the seed ⇒ reconcile is clean; only the canonical sqlite is missing.
        await client.seedFile(path: "\(basePath)/2024/03/b.jpg", data: Data([0xCC]))
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let litePath = MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        let canonicalBefore = await client.fileData(path: litePath)
        XCTAssertNil(canonicalBefore, "precondition: the canonical month sqlite is absent")

        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )

        XCTAssertNotNil(store.findByFileName("b.jpg"), "the matching seed resource is preserved, not pruned")
        XCTAssertFalse(store.dirty, "the forced republish flushed the canonical, leaving the store clean")
        let canonicalAfter = await client.fileData(path: litePath)
        XCTAssertNotNil(
            canonicalAfter,
            "an absent canonical behind a clean seed must be republished under the lease, not certified complete"
        )
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

    private func directoryEntry(_ path: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: path,
            name: (path as NSString).lastPathComponent,
            isDirectory: true,
            size: 0,
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
        // A committed month has its canonical sqlite present; seed it so the load exercises only the
        // destructive-prune gate, not the absent-canonical republish path (loadSeeded forces a dirty
        // republish when the canonical is missing).
        await client.seedFile(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3),
            data: Data([0x01])
        )
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

    func testLoadSeededLiteConfirmationListCancellationSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.enqueueListResult([])
        await client.enqueueListError(RemoteErrorFixtures.cancelled)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a cancelled confirmation LIST must surface")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
        }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "cancellation must not flush a pruned manifest")
    }

    // Two matching empty LISTs are still not enough when sampled metadata proves a manifest file exists.
    func testLoadSeededLiteMatchingEmptyListingsWithPresentMetadataDoesNotPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.seedFile(path: "\(basePath)/2024/03/b.jpg", data: Data([0xCC]))
        // Present canonical so the load exercises only the destructive-prune gate, not the absent-canonical
        // republish path (loadSeeded forces a dirty republish when the canonical month sqlite is missing).
        await client.seedFile(
            path: MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3),
            data: Data([0x01])
        )
        await client.enqueueListResult([])
        await client.enqueueListResult([])
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        XCTAssertNotNil(store.findByFileName("b.jpg"), "a present metadata sample must prove the empty LIST view unsafe")
        XCTAssertFalse(store.dirty)
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no manifest flush after sampled metadata disproves the destructive prune")
    }

    // A same-name directory is not the manifest resource file, so it must not block a confirmed prune.
    func testLoadSeededLiteMatchingEmptyListingsWithDirectoryMetadataStillPrunes() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.seedDirectory("\(basePath)/2024/03/b.jpg")
        await client.enqueueListResult([])
        await client.enqueueListResult([])
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        XCTAssertNil(store.findByFileName("b.jpg"), "directory metadata must not count as a present resource file")
        XCTAssertFalse(store.dirty, "confirmed prune should flush")
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(uploaded.isEmpty, "confirmed prune must flush the pruned manifest")
    }

    // A sampled metadata transport fault is inconclusive, so the destructive prune remains skipped.
    func testLoadSeededLiteMatchingEmptyListingsWithFaultingMetadataDoesNotPrune() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.enqueueListResult([])
        await client.enqueueListResult([])
        await client.failMetadata(forPathSuffix: "/2024/03/b.jpg", error: RemoteErrorFixtures.retryable)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
            assertOwnership: {}
        )
        XCTAssertNotNil(store.findByFileName("b.jpg"), "a faulting metadata sample must leave the manifest intact")
        XCTAssertFalse(store.dirty)
    }

    func testLoadSeededLiteMetadataCancellationSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/2024/03")
        await client.enqueueListResult([])
        await client.enqueueListResult([])
        await client.failMetadata(forPathSuffix: "/2024/03/b.jpg", error: RemoteErrorFixtures.cancelled)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCC]), fileName: "b.jpg")],
            assets: [],
            assetResourceLinks: []
        )
        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: 2024, month: 3, seed: seed, layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a cancelled metadata sample must surface")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
        }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "cancellation must not flush a pruned manifest")
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
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: Date())   // must not throw
        await session.stopAndRelease()
    }

    func testLeaseGateTripsAfterConfidenceLoss() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await session.lock.noteConfidenceLoss()
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)
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
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: faultTime)

        // A successful in-window refresh re-proves ownership; the gate can pass again.
        let later = faultTime.addingTimeInterval(60)
        let refresh = await session.lock.refresh(now: later)
        XCTAssertEqual(refresh, .refreshed)
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: later)   // must not throw
        await session.stopAndRelease()
    }

    // After a refresh upload fault degrades confidence, the read-tier gate recovers via the read-only proof
    // (own lock still present + fresh by body) — it must not abort and must not reclaim/write.
    func testLeaseGateRecoversByProofAfterRefreshUploadFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let faultTime = Date().addingTimeInterval(1)
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        _ = await session.lock.refresh(now: faultTime)   // degrades confidence
        let confidentBeforeGate = await session.lock.hasLeaseConfidence(now: faultTime)
        XCTAssertFalse(confidentBeforeGate, "gate must take the read-only proof path, not the cheap confident shortcut")
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: faultTime)   // recovers, must not throw
        await session.stopAndRelease()
    }

    // A missed confidence window falls back to the read-only proof; a still-owned, still-fresh lease recovers.
    func testLeaseGateRecoversByProofAfterMissedConfidenceWindow() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let stale = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        let confidentBeforeGate = await session.lock.hasLeaseConfidence(now: stale)
        XCTAssertFalse(confidentBeforeGate, "gate must take the read-only proof path, not the cheap confident shortcut")
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: stale)   // recovers, must not throw
        await session.stopAndRelease()
    }

    // A LIST fault that dropped confidence is recovered by the read-only proof's own (clean) LIST.
    func testLeaseGateRecoversByProofAfterListFaultDroppedConfidence() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        _ = await session.lock.assertStillOwned(now: now)   // LIST fault drops confidence
        let confidentBeforeGate = await session.lock.hasLeaseConfidence(now: now)
        XCTAssertFalse(confidentBeforeGate, "gate must take the read-only proof path, not the cheap confident shortcut")
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)   // recovers, must not throw
        await session.stopAndRelease()
    }

    func testBackgroundLeaseGateClearsStaleForeignWithinConfidenceWindow() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await backgroundAcquiredSession(client: client, now: now)
        let other = newWriterID()
        await client.seedLock(
            basePath: basePath,
            writerID: other,
            modificationDate: now.addingTimeInterval(-(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 60))
        )
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertFalse(foreignStillThere, "background lease gate clears stale foreign locks")
        await session.stopAndRelease()
    }

    // A confident foreground lease makes no remote call, so a stale foreign lock present during a live run
    // neither blocks nor is touched by the data-upload gate — it simply passes on in-memory confidence.
    func testForegroundLeaseGateToleratesStaleForeignWithinConfidenceWindow() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        let other = newWriterID()
        await client.seedLock(
            basePath: basePath,
            writerID: other,
            modificationDate: now.addingTimeInterval(-(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 60))
        )
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)   // must not throw
        await session.stopAndRelease()
    }

    // Once a background lease's local confidence has expired, the data-byte gate must run the FULL ownership
    // assertion (own-lock body proof), not the lightweight foreign-only probe: a same-writer successor that
    // replaced the body at the same lock filename presents no foreign evidence, so the light probe would pass.
    func testBackgroundLeaseGateFailsClosedOnSameWriterSuccessorAfterConfidenceExpiry() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await backgroundAcquiredSession(client: client, writerID: writerID, now: now)

        // A same-writer successor session reclaimed the lock filename with a different body; only the own
        // filename is present and no foreign lock is listed.
        let successor = LockFileBody(
            writerID: writerID,
            sessionToken: UUID().uuidString,
            lockToken: UUID().uuidString,
            generation: 9
        )
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: now, body: successor)

        let expired = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        await assertThrowsLiteError(.ownershipLost) {
            try await RepoLeaseGuard.assertLeaseConfidence(session, now: expired)
        }
        await session.stopAndRelease()
    }

    // Same root cause via explicit confidence degradation (e.g. a refresh fault dropped `confident`): the
    // background gate must run the full assertion and fail closed on a same-writer successor body, not pass on
    // mere own-filename presence.
    func testBackgroundLeaseGateFailsClosedOnSameWriterSuccessorAfterConfidenceDegraded() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await backgroundAcquiredSession(client: client, writerID: writerID, now: now)
        await session.lock.noteConfidenceLoss()

        let successor = LockFileBody(
            writerID: writerID,
            sessionToken: UUID().uuidString,
            lockToken: UUID().uuidString,
            generation: 9
        )
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: now, body: successor)

        await assertThrowsLiteError(.ownershipLost) {
            try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)
        }
        await session.stopAndRelease()
    }

    // Non-regression: a background lease whose confidence window lapsed but whose own lock is still present
    // and still ours (no foreign, no successor) must pass via the read-only ownership proof — without
    // rewriting the lock. Confidence is NOT auto-restored (the refresh task owns the mtime refresh); a
    // still-owned lease simply must not be aborted.
    func testBackgroundLeaseGateProvesStillOwnedLockAfterConfidenceExpiryWithoutWriting() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await backgroundAcquiredSession(client: client, writerID: writerID, now: now)
        let path = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count

        let expired = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: expired)   // still owned → must not throw

        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsAfter, uploadsBefore, "the read-only proof must not rewrite the lock")
        await session.stopAndRelease()
    }

    // A confident FOREGROUND data-upload gate makes no remote call, so it does not observe a same-writer
    // successor that replaced the body at our filename — intentional: the refresh task is the remote watchdog,
    // and the control-state writes a silently-lost lease could corrupt keep their own strong proof. Once the
    // confidence window lapses, the gate re-proves and fails closed on the successor body.
    func testForegroundLeaseGatePassesWhileConfidentThenFailsClosedOnSuccessorAfterLapse() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: now)

        let confidentBefore = await session.lock.hasLeaseConfidence(now: now)
        XCTAssertTrue(confidentBefore, "the foreground lease is still locally confident")

        // A same-writer successor reclaimed the lock filename with a different body.
        let successor = LockFileBody(
            writerID: writerID,
            sessionToken: UUID().uuidString,
            lockToken: UUID().uuidString,
            generation: 9
        )
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: now, body: successor)

        // While confident: no remote proof, so the successor is not seen and the gate passes.
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)

        // After the confidence window lapses: re-proves remotely and fails closed on the successor body.
        let expired = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        await assertThrowsLiteError(.ownershipLost) {
            try await RepoLeaseGuard.assertLeaseConfidence(session, now: expired)
        }
        await session.stopAndRelease()
    }

    // The headline of the session-managed lock: while a foreground lease is locally confident, the
    // data-upload gate makes ZERO remote calls — no LIST, no metadata, no body download, no rewrite. Workers
    // consume in-memory confidence; the refresh task is the sole remote lock I/O.
    func testForegroundLeaseGateMakesNoRemoteCallsWhileConfident() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)

        let listsBefore = await client.listedPaths.count
        let metadataBefore = await client.metadataAttemptPaths.count
        let downloadsBefore = await client.downloadAttemptPaths.count
        let uploadsBefore = await client.uploadedPaths.count

        try await RepoLeaseGuard.assertLeaseConfidence(session, now: now)   // confident → no remote op

        let listsAfter = await client.listedPaths.count
        let metadataAfter = await client.metadataAttemptPaths.count
        let downloadsAfter = await client.downloadAttemptPaths.count
        let uploadsAfter = await client.uploadedPaths.count
        XCTAssertEqual(listsAfter, listsBefore, "confident foreground gate must not LIST")
        XCTAssertEqual(metadataAfter, metadataBefore, "confident foreground gate must not read lock metadata")
        XCTAssertEqual(downloadsAfter, downloadsBefore, "confident foreground gate must not download the lock body")
        XCTAssertEqual(uploadsAfter, uploadsBefore, "confident foreground gate must not rewrite the lock")
        await session.stopAndRelease()
    }

    // Non-regression: once confidence lapses, a foreground lease whose own lock is still present and ours
    // proves ownership via the read-only path WITHOUT rewriting the lock (the refresh task is the sole writer).
    func testForegroundLeaseGateProvesStillOwnedLockAfterConfidenceExpiryWithoutWriting() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: now)
        let path = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count

        let expired = now.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        try await RepoLeaseGuard.assertLeaseConfidence(session, now: expired)   // still owned → must not throw

        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsAfter, uploadsBefore, "the lapsed read-only proof must not rewrite the lock")
        await session.stopAndRelease()
    }

    func testRefreshReconnectsAfterRetryableLockClientFault() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClientA = InMemoryRemoteStorageClient()
        let lockClientB = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: lockClientB)])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClientA,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )
        try await copyLockBytes(writerID: writerID, from: lockClientA, to: lockClientB, modificationDate: now)

        await lockClientA.enqueueDownloadError(RemoteErrorFixtures.retryable)
        let refresh = await session.refreshLease(now: now.addingTimeInterval(1))
        XCTAssertEqual(refresh, .refreshed)
        let callCount = await provider.callCount
        let oldConnected = await lockClientA.connected
        let newConnected = await lockClientB.connected
        XCTAssertEqual(callCount, 1)
        XCTAssertFalse(oldConnected, "replaced lock clients are disconnected immediately after a successful reconnect")
        XCTAssertTrue(newConnected)

        await session.stopAndRelease()
        let oldConnectedAfterRelease = await lockClientA.connected
        let newConnectedAfterRelease = await lockClientB.connected
        XCTAssertFalse(oldConnectedAfterRelease)
        XCTAssertFalse(newConnectedAfterRelease)
    }

    func testRefreshDoesNotReconnectAfterOwnLockIsLost() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClient = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: replacement)])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClient,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )

        try await lockClient.delete(path: RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!)
        let refresh = await session.refreshLease(now: now.addingTimeInterval(1))
        XCTAssertEqual(refresh, .degraded(.retryable))
        let callCount = await provider.callCount
        let originalConnected = await lockClient.connected
        let replacementConnected = await replacement.connected
        XCTAssertEqual(callCount, 0)
        XCTAssertTrue(originalConnected)
        XCTAssertTrue(replacementConnected)

        await session.stopAndRelease()
        let originalConnectedAfterRelease = await lockClient.connected
        let replacementConnectedAfterRelease = await replacement.connected
        XCTAssertFalse(originalConnectedAfterRelease)
        XCTAssertTrue(replacementConnectedAfterRelease)
    }

    func testRefreshDoesNotReconnectAfterBackwardClock() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClient = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: replacement)])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClient,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )

        let refresh = await session.refreshLease(now: now.addingTimeInterval(-1))
        XCTAssertEqual(refresh, .degraded(.retryable))
        let callCount = await provider.callCount
        let originalConnected = await lockClient.connected
        let replacementConnected = await replacement.connected
        XCTAssertEqual(callCount, 0)
        XCTAssertTrue(originalConnected)
        XCTAssertTrue(replacementConnected)

        await session.stopAndRelease()
        let originalConnectedAfterRelease = await lockClient.connected
        let replacementConnectedAfterRelease = await replacement.connected
        XCTAssertFalse(originalConnectedAfterRelease)
        XCTAssertTrue(replacementConnectedAfterRelease)
    }

    func testLeaseGateNoOpWhenSessionNil() async throws {
        try await RepoLeaseGuard.assertLeaseConfidence(nil)   // no write session: no gating
    }

    // MARK: - Flush ownership gate

    func testFlushOwnershipGatePassesWhenOwned() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)   // must not throw
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateTripsOnForeignWriter() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: now)   // fresh foreign
        await assertThrowsLiteError(.ownershipLost) {
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
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
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateTripsOnListFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let session = try await acquiredSession(client: client, now: now)
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        }
        await session.stopAndRelease()
    }

    func testFlushOwnershipGateReconnectsAfterRetryableLockClientListFault() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClientA = InMemoryRemoteStorageClient()
        let lockClientB = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: lockClientB)])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClientA,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )
        try await copyLockBytes(writerID: writerID, from: lockClientA, to: lockClientB, modificationDate: now)

        await lockClientA.enqueueListError(RemoteErrorFixtures.retryable)
        try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        let callCount = await provider.callCount
        let oldConnected = await lockClientA.connected
        let newConnected = await lockClientB.connected
        XCTAssertEqual(callCount, 1)
        XCTAssertFalse(oldConnected, "replaced lock clients are disconnected immediately after a successful reconnect")
        XCTAssertTrue(newConnected)

        await session.stopAndRelease()
        let oldConnectedAfterRelease = await lockClientA.connected
        let newConnectedAfterRelease = await lockClientB.connected
        XCTAssertFalse(oldConnectedAfterRelease)
        XCTAssertFalse(newConnectedAfterRelease)
    }

    func testFlushOwnershipGateThrowsWhenReconnectProviderUnavailable() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClient = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(errors: [RemoteErrorFixtures.retryable])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClient,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )

        await lockClient.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        }
        let callCount = await provider.callCount
        let connectedBeforeRelease = await lockClient.connected
        XCTAssertEqual(callCount, 1)
        XCTAssertTrue(connectedBeforeRelease)

        await session.stopAndRelease()
        let connectedAfterRelease = await lockClient.connected
        XCTAssertFalse(connectedAfterRelease)
    }

    func testFlushOwnershipGateThrowsWhenRetryOnReconnectedClientStillFaults() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        let lockClientA = InMemoryRemoteStorageClient()
        let lockClientB = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        let now = Date()
        let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: lockClientB)])
        let session = try await acquiredSession(
            dataClient: dataClient,
            lockClient: lockClientA,
            writerID: writerID,
            now: now,
            reconnectLockClient: { try await provider.make() }
        )
        try await copyLockBytes(writerID: writerID, from: lockClientA, to: lockClientB, modificationDate: now)

        await lockClientA.enqueueListError(RemoteErrorFixtures.retryable)
        await lockClientB.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        }
        let callCount = await provider.callCount
        let oldConnected = await lockClientA.connected
        let newConnected = await lockClientB.connected
        XCTAssertEqual(callCount, 1)
        XCTAssertFalse(oldConnected, "replaced lock clients are disconnected immediately after a successful reconnect")
        XCTAssertTrue(newConnected)

        await session.stopAndRelease()
        let oldConnectedAfterRelease = await lockClientA.connected
        let newConnectedAfterRelease = await lockClientB.connected
        XCTAssertFalse(oldConnectedAfterRelease)
        XCTAssertFalse(newConnectedAfterRelease)
    }

    func testFlushOwnershipGateDoesNotReconnectOnTerminalOrCancelledLockFaults() async throws {
        for fault in [RemoteErrorFixtures.terminal, RemoteErrorFixtures.cancelled] {
            let dataClient = InMemoryRemoteStorageClient()
            let lockClient = InMemoryRemoteStorageClient()
            let replacement = InMemoryRemoteStorageClient()
            let writerID = newWriterID()
            let now = Date()
            let provider = LockClientProviderSpy(handles: [LiteLockClientHandle(client: replacement)])
            let session = try await acquiredSession(
                dataClient: dataClient,
                lockClient: lockClient,
                writerID: writerID,
                now: now,
                reconnectLockClient: { try await provider.make() }
            )

            await lockClient.enqueueListError(fault)
            // Neither a terminal nor a cancelled lock fault triggers a reconnect (only a retryable one
            // does). A terminal fault is a genuine confidence loss; a cancelled fault is the run being
            // torn down and must surface as cancellation (A-F1), never a lease-fail-fast.
            if RemoteFaultLite.classify(fault) == .cancelled {
                do {
                    try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
                    XCTFail("a cancelled lock fault must throw")
                } catch is CancellationError {
                    // expected: cancellation surfaces as cancellation
                } catch {
                    XCTFail("a cancelled lock fault must surface as CancellationError, got \(error)")
                }
            } else {
                await assertThrowsLiteError(.leaseConfidenceLost) {
                    try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
                }
            }
            let callCount = await provider.callCount
            let originalConnected = await lockClient.connected
            XCTAssertEqual(callCount, 0)
            XCTAssertTrue(originalConnected)

            await session.stopAndRelease()
            let originalConnectedAfterRelease = await lockClient.connected
            let replacementConnected = await replacement.connected
            XCTAssertFalse(originalConnectedAfterRelease)
            XCTAssertTrue(replacementConnected)
        }
    }

    // A transient LIST fault during the read-only flush proof trips the gate for this attempt but must not
    // delete the own lock — so it is recoverable, not a permanent loss.
    func testFlushGateListFaultRetainsOwnLockAndRecovers() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let writerID = newWriterID()
        let session = try await acquiredSession(client: client, writerID: writerID, now: now)

        // The read-only proof's LIST faults transiently (no reconnect provider → no retry).
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        await assertThrowsLiteError(.leaseConfidenceLost) {
            try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)
        }
        let lockStillThere = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertTrue(lockStillThere, "a transient list fault must not delete the own lock")

        // The lock survived, so a subsequent owned re-assert (clean LIST) passes.
        try await RepoLeaseGuard.assertOwnedBeforeFlush(session, now: now)   // must not throw
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

    func testInlineFinalizerFatalDoesNotReportManifestFlushFailure() async throws {
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
        let profile = makeProfile(writerID: writerID)
        let eventStream = BackupEventStream()
        let collector = Task { () -> [BackupEvent] in
            var events: [BackupEvent] = []
            for await event in eventStream.stream {
                events.append(event)
            }
            return events
        }

        do {
            _ = try await makeExecutor().execute(
                preparedRun: prepared,
                profile: profile,
                workerCountOverride: nil,
                iCloudPhotoBackupMode: .disable,
                eventStream: eventStream,
                onMonthUploaded: { _, _ in .fatal("verify fatal", .ownershipLost) }
            )
            XCTFail("fatal finalizer errors must abort the run")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        } catch {
            eventStream.finish()
            throw error
        }
        eventStream.finish()
        let events = await collector.value
        let logs = events.compactMap { event -> String? in
            if case .log(let message, _) = event { return message }
            return nil
        }
        let expectedFinalizationLog = String.localizedStringWithFormat(
            String(localized: "backup.parallel.finalizationFailed"),
            1,
            month.text,
            "verify fatal"
        )
        let unexpectedFlushLog = String.localizedStringWithFormat(
            String(localized: "backup.parallel.flushManifestFailed"),
            1,
            month.text,
            profile.userFacingStorageErrorMessage(LiteRepoError.ownershipLost)
        )
        XCTAssertTrue(logs.contains(expectedFinalizationLog))
        XCTAssertFalse(logs.contains(unexpectedFlushLog),
                       "finalizer fatal must not be handled by the manifest flush catch")
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

    // MARK: - Background routing (skip / takeover / flush interval)

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

    // Regression (R04 Cluster B): a background run must repair a month whose only surviving manifest is a
    // recoverable `.bak` before loadOrCreate can mint a fresh empty one over it. Background now runs
    // month-scratch cleanup (mirroring foreground), restoring the `.bak` to the canonical path.
    func testBackgroundRestoresRecoverableMonthBakBeforeProceed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        let bakPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            + "/\(RepoLayoutLite.monthFilename(month: month)).\(UUID().uuidString).bak"
        let valid = try makeMonthSqliteData()
        await client.seedFile(path: bakPath, data: valid)

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client, lockClient: client, basePath: basePath, writerID: newWriterID()
        )
        guard case .proceed(let plan) = outcome else {
            return XCTFail("a committed repo with a recoverable month .bak must proceed in background")
        }

        let restored = await client.fileData(path: canonicalPath)
        XCTAssertEqual(restored, valid, "background must restore the recoverable .bak to the canonical month path")
        let bakGone = await client.fileData(path: bakPath)
        XCTAssertNil(bakGone, "the .bak is consumed by the restore")

        await plan.session.stopAndRelease()
    }

    // Regression (R05 Cluster B): if best-effort cleanup could not restore a recoverable month `.bak` (its
    // LIST/validation faulted), loadOrCreate must still refuse to mint a fresh manifest over it.
    func testLoadOrCreateRefusesFreshManifestOverRecoverableMonthScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        // A prior crash left the month's truth only as recoverable `.bak`; canonical is absent and no
        // cleanup pass has restored it.
        let bakPath = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            + "/\(RepoLayoutLite.monthFilename(month: month)).\(UUID().uuidString).bak"
        await client.seedFile(path: bakPath, data: try makeMonthSqliteData())

        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client, basePath: basePath, year: 2024, month: 3,
                layout: .lite, assertOwnership: {}
            )
            XCTFail("loadOrCreate must not mint a fresh manifest over recoverable month scratch")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "MonthManifestStore")
            XCTAssertEqual(error.code, -38)
        }

        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertNil(canonical, "a fresh canonical must not be written over recoverable scratch")
        let bakSurvives = await client.fileData(path: bakPath)
        XCTAssertNotNil(bakSurvives, "the recoverable scratch must survive untouched for the next run's cleanup")
    }

    func testLoadOrCreateRefusesFreshManifestWhenMetadataNilButMonthsListingContainsCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        await client.enqueueListError(RemoteErrorFixtures.notFound)
        await client.enqueueListResult([
            RemoteStorageEntry(
                path: canonicalPath,
                name: RepoLayoutLite.monthFilename(month: month),
                isDirectory: false,
                size: 128,
                creationDate: nil,
                modificationDate: nil
            )
        ])

        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client, basePath: basePath, year: 2024, month: 3,
                layout: .lite, assertOwnership: {}
            )
            XCTFail("loadOrCreate must not mint a fresh manifest when LIST still sees the canonical")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "MonthManifestStore")
            XCTAssertEqual(error.code, -38)
        }

        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertNil(canonical, "a fresh canonical must not be uploaded over an unconfirmed absence")
        let uploaded = await client.uploadedPaths
        XCTAssertFalse(uploaded.contains(canonicalPath), "the fresh manifest upload must not run")
        let listed = await client.listedPaths
        XCTAssertTrue(listed.contains("\(basePath)/2024/03"), "the data directory probe must run")
        XCTAssertTrue(listed.contains(RepoLayoutLite.monthsDirectoryPath(basePath: basePath)), "the months listing guard must run")
    }

    // Control: a genuinely fresh month (no canonical, no scratch) still mints fresh — the guard must not
    // regress the normal first-month path.
    func testLoadOrCreateMintsFreshManifestWhenNoRecoverableScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let month = LibraryMonthKey(year: 2024, month: 3)
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)

        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3,
            layout: .lite, assertOwnership: {}
        )

        let canonical = await client.fileData(path: canonicalPath)
        XCTAssertNotNil(canonical, "a genuinely fresh month mints a canonical manifest")
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
        XCTAssertTrue(foreignStillThere, "background must not take over a fresh foreign lock")
    }

    func testBackgroundSkipsOnFutureBodyForeignLockWithoutTakeover() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let other = newWriterID()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "future-session",
            lockToken: "future-token",
            generation: 1,
            writtenAt: now.addingTimeInterval(60)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: newWriterID(), now: now
        )
        guard case .skip = outcome else { return XCTFail("a future foreign lock body must make background skip") }
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertTrue(foreignStillThere, "background must not take over a future-dated foreign lock")
    }

    func testBackgroundTakesOverStaleForeignLock() async throws {
        let client = InMemoryRemoteStorageClient()
        let now = Date()
        let other = newWriterID()
        let stale = now.addingTimeInterval(-(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 60))
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale)
        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: newWriterID(), now: now
        )
        guard case .proceed = outcome else { return XCTFail("background should reclaim a stranger's stale lock") }
        let foreignStillThere = await client.lockExists(basePath: basePath, writerID: other)
        XCTAssertFalse(foreignStillThere, "background deletes a stale foreign lock")
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
        let oldV1Manifest = await client.fileData(
            path: MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNil(oldV1Manifest, "background migration prunes the copied V1 manifest after commit")
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
        let oldV1Manifest = await client.fileData(
            path: MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNil(oldV1Manifest, "background under-lock migration prunes the copied V1 manifest after commit")
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
        let oldV1Manifest = await client.fileData(
            path: MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: 2024, month: 3)
        )
        XCTAssertNil(oldV1Manifest, "reload migration prunes the copied V1 manifest after commit")
    }

    func testReloadV1MigrationReportsUpgradeScanningThenRemoteIndexProgress() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedV1Manifest(client)
        let service = try makePrepService()
        let recorder = RemoteSyncProgressRecorder()

        let digest = try await service.reloadRemoteIndex(
            client: client,
            profile: makeProfile(writerID: newWriterID()),
            onSyncProgress: { progress in
                recorder.append(progress)
            }
        )

        XCTAssertEqual(digest.resourceCount, 1)
        XCTAssertEqual(recorder.snapshots(), [
            RemoteSyncProgress(current: 0, total: 0, kind: .repoUpgrade(.copying)),
            RemoteSyncProgress(current: 0, total: 1, kind: .repoUpgrade(.copying)),
            RemoteSyncProgress(current: 1, total: 1, kind: .repoUpgrade(.copying)),
            RemoteSyncProgress(current: 0, total: 1, kind: .repoUpgrade(.validating)),
            RemoteSyncProgress(current: 1, total: 1, kind: .repoUpgrade(.validating)),
            RemoteSyncProgress(current: 0, total: 0, kind: .repoUpgrade(.finalizing)),
            RemoteSyncProgress(current: 0, total: 1, kind: .repoUpgrade(.cleaning)),
            RemoteSyncProgress(current: 1, total: 1, kind: .repoUpgrade(.cleaning)),
            RemoteSyncProgress(current: 0, total: 0, kind: .repoUpgrade(.cleaning)),
            RemoteSyncProgress(current: 0, total: 0, kind: .scanningRemoteIndex),
            RemoteSyncProgress(current: 0, total: 1, kind: .remoteIndex),
            RemoteSyncProgress(current: 1, total: 1, kind: .remoteIndex)
        ])
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

    func testDownloadVerificationPlanReusesSingleMaintenanceLeaseForMultipleMonths() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite,
            assertOwnership: {}
        )
        _ = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 4, layout: .lite,
            assertOwnership: {}
        )
        let writerID = newWriterID()
        let service = try makePrepService()
        let profile = makeProfile(writerID: writerID)

        try await service.withDownloadVerificationPlan(client: client, profile: profile) { verifier in
            try await verifier.verify(month: LibraryMonthKey(year: 2024, month: 3))
            try await verifier.verify(month: LibraryMonthKey(year: 2024, month: 4))
        }

        let lockPath = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        let lockUploads = (await client.uploadedPaths).filter { $0 == lockPath }
        let lockDeletes = (await client.deletedPaths).filter { $0 == lockPath }
        XCTAssertEqual(lockUploads.count, 1, "download verification scope must acquire one maintenance lease for all months")
        XCTAssertEqual(lockDeletes.count, 1, "download verification scope must release the maintenance lease once")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked)
    }

    func testDownloadVerificationPlanReleasesMaintenanceLeaseWhenBodyThrows() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedCommittedVersion(client)
        let writerID = newWriterID()
        let service = try makePrepService()
        let profile = makeProfile(writerID: writerID)

        do {
            try await service.withDownloadVerificationPlan(client: client, profile: profile) { _ in
                throw RemoteErrorFixtures.terminal
            }
            XCTFail("body error must propagate")
        } catch {
            XCTAssertEqual((error as NSError).domain, "WriteLockTestTerminal")
        }

        let lockPath = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        let lockDeletes = (await client.deletedPaths).filter { $0 == lockPath }
        XCTAssertEqual(lockDeletes.count, 1, "download verification scope must release the lock on body failure")
        let locked = await client.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertFalse(locked)
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

    func testLocalVolumeMetadataReturnsNilForMissingFileWhenRootReachable() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-localvol-meta-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let client = LocalVolumeClient(connectedRootURL: root)

        let entry = try await client.metadata(path: "/missing.sqlite")

        XCTAssertNil(entry)
    }

    func testLocalVolumeMetadataThrowsWhenRootUnavailableBeforeReturningNil() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-localvol-meta-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let client = LocalVolumeClient(connectedRootURL: root)
        try FileManager.default.removeItem(at: root)

        do {
            _ = try await client.metadata(path: "/missing.sqlite")
            XCTFail("metadata must fail closed when the local-volume root is unavailable")
        } catch let error as RemoteStorageClientError {
            guard case .externalStorageUnavailable = error else {
                return XCTFail("expected externalStorageUnavailable, got \(error)")
            }
        } catch {
            XCTFail("expected externalStorageUnavailable, got \(error)")
        }
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

    func testForegroundFreshCommitFailureLeavesEmptyMarkerDirectories() async throws {
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
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "unwind must not recursively delete the marker directory")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
                       "unwind must not recursively delete the locks directory")
    }

    func testMarkerUnwindKeepsMarkerWhenVersionPresent() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedMalformedVersion(client)
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let version = await client.fileData(path: RepoLayoutLite.versionPath(basePath: basePath))
        XCTAssertNotNil(version, "an existing version.json must survive fail-closed prepare")
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

    func testMarkerUnwindKeepsMarkerWithForeignControlDir() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(basePath)/.watermelon/unexpected-dir")
        await client.enqueueListResult([])   // initial base probe sees empty → .fresh; under-lock sees the foreign dir
        let writerID = newWriterID()

        await assertThrowsLiteError(.repoDamaged) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
            lockClient: client, basePath: self.basePath, writerID: writerID
            )
        }
        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "unwind must not delete a marker that contains a foreign control dir")
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

    func testBackgroundFreshCommitFailureLeavesEmptyMarkerDirectories() async throws {
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
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "background must leave the uncommitted marker directory in place")
    }

    func testBackgroundUnderLockSkipLeavesEmptyMarkerDirectories() async throws {
        let client = InMemoryRemoteStorageClient()
        let committed = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "t", createdBy: "seed")
        )
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: committed)
        await client.setOnDownload { path in
            if path == RepoLayoutLite.versionPath(basePath: self.basePath) {
                try? await client.delete(path: path)
            }
        }

        let outcome = try await LiteRepoGateway.prepareBackgroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )
        guard case .skip = outcome else { return XCTFail("background current→fresh drift should skip") }

        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.locksDirectoryPath(basePath: basePath)),
                       "background skip must not recursively delete the locks directory")
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "background skip must not recursively delete the marker directory")
    }

    func testMarkerUnwindDoesNotDeletePostProbeConcurrentWrites() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        let writerID = newWriterID()
        let foreignWriterID = newWriterID()
        let basePath = self.basePath
        let versionPath = RepoLayoutLite.versionPath(basePath: basePath)
        let locksDir = RepoLayoutLite.locksDirectoryPath(basePath: basePath)
        let concurrentVersion = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2026-06-12T00:00:00Z", createdBy: "other")
        )
        await client.enqueueExistsPostAction(forPathSuffix: RepoLayoutLite.versionFileName) {}
        await client.enqueueExistsPostAction(forPathSuffix: RepoLayoutLite.versionFileName) {
            await client.seedFile(path: versionPath, data: concurrentVersion)
            await client.seedLock(basePath: basePath, writerID: foreignWriterID, modificationDate: Date())
        }

        await assertThrowsLiteError(.versionCommitFailed) {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
                lockClient: client, basePath: basePath, writerID: writerID
            )
        }

        let deleted = await client.deletedPaths
        XCTAssertFalse(deleted.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)),
                       "a post-probe concurrent write must not be exposed to recursive marker delete")
        XCTAssertFalse(deleted.contains(locksDir),
                       "a post-probe concurrent lock must not be exposed to recursive locks delete")
        let version = await client.fileData(path: versionPath)
        let foreignLock = await client.lockExists(basePath: basePath, writerID: foreignWriterID)
        XCTAssertNotNil(version, "a concurrent version write must survive unwind")
        XCTAssertTrue(foreignLock, "a concurrent lock write must survive unwind")
    }

    // MARK: - Helpers

    private func acquiredSession(
        client: InMemoryRemoteStorageClient,
        writerID: String? = nil,
        now: Date
    ) async throws -> RepoLeaseSession {
        let id = writerID ?? newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client, basePath: basePath, writerID: id, now: now
        )
        return plan.session
    }

    // A background-acquired live session (no gateway init), for the unattended lease-confidence gate.
    private func backgroundAcquiredSession(
        client: InMemoryRemoteStorageClient,
        writerID: String? = nil,
        now: Date
    ) async throws -> RepoLeaseSession {
        let id = writerID ?? newWriterID()
        await client.seedDirectory(RepoLayoutLite.locksDirectoryPath(basePath: basePath))
        await client.setPendingUploadModificationDate(now)
        let lock = try XCTUnwrap(WriteLockService(basePath: basePath, writerID: id, client: client))
        let acquired = await lock.acquire(mode: .background, now: now)
        XCTAssertEqual(acquired, .acquired)
        return RepoLeaseSession(lock: lock)
    }

    private func acquiredSession(
        dataClient: InMemoryRemoteStorageClient,
        lockClient: InMemoryRemoteStorageClient,
        writerID: String,
        now: Date,
        reconnectLockClient: ConnectedLockClientProvider? = nil
    ) async throws -> RepoLeaseSession {
        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: dataClient,
            lockClient: lockClient,
            ownsLockClient: true,
            basePath: basePath,
            writerID: writerID,
            now: now,
            reconnectLockClient: reconnectLockClient
        )
        return plan.session
    }

    private func copyLockBytes(
        writerID: String,
        from source: InMemoryRemoteStorageClient,
        to destination: InMemoryRemoteStorageClient,
        modificationDate: Date?,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let path = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
        let sourceData = await source.fileData(path: path)
        let data = try XCTUnwrap(sourceData, file: file, line: line)
        await destination.seedFile(path: path, data: data, modificationDate: modificationDate)
    }

    private func makePreparedRun(
        client: any RemoteStorageClientProtocol,
        monthPlans: [MonthWorkItem],
        totalAssetCount: Int,
        session: RepoLeaseSession
    ) -> BackupPreparedRun {
        BackupPreparedRun(
            initialClient: client,
            snapshotSeedLookup: nil,
            monthPlans: monthPlans,
            workerCount: 1,
            connectionPoolSize: 1,
            totalAssetCount: totalAssetCount,
            makeClient: { client },
            writeMode: .lite(session, nil)
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
        ), expectedProfileKey: nil)
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
        ), expectedProfileKey: nil)
        XCTAssertTrue(service.allKnownMonths().contains(monthB),
                       "optimistic upsert should add month B to cache")

        // Second sync: remote unchanged → fast path. Must still evict month B.
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)

        XCTAssertFalse(service.allKnownMonths().contains(monthB),
                       "unanchored optimistic month B must be evicted on unchanged fast-path sync")
    }

    // MARK: - Parallel manifest download

    func testParallelDownloadSyncMatchesSerialAndReportsMonotonicProgress() async throws {
        let months = [
            LibraryMonthKey(year: 2024, month: 1),
            LibraryMonthKey(year: 2024, month: 2),
            LibraryMonthKey(year: 2024, month: 3),
            LibraryMonthKey(year: 2024, month: 4)
        ]

        // Serial baseline over the same seed.
        let serialClient = InMemoryRemoteStorageClient()
        for (i, month) in months.enumerated() {
            try await seedPopulatedLiteMonth(serialClient, month: month, hashByte: UInt8(i + 1))
        }
        let serialService = RemoteIndexSyncService()
        let serialDigest = try await serialService.syncIndex(
            client: serialClient, profile: makeProfile(writerID: nil), layout: .lite
        )

        // Parallel run.
        let parallelClient = InMemoryRemoteStorageClient()
        for (i, month) in months.enumerated() {
            try await seedPopulatedLiteMonth(parallelClient, month: month, hashByte: UInt8(i + 1))
        }
        let parallelService = RemoteIndexSyncService()
        let recorder = RemoteSyncProgressRecorder()
        let factoryCalls = CallCounter()
        let parallelDigest = try await parallelService.syncIndex(
            client: parallelClient,
            profile: makeProfile(writerID: nil),
            onSyncProgress: { recorder.append($0) },
            layout: .lite,
            makeClient: { () throws -> any RemoteStorageClientProtocol in
                factoryCalls.bump()
                return parallelClient
            },
            downloadConcurrency: 2
        )

        XCTAssertEqual(parallelDigest.resourceCount, serialDigest.resourceCount)
        XCTAssertEqual(parallelDigest.assetCount, serialDigest.assetCount)
        XCTAssertEqual(parallelService.allKnownMonths(), serialService.allKnownMonths())
        XCTAssertEqual(parallelService.allKnownMonths().count, months.count)
        XCTAssertGreaterThanOrEqual(factoryCalls.count, 1, "parallel path must build a download pool via makeClient")

        // Out-of-order completion still reports monotonic progress reaching the full changed-month count.
        let currents = recorder.snapshots()
            .filter { $0.kind == .remoteIndex && $0.total == months.count }
            .map(\.current)
        XCTAssertEqual(currents, currents.sorted(), "progress current must be monotonic non-decreasing")
        XCTAssertEqual(currents.last, months.count, "progress must reach the total changed-month count")
    }

    func testV1SyncStaysSerialAndNeverBuildsDownloadPool() async throws {
        let client = InMemoryRemoteStorageClient()
        // Two months so totalWorkers would be ≥2 if V1 were (incorrectly) parallelized.
        try await seedPopulatedV1Month(client, month: LibraryMonthKey(year: 2024, month: 2), hashByte: 0xA1)
        try await seedPopulatedV1Month(client, month: LibraryMonthKey(year: 2024, month: 3), hashByte: 0xA2)

        let service = RemoteIndexSyncService()
        let factoryCalls = CallCounter()
        let digest = try await service.syncIndex(
            client: client,
            profile: makeProfile(writerID: nil),
            layout: .v1,
            makeClient: { () throws -> any RemoteStorageClientProtocol in
                factoryCalls.bump()
                return client
            },
            downloadConcurrency: 2
        )

        XCTAssertEqual(factoryCalls.count, 0, "V1 may schema-push on load, so it must never build a download pool")
        XCTAssertEqual(service.allKnownMonths().count, 2)
        XCTAssertEqual(digest.resourceCount, 2)
    }

    func testParallelDownloadDegradesToPrimaryWhenPoolClientCannotConnect() async throws {
        let months = [
            LibraryMonthKey(year: 2024, month: 1),
            LibraryMonthKey(year: 2024, month: 2),
            LibraryMonthKey(year: 2024, month: 3)
        ]
        let client = InMemoryRemoteStorageClient()
        for (i, month) in months.enumerated() {
            try await seedPopulatedLiteMonth(client, month: month, hashByte: UInt8(i + 1))
        }

        // Every pooled (non-primary) connection fails to open; those workers bow out and the primary
        // connection drains all months — the sync still completes.
        struct ConnectFailure: Error {}
        let service = RemoteIndexSyncService()
        let digest = try await service.syncIndex(
            client: client,
            profile: makeProfile(writerID: nil),
            layout: .lite,
            makeClient: { () throws -> any RemoteStorageClientProtocol in throw ConnectFailure() },
            downloadConcurrency: 2
        )

        XCTAssertEqual(service.allKnownMonths().count, months.count, "primary must still sync every month when pool clients can't connect")
        XCTAssertEqual(digest.resourceCount, months.count)
    }

    func testParallelDownloadPartialPoolFailureStillCompletes() async throws {
        let months = (1 ... 5).map { LibraryMonthKey(year: 2024, month: $0) }
        let client = InMemoryRemoteStorageClient()
        for (i, month) in months.enumerated() {
            try await seedPopulatedLiteMonth(client, month: month, hashByte: UInt8(i + 1))
        }

        // concurrency 3 → 2 non-primary workers; only the first pooled connection opens, the second fails.
        // The primary + the one working pooled worker must still drain all months.
        let factory = OneShotClientFactory(client)
        let service = RemoteIndexSyncService()
        let digest = try await service.syncIndex(
            client: client,
            profile: makeProfile(writerID: nil),
            layout: .lite,
            makeClient: { try factory.make() },
            downloadConcurrency: 3
        )

        XCTAssertEqual(service.allKnownMonths().count, months.count, "partial pool failure must still sync every month")
        XCTAssertEqual(digest.resourceCount, months.count)
    }

    // MARK: - Manifest sqlite tuning

    func testManifestQueueDisablesJournalFsync() throws {
        let tmp = FileManager.default.temporaryDirectory
            .appendingPathComponent("wm-pragma-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: tmp) }
        let queue = try MonthManifestStore.makeManifestQueue(path: tmp.path)
        defer { try? queue.close() }
        try queue.read { db in
            XCTAssertEqual(try String.fetchOne(db, sql: "PRAGMA journal_mode"), "memory")
            XCTAssertEqual(try Int.fetchOne(db, sql: "PRAGMA synchronous"), 0)
        }
    }

    func testReloadCacheDecodesAllFieldsRoundTrip() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 7)
        await client.seedDirectory("\(basePath)/2024/07")

        let seedStore = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: month.year, month: month.month, layout: .lite,
            assertOwnership: {}
        )
        let resource = TestFixtures.remoteResource(
            year: month.year, month: month.month, contentHash: Data([0xDE, 0xAD]), fileName: "photo.jpg"
        )
        try seedStore.upsertResource(resource)
        // A second resource with NULL creationDateMs locks in the nullable positional decode (row[4]).
        let nullDateResource = RemoteManifestResource(
            year: month.year, month: month.month,
            fileName: "nodate.jpg", contentHash: Data([0xCA, 0xFE]),
            fileSize: 99, resourceType: 1, creationDateMs: nil, backedUpAtMs: 1_700_000_400_000
        )
        try seedStore.upsertResource(nullDateResource)
        let asset = RemoteManifestAsset(
            year: month.year, month: month.month,
            assetFingerprint: Data([0xBE, 0xEF]),
            creationDateMs: 1_700_000_000_000, backedUpAtMs: 1_700_000_500_000,
            resourceCount: 1, totalFileSizeBytes: 12345
        )
        let link = RemoteAssetResourceLink(
            year: month.year, month: month.month,
            assetFingerprint: Data([0xBE, 0xEF]), resourceHash: Data([0xDE, 0xAD]), role: 1, slot: 0
        )
        try seedStore.upsertAsset(asset, links: [link])
        _ = try await seedStore.flushToRemote()

        guard let loaded = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: month.year, month: month.month, layout: .lite
        ) else {
            return XCTFail("manifest should load")
        }
        let snapshot = loaded.unsortedSnapshot()

        XCTAssertEqual(snapshot.resources.count, 2)
        let r = try XCTUnwrap(snapshot.resources.first { $0.fileName == "photo.jpg" })
        XCTAssertEqual(r.contentHash, Data([0xDE, 0xAD]))
        XCTAssertEqual(r.fileSize, resource.fileSize)
        XCTAssertEqual(r.resourceType, resource.resourceType)
        let rNull = try XCTUnwrap(snapshot.resources.first { $0.fileName == "nodate.jpg" })
        XCTAssertNil(rNull.creationDateMs, "NULL creationDateMs must decode to nil")
        XCTAssertEqual(rNull.fileSize, 99)

        let a = try XCTUnwrap(snapshot.assets.first)
        XCTAssertEqual(snapshot.assets.count, 1)
        XCTAssertEqual(a.assetFingerprint, Data([0xBE, 0xEF]))
        XCTAssertEqual(a.creationDateMs, 1_700_000_000_000)
        XCTAssertEqual(a.backedUpAtMs, 1_700_000_500_000)
        XCTAssertEqual(a.resourceCount, 1)
        XCTAssertEqual(a.totalFileSizeBytes, 12345)

        let l = try XCTUnwrap(snapshot.links.first)
        XCTAssertEqual(snapshot.links.count, 1)
        XCTAssertEqual(l.assetFingerprint, Data([0xBE, 0xEF]))
        XCTAssertEqual(l.resourceHash, Data([0xDE, 0xAD]))
        XCTAssertEqual(l.role, 1)
        XCTAssertEqual(l.slot, 0)
    }
}
