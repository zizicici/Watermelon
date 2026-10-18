import XCTest
import GRDB
@testable import Watermelon

final class RemoteIndexCommitTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2024, month: 3)
    private let basePath = "/photos"

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(id: 1, name: "test", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "test", port: 443, shareName: "",
            basePath: basePath, username: "u", domain: nil, credentialRef: "test",
            backgroundBackupEnabled: false, createdAt: Date(), updatedAt: Date())
    }

    private func makeStore(_ client: InMemoryRemoteStorageClient, cache: RemoteLibrarySnapshotCache) throws -> MonthManifestStore {
        let url = MonthManifestStore.makeLocalManifestURL(year: month.year, month: month.month)
        let queue = try DatabaseQueue(path: url.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(client: client, basePath: basePath, year: month.year, month: month.month,
            localManifestURL: url, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: .uniform({}),
            snapshotObserver: cache.snapshotObserver(profileKey: cache.currentProfileKey()!))
    }

    @discardableResult
    private func addAsset(_ byte: UInt8, to store: MonthManifestStore) throws -> Data {
        let hash = Data(repeating: byte, count: 32)
        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)])
        try store.upsertResource(TestFixtures.remoteResource(year: month.year, month: month.month,
            contentHash: hash, fileName: "\(byte).jpg"))
        try store.upsertAsset(TestFixtures.remoteAsset(year: month.year, month: month.month,
            fingerprint: fingerprint), links: [TestFixtures.remoteLink(year: month.year, month: month.month,
                assetFingerprint: fingerprint, resourceHash: hash)])
        return fingerprint
    }

    func testWorkingChangesPublishAsOneCompleteMonthOnlyAfterFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let owner = RemoteIndexSyncService.remoteProfileKey(profile())
        cache.setProfileKey(owner)
        let store = try makeStore(client, cache: service.snapshotCache)
        let fingerprint = try addAsset(1, to: store)
        XCTAssertTrue(service.snapshotCache.current().assets.isEmpty)
        let before = service.snapshotCache.currentRevision()
        let observed = expectation(description: "one atomic publication")
        observed.assertForOverFulfill = true
        let token = cache.changes.addObserver {
            let state = cache.state(since: before)
            XCTAssertEqual(state.monthDeltas.first?.assets.count, 1)
            XCTAssertEqual(state.monthDeltas.first?.resources.count, 1)
            XCTAssertEqual(state.monthDeltas.first?.assetResourceLinks.count, 1)
            observed.fulfill()
        }
        defer { cache.changes.removeObserver(token) }
        try await store.flushToRemote()
        await fulfillment(of: [observed], timeout: 2)
        XCTAssertTrue(cache.containsAssetFingerprint(fingerprint).contains)
        let committedRevision = service.snapshotCache.currentRevision()
        try await store.flushToRemote()
        XCTAssertEqual(service.snapshotCache.currentRevision(), committedRevision)
    }

    @MainActor
    func testManifestIOPublishesOffMainActorWhenCalledFromUI() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("owner")
        let store = try makeStore(client, cache: cache)
        try addAsset(1, to: store)
        await client.seedFile(path: "\(basePath)/2024/03/1.jpg")
        let published = expectation(description: "flush, load and verify publish off main")
        published.expectedFulfillmentCount = 3
        let report: @Sendable () -> Void = {
            XCTAssertFalse(Thread.isMainThread)
            published.fulfill()
        }
        let token = cache.changes.addObserver(report)
        defer { cache.changes.removeObserver(token) }
        try await store.flushToRemote()
        _ = try await MonthManifestStore.loadOrCreate(client: client, basePath: basePath,
            year: month.year, month: month.month, layout: .lite, assertOwnership: .uniform({}),
            snapshotObserver: { _ in report() })
        try await MonthManifestStore.verifyMonth(client: client, basePath: basePath, month: month,
            layout: .lite, assertOwnership: .uniform({}), snapshotObserver: { _ in report() })
        await fulfillment(of: [published], timeout: 2)
    }

    func testCleanLoadAndFlushPreserveManifestDigest() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(Date(timeIntervalSince1970: 1_000))
        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        await client.seedFile(path: "\(basePath)/2024/03/1.jpg")
        try await store.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let downloads = await client.downloadAttemptPaths.count

        try await store.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let afterCleanFlush = await client.downloadAttemptPaths.count
        XCTAssertEqual(afterCleanFlush, downloads)

        let snapshot = store.unsortedSnapshot()
        let seed = MonthManifestStore.Seed(resources: snapshot.resources, assets: snapshot.assets,
            assetResourceLinks: snapshot.links)
        let loaded = try await MonthManifestStore.loadOrCreate(client: client, basePath: basePath, year: month.year, month: month.month,
            seed: seed, layout: .lite, assertOwnership: .uniform({}),
            snapshotObserver: cacheObserver(service))
        try await loaded.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let afterCleanLoad = await client.downloadAttemptPaths.count
        XCTAssertEqual(afterCleanLoad, downloads)
    }

    func testCancelledManifestReadPreservesConfirmedSnapshotAndAllowsScanRetry() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("writer")
        let store = try makeStore(client, cache: cache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        let service = RemoteIndexSyncService()
        await client.setOnDownload { _ in
            withUnsafeCurrentTask { $0?.cancel() }
        }
        let profile = profile()
        let scan = Task { try await service.syncIndex(client: client, profile: profile, layout: .lite) }
        do {
            _ = try await scan.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {} catch {
            XCTFail("Expected cancellation, got \(error)")
        }
        XCTAssertTrue(service.snapshotCache.current().assets.isEmpty)
        let month = month
        let basePath = basePath
        let verify = Task {
            try await MonthManifestStore.verifyMonth(client: client, basePath: basePath, month: month,
                layout: .lite, snapshotObserver: cache.snapshotObserver(profileKey: "writer"))
        }
        do {
            try await verify.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {} catch {
            XCTFail("Expected cancellation, got \(error)")
        }
        XCTAssertEqual(cache.current().assets.count, 1)
        await client.setOnDownload(nil)
        _ = try await service.syncIndex(client: client, profile: profile, layout: .lite)
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)
    }

    func testCommittedFlushInvalidatesEvenUnchangedManifestMetadata() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(Date(timeIntervalSince1970: 1_000))
        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let previousEntries = try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath))

        try addAsset(2, to: store)
        try await store.flushToRemote()
        let beforeReload = await client.downloadAttemptPaths.count
        await client.enqueueListResult(previousEntries)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let afterReload = await client.downloadAttemptPaths.count
        XCTAssertEqual(afterReload, beforeReload + 1)
        XCTAssertEqual(service.snapshotCache.current().assets.count, 2)
    }

    func testFailedLaterFlushPreservesCheckpointAndRequiresFullReconciliation() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let store = try makeStore(client, cache: service.snapshotCache)
        let committed = try addAsset(1, to: store)
        try await store.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let downloads = await client.downloadAttemptPaths.count
        try addAsset(2, to: store)
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        do {
            try await store.flushToRemote()
            XCTFail("Expected upload failure")
        } catch {}
        XCTAssertEqual(service.snapshotCache.current().assets.map(\.assetFingerprint), [committed])
        XCTAssertFalse(service.snapshotCache.state(since: nil).isAuthoritative)
        XCTAssertNil(cache.containsAssetFingerprint(committed).profileKey)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite, monthFilter: [month])
        XCTAssertTrue(service.snapshotCache.requiresReconciliation)
        let afterScoped = await client.downloadAttemptPaths.count
        XCTAssertGreaterThan(afterScoped, downloads, "Unchanged metadata must not skip uncertain months")
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        XCTAssertFalse(service.snapshotCache.requiresReconciliation)
        XCTAssertTrue(service.snapshotCache.state(since: nil).isAuthoritative)
        XCTAssertEqual(service.snapshotCache.current().assets.map(\.assetFingerprint), [committed])
    }

    func testLateFlushCannotPublishIntoAnotherProfile() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        cache.setProfileKey("a")
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        await client.setOnUpload { cache.reset(); cache.setProfileKey("b") }
        try await store.flushToRemote()
        XCTAssertTrue(service.snapshotCache.current().assets.isEmpty)
        XCTAssertEqual(cache.currentProfileKey(), "b")
        XCTAssertFalse(service.snapshotCache.requiresReconciliation)
    }

    func testCancellationAfterDurableVerificationStillPublishes() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        cache.setProfileKey("owner")
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        await client.setOnDownload { _ in
            await client.enqueueExistsPostAction(forPathSuffix: ".bak") {
                withUnsafeCurrentTask { $0?.cancel() }
            }
        }
        let task = Task {
            try await store.flushToRemote()
            XCTAssertTrue(Task.isCancelled)
        }
        try await task.value
        XCTAssertFalse(store.dirty)
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)
        XCTAssertFalse(service.snapshotCache.requiresReconciliation)
    }

    func testCancelledCheckpointPublishesWithoutWaitingForScan() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        await client.enqueueListResult([])
        await client.blockNextListUntilOpened()
        let scan = Task { try await service.syncIndex(client: client, profile: profile(), layout: .lite) }
        await client.waitUntilBlockedListEntered()
        let started = RestoreTestLatch()
        let committed = expectation(description: "checkpoint does not wait for scan")
        let flush = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            await started.open()
            try await store.flushToRemote(ignoreCancellation: true)
            committed.fulfill()
        }
        await started.wait()
        await fulfillment(of: [committed], timeout: 2)
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)
        await client.openBlockedList()
        _ = try await scan.value
        try await flush.value
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)
        XCTAssertFalse(store.dirty)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)
    }

    func testSlowMonthLoadDoesNotBlockAnotherMonthsCommit() async throws {
        let slow = InMemoryRemoteStorageClient()
        let fast = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey(RemoteIndexSyncService.remoteProfileKey(profile()))
        let store = try makeStore(fast, cache: cache)
        try addAsset(1, to: store)
        await slow.seedDirectory("\(basePath)/2024/04")
        await slow.blockNextListUntilOpened()
        let load = Task {
            try await MonthManifestStore.loadOrCreate(client: slow, basePath: basePath, year: 2024, month: 4,
                layout: .lite, assertOwnership: .uniform({}),
                snapshotObserver: cache.snapshotObserver(profileKey: RemoteIndexSyncService.remoteProfileKey(profile())))
        }
        await slow.waitUntilBlockedListEntered()
        let committed = expectation(description: "independent month committed")
        let flush = Task { try await store.flushToRemote(); committed.fulfill() }
        await fulfillment(of: [committed], timeout: 2)
        XCTAssertEqual(cache.current().assets.count, 1)
        await slow.openBlockedList()
        _ = try await load.value
        try await flush.value
    }

    func testOldScanCannotOverwriteCommitOrRestoreItsDigest() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(Date(timeIntervalSince1970: 1_000))
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        cache.setProfileKey(RemoteIndexSyncService.remoteProfileKey(profile()))
        let store = try makeStore(client, cache: cache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        let captured = RestoreTestLatch()
        let release = RestoreTestLatch()
        await client.setOnDownload { _ in
            await client.setOnDownload(nil)
            await captured.open()
            await release.wait()
        }
        let scan = Task { try await service.syncIndex(client: client, profile: profile(), layout: .lite) }
        await captured.wait()
        try addAsset(2, to: store)
        try await store.flushToRemote()
        await release.open()
        _ = try await scan.value
        XCTAssertEqual(cache.current().assets.count, 2)
        XCTAssertTrue(cache.requiresReconciliation)
        XCTAssertTrue(cache.state(since: nil).isAuthoritative)
        let before = await client.downloadAttemptPaths.count
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let after = await client.downloadAttemptPaths.count
        XCTAssertEqual(after, before + 1)
        XCTAssertEqual(cache.current().assets.count, 2)
        XCTAssertFalse(cache.requiresReconciliation)
    }

    func testContextClaimCannotStealForeignOwner() {
        let cache = RemoteLibrarySnapshotCache()
        XCTAssertTrue(cache.claimIfUnowned(profileKey: "a"))
        XCTAssertFalse(cache.claimIfUnowned(profileKey: "b"))
        XCTAssertEqual(cache.currentProfileKey(), "a")
    }

    func testIndependentBackgroundCommitReachesForegroundAndRejectsItsOldScan() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(Date(timeIntervalSince1970: 1_000))
        let foreground = RemoteIndexSyncService()
        let background = RemoteIndexSyncService()
        let owner = RemoteIndexSyncService.remoteProfileKey(profile())
        foreground.snapshotCache.setProfileKey(owner)
        background.snapshotCache.setProfileKey(owner)
        let store = try makeStore(client, cache: background.snapshotCache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        let captured = RestoreTestLatch()
        let release = RestoreTestLatch()
        await client.setOnDownload { _ in
            await client.setOnDownload(nil)
            await captured.open()
            await release.wait()
        }
        let scan = Task { try await foreground.syncIndex(client: client, profile: profile(), layout: .lite) }
        await captured.wait()
        try addAsset(2, to: store)
        try await store.flushToRemote()
        XCTAssertEqual(foreground.snapshotCache.current().assets.count, 2)
        await release.open()
        _ = try await scan.value
        XCTAssertEqual(foreground.snapshotCache.current().assets.count, 2)
        XCTAssertTrue(foreground.snapshotCache.requiresReconciliation)
        let before = await client.downloadAttemptPaths.count
        _ = try await foreground.syncIndex(client: client, profile: profile(), layout: .lite)
        let after = await client.downloadAttemptPaths.count
        XCTAssertEqual(after, before + 1)
        XCTAssertEqual(foreground.snapshotCache.current().assets.count, 2)
        XCTAssertFalse(foreground.snapshotCache.requiresReconciliation)
    }

    private func cacheObserver(_ service: RemoteIndexSyncService) -> MonthManifestStore.SnapshotObserver {
        service.snapshotCache.snapshotObserver(profileKey: RemoteIndexSyncService.remoteProfileKey(profile()))
    }

    func testLoadTimeReconciliationPublishesBeforeAnyLaterAssetWork() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(Date(timeIntervalSince1970: 1_000))
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let previousEntries = try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        XCTAssertEqual(service.snapshotCache.current().assets.count, 1)

        let loaded = try await MonthManifestStore.loadOrCreate(client: client, basePath: basePath, year: month.year, month: month.month,
            layout: .lite, assertOwnership: .uniform({}),
            snapshotObserver: cacheObserver(service))
        XCTAssertFalse(loaded.dirty)
        XCTAssertTrue(loaded.unsortedSnapshot().assets.isEmpty)
        XCTAssertTrue(service.snapshotCache.current().assets.isEmpty)
        XCTAssertTrue(service.snapshotCache.state(since: nil).isAuthoritative)
        let beforeReload = await client.downloadAttemptPaths.count
        await client.enqueueListResult(previousEntries)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        let afterReload = await client.downloadAttemptPaths.count
        XCTAssertEqual(afterReload, beforeReload + 1)
    }

    func testFailureAfterLoadTimeCommitInvalidatesRetainedSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let owner = RemoteIndexSyncService.remoteProfileKey(profile())
        cache.setProfileKey(owner)
        let store = try makeStore(client, cache: service.snapshotCache)
        try addAsset(1, to: store)
        try await store.flushToRemote()
        await client.setOnDownload { _ in
            await client.enqueueExistsPostAction(forPathSuffix: ".bak") {
                await client.enqueueCreateDirectoryError(RemoteErrorFixtures.terminal)
            }
        }
        do {
            _ = try await MonthManifestStore.loadOrCreate(client: client, basePath: basePath, year: month.year, month: month.month,
                layout: .lite, assertOwnership: .uniform({}),
            snapshotObserver: cacheObserver(service))
            XCTFail("Expected directory creation failure after manifest reconciliation")
        } catch {}
        XCTAssertTrue(service.snapshotCache.requiresReconciliation)
        XCTAssertFalse(service.snapshotCache.state(since: nil).isAuthoritative)
        await client.setOnDownload(nil)
        _ = try await service.syncIndex(client: client, profile: profile(), layout: .lite)
        XCTAssertTrue(service.snapshotCache.current().assets.isEmpty)
        XCTAssertFalse(service.snapshotCache.requiresReconciliation)
    }

    @MainActor
    func testUncertainRemoteStateStillAllowsLocalBrowsingButNotBackupVerdicts() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let coordinator = BackupCoordinator(photoLibraryService: dependencies.photoLibraryService,
            storageClientFactory: dependencies.storageClientFactory, hashIndexRepository: dependencies.hashIndexRepository,
            databaseManager: database, remoteIndexService: service)
        cache.setProfileKey("owner")
        let store = try makeStore(InMemoryRemoteStorageClient(), cache: service.snapshotCache)
        let fingerprint = try addAsset(1, to: store)
        try await store.flushToRemote()
        cache.invalidateIfOwned(by: "owner")
        let seed = HomeBrowserLocalSeed(localIDByFingerprint: [fingerprint: "local"], assets: [
            HomeBrowserLocalAsset(localIdentifier: "local", month: month, kind: .photo,
                creationDateMs: 1_000, fingerprint: fingerprint)
        ], monthGroupingTimeZone: .frozenCurrent())
        let presence = LibraryPresenceIndex(hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: coordinator, profileKey: { "owner" }, homeLocalSeedProvider: { seed })
        let source = LocalMediaSource(photoLibraryService: dependencies.photoLibraryService,
            hashIndexRepository: dependencies.hashIndexRepository, presenceIndex: presence, fileSizeLoader: { _ in [:] })
        guard case .loaded(let content) = await source.load() else {
            return XCTFail("Remote repair must not block local browsing")
        }
        XCTAssertEqual(content.sections.flatMap(\.items).map(\.id), [.local("local")])
        XCTAssertEqual(presence.backupPresenceVerdict(fingerprint), .unknown)
        XCTAssertFalse(presence.isRemotePresenceAuthoritative)
        XCTAssertFalse(presence.isRemotePresenceCurrent)
        let projection = await presence.prepareRemoteBrowserProjection()
        XCTAssertNotNil(projection, "Last confirmed remote content remains readable")
    }

    @MainActor
    func testConfirmedEmptyRepoPublishesOwnedEmptyBrowserProjection() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let dependencies = DependencyContainer(databaseManager: database, reconcileOneDriveAccounts: false)
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let coordinator = BackupCoordinator(photoLibraryService: dependencies.photoLibraryService,
            storageClientFactory: dependencies.storageClientFactory, hashIndexRepository: dependencies.hashIndexRepository,
            databaseManager: database, remoteIndexService: service)
        cache.setProfileKey("owner")
        let store = try makeStore(InMemoryRemoteStorageClient(), cache: service.snapshotCache)
        let fingerprint = try addAsset(1, to: store)
        try await store.flushToRemote()
        cache.invalidateIfOwned(by: "owner")
        let seed = HomeBrowserLocalSeed(localIDByFingerprint: [:], assets: [],
            monthGroupingTimeZone: .frozenCurrent())
        let presence = LibraryPresenceIndex(hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: coordinator, profileKey: { "owner" }, homeLocalSeedProvider: { seed })
        let previousProjection = await presence.prepareRemoteBrowserProjection()
        XCTAssertEqual(previousProjection?.assetsByMonth.values.reduce(0, { $0 + $1.count }), 1)

        let published = expectation(description: "owned empty snapshot")
        published.assertForOverFulfill = true
        let token = cache.changes.addObserver {
            let state = cache.state(since: nil)
            XCTAssertEqual(state.profileKey, "owner")
            XCTAssertTrue(state.isAuthoritative)
            XCTAssertTrue(state.monthDeltas.isEmpty)
            published.fulfill()
        }
        defer { cache.changes.removeObserver(token) }
        _ = service.snapshotCache.resetIfOwned(by: "owner")
        await fulfillment(of: [published], timeout: 2)

        let projection = await presence.prepareRemoteBrowserProjection()
        XCTAssertNotNil(projection)
        XCTAssertTrue(projection?.assetsByMonth.isEmpty == true)
        XCTAssertTrue(presence.isRemotePresenceAuthoritative)
        XCTAssertTrue(presence.isRemotePresenceCurrent)
        XCTAssertEqual(presence.backupPresenceVerdict(fingerprint), .absent)
        XCTAssertFalse(service.snapshotCache.requiresReconciliation)
    }
}
