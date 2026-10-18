import CryptoKit
import XCTest
import GRDB
@testable import Watermelon

@MainActor
final class LibraryMutationIndexTests: XCTestCase {
    private enum UploadFailure: Error { case flushFailed }

    func testCommittedChangesAcrossManagersReconcileBrowserWithoutActionCallbacks() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let background = try DatabaseManager(databaseURL: fixture.directory.appendingPathComponent("index.sqlite"))
        XCTAssertTrue(background.localIndexChanges === fixture.publisher)
        try ContentHashIndexRepository(databaseManager: background).writeHashIndex(
            assetLocalIdentifier: Fixture.localID, instances: [Fixture.instance])
        try await fixture.assertReconciled()

        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.hashIndexRepository.clearLocalHashIndex()
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
        let refreshed = await fixture.presence.refresh(notifyOnCommit: false)
        XCTAssertTrue(refreshed)
        XCTAssertEqual(Set(try fixture.mergedItems().map(\.presence)), [.localOnly, .remoteOnly])
    }

    func testMainThreadWriteInvalidatesImmediatelyAndDeliversUIAfterCommit() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let delivered = expectation(description: "Committed presence reaches the UI without blocking the writer")
        let presence = fixture.presence
        let repository = fixture.dependencies.hashIndexRepository
        let token = NotificationCenter.default.addObserver(forName: .LibraryPresenceDidChange,
            object: nil, queue: .main) { notification in
                guard notification.object as? LibraryPresenceIndex === presence else { return }
                XCTAssertNotNil(try? repository.fetchAssetHashCaches(assetIDs: ["new-screenshot"])["new-screenshot"])
                delivered.fulfill()
            }
        defer { NotificationCenter.default.removeObserver(token) }
        try fixture.writeFingerprint()
        XCTAssertNil(presence.browserLocalProjectionInput().seed)
        await fulfillment(of: [delivered], timeout: 3)
    }

    func testRemoteInvalidationIsImmediateCoalescedAndScopedToItsCache() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let coordinator = BackupCoordinator(photoLibraryService: fixture.dependencies.photoLibraryService,
            storageClientFactory: fixture.dependencies.storageClientFactory,
            hashIndexRepository: fixture.dependencies.hashIndexRepository,
            databaseManager: fixture.dependencies.databaseManager, remoteIndexService: service)
        let presence = LibraryPresenceIndex(hashIndexRepository: fixture.dependencies.hashIndexRepository,
            coordinator: coordinator, profileKey: { nil })
        let refreshed = await presence.refresh(notifyOnCommit: false)
        XCTAssertTrue(refreshed)
        XCTAssertTrue(presence.isRemotePresenceCurrent)

        let count = NotificationCount()
        let delivered = expectation(description: "One UI invalidation from the owned cache")
        let token = NotificationCenter.default.addObserver(forName: .LibraryPresenceDidChange,
            object: nil, queue: .main) { notification in
                guard notification.object as? LibraryPresenceIndex === presence else { return }
                count.increment()
                delivered.fulfill()
            }
        defer { NotificationCenter.default.removeObserver(token) }
        let backgroundCache = RemoteLibrarySnapshotCache()
        for _ in 0..<100 { backgroundCache.reset() }
        XCTAssertTrue(presence.isRemotePresenceCurrent)
        await withCheckedContinuation { continuation in
            DispatchQueue.main.async { continuation.resume() }
        }
        XCTAssertEqual(count.value, 0)

        for _ in 0..<100 { cache.reset() }
        XCTAssertFalse(presence.isRemotePresenceCurrent)
        XCTAssertEqual(count.value, 0)
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(count.value, 1)
    }

    func testMutationsDuringNotificationDeliveryScheduleAnotherDelivery() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let presence = fixture.presence
        let repository = fixture.dependencies.hashIndexRepository
        let count = NotificationCount()
        let delivered = expectation(description: "Reentrant commit remains observable")
        delivered.expectedFulfillmentCount = 2
        let token = NotificationCenter.default.addObserver(forName: .LibraryPresenceDidChange,
            object: nil, queue: .main) { notification in
                guard notification.object as? LibraryPresenceIndex === presence else { return }
                count.increment()
                if count.value == 1 {
                    do { try repository.clearLocalHashIndex() }
                    catch { XCTFail("Reentrant write failed: \(error)") }
                }
                delivered.fulfill()
            }
        defer { NotificationCenter.default.removeObserver(token) }
        try fixture.writeFingerprint()
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(count.value, 2)
        XCTAssertTrue(try repository.fetchAssetHashCaches(assetIDs: [Fixture.localID]).isEmpty)
    }

    private final class NotificationCount: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        func increment() { lock.withLock { count += 1 } }
        var value: Int { lock.withLock { count } }
    }

    func testRollbackDoesNotPublishOrAdvanceRevision() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let revision = fixture.publisher.currentRevision
        do {
            try fixture.dependencies.databaseManager.write { db in
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                               arguments: [Fixture.localID, Fixture.fingerprint, Date()])
                throw UploadFailure.flushFailed
            }
            XCTFail("Rollback must throw")
        } catch UploadFailure.flushFailed { }
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
        XCTAssertTrue(fixture.publisher.delta(since: revision).assetIDs.isEmpty)
    }

    func testRestoreCancellationPublishesCompletedImport() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Fixture.bytes)
        let service = fixture.restoreService(client: client, cancelAfterImport: true)

        do {
            _ = try await Task {
                try await service.restoreItems(items: [Fixture.descriptor], profile: Fixture.profile(), password: "",
                    onItemCompleted: { _, _, _ in })
            }.value
        } catch is CancellationError { }

        try await fixture.assertReconciled()
    }

    func testFailedBatchRestoreKeepsCompletedImportsReconciled() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Fixture.bytes)
        await client.enqueueDownloadData(Data("corrupt".utf8))
        let service = fixture.restoreService(client: client)
        let invalid = RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: Data(repeating: 0x44, count: 32),
            fileName: "second.png", fileSize: 7, remoteRelativePath: "2026/10/second.png", creationDateMs: nil)
        let second = RestoreService.RestoreItemDescriptor(instances: [invalid], identity: Data(repeating: 0x55, count: 32))

        do {
            _ = try await service.restoreItems(items: [Fixture.descriptor, second], profile: Fixture.profile(), password: "",
                onItemCompleted: { _, _, _ in })
            XCTFail("The corrupt second item must fail")
        } catch { }

        try await fixture.assertReconciled()
    }

    func testCancelledPreflightReconcilesRowsCommittedBeforeItStops() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let builder = CancellingBuilder { try fixture.writeFingerprint() }
        let task = Task { @MainActor in
            try await LocalDownloadIndexPreflight.run(assetIDs: [Fixture.localID], buildService: builder,
                iCloudPhotoBackupMode: .disable)
        }
        do {
            try await task.value
            XCTFail("Cancellation must propagate")
        } catch is CancellationError { }
        try await fixture.assertReconciled()
    }

    private final class CancellingBuilder: LocalHashIndexBuilding {
        let write: () throws -> Void
        init(write: @escaping () throws -> Void) { self.write = write }
        func buildIndex(for assetIDs: Set<String>, workerCount: Int, allowNetworkAccess: Bool,
                        progressHandler: LocalHashIndexProgressHandler?, tickHandler: LocalHashIndexProgressTickHandler?) async throws -> LocalHashIndexBuildResult {
            try write()
            withUnsafeCurrentTask { $0?.cancel() }
            try Task.checkCancellation()
            fatalError("Cancellation must throw")
        }
    }

    func testDeletingIndexEntryDoesNotMeanDeletingPhotosAsset() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try fixture.writeFingerprint()
        try await fixture.assertReconciled()
        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.hashIndexRepository.deleteIndexEntries(assetIDs: [Fixture.localID])
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
        let refreshed = await fixture.presence.refresh(notifyOnCommit: false)
        XCTAssertTrue(refreshed)
        XCTAssertEqual(Set(try fixture.mergedItems().map(\.presence)), [.localOnly, .remoteOnly])
    }

    func testSizeOnlyUpdatesDoNotInvalidateFingerprints() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try fixture.writeFingerprint()
        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.hashIndexRepository.upsertAssetSizes([
            AssetSizeUpdate(assetLocalIdentifier: Fixture.localID, totalFileSizeBytes: 456, modificationDateMs: 1)
        ])
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
    }

    func testFirstSizeScanDoesNotInvalidatePresenceOrOverflowHistory() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try await fixture.primeStaleSeed()
        let revision = fixture.publisher.currentRevision
        let entries = (0...8_192).map {
            AssetSizeUpdate(assetLocalIdentifier: "size-\($0)", totalFileSizeBytes: 100, modificationDateMs: 1)
        }
        try fixture.dependencies.hashIndexRepository.upsertAssetSizes(entries)
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
        XCTAssertTrue(fixture.publisher.delta(since: revision).assetIDs.isEmpty)
        XCTAssertNotNil(fixture.presence.browserLocalProjectionInput().seed)
        try fixture.dependencies.hashIndexRepository.clearLocalHashIndex()
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
    }

    func testClearingFingerprintAndRevalidatingAgePublishChanges() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try fixture.writeFingerprint()
        var revision = fixture.publisher.currentRevision
        try fixture.dependencies.databaseManager.write { db in
            try db.execute(sql: "UPDATE local_assets SET updatedAt = ? WHERE assetLocalIdentifier = ?",
                           arguments: [Date(timeIntervalSince1970: 1), Fixture.localID])
        }
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
        revision = fixture.publisher.currentRevision
        try fixture.dependencies.databaseManager.write { db in
            try db.execute(sql: "UPDATE local_assets SET assetFingerprint = NULL WHERE assetLocalIdentifier = ?",
                           arguments: [Fixture.localID])
        }
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
        revision = fixture.publisher.currentRevision
        try fixture.dependencies.hashIndexRepository.deleteIndexEntries(assetIDs: [Fixture.localID])
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
    }

    func testSizeOnlyRowBecomesObservableWhenFingerprintIsWritten() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let repository = fixture.dependencies.hashIndexRepository
        var revision = fixture.publisher.currentRevision
        try repository.upsertAssetSizes([
            AssetSizeUpdate(assetLocalIdentifier: Fixture.localID, totalFileSizeBytes: 100, modificationDateMs: 1)
        ])
        XCTAssertEqual(fixture.publisher.currentRevision, revision)
        try fixture.writeFingerprint()
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
        revision = fixture.publisher.currentRevision
        try repository.deleteIndexEntries(assetIDs: [Fixture.localID])
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
    }

    func testTransactionCoalescesChangesAndTracksDeletedRowIDReuse() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try fixture.writeFingerprint()
        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.databaseManager.write { db in
            try db.execute(sql: "DELETE FROM local_assets")
            try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                           arguments: ["replacement", Fixture.fingerprint, Date()])
        }
        XCTAssertEqual(fixture.publisher.currentRevision, revision + 1)
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID, "replacement"])
    }

    func testRolledBackSavepointDoesNotLeakIDsIntoCommit() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.databaseManager.write { db in
            try db.inSavepoint {
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                               arguments: ["rolled-back", Fixture.fingerprint, Date()])
                return .rollback
            }
            try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                           arguments: [Fixture.localID, Fixture.fingerprint, Date()])
        }
        XCTAssertEqual(fixture.publisher.delta(since: revision).assetIDs, [Fixture.localID])
    }

    func testBoundedHistoryFallsBackToFullReconciliation() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let revision = fixture.publisher.currentRevision
        try fixture.dependencies.databaseManager.write { db in
            for index in 0...8_192 {
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                               arguments: ["asset-\(index)", Fixture.fingerprint, Date()])
            }
        }
        guard case .bulkInvalidation = fixture.publisher.delta(since: revision).change else {
            return XCTFail("A lagging reader must reload when its history is no longer retained")
        }
        let caughtUp = fixture.publisher.currentRevision
        XCTAssertTrue(fixture.publisher.delta(since: caughtUp).assetIDs.isEmpty)
        try fixture.writeFingerprint()
        XCTAssertEqual(fixture.publisher.delta(since: caughtUp).assetIDs, [Fixture.localID])

        let beforeClear = fixture.publisher.currentRevision
        try fixture.dependencies.hashIndexRepository.clearLocalHashIndex()
        guard case .bulkInvalidation = fixture.publisher.delta(since: beforeClear).change else {
            return XCTFail("Clearing a large index must invalidate the full snapshot")
        }
    }

    func testHistoryCompactionKeepsCaughtUpReadersIncremental() throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        try fixture.dependencies.databaseManager.write { db in
            for index in 0..<8_192 {
                try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                               arguments: ["asset-\(index)", Fixture.fingerprint, Date()])
            }
        }
        let caughtUp = fixture.publisher.currentRevision
        try fixture.writeFingerprint()
        XCTAssertEqual(fixture.publisher.delta(since: caughtUp).assetIDs, [Fixture.localID])
    }

    private final class HomeSeed: @unchecked Sendable {
        private let lock = NSLock()
        private let engine = HomeLocalIndexEngine()
        private let repository: ContentHashIndexRepository
        private var revision: UInt64 = 0

        init(repository: ContentHashIndexRepository) {
            self.repository = repository
            _ = engine.reload(payload: TestFixtures.initialPayload([[
                TestFixtures.snapshot(id: "new-screenshot", year: 2026, month: 10),
            ]]), fingerprintByAsset: [:], remoteFingerprintsForMonth: { _ in [] })
        }

        var current: HomeBrowserLocalSeed? {
            lock.withLock {
                let delta = repository.changes.delta(since: revision)
                _ = engine.refreshExisting(assetIDs: ["new-screenshot"],
                    fingerprintsForIDs: { (try? repository.fetchAssetFingerprintRecords(assetIDs: $0)) ?? [:] },
                    remoteFingerprintsForMonth: { _ in [] })
                revision = delta.revision
                return engine.currentBrowserLocalSeed()
            }
        }
    }

    @MainActor
    private final class Fixture {
        static let localID = "new-screenshot"
        static let bytes = Data("screenshot".utf8)
        static let instance = RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: Data(SHA256.hash(data: bytes)),
            fileName: "screenshot.png", fileSize: Int64(bytes.count), remoteRelativePath: "2026/10/screenshot.png", creationDateMs: 1)
        static let fingerprint = AssetContentFingerprint.fingerprint(resources: [instance.contentIdentityResource])
        static let descriptor = RestoreService.RestoreItemDescriptor(instances: [instance], identity: fingerprint)
        private static let month = LibraryMonthKey(year: 2026, month: 10)
        let directory: URL
        let dependencies: DependencyContainer
        let home: HomeSeed
        let presence: LibraryPresenceIndex
        let publisher: LocalIndexChangePublisher

        init() throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            dependencies = DependencyContainer(
                databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")),
                reconcileOneDriveAccounts: false
            )
            home = HomeSeed(repository: dependencies.hashIndexRepository)
            publisher = dependencies.hashIndexRepository.changes
            let home = home
            presence = LibraryPresenceIndex(
                hashIndexRepository: dependencies.hashIndexRepository, coordinator: dependencies.backupCoordinator,
                profileKey: { nil }, homeLocalSeedProvider: { home.current }
            )
        }

        func close() {
            try? dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        func primeStaleSeed() async throws {
            let refreshed = await presence.refresh(notifyOnCommit: false)
            XCTAssertTrue(refreshed)
            let items = try mergedItems()
            XCTAssertEqual(items.count, 2)
            XCTAssertEqual(Set(items.map(\.presence)), [.remoteOnly, .localOnly])
        }

        func writeFingerprint() throws {
            try dependencies.hashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: Self.localID, assetFingerprint: Self.fingerprint,
                resourceCount: 1, totalFileSizeBytes: 123, modificationDateMs: nil
            )
        }

        func assertReconciled() async throws {
            let refreshed = await presence.refresh(notifyOnCommit: false)
            XCTAssertTrue(refreshed)
            XCTAssertEqual(try mergedItems().map(\.presence), [.both])

            NotificationCenter.default.post(name: .ExecutionLifecycleDidChange, object: nil)
            let afterLease = await presence.refresh(notifyOnCommit: false)
            XCTAssertTrue(afterLease)
            XCTAssertEqual(try mergedItems().map(\.presence), [.both])
        }

        func mergedItems() throws -> [MediaBrowserItem] {
            let seed = try XCTUnwrap(presence.browserLocalProjectionInput().seed)
            let local = try XCTUnwrap(LocalMediaSource.content(
                from: seed, backedUpFingerprints: [Self.fingerprint], excludingBackedUpFingerprints: [Self.fingerprint]
            ))
            let remote = MediaBrowserItem(
                kind: .photo, creationDateMs: 1, localIdentifier: seed.localIDByFingerprint[Self.fingerprint],
                remote: .init(fingerprint: Self.fingerprint, photoRelativePath: "screenshot.png", videoRelativePath: nil,
                              photoContentHash: nil, videoContentHash: nil, storageMonth: Self.month, isIncomplete: false)
            )
            return MergedMediaSource.mergePrepared(remoteSections: [.init(month: Self.month, items: [remote])],
                                                  localOnlySections: local.sections).flatMap(\.items)
        }

        func restoreService(client: InMemoryRemoteStorageClient, cancelAfterImport: Bool = false) -> RestoreService {
            let localID = Self.localID
            let instance = Self.instance
            return RestoreService(makeClient: { _, _ in client }, importAsset: { _, _ in
                if cancelAfterImport { withUnsafeCurrentTask { $0?.cancel() } }
                return localID
            }, inspectImportedAsset: { _, _ in
                .init(instances: [instance], modificationDateMs: nil)
            }, hashIndexRepository: dependencies.hashIndexRepository)
        }

        static func profile() -> ServerProfileRecord {
            ServerProfileRecord(id: nil, name: "test", storageType: StorageType.webdav.rawValue,
                connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "test", basePath: "/p",
                username: "test", domain: nil, credentialRef: "test", backgroundBackupEnabled: false,
                createdAt: Date(), updatedAt: Date(), writerID: nil)
        }
    }
}

private extension LocalIndexChangePublisher.Delta {
    var assetIDs: Set<String> {
        if case .touched(let ids) = change { return ids }
        return []
    }
}
