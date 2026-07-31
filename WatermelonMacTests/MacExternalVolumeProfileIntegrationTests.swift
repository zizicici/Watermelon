import GRDB
import XCTest
@testable import WatermelonMac

@MainActor
final class MacExternalVolumeProfileIntegrationTests: XCTestCase {
    func testSavedFolderProfileBuildsWorkingStorageClient()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let store = ProfileStore(
            databaseManager: fixture.database,
            keychainService: KeychainService(),
            appRuntimeFlags: AppRuntimeFlags()
        )

        let saved = try store.saveLocalProfile(
            folderURL: fixture.destination
        )
        let profileID = try XCTUnwrap(saved.id)
        let persisted = try XCTUnwrap(
            fixture.database.fetchServerProfiles().first {
                $0.id == profileID
            }
        )

        XCTAssertEqual(saved.name, fixture.destination.lastPathComponent)
        XCTAssertEqual(saved.resolvedStorageType, .externalVolume)
        XCTAssertEqual(store.profiles.map(\.id), [profileID])
        XCTAssertEqual(
            persisted.externalVolumeParams?.displayPath,
            fixture.destination.path
        )
        XCTAssertFalse(
            try XCTUnwrap(
                persisted.externalVolumeParams?.rootBookmarkData
            ).isEmpty
        )

        let client = try StorageClientFactory(
            databaseManager: fixture.database
        ).makeClient(
            profile: persisted,
            credentialPayload: ""
        )
        try await client.connect()
        do {
            let payload = Data("mac-external-volume".utf8)
            try payload.write(to: fixture.source)
            let remotePath = "/Watermelon/2026/07/photo.bin"

            try await client.upload(
                localURL: fixture.source,
                remotePath: remotePath,
                mode: .replace,
                respectTaskCancellation: false,
                onProgress: nil
            )

            let existsAfterUpload = try await client.exists(
                path: remotePath
            )
            XCTAssertTrue(existsAfterUpload)
            let entries = try await client.list(
                path: "/Watermelon/2026/07"
            )
            XCTAssertEqual(entries.map(\.name), ["photo.bin"])

            try await client.download(
                remotePath: remotePath,
                localURL: fixture.download
            )
            XCTAssertEqual(
                try Data(contentsOf: fixture.download),
                payload
            )

            try await client.delete(path: remotePath)
            let existsAfterDelete = try await client.exists(
                path: remotePath
            )
            XCTAssertFalse(existsAfterDelete)
        } catch {
            await client.disconnect()
            throw error
        }
        await client.disconnect()
    }

    func testSavedFolderProfileConnectsAndPublishesRemoteSnapshot()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let keychain = KeychainService()
        let runtimeFlags = AppRuntimeFlags()
        let profileStore = ProfileStore(
            databaseManager: fixture.database,
            keychainService: keychain,
            appRuntimeFlags: runtimeFlags
        )
        let saved = try profileStore.saveLocalProfile(
            folderURL: fixture.destination
        )
        let profileID = try XCTUnwrap(saved.id)
        var profile = try XCTUnwrap(
            fixture.database.fetchServerProfiles().first {
                $0.id == profileID
            }
        )
        profile.backgroundBackupEnabled = true
        try fixture.database.saveServerProfile(&profile)
        let clientFactory = StorageClientFactory(
            databaseManager: fixture.database
        )
        let remoteIndexService = RemoteIndexSyncService()
        let remoteLibraryReadService = RemoteLibraryReadService(
            storageClientFactory: clientFactory,
            remoteIndexService: remoteIndexService
        )
        let photoLibraryService = PhotoLibraryService()
        let backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: clientFactory,
            hashIndexRepository: ContentHashIndexRepository(
                databaseManager: fixture.database
            ),
            databaseManager: fixture.database,
            remoteIndexService: remoteIndexService,
            thumbnailRenderer: MacThumbnailRenderer()
        )
        let appSession = AppSession()
        let controller = MacRemoteConnectionController(
            databaseManager: fixture.database,
            keychainService: keychain,
            appSession: appSession,
            appRuntimeFlags: runtimeFlags,
            profileStore: profileStore,
            connectionService: StorageProfileConnectionService(
                databaseManager: fixture.database
            ),
            remoteLibraryReadService: remoteLibraryReadService,
            backupCoordinator: backupCoordinator
        )
        var publishedSnapshot: RemoteLibrarySnapshotState?
        var publishedReload = false
        var publishedGeneration: UInt64?
        controller.onRemoteSnapshot = {
            snapshot,
            didReload,
            generation in
            publishedSnapshot = snapshot
            publishedReload = didReload
            publishedGeneration = generation
        }

        controller.select(profile: profile)
        controller.connectSelected()
        try await waitUntil {
            controller.state.connectedProfile?.id == profileID
        }

        guard case .connected(_, let digest) = controller.state else {
            return XCTFail("Expected a connected external-volume profile")
        }
        XCTAssertEqual(digest.resourceCount, 0)
        XCTAssertEqual(digest.assetCount, 0)
        XCTAssertEqual(digest.linkCount, 0)
        XCTAssertEqual(appSession.snapshot.activeProfile?.id, profileID)
        XCTAssertEqual(appSession.snapshot.activePassword, "")
        XCTAssertFalse(
            appSession.snapshot.activeProfile?
                .backgroundBackupEnabled ?? true
        )
        XCTAssertEqual(
            try fixture.database.activeServerProfileID(),
            profileID
        )
        XCTAssertFalse(runtimeFlags.isConnecting(profileID: profileID))
        XCTAssertFalse(runtimeFlags.isExecuting)
        let snapshot = try XCTUnwrap(publishedSnapshot)
        XCTAssertTrue(snapshot.isFullSnapshot)
        XCTAssertTrue(snapshot.monthDeltas.isEmpty)
        XCTAssertEqual(
            snapshot.profileKey,
            RemoteIndexSyncService.remoteProfileKey(profile)
        )
        XCTAssertTrue(publishedReload)
        XCTAssertEqual(
            publishedGeneration,
            appSession.snapshot.generation
        )
        let connectedGeneration = appSession.snapshot.generation

        controller.disconnect()

        XCTAssertNil(controller.state.connectedProfile)
        XCTAssertNil(appSession.snapshot.activeProfile)
        XCTAssertNil(try fixture.database.activeServerProfileID())
        XCTAssertFalse(runtimeFlags.isConnecting(profileID: profileID))
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(publishedSnapshot)
        XCTAssertFalse(publishedReload)
        XCTAssertGreaterThan(
            appSession.snapshot.generation,
            connectedGeneration
        )
    }

    func testSavedFolderProfileRunsRealMaintenanceCore()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let runtimeFlags = AppRuntimeFlags()
        let profileStore = ProfileStore(
            databaseManager: fixture.database,
            keychainService: KeychainService(),
            appRuntimeFlags: runtimeFlags
        )
        let saved = try profileStore.saveLocalProfile(
            folderURL: fixture.destination
        )
        let profileID = try XCTUnwrap(saved.id)
        let profile = try XCTUnwrap(
            fixture.database.fetchServerProfiles().first {
                $0.id == profileID
            }
        )
        let clientFactory = StorageClientFactory(
            databaseManager: fixture.database
        )
        _ = try await seedRemoteAsset(
            profile: profile,
            clientFactory: clientFactory,
            fixture: fixture
        )
        let remoteIndexService = RemoteIndexSyncService()
        let photoLibraryService = PhotoLibraryService()
        let backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: clientFactory,
            hashIndexRepository: ContentHashIndexRepository(
                databaseManager: fixture.database
            ),
            databaseManager: fixture.database,
            remoteIndexService: remoteIndexService,
            thumbnailRenderer: MacThumbnailRenderer()
        )
        _ = try await backupCoordinator.reloadRemoteIndex(
            profile: profile,
            password: ""
        )
        let maintenanceController = RemoteMaintenanceController(
            backupCoordinator: backupCoordinator,
            appRuntimeFlags: runtimeFlags,
            databaseManager: fixture.database
        )

        XCTAssertTrue(
            maintenanceController.startFullVerify(
                profile: profile,
                password: ""
            )
        )
        try await waitUntil {
            !maintenanceController.isBusy
        }
        XCTAssertNil(maintenanceController.lastError)
        XCTAssertNotNil(
            try fixture.database.remoteVerifiedAt(
                profileID: profileID
            )
        )
        XCTAssertFalse(runtimeFlags.isExecuting)

        var scanResult: LeftoverScanResult?
        var scanFailure: String?
        XCTAssertTrue(
            maintenanceController.startScanLeftover(
                profile: profile,
                password: ""
            ) { outcome in
                switch outcome {
                case .completed(let result):
                    scanResult = result
                case .failed(let message):
                    scanFailure = message
                case .cancelled:
                    scanFailure = "cancelled"
                }
            }
        )
        try await waitUntil {
            scanResult != nil || scanFailure != nil
        }
        XCTAssertNil(scanFailure)
        XCTAssertEqual(scanResult?.totalCount, 0)
        XCTAssertEqual(scanResult?.orphanThumbnailCount, 0)
        XCTAssertFalse(runtimeFlags.isExecuting)
    }

    func testManualDownloadUsesRealCoreAgainstSavedFolderProfile()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let keychain = KeychainService()
        let runtimeFlags = AppRuntimeFlags()
        let profileStore = ProfileStore(
            databaseManager: fixture.database,
            keychainService: keychain,
            appRuntimeFlags: runtimeFlags
        )
        let saved = try profileStore.saveLocalProfile(
            folderURL: fixture.destination
        )
        let profileID = try XCTUnwrap(saved.id)
        let profile = try XCTUnwrap(
            fixture.database.fetchServerProfiles().first {
                $0.id == profileID
            }
        )
        let clientFactory = StorageClientFactory(
            databaseManager: fixture.database
        )
        let month = try await seedRemoteAsset(
            profile: profile,
            clientFactory: clientFactory,
            fixture: fixture
        )
        let remoteIndexService = RemoteIndexSyncService()
        let remoteLibraryReadService = RemoteLibraryReadService(
            storageClientFactory: clientFactory,
            remoteIndexService: remoteIndexService
        )
        let photoLibraryService = PhotoLibraryService()
        let hashIndexRepository = ContentHashIndexRepository(
            databaseManager: fixture.database
        )
        let backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: clientFactory,
            hashIndexRepository: hashIndexRepository,
            databaseManager: fixture.database,
            remoteIndexService: remoteIndexService,
            thumbnailRenderer: MacThumbnailRenderer()
        )
        let appSession = AppSession()
        let connectionController = MacRemoteConnectionController(
            databaseManager: fixture.database,
            keychainService: keychain,
            appSession: appSession,
            appRuntimeFlags: runtimeFlags,
            profileStore: profileStore,
            connectionService: StorageProfileConnectionService(
                databaseManager: fixture.database
            ),
            remoteLibraryReadService: remoteLibraryReadService,
            backupCoordinator: backupCoordinator
        )

        connectionController.select(profile: profile)
        connectionController.connectSelected()
        try await waitUntil {
            connectionController.state.connectedProfile?.id
                == profileID
        }
        guard case .connected(_, let digest) =
                connectionController.state else {
            return XCTFail("Expected the seeded destination to connect")
        }
        XCTAssertEqual(digest.resourceCount, 1)
        XCTAssertEqual(digest.assetCount, 1)
        XCTAssertEqual(digest.linkCount, 1)

        let downloadHelper = RecordingCoreDownloadWorkflowHelper()
        let executionController = MacBackupExecutionController(
            appSession: appSession,
            photoLibraryService: photoLibraryService,
            localHashIndexBuildService: LocalHashIndexBuildService(
                photoLibraryService: photoLibraryService,
                repository: hashIndexRepository
            ),
            backupCoordinator: backupCoordinator,
            remoteLibraryReadService: remoteLibraryReadService,
            hashIndexRepository: hashIndexRepository,
            downloadWorkflowHelper: downloadHelper,
            appRuntimeFlags: runtimeFlags,
            makeManualLogWriter: {
                ExecutionLogSessionWriter(
                    fileURL: fixture.directory
                        .appendingPathComponent(
                            "real-core-execution.log"
                        ),
                    kind: .manual,
                    startedAt: Date()
                )
            }
        )
        let plan = MacBackupExecutionPlan(
            backupMonths: [],
            downloadMonths: [month],
            complementMonths: [],
            localAssetIDsByMonth: [month: []],
            monthGroupingTimeZone: .fixedUTC(),
            incompleteDownloadPolicy: .skip
        )

        XCTAssertTrue(
            executionController.start(
                profileID: profileID,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: plan
            )
        )
        try await waitUntil {
            if case .completed = executionController.state {
                return true
            }
            return false
        }

        guard case .completed(let summary) =
                executionController.state else {
            return XCTFail("Expected the real-core download to complete")
        }
        XCTAssertNil(summary.upload)
        XCTAssertEqual(summary.restoredCount, 1)
        XCTAssertEqual(summary.skippedIncompleteCount, 0)
        XCTAssertEqual(summary.failedDownloadMonths, 0)
        XCTAssertEqual(downloadHelper.receivedItemCounts, [1])
        XCTAssertFalse(runtimeFlags.isExecuting)

        executionController.resetPresentation()
        let suspendingHelper =
            SuspendingCoreDownloadWorkflowHelper()
        let cancellationController =
            MacBackupExecutionController(
                appSession: appSession,
                photoLibraryService: photoLibraryService,
                localHashIndexBuildService:
                    LocalHashIndexBuildService(
                        photoLibraryService:
                            photoLibraryService,
                        repository: hashIndexRepository
                    ),
                backupCoordinator: backupCoordinator,
                remoteLibraryReadService:
                    remoteLibraryReadService,
                hashIndexRepository: hashIndexRepository,
                downloadWorkflowHelper: suspendingHelper,
                appRuntimeFlags: runtimeFlags,
                makeManualLogWriter: {
                    ExecutionLogSessionWriter(
                        fileURL: fixture.directory
                            .appendingPathComponent(
                                "real-core-cancellation.log"
                            ),
                        kind: .manual,
                        startedAt: Date()
                    )
                }
            )
        XCTAssertTrue(
            cancellationController.start(
                profileID: profileID,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: plan
            )
        )
        try await waitUntil {
            suspendingHelper.invocationCount == 1
        }

        cancellationController.stop()
        try await waitUntil {
            if case .cancelled = cancellationController.state {
                return true
            }
            return false
        }
        XCTAssertTrue(suspendingHelper.wasCancelled)
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(cancellationController.activeSessionLogURL)

        cancellationController.resetPresentation()
        XCTAssertTrue(
            executionController.start(
                profileID: profileID,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: plan
            )
        )
        try await waitUntil {
            if case .completed = executionController.state {
                return true
            }
            return false
        }
        XCTAssertEqual(downloadHelper.receivedItemCounts, [1, 1])
        XCTAssertFalse(runtimeFlags.isExecuting)

        executionController.resetPresentation()
        connectionController.disconnect()

        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(appSession.activeProfile)
        XCTAssertNil(try fixture.database.activeServerProfileID())
    }

    private func seedRemoteAsset(
        profile: ServerProfileRecord,
        clientFactory: StorageClientFactory,
        fixture: Fixture
    ) async throws -> LibraryMonthKey {
        let client = try clientFactory.makeClient(
            profile: profile,
            credentialPayload: ""
        )
        try await client.connect()
        do {
            let month = LibraryMonthKey(year: 2026, month: 7)
            let payload = Data("real-core-download".utf8)
            try payload.write(to: fixture.source)
            let digest = try FileDigestService.sha256AndSize(
                of: fixture.source
            )
            let fileName = "photo.jpg"
            let monthPath = String(
                format: "%04d/%02d",
                month.year,
                month.month
            )
            try await client.createDirectory(
                path: RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: monthPath
                )
            )
            try await client.upload(
                localURL: fixture.source,
                remotePath: RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath:
                        "\(monthPath)/\(fileName)"
                ),
                mode: .replace,
                respectTaskCancellation: false,
                onProgress: nil
            )

            let localManifestURL = fixture.directory
                .appendingPathComponent("seed.sqlite")
            let queue = try MonthManifestStore.makeManifestQueue(
                path: localManifestURL.path
            )
            try MonthManifestStore.migrate(queue)
            let ownership = RepoOwnershipGates(
                assertWrite: {},
                assertDestructive: {}
            )
            do {
                let store = MonthManifestStore(
                    client: client,
                    basePath: profile.basePath,
                    year: month.year,
                    month: month.month,
                    localManifestURL: localManifestURL,
                    dbQueue: queue,
                    remoteFilesByName: [:],
                    dirty: false,
                    layout: .lite,
                    liteWriteOwnership: ownership
                )
                let backedUpAtMs = Date().millisecondsSinceEpoch
                let resource = RemoteManifestResource(
                    year: month.year,
                    month: month.month,
                    fileName: fileName,
                    contentHash: digest.hash,
                    fileSize: digest.size,
                    resourceType: ResourceTypeCode.photo,
                    creationDateMs: backedUpAtMs,
                    backedUpAtMs: backedUpAtMs
                )
                let fingerprint =
                    BackupAssetResourcePlanner.assetFingerprint(
                        resourceRoleSlotHashes: [
                            (
                                role: ResourceTypeCode.photo,
                                slot: 0,
                                contentHash: digest.hash
                            )
                        ]
                    )
                _ = try store.upsertResource(resource)
                try store.upsertAsset(
                    RemoteManifestAsset(
                        year: month.year,
                        month: month.month,
                        assetFingerprint: fingerprint,
                        creationDateMs: backedUpAtMs,
                        backedUpAtMs: backedUpAtMs,
                        resourceCount: 1,
                        totalFileSizeBytes: digest.size
                    ),
                    links: [
                        RemoteAssetResourceLink(
                            year: month.year,
                            month: month.month,
                            assetFingerprint: fingerprint,
                            resourceHash: digest.hash,
                            role: ResourceTypeCode.photo,
                            slot: 0
                        )
                    ]
                )
                let didFlush = try await store.flushToRemote()
                XCTAssertTrue(didFlush)
            }
            _ = try await VersionManifestWriter(
                client: client,
                basePath: profile.basePath
            ).commit(
                createdAt: "2026-07-31T00:00:00Z",
                createdBy: "WatermelonMacTests"
            )
            await client.disconnect()
            return month
        } catch {
            await client.disconnect()
            throw error
        }
    }

    private func waitUntil(
        _ condition: @escaping @MainActor () -> Bool
    ) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(3))
        while clock.now < deadline {
            if condition() {
                return
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        XCTFail("Timed out waiting for connection state")
    }

    private final class Fixture {
        let directory: URL
        let destination: URL
        let source: URL
        let download: URL
        let database: DatabaseManager

        init() throws {
            directory = FileManager.default.temporaryDirectory
                .appendingPathComponent(
                    "WatermelonMacExternalVolume-\(UUID().uuidString)",
                    isDirectory: true
                )
            destination = directory.appendingPathComponent(
                "External Destination",
                isDirectory: true
            )
            source = directory.appendingPathComponent("source.bin")
            download = directory.appendingPathComponent("download.bin")
            try FileManager.default.createDirectory(
                at: destination,
                withIntermediateDirectories: true
            )
            database = try DatabaseManager(
                databaseURL: directory.appendingPathComponent(
                    "test.sqlite"
                )
            )
        }

        func remove() {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

@MainActor
private final class RecordingCoreDownloadWorkflowHelper:
    MacDownloadWorkflowHelping
{
    private(set) var receivedItemCounts: [Int] = []

    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context _: DownloadWorkflowHelper.Context,
        incompletePolicy _: IncompleteDownloadPolicy,
        onTransferState:
            @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored:
            @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        receivedItemCounts.append(remoteItems.count)
        guard !remoteItems.isEmpty else {
            return .success(
                restoredCount: 0,
                skippedIncompleteCount: 0
            )
        }
        onTransferState(
            BackupTransferState(
                kind: .download,
                workerID: 0,
                assetLocalIdentifier: "remote",
                assetDisplayName: "photo",
                resourceDate: nil,
                assetPosition: remoteItems.count,
                totalAssets: remoteItems.count,
                resourceDisplayName: "photo.jpg",
                resourcePosition: 1,
                totalResources: 1,
                resourceFraction: 1,
                resourceBytesTransferred: 18,
                resourceTotalBytes: 18,
                countsTowardTransferSpeed: true,
                stageDescription: ""
            )
        )
        for index in remoteItems.indices {
            await onItemRestored("restored-\(index)")
        }
        return .success(
            restoredCount: remoteItems.count,
            skippedIncompleteCount: 0
        )
    }
}

@MainActor
private final class SuspendingCoreDownloadWorkflowHelper:
    MacDownloadWorkflowHelping
{
    private(set) var invocationCount = 0
    private(set) var wasCancelled = false

    func downloadItems(
        _: [RemoteAlbumItem],
        context _: DownloadWorkflowHelper.Context,
        incompletePolicy _: IncompleteDownloadPolicy,
        onTransferState _:
            @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored _:
            @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        invocationCount += 1
        do {
            try await Task.sleep(for: .seconds(60))
            return .failed("Unexpected completion")
        } catch {
            wasCancelled = true
            return .cancelled
        }
    }
}
