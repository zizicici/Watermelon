#if WATERMELON_LIVE_PHOTOKIT_UPLOAD
import Photos
import XCTest
@testable import WatermelonMac

@MainActor
final class MacLivePhotoKitUploadIntegrationTests: XCTestCase {
    func testManualUploadUsesRealPhotoKitAndBackupCore()
        async throws
    {
        guard ProcessInfo.processInfo.environment[
            "WATERMELON_RUN_LIVE_PHOTOKIT_UPLOAD"
        ] == "1" else {
            throw XCTSkip(
                "Set WATERMELON_RUN_LIVE_PHOTOKIT_UPLOAD=1 to run against a disposable Photos library."
            )
        }
        let authorization = PHPhotoLibrary.authorizationStatus(
            for: .readWrite
        )
        guard authorization == .authorized
                || authorization == .limited else {
            throw XCTSkip(
                "Grant Photos access to the WatermelonMac test host before running this live test."
            )
        }
        let asset = try XCTUnwrap(selectedAsset())
        let timeZone = MonthGroupingTimeZonePreference.fixedUTC()
        let month = LibraryMonthKey.from(
            date: asset.creationDate,
            calendar: LibraryMonthKey.monthCalendar(
                preference: timeZone
            )
        )
        let fixture = try Fixture()
        defer { fixture.remove() }

        let runtimeFlags = AppRuntimeFlags()
        let profileStore = ProfileStore(
            databaseManager: fixture.database,
            keychainService: KeychainService(),
            appRuntimeFlags: runtimeFlags
        )
        let savedProfile = try profileStore.saveLocalProfile(
            folderURL: fixture.destination
        )
        let profileID = try XCTUnwrap(savedProfile.id)
        let profile = try XCTUnwrap(
            fixture.database.fetchServerProfiles().first {
                $0.id == profileID
            }
        )
        let clientFactory = StorageClientFactory(
            databaseManager: fixture.database
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
        let restoreService = RestoreService(
            databaseManager: fixture.database,
            storageClientFactory: clientFactory
        )
        let appSession = AppSession()
        appSession.activate(profile: profile, password: "")
        defer { appSession.clear() }

        let controller = MacBackupExecutionController(
            appSession: appSession,
            photoLibraryService: photoLibraryService,
            localHashIndexBuildService: LocalHashIndexBuildService(
                photoLibraryService: photoLibraryService,
                repository: hashIndexRepository
            ),
            backupCoordinator: backupCoordinator,
            remoteLibraryReadService: remoteLibraryReadService,
            hashIndexRepository: hashIndexRepository,
            downloadWorkflowHelper: DownloadWorkflowHelper(
                restoreService: restoreService,
                hashIndexRepository: hashIndexRepository
            ),
            appRuntimeFlags: runtimeFlags,
            makeManualLogWriter: {
                ExecutionLogSessionWriter(
                    fileURL: fixture.directory.appendingPathComponent(
                        "live-upload.log"
                    ),
                    kind: .manual,
                    startedAt: Date()
                )
            }
        )
        let plan = MacBackupExecutionPlan(
            backupMonths: [month],
            downloadMonths: [],
            complementMonths: [],
            localAssetIDsByMonth: [
                month: [asset.localIdentifier]
            ],
            monthGroupingTimeZone: timeZone,
            incompleteDownloadPolicy: .skip
        )

        XCTAssertTrue(
            controller.start(
                profileID: profileID,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: plan
            )
        )
        let terminalState = try await waitForTerminalState(
            of: controller
        )
        guard case .completed(let summary) = terminalState else {
            if case .failed(let message) = terminalState {
                XCTFail("Live PhotoKit upload failed: \(message)")
            } else {
                XCTFail("Live PhotoKit upload did not complete")
            }
            return
        }
        let upload = try XCTUnwrap(summary.upload)
        XCTAssertEqual(upload.total, 1)
        XCTAssertEqual(upload.succeeded, 1)
        XCTAssertEqual(upload.failed, 0)
        XCTAssertEqual(upload.skipped, 0)
        XCTAssertFalse(upload.paused)
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(controller.activeSessionLogURL)

        let readResult = try await remoteLibraryReadService.reload(
            profile: profile,
            credentialPayload: ""
        )
        XCTAssertEqual(readResult.digest.assetCount, 1)
        XCTAssertGreaterThanOrEqual(
            readResult.digest.resourceCount,
            1
        )
        XCTAssertGreaterThanOrEqual(
            readResult.digest.linkCount,
            1
        )
        XCTAssertEqual(
            readResult.snapshotState.monthDeltas.map(\.month),
            [month]
        )
        XCTAssertTrue(
            FileManager.default.fileExists(
                atPath: fixture.destination
                    .appendingPathComponent(
                        ".watermelon/version.json"
                    )
                    .path
            )
        )
        XCTAssertTrue(
            FileManager.default.fileExists(
                atPath: fixture.destination
                    .appendingPathComponent(
                        ".watermelon/months/\(month.text).sqlite"
                    )
                    .path
            )
        )
        let dataDirectory = fixture.destination
            .appendingPathComponent(
                String(format: "%04d", month.year),
                isDirectory: true
            )
            .appendingPathComponent(
                String(format: "%02d", month.month),
                isDirectory: true
            )
        XCTAssertFalse(
            try FileManager.default.contentsOfDirectory(
                atPath: dataDirectory.path
            ).isEmpty
        )
    }

    private func selectedAsset() -> PHAsset? {
        if let identifier = ProcessInfo.processInfo.environment[
            "WATERMELON_LIVE_PHOTO_ASSET_ID"
        ]?.trimmingCharacters(in: .whitespacesAndNewlines),
           !identifier.isEmpty {
            return PHAsset.fetchAssets(
                withLocalIdentifiers: [identifier],
                options: nil
            ).firstObject
        }
        let options = PHFetchOptions()
        options.fetchLimit = 1
        options.sortDescriptors = [
            NSSortDescriptor(
                key: "creationDate",
                ascending: false
            )
        ]
        return PHAsset.fetchAssets(
            with: .image,
            options: options
        ).firstObject
    }

    private func waitForTerminalState(
        of controller: MacBackupExecutionController
    ) async throws -> MacBackupExecutionState {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(600))
        while clock.now < deadline {
            switch controller.state {
            case .completed, .failed, .cancelled, .paused:
                return controller.state
            case .idle, .preflighting, .uploading, .downloading,
                 .pausing, .resuming, .stopping:
                try await Task.sleep(for: .milliseconds(100))
            }
        }
        controller.stop()
        XCTFail("Timed out waiting for the live PhotoKit upload")
        return controller.state
    }

    private final class Fixture {
        let directory: URL
        let destination: URL
        let database: DatabaseManager

        init() throws {
            directory = FileManager.default.temporaryDirectory
                .appendingPathComponent(
                    "WatermelonMacLiveUpload-\(UUID().uuidString)",
                    isDirectory: true
                )
            destination = directory.appendingPathComponent(
                "External Destination",
                isDirectory: true
            )
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
#endif
