import XCTest
@testable import WatermelonMac

@MainActor
final class MacBackupExecutionControllerIntegrationTests:
    XCTestCase
{
    func testPauseResumeAndStopPreserveThenReleaseExecutionLease()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let coordinator = SuspendingBackupCoordinator()
        let appSession = AppSession()
        let runtimeFlags = AppRuntimeFlags()
        appSession.activate(
            profile: fixture.profile,
            password: ""
        )
        let controller = fixture.makeController(
            appSession: appSession,
            coordinator: coordinator,
            runtimeFlags: runtimeFlags
        )
        let plan = MacBackupExecutionPlan(
            backupMonths: [fixture.month],
            downloadMonths: [],
            complementMonths: [],
            localAssetIDsByMonth: [
                fixture.month: ["asset-1"]
            ],
            monthGroupingTimeZone: .fixedUTC(),
            incompleteDownloadPolicy: .skip
        )

        XCTAssertTrue(
            controller.start(
                profileID: fixture.profile.id,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: plan
            )
        )
        let logURL = try XCTUnwrap(
            controller.currentSessionLogURL
        )
        defer { try? FileManager.default.removeItem(at: logURL) }

        try await waitUntil {
            coordinator.invocationCount == 1
                && controller.state.isUploading
        }
        XCTAssertTrue(runtimeFlags.isExecuting)

        controller.pause()
        try await waitUntil {
            controller.state.isPausedUpload
        }
        XCTAssertTrue(runtimeFlags.isExecuting)

        controller.resume()
        try await waitUntil {
            coordinator.invocationCount == 2
                && controller.state.isUploading
        }
        XCTAssertTrue(runtimeFlags.isExecuting)

        controller.stop()
        try await waitUntil {
            controller.state.isCancelled
        }
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(controller.activeSessionLogURL)
        XCTAssertEqual(
            coordinator.requestedAssetIDs,
            [["asset-1"], ["asset-1"]]
        )

        controller.resetPresentation()
        XCTAssertTrue(controller.state.isIdle)
    }

    func testResumeAfterSessionIsClearedFailsAndReleasesLease()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let coordinator = SuspendingBackupCoordinator()
        let appSession = AppSession()
        let runtimeFlags = AppRuntimeFlags()
        appSession.activate(
            profile: fixture.profile,
            password: ""
        )
        let controller = fixture.makeController(
            appSession: appSession,
            coordinator: coordinator,
            runtimeFlags: runtimeFlags
        )

        XCTAssertTrue(
            controller.start(
                profileID: fixture.profile.id,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: fixture.uploadPlan
            )
        )
        try await waitUntil {
            coordinator.invocationCount == 1
                && controller.state.isUploading
        }

        controller.pause()
        try await waitUntil {
            controller.state.isPausedUpload
        }
        appSession.clear()
        controller.resume()

        try await waitUntil {
            controller.state.isFailed
        }
        XCTAssertEqual(coordinator.invocationCount, 1)
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(controller.activeSessionLogURL)

        controller.resetPresentation()
        XCTAssertTrue(controller.state.isIdle)
    }

    func testDownloadPauseResumeAndStopStayInDownloadStage()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let coordinator = SuspendingBackupCoordinator(
            monthDelta: fixture.remoteDelta
        )
        let downloadHelper =
            SuspendingDownloadWorkflowHelper()
        let appSession = AppSession()
        let runtimeFlags = AppRuntimeFlags()
        appSession.activate(
            profile: fixture.profile,
            password: ""
        )
        let controller = fixture.makeController(
            appSession: appSession,
            coordinator: coordinator,
            runtimeFlags: runtimeFlags,
            downloadWorkflowHelper: downloadHelper
        )

        XCTAssertTrue(
            controller.start(
                profileID: fixture.profile.id,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: fixture.downloadPlan
            )
        )
        try await waitUntil {
            downloadHelper.invocationCount == 1
                && controller.state.isDownloading
        }
        XCTAssertEqual(
            controller.monthExecutionPhases[fixture.month],
            .downloading
        )

        controller.pause()
        try await waitUntil {
            controller.state.isPausedDownload
        }
        XCTAssertEqual(
            controller.monthExecutionPhases[fixture.month],
            .downloadPaused
        )
        XCTAssertTrue(runtimeFlags.isExecuting)

        controller.resume()
        try await waitUntil {
            downloadHelper.invocationCount == 2
                && controller.state.isDownloading
        }
        XCTAssertEqual(coordinator.invocationCount, 0)

        controller.stop()
        try await waitUntil {
            controller.state.isCancelled
        }
        XCTAssertEqual(
            downloadHelper.receivedItemCounts,
            [1, 1]
        )
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(controller.activeSessionLogURL)

        controller.resetPresentation()
        XCTAssertTrue(controller.state.isIdle)
    }

    func testComplementPauseAfterUploadCommitResumesDownloadOnly()
        async throws
    {
        let fixture = try Fixture()
        defer { fixture.remove() }
        let coordinator = ComplementFinalizingBackupCoordinator(
            month: fixture.month,
            monthDelta: fixture.remoteDelta
        )
        let downloadHelper =
            SuspendingDownloadWorkflowHelper()
        let appSession = AppSession()
        let runtimeFlags = AppRuntimeFlags()
        appSession.activate(
            profile: fixture.profile,
            password: ""
        )
        let controller = fixture.makeController(
            appSession: appSession,
            coordinator: coordinator,
            runtimeFlags: runtimeFlags,
            localHashIndexBuildService:
                ReadyLocalHashIndexBuildService(),
            downloadWorkflowHelper: downloadHelper
        )

        XCTAssertTrue(
            controller.start(
                profileID: fixture.profile.id,
                expectedSessionGeneration:
                    appSession.snapshot.generation,
                plan: fixture.complementPlan
            )
        )
        try await waitUntil {
            coordinator.invocationCount == 1
                && downloadHelper.invocationCount == 1
                && controller.state.isDownloading
        }

        controller.pause()
        try await waitUntil {
            controller.state.isPausedDownload
        }
        XCTAssertTrue(runtimeFlags.isExecuting)

        controller.resume()
        try await waitUntil {
            downloadHelper.invocationCount == 2
                && controller.state.isDownloading
        }
        XCTAssertEqual(coordinator.invocationCount, 1)
        XCTAssertEqual(
            coordinator.requestedAssetIDs,
            [["asset-1"]]
        )

        controller.stop()
        try await waitUntil {
            controller.state.isCancelled
        }
        XCTAssertFalse(runtimeFlags.isExecuting)
        XCTAssertNil(controller.activeSessionLogURL)

        controller.resetPresentation()
        XCTAssertTrue(controller.state.isIdle)
    }

    private func waitUntil(
        _ condition: @escaping @MainActor () -> Bool
    ) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(
            by: .seconds(3)
        )
        while clock.now < deadline {
            if condition() {
                return
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        XCTFail("Timed out waiting for controller state")
    }

    private final class Fixture {
        let directory: URL
        let database: DatabaseManager
        let profile: ServerProfileRecord
        let month = LibraryMonthKey(year: 2026, month: 7)
        var uploadPlan: MacBackupExecutionPlan {
            MacBackupExecutionPlan(
                backupMonths: [month],
                downloadMonths: [],
                complementMonths: [],
                localAssetIDsByMonth: [
                    month: ["asset-1"]
                ],
                monthGroupingTimeZone: .fixedUTC(),
                incompleteDownloadPolicy: .skip
            )
        }
        var downloadPlan: MacBackupExecutionPlan {
            MacBackupExecutionPlan(
                backupMonths: [],
                downloadMonths: [month],
                complementMonths: [],
                localAssetIDsByMonth: [month: []],
                monthGroupingTimeZone: .fixedUTC(),
                incompleteDownloadPolicy: .skip
            )
        }
        var complementPlan: MacBackupExecutionPlan {
            MacBackupExecutionPlan(
                backupMonths: [],
                downloadMonths: [],
                complementMonths: [month],
                localAssetIDsByMonth: [
                    month: ["asset-1"]
                ],
                monthGroupingTimeZone: .fixedUTC(),
                incompleteDownloadPolicy: .skip
            )
        }
        var remoteDelta: RemoteLibraryMonthDelta {
            let resourceHash = Data([0xA1])
            let fingerprint =
                BackupAssetResourcePlanner.assetFingerprint(
                    resourceRoleSlotHashes: [
                        (
                            role: 1,
                            slot: 0,
                            contentHash: resourceHash
                        )
                    ]
                )
            return RemoteLibraryMonthDelta(
                month: month,
                resources: [
                    RemoteManifestResource(
                        year: month.year,
                        month: month.month,
                        fileName: "photo.jpg",
                        contentHash: resourceHash,
                        fileSize: 8,
                        resourceType: 1,
                        creationDateMs: 0,
                        backedUpAtMs: 0
                    )
                ],
                assets: [
                    RemoteManifestAsset(
                        year: month.year,
                        month: month.month,
                        assetFingerprint: fingerprint,
                        creationDateMs: 0,
                        backedUpAtMs: 0,
                        resourceCount: 1,
                        totalFileSizeBytes: 8
                    )
                ],
                assetResourceLinks: [
                    RemoteAssetResourceLink(
                        year: month.year,
                        month: month.month,
                        assetFingerprint: fingerprint,
                        resourceHash: resourceHash,
                        role: 1,
                        slot: 0
                    )
                ]
            )
        }

        init() throws {
            directory = FileManager.default.temporaryDirectory
                .appendingPathComponent(
                    "WatermelonMacExecution-\(UUID().uuidString)",
                    isDirectory: true
                )
            try FileManager.default.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
            database = try DatabaseManager(
                databaseURL: directory.appendingPathComponent(
                    "test.sqlite"
                )
            )
            let params = ExternalVolumeConnectionParams(
                rootBookmarkData: Data([1]),
                displayPath: directory.path
            )
            profile = ServerProfileRecord(
                id: 1,
                name: "External",
                storageType:
                    StorageType.externalVolume.rawValue,
                connectionParams:
                    try ServerProfileRecord
                        .encodedConnectionParams(params),
                sortOrder: 0,
                host: "external",
                port: 0,
                shareName: "external-test",
                basePath: "/",
                username: "local",
                domain: nil,
                credentialRef: "external:test",
                createdAt: Date(),
                updatedAt: Date()
            )
        }

        @MainActor
        func makeController(
            appSession: AppSession,
            coordinator: any MacBackupCoordinating,
            runtimeFlags: AppRuntimeFlags,
            localHashIndexBuildService:
                (any MacLocalHashIndexBuilding)? = nil,
            downloadWorkflowHelper:
                (any MacDownloadWorkflowHelping)? = nil
        ) -> MacBackupExecutionController {
            let photoLibraryService = PhotoLibraryService()
            let repository = ContentHashIndexRepository(
                databaseManager: database
            )
            let clientFactory = StorageClientFactory(
                databaseManager: database
            )
            let restoreService = RestoreService(
                databaseManager: database,
                storageClientFactory: clientFactory
            )
            let workflowHelper:
                any MacDownloadWorkflowHelping =
                    downloadWorkflowHelper
                    ?? DownloadWorkflowHelper(
                        restoreService: restoreService,
                        hashIndexRepository: repository
                    )
            return MacBackupExecutionController(
                appSession: appSession,
                photoLibraryService: photoLibraryService,
                localHashIndexBuildService:
                    localHashIndexBuildService
                    ?? LocalHashIndexBuildService(
                            photoLibraryService:
                                photoLibraryService,
                            repository: repository
                        ),
                backupCoordinator: coordinator,
                remoteLibraryReadService:
                    RemoteLibraryReadService(
                        storageClientFactory: clientFactory
                ),
                hashIndexRepository: repository,
                downloadWorkflowHelper: workflowHelper,
                appRuntimeFlags: runtimeFlags,
                makeManualLogWriter: {
                    ExecutionLogSessionWriter(
                        fileURL: self.directory
                            .appendingPathComponent(
                                "execution.log"
                            ),
                        kind: .manual,
                        startedAt: Date()
                    )
                }
            )
        }

        func remove() {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

private struct ReadyLocalHashIndexBuildService:
    MacLocalHashIndexBuilding
{
    func buildIndex(
        for assetIDs: Set<String>,
        workerCount _: Int,
        allowNetworkAccess _: Bool,
        progressHandler _: LocalHashIndexProgressHandler?,
        tickHandler _: LocalHashIndexProgressTickHandler?
    ) async throws -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: assetIDs,
            readyAssetIDs: assetIDs,
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: [],
            networkPendingAssetIDs: []
        )
    }
}

private final class SuspendingBackupCoordinator:
    MacBackupCoordinating,
    @unchecked Sendable
{
    private let lock = NSLock()
    private var requests: [Set<String>] = []
    private let monthDelta: RemoteLibraryMonthDelta?

    init(monthDelta: RemoteLibraryMonthDelta? = nil) {
        self.monthDelta = monthDelta
    }

    var invocationCount: Int {
        lock.withLock { requests.count }
    }

    var requestedAssetIDs: [Set<String>] {
        lock.withLock { requests }
    }

    func runBackup(
        request: BackupRunRequest,
        eventStream _: BackupEventStream
    ) async throws -> BackupExecutionResult {
        lock.withLock {
            requests.append(
                request.onlyAssetLocalIdentifiers ?? []
            )
        }
        try await Task.sleep(for: .seconds(60))
        return BackupExecutionResult(
            total: 0,
            succeeded: 0,
            failed: 0,
            skipped: 0,
            paused: false
        )
    }

    func remoteMonthRawData(
        for month: LibraryMonthKey
    ) -> RemoteLibraryMonthDelta? {
        guard monthDelta?.month == month else { return nil }
        return monthDelta
    }

    func verifyMonth(
        profile _: ServerProfileRecord,
        password _: String,
        month _: LibraryMonthKey,
        reusing _: BackupMonthUploadContext?
    ) async throws {
        return
    }

    func withMacDownloadVerificationPlan(
        profile _: ServerProfileRecord,
        password _: String,
        body: @MainActor @escaping @Sendable (
            BackupDownloadVerificationPlan
        ) async -> Result<Int, Error>
    ) async throws -> Result<Int, Error> {
        await body(
            BackupDownloadVerificationPlan { _ in }
        )
    }
}

private final class ComplementFinalizingBackupCoordinator:
    MacBackupCoordinating,
    @unchecked Sendable
{
    private let lock = NSLock()
    private var requests: [Set<String>] = []
    private let month: LibraryMonthKey
    private let monthDelta: RemoteLibraryMonthDelta

    init(
        month: LibraryMonthKey,
        monthDelta: RemoteLibraryMonthDelta
    ) {
        self.month = month
        self.monthDelta = monthDelta
    }

    var invocationCount: Int {
        lock.withLock { requests.count }
    }

    var requestedAssetIDs: [Set<String>] {
        lock.withLock { requests }
    }

    func runBackup(
        request: BackupRunRequest,
        eventStream _: BackupEventStream
    ) async throws -> BackupExecutionResult {
        lock.withLock {
            requests.append(
                request.onlyAssetLocalIdentifiers ?? []
            )
        }
        guard let finalizer = request.onMonthUploaded else {
            throw CancellationError()
        }
        let result = await finalizer(
            month,
            BackupMonthUploadContext(
                writeMode: .lite(
                    NoopRepoWriteSession(),
                    nil
                )
            )
        )
        switch result {
        case .success:
            return BackupExecutionResult(
                total: 1,
                succeeded: 1,
                failed: 0,
                skipped: 0,
                paused: false
            )
        case .cancelled:
            return BackupExecutionResult(
                total: 1,
                succeeded: 1,
                failed: 0,
                skipped: 0,
                paused: true
            )
        case .failed, .fatal:
            return BackupExecutionResult(
                total: 1,
                succeeded: 0,
                failed: 1,
                skipped: 0,
                paused: false
            )
        }
    }

    func remoteMonthRawData(
        for month: LibraryMonthKey
    ) -> RemoteLibraryMonthDelta? {
        month == self.month ? monthDelta : nil
    }

    func verifyMonth(
        profile _: ServerProfileRecord,
        password _: String,
        month _: LibraryMonthKey,
        reusing _: BackupMonthUploadContext?
    ) async throws {
        return
    }

    func withMacDownloadVerificationPlan(
        profile _: ServerProfileRecord,
        password _: String,
        body: @MainActor @escaping @Sendable (
            BackupDownloadVerificationPlan
        ) async -> Result<Int, Error>
    ) async throws -> Result<Int, Error> {
        await body(
            BackupDownloadVerificationPlan { _ in }
        )
    }
}

private actor NoopRepoWriteSession: RepoWriteSession {
    func begin() async {}
    func release() async {}
    func assertWriteAllowed(now _: Date) async throws {}
    func assertDestructiveWriteAllowed(now _: Date) async throws {}
}

@MainActor
private final class SuspendingDownloadWorkflowHelper:
    MacDownloadWorkflowHelping
{
    private(set) var receivedItemCounts: [Int] = []

    var invocationCount: Int {
        receivedItemCounts.count
    }

    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context _: DownloadWorkflowHelper.Context,
        incompletePolicy _: IncompleteDownloadPolicy,
        onTransferState:
            @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored _:
            @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        receivedItemCounts.append(remoteItems.count)
        onTransferState(
            BackupTransferState(
                kind: .download,
                workerID: 0,
                assetLocalIdentifier: "remote",
                assetDisplayName: "photo",
                resourceDate: nil,
                assetPosition: 1,
                totalAssets: remoteItems.count,
                resourceDisplayName: "photo.jpg",
                resourcePosition: 1,
                totalResources: 1,
                resourceFraction: 0.5,
                resourceBytesTransferred: 4,
                resourceTotalBytes: 8,
                countsTowardTransferSpeed: true,
                stageDescription: ""
            )
        )
        do {
            try await Task.sleep(for: .seconds(60))
            return .success(
                restoredCount: remoteItems.count,
                skippedIncompleteCount: 0
            )
        } catch {
            return .cancelled
        }
    }
}

private extension MacBackupExecutionState {
    var isIdle: Bool {
        if case .idle = self {
            return true
        }
        return false
    }

    var isUploading: Bool {
        if case .uploading = self {
            return true
        }
        return false
    }

    var isPausedUpload: Bool {
        if case .paused(.upload) = self {
            return true
        }
        return false
    }

    var isDownloading: Bool {
        if case .downloading = self {
            return true
        }
        return false
    }

    var isPausedDownload: Bool {
        if case .paused(.download) = self {
            return true
        }
        return false
    }

    var isCancelled: Bool {
        if case .cancelled = self {
            return true
        }
        return false
    }

    var isFailed: Bool {
        if case .failed = self {
            return true
        }
        return false
    }
}
