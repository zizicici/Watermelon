import Foundation

final class BackupCoordinator: Sendable {
    private let remoteIndexService: RemoteIndexSyncService
    private let preparationService: BackupRunPreparationService
    private let parallelExecutor: BackupParallelExecutor

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        databaseManager: DatabaseManager,
        remoteIndexService: RemoteIndexSyncService? = nil,
        assetProcessor: AssetProcessor? = nil
    ) {
        let remoteIndexService = remoteIndexService ?? RemoteIndexSyncService()
        let assetProcessor = assetProcessor ?? AssetProcessor(
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            remoteIndexService: remoteIndexService
        )

        self.remoteIndexService = remoteIndexService
        preparationService = BackupRunPreparationService(
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory,
            hashIndexRepository: hashIndexRepository,
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )
        parallelExecutor = BackupParallelExecutor(
            hashIndexRepository: hashIndexRepository,
            assetProcessor: assetProcessor,
            remoteIndexService: remoteIndexService
        )
    }

    func runBackup(request: BackupRunRequest, eventStream: BackupEventStream) async throws -> BackupExecutionResult {
        let preparedRun = try await preparationService.prepareRun(
            request: request,
            eventStream: eventStream
        )
        return try await parallelExecutor.execute(
            preparedRun: preparedRun,
            profile: request.profile,
            workerCountOverride: request.workerCountOverride,
            iCloudPhotoBackupMode: request.iCloudPhotoBackupMode,
            eventStream: eventStream,
            onMonthUploaded: request.onMonthUploaded
        )
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> RemoteIndexSyncDigest {
        try await preparationService.reloadRemoteIndex(
            profile: profile,
            password: password,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress
        )
    }

    @discardableResult
    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey
    ) async throws -> MonthVerifyOutcome {
        try await preparationService.verifyMonth(
            profile: profile,
            password: password,
            month: month
        )
    }

    /// Aggregates per-month outcomes; any damaged month makes the whole run damaged.
    @discardableResult
    func verifyAllMonths(
        profile: ServerProfileRecord,
        password: String,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws -> MonthVerifyOutcome {
        try await preparationService.withConnectedClient(profile: profile, password: password) { client in
            _ = try await self.preparationService.reloadRemoteIndex(client: client, profile: profile)

            // `monthSummaries()` is asset-keyed and would skip resource-only residue.
            let uniqueMonths = Array(self.remoteIndexService.allKnownMonths()).sorted()
            let total = uniqueMonths.count
            await MainActor.run { onProgress(RemoteSyncProgress(current: 0, total: total)) }

            var aggregate: MonthVerifyOutcome = .clean
            for (index, month) in uniqueMonths.enumerated() {
                try Task.checkCancellation()
                let outcome = try await self.preparationService.verifyMonth(
                    client: client,
                    basePath: profile.basePath,
                    month: month,
                    profile: profile,
                    password: password
                )
                aggregate = aggregate.combined(with: outcome)
                let current = index + 1
                await MainActor.run { onProgress(RemoteSyncProgress(current: current, total: total)) }
            }
            return aggregate
        }
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        remoteIndexService.remoteMonthSummaries()
    }

    func healthDigest() -> RemoteHealthDigest {
        remoteIndexService.healthDigest()
    }

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        remoteIndexService.remoteMonthRawData(for: month)
    }

    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        remoteIndexService.currentState(since: revision)
    }

    /// `nil` until the first sync identifies the format; callers must not collapse nil to V1.
    func currentRepoIsV2() async -> Bool? {
        await remoteIndexService.currentRepoIsV2()
    }

    func prepareResumeHandle(profile: ServerProfileRecord, password: String) async throws -> RemoteViewHandle {
        try await preparationService.withConnectedClient(profile: profile, password: password) { client in
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let handle = try await self.remoteIndexService.syncOverlayAndCaptureHandle(
                    client: client,
                    basePath: profile.basePath
                )
                if handle.overlayFreshness == .fresh {
                    return handle
                }
                if attempt < maxAttempts {
                    try Task.checkCancellation()
                    try await Task.sleep(for: .milliseconds(500))
                }
            }
            throw RemoteViewHandleError.stalePhysicalPresenceOverlay
        }
    }
}
