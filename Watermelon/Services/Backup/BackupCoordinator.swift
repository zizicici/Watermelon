import Foundation

final class BackupCoordinator: Sendable {
    private let remoteIndexService: RemoteIndexSyncService
    private let preparationService: BackupRunPreparationService
    private let parallelExecutor: BackupParallelExecutor

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
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
            remoteIndexService: remoteIndexService
        )
        parallelExecutor = BackupParallelExecutor(
            hashIndexRepository: hashIndexRepository,
            assetProcessor: assetProcessor
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
    ) async throws -> RemoteLibrarySnapshot {
        try await preparationService.reloadRemoteIndex(
            profile: profile,
            password: password,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress
        )
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        remoteIndexService.remoteMonthSummaries()
    }

    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        remoteIndexService.currentState(since: revision)
    }
}
