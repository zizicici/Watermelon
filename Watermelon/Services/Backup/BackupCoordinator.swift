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

    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey
    ) async throws {
        try await preparationService.verifyMonth(
            profile: profile,
            password: password,
            month: month
        )
    }

    func verifyAllMonths(
        profile: ServerProfileRecord,
        password: String,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws {
        try await preparationService.withConnectedClient(profile: profile, password: password) { client in
            _ = try await self.preparationService.reloadRemoteIndex(client: client, profile: profile)

            // `monthSummaries()` is asset-keyed and would skip resource-only residue.
            let uniqueMonths = Array(self.remoteIndexService.allKnownMonths()).sorted()
            let total = uniqueMonths.count
            await MainActor.run { onProgress(RemoteSyncProgress(current: 0, total: total)) }

            for (index, month) in uniqueMonths.enumerated() {
                try Task.checkCancellation()
                try await self.preparationService.verifyMonth(
                    client: client,
                    basePath: profile.basePath,
                    month: month,
                    profile: profile,
                    password: password
                )
                let current = index + 1
                await MainActor.run { onProgress(RemoteSyncProgress(current: current, total: total)) }
            }
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

    /// Per-month is load-bearing — a flat set would let writer A mask writer B's pending fp on shared content.
    func backedUpAssetFingerprintsByMonth() -> PerMonth<Set<Data>> {
        remoteIndexService.committedAssetFingerprintsByMonth()
    }

    /// `nil` until the first sync identifies the format; callers must not collapse nil to V1.
    func currentRepoIsV2() async -> Bool? {
        await remoteIndexService.currentRepoIsV2()
    }

    /// Returns true only when every probed month succeeded; callers gate the committed-fp fast path on this.
    @discardableResult
    func refreshPhysicalPresenceForResume(profile: ServerProfileRecord, password: String) async throws -> Bool {
        try await preparationService.withConnectedClient(profile: profile, password: password) { client in
            try await self.remoteIndexService.refreshPhysicalPresenceOverlay(client: client, basePath: profile.basePath)
        }
    }

    func lastSyncOverlayFresh() async -> Bool {
        await remoteIndexService.lastSyncOverlayFresh()
    }
}
