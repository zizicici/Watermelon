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
        month: LibraryMonthKey,
        reusing uploadContext: BackupMonthUploadContext? = nil
    ) async throws {
        // An in-run upload finalizer reuses the run's live write lease so its reconcile flush is owned
        // without acquiring/releasing an independent same-writer maintenance lock — which would drop the
        // active outer lease. Out-of-run verify keeps an independent maintenance session.
        if let uploadContext, let session = uploadContext.writeMode.liteSession {
            try await preparationService.verifyMonth(
                profile: profile,
                password: password,
                month: month,
                reusingSession: session,
                layout: uploadContext.writeMode.manifestLayout
            )
        } else {
            try await preparationService.verifyMonth(
                profile: profile,
                password: password,
                month: month
            )
        }
    }

    func withDownloadVerificationPlan<T>(
        profile: ServerProfileRecord,
        password: String,
        body: (BackupDownloadVerificationPlan) async throws -> T
    ) async throws -> T {
        try await preparationService.withDownloadVerificationPlan(
            profile: profile,
            password: password,
            body: body
        )
    }

    func verifyAllMonths(
        profile: ServerProfileRecord,
        password: String,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws {
        try await preparationService.withConnectedClient(profile: profile, password: password) { client in
            // Acquire one verify lease for the whole sweep (Lite repos only), then reuse its resolved
            // layout for the remote index sync instead of a second pure-read format probe. Released on
            // every exit.
            let plan = try await self.preparationService.makeMaintenancePlan(
                client: client,
                profile: profile,
                password: password
            )
            do {
                _ = try await self.preparationService.reloadRemoteIndex(
                    client: client, profile: profile, reusing: plan
                )

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
                        plan: plan
                    )
                    let current = index + 1
                    await MainActor.run { onProgress(RemoteSyncProgress(current: current, total: total)) }
                }
                await plan.session?.stopAndRelease()
            } catch {
                await plan.session?.stopAndRelease()
                throw error
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
}
