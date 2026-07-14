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
            remoteIndexService: remoteIndexService,
            thumbnailRenderer: ThumbnailRenderer()
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
            incrementalFlushInterval: request.incrementalFlushInterval,
            onMonthUploaded: request.onMonthUploaded
        )
    }

    // On-demand backup of specific on-device assets (browser "Back Up This Item"). A scoped run reusing the
    // full pipeline (lease / manifest / verification). Fail-closed against a concurrent run via the lock.
    // Inherits the caller's iCloud-originals setting and the local-index month grouping so an item lands in
    // the same month a normal backup would (and iCloud-only originals upload when the user allows them).
    func backupAssets(
        _ localIdentifiers: Set<String>,
        profile: ServerProfileRecord,
        password: String,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference
    ) async throws -> BackupExecutionResult {
        let request = BackupRunRequest(
            profile: profile,
            password: password,
            onlyAssetLocalIdentifiers: localIdentifiers,
            iCloudPhotoBackupMode: iCloudPhotoBackupMode,
            monthGroupingTimeZone: monthGroupingTimeZone
        )
        let eventStream = BackupEventStream()
        defer { eventStream.finish() }
        return try await runBackup(request: request, eventStream: eventStream)
    }

    // On-demand deletion of one asset from a remote month manifest (browser "Delete from Backup").
    func deleteRemoteAsset(profile: ServerProfileRecord, password: String, month: LibraryMonthKey, assetFingerprint: Data) async throws {
        try await preparationService.deleteRemoteAsset(
            profile: profile,
            password: password,
            month: month,
            assetFingerprint: assetFingerprint
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
        if let uploadContext {
            try await preparationService.verifyMonth(
                profile: profile,
                password: password,
                month: month,
                reusingSession: uploadContext.writeMode.session,
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

                // A directory-valued month slot is skipped by the read-plane digest scan (so it never enters
                // allKnownMonths), but full verify is an owned maintenance op that must still surface it: enumerate
                // those slots so the owned verifyMonth fails the sweep closed instead of reporting success over damaged state.
                let scannedMonths = self.remoteIndexService.allKnownMonths()
                let directoryMonths = try await self.directoryValuedLiteMonthSlots(client: client, profile: profile, plan: plan)
                let uniqueMonths = Array(scannedMonths.union(directoryMonths)).sorted()
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
                await plan.session?.release()
            } catch {
                await plan.session?.release()
                throw error
            }
        }
    }

    func scanLeftoverFiles(
        profile: ServerProfileRecord,
        password: String,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws -> LeftoverScanResult {
        try await preparationService.scanLeftoverFiles(
            profile: profile,
            password: password,
            onProgress: onProgress
        )
    }

    func deleteLeftoverFiles(
        profile: ServerProfileRecord,
        password: String,
        targets: [LeftoverFile],
        includeThumbnails: Bool,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws -> LeftoverDeleteResult {
        try await preparationService.deleteLeftoverFiles(
            profile: profile,
            password: password,
            targets: targets,
            includeThumbnails: includeThumbnails,
            onProgress: onProgress
        )
    }

    // Directory-valued Lite month slots in the months listing, which the read-plane digest scan skips. Full
    // verify enumerates these so its owned verifyMonth fails closed on damaged control state. Non-Lite plans
    // (or an absent months directory) have none.
    private func directoryValuedLiteMonthSlots(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        plan: RepoMaintenancePlan
    ) async throws -> Set<LibraryMonthKey> {
        guard plan.layout == .lite else { return [] }
        let entries: [RemoteStorageEntry]
        if let listing = plan.monthsListing {
            entries = try await listing.entries(client: client, basePath: profile.basePath)
        } else {
            do {
                entries = try await client.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: profile.basePath))
            } catch {
                if RemoteFaultLite.classify(error) == .notFound { return [] }
                throw error
            }
        }
        return RemoteIndexSyncService.directoryValuedLiteMonthSlots(in: entries)
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

    func currentSnapshotRevision() -> UInt64 {
        remoteIndexService.snapshotRevision()
    }

    func snapshotContainsAssetFingerprint(_ fingerprint: Data) -> (contains: Bool, profileKey: String?) {
        remoteIndexService.snapshotContainsAssetFingerprint(fingerprint)
    }
}
