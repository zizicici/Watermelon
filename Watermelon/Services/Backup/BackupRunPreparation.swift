import Foundation
import Photos

struct BackupPreparedRun: Sendable {
    let initialClient: any RemoteStorageClientProtocol
    let snapshotSeedLookup: MonthSeedLookup?
    let monthPlans: [MonthWorkItem]
    let workerCount: Int
    let connectionPoolSize: Int
    let totalAssetCount: Int
    let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    let writeMode: RepoWriteMode
}

struct BackupRunPreparationService: Sendable {
    private static let monthSeedLookupEntryThreshold = 120_000

    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let remoteIndexService: RemoteIndexSyncService
    private let databaseManager: DatabaseManager

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService,
        databaseManager: DatabaseManager
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
        self.databaseManager = databaseManager
    }

    func prepareRun(
        request: BackupRunRequest,
        eventStream: BackupEventStream
    ) async throws -> BackupPreparedRun {
        let profile = request.profile
        let password = request.password
        let onlyAssetLocalIdentifiers = request.onlyAssetLocalIdentifiers

        do {
            try await ensurePhotoAuthorization()

            if request.iCloudPhotoBackupMode == .enable {
                eventStream.emitLog(
                    String(localized: "backup.log.icloudAccessEnabled"),
                    level: .info
                )
            }

            let client = try makeStorageClient(profile: profile, password: password)
            try await client.connect()

            var writeMode: RepoWriteMode?
            var lockClientHandle: LiteLockClientHandle?
            do {
                let liteProfile = try databaseManager.profileWithBackfilledWriterID(profile)
                let makeLockClient: ConnectedLockClientProvider = { [self, liteProfile, password] in
                    try await self.makeConnectedLockClient(profile: liteProfile, password: password)
                }
                var lockHandle = try await makeLockClient()
                lockClientHandle = lockHandle
                let plan = try await LiteRepoGateway.prepareForegroundWrite(
                    client: client,
                    lockClient: lockHandle.client,
                    ownsLockClient: lockHandle.ownsClient,
                    basePath: liteProfile.basePath,
                    writerID: liteProfile.writerID,
                    reconnectLockClient: makeLockClient,
                    onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: liteProfile, databaseManager: databaseManager)
                )
                lockHandle.transferToSession()
                lockClientHandle = lockHandle
                let activeWriteMode = RepoWriteMode.lite(plan.session, plan.monthsListing)
                writeMode = activeWriteMode
                var snapshotSeedLookup: MonthSeedLookup?

                do {
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        layout: activeWriteMode.manifestLayout,
                        liteMonthsListing: activeWriteMode.liteMonthsListing
                    )
                    snapshotSeedLookup = makeMonthSeedLookup(from: digest, eventStream: eventStream)
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.remoteIndexSynced"),
                            digest.resourceCount,
                            digest.assetCount
                        ),
                        level: .info
                    )
                } catch {
                    guard shouldContinueAfterRemoteIndexSyncFailure(error) else { throw error }
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.remoteIndexScanWarning"),
                            profile.userFacingStorageErrorMessage(error)
                        ),
                        level: .warning
                    )
                    snapshotSeedLookup = nil
                }

                let retryMode = onlyAssetLocalIdentifiers != nil
                let assetsResult: PHFetchResult<PHAsset>? = retryMode
                    ? nil
                    : photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
                let retryAssets = loadRetryAssets(from: onlyAssetLocalIdentifiers)

                let initialTotal = retryMode ? retryAssets.count : (assetsResult?.count ?? 0)
                eventStream.emit(.started(totalAssets: initialTotal))

                let monthAssetIDsByMonth: [MonthKey: [String]]
                if retryMode {
                    monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: retryAssets)
                } else if let assetsResult {
                    monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: assetsResult)
                } else {
                    monthAssetIDsByMonth = [:]
                }

                let totalAssetCount = monthAssetIDsByMonth.values.reduce(0) { partial, ids in
                    partial + ids.count
                }
                if retryMode {
                    let requested = onlyAssetLocalIdentifiers?.count ?? 0
                    let missing = max(requested - retryAssets.count, 0)
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.retryModeSummary"),
                            requested,
                            retryAssets.count,
                            missing
                        ),
                        level: .info
                    )
                } else {
                    eventStream.emitLog(String(localized: "backup.log.startBackupByAsset"), level: .info)
                }

                let estimatedBytesByMonth = fetchEstimatedBytesByMonth(
                    monthAssetIDsByMonth: monthAssetIDsByMonth,
                    eventStream: eventStream
                )
                let monthPlans = BackupMonthScheduler.buildMonthPlans(
                    assetLocalIdentifiersByMonth: monthAssetIDsByMonth,
                    estimatedBytesByMonth: estimatedBytesByMonth
                )
                let workerCount = BackupMonthScheduler.resolveWorkerCount(
                    profile: profile,
                    monthCount: monthPlans.count,
                    override: request.workerCountOverride
                )
                let connectionPoolSize = BackupMonthScheduler.resolveConnectionPoolSize(
                    profile: profile,
                    workerCount: workerCount,
                    override: request.workerCountOverride
                )

                return BackupPreparedRun(
                    initialClient: client,
                    snapshotSeedLookup: snapshotSeedLookup,
                    monthPlans: monthPlans,
                    workerCount: workerCount,
                    connectionPoolSize: connectionPoolSize,
                    totalAssetCount: totalAssetCount,
                    makeClient: { [storageClientFactory, profile, password] in
                        try storageClientFactory.makeClient(profile: profile, password: password)
                    },
                    writeMode: activeWriteMode
                )
            } catch {
                // Preparation error after lock acquire: drop the lease before unwinding.
                await writeMode?.stopAndRelease()
                await lockClientHandle?.disconnectIfOwned()
                await client.disconnectSafely()
                throw error
            }
        } catch {
            eventStream.emitErrorLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.preflight.prepareFailed"),
                    profile.userFacingStorageErrorMessage(error)
                ),
                unless: error
            )
            throw error
        }
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> RemoteIndexSyncDigest {
        try await withConnectedClient(profile: profile, password: password) { client in
            try await self.reloadRemoteIndex(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                makeConnectedLockClient: {
                    try await self.makeConnectedLockClient(profile: profile, password: password)
                }
            )
        }
    }

    // Reload using a maintenance plan's already-resolved layout so the verify sweep does not pay a second
    // pure-read format probe (the plan's classify already resolved the layout under its lock).
    func reloadRemoteIndex(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        reusing plan: LiteRepoGateway.MaintenancePlan,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> RemoteIndexSyncDigest {
        try await reloadRemoteIndex(
            client: client,
            profile: profile,
            resolvedLayout: plan.layout,
            liteMonthsListing: plan.monthsListing,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress
        )
    }

    func reloadRemoteIndex(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        resolvedLayout: MonthManifestStore.ManifestLayout? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        makeConnectedLockClient: ConnectedLockClientProvider? = nil
    ) async throws -> RemoteIndexSyncDigest {
        let layout: MonthManifestStore.ManifestLayout
        let upgradeSession: LiteWriteSession?
        let activeLiteMonthsListing: LiteMonthsListingSnapshot?
        if let resolvedLayout {
            // Caller already resolved the layout (e.g. the maintenance plan): no second classify.
            layout = resolvedLayout
            upgradeSession = nil
            activeLiteMonthsListing = liteMonthsListing
        } else {
            let prepared = try await prepareReloadLayout(
                client: client,
                profile: profile,
                makeConnectedLockClient: makeConnectedLockClient
            )
            layout = prepared.layout
            upgradeSession = prepared.session
            activeLiteMonthsListing = prepared.monthsListing
        }
        do {
            let digest = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                layout: layout,
                liteMonthsListing: activeLiteMonthsListing
            )
            await upgradeSession?.stopAndRelease()
            eventStream?.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.log.remoteIndexReloaded"),
                    digest.resourceCount,
                    digest.assetCount
                ),
                level: .info
            )
            return digest
        } catch {
            await upgradeSession?.stopAndRelease()
            throw error
        }
    }

    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey
    ) async throws {
        try await withConnectedClient(profile: profile, password: password) { client in
            let plan = try await self.makeMaintenancePlan(client: client, profile: profile, password: password)
            do {
                try await self.verifyMonth(client: client, basePath: profile.basePath, month: month, plan: plan)
                await plan.session?.stopAndRelease()
            } catch {
                await plan.session?.stopAndRelease()
                throw error
            }
        }
    }

    func withDownloadVerificationPlan<T>(
        profile: ServerProfileRecord,
        password: String,
        body: (BackupDownloadVerificationPlan) async throws -> T
    ) async throws -> T {
        try await withConnectedClient(profile: profile, password: password) { client in
            try await self.withDownloadVerificationPlan(
                client: client,
                profile: profile,
                makeConnectedLockClient: {
                    try await self.makeConnectedLockClient(profile: profile, password: password)
                },
                body: body
            )
        }
    }

    func withDownloadVerificationPlan<T>(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        makeConnectedLockClient: ConnectedLockClientProvider? = nil,
        body: (BackupDownloadVerificationPlan) async throws -> T
    ) async throws -> T {
        let plan = try await makeMaintenancePlan(
            client: client,
            profile: profile,
            makeConnectedLockClient: makeConnectedLockClient
        )
        let verifier = BackupDownloadVerificationPlan { [self] month in
            try await verifyMonth(
                client: client,
                basePath: profile.basePath,
                month: month,
                plan: plan
            )
        }
        do {
            let result = try await body(verifier)
            await plan.session?.stopAndRelease()
            return result
        } catch {
            await plan.session?.stopAndRelease()
            throw error
        }
    }

    // In-run upload-finalizer verify: reuse the run's live write lease (the outer `LiteWriteSession`)
    // for the reconcile flush instead of acquiring — and releasing — a fresh same-writer maintenance
    // lock, whose release would delete the active outer lock. Never releases `session`; the run owner does.
    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey,
        reusingSession session: LiteWriteSession?,
        layout: MonthManifestStore.ManifestLayout
    ) async throws {
        try await withConnectedClient(profile: profile, password: password) { client in
            try await self.verifyMonth(
                client: client,
                basePath: profile.basePath,
                month: month,
                plan: LiteRepoGateway.MaintenancePlan(layout: layout, session: session, monthsListing: nil)
            )
        }
    }

    // Resolves verify/maintenance routing. Caller owns releasing `plan.session` across success and failure.
    func makeMaintenancePlan(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        password: String
    ) async throws -> LiteRepoGateway.MaintenancePlan {
        try await makeMaintenancePlan(
            client: client,
            profile: profile,
            makeConnectedLockClient: {
                try await self.makeConnectedLockClient(profile: profile, password: password)
            }
        )
    }

    func makeMaintenancePlan(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        makeConnectedLockClient: ConnectedLockClientProvider? = nil
    ) async throws -> LiteRepoGateway.MaintenancePlan {
        let resolved = try databaseManager.profileWithBackfilledWriterID(profile)
        var lock = try await lockClient(
            fallback: client,
            makeConnectedLockClient: makeConnectedLockClient
        )
        do {
            let plan = try await LiteRepoGateway.prepareMaintenance(
                client: client,
                lockClient: lock.client,
                ownsLockClient: lock.ownsClient,
                basePath: resolved.basePath,
                writerID: resolved.writerID,
                reconnectLockClient: makeConnectedLockClient,
                onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: resolved, databaseManager: databaseManager)
            )
            lock.transferToSession()
            return plan
        } catch {
            await lock.disconnectIfOwned()
            throw error
        }
    }

    func verifyMonth(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        plan: LiteRepoGateway.MaintenancePlan
    ) async throws {
        let session = plan.session
        try await remoteIndexService.verifyMonth(
            client: client,
            basePath: basePath,
            month: month,
            layout: plan.layout,
            assertOwnership: LiteWriteGuard.ownershipAssertion(session)
        )
    }

    func withConnectedClient<T>(
        profile: ServerProfileRecord,
        password: String,
        body: (any RemoteStorageClientProtocol) async throws -> T
    ) async throws -> T {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        do {
            let result = try await body(client)
            await client.disconnectSafely()
            return result
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    private func ensurePhotoAuthorization() async throws {
        let status = photoLibraryService.authorizationStatus()
        if status != .authorized && status != .limited {
            let requested = await photoLibraryService.requestAuthorization()
            guard requested == .authorized || requested == .limited else {
                throw BackupError.photoPermissionDenied
            }
        }
    }

    private func makeStorageClient(
        profile: ServerProfileRecord,
        password: String
    ) throws -> any RemoteStorageClientProtocol {
        try storageClientFactory.makeClient(profile: profile, password: password)
    }

    private func makeConnectedLockClient(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> LiteLockClientHandle {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        return LiteLockClientHandle(client: client)
    }

    private func lockClient(
        fallback: any RemoteStorageClientProtocol,
        makeConnectedLockClient: ConnectedLockClientProvider?
    ) async throws -> LiteLockClientHandle {
        guard let makeConnectedLockClient else {
            return LiteLockClientHandle(client: fallback, ownsClient: false)
        }
        return try await makeConnectedLockClient()
    }

    private func prepareReloadLayout(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        makeConnectedLockClient: ConnectedLockClientProvider? = nil
    ) async throws -> (layout: MonthManifestStore.ManifestLayout, session: LiteWriteSession?, monthsListing: LiteMonthsListingSnapshot?) {
        let resolved = try databaseManager.profileWithBackfilledWriterID(profile)
        let plan = try await LiteRepoGateway.prepareReload(
            client: client,
            basePath: resolved.basePath,
            writerID: resolved.writerID,
            makeLockClient: {
                try await self.lockClient(
                    fallback: client,
                    makeConnectedLockClient: makeConnectedLockClient
                )
            },
            onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: resolved, databaseManager: databaseManager)
        )
        return (plan.layout, plan.session, plan.monthsListing)
    }

    private func loadRetryAssets(from onlyAssetLocalIdentifiers: Set<String>?) -> [PHAsset] {
        guard let retryTargets = onlyAssetLocalIdentifiers else { return [] }
        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: Array(retryTargets), options: nil)
        var retryAssets: [PHAsset] = []
        retryAssets.reserveCapacity(fetched.count)
        for index in 0 ..< fetched.count {
            retryAssets.append(fetched.object(at: index))
        }
        retryAssets.sort { lhs, rhs in
            (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
        }
        return retryAssets
    }

    private func fetchEstimatedBytesByMonth(
        monthAssetIDsByMonth: [MonthKey: [String]],
        eventStream: BackupEventStream
    ) -> [MonthKey: Int64] {
        var estimatedBytesByMonth: [MonthKey: Int64] = [:]
        estimatedBytesByMonth.reserveCapacity(monthAssetIDsByMonth.count)

        for (month, assetIDs) in monthAssetIDsByMonth {
            guard !assetIDs.isEmpty else {
                estimatedBytesByMonth[month] = 0
                continue
            }
            do {
                estimatedBytesByMonth[month] = try hashIndexRepository.fetchTotalFileSizeBytes(
                    assetIDs: Set(assetIDs)
                )
            } catch {
                eventStream.emitLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.log.localHashEstimateWarning"),
                        month.text,
                        error.localizedDescription
                    ),
                    level: .warning
                )
                estimatedBytesByMonth[month] = 0
            }
        }

        return estimatedBytesByMonth
    }

    private func makeMonthSeedLookup(
        from digest: RemoteIndexSyncDigest,
        eventStream: BackupEventStream
    ) -> MonthSeedLookup? {
        // Gate BEFORE materializing the flat-array snapshot — at 100K+ libraries the snapshot
        // itself is ~60-70 MB transient, and we're about to throw it away anyway.
        if digest.totalEntryCount > Self.monthSeedLookupEntryThreshold {
            eventStream.emitLog(
                String.localizedStringWithFormat(String(localized: "backup.log.remoteSnapshotLarge"), digest.totalEntryCount),
                level: .warning
            )
            return nil
        }

        let lookup = MonthSeedLookup(snapshot: remoteIndexService.fullSnapshot())
        return lookup.isEmpty ? nil : lookup
    }

    private func shouldContinueAfterRemoteIndexSyncFailure(_ error: Error) -> Bool {
        if RemoteFaultLite.classify(error) == .cancelled { return false }
        guard let liteError = error as? LiteRepoError else { return true }
        return !liteError.shouldAbortRemoteIndexSync
    }
}
