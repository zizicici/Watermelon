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
    // Where per-month manifests live for this run, and the live Lite write lease (nil under V1).
    let manifestLayout: MonthManifestStore.ManifestLayout
    let liteSession: LiteWriteSession?

    init(
        initialClient: any RemoteStorageClientProtocol,
        snapshotSeedLookup: MonthSeedLookup?,
        monthPlans: [MonthWorkItem],
        workerCount: Int,
        connectionPoolSize: Int,
        totalAssetCount: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol,
        manifestLayout: MonthManifestStore.ManifestLayout = .v1,
        liteSession: LiteWriteSession? = nil
    ) {
        self.initialClient = initialClient
        self.snapshotSeedLookup = snapshotSeedLookup
        self.monthPlans = monthPlans
        self.workerCount = workerCount
        self.connectionPoolSize = connectionPoolSize
        self.totalAssetCount = totalAssetCount
        self.makeClient = makeClient
        self.manifestLayout = manifestLayout
        self.liteSession = liteSession
    }
}

struct BackupRunPreparationService: Sendable {
    private static let monthSeedLookupEntryThreshold = 120_000

    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let remoteIndexService: RemoteIndexSyncService
    private let databaseManager: DatabaseManager
    private let formatCompatibilityService = RemoteFormatCompatibilityService()
    // Internal, default-off Repo V2 cutover switch. No UI path sets it; tests inject it through the
    // BackupCoordinator / service initializer.
    private let liteRepoEnabled: Bool

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService,
        databaseManager: DatabaseManager,
        liteRepoEnabled: Bool = false
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
        self.databaseManager = databaseManager
        self.liteRepoEnabled = liteRepoEnabled
    }

    // Diagnostic multi-device marker for a profile's Lite acquire. nil when the profile is unsaved.
    private func multiDeviceMarker(for profile: ServerProfileRecord) -> (@Sendable () async -> Void)? {
        guard let profileID = profile.id else { return nil }
        let databaseManager = self.databaseManager
        return { try? databaseManager.setMultiDeviceObserved(Date(), profileID: profileID) }
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

            var liteSession: LiteWriteSession?
            var manifestLayout: MonthManifestStore.ManifestLayout = .v1
            do {
                try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
                if liteRepoEnabled {
                    // Lite cutover: route format and take foreground ownership before any write. Skips the
                    // V1 compatibility verify, which would reject the very `.watermelon` repo we now own.
                    let plan = try await LiteRepoGateway.prepareForegroundWrite(
                        client: client,
                        basePath: profile.basePath,
                        writerID: profile.writerID,
                        onForeignWriterObserved: multiDeviceMarker(for: profile)
                    )
                    manifestLayout = plan.layout
                    liteSession = plan.session
                } else {
                    try await formatCompatibilityService.verify(client: client, profile: profile)
                }
                var snapshotSeedLookup: MonthSeedLookup?

                do {
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        layout: manifestLayout
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
                    if profile.isConnectionUnavailableError(error) {
                        throw error
                    }
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.remoteIndexScanWarning"),
                            profile.userFacingStorageErrorMessage(error)
                        ),
                        level: .warning
                    )
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
                    manifestLayout: manifestLayout,
                    liteSession: liteSession
                )
            } catch {
                // Preparation error after lock acquire: drop the lease before unwinding.
                await liteSession?.stopAndRelease()
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
                onSyncProgress: onSyncProgress
            )
        }
    }

    func reloadRemoteIndex(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> RemoteIndexSyncDigest {
        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let layout: MonthManifestStore.ManifestLayout
        if liteRepoEnabled {
            // Pure read: resolve layout via the router, take no lock. The .lite scan in syncIndex is
            // read-only (pushSchemaUpgrade: false).
            layout = try await LiteRepoGateway.resolveReadLayout(client: client, basePath: profile.basePath)
        } else {
            try await formatCompatibilityService.verify(client: client, profile: profile)
            layout = .v1
        }
        let digest = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress,
            layout: layout
        )
        eventStream?.emitLog(
            String.localizedStringWithFormat(
                String(localized: "backup.log.remoteIndexReloaded"),
                digest.resourceCount,
                digest.assetCount
            ),
            level: .info
        )
        return digest
    }

    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey
    ) async throws {
        try await withConnectedClient(profile: profile, password: password) { client in
            let plan = try await self.makeMaintenancePlan(client: client, profile: profile)
            do {
                try await self.verifyMonth(client: client, basePath: profile.basePath, month: month, plan: plan)
                await plan.session?.stopAndRelease()
            } catch {
                await plan.session?.stopAndRelease()
                throw error
            }
        }
    }

    // Resolves verify/maintenance routing: under the flag a Lite repo takes a foreground lock, otherwise
    // a lock-free V1 plan. Caller owns releasing `plan.session` across success and failure.
    func makeMaintenancePlan(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord
    ) async throws -> LiteRepoGateway.MaintenancePlan {
        guard liteRepoEnabled else {
            return LiteRepoGateway.MaintenancePlan(layout: .v1, session: nil)
        }
        return try await LiteRepoGateway.prepareMaintenance(
            client: client,
            basePath: profile.basePath,
            writerID: profile.writerID,
            onForeignWriterObserved: multiDeviceMarker(for: profile)
        )
    }

    func verifyMonth(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        plan: LiteRepoGateway.MaintenancePlan = LiteRepoGateway.MaintenancePlan(layout: .v1, session: nil)
    ) async throws {
        let session = plan.session
        try await remoteIndexService.verifyMonth(
            client: client,
            basePath: basePath,
            month: month,
            layout: plan.layout,
            assertOwnership: session.map { live in
                { await live.assertStillOwned() }
            }
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
}
