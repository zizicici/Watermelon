import Foundation
import Photos
import os.log

private let deleteLog = Logger(subsystem: "com.zizicici.watermelon", category: "RemoteAssetDelete")

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
        // True for both .retry and .scoped runs — any run targeting an explicit asset-ID set, which ignores
        // month scope (handled release-safe in resolveMonthScope). Combining with a non-.all scope is a caller
        // bug; surface it in debug for every non-.all case, not just .recentMonths.
        let targetsExplicitAssets = onlyAssetLocalIdentifiers != nil
        if targetsExplicitAssets {
            if case .all = request.monthScope {} else {
                assertionFailure("explicit asset IDs ignore monthScope; combining them is a caller bug")
            }
        }
        let monthCalendar = LibraryMonthKey.monthCalendar(preference: request.monthGroupingTimeZone)
        let monthScope = Self.resolveMonthScope(
            request.monthScope,
            targetsExplicitAssets: targetsExplicitAssets,
            now: request.monthScopeNow,
            calendar: monthCalendar
        )

        do {
            try await ensurePhotoAuthorization()

            if request.iCloudPhotoBackupMode == .enable {
                eventStream.emitLog(
                    String(localized: "backup.log.icloudAccessEnabled"),
                    level: .info
                )
            }

            let client = try await connectedClientWithBoundedRecovery(
                isBackground: request.leaseMode == .background,
                makeClient: { try makeStorageClient(profile: profile, password: password) }
            )

            var writeMode: RepoWriteMode?
            var lockClientHandle: LiteLockClientHandle?
            do {
                let liteProfile = try databaseManager.profileWithBackfilledWriterID(profile)
                let makeLockClient: ConnectedLockClientProvider = { [self, liteProfile, password] in
                    try await self.makeConnectedLockClient(profile: liteProfile, password: password)
                }
                let leaseDiagnosticLogger: RepoLeaseDiagnosticLogger = { [eventStream] message, level in
                    eventStream.emitLog(message, level: level)
                }
                var lockHandle = try await makeLockClient()
                lockClientHandle = lockHandle
                // Background records a run off `.started`; emit it BEFORE the write gateway. The gateway can
                // commit durable remote state (V1→Lite migration, version recovery, cleanup) and then still
                // throw — e.g. version.json is published live, then the read-back download fails — so emitting
                // after it would miss that and leave the foreground stale. A pre-gateway connect/lock failure
                // throws before this (correctly not recorded), and a `.skip` is handled before markProfileRan.
                // Placeholder count: the background drain only checks for `.started`, ignoring the total.
                if request.leaseMode == .background {
                    eventStream.emit(.started(totalAssets: 0, totalBytes: nil))
                }
                let plan: LiteRepoGateway.WritePlan
                switch request.leaseMode {
                case .foreground:
                    plan = try await LiteRepoGateway.prepareForegroundWrite(
                        client: client,
                        lockClient: lockHandle.client,
                        ownsLockClient: lockHandle.ownsClient,
                        basePath: liteProfile.basePath,
                        writerID: liteProfile.writerID,
                        reconnectLockClient: makeLockClient,
                        onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: liteProfile, databaseManager: databaseManager),
                        leaseDiagnosticLogger: leaseDiagnosticLogger
                    )
                case .background:
                    switch try await LiteRepoGateway.prepareBackgroundWrite(
                        client: client,
                        lockClient: lockHandle.client,
                        ownsLockClient: lockHandle.ownsClient,
                        basePath: liteProfile.basePath,
                        writerID: liteProfile.writerID,
                        reconnectLockClient: makeLockClient,
                        onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: liteProfile, databaseManager: databaseManager),
                        leaseDiagnosticLogger: leaseDiagnosticLogger,
                        onMigrationProgress: { [eventStream] progress in
                            // Skip the current==0 phase-start marker for counted phases (the per-month lines
                            // carry the count); finalizing has no count and is always logged.
                            if progress.phase != .finalizing, progress.total > 0, progress.current == 0 { return }
                            eventStream.emitLog(Self.migrationLogMessage(progress), level: .info)
                        }
                    ) {
                    case .proceed(let backgroundPlan):
                        plan = backgroundPlan
                    case .skip:
                        // Declined safely pre-lock: surface as skip (handled by the orchestrator), never a failure.
                        throw BackupRunSkipped()
                    }
                }
                lockHandle.transferToSession()
                lockClientHandle = lockHandle
                let activeWriteMode = RepoWriteMode.lite(plan.session, plan.monthsListing)
                writeMode = activeWriteMode
                var snapshotSeedLookup: MonthSeedLookup?

                let syncDownloadConcurrency = Self.resolveSyncDownloadConcurrency(
                    profile: profile,
                    override: request.workerCountOverride
                )
                do {
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        layout: activeWriteMode.manifestLayout,
                        liteMonthsListing: activeWriteMode.liteMonthsListing,
                        makeClient: { [storageClientFactory, profile, password] in
                            try storageClientFactory.makeClient(profile: profile, password: password)
                        },
                        downloadConcurrency: syncDownloadConcurrency,
                        monthFilter: monthScope?.months,
                        newestMonthFirst: request.monthOrdering == .newestMonthFirst,
                        // A run whose profile lost the shared cache to a cross-profile connect mid-flight must
                        // not steal it back (Home would then project the wrong node's library) — the throw
                        // lands in the catch below and the run degrades to a nil seed lookup.
                        contextPolicy: .claimIfUnowned
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

                let assetsResult: PHFetchResult<PHAsset>? = targetsExplicitAssets
                    ? nil
                    : photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true, since: monthScope?.cutoff)
                let retryAssets = loadRetryAssets(from: onlyAssetLocalIdentifiers)

                var monthAssetIDsByMonth: [MonthKey: [String]]
                if targetsExplicitAssets {
                    monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(
                        from: retryAssets,
                        calendar: monthCalendar
                    )
                } else if let assetsResult {
                    monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(
                        from: assetsResult,
                        calendar: monthCalendar
                    )
                } else {
                    monthAssetIDsByMonth = [:]
                }
                // Upload exactly the months the scoped sync seeded. The fetch predicate is open-ended
                // (creationDate >= cutoff), so a future-dated asset could bucket past the window — drop it
                // so we never write an out-of-scope month the sync neither downloaded nor reconciled.
                if let scopeMonths = monthScope?.months {
                    monthAssetIDsByMonth = monthAssetIDsByMonth.filter { scopeMonths.contains($0.key) }
                }

                let totalAssetCount = monthAssetIDsByMonth.values.reduce(0) { partial, ids in
                    partial + ids.count
                }
                if targetsExplicitAssets {
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
                let totalEstimatedBytes = estimatedBytesByMonth.values.reduce(Int64(0), +)
                eventStream.emit(.started(
                    totalAssets: totalAssetCount,
                    totalBytes: totalEstimatedBytes > 0 ? totalEstimatedBytes : nil
                ))
                let monthPlans = BackupMonthScheduler.buildMonthPlans(
                    assetLocalIdentifiersByMonth: monthAssetIDsByMonth,
                    estimatedBytesByMonth: estimatedBytesByMonth,
                    ordering: request.monthOrdering
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
            // A safe background skip is not a failure: re-throw without the prepare-failed error log.
            if error is BackupRunSkipped { throw error }
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
        let downloadConcurrency = Self.resolveSyncDownloadConcurrency(profile: profile)
        return try await withConnectedClient(profile: profile, password: password) { client in
            try await self.reloadRemoteIndex(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                makeConnectedLockClient: {
                    try await self.makeConnectedLockClient(profile: profile, password: password)
                },
                makeClient: { [storageClientFactory] in
                    try storageClientFactory.makeClient(profile: profile, password: password)
                },
                downloadConcurrency: downloadConcurrency
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
        makeConnectedLockClient: ConnectedLockClientProvider? = nil,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)? = nil,
        downloadConcurrency: Int = 1
    ) async throws -> RemoteIndexSyncDigest {
        let layout: MonthManifestStore.ManifestLayout
        let upgradeSession: RepoLeaseSession?
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
                makeConnectedLockClient: makeConnectedLockClient,
                onSyncProgress: onSyncProgress
            )
            layout = prepared.layout
            upgradeSession = prepared.session
            activeLiteMonthsListing = prepared.monthsListing
        }
        onSyncProgress?(RemoteSyncProgress(current: 0, total: 0, kind: .scanningRemoteIndex))
        do {
            let digest = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                layout: layout,
                liteMonthsListing: activeLiteMonthsListing,
                makeClient: makeClient,
                downloadConcurrency: downloadConcurrency
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

    // In-run upload-finalizer verify: reuse the run's live write lease (the outer `RepoLeaseSession`)
    // for the reconcile flush instead of acquiring — and releasing — a fresh same-writer maintenance
    // lock, whose release would delete the active outer lock. Never releases `session`; the run owner does.
    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey,
        reusingSession session: RepoLeaseSession,
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
        // Read-only lease gate: the in-run finalizer verifies concurrently with upload workers, so it must
        // never write the lock. Standalone verify reuses a session whose refresh task maintains the lease.
        try await remoteIndexService.verifyMonth(
            client: client,
            basePath: basePath,
            month: month,
            layout: plan.layout,
            assertOwnership: RepoLeaseGuard.leaseProvenAssertion(session)
        )
    }

    // MARK: - Manual leftover-file cleanup

    // Forward-scan for remote data files no month manifest records (interrupted backup: bytes uploaded,
    // manifest flush never completed). Acquires a maintenance lease for the scan window (migrates V1→Lite,
    // repairs scratch, gives a consistent months listing) and releases it before the user reviews.
    func scanLeftoverFiles(
        profile: ServerProfileRecord,
        password: String,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws -> LeftoverScanResult {
        try await withConnectedClient(profile: profile, password: password) { client in
            let plan = try await self.makeMaintenancePlan(client: client, profile: profile, password: password)
            do {
                let result: LeftoverScanResult
                if plan.layout == .lite {
                    let months = try await self.enumerateManifestMonths(client: client, profile: profile, plan: plan)
                    let scanner = LeftoverFileScanner(
                        client: client,
                        basePath: profile.basePath,
                        months: months,
                        manifestNames: Self.makeLeftoverManifestNamesProvider(client: client, basePath: profile.basePath)
                    )
                    let dataResult = try await scanner.scan { current, total in
                        Task { @MainActor in onProgress(RemoteSyncProgress(current: current, total: total)) }
                    }
                    // Thumbnail orphan scan — non-fatal (a load fault just skips reporting), but
                    // cancellation propagates. Fail-closed live set guards against false orphans.
                    var thumbCount = 0
                    var thumbBytes: Int64 = 0
                    do {
                        let live = try await self.buildLiveFingerprintHexes(
                            client: client, basePath: profile.basePath, months: months
                        )
                        let thumbResult = try await ThumbnailOrphanScanner(
                            client: client, basePath: profile.basePath, liveFingerprintHexes: live
                        ).scan()
                        thumbCount = thumbResult.count
                        thumbBytes = thumbResult.totalBytes
                    } catch {
                        if RemoteFaultLite.classify(error) == .cancelled { throw error }
                    }
                    result = LeftoverScanResult(
                        groups: dataResult.groups,
                        orphanThumbnailCount: thumbCount,
                        orphanThumbnailBytes: thumbBytes
                    )
                } else {
                    result = .empty
                }
                await plan.session?.stopAndRelease()
                return result
            } catch {
                await plan.session?.stopAndRelease()
                throw error
            }
        }
    }

    // Delete the reviewed leftover files under a fresh lease: re-list and re-read each month's manifest, recompute
    // the leftover set, and prove ownership before deleting. A file recorded since the scan is left intact.
    func deleteLeftoverFiles(
        profile: ServerProfileRecord,
        password: String,
        targets: [LeftoverFile],
        includeThumbnails: Bool,
        onProgress: @escaping @MainActor @Sendable (RemoteSyncProgress) -> Void
    ) async throws -> LeftoverDeleteResult {
        guard !targets.isEmpty || includeThumbnails else { return .empty }
        return try await withConnectedClient(profile: profile, password: password) { client in
            let plan = try await self.makeMaintenancePlan(client: client, profile: profile, password: password)
            do {
                let result: LeftoverDeleteResult
                if plan.layout == .lite {
                    let assertOwnership = RepoLeaseGuard.leaseProvenAssertion(plan.session)
                    let scanner = LeftoverFileScanner(
                        client: client,
                        basePath: profile.basePath,
                        months: [],
                        manifestNames: Self.makeLeftoverManifestNamesProvider(client: client, basePath: profile.basePath)
                    )
                    let dataResult = try await scanner.delete(
                        targets,
                        assertOwnership: assertOwnership
                    ) { current, total in
                        Task { @MainActor in onProgress(RemoteSyncProgress(current: current, total: total)) }
                    }
                    // Re-verify orphan thumbnails under the held lease (rebuild the live set + re-scan),
                    // then delete the current orphans. Non-fatal on fault (skip), cancellation propagates.
                    var thumbDeleted = 0
                    var thumbBytes: Int64 = 0
                    if includeThumbnails {
                        do {
                            let months = try await self.enumerateManifestMonths(client: client, profile: profile, plan: plan)
                            let live = try await self.buildLiveFingerprintHexes(
                                client: client, basePath: profile.basePath, months: months
                            )
                            let thumbScanner = ThumbnailOrphanScanner(
                                client: client, basePath: profile.basePath, liveFingerprintHexes: live
                            )
                            let orphans = try await thumbScanner.scan().orphans
                            let tResult = try await thumbScanner.delete(orphans, assertOwnership: assertOwnership)
                            thumbDeleted = tResult.deletedCount
                            thumbBytes = tResult.deletedBytes
                        } catch {
                            if RemoteFaultLite.classify(error) == .cancelled { throw error }
                        }
                    }
                    result = LeftoverDeleteResult(
                        deletedCount: dataResult.deletedCount,
                        deletedBytes: dataResult.deletedBytes,
                        failedCount: dataResult.failedCount,
                        deletedThumbnailCount: thumbDeleted,
                        deletedThumbnailBytes: thumbBytes
                    )
                } else {
                    result = LeftoverDeleteResult(deletedCount: 0, deletedBytes: 0, failedCount: targets.count)
                }
                await plan.session?.stopAndRelease()
                return result
            } catch {
                await plan.session?.stopAndRelease()
                throw error
            }
        }
    }

    // Authoritative union of every month's asset fingerprints — the live set for thumbnail-sidecar GC.
    // Fail-closed: a genuinely absent month manifest (notFound) is skipped, but any other load fault
    // throws so the set is never silently incomplete (which would falsely orphan live thumbnails).
    private func buildLiveFingerprintHexes(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        months: [LibraryMonthKey]
    ) async throws -> Set<String> {
        var live = Set<String>()
        for month in months {
            try Task.checkCancellation()
            let store: MonthManifestStore?
            do {
                store = try await MonthManifestStore.loadManifestDirect(
                    client: client,
                    basePath: basePath,
                    year: month.year,
                    month: month.month,
                    layout: .lite,
                    pushSchemaUpgrade: false,
                    assertOwnership: nil,
                    surfaceDownloadNotFound: true
                )
            } catch {
                if RemoteFaultLite.classify(error) == .notFound { continue }
                throw error
            }
            guard let store else { throw RemoteStorageClientError.unavailable }
            live.formUnion(store.assetFingerprintHexes())
        }
        return live
    }

    // Months proven to belong to us: those with a parseable `<YYYY-MM>.sqlite` under .watermelon/months.
    // A month with only a data directory and no manifest is never enumerated (we can't claim it).
    private func enumerateManifestMonths(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        plan: LiteRepoGateway.MaintenancePlan
    ) async throws -> [LibraryMonthKey] {
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
        var months = Set<LibraryMonthKey>()
        for entry in entries where !entry.isDirectory {
            if let month = RepoLayoutLite.month(fromFilename: entry.name) {
                months.insert(month)
            }
        }
        return months.sorted()
    }

    // Authoritative per-month manifest names via a pure read (no lease write, no schema push). nil only for a
    // genuinely absent manifest; any other fault throws so the expected set is never silently emptied.
    private static func makeLeftoverManifestNamesProvider(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) -> LeftoverFileScanner.ManifestNamesProvider {
        { @Sendable month in
            let store: MonthManifestStore?
            do {
                store = try await MonthManifestStore.loadManifestDirect(
                    client: client,
                    basePath: basePath,
                    year: month.year,
                    month: month.month,
                    layout: .lite,
                    pushSchemaUpgrade: false,
                    assertOwnership: nil,
                    surfaceDownloadNotFound: true
                )
            } catch {
                if RemoteFaultLite.classify(error) == .notFound { return nil }
                throw error
            }
            // loadManifestDirect returns nil for a non-notFound download fault — fail closed.
            guard let store else { throw RemoteStorageClientError.unavailable }
            return store.manifestFileNames()
        }
    }

    func withConnectedClient<T>(
        profile: ServerProfileRecord,
        password: String,
        body: (any RemoteStorageClientProtocol) async throws -> T
    ) async throws -> T {
        // Ride out a transient connect blip (and bound a half-open one) instead of failing the verify/reload/
        // inline-finalize path on a single wobble.
        let client = try await connectedClientWithBoundedRecovery(
            isBackground: false,
            makeClient: { try self.makeStorageClient(profile: profile, password: password) }
        )
        do {
            let result = try await body(client)
            await client.disconnectSafely()
            return result
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    // Resolves a `.recentMonths(n)` scope into the cutoff date (for the asset-fetch predicate) and the
    // exact month-key set (for the remote-index sync filter). `.all` returns nil — no scoping.
    static func resolveMonthScope(
        _ scope: BackupMonthScope,
        targetsExplicitAssets: Bool = false,
        now: Date = Date(),
        calendar: Calendar = LibraryMonthKey.currentPreferenceMonthCalendar()
    ) -> (cutoff: Date, months: Set<LibraryMonthKey>)? {
        // A run targeting explicit asset IDs (.retry/.scoped) ignores month scope — it would drop requested
        // targets. This is the release-safe contract, not just a debug assert.
        guard !targetsExplicitAssets else { return nil }
        switch scope {
        case .all:
            return nil
        case .recentMonths(let count):
            let n = max(1, count)
            guard let start = calendar.date(byAdding: .month, value: -(n - 1), to: now),
                  let cutoff = calendar.date(from: calendar.dateComponents([.year, .month], from: start)) else {
                return nil
            }
            var months: Set<LibraryMonthKey> = []
            var cursor = cutoff
            while cursor <= now {
                let comps = calendar.dateComponents([.year, .month], from: cursor)
                if let year = comps.year, let month = comps.month {
                    months.insert(LibraryMonthKey(year: year, month: month))
                }
                guard let next = calendar.date(byAdding: .month, value: 1, to: cursor) else { break }
                cursor = next
            }
            return (cutoff, months)
        }
    }

    // Background V1→Lite migration runs unattended with no overlay, so we trace it into the execution log.
    // Reuses the repo-upgrade overlay strings (already localized in every locale) rather than duplicating copy.
    // Background still emits .cleaning (the committed-V1-manifest prune runs unconditionally); it only skips
    // the broader orphan-cleanup pass (runCleanup: false).
    static func migrationLogMessage(_ progress: V1ToLiteMigrationProgress) -> String {
        func counted(_ key: String.LocalizationValue, fallback: String.LocalizationValue) -> String {
            progress.total > 0
                ? String.localizedStringWithFormat(String(localized: key), progress.current, progress.total)
                : String(localized: fallback)
        }
        switch progress.phase {
        case .copying:
            return counted("home.overlay.upgradingRepoMonths", fallback: "home.overlay.upgradingRepo")
        case .validating:
            return counted("home.overlay.validatingRepoMonths", fallback: "home.overlay.upgradingRepo")
        case .finalizing:
            return String(localized: "home.overlay.finalizingRepo")
        case .cleaning:
            return counted("home.overlay.cleaningRepoMonths", fallback: "home.overlay.cleaningRepo")
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

    // Deletes ONE asset from a remote month manifest on demand (browser "Delete from Backup"), reusing the
    // same foreground write-lease lifecycle a backup run uses. Ref-counted resource files that become
    // orphaned are deleted too. Fail-closed: if a run already holds the lease, prepareForegroundWrite throws
    // and nothing is mutated. `stopAndRelease()` is idempotent, so the lock is never leaked.
    func deleteRemoteAsset(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey,
        assetFingerprint: Data
    ) async throws {
        let client = try await connectedClientWithBoundedRecovery(
            isBackground: false,
            makeClient: { try self.makeStorageClient(profile: profile, password: password) }
        )
        var writeMode: RepoWriteMode?
        var lockClientHandle: LiteLockClientHandle?
        // Owner key for the cache reconciliations below: a cross-profile connect completing while the remote
        // leg ran re-tags the shared cache, and this delete's write-back must then be dropped, not applied.
        let actingProfileKey = RemoteIndexSyncService.remoteProfileKey(profile)
        do {
            let liteProfile = try databaseManager.profileWithBackfilledWriterID(profile)

            // Resolve the delete target ONCE into a 3-way outcome: the asset's month manifest is `.present`
            // (mutate it), the `.monthGone`, or the whole repo is gone (`.repoGone`, a `.fresh` classify). Both
            // "gone" cases skip the remote write and just reconcile the cache below.
            enum DeleteTarget {
                case present(mode: RepoWriteMode, store: MonthManifestStore)
                case monthGone
                case repoGone
            }
            let resolved: DeleteTarget
            if case .fresh = try await LiteRepoGateway.classifyRepo(client: client, basePath: liteProfile.basePath) {
                // Repo gone (`.fresh` = no committed / V1 repo): decide BEFORE acquiring the lock, else
                // prepareForegroundWrite would re-initialize a brand-new repo (basePath + version.json + lock).
                resolved = .repoGone
            } else {
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
                let mode = RepoWriteMode.lite(plan.session, plan.monthsListing)
                writeMode = mode

                // Month gone: loadOrCreate would mint + flush a fresh empty manifest (right for a backup, wrong
                // for a delete), so probe first. Absent → same "gone" no-op path as a fresh repo.
                let manifestPath = mode.manifestLayout.manifestAbsolutePath(basePath: liteProfile.basePath, year: month.year, month: month.month)
                if try await client.metadata(path: manifestPath) != nil {
                    resolved = .present(mode: mode, store: try await MonthManifestStore.loadOrCreate(
                        client: client,
                        basePath: liteProfile.basePath,
                        year: month.year,
                        month: month.month,
                        layout: mode.manifestLayout,
                        assertOwnership: mode.leaseProvenAssertion,
                        liteMonthsListing: mode.liteMonthsListing
                    ))
                } else {
                    resolved = .monthGone
                }
            }

            switch resolved {
            case let .present(mode, store):
                let orphanFileNames = try store.removeAsset(fingerprint: assetFingerprint)
                _ = try await store.flushToRemote()
                // The manifest is committed (the asset IS removed). Delete the now-orphaned resource files
                // through a LeasedRemoteWriter, which re-proves ownership before EACH delete: a lease lost
                // mid-sweep stops it, leaving the rest as a leak — never corruption of a writer that
                // re-referenced one of these content-addressed files. NOTE: leaked resource bytes are NOT
                // auto-reclaimed (OrphanCleanupLite only sweeps metadata scratch / locks / V1 manifests); they
                // surface only via manual leftover cleanup, so log which files leaked.
                let writer = LeasedRemoteWriter(client: client, mode: mode)
                let monthDir = String(format: "%04d/%02d", month.year, month.month)
                var leakedFileNames: [String] = []
                orphanSweep: for (index, fileName) in orphanFileNames.enumerated() {
                    let path = RemotePathBuilder.absolutePath(basePath: liteProfile.basePath, remoteRelativePath: "\(monthDir)/\(fileName)")
                    switch await writer.deleteOrphan(path: path) {
                    case .deleted:
                        continue
                    case .failed:
                        leakedFileNames.append(fileName)
                    case .leaseLost, .cancelled:
                        leakedFileNames.append(contentsOf: orphanFileNames[index...])   // this + all remaining leak
                        break orphanSweep
                    }
                }
                if !leakedFileNames.isEmpty {
                    deleteLog.error("deleteRemoteAsset: \(leakedFileNames.count, privacy: .public) orphan resource file(s) leaked (asset deleted; recoverable only via manual leftover cleanup): \(leakedFileNames, privacy: .public)")
                }
                let snap = store.unsortedSnapshot()
                await remoteIndexService.replaceCachedMonthSynchronized(month, resources: snap.resources, assets: snap.assets, links: snap.links, expectedProfileKey: actingProfileKey)
                // The now-orphaned thumbnail sidecar is reclaimed by the authoritative ThumbnailOrphanScanner
                // maintenance sweep (which lists the real remote thumbs against the full manifest). We deliberately
                // don't reclaim it inline off the cached snapshot — that could miss a grouping-TZ twin in an
                // unsynced month and delete a still-referenced shared sidecar.
            case .repoGone:
                // Whole repo gone: every cached month is stale, so drop the entire snapshot.
                await remoteIndexService.resetSnapshotCache(expectedProfileKey: actingProfileKey)
            case .monthGone:
                // Only this month gone: drop just it so a reload doesn't resurrect the deleted asset, and forget
                // its digest so a later sync doesn't read a stale "unchanged" and skip re-pulling a re-created month.
                await remoteIndexService.replaceCachedMonthSynchronized(month, resources: [], assets: [], links: [], expectedProfileKey: actingProfileKey)
                await remoteIndexService.forgetMonthDigest(month, expectedProfileKey: actingProfileKey)
            }
            // The cache mutations above changed the browser-visible remote library. The sync path posts this on
            // its own commits; the on-demand delete must too, so every LibraryPresenceIndex (not just the acting
            // one) rebuilds instead of holding a stale fingerprint set.
            NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
            await writeMode?.stopAndRelease()
            await client.disconnectSafely()
        } catch {
            await writeMode?.stopAndRelease()
            await lockClientHandle?.disconnectIfOwned()
            await client.disconnectSafely()
            throw error
        }
    }

    private func makeStorageClient(
        profile: ServerProfileRecord,
        password: String
    ) throws -> any RemoteStorageClientProtocol {
        try storageClientFactory.makeClient(profile: profile, password: password)
    }

    // Manifest-download concurrency for index sync: the protocol's connection-pool cap (the real bound is
    // applied inside syncIndex via min(_, changedMonths.count)). `monthCount: .max` yields the pure policy cap.
    static func resolveSyncDownloadConcurrency(profile: ServerProfileRecord, override: Int? = nil) -> Int {
        let workerCount = BackupMonthScheduler.resolveWorkerCount(
            profile: profile,
            monthCount: Int.max,
            override: override
        )
        return BackupMonthScheduler.resolveConnectionPoolSize(
            profile: profile,
            workerCount: workerCount,
            override: override
        )
    }

    private func makeConnectedLockClient(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> LiteLockClientHandle {
        let client = try makeStorageClient(profile: profile, password: password)
        try await NetworkRecovery.boundedConnect(
            client, deadline: Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout)
        )
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
        makeConnectedLockClient: ConnectedLockClientProvider? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> (layout: MonthManifestStore.ManifestLayout, session: RepoLeaseSession?, monthsListing: LiteMonthsListingSnapshot?) {
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
            onForeignWriterObserved: MultiDeviceMarkerFactory.make(for: resolved, databaseManager: databaseManager),
            onMigrationProgress: { progress in
                onSyncProgress?(
                    RemoteSyncProgress(
                        current: progress.current,
                        total: progress.total,
                        kind: .repoUpgrade(progress.phase)
                    )
                )
            }
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
        switch RemoteFaultLite.classify(error) {
        case .cancelled:
            return false
        case .retryable:
            // A transient sync fault degrades to a nil seed and proceeds (loadOrCreate re-lists per month),
            // rather than aborting the whole run — classification-driven, not error-type-driven.
            return true
        case .terminal, .notFound:
            guard let liteError = error as? LiteRepoError else { return true }
            return !liteError.shouldAbortRemoteIndexSync
        }
    }

    // Bounded reconnect for the run's initial data-client connect: a transient at run start rides out the window
    // or pauses (resumable via BackupNetworkRecoveryExhausted), instead of failing the whole run; terminal /
    // not-found fail fast. Window mirrors the worker's (foreground ~ lease expiry; background ~ BG-task grace).
    private func connectedClientWithBoundedRecovery(
        isBackground: Bool,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) async throws -> any RemoteStorageClientProtocol {
        let deadline = Date().addingTimeInterval(NetworkRecoveryPolicy.window(background: isBackground))
        switch await NetworkRecovery.connectRidingOut(deadline: deadline, makeClient: makeClient) {
        case .succeeded(let client):
            return client
        case .failed(let error):
            throw error
        case .exhausted(let error):
            throw BackupNetworkRecoveryExhausted(underlying: error)
        case .cancelled:
            throw CancellationError()
        case .stopped(let error):   // no shouldStop predicate, so this never occurs
            throw error
        }
    }
}
