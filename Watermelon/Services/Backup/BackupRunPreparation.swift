import Foundation
import Photos

struct BackupPreparedRun: Sendable {
    let initialClient: any RemoteStorageClientProtocol
    let monthPlans: [MonthWorkItem]
    let workerCount: Int
    let connectionPoolSize: Int
    let totalAssetCount: Int
    let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    let v2Services: BackupV2RuntimeServices?
}

struct BackupRunPreparationService: Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let remoteIndexService: RemoteIndexSyncService
    private let databaseManager: DatabaseManager
    private let formatCompatibilityService = RemoteFormatCompatibilityService()

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

            var leaseForCleanup: BackupV2RuntimeLease?
            do {
                let lease = try await prepareV2Runtime(client: client, profile: profile, password: password, eventStream: eventStream)
                leaseForCleanup = lease
                let v2Services = lease?.services

                do {
                    let preMaterialized = await v2Services?.initialMaterializeOutput.peek()
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        preMaterialized: preMaterialized,
                        preInspection: v2Services?.postOpenSyncInspection,
                        expectV2: v2Services != nil,
                        localRepoID: v2Services?.repoID
                    )
                    _ = await v2Services?.initialMaterializeOutput.consume()
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.remoteIndexSynced"),
                            digest.resourceCount,
                            digest.assetCount
                        ),
                        level: .info
                    )
                } catch {
                    if error is CancellationError { throw error }
                    if profile.isConnectionUnavailableError(error) {
                        throw error
                    }
                    // Fatal: continuing past a compat error would spin up worker pools against a half-initialized repo.
                    if error is BackupCompatibilityError {
                        throw error
                    }
                    if v2Services != nil {
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

                // V2 runtime built ≡ V2 repo; pin isV2 so a non-fatal sync throw can't drop resume back to V1 dedup.
                if v2Services != nil {
                    await remoteIndexService.markIsV2()
                }

                let retryMode = onlyAssetLocalIdentifiers != nil
                let assetsResult: PHFetchResult<PHAsset>? = retryMode
                    ? nil
                    : photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
                let retryAssets = loadRetryAssets(from: onlyAssetLocalIdentifiers)

                let initialTotal = retryMode ? retryAssets.count : (assetsResult?.count ?? 0)
                eventStream.emit(.started(totalAssets: initialTotal))

                let monthAssetIDsByMonth: [MonthKey: [PhotoKitLocalIdentifier]]
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
                    monthPlans: monthPlans,
                    workerCount: workerCount,
                    connectionPoolSize: connectionPoolSize,
                    totalAssetCount: totalAssetCount,
                    makeClient: { [storageClientFactory, profile, password] in
                        try storageClientFactory.makeClient(profile: profile, password: password)
                    },
                    v2Services: v2Services
                )
            } catch {
                if let lease = leaseForCleanup {
                    await lease.shutdown()
                }
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
        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
        let identity = RepoIdentity(database: databaseManager)
        let localRepoID = try await identity.findRepoStateByProfile(profileID: profile.id ?? 0)?.repoID
        let digest = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress,
            localRepoID: localRepoID
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

    @discardableResult
    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey
    ) async throws -> Bool {
        try await withConnectedClient(profile: profile, password: password) { client in
            try await self.verifyMonth(client: client, basePath: profile.basePath, month: month, profile: profile, password: password)
        }
    }

    @discardableResult
    func verifyMonth(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        profile: ServerProfileRecord? = nil,
        password: String? = nil
    ) async throws -> Bool {
        let inspection: RemoteFormatInspection
        if let profile {
            inspection = try await formatCompatibilityService
                .inspectRemoteFormat(client: client, profile: profile)
        } else {
            inspection = try await formatCompatibilityService
                .inspectRemoteFormatProfileless(client: client, basePath: basePath)
        }
        let hasPriorV2Binding: Bool
        if let profileID = profile?.id {
            let identity = RepoIdentity(database: databaseManager)
            let localState = try await identity.findRepoStateByProfile(profileID: profileID)
            if localState != nil {
                hasPriorV2Binding = true
            } else {
                hasPriorV2Binding = await remoteIndexService.materializedRepoID() != nil
            }
        } else {
            hasPriorV2Binding = false
        }
        let action = BackupV2RepoVerifyPlanner.plan(
            inspection: inspection,
            hasPriorV2Binding: hasPriorV2Binding
        )
        switch action {
        case .throwUnsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .verifyMonthV2:
            _ = try await verifyMonthV2(client: client, basePath: basePath, month: month, profile: profile, password: password)
            return true
        case .throwRequiresForegroundMigration:
            throw BackupCompatibilityError.requiresForegroundMigration
        case .verifyMonthV1:
            try await remoteIndexService.verifyMonth(
                client: client,
                basePath: basePath,
                month: month
            )
            // V1 verify may flush manifest changes; treat as potentially mutating.
            return true
        case .skipFreshRepo:
            return false
        case .throwDamagedV2Repo:
            throw BackupCompatibilityError.damagedV2Repo
        }
    }

    @discardableResult
    func verifyMonthV2(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        profile: ServerProfileRecord? = nil,
        password: String? = nil
    ) async throws -> VerifyMonthReport {
        let metadataClient = wrapIfSerial(client)
        // Verifier needs remote repoID to filter foreign-id commits; absent on a V2 repo means broken identity, refuse.
        let expectedRepoID: String
        do {
            expectedRepoID = try await RepoCanonicalIdentityReader(client: metadataClient, basePath: basePath)
                .requireCanonical(absentError: {
                    NSError(
                        domain: "BackupRunPreparation",
                        code: -51,
                        userInfo: [NSLocalizedDescriptionKey: "V2 repo missing canonical identity - run a backup to repair before verifying"]
                    )
                })
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            throw BackupV2RuntimeOpenErrorMapping.translateToCompatibilityError(bootstrapError: bootstrap)
        }
        if let profileID = profile?.id {
            let identity = RepoIdentity(database: databaseManager)
            let localState = try await identity.findRepoStateByProfile(profileID: profileID)
            let priorRepoID: String?
            if let stateRepoID = localState?.repoID {
                priorRepoID = stateRepoID
            } else {
                priorRepoID = await remoteIndexService.materializedRepoID()
            }
            if let priorRepoID, priorRepoID != expectedRepoID {
                // Proven a different repo at the same endpoint: drop the stale committed view so
                // Home can't keep serving the old repo's rows after this verify refusal.
                remoteIndexService.invalidateCommittedViewForCompatibilityFailure()
                throw BackupCompatibilityError.repoIdentityMismatch(stored: priorRepoID, observed: expectedRepoID)
            }
        }
        let verifier = RepoVerifyMonthService(client: metadataClient, basePath: basePath, expectedRepoID: expectedRepoID)
        var report = try await verifier.verify(month: month)
        if !report.cleanupCandidates.isEmpty, let profile {
            let lease = try await BackupV2RuntimeLease.forVerifyMonth(
                client: client,
                borrowedMetadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: formatCompatibilityService
            )
            do {
                let appliedFingerprints = try await verifier.applyTombstones(
                    month: month,
                    cleanupItems: report.cleanupCandidates,
                    services: lease.services
                )
                report.didMutateRemote = !appliedFingerprints.isEmpty
                // Evict only what was actually tombstoned; applyTombstones may have skipped since-healed items.
                if !appliedFingerprints.isEmpty,
                   let monthData = remoteIndexService.remoteMonthRawData(for: month) {
                    let remainingAssets = monthData.assets.filter { !appliedFingerprints.contains($0.assetFingerprint) }
                    let remainingLinks = monthData.assetResourceLinks.filter { !appliedFingerprints.contains($0.assetFingerprint) }
                    remoteIndexService.replaceCachedMonth(
                        month,
                        resources: monthData.resources,
                        assets: remainingAssets,
                        links: remainingLinks
                    )
                }
                _ = try await remoteIndexService.syncIndex(client: client, profile: profile, expectV2: true, localRepoID: expectedRepoID)
                await lease.shutdown()
            } catch {
                await lease.shutdown()
                throw error
            }
        } else if let profile {
            _ = try await remoteIndexService.syncIndex(client: client, profile: profile, expectV2: true, localRepoID: expectedRepoID)
        }
        return report
    }

    func withConnectedClient<T>(
        profile: ServerProfileRecord,
        password: String,
        body: (any RemoteStorageClientProtocol) async throws -> T
    ) async throws -> T {
        let client = try makeStorageClient(profile: profile, password: password)
        do {
            try await client.connect()
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
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

    private func loadRetryAssets(from onlyAssetLocalIdentifiers: Set<PhotoKitLocalIdentifier>?) -> [PHAsset] {
        guard let retryTargets = onlyAssetLocalIdentifiers else { return [] }
        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: retryTargets.rawValues, options: nil)
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
        monthAssetIDsByMonth: [MonthKey: [PhotoKitLocalIdentifier]],
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

    private func prepareV2Runtime(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream
    ) async throws -> BackupV2RuntimeLease? {
        return try await BackupV2RuntimeLease.forForegroundRun(
            client: client,
            profile: profile,
            databaseManager: databaseManager,
            format: formatCompatibilityService,
            eventStream: eventStream,
            makeMetadataClient: {
                // Dedicated metadata connection so metadata writes don't contend with worker uploads.
                let raw = try storageClientFactory.makeClient(profile: profile, password: password)
                try await raw.connect()
                return raw
            }
        )
    }

}
