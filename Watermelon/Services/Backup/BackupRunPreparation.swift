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
    private static let maxProfileLessVersionFileBytes: Int64 = 64 * 1024

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

            var v2ServicesForCleanup: BackupV2RuntimeServices?
            do {
                let v2Services = try await prepareV2Runtime(client: client, profile: profile, password: password, eventStream: eventStream)
                v2ServicesForCleanup = v2Services

                do {
                    let preMaterialized = await v2Services?.initialMaterializeOutput.peek()
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        preMaterialized: preMaterialized,
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
                if let v2 = v2ServicesForCleanup {
                    await v2.shutdown()
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
        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
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
            inspection = try await RemoteFormatCompatibilityService()
                .inspectRemoteFormat(client: client, profile: profile)
        } else {
            // Profile-less inspect must still parse version.json so a future format isn't downgraded to V2 stamped at the local formatVersion.
            inspection = try await Self.profileLessInspect(client: client, basePath: basePath)
        }
        if let profileID = profile?.id {
            let identity = RepoIdentity(database: databaseManager)
            let localState = try await identity.findRepoStateByProfile(profileID: profileID)
            if localState != nil {
                switch inspection {
                case .fresh:
                    throw BackupCompatibilityError.damagedV2Repo
                case .v1:
                    throw BackupCompatibilityError.requiresForegroundMigration
                default:
                    break
                }
            } else if await remoteIndexService.materializedRepoID() != nil {
                switch inspection {
                case .fresh:
                    throw BackupCompatibilityError.damagedV2Repo
                case .v1:
                    throw BackupCompatibilityError.requiresForegroundMigration
                default:
                    break
                }
            }
        }
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2, .v2WithPendingMigrationCleanup:
            _ = try await verifyMonthV2(client: client, basePath: basePath, month: month, profile: profile, password: password)
            return true
        case .v2WithV1Manifests:
            throw BackupCompatibilityError.requiresForegroundMigration
        case .v1:
            try await remoteIndexService.verifyMonth(
                client: client,
                basePath: basePath,
                month: month
            )
            // V1 verify may flush manifest changes; treat as potentially mutating.
            return true
        case .fresh:
            return false
        }
    }

    /// Read+parse `.watermelon/version.json` directly so the profile-less verify path doesn't hardcode the local formatVersion onto a remote that may be V3+.
    private static func profileLessInspect(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RemoteFormatInspection {
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        guard let meta = try await client.metadata(path: versionPath) else {
            return .v1
        }
        // Pre-bound the size so a damaged/oversized version.json never reaches the parser.
        guard !meta.isDirectory,
              meta.size <= Self.maxProfileLessVersionFileBytes else {
            throw BackupCompatibilityError.damagedV2Repo
        }
        let load: VersionManifestStore.Load
        do {
            load = try await VersionManifestStore(client: client, basePath: basePath).load()
        } catch is RepoBootstrap.VersionConflict {
            throw BackupCompatibilityError.damagedV2Repo
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            if case .ioFailure = bootstrap { throw BackupCompatibilityError.damagedV2Repo }
            throw bootstrap
        }
        switch load {
        case .absent:
            return .v1
        case .found(let manifest):
            let formatVersion = manifest.formatVersion
            if formatVersion > RepoLayout.currentSupportedFormatVersion {
                return .unsupported(minAppVersion: manifest.minAppVersion)
            }
            if formatVersion >= 2 {
                return .v2(formatVersion: formatVersion)
            }
            return .v1
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
        let bootstrap = RepoBootstrap(client: metadataClient, basePath: basePath)
        let expectedRepoID: String
        switch try await bootstrap.loadRepoIDStrict() {
        case .absent:
            throw NSError(
                domain: "BackupRunPreparation",
                code: -51,
                userInfo: [NSLocalizedDescriptionKey: "V2 repo missing .watermelon/repo.json — run a backup to repair before verifying"]
            )
        case .found(let id):
            expectedRepoID = id
        }
        if let profileID = profile?.id {
            let identity = RepoIdentity(database: databaseManager)
            let localState = try await identity.findRepoStateByProfile(profileID: profileID)
            if let localState, localState.repoID != expectedRepoID {
                throw BackupCompatibilityError.repoIdentityMismatch
            }
            if localState == nil {
                let cachedRepoID = await remoteIndexService.materializedRepoID()
                if let cachedRepoID, cachedRepoID != expectedRepoID {
                    throw BackupCompatibilityError.repoIdentityMismatch
                }
            }
        }
        let verifier = RepoVerifyMonthService(client: metadataClient, basePath: basePath, expectedRepoID: expectedRepoID)
        var report = try await verifier.verify(month: month)
        if !report.cleanupCandidates.isEmpty, let profile {
            let v2: BackupV2RuntimeServices
            do {
                v2 = try await BackupV2RuntimeBuilder.build(
                    client: client,
                    metadataClient: metadataClient,
                    ownsMetadataClient: false,
                    runMaintenanceTasks: false,
                    profile: profile,
                    databaseManager: databaseManager,
                    format: formatCompatibilityService,
                    allowMigration: false
                )
            } catch BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) {
                throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
            } catch BackupV2RuntimeBuildError.repoIdentityMismatch {
                throw BackupCompatibilityError.repoIdentityMismatch
            } catch BackupV2RuntimeBuildError.requiresForegroundMigration {
                throw BackupCompatibilityError.requiresForegroundMigration
            } catch BackupV2RuntimeBuildError.repoFormatRegression {
                throw BackupCompatibilityError.repoFormatRegression
            } catch BackupV2RuntimeBuildError.damagedV2Repo {
                throw BackupCompatibilityError.damagedV2Repo
            } catch {
                throw error
            }
            do {
                let appliedFingerprints = try await verifier.applyTombstones(
                    month: month,
                    cleanupItems: report.cleanupCandidates,
                    services: v2
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
                await v2.shutdown()
            } catch {
                await v2.shutdown()
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

    private func prepareV2Runtime(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream
    ) async throws -> BackupV2RuntimeServices? {
        // Dedicated metadata connection so metadata writes don't contend with worker uploads.
        let raw = try storageClientFactory.makeClient(profile: profile, password: password)
        try await raw.connect()
        // serialOnly backends must serialize concurrent metadata writes.
        let metadataClient = wrapIfSerial(raw)
        return try await withBackupV2RuntimeBuildErrorMapping(metadataClient: metadataClient) {
            try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: formatCompatibilityService,
                allowMigration: true,
                onMigrationStart: {
                    eventStream.emitLog(String(localized: "backup.repo.migrationStarted"), level: .info)
                },
                onMigrationComplete: { processed in
                    eventStream.emitLog(String.localizedStringWithFormat(String(localized: "backup.repo.migrationCompleted"), processed), level: .info)
                },
                onBootstrap: {
                    eventStream.emitLog(String(localized: "backup.repo.bootstrapped"), level: .info)
                }
            )
        }
    }

}

func withBackupV2RuntimeBuildErrorMapping<T>(
    metadataClient: any RemoteStorageClientProtocol,
    build: () async throws -> T
) async throws -> T {
    do {
        return try await build()
    } catch BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) {
        await metadataClient.disconnectSafely()
        throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
    } catch BackupV2RuntimeBuildError.repoIdentityMismatch {
        await metadataClient.disconnectSafely()
        throw BackupCompatibilityError.repoIdentityMismatch
    } catch BackupV2RuntimeBuildError.requiresForegroundMigration {
        await metadataClient.disconnectSafely()
        throw BackupCompatibilityError.requiresForegroundMigration
    } catch BackupV2RuntimeBuildError.repoFormatRegression {
        await metadataClient.disconnectSafely()
        throw BackupCompatibilityError.repoFormatRegression
    } catch BackupV2RuntimeBuildError.damagedV2Repo {
        await metadataClient.disconnectSafely()
        throw BackupCompatibilityError.damagedV2Repo
    } catch BackupV2RuntimeBuildError.profileMissingID {
        await metadataClient.disconnectSafely()
        // Fail-closed: V1 fallback would write V1 manifests over a V2 repo.
        throw NSError(
            domain: "BackupRunPreparation",
            code: -90,
            userInfo: [NSLocalizedDescriptionKey: "profile missing id — cannot prepare V2 runtime"]
        )
    } catch {
        await metadataClient.disconnectSafely()
        throw error
    }
}
