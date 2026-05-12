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
    /// True only on a clean syncIndex; gates the committed-fp fast path so a stale cache can't mask a month as done.
    let fastPathFresh: Bool
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

            var v2ServicesForCleanup: BackupV2RuntimeServices?
            do {
                // basePath creation moved into BackupV2RuntimeBuilder so background runner
                // benefits too; explicit call here is now redundant.
                let v2Services = try await prepareV2Runtime(client: client, profile: profile, password: password, eventStream: eventStream)
                v2ServicesForCleanup = v2Services

                var fastPathFresh = false
                do {
                    // Peek so a syncIndex throw doesn't waste the cold-start materialize.
                    // Consume only after success.
                    let preMaterialized = await v2Services?.initialMaterializeOutput.peek()
                    let digest = try await remoteIndexService.syncIndex(
                        client: client,
                        profile: profile,
                        eventStream: eventStream,
                        preMaterialized: preMaterialized,
                        expectV2: v2Services != nil
                    )
                    _ = await v2Services?.initialMaterializeOutput.consume()
                    // Stale overlay can let a since-deleted asset look healthy; require a clean refresh to skip loadOrCreate + LIST.
                    fastPathFresh = await remoteIndexService.lastSyncOverlayFresh()
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
                    // damagedV2Repo / repoFormatRegression / repoIdentityMismatch
                    // are fatal — continuing would write into a half-initialized
                    // repo. BackupV2RuntimeBuilder would re-throw later anyway,
                    // but only after we've spun up worker pools.
                    if error is BackupCompatibilityError {
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
                    v2Services: v2Services,
                    fastPathFresh: fastPathFresh
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
        let digest = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            eventStream: eventStream,
            onSyncProgress: onSyncProgress
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
            // Profile + password let verify build a V2 runtime (dedicated metadata client + tombstones).
            try await self.verifyMonth(client: client, basePath: profile.basePath, month: month, profile: profile, password: password)
        }
    }

    func verifyMonth(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        profile: ServerProfileRecord? = nil,
        password: String? = nil
    ) async throws {
        // Inspect drives routing AND format gating — separate version.json probe
        // would re-open the damagedV2-vs-V1 TOCTOU window.
        let inspection: RemoteFormatInspection
        if let profile {
            inspection = try await RemoteFormatCompatibilityService()
                .inspectRemoteFormat(client: client, profile: profile)
        } else {
            // Profile-less paths can't run damagedV2 detection.
            let versionPath = RepoLayout.versionFilePath(base: basePath)
            inspection = try await client.metadata(path: versionPath) != nil
                ? .v2(formatVersion: RepoLayout.formatVersion)
                : .v1
        }
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2:
            _ = try await verifyMonthV2(client: client, basePath: basePath, month: month, profile: profile, password: password)
        case .v1:
            try await remoteIndexService.verifyMonth(
                client: client,
                basePath: basePath,
                month: month
            )
        case .fresh:
            break
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
        // RepoID from remote up-front lets the verifier filter foreign-id commits;
        // absent on a V2 repo is broken identity state — verify must not run.
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
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
        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: expectedRepoID)
        let report = try await verifier.verify(month: month)
        if !report.cleanupCandidates.isEmpty, let profile, let password {
            // Verify cleanup is sequential; reuse caller's client (no maintenance tasks).
            _ = password
            let metadataClient = wrapIfSerial(client)
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
            } catch {
                throw error
            }
            do {
                let appliedFingerprints = try await verifier.applyTombstones(
                    month: month,
                    cleanupItems: report.cleanupCandidates,
                    services: v2
                )
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
                _ = try? await remoteIndexService.syncIndex(client: client, profile: profile)
                await v2.shutdown()
            } catch {
                await v2.shutdown()
                throw error
            }
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
        // Dedicated metadata connection so commits/snapshots/liveness don't contend with worker uploads.
        let raw = try storageClientFactory.makeClient(profile: profile, password: password)
        try await raw.connect()
        // serialOnly backends (SMB / SFTP) — wrap so concurrent workers writing
        // commits/snapshots can't violate the single-connection contract.
        let metadataClient = wrapIfSerial(raw)
        do {
            return try await BackupV2RuntimeBuilder.build(
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
        } catch BackupV2RuntimeBuildError.profileMissingID {
            await metadataClient.disconnectSafely()
            // Fail-closed: profile with no id means we can't bind to a repo state row.
            // Fallback to V1 path would write V1 manifests on a V2 repo (dual-format
            // corruption); surface as a generic error instead.
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

}
