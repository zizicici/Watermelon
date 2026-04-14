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
}

struct BackupRunPreparationService: Sendable {
    private static let monthSeedLookupEntryThreshold = 120_000

    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let remoteIndexService: RemoteIndexSyncService

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
    }

    func prepareRun(
        request: BackupRunRequest,
        eventStream: BackupEventStream
    ) async throws -> BackupPreparedRun {
        let profile = request.profile
        let password = request.password
        let onlyAssetLocalIdentifiers = request.onlyAssetLocalIdentifiers

        try await ensurePhotoAuthorization()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()

        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
            var snapshotSeedLookup: MonthSeedLookup?

            do {
                let snapshot = try await remoteIndexService.syncIndex(
                    client: client,
                    profile: profile,
                    eventStream: eventStream
                )
                snapshotSeedLookup = makeMonthSeedLookup(from: snapshot, eventStream: eventStream)
                eventStream.emit(.log(
                    "Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
                ))
            } catch {
                if profile.isConnectionUnavailableError(error) {
                    throw error
                }
                eventStream.emit(.log("Remote index scan warning: \(error.localizedDescription)"))
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
                eventStream.emit(.log(
                    "Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing)."
                ))
            } else {
                eventStream.emit(.log("Start backup by asset (oldest month first)."))
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
                }
            )
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream? = nil,
        onMonthSynced: (@Sendable () -> Void)? = nil
    ) async throws -> RemoteLibrarySnapshot {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
            let snapshot = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onMonthSynced: onMonthSynced
            )
            eventStream?.emit(.log(
                "Remote index reloaded. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
            ))
            await client.disconnectSafely()
            return snapshot
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
                eventStream.emit(.log(
                    "Local hash size estimate warning for month \(month.text): \(error.localizedDescription)"
                ))
                estimatedBytesByMonth[month] = 0
            }
        }

        return estimatedBytesByMonth
    }

    private func makeMonthSeedLookup(
        from snapshot: RemoteLibrarySnapshot,
        eventStream: BackupEventStream
    ) -> MonthSeedLookup? {
        let totalEntries = snapshot.totalResourceCount + snapshot.totalCount + snapshot.assetResourceLinks.count
        if totalEntries > Self.monthSeedLookupEntryThreshold {
            eventStream.emit(.log(
                "Remote snapshot is large (\(totalEntries) entries). Disable in-memory month seeding and load manifests on demand."
            ))
            return nil
        }

        let lookup = MonthSeedLookup(snapshot: snapshot)
        return lookup.isEmpty ? nil : lookup
    }
}
