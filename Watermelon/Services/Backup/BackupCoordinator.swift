import Foundation
import Photos

final class BackupCoordinator: Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let remoteIndexService: RemoteIndexSyncService
    private let assetProcessor: AssetProcessor

    init(
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService? = nil,
        assetProcessor: AssetProcessor? = nil
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService ?? RemoteIndexSyncService()
        self.assetProcessor = assetProcessor ?? AssetProcessor(
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            remoteIndexService: self.remoteIndexService
        )
    }

    func runBackup(request: BackupRunRequest, eventStream: BackupEventStream) async throws -> BackupExecutionResult {
        let profile = request.profile
        let password = request.password
        let onlyAssetLocalIdentifiers = request.onlyAssetLocalIdentifiers

        try await ensurePhotoAuthorization()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        var initialClientManagedByPool = false

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
                if profile.isExternalStorageUnavailableError(error) {
                    throw error
                }
                eventStream.emit(.log("Remote index scan warning: \(error.localizedDescription)"))
            }

            let retryMode = onlyAssetLocalIdentifiers != nil
            let assetsResult: PHFetchResult<PHAsset>? = retryMode
                ? nil
                : photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)

            var retryAssets: [PHAsset] = []
            if let retryTargets = onlyAssetLocalIdentifiers {
                let fetched = PHAsset.fetchAssets(withLocalIdentifiers: Array(retryTargets), options: nil)
                retryAssets.reserveCapacity(fetched.count)
                for index in 0 ..< fetched.count {
                    retryAssets.append(fetched.object(at: index))
                }
                retryAssets.sort { lhs, rhs in
                    (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
                }
            }

            var state = BackupRunState(total: retryMode ? retryAssets.count : (assetsResult?.count ?? 0))

            if retryMode {
                let requested = onlyAssetLocalIdentifiers?.count ?? 0
                let missing = max(requested - retryAssets.count, 0)
                eventStream.emit(.log(
                    "Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing)."
                ))
            } else {
                eventStream.emit(.log("Start backup by asset (oldest month first)."))
            }

            eventStream.emit(.started(totalAssets: state.total))

            if state.total == 0 {
                let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
                eventStream.emit(.finished(result))
                await disconnectClient(client)
                return result
            }

            let monthAssetIDsByMonth: [MonthKey: [String]]
            if retryMode {
                monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: retryAssets)
            } else if let assetsResult {
                monthAssetIDsByMonth = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: assetsResult)
            } else {
                monthAssetIDsByMonth = [:]
            }

            state.total = monthAssetIDsByMonth.values.reduce(0) { partial, ids in
                partial + ids.count
            }
            if state.total == 0 {
                let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
                eventStream.emit(.finished(result))
                await disconnectClient(client)
                return result
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
            let monthQueue = MonthWorkQueue(months: monthPlans)
            let workerCountSource = request.workerCountOverride == nil ? "protocol-default" : "user-override"
            if let requestedWorkers = request.workerCountOverride,
               workerCount < requestedWorkers {
                eventStream.emit(.log(
                    "Worker override adjusted: requested=\(requestedWorkers), effective=\(workerCount), reason=month-count(\(monthPlans.count))."
                ))
            }
            eventStream.emit(.log(
                "Parallel month scheduler: month(s)=\(monthPlans.count), worker(s)=\(workerCount), connectionPool=\(connectionPoolSize), strategy=dynamic-pull, source=\(workerCountSource), storage=\(profile.resolvedStorageType.rawValue)."
            ))

            let aggregator = ParallelBackupProgressAggregator(total: state.total)
            let clientPool = StorageClientPool(
                maxConnections: connectionPoolSize,
                makeClient: { [self, profile, password] in
                    try makeStorageClient(profile: profile, password: password)
                }
            )
            await clientPool.seedConnectedClient(client)
            initialClientManagedByPool = true

            do {
                try await withThrowingTaskGroup(of: WorkerRunState.self) { group in
                    for workerID in 0 ..< workerCount {
                        group.addTask { [self] in
                            try await runParallelMonthWorker(
                                workerID: workerID,
                                monthQueue: monthQueue,
                                profile: profile,
                                snapshotSeedLookup: snapshotSeedLookup,
                                eventStream: eventStream,
                                aggregator: aggregator,
                                clientPool: clientPool
                            )
                        }
                    }

                    for try await workerState in group where workerState.paused {
                        await aggregator.markPaused()
                    }
                }
            } catch {
                await clientPool.shutdown()
                if profile.isExternalStorageUnavailableError(error) {
                    eventStream.emit(.log("External storage unavailable. Stop backup immediately."))
                    throw error
                }
                throw error
            }

            await clientPool.shutdown()

            if let finalStageTimingSummary = await aggregator.finalTimingSummary() {
                eventStream.emit(.log(finalStageTimingSummary))
            }
            state = await aggregator.snapshot()

            let result = BackupExecutionResult(
                total: state.total,
                succeeded: state.succeeded,
                failed: state.failed,
                skipped: state.skipped,
                paused: state.paused
            )
            eventStream.emit(.finished(result))
            return result
        } catch {
            if !initialClientManagedByPool {
                await disconnectClient(client)
            }
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
            await disconnectClient(client)
            return snapshot
        } catch {
            await disconnectClient(client)
            throw error
        }
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        remoteIndexService.remoteMonthSummaries()
    }

    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        remoteIndexService.currentState(since: revision)
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

    private func emitProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        asset: PHAsset
    ) {
        let message = Self.message(for: result, position: position, total: state.total)
        let event = BackupItemEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: result.assetFingerprint,
            displayName: result.displayName,
            resourceDate: asset.creationDate,
            status: result.status,
            reason: result.reason,
            resourceSummary: result.resourceSummary,
            updatedAt: Date()
        )
        let progress = BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: event,
            transferState: nil
        )
        eventStream.emit(.progress(progress))
    }

    private func emitFailureProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        asset: PHAsset,
        displayName: String,
        errorMessage: String,
        position: Int
    ) {
        let message = "[\(position)/\(state.total)] Failed asset \(displayName)"
        let event = BackupItemEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: nil,
            displayName: displayName,
            resourceDate: asset.creationDate,
            status: .failed,
            reason: errorMessage,
            resourceSummary: "资源处理失败",
            updatedAt: Date()
        )
        let progress = BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: event,
            transferState: nil
        )
        eventStream.emit(.progress(progress))
    }

    private static func message(for result: AssetProcessResult, position: Int, total: Int) -> String {
        let prefix = "[\(position)/\(max(total, 1))]"
        switch result.status {
        case .success:
            return "\(prefix) Asset done \(result.displayName)"
        case .failed:
            return "\(prefix) Asset failed \(result.displayName)"
        case .skipped:
            if let reason = result.reason {
                return "\(prefix) Asset skipped \(result.displayName) (\(reason))"
            }
            return "\(prefix) Asset skipped \(result.displayName)"
        }
    }

    private func disconnectClient(_ client: any RemoteStorageClientProtocol) async {
        if Task.isCancelled {
            let cleanupTask = Task.detached(priority: .utility) {
                await client.disconnect()
            }
            _ = await cleanupTask.value
            return
        }
        await client.disconnect()
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

    private func runParallelMonthWorker(
        workerID: Int,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        snapshotSeedLookup: MonthSeedLookup?,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        clientPool: StorageClientPool
    ) async throws -> WorkerRunState {
        var workerState = WorkerRunState()
        let client = try await clientPool.acquire()
        var clientReusable = true
        do {
            while let monthPlan = await monthQueue.next() {
                if Task.isCancelled {
                    workerState.paused = true
                    break
                }

                let monthKey = monthPlan.month
                let monthStore: MonthManifestStore
                do {
                    monthStore = try await MonthManifestStore.loadOrCreate(
                        client: client,
                        basePath: profile.basePath,
                        year: monthKey.year,
                        month: monthKey.month,
                        seed: snapshotSeedLookup?.seed(for: monthKey)
                    )
                } catch {
                    if error is CancellationError {
                        workerState.paused = true
                        break
                    }
                    throw error
                }

                eventStream.emit(.log(
                    "Worker\(workerID + 1) claimed month \(monthKey.text), assets=\(monthPlan.assetLocalIdentifiers.count), est=\(StageTimingWindow.formatBytes(monthPlan.estimatedBytes))."
                ))
                eventStream.emit(.monthChanged(MonthChangeEvent(
                    year: monthKey.year,
                    month: monthKey.month,
                    action: .started
                )))

                var monthFatalError: Error?
                let monthAssetIDs = monthPlan.assetLocalIdentifiers
                let fetchBatchSize = 500
                var missingAssetCount = 0
                var hasLoggedLocalHashCacheWarning = false

                for batchStart in stride(from: 0, to: monthAssetIDs.count, by: fetchBatchSize) {
                    let batchEnd = min(batchStart + fetchBatchSize, monthAssetIDs.count)
                    let batchAssetIDs = Array(monthAssetIDs[batchStart ..< batchEnd])
                    guard !batchAssetIDs.isEmpty else { continue }

                    var batchLocalHashCacheByAssetID: [String: LocalAssetHashCache]
                    do {
                        batchLocalHashCacheByAssetID = try hashIndexRepository.fetchAssetHashCaches(
                            assetIDs: Set(batchAssetIDs)
                        )
                    } catch {
                        if !hasLoggedLocalHashCacheWarning {
                            eventStream.emit(.log(
                                "Worker\(workerID + 1) local hash cache warning for month \(monthKey.text): \(error.localizedDescription)"
                            ))
                            hasLoggedLocalHashCacheWarning = true
                        }
                        batchLocalHashCacheByAssetID = [:]
                    }

                    let batchAssetsResult = PHAsset.fetchAssets(
                        withLocalIdentifiers: batchAssetIDs,
                        options: nil
                    )
                    var batchAssetsByLocalIdentifier: [String: PHAsset] = [:]
                    batchAssetsByLocalIdentifier.reserveCapacity(batchAssetsResult.count)
                    for index in 0 ..< batchAssetsResult.count {
                        let asset = batchAssetsResult.object(at: index)
                        batchAssetsByLocalIdentifier[asset.localIdentifier] = asset
                    }
                    missingAssetCount += max(batchAssetIDs.count - batchAssetsByLocalIdentifier.count, 0)

                    for assetID in batchAssetIDs {
                        if Task.isCancelled {
                            workerState.paused = true
                            break
                        }

                        guard let asset = batchAssetsByLocalIdentifier.removeValue(forKey: assetID) else {
                            await aggregator.reduceTotalForEmptyAsset()
                            continue
                        }

                        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                            from: PHAssetResource.assetResources(for: asset)
                        )
                        if selectedResources.isEmpty {
                            await aggregator.reduceTotalForEmptyAsset()
                            continue
                        }

                        let dispatch = await aggregator.allocateDispatchSlot()
                        let cachedLocalHash = batchLocalHashCacheByAssetID.removeValue(forKey: assetID)

                        do {
                            let context = AssetProcessContext(
                                workerID: workerID + 1,
                                asset: asset,
                                selectedResources: selectedResources,
                                cachedLocalHash: cachedLocalHash,
                                monthStore: monthStore,
                                profile: profile,
                                assetPosition: dispatch.position,
                                totalAssets: dispatch.total
                            )

                            let result = try await assetProcessor.process(
                                context: context,
                                client: client,
                                eventStream: eventStream,
                                cancellationController: nil
                            )
                            let progressState = await aggregator.record(result: result)

                            emitProgress(
                                eventStream: eventStream,
                                state: progressState.state,
                                result: result,
                                position: progressState.position,
                                asset: asset
                            )
                            if let timingSummary = progressState.timingSummary {
                                eventStream.emit(.log(timingSummary))
                            }
                        } catch {
                            if error is CancellationError {
                                workerState.paused = true
                                break
                            }
                            if profile.isExternalStorageUnavailableError(error) {
                                clientReusable = false
                                monthFatalError = error
                                break
                            }

                            let displayName = BackupAssetResourcePlanner.assetDisplayName(
                                asset: asset,
                                selectedResources: selectedResources
                            )
                            let errorMessage = profile.userFacingStorageErrorMessage(error)
                            eventStream.emit(.log("Failed asset: \(displayName) - \(errorMessage)"))

                            let progressState = await aggregator.recordFailure()
                            emitFailureProgress(
                                eventStream: eventStream,
                                state: progressState.state,
                                asset: asset,
                                displayName: displayName,
                                errorMessage: errorMessage,
                                position: progressState.position
                            )
                            if let timingSummary = progressState.timingSummary {
                                eventStream.emit(.log(timingSummary))
                            }
                        }
                    }

                    if workerState.paused || monthFatalError != nil {
                        break
                    }

                    // Release batch containers eagerly before next batch.
                    batchAssetsByLocalIdentifier.removeAll(keepingCapacity: false)
                    batchLocalHashCacheByAssetID.removeAll(keepingCapacity: false)
                }

                if missingAssetCount > 0 {
                    eventStream.emit(.log(
                        "Worker\(workerID + 1) month \(monthKey.text): \(missingAssetCount) asset(s) missing in photo library snapshot."
                    ))
                }

                let shouldForceFlush = workerState.paused && monthStore.dirty
                do {
                    _ = try await monthStore.flushToRemote(ignoreCancellation: shouldForceFlush)
                    if shouldForceFlush {
                        eventStream.emit(.monthChanged(MonthChangeEvent(
                            year: monthKey.year,
                            month: monthKey.month,
                            action: .flushed
                        )))
                        eventStream.emit(.log(
                            "Worker\(workerID + 1): cancellation requested. Month \(monthKey.text) manifest flushed before exit."
                        ))
                    } else {
                        eventStream.emit(.monthChanged(MonthChangeEvent(
                            year: monthKey.year,
                            month: monthKey.month,
                            action: .completed
                        )))
                    }
                } catch {
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: monthKey.year,
                        month: monthKey.month,
                        action: .flushFailed(error.localizedDescription)
                    )))
                    throw error
                }

                if let monthFatalError {
                    throw monthFatalError
                }

                if workerState.paused {
                    break
                }
            }

            await clientPool.release(client, reusable: clientReusable)
            return workerState
        } catch {
            if profile.isExternalStorageUnavailableError(error) {
                clientReusable = false
            }
            await clientPool.release(client, reusable: clientReusable)
            throw error
        }
    }

    private static let monthSeedLookupEntryThreshold = 120_000

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
