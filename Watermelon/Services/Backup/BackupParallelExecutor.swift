import Foundation
import Photos

struct BackupParallelExecutor: Sendable {
    private let hashIndexRepository: ContentHashIndexRepository
    private let assetProcessor: AssetProcessor

    init(
        hashIndexRepository: ContentHashIndexRepository,
        assetProcessor: AssetProcessor
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.assetProcessor = assetProcessor
    }

    func execute(
        preparedRun: BackupPreparedRun,
        profile: ServerProfileRecord,
        workerCountOverride: Int?,
        eventStream: BackupEventStream
    ) async throws -> BackupExecutionResult {
        guard preparedRun.totalAssetCount > 0 else {
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            eventStream.emit(.finished(result))
            await disconnectClient(preparedRun.initialClient)
            return result
        }

        let workerCountSource = workerCountOverride == nil ? "protocol-default" : "user-override"
        if let requestedWorkers = workerCountOverride,
           preparedRun.workerCount < requestedWorkers {
            eventStream.emit(.log(
                "Worker override adjusted: requested=\(requestedWorkers), effective=\(preparedRun.workerCount), reason=month-count(\(preparedRun.monthPlans.count))."
            ))
        }
        eventStream.emit(.log(
            "Parallel month scheduler: month(s)=\(preparedRun.monthPlans.count), worker(s)=\(preparedRun.workerCount), connectionPool=\(preparedRun.connectionPoolSize), strategy=dynamic-pull, source=\(workerCountSource), storage=\(profile.resolvedStorageType.rawValue)."
        ))

        let aggregator = ParallelBackupProgressAggregator(total: preparedRun.totalAssetCount)
        let clientPool = StorageClientPool(
            maxConnections: preparedRun.connectionPoolSize,
            makeClient: preparedRun.makeClient
        )
        await clientPool.seedConnectedClient(preparedRun.initialClient)

        do {
            do {
                try await withThrowingTaskGroup(of: WorkerRunState.self) { group in
                    let monthQueue = MonthWorkQueue(months: preparedRun.monthPlans)

                    for workerID in 0 ..< preparedRun.workerCount {
                        group.addTask {
                            try await runParallelMonthWorker(
                                workerID: workerID,
                                monthQueue: monthQueue,
                                profile: profile,
                                snapshotSeedLookup: preparedRun.snapshotSeedLookup,
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
                }
                throw error
            }

            await clientPool.shutdown()

            if let finalStageTimingSummary = await aggregator.finalTimingSummary() {
                eventStream.emit(.log(finalStageTimingSummary))
            }
            let state = await aggregator.snapshot()
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
            throw error
        }
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

                    batchAssetsByLocalIdentifier.removeAll(keepingCapacity: false)
                    batchLocalHashCacheByAssetID.removeAll(keepingCapacity: false)
                }

                if missingAssetCount > 0 {
                    eventStream.emit(.log(
                        "Worker\(workerID + 1) month \(monthKey.text): \(missingAssetCount) asset(s) missing in photo library snapshot."
                    ))
                }

                let shouldFinishMonth = !workerState.paused
                let hadDirtyManifestBeforeFinalize = monthStore.dirty
                do {
                    try await monthStore.flushToRemote(ignoreCancellation: workerState.paused)
                    if shouldFinishMonth {
                        eventStream.emit(.monthChanged(MonthChangeEvent(
                            year: monthKey.year,
                            month: monthKey.month,
                            action: .completed
                        )))
                    } else {
                        let pauseLog = hadDirtyManifestBeforeFinalize
                            ? "Worker\(workerID + 1): cancellation requested. Month \(monthKey.text) manifest flushed before exit."
                            : "Worker\(workerID + 1): cancellation requested. Month \(monthKey.text) paused before completion."
                        eventStream.emit(.log(pauseLog))
                    }
                } catch {
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
}
