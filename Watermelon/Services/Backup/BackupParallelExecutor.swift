import Foundation
import Photos
import os.log

private let executorLog = Logger(subsystem: "com.zizicici.watermelon", category: "BackupParallel")

struct BackupParallelExecutor: Sendable {
    private let hashIndexRepository: ContentHashIndexRepository
    private let assetProcessor: AssetProcessor
    private let remoteIndexService: RemoteIndexSyncService

    init(
        hashIndexRepository: ContentHashIndexRepository,
        assetProcessor: AssetProcessor,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.assetProcessor = assetProcessor
        self.remoteIndexService = remoteIndexService
    }

    func execute(
        preparedRun: BackupPreparedRun,
        profile: ServerProfileRecord,
        workerCountOverride: Int?,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        eventStream: BackupEventStream,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) async throws -> BackupExecutionResult {
        guard preparedRun.totalAssetCount > 0 else {
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            eventStream.emit(.finished(result))
            await preparedRun.initialClient.disconnectSafely()
            return result
        }

        let workerCountSource = workerCountOverride == nil ? "protocol-default" : "user-override"
        if let requestedWorkers = workerCountOverride,
           preparedRun.workerCount < requestedWorkers {
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.workerOverrideAdjusted"),
                    requestedWorkers,
                    preparedRun.workerCount,
                    preparedRun.monthPlans.count
                ),
                level: .warning
            )
        }
        eventStream.emitLog(
            String.localizedStringWithFormat(
                String(localized: "backup.parallel.schedulerSummary"),
                preparedRun.monthPlans.count,
                preparedRun.workerCount,
                preparedRun.connectionPoolSize,
                workerCountSource,
                profile.resolvedStorageType.rawValue
            ),
            level: .debug
        )

        let aggregator = ParallelBackupProgressAggregator(total: preparedRun.totalAssetCount)
        let clientPool = StorageClientPool(
            maxConnections: preparedRun.connectionPoolSize,
            makeClient: preparedRun.makeClient
        )
        await clientPool.seedConnectedClient(preparedRun.initialClient)

        do {
            try await withThrowingTaskGroup(of: WorkerRunState.self) { group in
                let monthQueue = MonthWorkQueue(months: preparedRun.monthPlans)

                for workerID in 0 ..< preparedRun.workerCount {
                    group.addTask {
                        try await runParallelMonthWorker(
                            workerID: workerID,
                            monthQueue: monthQueue,
                            profile: profile,
                            iCloudPhotoBackupMode: iCloudPhotoBackupMode,
                            snapshotSeedLookup: preparedRun.snapshotSeedLookup,
                            eventStream: eventStream,
                            aggregator: aggregator,
                            clientPool: clientPool,
                            onMonthUploaded: onMonthUploaded
                        )
                    }
                }

                for try await workerState in group where workerState.paused {
                    await aggregator.markPaused()
                }
            }
        } catch {
            await clientPool.shutdown()
            if profile.isConnectionUnavailableError(error) {
                eventStream.emitLog(String(localized: "backup.parallel.remoteUnavailable"), level: .error)
            } else {
                eventStream.emitErrorLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.parallel.runAborted"),
                        profile.userFacingStorageErrorMessage(error)
                    ),
                    unless: error
                )
            }
            throw error
        }

        await clientPool.shutdown()

        if let finalStageTimingSummary = await aggregator.finalTimingSummary() {
            eventStream.emitLog(finalStageTimingSummary, level: .debug)
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
    }

    private func runParallelMonthWorker(
        workerID: Int,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        snapshotSeedLookup: MonthSeedLookup?,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        clientPool: StorageClientPool,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) async throws -> WorkerRunState {
        var workerState = WorkerRunState()
        let client: any RemoteStorageClientProtocol
        do {
            client = try await clientPool.acquire()
        } catch {
            eventStream.emitErrorLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.clientAcquireFailed"),
                    workerID + 1,
                    profile.userFacingStorageErrorMessage(error)
                ),
                unless: error
            )
            throw error
        }
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
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.loadManifestFailed"),
                            workerID + 1,
                            monthKey.text,
                            profile.userFacingStorageErrorMessage(error)
                        ),
                        level: .error
                    )
                    throw error
                }

                // loadOrCreate may have cleaned manifest rows; sync to snapshotCache so consumers don't see stale state.
                let loadedSnapshot = monthStore.unsortedSnapshot()
                remoteIndexService.replaceCachedMonth(
                    monthKey,
                    resources: loadedSnapshot.resources,
                    assets: loadedSnapshot.assets,
                    links: loadedSnapshot.links
                )

                eventStream.emitLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.parallel.claimedMonth"),
                        workerID + 1,
                        monthKey.text,
                        monthPlan.assetLocalIdentifiers.count,
                        StageTimingWindow.formatBytes(monthPlan.estimatedBytes)
                    ),
                    level: .debug
                )
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

                var skippedMonthShortCircuit = false
                if monthAlreadyFullyBackedUp(
                    monthAssetIDs: monthAssetIDs,
                    monthStore: monthStore
                ) {
                    let progressState = await aggregator.recordMonthSkipped(count: monthAssetIDs.count)
                    eventStream.emit(.progress(BackupProgress(
                        succeeded: progressState.state.succeeded,
                        failed: progressState.state.failed,
                        skipped: progressState.state.skipped,
                        total: progressState.state.total,
                        message: String.localizedStringWithFormat(
                            String(localized: "backup.parallel.monthPreCovered"),
                            workerID + 1,
                            monthKey.text,
                            monthAssetIDs.count
                        ),
                        logMessage: nil,
                        logLevel: .info,
                        itemEvent: nil,
                        transferState: nil
                    )))
                    if let timingSummary = progressState.timingSummary {
                        eventStream.emitLog(timingSummary, level: .debug)
                    }
                    skippedMonthShortCircuit = true
                }

                if !skippedMonthShortCircuit {
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
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.localHashCacheWarning"),
                                    workerID + 1,
                                    monthKey.text,
                                    error.localizedDescription
                                ),
                                level: .warning
                            )
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
                                iCloudPhotoBackupMode: iCloudPhotoBackupMode,
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
                                asset: asset,
                                workerID: workerID + 1,
                                monthText: monthKey.text
                            )
                            if let timingSummary = progressState.timingSummary {
                                eventStream.emitLog(timingSummary, level: .debug)
                            }
                        } catch {
                            if error is CancellationError {
                                workerState.paused = true
                                break
                            }
                            let displayName = BackupAssetResourcePlanner.assetDisplayName(
                                asset: asset,
                                selectedResources: selectedResources
                            )
                            let errorMessage = profile.userFacingStorageErrorMessage(error)
                            if profile.isConnectionUnavailableError(error) {
                                clientReusable = false
                                monthFatalError = error
                                eventStream.emitLog(
                                    String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.connectionLostDuringAsset"),
                                        workerID + 1,
                                        displayName,
                                        monthKey.text,
                                        errorMessage
                                    ),
                                    level: .error
                                )
                                break
                            }

                            print("[BackupUpload] asset processing FAILED: asset=\(displayName), reason=\(errorMessage)")
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.failedAssetLog"),
                                    displayName,
                                    errorMessage
                                ) + Self.contextSuffix(workerID: workerID + 1, monthText: monthKey.text),
                                level: .error
                            )

                            let progressState = await aggregator.recordFailure()
                            emitFailureProgress(
                                eventStream: eventStream,
                                state: progressState.state,
                                asset: asset,
                                displayName: displayName,
                                errorMessage: errorMessage,
                                position: progressState.position,
                                workerID: workerID + 1,
                                monthText: monthKey.text
                            )
                            if let timingSummary = progressState.timingSummary {
                                eventStream.emitLog(timingSummary, level: .debug)
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
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.missingAssets"),
                            workerID + 1,
                            monthKey.text,
                            missingAssetCount
                        ),
                        level: .warning
                    )
                }
                }

                let shouldFinishMonth = !workerState.paused && monthFatalError == nil
                let hadDirtyManifestBeforeFinalize = monthStore.dirty
                let skipFlushDueToUnavailable = monthFatalError.map(profile.isConnectionUnavailableError) ?? false

                if skipFlushDueToUnavailable {
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.skipManifestFlush"),
                            workerID + 1,
                            monthKey.text
                        ),
                        level: .error
                    )
                } else {
                    do {
                        try await monthStore.flushToRemote(ignoreCancellation: workerState.paused)
                        if shouldFinishMonth {
                            eventStream.emit(.monthChanged(MonthChangeEvent(
                                year: monthKey.year,
                                month: monthKey.month,
                                action: .completed
                            )))
                            if let onMonthUploaded {
                                switch await onMonthUploaded(monthKey) {
                                case .success:
                                    break
                                case .failed(let message):
                                    eventStream.emitLog(
                                        String.localizedStringWithFormat(
                                            String(localized: "backup.parallel.finalizationFailed"),
                                            workerID + 1,
                                            monthKey.text,
                                            message
                                        ),
                                        level: .error
                                    )
                                case .cancelled:
                                    workerState.paused = true
                                    eventStream.emitLog(
                                        String.localizedStringWithFormat(
                                            String(localized: "backup.parallel.finalizationCancelled"),
                                            workerID + 1,
                                            monthKey.text
                                        ),
                                        level: .info
                                    )
                                }
                                if workerState.paused {
                                    break
                                }
                            }
                        } else {
                            if monthFatalError != nil {
                                eventStream.emitLog(
                                    String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.monthFatalError"),
                                        workerID + 1,
                                        monthKey.text
                                    ),
                                    level: .error
                                )
                            } else {
                                let pauseLog = hadDirtyManifestBeforeFinalize
                                    ? String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.monthPausedFlushed"),
                                        workerID + 1,
                                        monthKey.text
                                    )
                                    : String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.monthPaused"),
                                        workerID + 1,
                                        monthKey.text
                                    )
                                eventStream.emitLog(pauseLog, level: .info)
                            }
                        }
                    } catch {
                        eventStream.emitErrorLog(
                            String.localizedStringWithFormat(
                                String(localized: "backup.parallel.flushManifestFailed"),
                                workerID + 1,
                                monthKey.text,
                                profile.userFacingStorageErrorMessage(error)
                            ),
                            unless: error
                        )
                        throw error
                    }
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
            if profile.isConnectionUnavailableError(error) {
                clientReusable = false
            }
            await clientPool.release(client, reusable: clientReusable)
            throw error
        }
    }

    private func monthAlreadyFullyBackedUp(
        monthAssetIDs: [String],
        monthStore: MonthManifestStore
    ) -> Bool {
        guard !monthAssetIDs.isEmpty else { return true }
        guard !monthStore.assetsByFingerprint.isEmpty else { return false }

        guard let cachedHashes = try? hashIndexRepository.fetchAssetHashCaches(
            assetIDs: Set(monthAssetIDs)
        ) else { return false }
        guard cachedHashes.count == monthAssetIDs.count else { return false }

        let fetchResult = PHAsset.fetchAssets(
            withLocalIdentifiers: monthAssetIDs,
            options: nil
        )
        guard fetchResult.count == monthAssetIDs.count else { return false }

        for index in 0 ..< fetchResult.count {
            let asset = fetchResult.object(at: index)
            guard let cache = cachedHashes[asset.localIdentifier] else { return false }
            if let modDate = asset.modificationDate, modDate > cache.updatedAt {
                return false
            }
            if !monthStore.containsAssetFingerprint(cache.assetFingerprint) {
                return false
            }
            // Force full processing so AssetProcessor heals incomplete assets.
            if monthStore.isAssetIncomplete(cache.assetFingerprint) {
                executorLog.info("[heal] month \(monthStore.year)-\(monthStore.month) has incomplete asset")
                return false
            }
        }
        return true
    }

    private func emitProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        asset: PHAsset,
        workerID: Int,
        monthText: String
    ) {
        let displayMessage = Self.message(for: result, position: position, total: state.total)
        let logMessage = Self.logMessage(
            for: result,
            position: position,
            total: state.total,
            displayMessage: displayMessage,
            workerID: workerID,
            monthText: monthText
        )
        eventStream.emit(.progress(BackupProgress(
            succeeded: state.succeeded, failed: state.failed,
            skipped: state.skipped, total: state.total,
            message: displayMessage,
            logMessage: logMessage,
            logLevel: Self.logLevel(for: result),
            itemEvent: BackupItemEvent(
                assetLocalIdentifier: asset.localIdentifier,
                assetFingerprint: result.assetFingerprint,
                displayName: result.displayName,
                resourceDate: asset.creationDate,
                status: result.status,
                reason: result.reason,
                updatedAt: Date()
            ),
            transferState: nil
        )))
    }

    private func emitFailureProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        asset: PHAsset,
        displayName: String,
        errorMessage: String,
        position: Int,
        workerID: Int,
        monthText: String
    ) {
        let displayMessage = Self.failureMessage(
            position: position,
            total: state.total,
            displayName: displayName
        )
        eventStream.emit(.progress(BackupProgress(
            succeeded: state.succeeded, failed: state.failed,
            skipped: state.skipped, total: state.total,
            message: displayMessage,
            logMessage: displayMessage + Self.contextSuffix(workerID: workerID, monthText: monthText),
            logLevel: .error,
            itemEvent: BackupItemEvent(
                assetLocalIdentifier: asset.localIdentifier,
                assetFingerprint: nil,
                displayName: displayName,
                resourceDate: asset.creationDate,
                status: .failed,
                reason: errorMessage,
                updatedAt: Date()
            ),
            transferState: nil
        )))
    }

    private static func message(
        for result: AssetProcessResult,
        position: Int,
        total: Int
    ) -> String {
        switch result.status {
        case .success:
            return String.localizedStringWithFormat(
                String(localized: "backup.progress.assetDone"),
                position,
                max(total, 1),
                result.displayName
            )
        case .failed:
            return String.localizedStringWithFormat(
                String(localized: "backup.progress.assetFailed"),
                position,
                max(total, 1),
                result.displayName
            )
        case .skipped:
            if let reason = result.reason {
                return String.localizedStringWithFormat(
                    String(localized: "backup.progress.assetSkippedReason"),
                    position,
                    max(total, 1),
                    result.displayName,
                    localizedSkipReason(reason)
                )
            }
            return String.localizedStringWithFormat(
                String(localized: "backup.progress.assetSkipped"),
                position,
                max(total, 1),
                result.displayName
            )
        }
    }

    private static func logMessage(
        for result: AssetProcessResult,
        position: Int,
        total: Int,
        displayMessage: String,
        workerID: Int,
        monthText: String
    ) -> String {
        let suffix = contextSuffix(workerID: workerID, monthText: monthText)
        if result.status == .failed, let reason = result.reason {
            return String.localizedStringWithFormat(
                String(localized: "backup.progress.assetFailedReason"),
                position,
                max(total, 1),
                result.displayName,
                reason
            ) + suffix
        }
        return displayMessage + suffix
    }

    private static func failureMessage(
        position: Int,
        total: Int,
        displayName: String
    ) -> String {
        String.localizedStringWithFormat(
            String(localized: "backup.progress.assetFailed"),
            position,
            max(total, 1),
            displayName
        )
    }

    private static func contextSuffix(workerID: Int, monthText: String) -> String {
        String.localizedStringWithFormat(
            String(localized: "backup.progress.assetContextSuffix"),
            workerID,
            monthText
        )
    }

    private static func localizedSkipReason(_ reason: String) -> String {
        switch reason {
        case "resources_reused", "resources_reused_cached", "asset_exists_cached":
            return String(localized: "backup.reason.resourcesReused")
        case "icloud_photo_backup_disabled":
            return String(localized: "backup.reason.icloudDisabled")
        default:
            return reason
        }
    }

    private static func logLevel(for result: AssetProcessResult) -> ExecutionLogLevel {
        switch result.status {
        case .success:
            return .debug
        case .failed:
            return .error
        case .skipped:
            if result.reason == "icloud_photo_backup_disabled" {
                return .warning
            }
            return .debug
        }
    }

}
