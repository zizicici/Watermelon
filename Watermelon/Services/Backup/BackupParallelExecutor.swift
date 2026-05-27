import Foundation
import Photos
import os.log

private let executorLog = Logger(subsystem: "com.zizicici.watermelon", category: "BackupParallel")

private actor BackupThermalThrottle {
    static let shared = BackupThermalThrottle()
    private var lastObservedState: ProcessInfo.ThermalState = .nominal

    func waitIfNeeded() async {
        let state = ProcessInfo.processInfo.thermalState
        if state != lastObservedState {
            executorLog.info("thermal state \(Self.name(self.lastObservedState)) -> \(Self.name(state))")
            lastObservedState = state
        }
        switch state {
        case .nominal, .fair, .serious:
            return
        case .critical:
            while !Task.isCancelled, ProcessInfo.processInfo.thermalState == .critical {
                do {
                    try await Task.sleep(for: .seconds(10))
                } catch {
                    return
                }
            }
        @unknown default:
            return
        }
    }

    private static func name(_ state: ProcessInfo.ThermalState) -> String {
        switch state {
        case .nominal: return "nominal"
        case .fair: return "fair"
        case .serious: return "serious"
        case .critical: return "critical"
        @unknown default: return "unknown"
        }
    }
}

struct BackupParallelExecutor: Sendable {
    static let flushInterval = BackupV2Constants.batchFlushInterval

    static func foregroundFinalFlushIgnoresCancellation(paused: Bool, hasV2Services: Bool) -> Bool {
        // U01: V2 batch commits can hold up to BackupV2Constants.batchFlushInterval row-writes in
        // memory at pause time. Respecting cancellation here would drop the partial batch and
        // orphan its uploaded resources. Pause-final ignores cancellation for both V1 and V2.
        _ = hasV2Services
        return paused
    }

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
            await preparedRun.v2Services?.shutdown()
            await preparedRun.initialClient.disconnectSafely()
            return result
        }

        // AssetProcessor is held by DependencyContainer and shared across runs; queued intents from
        // a prior aborted run would otherwise drain against this run's commit deltas.
        await assetProcessor.clearAllPendingHashIndexIntents()

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
                    group.addTask { [v2Services = preparedRun.v2Services] in
                        try await runParallelMonthWorker(
                            workerID: workerID,
                            monthQueue: monthQueue,
                            profile: profile,
                            iCloudPhotoBackupMode: iCloudPhotoBackupMode,
                            eventStream: eventStream,
                            aggregator: aggregator,
                            clientPool: clientPool,
                            onMonthUploaded: onMonthUploaded,
                            v2Services: v2Services
                        )
                    }
                }

                for try await workerState in group where workerState.paused {
                    await aggregator.markPaused()
                }
            }
        } catch {
            await preparedRun.v2Services?.shutdown()
            await clientPool.shutdown()
            if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
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

        await preparedRun.v2Services?.shutdown()
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
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        clientPool: StorageClientPool,
        onMonthUploaded: BackupMonthFinalizer? = nil,
        v2Services: BackupV2RuntimeServices? = nil
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
                let monthAssetIDs = monthPlan.assetLocalIdentifiers

                let monthStore: any BackupMonthStore
                do {
                    if let v2Services {
                        monthStore = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
                            client: client,
                            basePath: profile.basePath,
                            month: monthKey,
                            v2Services: v2Services,
                            remoteIndexService: remoteIndexService,
                            stepLogger: { message in
                                eventStream.emitLog(message, level: .error)
                            }
                        )
                    } else {
                        monthStore = try await MonthManifestStore.loadOrCreate(
                            client: client,
                            basePath: profile.basePath,
                            year: monthKey.year,
                            month: monthKey.month,
                            stepLogger: { message in
                                eventStream.emitLog(message, level: .error)
                            }
                        )
                        // loadOrCreate may have cleaned manifest rows; sync to snapshotCache so consumers don't see stale state.
                        remoteIndexService.publishMonthSnapshot(of: monthStore, for: monthKey)
                    }
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
                let fetchBatchSize = 500
                var missingAssetCount = 0
                var hasLoggedLocalHashCacheWarning = false
                var flushCounter = AssetBatchFlushCounter(threshold: Self.flushInterval)
                var shouldFlushAfterDataConnectionLoss = false

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

                    var batchLocalHashCacheByAssetID: [PhotoKitLocalIdentifier: LocalAssetHashCache]
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
                        withLocalIdentifiers: batchAssetIDs.rawValues,
                        options: nil
                    )
                    var batchAssetsByLocalIdentifier: [PhotoKitLocalIdentifier: PHAsset] = [:]
                    batchAssetsByLocalIdentifier.reserveCapacity(batchAssetsResult.count)
                    for index in 0 ..< batchAssetsResult.count {
                        let asset = batchAssetsResult.object(at: index)
                        batchAssetsByLocalIdentifier[PhotoKitLocalIdentifier(asset)] = asset
                    }
                    missingAssetCount += max(batchAssetIDs.count - batchAssetsByLocalIdentifier.count, 0)

                    for assetID in batchAssetIDs {
                        if Task.isCancelled {
                            workerState.paused = true
                            break
                        }

                        await BackupThermalThrottle.shared.waitIfNeeded()
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

                            // U01 provisional tracking — V2 only AND only for results that
                            // actually added a pending V2 row (`wroteProvisionalV2Row == true`).
                            // The durable cached-skip short-circuit (`asset_exists_cached`) writes
                            // its hash-index row inline and never calls `finalizeRowWritingAsset`,
                            // so its row is already durable and must NOT enter the rollback buffer
                            // — otherwise a later batch failure would revert an unrelated cached
                            // skip. V1 is also excluded (eager commit inside upsertAsset).
                            if v2Services != nil,
                               result.wroteProvisionalV2Row,
                               let fingerprint = result.assetFingerprint {
                                switch result.status {
                                case .success:
                                    await aggregator.recordProvisional(
                                        month: monthKey,
                                        fingerprint: fingerprint,
                                        assetLocalIdentifier: PhotoKitLocalIdentifier(asset),
                                        status: .success
                                    )
                                case .skipped:
                                    await aggregator.recordProvisional(
                                        month: monthKey,
                                        fingerprint: fingerprint,
                                        assetLocalIdentifier: PhotoKitLocalIdentifier(asset),
                                        status: .skipped
                                    )
                                case .failed:
                                    break
                                }
                            }

                            // Cached-reuse `.skipped` also writes asset rows; only `.failed`
                            // skips the batch counter.
                            if result.status != .failed {
                                if flushCounter.recordSuccessAndCheckThreshold() {
                                    var intervalOutcome: V2MonthFlushOutcome?
                                    var shouldBreakAssetLoop = false
                                    do {
                                        intervalOutcome = try await Self.flushMonthStorePublishingDefensiveCommits(
                                            monthStore: monthStore,
                                            month: monthKey,
                                            remoteIndexService: remoteIndexService,
                                            ignoreCancellation: false
                                        )
                                    } catch {
                                        switch BackupFlushFailureClassification.classify(error, on: profile).foregroundIntervalAction {
                                        case .continueAssetLoopAndResetCounter:
                                            // No commit landed; pending stays in memory for the
                                            // next attempt — provisional buffer also carries.
                                            flushCounter.reset()
                                            continue
                                        case .pauseAndBreakAssetLoop:
                                            // U01 R03: do NOT roll back provisional/intents here.
                                            // The paused end-of-month flush below runs with
                                            // ignoreCancellation=true and can still commit these
                                            // same pending V2 ops; pre-emptive rollback would
                                            // discard intents/counters for assets that ultimately
                                            // become durable. Reconciliation happens after EOM
                                            // (applyDurableBatchSideEffects + hasUncommittedV2Ops
                                            // branch, EOM catch, or EOM-skip branch below).
                                            workerState.paused = true
                                            shouldBreakAssetLoop = true
                                        case .abortMonthBreakAssetLoop:
                                            // U01 R03: see above — EOM-skip branch below rolls
                                            // back any remaining buffer/intents for the
                                            // connection-unavailable abort path that suppresses
                                            // the final flush.
                                            clientReusable = false
                                            monthFatalError = error
                                            eventStream.emitLog(
                                                String.localizedStringWithFormat(
                                                    String(localized: "backup.parallel.flushManifestFailed"),
                                                    workerID + 1,
                                                    monthKey.text,
                                                    profile.userFacingStorageErrorMessage(error)
                                                ),
                                                level: .error
                                            )
                                            shouldBreakAssetLoop = true
                                        case .logWarningAndContinue:
                                            // U01: classifier no longer returns this for V2; kept
                                            // for compiler-required exhaustive switch over the
                                            // unchanged enum.
                                            eventStream.emitLog(
                                                String.localizedStringWithFormat(
                                                    String(localized: "backup.parallel.flushManifestFailed"),
                                                    workerID + 1,
                                                    monthKey.text,
                                                    profile.userFacingStorageErrorMessage(error)
                                                ),
                                                level: .warning
                                            )
                                        }
                                    }
                                    if let outcome = intervalOutcome {
                                        await Self.applyDurableBatchSideEffects(
                                            aggregator: aggregator,
                                            assetProcessor: assetProcessor,
                                            month: monthKey,
                                            outcome: outcome,
                                            eventStream: eventStream,
                                            profile: profile,
                                            workerID: workerID + 1
                                        )
                                    }
                                    // U01 R02: partial multi-chunk failure at interval (chunk N+1
                                    // still pending). Force a hard stop so the asset loop cannot
                                    // accumulate past the 200-op boundary. R03: do NOT roll back
                                    // here — the paused EOM flush below can still commit chunk N+1
                                    // (or the abort-path EOM-skip branch will reconcile it).
                                    if let outcome = intervalOutcome,
                                       monthStore.hasUncommittedV2Ops {
                                        let displayError = outcome.displayError
                                        let underlyingMessage = displayError.map {
                                            profile.userFacingStorageErrorMessage($0)
                                        } ?? ""
                                        eventStream.emitLog(
                                            String.localizedStringWithFormat(
                                                String(localized: "backup.parallel.flushManifestFailed"),
                                                workerID + 1,
                                                monthKey.text,
                                                underlyingMessage
                                            ),
                                            level: .error
                                        )
                                        if let displayError,
                                           profile.isConnectionUnavailableErrorIncludingFlushUnderlying(displayError) {
                                            clientReusable = false
                                            monthFatalError = displayError
                                        } else {
                                            workerState.paused = true
                                        }
                                        shouldBreakAssetLoop = true
                                    } else if let outcome = intervalOutcome,
                                       let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
                                            outcome: outcome,
                                            profile: profile
                                       ) {
                                        switch dispatch.action {
                                        case .pauseAndBreakAssetLoop:
                                            workerState.paused = true
                                            shouldBreakAssetLoop = true
                                        case .abortMonthBreakAssetLoop:
                                            clientReusable = false
                                            monthFatalError = dispatch.displayError
                                            eventStream.emitLog(
                                                String.localizedStringWithFormat(
                                                    String(localized: "backup.parallel.flushManifestFailed"),
                                                    workerID + 1,
                                                    monthKey.text,
                                                    profile.userFacingStorageErrorMessage(dispatch.displayError)
                                                ),
                                                level: .error
                                            )
                                            shouldBreakAssetLoop = true
                                        case .logWarningAndContinue:
                                            eventStream.emitLog(
                                                String.localizedStringWithFormat(
                                                    String(localized: "backup.parallel.flushManifestFailed"),
                                                    workerID + 1,
                                                    monthKey.text,
                                                    profile.userFacingStorageErrorMessage(dispatch.displayError)
                                                ),
                                                level: .warning
                                            )
                                        }
                                    }
                                    if shouldBreakAssetLoop { break }
                                    flushCounter.reset()
                                }
                            }
                        } catch {
                            let assetDispatch = BackupFlushFailureClassification.foregroundAssetErrorDispatch(
                                error: error,
                                profile: profile
                            )
                            var shouldBreakAssetLoop = false
                            switch assetDispatch.action {
                            case .pauseAndBreakAssetLoop:
                                workerState.paused = true
                                shouldBreakAssetLoop = true
                            case .abortMonthDataConnectionLossBreakAssetLoop:
                                let displayName = BackupAssetResourcePlanner.assetDisplayName(
                                    asset: asset,
                                    selectedResources: selectedResources
                                )
                                let errorMessage = profile.userFacingStorageErrorMessage(assetDispatch.error)
                                clientReusable = false
                                monthFatalError = assetDispatch.error
                                shouldFlushAfterDataConnectionLoss = true
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
                                shouldBreakAssetLoop = true
                            case .logGenericFailureAndContinue:
                                let displayName = BackupAssetResourcePlanner.assetDisplayName(
                                    asset: asset,
                                    selectedResources: selectedResources
                                )
                                let errorMessage = profile.userFacingStorageErrorMessage(assetDispatch.error)
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
                            if shouldBreakAssetLoop { break }
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
                let skipFlushDueToUnavailable = monthFatalError.map(profile.isConnectionUnavailableErrorIncludingFlushUnderlying) ?? false
                let canFlushV2AfterDataConnectionLoss = skipFlushDueToUnavailable && v2Services != nil && shouldFlushAfterDataConnectionLoss

                if skipFlushDueToUnavailable && !canFlushV2AfterDataConnectionLoss {
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.skipManifestFlush"),
                            workerID + 1,
                            monthKey.text
                        ),
                        level: .error
                    )
                    // U01 R03: EOM was skipped — no further flush attempt will commit the
                    // remaining V2 pending ops. Reconcile any provisional progress / hash-index
                    // intents left from earlier interval-fail paths (R03 deferred those rollbacks
                    // for the pause-EOM-may-commit case). For the EOM-skip case (typically
                    // connection-unavailable abort), the buffer/queue is stale and must be
                    // rolled back so counters and queued local writes match reality.
                    await Self.rollBackProvisionalAndIntentsForHardAbort(
                        aggregator: aggregator,
                        assetProcessor: assetProcessor,
                        month: monthKey
                    )
                } else {
                    var eomOutcome: V2MonthFlushOutcome?
                    var shouldBreakMonthLoop = false
                    do {
                        eomOutcome = try await Self.flushMonthStorePublishingDefensiveCommits(
                            monthStore: monthStore,
                            month: monthKey,
                            remoteIndexService: remoteIndexService,
                            ignoreCancellation: Self.foregroundFinalFlushIgnoresCancellation(
                                paused: workerState.paused,
                                hasV2Services: monthStore.v2Services != nil
                            )
                        )
                    } catch {
                        switch BackupFlushFailureClassification.classify(error, on: profile).foregroundEndOfMonthAction {
                        case .ignoreConcurrentReject:
                            // Another flusher owns the pending state.
                            break
                        case .pauseAndBreakMonthLoop:
                            await Self.rollBackProvisionalAndIntentsForHardAbort(
                                aggregator: aggregator,
                                assetProcessor: assetProcessor,
                                month: monthKey
                            )
                            workerState.paused = true
                            shouldBreakMonthLoop = true
                        case .abortMonthBreakMonthLoop:
                            await Self.rollBackProvisionalAndIntentsForHardAbort(
                                aggregator: aggregator,
                                assetProcessor: assetProcessor,
                                month: monthKey
                            )
                            clientReusable = false
                            monthFatalError = error
                            eventStream.emitErrorLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.flushManifestFailed"),
                                    workerID + 1,
                                    monthKey.text,
                                    profile.userFacingStorageErrorMessage(error)
                                ),
                                unless: error
                            )
                            shouldBreakMonthLoop = true
                        case .logErrorAndRethrow:
                            await Self.rollBackProvisionalAndIntentsForHardAbort(
                                aggregator: aggregator,
                                assetProcessor: assetProcessor,
                                month: monthKey
                            )
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

                    if let outcome = eomOutcome {
                        await Self.applyDurableBatchSideEffects(
                            aggregator: aggregator,
                            assetProcessor: assetProcessor,
                            month: monthKey,
                            outcome: outcome,
                            eventStream: eventStream,
                            profile: profile,
                            workerID: workerID + 1
                        )
                        // U01 R02: end-of-month partial multi-chunk failure. The remaining
                        // V2 pending ops die with this V2MonthSession (no further flush in
                        // this session for this month), so reconcile counters + intents.
                        if monthStore.hasUncommittedV2Ops {
                            await Self.rollBackProvisionalAndIntentsForHardAbort(
                                aggregator: aggregator,
                                assetProcessor: assetProcessor,
                                month: monthKey
                            )
                            let displayError = outcome.displayError
                            let underlyingMessage = displayError.map {
                                profile.userFacingStorageErrorMessage($0)
                            } ?? ""
                            eventStream.emitErrorLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.flushManifestFailed"),
                                    workerID + 1,
                                    monthKey.text,
                                    underlyingMessage
                                ),
                                unless: displayError ?? NSError(domain: "U01Partial", code: 0)
                            )
                            if let displayError,
                               profile.isConnectionUnavailableErrorIncludingFlushUnderlying(displayError) {
                                clientReusable = false
                                monthFatalError = displayError
                            } else {
                                workerState.paused = true
                            }
                            shouldBreakMonthLoop = true
                        } else if let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
                            outcome: outcome,
                            profile: profile,
                            shouldFinishMonth: shouldFinishMonth
                        ) {
                            switch dispatch.action {
                            case .pauseAndBreakMonthLoop:
                                workerState.paused = true
                                shouldBreakMonthLoop = true
                            case .abortMonthBreakMonthLoop:
                                clientReusable = false
                                monthFatalError = dispatch.displayError
                                eventStream.emitErrorLog(
                                    String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.flushManifestFailed"),
                                        workerID + 1,
                                        monthKey.text,
                                        profile.userFacingStorageErrorMessage(dispatch.displayError)
                                    ),
                                    unless: dispatch.displayError
                                )
                                shouldBreakMonthLoop = true
                            case .logErrorAndEmitDeferred:
                                eventStream.emitErrorLog(
                                    String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.flushManifestFailed"),
                                        workerID + 1,
                                        monthKey.text,
                                        profile.userFacingStorageErrorMessage(dispatch.displayError)
                                    ),
                                    unless: dispatch.displayError
                                )
                                Self.emitUploadDurableSnapshotDeferred(
                                    eventStream: eventStream,
                                    month: monthKey,
                                    message: profile.userFacingStorageErrorMessage(dispatch.displayError)
                                )
                            case .logErrorOnly:
                                eventStream.emitErrorLog(
                                    String.localizedStringWithFormat(
                                        String(localized: "backup.parallel.flushManifestFailed"),
                                        workerID + 1,
                                        monthKey.text,
                                        profile.userFacingStorageErrorMessage(dispatch.displayError)
                                    ),
                                    unless: dispatch.displayError
                                )
                            }
                        } else if shouldFinishMonth {
                            let emitCompleted: () -> Void = {
                                eventStream.emit(.monthChanged(MonthChangeEvent(
                                    year: monthKey.year,
                                    month: monthKey.month,
                                    action: .completed
                                )))
                            }
                            if let onMonthUploaded {
                                switch await onMonthUploaded(monthKey) {
                                case .success:
                                    emitCompleted()
                                case .incomplete(let summary):
                                    let message = BackupMonthIncompleteSummaryRenderer.message(for: summary, month: monthKey)
                                    eventStream.emitLog(
                                        String.localizedStringWithFormat(
                                            String(localized: "backup.parallel.finalizationFailed"),
                                            workerID + 1,
                                            monthKey.text,
                                            message
                                        ),
                                        level: .warning
                                    )
                                    eventStream.emit(.monthChanged(MonthChangeEvent(
                                        year: monthKey.year,
                                        month: monthKey.month,
                                        action: .incomplete(summary)
                                    )))
                                case .failed(let failure):
                                    eventStream.emitLog(
                                        String.localizedStringWithFormat(
                                            String(localized: "backup.parallel.finalizationFailed"),
                                            workerID + 1,
                                            monthKey.text,
                                            failure.message
                                        ),
                                        level: .error
                                    )
                                    // Surface as fatal so Home and the worker agree the month failed.
                                    var userInfo: [String: Any] = [
                                        NSLocalizedDescriptionKey: "onMonthUploaded failed: \(failure.message)"
                                    ]
                                    if let underlyingError = failure.underlyingError {
                                        userInfo[NSUnderlyingErrorKey] = underlyingError
                                    }
                                    monthFatalError = NSError(
                                        domain: "BackupParallelExecutor",
                                        code: -201,
                                        userInfo: userInfo
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
                            } else {
                                emitCompleted()
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
                    }
                    if shouldBreakMonthLoop { break }
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
            if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                clientReusable = false
            }
            await clientPool.release(client, reusable: clientReusable)
            throw error
        }
    }

    private func monthAlreadyFullyBackedUp(
        monthAssetIDs: [PhotoKitLocalIdentifier],
        monthStore: any BackupMonthStore
    ) -> Bool {
        guard !monthAssetIDs.isEmpty else { return true }
        guard monthStore.hasAnyAsset else { return false }

        guard let cachedHashes = try? hashIndexRepository.fetchAssetHashCaches(
            assetIDs: Set(monthAssetIDs)
        ) else { return false }
        guard cachedHashes.count == monthAssetIDs.count else { return false }

        let fetchResult = PHAsset.fetchAssets(
            withLocalIdentifiers: monthAssetIDs.rawValues,
            options: nil
        )
        guard fetchResult.count == monthAssetIDs.count else { return false }

        for index in 0 ..< fetchResult.count {
            let asset = fetchResult.object(at: index)
            guard let cache = cachedHashes[PhotoKitLocalIdentifier(asset)] else { return false }
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
            guard LocalHashIndexTrust.cacheFieldsPassCheapChecks(
                cache.trustFields,
                modificationDate: asset.modificationDate
            ) else { return false }
            // PHAsset resource-shape drift can occur without an mtime bump.
            let currentResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                from: PHAssetResource.assetResources(for: asset)
            )
            if currentResources.count != cache.resourceCount {
                return false
            }
            let currentSignature = BackupAssetResourcePlanner.resourceSignature(orderedResources: currentResources)
            if !LocalHashIndexTrust.signatureMatches(cache.trustFields, currentSignature: currentSignature) {
                return false
            }
            // Pre-fix manifests may still carry strict-subset survivors of this asset;
            // force the per-asset path so AssetProcessor tombstones them.
            let resourceKeys = Set(
                cache.hashesByRoleSlot.map {
                    AssetResourceLinkKey(role: $0.key.role, slot: $0.key.slot, hash: $0.value)
                }
            )
            if monthStore.hasStrictSubsetAssetFingerprint(forResourceKeys: resourceKeys) {
                return false
            }
        }
        return true
    }

    @discardableResult
    static func flushMonthStorePublishingDefensiveCommits(
        monthStore: any BackupMonthStore,
        month: LibraryMonthKey,
        remoteIndexService: RemoteIndexSyncService,
        ignoreCancellation: Bool
    ) async throws -> V2MonthFlushOutcome {
        do {
            let delta = try await monthStore.flushToRemote(ignoreCancellation: ignoreCancellation)
            publishDefensiveFlushSnapshotIfNeeded(
                monthStore: monthStore,
                month: month,
                remoteIndexService: remoteIndexService,
                delta: delta
            )
            return .completed(delta)
        } catch let flushError as V2MonthSession.FlushError {
            switch flushError {
            case .concurrentFlushRejected:
                throw flushError
            case .snapshotWriteFailed(let assets, let tombstones, _):
                let recovered = BackupMonthFlushDelta(
                    didFlush: true,
                    committedAssetFingerprints: assets,
                    committedTombstoneFingerprints: tombstones
                )
                publishDefensiveFlushSnapshotIfNeeded(
                    monthStore: monthStore,
                    month: month,
                    remoteIndexService: remoteIndexService,
                    delta: recovered
                )
                return .commitDurableSnapshotDeferred(delta: recovered, flushError: flushError)
            }
        }
    }

    static func publishDefensiveFlushSnapshotIfNeeded(
        monthStore: any BackupMonthStore,
        month: LibraryMonthKey,
        remoteIndexService: RemoteIndexSyncService,
        delta: BackupMonthFlushDelta
    ) {
        let committed = delta.committedAssetFingerprints.union(delta.committedTombstoneFingerprints)
        guard !committed.isEmpty else { return }
        // U01 R02: under partial multi-chunk failure, `monthStore.unsortedSnapshot()` still carries
        // the uncommitted chunk-N+1 rows in `assetsByFingerprint`. Publishing here would leak those
        // non-durable fingerprints into `RemoteIndexSyncService.committedView`. The in-process
        // optimistic overlay (per-asset `appendAsset` during processing) already surfaces chunk-1
        // to in-session consumers, and next-session materialize rebuilds the committed view from
        // durable commit files — skipping the publish here is the conservative choice.
        guard !monthStore.hasUncommittedV2Ops else { return }
        remoteIndexService.publishMonthSnapshot(of: monthStore, for: month)
    }

    /// Drain queued hash-index intents for the fingerprints the batch commit just made durable.
    /// Drain failure is logged but never propagated: remote commit is durable; the asset will
    /// safely re-process next session via the `containsDurableAssetFingerprint` short-circuit.
    /// Returns whether any drain failure occurred plus the first error (for callers that want to
    /// emit log lines in their own format — foreground emits via eventStream, background via the
    /// session writer).
    @discardableResult
    static func drainHashIndexIntentsForDurableFlush(
        assetProcessor: AssetProcessor,
        month: LibraryMonthKey,
        outcome: V2MonthFlushOutcome
    ) async -> HashIndexDrainOutcome {
        let durableFingerprints = outcome.delta.committedAssetFingerprints
        guard !durableFingerprints.isEmpty else { return .allDrained(count: 0) }
        return await assetProcessor.drainHashIndexIntents(
            month: month,
            durableAssetFingerprints: durableFingerprints
        )
    }

    /// Foreground post-flush reconciliation for both `.completed` and
    /// `.commitDurableSnapshotDeferred` — drain intents, clear matching provisional progress
    /// entries on the aggregator, and emit a warning log on partial drain.
    static func applyDurableBatchSideEffects(
        aggregator: ParallelBackupProgressAggregator,
        assetProcessor: AssetProcessor,
        month: LibraryMonthKey,
        outcome: V2MonthFlushOutcome,
        eventStream: BackupEventStream,
        profile: ServerProfileRecord,
        workerID: Int
    ) async {
        let durableFingerprints = outcome.delta.committedAssetFingerprints
        guard !durableFingerprints.isEmpty else { return }
        let drainOutcome = await drainHashIndexIntentsForDurableFlush(
            assetProcessor: assetProcessor,
            month: month,
            outcome: outcome
        )
        if case .partial(_, let failedCount, let firstError) = drainOutcome {
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.hashIndexDrainPartial"),
                    workerID,
                    month.text,
                    failedCount,
                    profile.userFacingStorageErrorMessage(firstError)
                ),
                level: .warning
            )
        }
        await aggregator.markBatchDurable(
            month: month,
            committedAssetFingerprints: durableFingerprints
        )
    }

    /// Foreground hard-abort reconciliation. Counters revert for fingerprints we provisionally
    /// counted as success/skipped but whose commit never landed; the matching hash-index intents
    /// are discarded so a same-session retry does not double-write. The optimistic month overlay
    /// in `RemoteIndexSyncService.committedView` is also dropped (U01 R05) so the per-asset
    /// `optimisticWriter.appendAsset` calls that ran before the failed commit do not keep
    /// surfacing non-durable fingerprints through `remoteMonthRawData(for:)` /
    /// `resumeSafeToSkipAssetFingerprintsByMonth()`.
    static func rollBackProvisionalAndIntentsForHardAbort(
        aggregator: ParallelBackupProgressAggregator,
        assetProcessor: AssetProcessor,
        month: LibraryMonthKey
    ) async {
        let rolledBackFingerprints = await aggregator.rollBackProvisionalBatch(month: month)
        if !rolledBackFingerprints.isEmpty {
            await assetProcessor.rollBackHashIndexIntents(
                month: month,
                fingerprints: rolledBackFingerprints
            )
        }
        assetProcessor.remoteIndexService.dropOptimisticMonthIfStale(month: month)
    }

    @discardableResult
    static func emitUploadDurableSnapshotDeferred(
        eventStream: BackupEventStream,
        month: LibraryMonthKey,
        message: String
    ) -> Bool {
        eventStream.emit(.monthChanged(MonthChangeEvent(
            year: month.year,
            month: month.month,
            action: .uploadDurableSnapshotDeferred(message: message)
        )))
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
