import Foundation
import Photos
import os.log

private let executorLog = Logger(subsystem: "com.zizicici.watermelon", category: "BackupParallel")

actor BackupThermalThrottle {
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

    let hashIndexRepository: ContentHashIndexRepository
    let assetProcessor: AssetProcessor
    let remoteIndexService: RemoteIndexSyncService

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
            // Match the normal completion path: shutdown V2 runtime and disconnect the data
            // client BEFORE emitting .finished. .finished triggers BSC.handleEvent →
            // clearActiveRunState which nils runTask; if shutdown lagged the emit, the
            // foreground exit cleanup chain (HomeExecutionCoordinator → awaitCleanup →
            // awaitRunTaskCompletion → await runTask?.value) could observe nil and release
            // the AppRuntimeFlags execution lease while the runtime is still tearing down.
            await preparedRun.v2Services?.shutdown()
            await preparedRun.initialClient.disconnectSafely()
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            eventStream.emit(.finished(result))
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
        var workUnit = MonthWorkUnit()
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
        do {
            while let monthPlan = await monthQueue.next() {
                if Task.isCancelled {
                    workUnit.markPaused()
                    break
                }

                let monthKey = monthPlan.month
                let monthAssetIDs = monthPlan.assetLocalIdentifiers

                guard let monthStore = try await loadMonthStoreForWorker(
                    client: client,
                    monthKey: monthKey,
                    profile: profile,
                    workerID: workerID,
                    v2Services: v2Services,
                    eventStream: eventStream,
                    workUnit: &workUnit
                ) else {
                    // nil == cancellation during load; workUnit already marked paused.
                    break
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

                workUnit.beginMonth()
                let transaction = MonthCommitTransaction(
                    aggregator: aggregator,
                    assetProcessor: assetProcessor,
                    eventStream: eventStream,
                    profile: profile,
                    month: monthKey,
                    workerID: workerID + 1
                )
                let skippedMonthShortCircuit = await emitMonthSkipIfFullyBackedUp(
                    monthAssetIDs: monthAssetIDs,
                    monthStore: monthStore,
                    monthKey: monthKey,
                    workerID: workerID,
                    aggregator: aggregator,
                    eventStream: eventStream
                )

                if !skippedMonthShortCircuit {
                    await processMonthBatches(
                        context: MonthBatchContext(
                            monthKey: monthKey,
                            monthAssetIDs: monthAssetIDs,
                            monthStore: monthStore,
                            workerID: workerID,
                            iCloudPhotoBackupMode: iCloudPhotoBackupMode,
                            profile: profile,
                            v2Services: v2Services,
                            transaction: transaction,
                            fetchBatchSize: 500
                        ),
                        client: client,
                        eventStream: eventStream,
                        aggregator: aggregator,
                        workUnit: &workUnit
                    )
                }

                let eomFlow = try await finishMonthFlushingAndPublishing(
                    monthStore: monthStore,
                    monthKey: monthKey,
                    profile: profile,
                    workerID: workerID,
                    v2Services: v2Services,
                    transaction: transaction,
                    onMonthUploaded: onMonthUploaded,
                    workUnit: &workUnit,
                    eventStream: eventStream
                )
                if case .breakMonthLoop = eomFlow {
                    break
                }

                if let monthFatalError = workUnit.fatalError {
                    throw monthFatalError
                }

                if workUnit.paused {
                    break
                }
            }

            await clientPool.release(client, reusable: workUnit.clientReusable)
            return workUnit.workerRunState
        } catch {
            if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                workUnit.markClientNotReusable()
            }
            await clientPool.release(client, reusable: workUnit.clientReusable)
            throw error
        }
    }

    /// Returns the loaded month store, or `nil` to signal a cancellation `break` of the month loop
    /// (workUnit is marked paused before returning nil). Load failures throw, propagating exactly
    /// as the inline `throw error` did.
    private func loadMonthStoreForWorker(
        client: any RemoteStorageClientProtocol,
        monthKey: LibraryMonthKey,
        profile: ServerProfileRecord,
        workerID: Int,
        v2Services: BackupV2RuntimeServices?,
        eventStream: BackupEventStream,
        workUnit: inout MonthWorkUnit
    ) async throws -> (any BackupMonthStore)? {
        guard let v2Services else {
            throw BackupV2RuntimeBuildError.requiresForegroundMigration
        }
        let monthStore: any BackupMonthStore
        do {
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
        } catch {
            if error is CancellationError {
                workUnit.markPaused()
                return nil
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
        return monthStore
    }

    private func emitMonthSkipIfFullyBackedUp(
        monthAssetIDs: [PhotoKitLocalIdentifier],
        monthStore: any BackupMonthStore,
        monthKey: LibraryMonthKey,
        workerID: Int,
        aggregator: ParallelBackupProgressAggregator,
        eventStream: BackupEventStream
    ) async -> Bool {
        guard monthAlreadyFullyBackedUp(
            monthAssetIDs: monthAssetIDs,
            monthStore: monthStore
        ) else {
            return false
        }
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
        return true
    }

    /// End-of-month flush + finish/publish. Returns `.breakMonthLoop` for every path the inline code
    /// broke the month loop on (the `shouldBreakMonthLoop` flag and the embedded onMonthUploaded
    /// pause-break), `.proceed` otherwise. The `logErrorAndRethrow` path throws, propagating unchanged.
    private func finishMonthFlushingAndPublishing(
        monthStore: any BackupMonthStore,
        monthKey: LibraryMonthKey,
        profile: ServerProfileRecord,
        workerID: Int,
        v2Services: BackupV2RuntimeServices?,
        transaction: MonthCommitTransaction,
        onMonthUploaded: BackupMonthFinalizer?,
        workUnit: inout MonthWorkUnit,
        eventStream: BackupEventStream
    ) async throws -> MonthLoopFlow {
        let shouldFinishMonth = !workUnit.paused && !workUnit.hasFatal
        let hadDirtyManifestBeforeFinalize = monthStore.dirty
        let skipFlushDueToUnavailable = workUnit.fatalError.map(profile.isConnectionUnavailableErrorIncludingFlushUnderlying) ?? false
        let canFlushV2AfterDataConnectionLoss = skipFlushDueToUnavailable && v2Services != nil && workUnit.shouldFlushAfterDataConnectionLoss

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
            await transaction.abort()
        } else {
            var eomOutcome: V2MonthFlushOutcome?
            var shouldBreakMonthLoop = false
            do {
                eomOutcome = try await Self.flushMonthStorePublishingDefensiveCommits(
                    monthStore: monthStore,
                    month: monthKey,
                    remoteIndexService: remoteIndexService,
                    ignoreCancellation: Self.foregroundFinalFlushIgnoresCancellation(
                        paused: workUnit.paused,
                        hasV2Services: monthStore.v2Services != nil
                    )
                )
            } catch {
                switch BackupFlushFailureClassification.classify(error, on: profile).foregroundEndOfMonthAction {
                case .ignoreConcurrentReject:
                    // Another flusher owns the pending state.
                    break
                case .pauseAndBreakMonthLoop:
                    await transaction.abort()
                    workUnit.markPaused()
                    shouldBreakMonthLoop = true
                case .abortMonthBreakMonthLoop:
                    await transaction.abort()
                    workUnit.markFatal(error)
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
                    await transaction.abort()
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
                await transaction.applyDurableSideEffects(outcome: outcome)
                // U01 R02: end-of-month partial multi-chunk failure. The remaining
                // V2 pending ops die with this V2MonthSession (no further flush in
                // this session for this month), so reconcile counters + intents.
                if monthStore.hasUncommittedV2Ops {
                    await transaction.abort()
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
                        workUnit.markFatal(displayError)
                    } else {
                        workUnit.markPaused()
                    }
                    shouldBreakMonthLoop = true
                } else if let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
                    outcome: outcome,
                    profile: profile,
                    shouldFinishMonth: shouldFinishMonth
                ) {
                    switch dispatch.action {
                    case .pauseAndBreakMonthLoop:
                        workUnit.markPaused()
                        shouldBreakMonthLoop = true
                    case .abortMonthBreakMonthLoop:
                        workUnit.markFatal(dispatch.displayError)
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
                            workUnit.markFatalKeepingClient(NSError(
                                domain: "BackupParallelExecutor",
                                code: -201,
                                userInfo: userInfo
                            ))
                        case .cancelled:
                            workUnit.markPaused()
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.finalizationCancelled"),
                                    workerID + 1,
                                    monthKey.text
                                ),
                                level: .info
                            )
                        }
                        if workUnit.paused {
                            return .breakMonthLoop
                        }
                    } else {
                        emitCompleted()
                    }
                } else {
                    if workUnit.hasFatal {
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
            if shouldBreakMonthLoop { return .breakMonthLoop }
        }
        return .proceed
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
        } catch let deferred as V2MonthSession.MonthDurableSnapshotDeferred {
            publishDefensiveFlushSnapshotIfNeeded(
                monthStore: monthStore,
                month: month,
                remoteIndexService: remoteIndexService,
                delta: deferred.delta
            )
            return .commitDurableSnapshotDeferred(delta: deferred.delta, flushError: deferred.flushError)
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
            committedAssetFingerprints: durableFingerprints,
            committedTombstoneFingerprints: outcome.delta.committedTombstoneFingerprints
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
        MonthOverlayCoordinator(remoteIndexService: assetProcessor.remoteIndexService).onHardAbort(month: month)
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

    func emitProgress(
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

    func emitFailureProgress(
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

    static func contextSuffix(workerID: Int, monthText: String) -> String {
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
