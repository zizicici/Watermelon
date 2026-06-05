import Foundation
import Photos

/// Per-batch and per-asset processing for one month, extracted from `runParallelMonthWorker`.
/// Each helper containing loop control flow returns an explicit flow signal (`AssetLoopFlow` /
/// `BatchLoopFlow`) because Swift `break`/`continue` cannot cross a function boundary — the caller
/// translates the signal back into the corresponding `break`/fall-through.
extension BackupParallelExecutor {

    /// Inputs constant across every batch/asset of one month.
    struct MonthBatchContext {
        let monthKey: LibraryMonthKey
        let monthAssetIDs: [PhotoKitLocalIdentifier]
        let monthStore: any BackupMonthStore
        let workerID: Int
        let iCloudPhotoBackupMode: ICloudPhotoBackupMode
        let profile: ServerProfileRecord
        let v2Services: BackupV2RuntimeServices?
        let transaction: MonthDurableTransaction
        let fetchBatchSize: Int
    }

    /// Drives every batch of the month. Owns the per-month mutable locals (`missingAssetCount`,
    /// `hasLoggedLocalHashCacheWarning`, `flushCounter`) and emits the missing-asset warning at the end.
    func processMonthBatches(
        context: MonthBatchContext,
        client: any RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        workUnit: inout MonthWorkUnit
    ) async {
        var missingAssetCount = 0
        var hasLoggedLocalHashCacheWarning = false
        var flushCounter = AssetBatchFlushCounter(threshold: Self.flushInterval)

        for batchStart in stride(from: 0, to: context.monthAssetIDs.count, by: context.fetchBatchSize) {
            let batchEnd = min(batchStart + context.fetchBatchSize, context.monthAssetIDs.count)
            let batchAssetIDs = Array(context.monthAssetIDs[batchStart ..< batchEnd])
            guard !batchAssetIDs.isEmpty else { continue }

            let flow = await processBatch(
                batchAssetIDs: batchAssetIDs,
                context: context,
                client: client,
                eventStream: eventStream,
                aggregator: aggregator,
                workUnit: &workUnit,
                flushCounter: &flushCounter,
                missingAssetCount: &missingAssetCount,
                hasLoggedLocalHashCacheWarning: &hasLoggedLocalHashCacheWarning
            )
            if case .breakBatchLoop = flow { break }
        }

        if missingAssetCount > 0 {
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.missingAssets"),
                    context.workerID + 1,
                    context.monthKey.text,
                    missingAssetCount
                ),
                level: .warning
            )
        }
    }

    /// One batch: fetch caches/assets, run the asset loop, then the post-loop break + cleanup.
    private func processBatch(
        batchAssetIDs: [PhotoKitLocalIdentifier],
        context: MonthBatchContext,
        client: any RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        workUnit: inout MonthWorkUnit,
        flushCounter: inout AssetBatchFlushCounter,
        missingAssetCount: inout Int,
        hasLoggedLocalHashCacheWarning: inout Bool
    ) async -> BatchLoopFlow {
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
                        context.workerID + 1,
                        context.monthKey.text,
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
            let flow = await processSingleAsset(
                assetID: assetID,
                context: context,
                client: client,
                eventStream: eventStream,
                aggregator: aggregator,
                workUnit: &workUnit,
                flushCounter: &flushCounter,
                batchAssetsByLocalIdentifier: &batchAssetsByLocalIdentifier,
                batchLocalHashCacheByAssetID: &batchLocalHashCacheByAssetID
            )
            if case .breakAssetLoop = flow { break }
        }

        if workUnit.paused || workUnit.hasFatal {
            return .breakBatchLoop
        }

        batchAssetsByLocalIdentifier.removeAll(keepingCapacity: false)
        batchLocalHashCacheByAssetID.removeAll(keepingCapacity: false)
        return .completed
    }

    /// Process one asset: cancellation/empty guards, `assetProcessor.process`, provisional tracking,
    /// then the interval flush or the asset-error catch. `.skippedEmpty` for the empty-asset
    /// `continue`; `.breakAssetLoop` for every path the inline loop broke on; `.processed` otherwise.
    private func processSingleAsset(
        assetID: PhotoKitLocalIdentifier,
        context: MonthBatchContext,
        client: any RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        workUnit: inout MonthWorkUnit,
        flushCounter: inout AssetBatchFlushCounter,
        batchAssetsByLocalIdentifier: inout [PhotoKitLocalIdentifier: PHAsset],
        batchLocalHashCacheByAssetID: inout [PhotoKitLocalIdentifier: LocalAssetHashCache]
    ) async -> AssetLoopFlow {
        let monthKey = context.monthKey
        let workerID = context.workerID

        if Task.isCancelled {
            workUnit.markPaused()
            return .breakAssetLoop
        }

        await BackupThermalThrottle.shared.waitIfNeeded()
        if Task.isCancelled {
            workUnit.markPaused()
            return .breakAssetLoop
        }

        guard let asset = batchAssetsByLocalIdentifier.removeValue(forKey: assetID) else {
            await aggregator.reduceTotalForEmptyAsset()
            return .skippedEmpty
        }

        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: asset)
        )
        if selectedResources.isEmpty {
            await aggregator.reduceTotalForEmptyAsset()
            return .skippedEmpty
        }

        let dispatch = await aggregator.allocateDispatchSlot()
        let cachedLocalHash = batchLocalHashCacheByAssetID.removeValue(forKey: assetID)

        do {
            let processContext = AssetProcessContext(
                workerID: workerID + 1,
                asset: asset,
                selectedResources: selectedResources,
                cachedLocalHash: cachedLocalHash,
                iCloudPhotoBackupMode: context.iCloudPhotoBackupMode,
                monthStore: context.monthStore,
                profile: context.profile,
                assetPosition: dispatch.position,
                totalAssets: dispatch.total
            )

            let result = try await assetProcessor.process(
                context: processContext,
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
            if context.v2Services != nil,
               result.wroteProvisionalV2Row,
               let fingerprint = result.assetFingerprint {
                switch result.status {
                case .success:
                    await aggregator.recordProvisional(
                        month: monthKey,
                        fingerprint: fingerprint,
                        assetLocalIdentifier: PhotoKitLocalIdentifier(asset),
                        status: .success,
                        tombstonedSubsets: result.tombstonedSubsetFingerprints
                    )
                case .skipped:
                    await aggregator.recordProvisional(
                        month: monthKey,
                        fingerprint: fingerprint,
                        assetLocalIdentifier: PhotoKitLocalIdentifier(asset),
                        status: .skipped,
                        tombstonedSubsets: result.tombstonedSubsetFingerprints
                    )
                case .failed:
                    break
                }
            }

            // Cached-reuse `.skipped` also writes asset rows; only `.failed`
            // skips the batch counter.
            if result.status != .failed {
                if flushCounter.recordSuccessAndCheckThreshold() {
                    return await handleIntervalFlush(
                        context: context,
                        eventStream: eventStream,
                        workUnit: &workUnit,
                        flushCounter: &flushCounter
                    )
                }
            }
            return .processed
        } catch {
            return await handleAssetError(
                error: error,
                asset: asset,
                selectedResources: selectedResources,
                context: context,
                eventStream: eventStream,
                aggregator: aggregator,
                workUnit: &workUnit
            )
        }
    }

    /// Interval flush after the batch threshold. `.processed` continues the asset loop (counter
    /// reset applied); `.breakAssetLoop` stops it (workUnit transition applied). All R02/R03
    /// no-rollback-at-interval behavior is preserved.
    private func handleIntervalFlush(
        context: MonthBatchContext,
        eventStream: BackupEventStream,
        workUnit: inout MonthWorkUnit,
        flushCounter: inout AssetBatchFlushCounter
    ) async -> AssetLoopFlow {
        let monthKey = context.monthKey
        let workerID = context.workerID
        let profile = context.profile
        let monthStore = context.monthStore

        var intervalOutcome: V2MonthFlushOutcome?
        do {
            intervalOutcome = try await Self.commitMonthStoreDefensively(
                monthStore: monthStore,
                ignoreCancellation: false
            )
        } catch {
            switch BackupFlushFailureClassification.classify(error, on: profile).foregroundIntervalAction {
            case .continueAssetLoopAndResetCounter:
                // No commit landed; pending stays in memory for the
                // next attempt — provisional buffer also carries.
                flushCounter.reset()
                return .processed
            case .pauseAndBreakAssetLoop:
                // U01 R03: do NOT roll back provisional/intents here.
                // The paused end-of-month flush below runs with
                // ignoreCancellation=true and can still commit these
                // same pending V2 ops; pre-emptive rollback would
                // discard intents/counters for assets that ultimately
                // become durable. Reconciliation happens after EOM
                // (applyDurableBatchSideEffects + hasUncommittedV2Ops
                // branch, EOM catch, or EOM-skip branch below).
                workUnit.markPaused()
                return .breakAssetLoop
            case .abortMonthBreakAssetLoop:
                // U01 R03: see above — EOM-skip branch below rolls
                // back any remaining buffer/intents for the
                // connection-unavailable abort path that suppresses
                // the final flush.
                workUnit.markFatal(error)
                eventStream.emitLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.parallel.flushManifestFailed"),
                        workerID + 1,
                        monthKey.text,
                        profile.userFacingStorageErrorMessage(error)
                    ),
                    level: .error
                )
                return .breakAssetLoop
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
            context.transaction.beginCommitDurable(outcome: outcome)
            try? await context.transaction.drainSideEffects()
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
                workUnit.markFatal(displayError)
            } else {
                workUnit.markPaused()
            }
            return .breakAssetLoop
        } else if let outcome = intervalOutcome {
            // W1: publish trails the side-effect drain (gated off while uncommitted ops remain).
            try? context.transaction.publishCommittedView(monthStore: monthStore)
            if let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
                outcome: outcome,
                profile: profile
            ) {
                switch dispatch.action {
                case .pauseAndBreakAssetLoop:
                    workUnit.markPaused()
                    return .breakAssetLoop
                case .abortMonthBreakAssetLoop:
                    workUnit.markFatal(dispatch.displayError)
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.flushManifestFailed"),
                            workerID + 1,
                            monthKey.text,
                            profile.userFacingStorageErrorMessage(dispatch.displayError)
                        ),
                        level: .error
                    )
                    return .breakAssetLoop
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
        }
        flushCounter.reset()
        return .processed
    }

    /// Asset-processing error handler. `.breakAssetLoop` (pause / data-connection-loss) or
    /// `.processed` (logged generic failure, loop continues).
    private func handleAssetError(
        error: Error,
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        context: MonthBatchContext,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        workUnit: inout MonthWorkUnit
    ) async -> AssetLoopFlow {
        let monthKey = context.monthKey
        let workerID = context.workerID
        let profile = context.profile

        let assetDispatch = BackupFlushFailureClassification.foregroundAssetErrorDispatch(
            error: error,
            profile: profile
        )
        switch assetDispatch.action {
        case .pauseAndBreakAssetLoop:
            workUnit.markPaused()
            return .breakAssetLoop
        case .abortMonthDataConnectionLossBreakAssetLoop:
            let displayName = BackupAssetResourcePlanner.assetDisplayName(
                asset: asset,
                selectedResources: selectedResources
            )
            let errorMessage = profile.userFacingStorageErrorMessage(assetDispatch.error)
            workUnit.markDataConnectionLost(assetDispatch.error)
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
            return .breakAssetLoop
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
            return .processed
        }
    }
}
