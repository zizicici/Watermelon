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
                try? await Task.sleep(for: .seconds(10))
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
    private let hashIndexRepository: ContentHashIndexRepository
    private let assetProcessor: AssetProcessor
    private let remoteIndexService: RemoteIndexSyncService

    enum RecoveryOutcome {
        case recovered(any RemoteStorageClientProtocol)
        case cancelled        // this worker's task was cancelled (user pause/stop)
        case stopped          // a sibling stopped the queue (defer to its fatal; don't compete)
        case failed(Error)    // a terminal fault (auth/config/cert) surfaced during reconnect — real failure
        case exhausted        // the window elapsed with the network still down — resumable pause
    }

    // Worker state finalizeMonth mutates in place; the worker reads the fields back after it returns.
    final class MonthFinalizeState {
        var client: any RemoteStorageClientProtocol
        var clientReusable: Bool
        var recoveryDeadline: Date?
        var run: WorkerRunState
        var monthFatalError: Error?
        init(
            client: any RemoteStorageClientProtocol,
            clientReusable: Bool,
            recoveryDeadline: Date?,
            run: WorkerRunState,
            monthFatalError: Error?
        ) {
            self.client = client
            self.clientReusable = clientReusable
            self.recoveryDeadline = recoveryDeadline
            self.run = run
            self.monthFatalError = monthFatalError
        }
    }

    enum MonthFinalizeDisposition {
        case proceed          // worker re-checks monthFatalError / paused / stopped, then continues
        case breakMonthLoop   // pause-uncommitted: worker breaks the month loop
        case throwError(Error)   // finishing-run flush / finalizer failure the worker rethrows (after read-back)
    }

    // Skip-flush / pause routing for a month whose fatal is the network being unavailable (an ejected
    // external volume, or a worker whose bounded recovery was exhausted), as opposed to a real failure.
    static func isNetworkUnavailableFatal(_ error: Error, profile: ServerProfileRecord) -> Bool {
        error is BackupNetworkRecoveryExhausted || profile.isConnectionUnavailableError(error)
    }

    init(
        hashIndexRepository: ContentHashIndexRepository,
        assetProcessor: AssetProcessor,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.assetProcessor = assetProcessor
        self.remoteIndexService = remoteIndexService
    }

    static func shouldContinueAfterManifestFlushFailure(_ error: Error) -> Bool {
        MonthManifestStore.isReadBackVerificationError(error)
    }

    // Which results wrote month-manifest asset/link rows that are not durable until the month SQLite commits.
    // `.skipped` reused-resource paths still `upsertAsset` before returning (AssetProcessor), so they are as
    // much at risk on a failed flush as `.success`; `asset_exists_cached`/`icloud_*`/`asset_gone` write no rows.
    static func resultDirtiedMonthManifest(status: BackupItemStatus, reason: String?) -> Bool {
        switch status {
        case .success:
            return true
        case .skipped:
            return reason == "resources_reused" || reason == "resources_reused_cached"
        case .failed:
            return false
        }
    }

    // A user pause/stop landing while a dirty month has not committed must un-mark that month's assets and pause
    // cleanly, not throw: a thrown error still becomes `.paused` via applyRunError but leaves the uncommitted
    // assets resume-complete, so resume would skip them.
    static func shouldPauseUncommittedMonthOnCancellation(cancelled: Bool, hadDirtyManifest: Bool) -> Bool {
        cancelled && hadDirtyManifest
    }

    static func skippedMonthTransferState(
        monthKey: MonthKey,
        workerID: Int,
        skippedAssetCount: Int,
        estimatedBytes: Int64
    ) -> BackupTransferState? {
        guard estimatedBytes > 0 else { return nil }
        return AssetProcessor.makeTransferState(
            kind: .upload,
            workerID: workerID,
            assetLocalIdentifier: "month:\(monthKey.year)-\(String(format: "%02d", monthKey.month))",
            assetDisplayName: monthKey.text,
            resourceDate: nil,
            assetPosition: 1,
            totalAssets: skippedAssetCount,
            resourceDisplayName: monthKey.text,
            resourcePosition: 1,
            totalResources: 1,
            resourceFraction: 1,
            resourceBytesTransferred: estimatedBytes,
            resourceTotalBytes: estimatedBytes,
            countsTowardTransferSpeed: false,
            stageDescription: String(localized: "backup.transfer.uploadCompleted")
        )
    }

    static func failedAssetTransferStates(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        workerID: Int,
        assetPosition: Int,
        totalAssets: Int,
        displayName: String
    ) -> [BackupTransferState] {
        let states = selectedResources.enumerated().compactMap { index, selected -> BackupTransferState? in
            let fileSize = PhotoLibraryService.resourceFileSize(selected.resource)
            guard fileSize > 0 else { return nil }
            return AssetProcessor.makeTransferState(
                kind: .upload,
                workerID: workerID,
                assetLocalIdentifier: asset.localIdentifier,
                assetDisplayName: displayName,
                resourceDate: asset.creationDate ?? asset.modificationDate,
                assetPosition: assetPosition,
                totalAssets: totalAssets,
                resourceDisplayName: PhotoLibraryService.safeOriginalFilename(for: selected.resource),
                resourcePosition: index + 1,
                totalResources: selectedResources.count,
                resourceFraction: 1,
                resourceBytesTransferred: fileSize,
                resourceTotalBytes: fileSize,
                countsTowardTransferSpeed: false,
                stageDescription: String(localized: "backup.transfer.uploadCompleted")
            )
        }
        return states
    }

    static func estimatedAssetTransferState(
        assetLocalIdentifier: String,
        displayName: String,
        totalBytes: Int64,
        workerID: Int,
        assetPosition: Int,
        totalAssets: Int
    ) -> BackupTransferState? {
        guard totalBytes > 0 else { return nil }
        return AssetProcessor.makeTransferState(
            kind: .upload,
            workerID: workerID,
            assetLocalIdentifier: assetLocalIdentifier,
            assetDisplayName: displayName,
            resourceDate: nil,
            assetPosition: assetPosition,
            totalAssets: totalAssets,
            resourceDisplayName: displayName,
            resourcePosition: 1,
            totalResources: 1,
            resourceFraction: 1,
            resourceBytesTransferred: totalBytes,
            resourceTotalBytes: totalBytes,
            countsTowardTransferSpeed: false,
            stageDescription: String(localized: "backup.transfer.uploadCompleted")
        )
    }

    static func shouldEmitResultCredit(_ result: AssetProcessResult) -> Bool {
        guard result.status == .skipped else { return false }
        switch result.reason {
        case "asset_exists_cached",
             "resources_reused_cached",
             "icloud_photo_backup_disabled",
             "asset_gone",
             "asset_no_resources":
            return true
        default:
            return false
        }
    }

    static func makeItemEvent(
        assetLocalIdentifier: String,
        assetFingerprint: Data?,
        displayName: String,
        resourceDate: Date?,
        status: BackupItemStatus,
        reason: String?,
        in monthPlan: MonthWorkItem,
        updatedAt: Date = Date()
    ) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: assetLocalIdentifier,
            assetFingerprint: assetFingerprint,
            month: monthPlan.month,
            displayName: displayName,
            resourceDate: resourceDate,
            status: status,
            reason: reason,
            updatedAt: updatedAt
        )
    }

    private static func progressForMonthPlan(
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        assetLocalIdentifier: String,
        resourceDate: Date?,
        monthPlan: MonthWorkItem,
        workerID: Int,
        monthText: String,
        updatedAt: Date = Date()
    ) -> BackupProgress {
        let displayMessage = Self.message(for: result, position: position, total: state.total)
        let logMessage = Self.logMessage(
            for: result,
            position: position,
            total: state.total,
            displayMessage: displayMessage,
            workerID: workerID,
            monthText: monthText
        )
        return BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: displayMessage,
            logMessage: logMessage,
            logLevel: Self.logLevel(for: result),
            itemEvent: Self.makeItemEvent(
                assetLocalIdentifier: assetLocalIdentifier,
                assetFingerprint: result.assetFingerprint,
                displayName: result.displayName,
                resourceDate: resourceDate,
                status: result.status,
                reason: result.reason,
                in: monthPlan,
                updatedAt: updatedAt
            ),
            transferState: nil
        )
    }

    static func emitItemProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        assetLocalIdentifier: String,
        resourceDate: Date?,
        monthPlan: MonthWorkItem,
        workerID: Int,
        monthText: String,
        updatedAt: Date = Date()
    ) {
        eventStream.emit(.progress(Self.progressForMonthPlan(
            state: state,
            result: result,
            position: position,
            assetLocalIdentifier: assetLocalIdentifier,
            resourceDate: resourceDate,
            monthPlan: monthPlan,
            workerID: workerID,
            monthText: monthText,
            updatedAt: updatedAt
        )))
    }

    private static func failureProgressForMonthPlan(
        state: BackupRunState,
        assetLocalIdentifier: String,
        resourceDate: Date?,
        monthPlan: MonthWorkItem,
        displayName: String,
        errorMessage: String,
        position: Int,
        workerID: Int,
        monthText: String,
        updatedAt: Date = Date()
    ) -> BackupProgress {
        let displayMessage = Self.failureMessage(
            position: position,
            total: state.total,
            displayName: displayName
        )
        return BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: displayMessage,
            logMessage: displayMessage + Self.contextSuffix(workerID: workerID, monthText: monthText),
            logLevel: .error,
            itemEvent: Self.makeItemEvent(
                assetLocalIdentifier: assetLocalIdentifier,
                assetFingerprint: nil,
                displayName: displayName,
                resourceDate: resourceDate,
                status: .failed,
                reason: errorMessage,
                in: monthPlan,
                updatedAt: updatedAt
            ),
            transferState: nil
        )
    }

    static func emitItemFailureProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        assetLocalIdentifier: String,
        resourceDate: Date?,
        monthPlan: MonthWorkItem,
        displayName: String,
        errorMessage: String,
        position: Int,
        workerID: Int,
        monthText: String,
        updatedAt: Date = Date()
    ) {
        eventStream.emit(.progress(Self.failureProgressForMonthPlan(
            state: state,
            assetLocalIdentifier: assetLocalIdentifier,
            resourceDate: resourceDate,
            monthPlan: monthPlan,
            displayName: displayName,
            errorMessage: errorMessage,
            position: position,
            workerID: workerID,
            monthText: monthText,
            updatedAt: updatedAt
        )))
    }

    enum MonthCompletionDisposition: Equatable {
        case finish
        case paused
        case stoppedByRunFatal
        case ownFatal
    }

    enum ManifestFlushFailureDisposition: Equatable {
        case recordMonthFailure
        case continueWithoutFinishing
        case deferToOwnFatal
        case throwFlushError
    }

    static func monthCompletionDisposition(
        paused: Bool,
        hasMonthFatalError: Bool,
        queueStopped: Bool
    ) -> MonthCompletionDisposition {
        if hasMonthFatalError { return .ownFatal }
        if paused { return .paused }
        if queueStopped { return .stoppedByRunFatal }
        return .finish
    }

    static func monthCompletionDisposition(
        paused: Bool,
        monthFatalError: Error?,
        monthQueue: MonthWorkQueue
    ) async -> MonthCompletionDisposition {
        let queueStopped = monthFatalError == nil ? await monthQueue.isStopped() : false
        return monthCompletionDisposition(
            paused: paused,
            hasMonthFatalError: monthFatalError != nil,
            queueStopped: queueStopped
        )
    }

    static func shouldRecordManifestReadBackFailure(
        disposition: MonthCompletionDisposition,
        error: Error
    ) -> Bool {
        disposition == .finish && shouldContinueAfterManifestFlushFailure(error)
    }

    static func manifestFlushFailureDisposition(
        completion: MonthCompletionDisposition,
        error: Error
    ) -> ManifestFlushFailureDisposition {
        if completion == .ownFatal { return .deferToOwnFatal }
        guard shouldContinueAfterManifestFlushFailure(error) else {
            return completion == .stoppedByRunFatal ? .continueWithoutFinishing : .throwFlushError
        }
        return completion == .finish ? .recordMonthFailure : .continueWithoutFinishing
    }

    // Non-throwing flush-failure exits leave the month uncommitted, so they must drop its dirtied assets from
    // the resume set via `.uploadFailed`: the run can still settle `.paused` through `.finished(paused:)` (a
    // sibling's clean reroute), which never reaches applyRunError's clear. `.recordMonthFailure` reports them
    // failed (count); throwing exits emit nothing (applyRunError clears those). nil = no emit.
    static func flushFailureResumeUnmarkCount(
        _ disposition: ManifestFlushFailureDisposition,
        dirtyAssetCount: Int
    ) -> Int? {
        switch disposition {
        case .recordMonthFailure:
            return dirtyAssetCount
        case .continueWithoutFinishing:
            return 0
        case .deferToOwnFatal, .throwFlushError:
            return nil
        }
    }

    func execute(
        preparedRun: BackupPreparedRun,
        profile: ServerProfileRecord,
        workerCountOverride: Int?,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        eventStream: BackupEventStream,
        incrementalFlushInterval: Int? = nil,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) async throws -> BackupExecutionResult {
        // The Lite write lease must be released while the client backing WriteLockService is still
        // connected — real backends reject lock deletion after disconnect. So every termination path
        // (success, zero-asset, execution error, cancellation, pause) releases *before* it disconnects
        // the initial client or shuts the pool down, never after.
        guard preparedRun.totalAssetCount > 0 else {
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            await preparedRun.writeMode.stopAndRelease()
            await preparedRun.initialClient.disconnectSafely()
            eventStream.emit(.finished(result))
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
                            writeMode: preparedRun.writeMode,
                            eventStream: eventStream,
                            aggregator: aggregator,
                            clientPool: clientPool,
                            incrementalFlushInterval: incrementalFlushInterval,
                            onMonthUploaded: onMonthUploaded
                        )
                    }
                }

                for try await workerState in group where workerState.paused {
                    await aggregator.markPaused()
                }
            }
        } catch {
            // Release the lease while the pool (and its seeded initial client) is still connected.
            await preparedRun.writeMode.stopAndRelease()
            await clientPool.shutdown()
            if Self.isNetworkUnavailableFatal(error, profile: profile) {
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

        // Release the lease while the pool (and its seeded initial client) is still connected.
        await preparedRun.writeMode.stopAndRelease()
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

    // Thin recovery policy: backoff + fault classification; the connection lifecycle is the pool's. `deadline` is
    // cumulative across recover→retry cycles (reset only on real progress), so a link that reconnects but never
    // completes an operation still exhausts and pauses instead of looping forever.
    static func recoverWorkerConnection(
        broken: any RemoteStorageClientProtocol,
        monthStore: MonthManifestStore?,
        deadline: Date,
        clientPool: StorageClientPool,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream,
        workerID: Int,
        monthText: String,
        error: Error
    ) async -> RecoveryOutcome {
        if profile.isBrowserLinkProfile { return .failed(error) }
        await clientPool.retireForReplacement(broken)
        var delayNanos = NetworkRecoveryPolicy.backoffBaseNanos
        let maxDelayNanos = NetworkRecoveryPolicy.backoffCapNanos
        var attempt = 0
        while Date() < deadline {
            if Task.isCancelled { return .cancelled }
            if await monthQueue.isStopped() { return .stopped }
            attempt += 1
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.networkRecovering"),
                    workerID + 1,
                    monthText,
                    attempt,
                    profile.userFacingStorageErrorMessage(error)
                ),
                level: .warning
            )
            let remainingNanos = UInt64(max(0, deadline.timeIntervalSinceNow) * 1_000_000_000)
            let jitterNanos = UInt64.random(in: 0 ... max(1, delayNanos / 2))
            do {
                try await Task.sleep(nanoseconds: min(delayNanos + jitterNanos, remainingNanos))
            } catch {
                return .cancelled
            }
            if Task.isCancelled { return .cancelled }
            if await monthQueue.isStopped() { return .stopped }
            if Date() >= deadline { break }
            // Per-attempt connectTimeout cap (the cumulative `deadline` stays the outer bound): a half-open
            // reconnect is abandoned at connectTimeout so this loop can retry a fresh one, instead of one hung
            // connect consuming the whole recovery window.
            let attemptDeadline = min(deadline, Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout))
            switch await clientPool.connectReplacement(by: attemptDeadline, abortIf: { await monthQueue.isStopped() }) {
            case .connected(let fresh):
                // A sibling may have stopped the queue while connect ran — don't resume on a connection
                // nobody will use.
                if Task.isCancelled { await fresh.disconnectSafely(); return .cancelled }
                if await monthQueue.isStopped() { await fresh.disconnectSafely(); return .stopped }
                monthStore?.replaceClient(fresh)
                eventStream.emitLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.parallel.networkRecovered"),
                        workerID + 1,
                        monthText
                    ),
                    level: .info
                )
                return .recovered(fresh)
            case .failed(let connectError):
                // Classify the reconnect fault, not Task.isCancelled. A terminal fault (auth/config/cert) — or a
                // not-found, which on a reconnect probe means a wrong endpoint/base path, not a transient blip —
                // fails the run; only a genuine transient keeps backing off.
                switch RemoteFaultLite.classify(connectError) {
                case .cancelled:
                    return .cancelled
                case .terminal, .notFound:
                    return .failed(connectError)
                case .retryable:
                    delayNanos = min(delayNanos * 2, maxDelayNanos)
                }
            case .timedOut:
                break   // deadline preempted the connect; the loop top re-evaluates and exits to .exhausted
            }
        }
        // A stop/cancel landing exactly as the deadline expires must defer to it, not mask a sibling's real
        // fatal (or a user stop) as a resumable network-exhaustion pause.
        if Task.isCancelled { return .cancelled }
        if await monthQueue.isStopped() { return .stopped }
        eventStream.emitLog(
            String.localizedStringWithFormat(
                String(localized: "backup.parallel.networkRecoveryExhausted"),
                workerID + 1,
                monthText
            ),
            level: .error
        )
        return .exhausted
    }

    // Initial worker-client acquire with the same bounded recovery as mid-run reconnects: a transient fault while
    // establishing the connection (common when workers 2..N connect in parallel) rides out the window instead of
    // failing the whole run; a sustained outage stops the queue and pauses (resumable); a terminal or not-found
    // endpoint fault fails fast.
    static func acquireWorkerClient(
        clientPool: StorageClientPool,
        deadline: Date,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream,
        workerID: Int
    ) async throws -> (any RemoteStorageClientProtocol)? {
        let result: NetworkRecoveryResult<any RemoteStorageClientProtocol> = await NetworkRecovery.run(
            deadline: deadline,
            shouldStop: { await monthQueue.isStopped() }
        ) {
            // Per-attempt connectTimeout cap (the cumulative `deadline` is still the outer `run` bound): a
            // half-open initial connect is abandoned at connectTimeout so `run` retries a fresh one, instead of
            // one hung connect consuming the whole recovery window.
            let attemptDeadline = min(deadline, Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout))
            switch await clientPool.acquire(by: attemptDeadline, abortIf: { await monthQueue.isStopped() }) {
            case .connected(let client):
                return .succeeded(client)
            case .timedOut:
                return .abandoned   // deadline/abort preempted the connect — driver re-evaluates to exhausted/stopped
            case .failed(let error):
                eventStream.emitLog(
                    String.localizedStringWithFormat(
                        String(localized: "backup.parallel.clientAcquireFailed"),
                        workerID + 1,
                        profile.userFacingStorageErrorMessage(error)
                    ),
                    level: .warning
                )
                return .failed(error)
            }
        }
        switch result {
        case .succeeded(let client):
            return client
        case .failed(let error):
            throw error
        case .exhausted(let error):
            await monthQueue.stop()
            throw BackupNetworkRecoveryExhausted(underlying: error)
        case .cancelled:
            throw CancellationError()
        case .stopped:
            // A sibling stopped the queue and carries the run's real outcome. Defer (return nil) instead of
            // throwing a competing error, so this worker can mask neither the sibling's fatal nor its pause.
            return nil
        }
    }

    private func runParallelMonthWorker(
        workerID: Int,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        snapshotSeedLookup: MonthSeedLookup?,
        writeMode: RepoWriteMode,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        clientPool: StorageClientPool,
        incrementalFlushInterval: Int? = nil,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) async throws -> WorkerRunState {
        var workerState = WorkerRunState()
        // Background bounds recovery tightly (BG-task grace); foreground stays within the lease expiry window.
        let recoveryWindow = NetworkRecoveryPolicy.window(background: incrementalFlushInterval != nil)
        var client: any RemoteStorageClientProtocol
        do {
            guard let acquired = try await Self.acquireWorkerClient(
                clientPool: clientPool,
                deadline: Date().addingTimeInterval(recoveryWindow),
                monthQueue: monthQueue,
                profile: profile,
                eventStream: eventStream,
                workerID: workerID
            ) else {
                // A sibling stopped the queue while we were still acquiring; defer to its outcome (it threw the
                // run's real fatal/pause) and return clean without competing. No client/slot was taken.
                workerState.paused = true
                return workerState
            }
            client = acquired
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
        // Cumulative recovery deadline: set on the first reconnect, cleared on any forward progress. Bounds
        // total time spent recovering *without* a successful operation, so a flapping link still pauses.
        var recoveryDeadline: Date?
        do {
            monthLoop: while let monthPlan = await monthQueue.next() {
                if Task.isCancelled {
                    workerState.paused = true
                    break
                }

                let monthKey = monthPlan.month
                var loadedMonthStore: MonthManifestStore?
                loadRecovery: while loadedMonthStore == nil {
                    do {
                        loadedMonthStore = try await MonthManifestStore.loadOrCreate(
                            client: client,
                            basePath: profile.basePath,
                            year: monthKey.year,
                            month: monthKey.month,
                            seed: snapshotSeedLookup?.seed(for: monthKey),
                            layout: writeMode.manifestLayout,
                            stepLogger: { message in
                                eventStream.emitLog(message, level: .error)
                            },
                            // Read-only lease gate: parallel workers must never write the lock (concurrent
                            // reclaims corrupt it). The session's refresh task is the sole lock writer.
                            assertOwnership: writeMode.leaseProvenAssertion,
                            liteMonthsListing: writeMode.liteMonthsListing
                        )
                    } catch {
                        if error is CancellationError {
                            workerState.paused = true
                            break loadRecovery
                        }
                        if AssetProcessor.isRecoverableNetworkFault(error, profile: profile) {
                            if recoveryDeadline == nil { recoveryDeadline = Date().addingTimeInterval(recoveryWindow) }
                            switch await Self.recoverWorkerConnection(
                                broken: client, monthStore: nil, deadline: recoveryDeadline!,
                                clientPool: clientPool, monthQueue: monthQueue, profile: profile,
                                eventStream: eventStream, workerID: workerID, monthText: monthKey.text, error: error
                            ) {
                            case .recovered(let fresh):
                                client = fresh
                                continue loadRecovery
                            case .cancelled:
                                clientReusable = false
                                workerState.paused = true
                                break loadRecovery
                            case .stopped:
                                clientReusable = false
                                break loadRecovery
                            case .failed(let terminal):
                                clientReusable = false
                                await monthQueue.stop()
                                throw terminal
                            case .exhausted:
                                clientReusable = false
                                await monthQueue.stop()
                                throw BackupNetworkRecoveryExhausted(underlying: error)
                            }
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
                }
                guard let monthStore = loadedMonthStore else {
                    break
                }
                recoveryDeadline = nil   // load succeeded

                // loadOrCreate may have cleaned manifest rows; sync to snapshotCache so consumers don't see stale state.
                // Mutable: an incremental flush durably commits progress, so the rollback baseline advances to it.
                var loadedSnapshot = monthStore.unsortedSnapshot()
                remoteIndexService.replaceCachedMonth(
                    monthKey,
                    resources: loadedSnapshot.resources,
                    assets: loadedSnapshot.assets,
                    links: loadedSnapshot.links,
                    expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile)
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
                var shouldStopAssetProcessing = false
                let monthAssetIDs = monthPlan.assetLocalIdentifiers
                let fetchBatchSize = 500
                var missingAssetCount = 0
                var hasLoggedLocalHashCacheWarning = false
                var monthProgressCounts = BackupMonthProgressCounts()
                // Asset IDs whose results wrote (still-uncommitted) month-manifest rows — `.success` and
                // reused-resource `.skipped` alike — so a failed month-end flush can un-mark them as resume-complete.
                var monthDirtyAssetIDs = Set<String>()
                // Background bounds the at-risk window: checkpoint-flush every N uploads so a BG-task expiration
                // (cancel + short grace window) never strands a whole large month's manifest. nil = foreground.
                var uploadsSinceIncrementalFlush = 0

                var skippedMonthShortCircuit = false
                if monthAlreadyFullyBackedUp(
                    monthAssetIDs: monthAssetIDs,
                    monthStore: monthStore
                ) {
                    let progressState = await aggregator.recordMonthSkipped(count: monthAssetIDs.count)
                    if let transferState = Self.skippedMonthTransferState(
                        monthKey: monthKey,
                        workerID: workerID + 1,
                        skippedAssetCount: monthAssetIDs.count,
                        estimatedBytes: monthPlan.estimatedBytes
                    ) {
                        eventStream.emit(.transferState(transferState))
                    }
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
                    monthProgressCounts.skipped += monthAssetIDs.count
                    skippedMonthShortCircuit = true
                }

                if !skippedMonthShortCircuit {
                for batchStart in stride(from: 0, to: monthAssetIDs.count, by: fetchBatchSize) {
                    if await monthQueue.isStopped() {
                        shouldStopAssetProcessing = true
                        break
                    }
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
                        if await monthQueue.isStopped() {
                            shouldStopAssetProcessing = true
                            break
                        }
                        if Task.isCancelled {
                            workerState.paused = true
                            break
                        }

                        await BackupThermalThrottle.shared.waitIfNeeded()
                        if Task.isCancelled {
                            workerState.paused = true
                            break
                        }

                        let cachedLocalHash = batchLocalHashCacheByAssetID.removeValue(forKey: assetID)
                        guard let asset = batchAssetsByLocalIdentifier.removeValue(forKey: assetID) else {
                            if let transferState = Self.estimatedAssetTransferState(
                                assetLocalIdentifier: assetID,
                                displayName: String(assetID.prefix(12)),
                                totalBytes: cachedLocalHash?.totalFileSizeBytes ?? 0,
                                workerID: workerID + 1,
                                assetPosition: 1,
                                totalAssets: monthAssetIDs.count
                            ) {
                                eventStream.emit(.transferState(transferState))
                            }
                            await aggregator.reduceTotalForEmptyAsset()
                            continue
                        }

                        let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                            from: PHAssetResource.assetResources(for: asset)
                        )
                        if selectedResources.isEmpty {
                            if let transferState = Self.estimatedAssetTransferState(
                                assetLocalIdentifier: asset.localIdentifier,
                                displayName: BackupAssetResourcePlanner.assetDisplayName(
                                    asset: asset,
                                    selectedResources: []
                                ),
                                totalBytes: cachedLocalHash?.totalFileSizeBytes ?? 0,
                                workerID: workerID + 1,
                                assetPosition: 1,
                                totalAssets: monthAssetIDs.count
                            ) {
                                eventStream.emit(.transferState(transferState))
                            }
                            await aggregator.reduceTotalForEmptyAsset()
                            continue
                        }

                        let dispatch = await aggregator.allocateDispatchSlot()

                        var processResult: AssetProcessResult?
                        var assetFailureHandled = false
                        processRetry: while true {
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
                                    totalAssets: dispatch.total,
                                    writeMode: writeMode
                                )
                                processResult = try await assetProcessor.process(
                                    context: context,
                                    client: client,
                                    eventStream: eventStream,
                                    cancellationController: nil
                                )
                                break processRetry
                            } catch {
                                if error is CancellationError {
                                    workerState.paused = true
                                    break processRetry
                                }
                                if AssetProcessor.isLeaseFailFast(error) {
                                    eventStream.emitLog(
                                        Self.fatalUploadStopLog(
                                            workerID: workerID + 1,
                                            monthText: monthKey.text,
                                            source: "asset-processing",
                                            error: error,
                                            profile: profile
                                        ),
                                        level: .error
                                    )
                                    await monthQueue.stop()
                                    monthFatalError = error
                                    break processRetry
                                }
                                if AssetProcessor.isRecoverableNetworkFault(error, profile: profile) {
                                    if recoveryDeadline == nil { recoveryDeadline = Date().addingTimeInterval(recoveryWindow) }
                                    switch await Self.recoverWorkerConnection(
                                        broken: client, monthStore: monthStore, deadline: recoveryDeadline!,
                                        clientPool: clientPool, monthQueue: monthQueue, profile: profile,
                                        eventStream: eventStream, workerID: workerID, monthText: monthKey.text, error: error
                                    ) {
                                    case .recovered(let fresh):
                                        client = fresh
                                        continue processRetry
                                    case .cancelled:
                                        clientReusable = false
                                        workerState.paused = true
                                    case .stopped:
                                        clientReusable = false
                                        shouldStopAssetProcessing = true
                                    case .failed(let terminal):
                                        clientReusable = false
                                        await monthQueue.stop()
                                        monthFatalError = terminal
                                    case .exhausted:
                                        clientReusable = false
                                        await monthQueue.stop()
                                        monthFatalError = BackupNetworkRecoveryExhausted(underlying: error)
                                        eventStream.emitLog(
                                            Self.fatalUploadStopLog(
                                                workerID: workerID + 1,
                                                monthText: monthKey.text,
                                                source: "network-recovery-exhausted",
                                                error: error,
                                                profile: profile
                                            ),
                                            level: .error
                                        )
                                    }
                                    break processRetry
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
                                    break processRetry
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
                                monthProgressCounts.failed += 1
                                let failureTransferStates = Self.failedAssetTransferStates(
                                    asset: asset,
                                    selectedResources: selectedResources,
                                    workerID: workerID + 1,
                                    assetPosition: dispatch.position,
                                    totalAssets: dispatch.total,
                                    displayName: displayName
                                )
                                if failureTransferStates.isEmpty,
                                   let transferState = Self.estimatedAssetTransferState(
                                    assetLocalIdentifier: asset.localIdentifier,
                                    displayName: displayName,
                                    totalBytes: cachedLocalHash?.totalFileSizeBytes ?? 0,
                                    workerID: workerID + 1,
                                    assetPosition: dispatch.position,
                                    totalAssets: dispatch.total
                                   ) {
                                    eventStream.emit(.transferState(transferState))
                                } else {
                                    for transferState in failureTransferStates {
                                        eventStream.emit(.transferState(transferState))
                                    }
                                }
                                Self.emitItemFailureProgress(
                                    eventStream: eventStream,
                                    state: progressState.state,
                                    assetLocalIdentifier: asset.localIdentifier,
                                    resourceDate: asset.creationDate,
                                    monthPlan: monthPlan,
                                    displayName: displayName,
                                    errorMessage: errorMessage,
                                    position: progressState.position,
                                    workerID: workerID + 1,
                                    monthText: monthKey.text
                                )
                                if let timingSummary = progressState.timingSummary {
                                    eventStream.emitLog(timingSummary, level: .debug)
                                }
                                assetFailureHandled = true
                                break processRetry
                            }
                        }

                        if workerState.paused || monthFatalError != nil || shouldStopAssetProcessing {
                            break
                        }
                        if assetFailureHandled {
                            continue
                        }
                        guard let result = processResult else {
                            continue
                        }
                        recoveryDeadline = nil   // asset processed

                        let progressState = await aggregator.record(result: result)
                        monthProgressCounts.record(result.status)
                        if Self.resultDirtiedMonthManifest(status: result.status, reason: result.reason) {
                            monthDirtyAssetIDs.insert(asset.localIdentifier)
                        }
                        if Self.shouldEmitResultCredit(result),
                           let transferState = Self.estimatedAssetTransferState(
                            assetLocalIdentifier: asset.localIdentifier,
                            displayName: result.displayName,
                            totalBytes: result.totalFileSizeBytes > 0
                                ? result.totalFileSizeBytes
                                : (cachedLocalHash?.totalFileSizeBytes ?? 0),
                            workerID: workerID + 1,
                            assetPosition: dispatch.position,
                            totalAssets: dispatch.total
                           ) {
                            eventStream.emit(.transferState(transferState))
                        }

                        Self.emitItemProgress(
                            eventStream: eventStream,
                            state: progressState.state,
                            result: result,
                            position: progressState.position,
                            assetLocalIdentifier: asset.localIdentifier,
                            resourceDate: asset.creationDate,
                            monthPlan: monthPlan,
                            workerID: workerID + 1,
                            monthText: monthKey.text
                        )
                        if let timingSummary = progressState.timingSummary {
                            eventStream.emitLog(timingSummary, level: .debug)
                        }

                        if result.status == .success, let incrementalFlushInterval {
                            uploadsSinceIncrementalFlush += 1
                            if uploadsSinceIncrementalFlush >= incrementalFlushInterval {
                                uploadsSinceIncrementalFlush = 0
                                incrementalFlush: while true {
                                    do {
                                        // Re-asserts the run's write lease inside flushToRemote (store-owned gate).
                                        try await monthStore.flushToRemote(ignoreCancellation: true)
                                        // Progress is now durable; advance the rollback baseline so a later
                                        // failure can't revert the cache below what was committed here, and bank
                                        // the committed segment: a later final-flush failure must only un-mark /
                                        // count assets uploaded SINCE this checkpoint, not these.
                                        loadedSnapshot = monthStore.unsortedSnapshot()
                                        monthDirtyAssetIDs.removeAll(keepingCapacity: true)
                                        monthProgressCounts = BackupMonthProgressCounts()
                                        recoveryDeadline = nil   // incremental flush committed
                                        break incrementalFlush
                                    } catch {
                                        // Lease loss must stop fast (don't keep writing data we no longer own).
                                        if AssetProcessor.isLeaseFailFast(error) {
                                            eventStream.emitLog(
                                                Self.fatalUploadStopLog(
                                                    workerID: workerID + 1,
                                                    monthText: monthKey.text,
                                                    source: "incremental-manifest-flush",
                                                    error: error,
                                                    profile: profile
                                                ),
                                                level: .error
                                            )
                                            await monthQueue.stop()
                                            monthFatalError = error
                                            break incrementalFlush
                                        }
                                        // Recoverable network fault: reconnect and retry the flush so just-uploaded
                                        // files are never stranded out of the manifest (orphans / collisions next run).
                                        if AssetProcessor.isRecoverableNetworkFault(error, profile: profile) {
                                            if recoveryDeadline == nil { recoveryDeadline = Date().addingTimeInterval(recoveryWindow) }
                                            switch await Self.recoverWorkerConnection(
                                                broken: client, monthStore: monthStore, deadline: recoveryDeadline!,
                                                clientPool: clientPool, monthQueue: monthQueue, profile: profile,
                                                eventStream: eventStream, workerID: workerID, monthText: monthKey.text, error: error
                                            ) {
                                            case .recovered(let fresh):
                                                client = fresh
                                                continue incrementalFlush
                                            case .cancelled:
                                                clientReusable = false
                                                workerState.paused = true
                                                break incrementalFlush
                                            case .stopped:
                                                clientReusable = false
                                                shouldStopAssetProcessing = true
                                                break incrementalFlush
                                            case .failed(let terminal):
                                                clientReusable = false
                                                await monthQueue.stop()
                                                monthFatalError = terminal
                                                break incrementalFlush
                                            case .exhausted:
                                                clientReusable = false
                                                await monthQueue.stop()
                                                monthFatalError = BackupNetworkRecoveryExhausted(underlying: error)
                                                break incrementalFlush
                                            }
                                        }
                                        if profile.isConnectionUnavailableError(error) {
                                            eventStream.emitLog(
                                                Self.fatalUploadStopLog(
                                                    workerID: workerID + 1,
                                                    monthText: monthKey.text,
                                                    source: "incremental-manifest-flush",
                                                    error: error,
                                                    profile: profile
                                                ),
                                                level: .error
                                            )
                                            clientReusable = false
                                            await monthQueue.stop()
                                            monthFatalError = error
                                            break incrementalFlush
                                        }
                                        // Other transient faults: retry at month-end flush.
                                        eventStream.emitLog(
                                            String.localizedStringWithFormat(
                                                String(localized: "backup.parallel.flushManifestFailed"),
                                                workerID + 1,
                                                monthKey.text,
                                                profile.userFacingStorageErrorMessage(error)
                                            ),
                                            level: .warning
                                        )
                                        break incrementalFlush
                                    }
                                }
                                if workerState.paused || monthFatalError != nil || shouldStopAssetProcessing {
                                    break
                                }
                            }
                        }
                    }

                    if workerState.paused || monthFatalError != nil || shouldStopAssetProcessing {
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

                let finalizeState = MonthFinalizeState(
                    client: client,
                    clientReusable: clientReusable,
                    recoveryDeadline: recoveryDeadline,
                    run: workerState,
                    monthFatalError: monthFatalError
                )
                let finalizeDisposition = await finalizeMonth(
                    monthStore: monthStore,
                    monthKey: monthKey,
                    loadedSnapshot: loadedSnapshot,
                    monthDirtyAssetIDs: monthDirtyAssetIDs,
                    monthProgressCounts: monthProgressCounts,
                    incrementalFlushInterval: incrementalFlushInterval,
                    recoveryWindow: recoveryWindow,
                    writeMode: writeMode,
                    clientPool: clientPool,
                    monthQueue: monthQueue,
                    profile: profile,
                    eventStream: eventStream,
                    aggregator: aggregator,
                    onMonthUploaded: onMonthUploaded,
                    workerID: workerID,
                    state: finalizeState
                )
                client = finalizeState.client
                clientReusable = finalizeState.clientReusable
                recoveryDeadline = finalizeState.recoveryDeadline
                workerState = finalizeState.run
                monthFatalError = finalizeState.monthFatalError

                switch finalizeDisposition {
                case .breakMonthLoop:
                    break monthLoop
                case .throwError(let error):
                    throw error
                case .proceed:
                    if let monthFatalError {
                        throw monthFatalError
                    }
                    if workerState.paused {
                        break monthLoop
                    }
                    if await monthQueue.isStopped() {
                        break monthLoop
                    }
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

    // Commits the month manifest with bounded network recovery, then resolves the month disposition.
    func finalizeMonth(
        monthStore: MonthManifestStore,
        monthKey: LibraryMonthKey,
        loadedSnapshot: (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]),
        monthDirtyAssetIDs: Set<String>,
        monthProgressCounts: BackupMonthProgressCounts,
        incrementalFlushInterval: Int?,
        recoveryWindow: TimeInterval,
        writeMode: RepoWriteMode,
        clientPool: StorageClientPool,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        onMonthUploaded: BackupMonthFinalizer?,
        workerID: Int,
        state: MonthFinalizeState
    ) async -> MonthFinalizeDisposition {
        let hadDirtyManifestBeforeFinalize = monthStore.dirty
        // A month already fatal skips the flush and rolls back — never re-enter recovery for a flush we know fails.
        let skipFlushDueToMonthFatal = state.monthFatalError != nil
        var readBackVerificationFailed = false
        var flushSucceeded = false

        if skipFlushDueToMonthFatal {
            // The month did not commit, so roll back optimistic cache mutations to the last
            // committed month state.
            remoteIndexService.replaceCachedMonth(
                monthKey,
                resources: loadedSnapshot.resources,
                assets: loadedSnapshot.assets,
                links: loadedSnapshot.links,
                expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile)
            )
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.parallel.skipManifestFlush"),
                    workerID + 1,
                    monthKey.text
                ),
                level: .error
            )
            // Pause/stop coinciding with the connection loss leaves this month uncommitted; un-mark its
            // dirtied assets and pause cleanly (reliable) so resume reprocesses them instead of skipping.
            if Self.shouldPauseUncommittedMonthOnCancellation(
                cancelled: Task.isCancelled,
                hadDirtyManifest: hadDirtyManifestBeforeFinalize
            ) {
                state.run.paused = true
                emitMonthUploadFailed(
                    eventStream: eventStream,
                    monthKey: monthKey,
                    assetIDs: monthDirtyAssetIDs,
                    failedItemCount: 0
                )
                return .breakMonthLoop
            }
        } else {
            monthEndFlush: while true {
            do {
                // Dirty Lite manifest flush re-asserts the run's write lease inside flushToRemote
                // (store-owned gate) and fails closed if lost, so a foreign writer is never overwritten.
                // Background (incrementalFlushInterval set) also ignores cancellation here: a BG-task
                // expiration landing after the last asset but before the next cancellation check must
                // still commit the dirty manifest, else just-uploaded files are left out of it.
                let ignoreCancellation = state.run.paused || incrementalFlushInterval != nil
                try await monthStore.flushToRemote(ignoreCancellation: ignoreCancellation)
                flushSucceeded = true
                state.recoveryDeadline = nil   // month committed
                break monthEndFlush
            } catch {
                // A cancellation-shaped reconnect fault (e.g. server RST → NSURLErrorCancelled) returns
                // .cancelled while Task.isCancelled is false; it must still pause-clean like a real cancel.
                var recoveryCancelled = false
                // Recoverable network fault at commit time: reconnect + retry so the whole month's
                // uploads are not stranded as orphans (→ collision downloads next run).
                if AssetProcessor.isRecoverableNetworkFault(error, profile: profile) {
                    if state.recoveryDeadline == nil { state.recoveryDeadline = Date().addingTimeInterval(recoveryWindow) }
                    switch await Self.recoverWorkerConnection(
                        broken: state.client, monthStore: monthStore, deadline: state.recoveryDeadline!,
                        clientPool: clientPool, monthQueue: monthQueue, profile: profile,
                        eventStream: eventStream, workerID: workerID, monthText: monthKey.text, error: error
                    ) {
                    case .recovered(let fresh):
                        state.client = fresh
                        continue monthEndFlush
                    case .cancelled:
                        state.clientReusable = false
                        state.run.paused = true
                        recoveryCancelled = true
                    case .stopped:
                        state.clientReusable = false
                        break   // defer to the sibling's fatal: fall through to .stoppedByRunFatal
                    case .failed(let terminal):
                        state.clientReusable = false
                        await monthQueue.stop()
                        state.monthFatalError = terminal
                    case .exhausted:
                        // Match the other sites: stop the queue + sentinel-fatal so siblings tear down
                        // and the run settles a resumable pause (rethrows at the monthFatalError check).
                        state.clientReusable = false
                        await monthQueue.stop()
                        state.monthFatalError = BackupNetworkRecoveryExhausted(underlying: error)
                    }
                }
                // Flush failed: roll back optimistic cache mutations so the cache
                // reflects the last committed month state, not uncommitted upserts.
                remoteIndexService.replaceCachedMonth(
                    monthKey,
                    resources: loadedSnapshot.resources,
                    assets: loadedSnapshot.assets,
                    links: loadedSnapshot.links,
                    expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile)
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
                // User cancel — or a cancellation-shaped reconnect fault — during the commit phase: un-mark
                // this month's uncommitted assets so resume reprocesses them, and pause cleanly instead of
                // throwing (which would leave them resume-complete). Network exhaustion takes the
                // sentinel-fatal path above, not this one.
                if Self.shouldPauseUncommittedMonthOnCancellation(
                    cancelled: Task.isCancelled || recoveryCancelled,
                    hadDirtyManifest: hadDirtyManifestBeforeFinalize
                ) {
                    state.run.paused = true
                    emitMonthUploadFailed(
                        eventStream: eventStream,
                        monthKey: monthKey,
                        assetIDs: monthDirtyAssetIDs,
                        failedItemCount: 0
                    )
                    return .breakMonthLoop
                }
                let disposition = await Self.monthCompletionDisposition(
                    paused: state.run.paused,
                    monthFatalError: state.monthFatalError,
                    monthQueue: monthQueue
                )
                let failureDisposition = Self.manifestFlushFailureDisposition(
                    completion: disposition,
                    error: error
                )
                if failureDisposition == .recordMonthFailure {
                    // Finishing run, month uncommitted: convert this month's successes AND its reused-resource
                    // skips (which also wrote now-uncommitted manifest rows) to failures, so it is reported
                    // failed, not completed — matching the assets it un-marks for resume below.
                    let dirtiedSkippedCount = max(0, monthDirtyAssetIDs.count - monthProgressCounts.succeeded)
                    let progressState = await aggregator.recordFinalizationFailure(
                        monthProgressCounts,
                        dirtiedSkippedCount: dirtiedSkippedCount
                    )
                    if let timingSummary = progressState.timingSummary {
                        eventStream.emitLog(timingSummary, level: .debug)
                    }
                    readBackVerificationFailed = true
                }
                if let unmarkFailedItemCount = Self.flushFailureResumeUnmarkCount(
                    failureDisposition,
                    dirtyAssetCount: monthDirtyAssetIDs.count
                ) {
                    emitMonthUploadFailed(
                        eventStream: eventStream,
                        monthKey: monthKey,
                        assetIDs: monthDirtyAssetIDs,
                        failedItemCount: unmarkFailedItemCount
                    )
                }
                if failureDisposition == .throwFlushError {
                    return .throwError(error)
                }
                break monthEndFlush
            }
            }
            if !readBackVerificationFailed {
                let disposition = await Self.monthCompletionDisposition(
                    paused: state.run.paused,
                    monthFatalError: state.monthFatalError,
                    monthQueue: monthQueue
                )
                switch disposition {
                case .finish:
                    if let onMonthUploaded {
                        let uploadContext = BackupMonthUploadContext(
                            writeMode: writeMode
                        )
                        switch await onMonthUploaded(monthKey, uploadContext) {
                        case .success:
                            eventStream.emit(.monthChanged(MonthChangeEvent(
                                year: monthKey.year,
                                month: monthKey.month,
                                action: .completed
                            )))
                        case .failed(let message):
                            // The upload manifest already committed here; only the inline download leg failed, and
                            // this path doesn't un-mark assets — so don't convert reused-skips (they're durable).
                            let progressState = await aggregator.recordFinalizationFailure(
                                monthProgressCounts
                            )
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.finalizationFailed"),
                                    workerID + 1,
                                    monthKey.text,
                                    message
                                ),
                                level: .error
                            )
                            if let timingSummary = progressState.timingSummary {
                                eventStream.emitLog(timingSummary, level: .debug)
                            }
                        case .fatal(let message, let error):
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.finalizationFailed"),
                                    workerID + 1,
                                    monthKey.text,
                                    message
                                ),
                                level: .error
                            )
                            return .throwError(error)
                        case .cancelled:
                            state.run.paused = true
                            eventStream.emitLog(
                                String.localizedStringWithFormat(
                                    String(localized: "backup.parallel.finalizationCancelled"),
                                    workerID + 1,
                                    monthKey.text
                                ),
                                level: .info
                            )
                        }
                        if state.run.paused {
                            break
                        }
                    } else {
                        eventStream.emit(.monthChanged(MonthChangeEvent(
                            year: monthKey.year,
                            month: monthKey.month,
                            action: .completed
                        )))
                    }
                case .ownFatal:
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.parallel.monthFatalError"),
                            workerID + 1,
                            monthKey.text
                        ),
                        level: .error
                    )
                case .paused:
                    let pauseLog = hadDirtyManifestBeforeFinalize && flushSucceeded
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
                case .stoppedByRunFatal:
                    break
                }
            }
        }

        return .proceed
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

    private func emitMonthUploadFailed(
        eventStream: BackupEventStream,
        monthKey: LibraryMonthKey,
        assetIDs: Set<String>,
        failedItemCount: Int
    ) {
        guard !assetIDs.isEmpty || failedItemCount > 0 else { return }
        eventStream.emit(.monthChanged(MonthChangeEvent(
            year: monthKey.year,
            month: monthKey.month,
            action: .uploadFailed(
                resumableAssetLocalIdentifiers: assetIDs,
                failedItemCount: failedItemCount
            )
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

    private static func fatalUploadStopLog(
        workerID: Int,
        monthText: String,
        source: String,
        error: Error,
        profile: ServerProfileRecord
    ) -> String {
        let reason = profile.userFacingStorageErrorMessage(error)
        let prefix = AssetProcessor.isLeaseFailFast(error)
            ? "[WriteLock] stopping upload after fatal lease failure"
            : "[BackupUpload] stopping upload after connection failure"
        return "\(prefix): source=\(source), worker=#\(workerID), month=\(monthText), reason=\(reason), raw=\(String(reflecting: error))"
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
