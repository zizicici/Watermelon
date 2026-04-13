import Foundation

enum BackupSessionControlPhase {
    case idle
    case starting
    case resuming
    case pausing
    case stopping
}

enum BackupSessionNotificationDirective {
    case none
    case throttled
    case immediate
}

struct BackupSessionReductionOutcome {
    let shouldStop: Bool
    let notification: BackupSessionNotificationDirective
}

struct BackupSessionStartContext {
    let previousState: BackupSessionController.State
    let previousStatusText: String
}

struct BackupSessionResumeContext {
    let pausedMode: BackupRunMode
    let pausedDisplayMode: BackupRunMode
}

struct BackupSessionState {
    var controlPhase: BackupSessionControlPhase = .idle
    var currentRunMode: BackupRunMode = .full
    var lastPausedRunMode: BackupRunMode?
    var lastPausedDisplayRunMode: BackupRunMode?
    var isStartCommandInFlight = false
    var backupScopeSelection = BackupScopeSelection(
        selectedAssetIDs: nil,
        selectedAssetCount: 0,
        selectedEstimatedBytes: nil,
        totalAssetCount: 0,
        totalEstimatedBytes: nil
    )

    var state: BackupSessionController.State = .idle
    var statusText: String = "未开始"
    var succeeded: Int = 0
    var failed: Int = 0
    var skipped: Int = 0
    var total: Int = 0
    var completedAssetIDsForResume: Set<String> = []
    var startedMonths = Set<LibraryMonthKey>()
    var checkpointedMonths = Set<LibraryMonthKey>()
    var completedMonths = Set<LibraryMonthKey>()
    var processedCountByMonth: [LibraryMonthKey: Int] = [:]
    var failedCountByMonth: [LibraryMonthKey: Int] = [:]

    var canUpdateScopeSelection: Bool {
        guard controlPhase == .idle else { return false }
        switch state {
        case .running, .paused:
            return false
        default:
            return true
        }
    }

    func snapshot() -> BackupSessionController.Snapshot {
        BackupSessionController.Snapshot(
            state: state,
            controlPhase: controlPhase,
            statusText: statusText,
            succeeded: succeeded,
            failed: failed,
            skipped: skipped,
            total: total,
            startedMonths: startedMonths,
            checkpointedMonths: checkpointedMonths,
            completedMonths: completedMonths,
            processedCountByMonth: processedCountByMonth,
            failedCountByMonth: failedCountByMonth
        )
    }

    mutating func setScopeSelection(_ selection: BackupScopeSelection) {
        backupScopeSelection = selection
    }

    mutating func failForMissingConnection() {
        state = .failed
        statusText = "请先连接远端存储"
    }

    mutating func prepareForStart(mode: BackupRunMode) -> BackupSessionStartContext {
        let context = BackupSessionStartContext(previousState: state, previousStatusText: statusText)

        currentRunMode = mode
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil

        let shouldResetSessionItems =
            state == .idle ||
            state == .completed ||
            state == .failed ||
            (state == .stopped && !mode.isRetry)

        completedAssetIDsForResume.removeAll()
        if shouldResetSessionItems {
            startedMonths.removeAll()
            checkpointedMonths.removeAll()
            completedMonths.removeAll()
            processedCountByMonth.removeAll()
            failedCountByMonth.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        controlPhase = .starting
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
        isStartCommandInFlight = true
        return context
    }

    mutating func completeAcceptedStartLaunch() {
        isStartCommandInFlight = false
        controlPhase = .idle
    }

    mutating func restoreRejectedStart(using context: BackupSessionStartContext) {
        isStartCommandInFlight = false
        controlPhase = .idle
        currentRunMode = .full
        state = context.previousState
        statusText = context.previousStatusText
    }

    mutating func beginPauseRequest() {
        controlPhase = .pausing
        statusText = "正在暂停..."
    }

    mutating func transitionToPausedWithoutRun() {
        controlPhase = .idle
        state = .paused
        statusText = "备份已暂停"
    }

    mutating func beginStopRequest() {
        controlPhase = .stopping
        statusText = "正在停止..."
    }

    mutating func transitionToStoppedWithoutRun() {
        controlPhase = .idle
        state = .stopped
        statusText = "备份已停止"
    }

    mutating func resolveStartCancellation(mode: BackupRunMode) {
        let phaseBeforeCancel = controlPhase
        controlPhase = .idle
        isStartCommandInFlight = false

        let intent: BackupTerminationIntent = (phaseBeforeCancel == .stopping) ? .stop : .pause
        if intent == .stop {
            lastPausedRunMode = nil
            lastPausedDisplayRunMode = nil
            currentRunMode = .full
            state = .stopped
            statusText = "备份已停止"
        } else {
            lastPausedRunMode = mode
            lastPausedDisplayRunMode = mode
            currentRunMode = mode
            state = .paused
            statusText = "备份已暂停"
        }
    }

    mutating func prepareForResume() -> BackupSessionResumeContext {
        let pausedMode = lastPausedRunMode ?? .full
        let pausedDisplayMode = lastPausedDisplayRunMode ?? pausedMode
        state = .running
        controlPhase = .resuming
        currentRunMode = pausedDisplayMode
        statusText = "正在准备继续..."
        return BackupSessionResumeContext(pausedMode: pausedMode, pausedDisplayMode: pausedDisplayMode)
    }

    mutating func completeResumeWithoutPendingWork() {
        controlPhase = .idle
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .completed
        statusText = "备份完成"
    }

    mutating func completeResumeLaunchSucceeded(displayMode: BackupRunMode) {
        controlPhase = .idle
        currentRunMode = displayMode
    }

    mutating func completeResumeLaunchFailed() {
        controlPhase = .idle
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .failed
        statusText = "继续备份失败"
    }

    mutating func cancelResume(
        pausedMode: BackupRunMode,
        pausedDisplayMode: BackupRunMode
    ) {
        let phaseBeforeCancel = controlPhase
        controlPhase = .idle
        let intent: BackupTerminationIntent = (phaseBeforeCancel == .stopping) ? .stop : .pause
        state = intent == .stop ? .stopped : .paused
        statusText = intent == .stop ? "备份已停止" : "备份已暂停"
        if intent == .stop {
            lastPausedRunMode = nil
            lastPausedDisplayRunMode = nil
            currentRunMode = .full
        } else {
            lastPausedRunMode = pausedMode
            lastPausedDisplayRunMode = pausedDisplayMode
            currentRunMode = pausedDisplayMode
        }
    }

    mutating func failResumePreparation() {
        controlPhase = .idle
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .failed
        statusText = "继续备份失败"
    }

    mutating func reduce(
        event: BackupEvent,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) -> BackupSessionReductionOutcome {
        isStartCommandInFlight = false
        currentRunMode = displayMode

        switch event {
        case .progress(let progress):
            succeeded = progress.succeeded
            failed = progress.failed
            skipped = progress.skipped
            total = progress.total
            statusText = progress.message
            if let itemEvent = progress.itemEvent {
                applyProgressEvent(itemEvent)
            }
            return BackupSessionReductionOutcome(shouldStop: false, notification: .throttled)

        case .log, .transferState:
            return BackupSessionReductionOutcome(shouldStop: false, notification: .none)

        case .monthChanged(let change):
            let monthKey = LibraryMonthKey(year: change.year, month: change.month)
            switch change.action {
            case .started:
                startedMonths.insert(monthKey)
            case .checkpointSaved:
                checkpointedMonths.insert(monthKey)
            case .completed:
                completedMonths.insert(monthKey)
            case .checkpointFailed:
                break
            }
            return BackupSessionReductionOutcome(shouldStop: false, notification: .throttled)

        case .remoteIndexSynced:
            return BackupSessionReductionOutcome(shouldStop: false, notification: .throttled)

        case .started(let totalAssets):
            total = totalAssets
            return BackupSessionReductionOutcome(shouldStop: false, notification: .throttled)

        case .finished(let result):
            finishRun(
                result: result,
                runMode: runMode,
                displayMode: displayMode,
                terminalIntent: terminalIntent
            )
            return BackupSessionReductionOutcome(shouldStop: true, notification: .immediate)
        }
    }

    mutating func applyRunError(
        _ error: Error,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        externalUnavailable: Bool,
        intent: BackupTerminationIntent,
        phaseBeforeFailure: BackupSessionControlPhase
    ) {
        controlPhase = .idle
        isStartCommandInFlight = false

        let effectiveIntent: BackupTerminationIntent
        if intent != .none {
            effectiveIntent = intent
        } else if error is CancellationError {
            effectiveIntent = (phaseBeforeFailure == .stopping) ? .stop : .pause
        } else {
            effectiveIntent = .none
        }

        if effectiveIntent != .none || error is CancellationError {
            if effectiveIntent == .stop {
                lastPausedRunMode = nil
                lastPausedDisplayRunMode = nil
                currentRunMode = .full
            } else {
                lastPausedRunMode = runMode
                lastPausedDisplayRunMode = displayMode
                currentRunMode = displayMode
            }
            state = effectiveIntent == .stop ? .stopped : .paused
            statusText = effectiveIntent == .stop ? "备份已停止" : "备份已暂停"
            return
        }

        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .failed
        statusText = externalUnavailable ? "外接存储已断开" : "备份失败"
    }

    private mutating func finishRun(
        result: BackupExecutionResult,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) {
        isStartCommandInFlight = false
        controlPhase = .idle

        succeeded = result.succeeded
        failed = result.failed
        skipped = result.skipped
        total = result.total

        if terminalIntent == .stop {
            lastPausedRunMode = nil
            lastPausedDisplayRunMode = nil
            currentRunMode = .full
            state = .stopped
            statusText = "备份已停止"
            return
        }

        if result.paused || terminalIntent == .pause {
            lastPausedRunMode = runMode
            lastPausedDisplayRunMode = displayMode
            currentRunMode = displayMode
            state = .paused
            statusText = "备份已暂停"
            return
        }

        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .completed
        completedAssetIDsForResume.removeAll()
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
    }

    private mutating func applyProgressEvent(_ event: BackupItemEvent) {
        let monthKey = LibraryMonthKey.from(date: event.resourceDate)
        processedCountByMonth[monthKey, default: 0] += 1

        if event.status == .failed {
            failedCountByMonth[monthKey, default: 0] += 1
            completedAssetIDsForResume.remove(event.assetLocalIdentifier)
        } else {
            completedAssetIDsForResume.insert(event.assetLocalIdentifier)
        }
    }
}
