import Foundation

struct HomeExecutionSession {

    struct AlertMessage {
        let title: String
        let message: String
    }

    enum RuntimeTransition {
        case upload
        case download
    }

    enum UploadResultOutcome {
        case continueToDownload
        case paused
        case failed(AlertMessage)
        case exit
        case finished
    }

    private(set) var phase: ExecutionPhase?
    private(set) var monthPlans: [LibraryMonthKey: MonthPlan] = [:]
    private(set) var assetCountByMonth: [LibraryMonthKey: Int] = [:]
    private(set) var processedCountByMonth: [LibraryMonthKey: Int] = [:]
    private(set) var uploadAssetIDsByMonth: [LibraryMonthKey: Set<PhotoKitLocalIdentifier>] = [:]
    private(set) var backupMonths: [LibraryMonthKey] = []
    private(set) var downloadMonths: [LibraryMonthKey] = []
    private(set) var complementMonths: [LibraryMonthKey] = []
    private(set) var uploadPhaseCompleted = false
    private(set) var localIndexPreflightCompleted = false

    private var pendingUploadScope: BackupScopeSelection?
    private var lastSyncTime: CFAbsoluteTime = 0

    var isActive: Bool { phase != nil }
    var hasComplementMonths: Bool { !complementMonths.isEmpty }
    var needsLocalIndexPreflight: Bool { !localIndexPreflightCompleted }
    var requiresCompleteLocalIndexBeforeExecution: Bool {
        monthPlans.values.contains(where: \.needsDownload)
    }

    var phaseProgressCounter: (current: Int, total: Int)? {
        switch phase {
        case .uploading, .uploadPaused:
            let targets = backupMonths + complementMonths
            guard !targets.isEmpty else { return nil }
            let current = targets.reduce(into: 0) { acc, month in
                if let plan = monthPlans[month], plan.phase != .pending {
                    acc += 1
                }
            }
            return (current, targets.count)
        case .downloading, .downloadPaused:
            let targets = downloadMonths + complementMonths
            guard !targets.isEmpty else { return nil }
            let downloadPhases: Set<MonthPlan.Phase> = [
                .downloading, .downloadPaused,
                .completed, .partiallyFailed, .failed,
            ]
            let current = targets.reduce(into: 0) { acc, month in
                if let plan = monthPlans[month], downloadPhases.contains(plan.phase) {
                    acc += 1
                }
            }
            return (current, targets.count)
        default:
            return nil
        }
    }

    func currentState(controlState: ExecutionControlState, statusText: String) -> HomeExecutionState? {
        guard let phase else { return nil }
        return HomeExecutionState(
            monthPlans: monthPlans,
            phase: phase,
            controlState: controlState,
            statusText: statusText,
            processedCountByMonth: processedCountByMonth,
            assetCountByMonth: assetCountByMonth,
            backupMonths: backupMonths,
            downloadMonths: downloadMonths,
            complementMonths: complementMonths
        )
    }

    var shouldRunUploadPhase: Bool { !uploadPhaseCompleted }

    var uploadScopeAssetIDs: Set<PhotoKitLocalIdentifier> {
        uploadAssetIDsByMonth.values.reduce(into: Set<PhotoKitLocalIdentifier>()) { $0.formUnion($1) }
    }

    mutating func enter(
        backup: [LibraryMonthKey],
        download: [LibraryMonthKey],
        complement: [LibraryMonthKey],
        localAssetIDs: (LibraryMonthKey) -> Set<PhotoKitLocalIdentifier>
    ) {
        backupMonths = backup
        downloadMonths = download
        complementMonths = complement

        monthPlans.removeAll()
        for month in backup {
            monthPlans[month] = MonthPlan(needsUpload: true, needsDownload: false)
        }
        for month in download {
            monthPlans[month] = MonthPlan(needsUpload: false, needsDownload: true)
        }
        for month in complement {
            monthPlans[month] = MonthPlan(needsUpload: true, needsDownload: true)
        }

        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        lastSyncTime = 0

        uploadPhaseCompleted = (backup + complement).isEmpty
        localIndexPreflightCompleted = false
        pendingUploadScope = uploadPhaseCompleted ? nil : buildUploadScope(localAssetIDs: localAssetIDs)
        phase = uploadPhaseCompleted ? .downloading : .uploading
    }

    mutating func reset() {
        phase = nil
        monthPlans.removeAll()
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        uploadAssetIDsByMonth.removeAll()
        backupMonths.removeAll()
        downloadMonths.removeAll()
        complementMonths.removeAll()
        uploadPhaseCompleted = false
        localIndexPreflightCompleted = false
        pendingUploadScope = nil
        lastSyncTime = 0
    }

    mutating func pause() -> RuntimeTransition? {
        switch phase {
        case .uploading:
            pauseUploadPhaseMonths()
            phase = .uploadPaused
            return .upload
        case .downloading:
            applyToMonths(where: { $0.phase == .downloading }) { $0.markDownloadPaused() }
            phase = .downloadPaused
            return .download
        default:
            return nil
        }
    }

    mutating func resume() -> RuntimeTransition? {
        switch phase {
        case .uploadPaused:
            resumeUploadPhaseMonths()
            phase = .uploading
            lastSyncTime = 0
            return .upload
        case .downloadPaused:
            applyToMonths(where: { $0.phase == .downloadPaused }) { $0.markDownloadResumed() }
            phase = .downloading
            return .download
        default:
            return nil
        }
    }

    mutating func consumePendingUploadScope() -> BackupScopeSelection? {
        defer { pendingUploadScope = nil }
        return pendingUploadScope
    }

    mutating func handleUploadProgress(
        _ progress: BackupSessionAsyncBridge.UploadProgress,
        now: CFAbsoluteTime,
        syncThrottleInterval: CFAbsoluteTime
    ) -> Bool {
        for month in progress.newlyStartedMonths where monthPlans[month] != nil {
            monthPlans[month]?.markUploadStarted()
        }

        let hasNewUploadCompletions = progress.newlyUploadCompletedMonths.contains {
            monthPlans[$0]?.needsUpload == true
        }

        for month in progress.newlyUploadCompletedMonths where monthPlans[month]?.needsUpload == true {
            monthPlans[month]?.markUploadDurablyCompleted()
        }

        processedCountByMonth = progress.processedCountByMonth

        if hasNewUploadCompletions {
            lastSyncTime = now
            return true
        }
        if !progress.processedCountByMonth.isEmpty,
           now - lastSyncTime >= syncThrottleInterval {
            lastSyncTime = now
            return true
        }
        return false
    }

    mutating func handleUploadResult(_ result: BackupSessionAsyncBridge.UploadResult) -> UploadResultOutcome {
        switch result {
        case .completed(let failedCountByMonth, let incompleteSummaryByMonth, let uploadSnapshotDeferredMessageByMonth):
            uploadPhaseCompleted = true
            for (month, failedCount) in failedCountByMonth where failedCount > 0 {
                monthPlans[month]?.recordUploadFailures(observedFailedItemCount: failedCount)
            }
            recordMonthIncompleteSummaries(incompleteSummaryByMonth)
            recordDurableUploadSnapshotDeferredMessages(uploadSnapshotDeferredMessageByMonth)

            if pendingDownloadMonths().isEmpty {
                finishExecution()
                return .finished
            }

            phase = .downloading
            return .continueToDownload

        case .paused:
            phase = .uploadPaused
            pauseUploadPhaseMonths()
            return .paused

        case .failed(let message, let failedCountByMonth, let incompleteSummaryByMonth, let uploadSnapshotDeferredMessageByMonth):
            recordDurableUploadSnapshotDeferredMessages(uploadSnapshotDeferredMessageByMonth)
            recordUploadTargetsTerminalFailure(kind: .uploadRunFailed, message: message)
            for (month, failedCount) in failedCountByMonth where failedCount > 0 {
                monthPlans[month]?.recordUploadFailures(observedFailedItemCount: failedCount)
            }
            recordMonthIncompleteSummaries(incompleteSummaryByMonth)
            phase = .failed(message)
            return .failed(AlertMessage(title: String(localized: "home.execution.uploadFailed"), message: message))

        case .stopped:
            return .exit

        case .startFailed:
            let message = String(localized: "home.execution.startFailed")
            recordUploadTargetsTerminalFailure(kind: .backupStartFailed, message: message)
            phase = .failed(message)
            return .failed(AlertMessage(title: String(localized: "home.execution.uploadFailed"), message: message))
        }
    }

    func pendingDownloadMonths() -> [LibraryMonthKey] {
        (downloadMonths + complementMonths).filter { monthPlans[$0]?.hasPendingDownloadWork == true }
    }

    mutating func beginDownloadPhase() {
        phase = .downloading
    }

    mutating func failForMissingConnection(messageOverride: String? = nil) -> AlertMessage {
        let message = messageOverride ?? String(localized: "home.execution.notConnected")
        let failure = MonthTerminalFailure(kind: .missingConnection, message: message)
        applyToMonths(where: \.shouldReceiveRunAbortFailure) { $0.recordTerminalFailure(failure) }
        phase = .failed(message)
        return AlertMessage(title: String(localized: "common.error"), message: message)
    }

    mutating func beginDownloadMonth(_ month: LibraryMonthKey) {
        if monthPlans[month]?.phase == .downloadPaused {
            monthPlans[month]?.markDownloadResumed()
        } else {
            monthPlans[month]?.markDownloadStarted()
        }
        assetCountByMonth.removeValue(forKey: month)
        processedCountByMonth.removeValue(forKey: month)
    }

    mutating func completeComplementMonthUpload(_ month: LibraryMonthKey) {
        monthPlans[month]?.markUploadDurablyCompleted()
    }

    mutating func completeDownloadMonth(_ month: LibraryMonthKey) {
        finishDownloadAttempt(month)
    }

    mutating func failDownloadMonth(_ month: LibraryMonthKey, reason: String) {
        finishDownloadAttemptWithFailure(
            month,
            failure: MonthTerminalFailure(kind: .downloadRunFailed, message: reason)
        )
    }

    mutating func recordMonthIncomplete(_ month: LibraryMonthKey, summary: BackupMonthIncompleteSummary) {
        monthPlans[month]?.recordDownloadIncomplete(summary)
    }

    mutating func recordDurableUploadSnapshotDeferred(_ month: LibraryMonthKey, message: String) {
        monthPlans[month]?.recordDurableUploadSnapshotDeferred(message: message)
    }

    mutating func finishDownloadAttempt(_ month: LibraryMonthKey) {
        monthPlans[month]?.closeDownloadAttemptClean()
    }

    mutating func finishDownloadAttemptWithIncomplete(
        _ month: LibraryMonthKey,
        summary: BackupMonthIncompleteSummary
    ) {
        monthPlans[month]?.closeDownloadAttemptIncomplete(summary)
    }

    mutating func finishDownloadAttemptWithFailure(
        _ month: LibraryMonthKey,
        failure: MonthTerminalFailure
    ) {
        monthPlans[month]?.closeDownloadAttemptFailed(failure)
    }

    func phaseLabel(for month: LibraryMonthKey) -> String {
        monthPlans[month]?.needsUpload == true ? String(localized: "home.execution.phaseComplement") : String(localized: "home.execution.phaseDownload")
    }

    mutating func finishExecution() {
        for key in monthPlans.keys {
            closeMonthForFinishedExecutionRollup(key)
        }
        let hasFailure = monthPlans.values.contains(where: \.hasUserVisibleFailure)
        phase = hasFailure ? .failed(String(localized: "home.execution.partialFailed")) : .completed
    }

    mutating func markLocalIndexPreflightCompleted() {
        localIndexPreflightCompleted = true
    }

    mutating func failExecution(
        reason: String,
        kind: MonthTerminalFailure.Kind = .generic
    ) -> AlertMessage {
        let failure = MonthTerminalFailure(kind: kind, message: reason)
        applyToMonths(where: \.shouldReceiveRunAbortFailure) { $0.recordTerminalFailure(failure) }
        phase = .failed(reason)
        return AlertMessage(title: String(localized: "common.error"), message: reason)
    }

    private mutating func buildUploadScope(
        localAssetIDs: (LibraryMonthKey) -> Set<PhotoKitLocalIdentifier>
    ) -> BackupScopeSelection {
        let complementMonthSet = Set(complementMonths)
        var allAssetIDs = Set<PhotoKitLocalIdentifier>()
        uploadAssetIDsByMonth.removeAll(keepingCapacity: true)

        for month in backupMonths + complementMonths {
            let ids = localAssetIDs(month)
            uploadAssetIDsByMonth[month] = ids
            allAssetIDs.formUnion(ids)
            if !complementMonthSet.contains(month) {
                assetCountByMonth[month] = ids.count
            }
        }

        lastSyncTime = 0
        return BackupScopeSelection(
            selectedAssetIDs: allAssetIDs,
            selectedAssetCount: allAssetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: allAssetIDs.count,
            totalEstimatedBytes: nil
        )
    }

    private mutating func recordUploadTargetsTerminalFailure(
        kind: MonthTerminalFailure.Kind,
        message: String
    ) {
        let uploadTargets = Set(backupMonths).union(complementMonths)
        for month in uploadTargets {
            guard let plan = monthPlans[month], plan.shouldReceiveUploadRunFailure else { continue }
            monthPlans[month]?.recordTerminalFailure(MonthTerminalFailure(kind: kind, message: message))
        }
    }

    private mutating func recordMonthIncompleteSummaries(
        _ summariesByMonth: [LibraryMonthKey: BackupMonthIncompleteSummary]
    ) {
        for (month, summary) in summariesByMonth {
            monthPlans[month]?.recordDownloadIncomplete(summary)
        }
    }

    private mutating func recordDurableUploadSnapshotDeferredMessages(
        _ messagesByMonth: [LibraryMonthKey: String]
    ) {
        for (month, message) in messagesByMonth {
            monthPlans[month]?.recordDurableUploadSnapshotDeferred(message: message)
        }
    }

    private mutating func pauseUploadPhaseMonths() {
        for key in monthPlans.keys {
            switch monthPlans[key]?.phase {
            case .uploading:
                monthPlans[key]?.markUploadPaused()
            case .downloading:
                monthPlans[key]?.markDownloadPaused()
            default:
                break
            }
        }
    }

    private mutating func resumeUploadPhaseMonths() {
        for key in monthPlans.keys {
            switch monthPlans[key]?.phase {
            case .uploadPaused:
                monthPlans[key]?.markUploadResumed()
            default:
                break
            }
        }
    }

    private mutating func closeMonthForFinishedExecutionRollup(_ key: LibraryMonthKey) {
        guard var plan = monthPlans[key] else { return }
        plan.workFacts.uploadFinished = true
        plan.workFacts.downloadFinished = true
        if plan.failureFacts.hasTerminalFailure {
            plan.phase = .failed
        } else if plan.failureFacts.hasNonFatalIssues {
            plan.phase = .partiallyFailed
        } else {
            plan.phase = .completed
        }
        monthPlans[key] = plan
    }

    private mutating func applyToMonths(where predicate: (MonthPlan) -> Bool, _ body: (inout MonthPlan) -> Void) {
        for key in monthPlans.keys {
            if var plan = monthPlans[key], predicate(plan) {
                body(&plan)
                monthPlans[key] = plan
            }
        }
    }
}
