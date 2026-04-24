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
    private(set) var uploadAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
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

    var uploadScopeAssetIDs: Set<String> {
        uploadAssetIDsByMonth.values.reduce(into: Set<String>()) { $0.formUnion($1) }
    }

    mutating func enter(
        backup: [LibraryMonthKey],
        download: [LibraryMonthKey],
        complement: [LibraryMonthKey],
        localAssetIDs: (LibraryMonthKey) -> Set<String>
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
            applyEvent(.downloadPaused, where: { $0.phase == .downloading })
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
            applyEvent(.downloadResumed, where: { $0.phase == .downloadPaused })
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
        let previousDoneCount = monthPlans.values.filter(\.isDone).count

        for month in progress.newlyStartedMonths where monthPlans[month] != nil {
            monthPlans[month]?.apply(.uploadStarted)
        }

        for month in progress.newlyCompletedMonths where monthPlans[month] != nil {
            monthPlans[month]?.apply(.uploadCompleted)
        }

        let currentDoneCount = monthPlans.values.filter(\.isDone).count
        let hasNewCompletions = currentDoneCount > previousDoneCount
        processedCountByMonth = progress.processedCountByMonth

        if hasNewCompletions {
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
        case .completed(let failedCountByMonth):
            uploadPhaseCompleted = true
            for (month, failedCount) in failedCountByMonth where failedCount > 0 {
                monthPlans[month]?.apply(.partiallyFailed(count: failedCount))
            }

            if remainingDownloadMonths().isEmpty {
                finishExecution()
                return .finished
            }

            phase = .downloading
            return .continueToDownload

        case .paused:
            phase = .uploadPaused
            pauseUploadPhaseMonths()
            return .paused

        case .failed(let message):
            applyUploadTargetsFailed(reason: message)
            phase = .failed(message)
            return .failed(AlertMessage(title: String(localized: "home.execution.uploadFailed"), message: message))

        case .stopped:
            return .exit

        case .startFailed:
            let message = String(localized: "home.execution.startFailed")
            applyUploadTargetsFailed(reason: message)
            phase = .failed(message)
            return .failed(AlertMessage(title: String(localized: "home.execution.uploadFailed"), message: message))
        }
    }

    func remainingDownloadMonths() -> [LibraryMonthKey] {
        (downloadMonths + complementMonths).filter { monthPlans[$0]?.isTerminal != true }
    }

    mutating func beginDownloadPhase() {
        phase = .downloading
    }

    mutating func failForMissingConnection() -> AlertMessage {
        let message = String(localized: "home.execution.notConnected")
        applyEvent(.failed(reason: message), where: { !$0.isTerminal })
        phase = .failed(message)
        return AlertMessage(title: String(localized: "common.error"), message: message)
    }

    mutating func beginDownloadMonth(_ month: LibraryMonthKey) {
        if monthPlans[month]?.phase == .downloadPaused {
            monthPlans[month]?.apply(.downloadResumed)
        } else {
            monthPlans[month]?.apply(.downloadStarted)
        }
        assetCountByMonth.removeValue(forKey: month)
        processedCountByMonth.removeValue(forKey: month)
    }

    mutating func completeComplementMonthUpload(_ month: LibraryMonthKey) {
        monthPlans[month]?.apply(.uploadCompleted)
    }

    mutating func completeDownloadMonth(_ month: LibraryMonthKey) {
        monthPlans[month]?.apply(.downloadCompleted)
    }

    mutating func failDownloadMonth(_ month: LibraryMonthKey, reason: String) {
        monthPlans[month]?.apply(.failed(reason: reason))
    }

    func phaseLabel(for month: LibraryMonthKey) -> String {
        monthPlans[month]?.needsUpload == true ? String(localized: "home.execution.phaseComplement") : String(localized: "home.execution.phaseDownload")
    }

    mutating func finishExecution() {
        for key in monthPlans.keys {
            monthPlans[key]?.apply(.completed)
        }
        phase = monthPlans.values.contains(where: \.isFailed) ? .failed(String(localized: "home.execution.partialFailed")) : .completed
    }

    mutating func markLocalIndexPreflightCompleted() {
        localIndexPreflightCompleted = true
    }

    mutating func failExecution(reason: String) -> AlertMessage {
        applyEvent(.failed(reason: reason), where: { !$0.isTerminal })
        phase = .failed(reason)
        return AlertMessage(title: String(localized: "common.error"), message: reason)
    }

    private mutating func buildUploadScope(
        localAssetIDs: (LibraryMonthKey) -> Set<String>
    ) -> BackupScopeSelection {
        let complementMonthSet = Set(complementMonths)
        var allAssetIDs = Set<String>()
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

    private mutating func applyUploadTargetsFailed(reason: String) {
        let uploadTargets = Set(backupMonths).union(complementMonths)
        for month in uploadTargets {
            let phase = monthPlans[month]?.phase
            guard phase != .uploadDone && phase != .completed && phase != .partiallyFailed else { continue }
            monthPlans[month]?.apply(.failed(reason: reason))
        }
    }

    private mutating func pauseUploadPhaseMonths() {
        for key in monthPlans.keys {
            switch monthPlans[key]?.phase {
            case .uploading:
                monthPlans[key]?.apply(.uploadPaused)
            case .downloading:
                monthPlans[key]?.apply(.downloadPaused)
            default:
                break
            }
        }
    }

    private mutating func resumeUploadPhaseMonths() {
        for key in monthPlans.keys {
            switch monthPlans[key]?.phase {
            case .uploadPaused:
                monthPlans[key]?.apply(.uploadResumed)
            default:
                break
            }
        }
    }

    private mutating func applyEvent(_ event: MonthEvent, where predicate: (MonthPlan) -> Bool) {
        for key in monthPlans.keys {
            if let plan = monthPlans[key], predicate(plan) {
                monthPlans[key]?.apply(event)
            }
        }
    }
}
