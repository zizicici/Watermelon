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
    private(set) var uploadMonths: [LibraryMonthKey] = []
    private(set) var downloadMonths: [LibraryMonthKey] = []
    private(set) var syncMonths: [LibraryMonthKey] = []
    private(set) var uploadPhaseCompleted = false
    private(set) var localIndexPreflightCompleted = false

    private var pendingUploadScope: BackupScopeSelection?
    private var lastSyncTime: CFAbsoluteTime = 0

    var isActive: Bool { phase != nil }
    var hasSyncMonths: Bool { !syncMonths.isEmpty }
    var needsLocalIndexPreflight: Bool { !localIndexPreflightCompleted }
    var requiresCompleteLocalIndexBeforeExecution: Bool {
        monthPlans.values.contains(where: \.needsDownload)
    }

    func currentState(controlState: ExecutionControlState) -> HomeExecutionState? {
        guard let phase else { return nil }
        return HomeExecutionState(
            monthPlans: monthPlans,
            phase: phase,
            controlState: controlState,
            processedCountByMonth: processedCountByMonth,
            assetCountByMonth: assetCountByMonth,
            uploadMonths: uploadMonths,
            downloadMonths: downloadMonths,
            syncMonths: syncMonths
        )
    }

    var shouldRunUploadPhase: Bool { !uploadPhaseCompleted }

    mutating func enter(
        upload: [LibraryMonthKey],
        download: [LibraryMonthKey],
        sync: [LibraryMonthKey],
        localAssetIDs: (LibraryMonthKey) -> Set<String>
    ) {
        uploadMonths = upload
        downloadMonths = download
        syncMonths = sync

        monthPlans.removeAll()
        for month in upload {
            monthPlans[month] = MonthPlan(needsUpload: true, needsDownload: false)
        }
        for month in download {
            monthPlans[month] = MonthPlan(needsUpload: false, needsDownload: true)
        }
        for month in sync {
            monthPlans[month] = MonthPlan(needsUpload: true, needsDownload: true)
        }

        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        lastSyncTime = 0

        uploadPhaseCompleted = (upload + sync).isEmpty
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
        uploadMonths.removeAll()
        downloadMonths.removeAll()
        syncMonths.removeAll()
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
            return .failed(AlertMessage(title: "上传失败", message: message))

        case .stopped:
            return .exit

        case .startFailed:
            let message = "备份启动失败"
            applyUploadTargetsFailed(reason: message)
            phase = .failed(message)
            return .failed(AlertMessage(title: "上传失败", message: message))
        }
    }

    func remainingDownloadMonths() -> [LibraryMonthKey] {
        (downloadMonths + syncMonths).filter { monthPlans[$0]?.isTerminal != true }
    }

    mutating func beginDownloadPhase() {
        phase = .downloading
    }

    mutating func failForMissingConnection() -> AlertMessage {
        let message = "未连接远端存储"
        applyEvent(.failed(reason: message), where: { !$0.isTerminal })
        phase = .failed(message)
        return AlertMessage(title: "错误", message: message)
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

    mutating func completeSyncMonthUpload(_ month: LibraryMonthKey) {
        monthPlans[month]?.apply(.uploadCompleted)
    }

    mutating func completeDownloadMonth(_ month: LibraryMonthKey) {
        monthPlans[month]?.apply(.downloadCompleted)
    }

    mutating func failDownloadMonth(_ month: LibraryMonthKey, reason: String) {
        monthPlans[month]?.apply(.failed(reason: reason))
    }

    func phaseLabel(for month: LibraryMonthKey) -> String {
        monthPlans[month]?.needsUpload == true ? "同步" : "下载"
    }

    mutating func finishExecution() {
        for key in monthPlans.keys {
            monthPlans[key]?.apply(.completed)
        }
        phase = monthPlans.values.contains(where: \.isFailed) ? .failed("部分月份失败") : .completed
    }

    mutating func markLocalIndexPreflightCompleted() {
        localIndexPreflightCompleted = true
    }

    mutating func failExecution(reason: String) -> AlertMessage {
        applyEvent(.failed(reason: reason), where: { !$0.isTerminal })
        phase = .failed(reason)
        return AlertMessage(title: "错误", message: reason)
    }

    private mutating func buildUploadScope(
        localAssetIDs: (LibraryMonthKey) -> Set<String>
    ) -> BackupScopeSelection {
        let syncMonthSet = Set(syncMonths)
        var allAssetIDs = Set<String>()
        uploadAssetIDsByMonth.removeAll(keepingCapacity: true)

        for month in uploadMonths + syncMonths {
            let ids = localAssetIDs(month)
            uploadAssetIDsByMonth[month] = ids
            allAssetIDs.formUnion(ids)
            if !syncMonthSet.contains(month) {
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
        let uploadTargets = Set(uploadMonths).union(syncMonths)
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
