import Foundation
import Photos
import os.log

private let homeExecLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeExec")

@MainActor
final class HomeExecutionCoordinator {

    // MARK: - Public State

    private(set) var phase: ExecutionPhase?

    var isActive: Bool { phase != nil }

    var currentState: HomeExecutionState? {
        guard let phase else { return nil }
        return HomeExecutionState(
            monthPlans: monthPlans,
            phase: phase,
            processedCountByMonth: processedCountByMonth,
            assetCountByMonth: assetCountByMonth,
            uploadMonths: uploadMonths,
            downloadMonths: pendingDownloadMonths,
            syncMonths: pendingSyncMonths
        )
    }

    // MARK: - Callbacks

    var onStateChanged: (() -> Void)?
    var onAlert: ((String, String) -> Void)?

    // MARK: - Data Access (provided by Store)

    struct DataAccess {
        let localAssetIDs: (LibraryMonthKey) -> Set<String>
        let remoteOnlyItems: (LibraryMonthKey) -> [RemoteAlbumItem]
        let syncRemoteData: () -> Void
        let refreshLocalIndex: (Set<String>) -> Void
    }

    private let dataAccess: DataAccess

    // MARK: - Dependencies

    private let dependencies: DependencyContainer

    // MARK: - Execution State

    private var monthPlans: [LibraryMonthKey: MonthPlan] = [:]

    private var assetCountByMonth: [LibraryMonthKey: Int] = [:]
    private var processedCountByMonth: [LibraryMonthKey: Int] = [:]

    private var uploadMonths: [LibraryMonthKey] = []
    private var pendingDownloadMonths: [LibraryMonthKey] = []
    private var pendingSyncMonths: [LibraryMonthKey] = []
    private var executionTask: Task<Void, Never>?
    private var uploadPhaseCompleted = false
    private var pendingUploadScope: BackupScopeSelection?
    private var backupSessionController: BackupSessionController!
    private var uploadHelper: UploadWorkflowHelper!
    private var downloadHelper: DownloadWorkflowHelper!

    // MARK: - Sync Throttle

    private var lastSyncTime: CFAbsoluteTime = 0
    private static let syncThrottleInterval: CFAbsoluteTime = 2.0

    // MARK: - Init

    init(dependencies: DependencyContainer, dataAccess: DataAccess) {
        self.dependencies = dependencies
        self.dataAccess = dataAccess
    }

    // MARK: - Enter / Exit

    func enter(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        uploadMonths = upload
        pendingDownloadMonths = download
        pendingSyncMonths = sync
        executionTask = nil

        monthPlans.removeAll()
        for m in upload   { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: false) }
        for m in download { monthPlans[m] = MonthPlan(needsUpload: false, needsDownload: true) }
        for m in sync     { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: true) }

        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        lastSyncTime = 0
        backupSessionController = BackupSessionController(dependencies: dependencies)
        uploadHelper = UploadWorkflowHelper(backupSessionController: backupSessionController)
        downloadHelper = DownloadWorkflowHelper(
            dependencies: dependencies,
            backupSessionController: backupSessionController
        )

        uploadPhaseCompleted = (upload + sync).isEmpty
        pendingUploadScope = uploadPhaseCompleted ? nil : buildUploadScope()
        phase = uploadPhaseCompleted ? .downloading : .uploading
        onStateChanged?()
        startExecution()
    }

    func exit() {
        executionTask?.cancel()
        executionTask = nil
        uploadHelper?.cancel()
        downloadHelper?.cancel()
        phase = nil
        monthPlans.removeAll()

        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        uploadMonths.removeAll()
        pendingDownloadMonths.removeAll()
        pendingSyncMonths.removeAll()
        onStateChanged?()
    }

    func pause() {
        switch phase {
        case .uploading:
            uploadHelper.pause()
            // BSC cooperative pause → runUpload returns .paused → processUploadResult
        case .downloading:
            executionTask?.cancel()
            executionTask = nil
            downloadHelper.cancel()
            applyEvent(.downloadPaused, where: { $0.phase == .downloading })
            phase = .downloadPaused
            onStateChanged?()
        default:
            break
        }
    }

    func resume() {
        switch phase {
        case .uploadPaused:
            applyEvent(.uploadResumed, where: { $0.phase == .uploadPaused })
            phase = .uploading
            lastSyncTime = 0
            onStateChanged?()
            startExecution()
        case .downloadPaused:
            applyEvent(.downloadResumed, where: { $0.phase == .downloadPaused })
            phase = .downloading
            onStateChanged?()
            startExecution()
        default:
            break
        }
    }

    func stop() {
        switch phase {
        case .uploading:
            uploadHelper.stop()
            // BSC cooperative stop → runUpload returns .stopped → processUploadResult → exit
        case .uploadPaused:
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .downloading, .downloadPaused:
            executionTask?.cancel()
            executionTask = nil
            downloadHelper.cancel()
            exit()
        case .completed, .failed:
            exit()
        default:
            break
        }
    }

    // MARK: - Execution Task

    private func startExecution() {
        executionTask = Task { [weak self] in
            guard let self else { return }

            // Upload phase
            if !self.uploadPhaseCompleted {
                let scope = self.pendingUploadScope
                self.pendingUploadScope = nil
                let result = await self.uploadHelper.runUpload(scope: scope) { [weak self] progress in
                    self?.handleUploadProgress(progress)
                }
                guard !Task.isCancelled else { return }
                guard self.processUploadResult(result) else { return }
                self.uploadPhaseCompleted = true
            }

            // Download phase
            await self.runDownloadPhase()
        }
    }

    // MARK: - Upload Phase

    private func buildUploadScope() -> BackupScopeSelection {
        let syncMonthSet = Set(pendingSyncMonths)
        var allAssetIDs = Set<String>()
        for month in uploadMonths + pendingSyncMonths {
            let ids = dataAccess.localAssetIDs(month)
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

    private func handleUploadProgress(_ progress: UploadWorkflowHelper.UploadProgress) {
        let previousDoneCount = monthPlans.values.filter(\.isDone).count

        for month in progress.startedMonths where monthPlans[month] != nil {
            monthPlans[month]?.apply(.uploadStarted)
        }

        for month in progress.completedMonths where monthPlans[month] != nil {
            monthPlans[month]?.apply(.uploadCompleted)
        }

        let currentDoneCount = monthPlans.values.filter(\.isDone).count
        let hasNewCompletions = currentDoneCount > previousDoneCount
        processedCountByMonth = progress.processedCountByMonth

        let now = CFAbsoluteTimeGetCurrent()
        if hasNewCompletions {
            dataAccess.syncRemoteData()
            lastSyncTime = now
        } else if !progress.processedCountByMonth.isEmpty,
                  now - lastSyncTime >= Self.syncThrottleInterval {
            dataAccess.syncRemoteData()
            lastSyncTime = now
        }

        onStateChanged?()
    }

    @discardableResult
    private func processUploadResult(_ result: UploadWorkflowHelper.UploadResult) -> Bool {
        switch result {
        case .completed(let failedCountByMonth):
            let backupTargets = Set(uploadMonths).union(pendingSyncMonths)
            for month in backupTargets where monthPlans[month] != nil {
                let failedCount = failedCountByMonth[month] ?? 0
                if failedCount > 0 {
                    monthPlans[month]?.apply(.partiallyFailed(count: failedCount))
                } else {
                    monthPlans[month]?.apply(.uploadCompleted)
                }
            }
            dataAccess.syncRemoteData()

            let hasRemainingWork = (pendingDownloadMonths + pendingSyncMonths).contains {
                monthPlans[$0]?.isTerminal != true
            }
            if hasRemainingWork {
                phase = .downloading
                onStateChanged?()
                return true
            } else {
                handleCompletion()
                return false
            }

        case .paused:
            phase = .uploadPaused
            applyEvent(.uploadPaused, where: { $0.isActive })
            onStateChanged?()
            return false

        case .failed(let message):
            // Only mark upload targets as failed; download-only months stay .pending
            applyUploadTargetsFailed(reason: message)
            phase = .failed(message)
            onStateChanged?()
            onAlert?("上传失败", message)
            return false

        case .stopped:
            exit()
            return false

        case .startFailed:
            applyUploadTargetsFailed(reason: "备份启动失败")
            phase = .failed("备份启动失败")
            onStateChanged?()
            onAlert?("上传失败", "备份启动失败")
            return false
        }
    }

    // MARK: - Download Phase

    private func runDownloadPhase() async {
        let remaining = (pendingDownloadMonths + pendingSyncMonths).filter {
            monthPlans[$0]?.isTerminal != true
        }

        guard !remaining.isEmpty else {
            handleCompletion()
            return
        }

        guard let profile = dependencies.appSession.activeProfile,
              let password = resolvedSessionPassword(for: profile) else {
            applyEvent(.failed(reason: "未连接远端存储"), where: { !$0.isTerminal })
            phase = .failed("未连接远端存储")
            onStateChanged?()
            onAlert?("错误", "未连接远端存储")
            return
        }

        phase = .downloading
        onStateChanged?()

        let context = DownloadWorkflowHelper.Context(profile: profile, password: password)

        for month in remaining {
            if Task.isCancelled { return }
            await runDownloadMonth(month, context: context, phaseLabel: monthPlans[month]?.needsUpload == true ? "同步" : "下载")
        }

        if !Task.isCancelled {
            handleCompletion()
        }
    }

    private func runDownloadMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context,
        phaseLabel: String
    ) async {
        monthPlans[month]?.apply(.downloadStarted)
        assetCountByMonth.removeValue(forKey: month)
        processedCountByMonth.removeValue(forKey: month)
        onStateChanged?()

        // Step 1: Scoped backup to build hash index
        let assetIDs = dataAccess.localAssetIDs(month)
        if !assetIDs.isEmpty {
            let ok = await downloadHelper.runScopedBackup(assetIDs: assetIDs) { [weak self] in
                self?.onStateChanged?()
            }
            if Task.isCancelled { return }
            if !ok {
                monthPlans[month]?.apply(.failed(reason: "备份索引失败"))
                onStateChanged?()
                onAlert?("\(phaseLabel)失败", "\(month.displayText): 备份索引失败")
                return
            }
        }

        // Step 2: Sync remote data + refresh local index
        dataAccess.syncRemoteData()
        if !assetIDs.isEmpty {
            dataAccess.refreshLocalIndex(assetIDs)
        }

        // Step 3: Download remote-only items
        let remoteItems = dataAccess.remoteOnlyItems(month)
        let result = await downloadHelper.downloadItems(remoteItems, context: context) { [weak self] assetID in
            self?.dataAccess.refreshLocalIndex([assetID])
            self?.onStateChanged?()
        }

        switch result {
        case .success:
            monthPlans[month]?.apply(.downloadCompleted)
            onStateChanged?()
        case .failed(let message):
            monthPlans[month]?.apply(.failed(reason: message))
            onStateChanged?()
            onAlert?("\(phaseLabel)失败", "\(month.displayText): \(message)")
        case .cancelled:
            break
        }
    }

    // MARK: - Helpers

    private func applyUploadTargetsFailed(reason: String) {
        let uploadTargets = Set(uploadMonths).union(pendingSyncMonths)
        for month in uploadTargets {
            // Skip months that already completed upload — their upload was fine,
            // only subsequent phases were prevented by the failure.
            let p = monthPlans[month]?.phase
            guard p != .uploadDone && p != .completed && p != .partiallyFailed else { continue }
            monthPlans[month]?.apply(.failed(reason: reason))
        }
    }

    private func applyEvent(_ event: MonthEvent, where predicate: (MonthPlan) -> Bool) {
        for key in monthPlans.keys {
            if let plan = monthPlans[key], predicate(plan) {
                monthPlans[key]?.apply(event)
            }
        }
    }

    private func handleCompletion() {
        for key in monthPlans.keys {
            monthPlans[key]?.apply(.completed)
        }
        // .partiallyFailed months count as "done" — global phase is .completed
        // with the failure summary button providing the warning (product decision).
        // Only hard .failed months trigger global .failed.
        let hasFailed = monthPlans.values.contains(where: \.isFailed)
        if hasFailed {
            phase = .failed("部分月份失败")
        } else {
            phase = .completed
        }
        onStateChanged?()
    }

    private func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionPassword(from: dependencies.appSession)
    }
}
