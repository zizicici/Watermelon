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
            activeMonths: activeMonths,
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
    private var activeMonths = Set<LibraryMonthKey>()
    private var uploadObserverID: UUID?

    private var assetCountByMonth: [LibraryMonthKey: Int] = [:]
    private var processedCountByMonth: [LibraryMonthKey: Int] = [:]

    private var uploadMonths: [LibraryMonthKey] = []
    private var pendingDownloadMonths: [LibraryMonthKey] = []
    private var pendingSyncMonths: [LibraryMonthKey] = []
    private var downloadTask: Task<Void, Never>?
    private var backupSessionController: BackupSessionController!
    private var downloadHelper: DownloadWorkflowHelper!

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
        downloadTask = nil

        monthPlans.removeAll()
        for m in upload   { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: false) }
        for m in download { monthPlans[m] = MonthPlan(needsUpload: false, needsDownload: true) }
        for m in sync     { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: true) }
        activeMonths.removeAll()
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        backupSessionController = BackupSessionController(dependencies: dependencies)
        downloadHelper = DownloadWorkflowHelper(
            dependencies: dependencies,
            backupSessionController: backupSessionController,
            callbacks: DownloadWorkflowHelper.Callbacks(
                localAssetIDs: dataAccess.localAssetIDs,
                remoteOnlyItems: dataAccess.remoteOnlyItems,
                syncRemoteData: dataAccess.syncRemoteData,
                refreshLocalIndex: dataAccess.refreshLocalIndex,
                onProgress: { [weak self] in self?.onStateChanged?() }
            )
        )

        let backupTargetMonths = upload + sync
        if !backupTargetMonths.isEmpty {
            phase = .uploading
            onStateChanged?()
            startUploadPhase(months: backupTargetMonths)
        } else {
            phase = .downloading
            onStateChanged?()
            startDownloadPhase()
        }
    }

    func exit() {
        downloadTask?.cancel()
        downloadTask = nil
        downloadHelper?.cancel()
        phase = nil
        monthPlans.removeAll()
        activeMonths.removeAll()
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        uploadMonths.removeAll()
        pendingDownloadMonths.removeAll()
        pendingSyncMonths.removeAll()

        if let observerID = uploadObserverID {
            backupSessionController.removeObserver(observerID)
            uploadObserverID = nil
        }

        onStateChanged?()
    }

    func pause() {
        switch phase {
        case .uploading:
            backupSessionController.pauseBackup()
            // phase changes to .uploadPaused when observer reports .paused
        case .downloading:
            downloadTask?.cancel()
            downloadTask = nil
            backupSessionController.stopBackup()
            downloadHelper?.cancel()
            phase = .downloadPaused
            activeMonths.removeAll()
            onStateChanged?()
        default:
            break
        }
    }

    func resume() {
        switch phase {
        case .uploadPaused:
            phase = .uploading
            onStateChanged?()
            backupSessionController.startBackup()
        case .downloadPaused:
            phase = .downloading
            onStateChanged?()
            startDownloadPhase()
        default:
            break
        }
    }

    func stop() {
        switch phase {
        case .uploading, .uploadPaused:
            backupSessionController.stopBackup()
            // observer receives .stopped → handleBackupSnapshot → exit()
        case .downloading, .downloadPaused:
            downloadTask?.cancel()
            downloadTask = nil
            backupSessionController.stopBackup()
            exit()
        case .completed, .failed:
            exit()
        default:
            break
        }
    }

    // MARK: - Upload Phase

    private func startUploadPhase(months: [LibraryMonthKey]) {
        let syncMonthSet = Set(pendingSyncMonths)
        var allAssetIDs = Set<String>()
        for month in months {
            let ids = dataAccess.localAssetIDs(month)
            allAssetIDs.formUnion(ids)
            if !syncMonthSet.contains(month) {
                assetCountByMonth[month] = ids.count
            }
        }

        let selection = BackupScopeSelection(
            selectedAssetIDs: allAssetIDs,
            selectedAssetCount: allAssetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: allAssetIDs.count,
            totalEstimatedBytes: nil
        )
        backupSessionController.updateScopeSelection(selection)

        uploadObserverID = backupSessionController.addObserver { [weak self] snapshot in
            self?.handleBackupSnapshot(snapshot)
        }

        backupSessionController.startBackup()
    }

    private func handleBackupSnapshot(_ snapshot: BackupSessionController.Snapshot) {
        guard phase != nil else { return }

        let previouslyCompleted = Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key))

        for month in snapshot.startedMonths where monthPlans[month] != nil {
            if monthPlans[month]?.phase == .pending {
                monthPlans[month]?.phase = .uploading
            }
        }

        for month in snapshot.completedMonths where monthPlans[month] != nil {
            if monthPlans[month]?.needsDownload == true {
                monthPlans[month]?.phase = .uploadDone
            } else {
                monthPlans[month]?.phase = .completed
            }
        }

        let nowCompleted = Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key))
        let hasNewCompletions = !nowCompleted.subtracting(previouslyCompleted).isEmpty

        let executionMonthSet = Set(monthPlans.keys)
        activeMonths = snapshot.startedMonths.intersection(executionMonthSet).subtracting(nowCompleted)
        processedCountByMonth = snapshot.processedCountByMonth

        switch snapshot.state {
        case .completed:
            let backupTargets = Set(uploadMonths).union(pendingSyncMonths)
            for month in backupTargets where monthPlans[month] != nil {
                if monthPlans[month]?.needsDownload == true {
                    if monthPlans[month]?.phase == .pending { monthPlans[month]?.phase = .uploadDone }
                } else {
                    monthPlans[month]?.phase = .completed
                }
            }
            activeMonths.removeAll()

            if let id = uploadObserverID {
                backupSessionController.removeObserver(id)
                uploadObserverID = nil
            }

            dataAccess.syncRemoteData()

            if !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty {
                phase = .downloading
                startDownloadPhase()
            } else {
                phase = .completed
                showExecutionCompleted()
            }

        case .paused:
            phase = .uploadPaused
            activeMonths.removeAll()
            onStateChanged?()

        case .failed:
            for month in activeMonths {
                monthPlans[month]?.phase = .failed
            }
            activeMonths.removeAll()
            phase = .failed(snapshot.statusText)
            onStateChanged?()
            onAlert?("上传失败", snapshot.statusText)

        case .stopped:
            exit()

        default:
            if hasNewCompletions || !snapshot.processedCountByMonth.isEmpty {
                homeExecLog.info("[HomeExec] backupSnapshot: syncRemote, hasNewCompletions=\(hasNewCompletions), hasProgress=\(!snapshot.processedCountByMonth.isEmpty)")
                dataAccess.syncRemoteData()
            }
            onStateChanged?()
        }
    }

    // MARK: - Download Phase

    private func startDownloadPhase() {
        guard !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty else {
            showExecutionCompleted()
            return
        }

        onStateChanged?()

        guard let profile = dependencies.appSession.activeProfile,
              let password = resolvedSessionPassword(for: profile) else {
            onAlert?("错误", "未连接远端存储")
            exit()
            return
        }

        let remainingDownloads = pendingDownloadMonths.filter { monthPlans[$0]?.isFullyCompleted != true }
        let remainingSyncs = pendingSyncMonths.filter { monthPlans[$0]?.isFullyCompleted != true }
        let context = DownloadWorkflowHelper.Context(profile: profile, password: password)

        downloadTask = Task { [weak self] in
            guard let self else { return }

            for month in remainingDownloads {
                if Task.isCancelled { return }
                await self.runDownloadMonth(month, context: context, phaseLabel: "下载")
            }

            for month in remainingSyncs {
                if Task.isCancelled { return }
                await self.runDownloadMonth(month, context: context, phaseLabel: "同步")
            }

            if !Task.isCancelled {
                await MainActor.run {
                    self.activeMonths.removeAll()
                    let hasFailed = self.monthPlans.values.contains(where: \.isFailed)
                    if hasFailed {
                        self.phase = .failed("部分月份失败")
                        self.onStateChanged?()
                    } else {
                        self.phase = .completed
                        self.showExecutionCompleted()
                    }
                }
            }
        }
    }

    private func runDownloadMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context,
        phaseLabel: String
    ) async {
        await MainActor.run {
            self.activeMonths = [month]
            self.monthPlans[month]?.phase = .downloading
            self.assetCountByMonth.removeValue(forKey: month)
            self.processedCountByMonth.removeValue(forKey: month)
            self.onStateChanged?()
        }

        let result = await downloadHelper.downloadMonth(month, context: context, phaseLabel: phaseLabel)

        await MainActor.run {
            switch result {
            case .success:
                self.monthPlans[month]?.phase = .completed
                self.onStateChanged?()
            case .failed(let message):
                self.monthPlans[month]?.phase = .failed
                self.onStateChanged?()
                self.onAlert?("\(phaseLabel)失败", message)
            case .cancelled:
                break
            }
        }
    }

    // MARK: - Helpers

    private func showExecutionCompleted() {
        for key in monthPlans.keys where monthPlans[key]?.phase != .failed {
            monthPlans[key]?.phase = .completed
        }
        activeMonths.removeAll()
        onStateChanged?()
    }

    private func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionPassword(from: dependencies.appSession)
    }
}
