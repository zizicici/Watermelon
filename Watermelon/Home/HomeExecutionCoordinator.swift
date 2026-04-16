import Foundation

@MainActor
final class HomeExecutionCoordinator {

    // MARK: - Public State

    var phase: ExecutionPhase? { session.phase }
    var isActive: Bool { session.isActive }
    var currentState: HomeExecutionState? { session.currentState(controlState: currentControlState) }

    // MARK: - Callbacks

    var onStateChanged: (() -> Void)?
    var onAlert: ((String, String) -> Void)?

    // MARK: - Data Access (provided by Store)

    struct DataAccess {
        let localAssetIDs: (LibraryMonthKey) -> Set<String>
        let remoteOnlyItems: (LibraryMonthKey) -> [RemoteAlbumItem]
        let syncRemoteData: () async -> Set<LibraryMonthKey>
        let refreshLocalIndex: (Set<String>) async -> Set<LibraryMonthKey>
    }

    // MARK: - Dependencies

    private let dependencies: DependencyContainer
    private let dataAccess: DataAccess

    // MARK: - Runtime

    private var session = HomeExecutionSession()
    private let dataRefresher: HomeExecutionDataRefresher
    private var executionTask: Task<Void, Never>?
    private var transientControlState: ExecutionControlState?
    private var backupSessionController: BackupSessionController!
    private var backupBridge: BackupSessionAsyncBridge!
    private var downloadHelper: DownloadWorkflowHelper!

    private static let syncThrottleInterval: CFAbsoluteTime = 2.0
    private static let localIndexPreflightWorkerCount = 2

    init(dependencies: DependencyContainer, dataAccess: DataAccess) {
        self.dependencies = dependencies
        self.dataAccess = dataAccess
        self.dataRefresher = HomeExecutionDataRefresher(
            syncRemoteData: dataAccess.syncRemoteData,
            refreshLocalIndex: dataAccess.refreshLocalIndex
        )
        self.dataRefresher.onStateChanged = { [weak self] in
            self?.notifyStateChanged()
        }
    }

    // MARK: - Enter / Exit

    func enter(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        executionTask = nil
        transientControlState = nil
        dataRefresher.reset()
        session.enter(upload: upload, download: download, sync: sync, localAssetIDs: dataAccess.localAssetIDs)
        backupSessionController = BackupSessionController(dependencies: dependencies)
        backupBridge = BackupSessionAsyncBridge(backupSessionController: backupSessionController)
        downloadHelper = DownloadWorkflowHelper(dependencies: dependencies)
        notifyStateChanged()
        startExecution()
    }

    func exit() {
        executionTask?.cancel()
        executionTask = nil
        transientControlState = nil
        dataRefresher.cancel()
        backupBridge?.cancel()
        downloadHelper?.cancel()
        session.reset()
        notifyStateChanged()
    }

    func consumePendingDataChangedMonths() -> Set<LibraryMonthKey> {
        dataRefresher.consumePendingChangedMonths()
    }

    func pause() {
        switch session.pause() {
        case .upload:
            backupBridge.markAssetIDsPendingForResume(assetIDsAwaitingInlineSyncResume())
            executionTask?.cancel()
            executionTask = nil
            dataRefresher.cancel()
            downloadHelper.cancel()
            backupBridge.requestPause()
            notifyStateChanged()
        case .download:
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .pausing
            backupBridge.cancel()
            downloadHelper.cancel()
            notifyStateChanged()
            settleDownloadPause(after: taskToAwait)
        case nil:
            break
        }
    }

    func resume() {
        guard currentControlState == .idle else { return }
        guard session.resume() != nil else { return }
        notifyStateChanged()
        startExecution()
    }

    func stop() {
        switch session.phase {
        case .uploading:
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .stopping
            dataRefresher.cancel()
            downloadHelper.cancel()
            notifyStateChanged()
            backupBridge.requestStop()
            settleStop(after: taskToAwait)
        case .uploadPaused:
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .downloading:
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .stopping
            backupBridge.cancel()
            downloadHelper.cancel()
            notifyStateChanged()
            settleStop(after: taskToAwait)
        case .downloadPaused:
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .completed, .failed:
            exit()
        default:
            break
        }
    }

    func failForMissingConnection() {
        guard let phase = session.phase else { return }
        switch phase {
        case .completed, .failed:
            return
        default:
            break
        }

        executionTask?.cancel()
        executionTask = nil
        transientControlState = nil
        dataRefresher.cancel()
        backupBridge?.requestStop()
        backupBridge?.cancel()
        downloadHelper?.cancel()

        let alert = session.failForMissingConnection()
        notifyStateChanged()
        onAlert?(alert.title, alert.message)
    }

    // MARK: - Execution Task

    private func startExecution() {
        executionTask = Task { [weak self] in
            guard let self else { return }

            if self.session.needsLocalIndexPreflight {
                await MainActor.run {
                    self.transientControlState = .starting
                    self.notifyStateChanged()
                }

                let prepared = await self.prepareLocalIndexIfNeeded()
                guard !Task.isCancelled else { return }
                guard prepared else { return }

                await MainActor.run {
                    if self.transientControlState == .starting {
                        self.transientControlState = nil
                    }
                    self.notifyStateChanged()
                }
            }

            if self.session.shouldRunUploadPhase {
                guard !Task.isCancelled else { return }
                let scope = self.session.consumePendingUploadScope()
                let result = await self.backupBridge.runUpload(
                    scope: scope,
                    onMonthUploaded: self.makeUploadMonthFinalizer()
                ) { [weak self] progress in
                    self?.handleUploadProgress(progress)
                }
                guard !Task.isCancelled else { return }
                guard await self.handleUploadResult(result) else { return }
                guard !Task.isCancelled else { return }
            }

            await self.runDownloadPhase()
        }
    }

    // MARK: - Upload Phase

    private func handleUploadProgress(_ progress: BackupSessionAsyncBridge.UploadProgress) {
        let shouldSyncRemoteData = session.handleUploadProgress(
            progress,
            now: CFAbsoluteTimeGetCurrent(),
            syncThrottleInterval: Self.syncThrottleInterval
        )
        if shouldSyncRemoteData {
            dataRefresher.scheduleRemoteSync()
        }
        notifyStateChanged()
    }

    @discardableResult
    private func handleUploadResult(_ result: BackupSessionAsyncBridge.UploadResult) async -> Bool {
        switch session.handleUploadResult(result) {
        case .continueToDownload:
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            notifyStateChanged()
            return true
        case .paused:
            notifyStateChanged()
            return false
        case .failed(let alert):
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        case .exit:
            exit()
            return false
        case .finished:
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            notifyStateChanged()
            return false
        }
    }

    // MARK: - Download Phase

    private func runDownloadPhase() async {
        let remaining = session.remainingDownloadMonths()
        guard !remaining.isEmpty else {
            session.finishExecution()
            notifyStateChanged()
            return
        }

        guard let context = makeDownloadContext() else {
            let alert = session.failForMissingConnection()
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return
        }

        session.beginDownloadPhase()
        notifyStateChanged()

        for month in remaining {
            if Task.isCancelled { return }
            await runDownloadMonth(month, context: context, phaseLabel: session.phaseLabel(for: month))
        }

        if !Task.isCancelled {
            session.finishExecution()
            notifyStateChanged()
        }
    }

    private func runDownloadMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context,
        phaseLabel: String
    ) async {
        session.beginDownloadMonth(month)
        notifyStateChanged()

        let assetIDs = dataAccess.localAssetIDs(month)
        let result = await downloadRemoteMonth(month, assetIDs: assetIDs, context: context)
        _ = applyDownloadResult(result, month: month, phaseLabel: phaseLabel)
    }

    private func notifyStateChanged() {
        onStateChanged?()
    }

    private func prepareLocalIndexIfNeeded() async -> Bool {
        let assetIDs = selectedLocalAssetIDsForExecution()
        guard !assetIDs.isEmpty else {
            session.markLocalIndexPreflightCompleted()
            return true
        }

        do {
            let result = try await dependencies.localHashIndexBuildService.buildIndex(
                for: assetIDs,
                workerCount: Self.localIndexPreflightWorkerCount,
                allowNetworkAccess: false
            )
            guard !Task.isCancelled else { return false }

            if !result.readyAssetIDs.isEmpty {
                await dataRefresher.refreshLocalIndexAndNotify(result.readyAssetIDs)
                guard !Task.isCancelled else { return false }
            }

            session.markLocalIndexPreflightCompleted()

            if session.requiresCompleteLocalIndexBeforeExecution,
               !result.incompleteAssetIDs.isEmpty {
                let message = makeLocalIndexIncompleteMessage(from: result)
                let alert = session.failExecution(reason: message)
                transientControlState = nil
                notifyStateChanged()
                onAlert?(alert.title, alert.message)
                return false
            }

            return true
        } catch is CancellationError {
            return false
        } catch {
            let message = "建立本地索引失败：\(error.localizedDescription)"
            let alert = session.failExecution(reason: message)
            transientControlState = nil
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        }
    }

    private func selectedLocalAssetIDsForExecution() -> Set<String> {
        var assetIDs = Set<String>()
        for month in session.monthPlans.keys {
            assetIDs.formUnion(dataAccess.localAssetIDs(month))
        }
        return assetIDs
    }

    private func makeLocalIndexIncompleteMessage(
        from result: LocalHashIndexBuildResult
    ) -> String {
        var parts: [String] = []
        if !result.unavailableAssetIDs.isEmpty {
            parts.append("\(result.unavailableAssetIDs.count) 项资源未下载到本机")
        }
        if !result.failedAssetIDs.isEmpty {
            parts.append("\(result.failedAssetIDs.count) 项资源读取失败")
        }
        let detail = parts.joined(separator: "，")
        return "本地索引不完整：\(detail)。为避免下载或同步产生重复图片，本次操作已停止。"
    }

    private func makeUploadMonthFinalizer() -> BackupMonthFinalizer? {
        guard session.hasSyncMonths else { return nil }
        let context = makeDownloadContext()
        return { [weak self] month in
            guard let self else { return .cancelled }
            return await self.finalizeUploadedMonth(month, context: context)
        }
    }

    private func makeDownloadContext() -> DownloadWorkflowHelper.Context? {
        guard let profile = dependencies.appSession.activeProfile,
              let password = profile.resolvedSessionPassword(from: dependencies.appSession) else {
            return nil
        }
        return DownloadWorkflowHelper.Context(profile: profile, password: password)
    }

    private func finalizeUploadedMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context?
    ) async -> BackupMonthFinalizationResult {
        guard session.monthPlans[month]?.needsUpload == true,
              session.monthPlans[month]?.needsDownload == true,
              session.monthPlans[month]?.isTerminal != true else {
            return .success
        }
        guard !Task.isCancelled else { return .cancelled }

        let phaseLabel = session.phaseLabel(for: month)
        session.completeSyncMonthUpload(month)
        session.beginDownloadMonth(month)
        notifyStateChanged()

        guard let context else {
            let message = "未连接远端存储"
            session.failDownloadMonth(month, reason: message)
            notifyStateChanged()
            onAlert?("\(phaseLabel)失败", "\(month.displayText): \(message)")
            return .failed(message)
        }

        let assetIDs = dataAccess.localAssetIDs(month)
        let result = await downloadRemoteMonth(month, assetIDs: assetIDs, context: context)
        return applyDownloadResult(result, month: month, phaseLabel: phaseLabel)
    }

    private func downloadRemoteMonth(
        _ month: LibraryMonthKey,
        assetIDs: Set<String>,
        context: DownloadWorkflowHelper.Context
    ) async -> DownloadMonthResult {
        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled { return .cancelled }
        if !assetIDs.isEmpty {
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return .cancelled }
        }

        let remoteItems = dataAccess.remoteOnlyItems(month)
        return await downloadHelper.downloadItems(remoteItems, context: context) { [weak self] assetID in
            guard let self else { return }
            await self.dataRefresher.refreshLocalIndexAndNotify([assetID])
        }
    }

    @discardableResult
    private func applyDownloadResult(
        _ result: DownloadMonthResult,
        month: LibraryMonthKey,
        phaseLabel: String
    ) -> BackupMonthFinalizationResult {
        switch result {
        case .success:
            session.completeDownloadMonth(month)
            notifyStateChanged()
            return .success
        case .failed(let message):
            session.failDownloadMonth(month, reason: message)
            notifyStateChanged()
            onAlert?("\(phaseLabel)失败", "\(month.displayText): \(message)")
            return .failed(message)
        case .cancelled:
            return .cancelled
        }
    }

    private func assetIDsAwaitingInlineSyncResume() -> Set<String> {
        var assetIDs = Set<String>()
        for (month, plan) in session.monthPlans {
            guard plan.needsUpload && plan.needsDownload else { continue }
            switch plan.phase {
            case .uploadDone, .downloadPaused:
                assetIDs.formUnion(session.uploadAssetIDsByMonth[month] ?? [])
            default:
                break
            }
        }
        return assetIDs
    }

    private var currentControlState: ExecutionControlState {
        if let transientControlState {
            return transientControlState
        }
        guard session.isActive else { return .idle }
        switch backupSessionController?.snapshot().controlPhase {
        case .starting:
            return .starting
        case .resuming:
            return .resuming
        case .pausing:
            return .pausing
        case .stopping:
            return .stopping
        case .idle, .none:
            return .idle
        }
    }

    private func settleDownloadPause(after task: Task<Void, Never>?) {
        guard let task else {
            transientControlState = nil
            notifyStateChanged()
            return
        }

        Task { [weak self] in
            _ = await task.value
            await MainActor.run {
                guard let self,
                      self.transientControlState == .pausing,
                      self.session.phase == .downloadPaused else { return }
                self.transientControlState = nil
                self.notifyStateChanged()
            }
        }
    }

    private func settleStop(after task: Task<Void, Never>?) {
        guard let task else {
            exit()
            return
        }

        Task { [weak self] in
            _ = await task.value
            await MainActor.run {
                guard let self,
                      self.transientControlState == .stopping,
                      self.session.isActive else { return }
                self.exit()
            }
        }
    }
}
