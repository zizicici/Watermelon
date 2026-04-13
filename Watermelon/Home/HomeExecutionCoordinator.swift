import Foundation

@MainActor
final class HomeExecutionCoordinator {

    // MARK: - Public State

    var phase: ExecutionPhase? { session.currentPhase }
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
            executionTask?.cancel()
            executionTask = nil
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
        guard session.resume() != nil else { return }
        notifyStateChanged()
        startExecution()
    }

    func stop() {
        switch session.currentPhase {
        case .uploading:
            transientControlState = .stopping
            notifyStateChanged()
            backupBridge.requestStop()
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
        guard let phase = session.currentPhase else { return }
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

        let alert = session.failExecutionForMissingConnection()
        notifyStateChanged()
        onAlert?(alert.title, alert.message)
    }

    // MARK: - Execution Task

    private func startExecution() {
        executionTask = Task { [weak self] in
            guard let self else { return }

            if self.session.shouldRunUploadPhase {
                guard !Task.isCancelled else { return }
                let scope = self.session.consumePendingUploadScope()
                let result = await self.backupBridge.runUpload(scope: scope) { [weak self] progress in
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

        guard let profile = dependencies.appSession.activeProfile,
              let password = profile.resolvedSessionPassword(from: dependencies.appSession) else {
            let alert = session.failRemainingDownloadsForMissingConnection()
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return
        }

        session.beginDownloadPhase()
        notifyStateChanged()

        let context = DownloadWorkflowHelper.Context(profile: profile, password: password)

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
        if !assetIDs.isEmpty {
            let ok = await backupBridge.runScopedBackup(assetIDs: assetIDs) { [weak self] in
                self?.notifyStateChanged()
            }
            if Task.isCancelled { return }
            if !ok {
                session.failDownloadMonth(month, reason: "备份索引失败")
                notifyStateChanged()
                onAlert?("\(phaseLabel)失败", "\(month.displayText): 备份索引失败")
                return
            }
        }

        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled { return }
        if !assetIDs.isEmpty {
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return }
        }

        let remoteItems = dataAccess.remoteOnlyItems(month)
        let result = await downloadHelper.downloadItems(remoteItems, context: context) { [weak self] assetID in
            guard let self else { return }
            await self.dataRefresher.refreshLocalIndexAndNotify([assetID])
        }

        switch result {
        case .success:
            session.completeDownloadMonth(month)
            notifyStateChanged()
        case .failed(let message):
            session.failDownloadMonth(month, reason: message)
            notifyStateChanged()
            onAlert?("\(phaseLabel)失败", "\(month.displayText): \(message)")
        case .cancelled:
            break
        }
    }

    private func notifyStateChanged() {
        onStateChanged?()
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
                      self.session.currentPhase == .downloadPaused else { return }
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
