import Foundation

@MainActor
final class HomeExecutionCoordinator {

    // MARK: - Public State

    var phase: ExecutionPhase? { session.currentPhase }
    var isActive: Bool { session.isActive }
    var currentState: HomeExecutionState? { session.currentState }

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
    private var executionTask: Task<Void, Never>?
    private var remoteSyncTask: Task<Void, Never>?
    private var remoteSyncRequested = false
    private var remoteSyncWaiters: [CheckedContinuation<Set<LibraryMonthKey>, Never>] = []
    private var pendingDataChangedMonths = Set<LibraryMonthKey>()
    private var backupSessionController: BackupSessionController!
    private var uploadHelper: UploadWorkflowHelper!
    private var downloadHelper: DownloadWorkflowHelper!

    private static let syncThrottleInterval: CFAbsoluteTime = 2.0

    init(dependencies: DependencyContainer, dataAccess: DataAccess) {
        self.dependencies = dependencies
        self.dataAccess = dataAccess
    }

    // MARK: - Enter / Exit

    func enter(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        executionTask = nil
        remoteSyncRequested = false
        pendingDataChangedMonths.removeAll()
        session.enter(upload: upload, download: download, sync: sync, localAssetIDs: dataAccess.localAssetIDs)
        backupSessionController = BackupSessionController(dependencies: dependencies)
        uploadHelper = UploadWorkflowHelper(backupSessionController: backupSessionController)
        downloadHelper = DownloadWorkflowHelper(
            dependencies: dependencies,
            backupSessionController: backupSessionController
        )
        notifyStateChanged()
        startExecution()
    }

    func exit() {
        executionTask?.cancel()
        executionTask = nil
        remoteSyncTask?.cancel()
        remoteSyncTask = nil
        remoteSyncRequested = false
        uploadHelper?.cancel()
        downloadHelper?.cancel()
        let remoteSyncWaiters = self.remoteSyncWaiters
        self.remoteSyncWaiters.removeAll()
        pendingDataChangedMonths.removeAll()
        for waiter in remoteSyncWaiters {
            waiter.resume(returning: [])
        }
        session.reset()
        notifyStateChanged()
    }

    func consumePendingDataChangedMonths() -> Set<LibraryMonthKey> {
        defer { pendingDataChangedMonths.removeAll() }
        return pendingDataChangedMonths
    }

    func pause() {
        switch session.pause() {
        case .upload:
            executionTask?.cancel()
            executionTask = nil
            uploadHelper.pause()
            notifyStateChanged()
        case .download:
            executionTask?.cancel()
            executionTask = nil
            downloadHelper.cancel()
            notifyStateChanged()
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
            uploadHelper.stop()
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

            if self.session.shouldRunUploadPhase {
                guard !Task.isCancelled else { return }
                let scope = self.session.consumePendingUploadScope()
                let result = await self.uploadHelper.runUpload(scope: scope) { [weak self] progress in
                    self?.handleUploadProgress(progress)
                }
                guard !Task.isCancelled else { return }
                guard await self.handleUploadResult(result) else { return }
            }

            await self.runDownloadPhase()
        }
    }

    // MARK: - Upload Phase

    private func handleUploadProgress(_ progress: UploadWorkflowHelper.UploadProgress) {
        let shouldSyncRemoteData = session.handleUploadProgress(
            progress,
            now: CFAbsoluteTimeGetCurrent(),
            syncThrottleInterval: Self.syncThrottleInterval
        )
        if shouldSyncRemoteData {
            scheduleRemoteSync()
        }
        notifyStateChanged()
    }

    @discardableResult
    private func handleUploadResult(_ result: UploadWorkflowHelper.UploadResult) async -> Bool {
        switch session.handleUploadResult(result) {
        case .continueToDownload:
            _ = await syncRemoteDataAndWait()
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
            _ = await syncRemoteDataAndWait()
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
            let ok = await downloadHelper.runScopedBackup(assetIDs: assetIDs) { [weak self] in
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

        _ = await syncRemoteDataAndWait()
        if !assetIDs.isEmpty {
            await refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return }
        }

        let remoteItems = dataAccess.remoteOnlyItems(month)
        let result = await downloadHelper.downloadItems(remoteItems, context: context) { [weak self] assetID in
            guard let self else { return }
            await self.refreshLocalIndexAndNotify([assetID])
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

    private func refreshLocalIndexAndNotify(_ assetIDs: Set<String>) async {
        let changedMonths = await dataAccess.refreshLocalIndex(assetIDs)
        guard !changedMonths.isEmpty else { return }
        pendingDataChangedMonths.formUnion(changedMonths)
        notifyStateChanged()
    }

    private func scheduleRemoteSync() {
        remoteSyncRequested = true
        ensureRemoteSyncTask()
    }

    private func syncRemoteDataAndWait() async -> Set<LibraryMonthKey> {
        remoteSyncRequested = true
        ensureRemoteSyncTask()
        return await withCheckedContinuation { continuation in
            remoteSyncWaiters.append(continuation)
        }
    }

    private func ensureRemoteSyncTask() {
        guard remoteSyncTask == nil else { return }

        remoteSyncTask = Task { [weak self] in
            guard let self else { return }

            var aggregatedChangedMonths = Set<LibraryMonthKey>()
            while self.remoteSyncRequested {
                self.remoteSyncRequested = false
                if Task.isCancelled { break }

                let changedMonths = await self.dataAccess.syncRemoteData()
                if Task.isCancelled { break }
                aggregatedChangedMonths.formUnion(changedMonths)
                self.pendingDataChangedMonths.formUnion(changedMonths)

                if !changedMonths.isEmpty {
                    self.notifyStateChanged()
                }
            }

            let waiters = self.remoteSyncWaiters
            self.remoteSyncWaiters.removeAll()
            self.remoteSyncTask = nil

            for waiter in waiters {
                waiter.resume(returning: aggregatedChangedMonths)
            }
        }
    }
}
