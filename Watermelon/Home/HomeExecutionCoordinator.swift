import Foundation
import MoreKit

struct HomeExecutionLogEntry {
    let id = UUID()
    let timestamp: Date
    let message: String
    let level: ExecutionLogLevel
}

struct HomeExecutionLogSnapshot {
    let statusText: String
    let entries: [HomeExecutionLogEntry]
}

@MainActor
final class HomeExecutionCoordinator {

    private struct ExecutionSettingsSnapshot {
        let uploadWorkerCountOverride: Int?
        let iCloudPhotoBackupMode: ICloudPhotoBackupMode

        static func fromCurrentSettings() -> ExecutionSettingsSnapshot {
            ExecutionSettingsSnapshot(
                uploadWorkerCountOverride: BackupWorkerCountMode.getValue().workerCountOverride,
                iCloudPhotoBackupMode: ICloudPhotoBackupMode.getValue()
            )
        }

        func makeUploadRunConfiguration(
            forcedWorkerCountOverride: Int?
        ) -> BackupRunConfigurationOverride {
            BackupRunConfigurationOverride(
                workerCountOverride: forcedWorkerCountOverride ?? uploadWorkerCountOverride,
                iCloudPhotoBackupMode: iCloudPhotoBackupMode
            )
        }
    }

    // MARK: - Public State

    var phase: ExecutionPhase? { session.phase }
    var isActive: Bool { session.isActive }
    var currentState: HomeExecutionState? {
        session.currentState(
            controlState: currentControlState,
            statusText: currentStatusText
        )
    }
    var currentLogSnapshot: HomeExecutionLogSnapshot {
        HomeExecutionLogSnapshot(statusText: currentStatusText, entries: logEntries)
    }

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
    private var executionSettingsSnapshot: ExecutionSettingsSnapshot?
    private var forcedUploadWorkerCountOverride: Int?
    private var currentStatusText = "未开始"
    private var logEntries: [HomeExecutionLogEntry] = []
    private var logObservers: [UUID: @MainActor (HomeExecutionLogSnapshot) -> Void] = [:]
    private var backupEventObserverID: UUID?

    private static let syncThrottleInterval: CFAbsoluteTime = 2.0
    private static let localAvailabilityProbeWorkerCount = 2
    private static let localIndexPreflightWorkerCount = 2
    private static let localIndexICloudPreflightWorkerCount = 1

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

    @discardableResult
    func addLogObserver(_ observer: @escaping @MainActor (HomeExecutionLogSnapshot) -> Void) -> UUID {
        let id = UUID()
        logObservers[id] = observer
        observer(currentLogSnapshot)
        return id
    }

    func removeLogObserver(_ id: UUID) {
        logObservers[id] = nil
    }

    // MARK: - Enter / Exit

    func enter(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        executionTask = nil
        transientControlState = nil
        executionSettingsSnapshot = ExecutionSettingsSnapshot.fromCurrentSettings()
        forcedUploadWorkerCountOverride = nil
        dataRefresher.reset()
        logEntries.removeAll(keepingCapacity: true)
        session.enter(upload: upload, download: download, sync: sync, localAssetIDs: dataAccess.localAssetIDs)
        setStatusText("准备执行", notifyState: false)
        appendInfoLog("开始执行：上传 \(upload.count) 个，下载 \(download.count) 个，同步 \(sync.count) 个。")
        backupSessionController = BackupSessionController(dependencies: dependencies)
        backupEventObserverID = backupSessionController.addEventObserver { [weak self] event in
            self?.handleBackupEvent(event)
        }
        backupBridge = BackupSessionAsyncBridge(backupSessionController: backupSessionController)
        downloadHelper = DownloadWorkflowHelper(dependencies: dependencies)
        notifyStateChanged()
        startExecution()
    }

    func exit() {
        executionTask?.cancel()
        executionTask = nil
        transientControlState = nil
        executionSettingsSnapshot = nil
        forcedUploadWorkerCountOverride = nil
        dataRefresher.cancel()
        if let backupEventObserverID {
            backupSessionController?.removeEventObserver(backupEventObserverID)
            self.backupEventObserverID = nil
        }
        backupBridge?.cancel()
        downloadHelper?.cancel()
        session.reset()
        setStatusText("未开始", notifyState: false)
        logEntries.removeAll(keepingCapacity: true)
        notifyLogObservers()
        notifyStateChanged()
    }

    func consumePendingDataChangedMonths() -> Set<LibraryMonthKey> {
        dataRefresher.consumePendingChangedMonths()
    }

    func pause() {
        let uploadRunSnapshot = backupSessionController?.snapshot()
        let shouldPauseBeforeUploadStart =
            uploadRunSnapshot?.state == .idle &&
            uploadRunSnapshot?.controlPhase == .idle

        switch session.pause() {
        case .upload:
            appendInfoLog("请求暂停执行。")
            setStatusText("正在暂停...")
            backupBridge.markAssetIDsPendingForResume(assetIDsAwaitingInlineSyncResume())
            dataRefresher.cancel()
            downloadHelper.cancel()
            if shouldPauseBeforeUploadStart {
                let taskToAwait = executionTask
                executionTask?.cancel()
                executionTask = nil
                transientControlState = .pausing
                notifyStateChanged()
                settleUploadPause(after: taskToAwait)
                return
            }

            backupBridge.requestPause()
            notifyStateChanged()
        case .download:
            appendInfoLog("请求暂停执行。")
            setStatusText("正在暂停...")
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
        appendInfoLog("继续执行。")
        setStatusText("正在恢复...")
        notifyStateChanged()
        startExecution()
    }

    func stop() {
        switch session.phase {
        case .uploading:
            appendWarningLog("请求停止执行。")
            setStatusText("正在停止...")
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
            appendWarningLog("执行已停止。")
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .downloading:
            appendWarningLog("请求停止执行。")
            setStatusText("正在停止...")
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .stopping
            backupBridge.cancel()
            downloadHelper.cancel()
            notifyStateChanged()
            settleStop(after: taskToAwait)
        case .downloadPaused:
            appendWarningLog("执行已停止。")
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
        setErrorStatus(alert.message, log: "执行失败：\(alert.message)")
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

                let prepared = await self.prepareExecutionIfNeeded()
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
                let runConfigurationOverride = self.activeExecutionSettingsSnapshot().makeUploadRunConfiguration(
                    forcedWorkerCountOverride: self.forcedUploadWorkerCountOverride
                )
                let result = await self.backupBridge.runUpload(
                    scope: scope,
                    runConfigurationOverride: runConfigurationOverride,
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
            appendInfoLog("上传阶段完成，开始处理下载任务。")
            setStatusText("准备下载...")
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            notifyStateChanged()
            return true
        case .paused:
            appendWarningLog("执行已暂停。")
            setStatusText("已暂停", notifyState: false)
            notifyStateChanged()
            return false
        case .failed(let alert):
            setErrorStatus(alert.message, log: "上传阶段失败：\(alert.message)")
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        case .exit:
            appendWarningLog("执行已停止。")
            exit()
            return false
        case .finished:
            appendInfoLog("执行阶段完成，正在同步最终结果。")
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return false
        }
    }

    // MARK: - Download Phase

    private func runDownloadPhase() async {
        let remaining = session.remainingDownloadMonths()
        guard !remaining.isEmpty else {
            session.finishExecution()
            appendInfoLog("全部任务处理完成。")
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return
        }

        guard let context = makeDownloadContext() else {
            let alert = session.failForMissingConnection()
            setErrorStatus(alert.message, log: "执行失败：\(alert.message)")
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return
        }

        session.beginDownloadPhase()
        appendInfoLog("开始下载阶段，共 \(remaining.count) 个月份。")
        setStatusText("下载中", notifyState: false)
        notifyStateChanged()

        for month in remaining {
            if Task.isCancelled { return }
            await runDownloadMonth(month, context: context, phaseLabel: session.phaseLabel(for: month))
        }

        if !Task.isCancelled {
            session.finishExecution()
            appendInfoLog("全部任务处理完成。")
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
        }
    }

    private func runDownloadMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context,
        phaseLabel: String
    ) async {
        session.beginDownloadMonth(month)
        appendInfoLog("开始\(phaseLabel)：\(month.displayText)。")
        setStatusText("\(phaseLabel) \(month.displayText)", notifyState: false)
        notifyStateChanged()

        let assetIDs = dataAccess.localAssetIDs(month)
        let result = await downloadRemoteMonth(month, assetIDs: assetIDs, context: context)
        _ = applyDownloadResult(result, month: month, phaseLabel: phaseLabel)
    }

    private func notifyStateChanged() {
        onStateChanged?()
    }

    private func prepareExecutionIfNeeded() async -> Bool {
        if !(await prepareLocalAvailabilityProbeIfNeeded()) {
            return false
        }
        return await prepareLocalIndexIfNeeded()
    }

    private func prepareLocalAvailabilityProbeIfNeeded() async -> Bool {
        forcedUploadWorkerCountOverride = nil

        let settings = activeExecutionSettingsSnapshot()

        guard session.shouldRunUploadPhase else { return true }
        guard settings.iCloudPhotoBackupMode == .enable else { return true }

        let assetIDs = selectedLocalAssetIDsForUploadPhase()
        guard !assetIDs.isEmpty else { return true }

        do {
            let progressHandler = makePreflightProgressHandler()
            appendInfoLog("开始检测 iCloud 资源可用性，共 \(assetIDs.count) 项资源。")
            setStatusText("检测 iCloud 资源可用性")
            let probeResult = try await dependencies.localHashIndexBuildService.probeAvailability(
                for: assetIDs,
                workerCount: Self.localAvailabilityProbeWorkerCount,
                progressHandler: progressHandler
            )
            guard !Task.isCancelled else { return false }

            if probeResult.requiresSingleWorker {
                forcedUploadWorkerCountOverride = 1
                let unavailableCount = probeResult.unavailableAssetIDs.count
                let failedCount = probeResult.failedAssetIDs.count
                print("[HomeExecution] Availability probe requires conservative upload concurrency. unavailable=\(unavailableCount), failed=\(failedCount). Force upload worker count to 1.")
                appendWarningLog("检测到 \(unavailableCount) 项 iCloud 资源未就绪，\(failedCount) 项检测失败。上传并发已降为 1。")
            }
            appendInfoLog("iCloud 资源可用性检测完成。")
            return true
        } catch is CancellationError {
            return false
        } catch {
            let message = "检测 iCloud 资源状态失败：\(error.localizedDescription)"
            let alert = session.failExecution(reason: message)
            transientControlState = nil
            setErrorStatus(message, log: "执行失败：\(message)")
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        }
    }

    private func prepareLocalIndexIfNeeded() async -> Bool {
        let settings = activeExecutionSettingsSnapshot()
        let assetIDs = selectedLocalAssetIDsForExecution()
        guard !assetIDs.isEmpty else {
            session.markLocalIndexPreflightCompleted()
            return true
        }

        do {
            let progressHandler = makePreflightProgressHandler()
            appendInfoLog("开始补齐本地索引，共 \(assetIDs.count) 项资源。")
            setStatusText("补齐本地索引")
            let initialResult = try await dependencies.localHashIndexBuildService.buildIndex(
                for: assetIDs,
                workerCount: Self.localIndexPreflightWorkerCount,
                allowNetworkAccess: false,
                progressHandler: progressHandler
            )
            guard !Task.isCancelled else { return false }

            if !initialResult.readyAssetIDs.isEmpty {
                appendDebugLog("本地索引预检结果开始写回首页：\(initialResult.readyAssetIDs.count) 项。")
                await dataRefresher.refreshLocalIndexAndNotify(initialResult.readyAssetIDs)
                guard !Task.isCancelled else { return false }
                appendDebugLog("首页本地索引刷新完成：\(initialResult.readyAssetIDs.count) 项。")
            }

            let result: LocalHashIndexBuildResult
            if session.requiresCompleteLocalIndexBeforeExecution,
               !initialResult.unavailableAssetIDs.isEmpty,
               settings.iCloudPhotoBackupMode == .enable {
                appendWarningLog("发现 \(initialResult.unavailableAssetIDs.count) 项资源仅存于 iCloud，开始补拉原件。")
                let iCloudResult = try await dependencies.localHashIndexBuildService.buildIndex(
                    for: initialResult.unavailableAssetIDs,
                    workerCount: Self.localIndexICloudPreflightWorkerCount,
                    allowNetworkAccess: true,
                    progressHandler: progressHandler
                )
                guard !Task.isCancelled else { return false }

                if !iCloudResult.readyAssetIDs.isEmpty {
                    appendDebugLog("iCloud 补索引结果开始写回首页：\(iCloudResult.readyAssetIDs.count) 项。")
                    await dataRefresher.refreshLocalIndexAndNotify(iCloudResult.readyAssetIDs)
                    guard !Task.isCancelled else { return false }
                    appendDebugLog("首页本地索引刷新完成：新增 \(iCloudResult.readyAssetIDs.count) 项。")
                }

                result = mergedLocalIndexBuildResult(
                    initial: initialResult,
                    iCloudRecovery: iCloudResult
                )
            } else {
                result = initialResult
            }

            session.markLocalIndexPreflightCompleted()
            appendLog(
                "本地索引补齐完成：就绪 \(result.readyAssetIDs.count) 项，不可用 \(result.unavailableAssetIDs.count) 项，失败 \(result.failedAssetIDs.count) 项。",
                level: result.incompleteAssetIDs.isEmpty ? .info : .warning
            )

            if session.requiresCompleteLocalIndexBeforeExecution,
               !result.incompleteAssetIDs.isEmpty {
                let message = makeLocalIndexIncompleteMessage(
                    from: result,
                    iCloudPhotoBackupMode: settings.iCloudPhotoBackupMode
                )
                let alert = session.failExecution(reason: message)
                transientControlState = nil
                setErrorStatus(message, log: "执行失败：\(message)")
                notifyStateChanged()
                onAlert?(alert.title, alert.message)
                return false
            }

            setStatusText(session.shouldRunUploadPhase ? "准备上传..." : "准备下载...", notifyState: false)
            return true
        } catch is CancellationError {
            return false
        } catch {
            let message = "建立本地索引失败：\(error.localizedDescription)"
            let alert = session.failExecution(reason: message)
            transientControlState = nil
            setErrorStatus(message, log: "执行失败：\(message)")
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        }
    }

    private func activeExecutionSettingsSnapshot() -> ExecutionSettingsSnapshot {
        if let executionSettingsSnapshot {
            return executionSettingsSnapshot
        }

        let snapshot = ExecutionSettingsSnapshot.fromCurrentSettings()
        executionSettingsSnapshot = snapshot
        return snapshot
    }

    private func selectedLocalAssetIDsForExecution() -> Set<String> {
        var assetIDs = Set<String>()
        for month in session.monthPlans.keys {
            assetIDs.formUnion(dataAccess.localAssetIDs(month))
        }
        return assetIDs
    }

    private func selectedLocalAssetIDsForUploadPhase() -> Set<String> {
        var assetIDs = Set<String>()
        for month in session.uploadMonths + session.syncMonths {
            assetIDs.formUnion(dataAccess.localAssetIDs(month))
        }
        return assetIDs
    }

    private func makeLocalIndexIncompleteMessage(
        from result: LocalHashIndexBuildResult,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode
    ) -> String {
        var parts: [String] = []
        if !result.unavailableAssetIDs.isEmpty {
            parts.append("\(result.unavailableAssetIDs.count) 项资源未下载到本机")
        }
        if !result.failedAssetIDs.isEmpty {
            parts.append("\(result.failedAssetIDs.count) 项资源读取失败")
        }
        let detail = parts.joined(separator: "，")
        if !result.unavailableAssetIDs.isEmpty, iCloudPhotoBackupMode == .disable {
            return "本地索引不完整：\(detail)。为避免下载或同步产生重复图片，本次操作已停止。请先在设置中启用“允许访问 iCloud 原件”，或先将这些原件下载到本机。"
        }
        return "本地索引不完整：\(detail)。为避免下载或同步产生重复图片，本次操作已停止。"
    }

    private func mergedLocalIndexBuildResult(
        initial: LocalHashIndexBuildResult,
        iCloudRecovery: LocalHashIndexBuildResult
    ) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: initial.requestedAssetIDs,
            readyAssetIDs: initial.readyAssetIDs.union(iCloudRecovery.readyAssetIDs),
            unavailableAssetIDs: iCloudRecovery.unavailableAssetIDs,
            failedAssetIDs: initial.failedAssetIDs.union(iCloudRecovery.failedAssetIDs),
            missingAssetIDs: initial.missingAssetIDs.union(iCloudRecovery.missingAssetIDs)
        )
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
        appendInfoLog("上传完成，开始\(phaseLabel)：\(month.displayText)。")
        setStatusText("\(phaseLabel) \(month.displayText)", notifyState: false)
        notifyStateChanged()

        guard let context else {
            let message = "未连接远端存储"
            session.failDownloadMonth(month, reason: message)
            setErrorStatus(message, log: "\(phaseLabel)失败：\(month.displayText) - \(message)")
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
        appendInfoLog("同步远端索引：\(month.displayText)。")
        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled { return .cancelled }
        if !assetIDs.isEmpty {
            appendDebugLog("刷新本地索引：\(month.displayText)。")
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return .cancelled }
        }

        let remoteItems = dataAccess.remoteOnlyItems(month)
        appendDebugLog("\(month.displayText) 待下载资源：\(remoteItems.count) 项。")
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
            appendInfoLog("\(phaseLabel)完成：\(month.displayText)。")
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return .success
        case .failed(let message):
            session.failDownloadMonth(month, reason: message)
            setErrorStatus(message, log: "\(phaseLabel)失败：\(month.displayText) - \(message)")
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

    private func handleBackupEvent(_ event: BackupEvent) {
        switch event {
        case .log(let message, let level):
            appendLog(message, level: level)
        case .monthChanged(let change):
            let month = LibraryMonthKey(year: change.year, month: change.month)
            switch change.action {
            case .started:
                appendInfoLog("开始上传：\(month.displayText)。")
            case .completed:
                appendInfoLog("上传完成：\(month.displayText)。")
            }
        case .started(let totalAssets):
            setStatusText("上传中")
            appendInfoLog("上传阶段开始，共 \(totalAssets) 项资源。")
        case .finished(let result):
            appendLog(
                "上传阶段结束：成功 \(result.succeeded) 项，失败 \(result.failed) 项，跳过 \(result.skipped) 项。",
                level: result.failed > 0 ? .warning : .info
            )
        case .progress(let progress):
            appendLog(progress.message, level: progress.logLevel)
        case .transferState:
            break
        }
    }

    private func appendLog(
        _ message: String,
        level: ExecutionLogLevel = .info
    ) {
        let entry = HomeExecutionLogEntry(timestamp: Date(), message: message, level: level)
        logEntries.append(entry)
        if logEntries.count > 800 {
            logEntries.removeFirst(logEntries.count - 800)
        }
        notifyLogObservers()
    }

    private func appendDebugLog(_ message: String) {
        appendLog(message, level: .debug)
    }

    private func appendInfoLog(_ message: String) {
        appendLog(message, level: .info)
    }

    private func appendWarningLog(_ message: String) {
        appendLog(message, level: .warning)
    }

    private func appendErrorLog(_ message: String) {
        appendLog(message, level: .error)
    }

    private func setErrorStatus(_ statusText: String, log logMessage: String) {
        appendErrorLog(logMessage)
        setStatusText(statusText, notifyState: false)
    }

    private func setStatusText(_ text: String, notifyState: Bool = true) {
        guard currentStatusText != text else { return }
        currentStatusText = text
        notifyLogObservers()
        if notifyState {
            onStateChanged?()
        }
    }

    private func refreshTerminalStatus(notifyState: Bool = true) {
        let text: String
        switch session.phase {
        case .completed:
            text = "执行完毕"
        case .failed(let message):
            text = message
        case .uploadPaused, .downloadPaused:
            text = "已暂停"
        case .uploading:
            text = "上传中"
        case .downloading:
            text = "下载中"
        case nil:
            text = "未开始"
        }
        setStatusText(text, notifyState: notifyState)
    }

    private func makePreflightProgressHandler() -> LocalHashIndexProgressHandler {
        { [weak self] message, level in
            guard let coordinator = self else { return }
            await MainActor.run {
                coordinator.appendLog(message, level: level)
            }
        }
    }

    private func notifyLogObservers() {
        let snapshot = currentLogSnapshot
        logObservers.values.forEach { $0(snapshot) }
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
            setStatusText("已暂停", notifyState: false)
            appendWarningLog("执行已暂停。")
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
                self.setStatusText("已暂停", notifyState: false)
                self.appendWarningLog("执行已暂停。")
                self.notifyStateChanged()
            }
        }
    }

    private func settleUploadPause(after task: Task<Void, Never>?) {
        guard let task else {
            transientControlState = nil
            setStatusText("已暂停", notifyState: false)
            appendWarningLog("执行已暂停。")
            notifyStateChanged()
            return
        }

        Task { [weak self] in
            _ = await task.value
            await MainActor.run {
                guard let self,
                      self.transientControlState == .pausing,
                      self.session.phase == .uploadPaused else { return }
                self.transientControlState = nil
                self.setStatusText("已暂停", notifyState: false)
                self.appendWarningLog("执行已暂停。")
                self.notifyStateChanged()
            }
        }
    }

    private func settleStop(after task: Task<Void, Never>?) {
        guard let task else {
            appendWarningLog("执行已停止。")
            exit()
            return
        }

        Task { [weak self] in
            _ = await task.value
            await MainActor.run {
                guard let self,
                      self.transientControlState == .stopping,
                      self.session.isActive else { return }
                self.appendWarningLog("执行已停止。")
                self.exit()
            }
        }
    }
}
