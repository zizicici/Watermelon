import Foundation
import Photos
import UIKit

@MainActor
final class BackupSessionController {
    private enum ControlPhase {
        case idle
        case starting
        case resuming
        case pausing
        case stopping
    }

    enum State {
        case idle
        case running
        case paused
        case stopped
        case failed
        case completed

        var buttonColor: UIColor {
            switch self {
            case .idle:
                return .systemBlue
            case .running:
                return .systemGreen
            case .paused:
                return .systemOrange
            case .stopped:
                return .systemRed
            case .failed:
                return .systemRed
            case .completed:
                return .systemBlue
            }
        }
    }

    struct FailedItem: Hashable, Identifiable {
        let jobID: Int64
        let assetLocalIdentifier: String
        let displayName: String
        let errorMessage: String
        let retryCount: Int
        let updatedAt: Date

        var id: String { assetLocalIdentifier }
    }

    struct ProcessedItem: Hashable, Identifiable {
        let assetLocalIdentifier: String
        let displayName: String
        let status: BackupItemStatus
        let reason: String?
        let resourceSummary: String?
        let updatedAt: Date

        var id: String { assetLocalIdentifier }
    }

    struct Snapshot {
        let state: State
        let primaryActionTitle: String
        let controlsLocked: Bool
        let canStart: Bool
        let canPause: Bool
        let canStop: Bool
        let canAdjustScope: Bool
        let scopeSummary: BackupScopeSummary
        let statusText: String
        let succeeded: Int
        let failed: Int
        let skipped: Int
        let total: Int
        let logs: [String]
        let processedItems: [ProcessedItem]
        let failedItems: [FailedItem]
        let latestItemEvent: BackupItemEvent?
        let transferState: BackupTransferState?
        let transferStatesByWorkerID: [Int: BackupTransferState]
    }

    private let appSession: AppSession
    private let databaseManager: DatabaseManager
    private let photoLibraryService: PhotoLibraryServiceProtocol
    private let runCommandActor: BackupRunCommandActor

    private var observers: [UUID: (Snapshot) -> Void] = [:]
    private var commandSignalTask: Task<Void, Never>?
    private var startCommandTask: Task<Void, Never>?
    private var resumePreparationTask: Task<Void, Never>?
    private var notifyThrottleTask: Task<Void, Never>?
    private var hasPendingObserverNotification = false
    private var controlPhase: ControlPhase = .idle
    private var currentRunMode: BackupRunMode = .full
    private var lastPausedRunMode: BackupRunMode?
    private var lastPausedDisplayRunMode: BackupRunMode?
    private var isStartCommandInFlight = false
    private var activeCommandRunToken: UInt64?
    private var backupScopeSelection = BackupScopeSelection(
        selectedAssetIDs: nil,
        selectedAssetCount: 0,
        selectedEstimatedBytes: nil,
        totalAssetCount: 0,
        totalEstimatedBytes: nil
    )

    private(set) var state: State = .idle
    private(set) var statusText: String = "未开始"
    private(set) var succeeded: Int = 0
    private(set) var failed: Int = 0
    private(set) var skipped: Int = 0
    private(set) var total: Int = 0
    private(set) var logs: [String] = []
    private var processedItemsByAssetID: [String: ProcessedItem] = [:]
    private var processedAssetTimeline: [String] = []
    private var processedAssetTimelineHead = 0
    private var completedAssetIDsForResume: Set<String> = []
    private(set) var latestItemEvent: BackupItemEvent?
    private(set) var transferState: BackupTransferState?
    private(set) var transferStatesByWorkerID: [Int: BackupTransferState] = [:]
    private var retryCountByAssetID: [String: Int] = [:]
    private var failedItemsByAssetID: [String: FailedItem] = [:]
    private(set) var failedItems: [FailedItem] = []

    init(
        backupCoordinator: BackupCoordinatorProtocol,
        appSession: AppSession,
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryServiceProtocol
    ) {
        self.appSession = appSession
        self.databaseManager = databaseManager
        self.photoLibraryService = photoLibraryService
        runCommandActor = BackupRunCommandActor(
            backupCoordinator: backupCoordinator,
            photoLibraryService: photoLibraryService
        )

        let commandActor = runCommandActor
        commandSignalTask = Task { [weak self] in
            let signalStream = await commandActor.makeSignalStream()
            for await signal in signalStream {
                guard let self else { return }
                await self.handleCommandSignal(signal)
            }
        }
    }

    convenience init(dependencies: DependencyContainer) {
        self.init(
            backupCoordinator: dependencies.backupCoordinator,
            appSession: dependencies.appSession,
            databaseManager: dependencies.databaseManager,
            photoLibraryService: dependencies.photoLibraryService
        )
    }

    deinit {
        commandSignalTask?.cancel()
        startCommandTask?.cancel()
        resumePreparationTask?.cancel()
        notifyThrottleTask?.cancel()
    }

    func snapshot() -> Snapshot {
        let availability = buttonAvailability()
        return Snapshot(
            state: state,
            primaryActionTitle: primaryActionTitle(for: state),
            controlsLocked: controlsLocked(),
            canStart: availability.canStart,
            canPause: availability.canPause,
            canStop: availability.canStop,
            canAdjustScope: canAdjustScope(),
            scopeSummary: currentScopeSummary(),
            statusText: statusText,
            succeeded: succeeded,
            failed: failed,
            skipped: skipped,
            total: total,
            logs: logs,
            processedItems: processedItemsSnapshot(),
            failedItems: failedItems,
            latestItemEvent: latestItemEvent,
            transferState: transferState,
            transferStatesByWorkerID: transferStatesByWorkerID
        )
    }

    private func processedItemsSnapshot() -> [ProcessedItem] {
        let start = processedAssetTimelineHead
        guard start < processedAssetTimeline.count else { return [] }
        let reserveCount = min(Self.processedItemsSnapshotLimit, processedAssetTimeline.count - start)
        var items: [ProcessedItem] = []
        items.reserveCapacity(reserveCount)
        var visited = Set<String>()
        visited.reserveCapacity(reserveCount)

        for index in stride(from: processedAssetTimeline.count - 1, through: start, by: -1) {
            let assetID = processedAssetTimeline[index]
            if visited.contains(assetID) {
                continue
            }
            visited.insert(assetID)
            if let item = processedItemsByAssetID[assetID] {
                items.append(item)
                if items.count >= Self.processedItemsSnapshotLimit {
                    break
                }
            }
        }
        return items
    }

    @discardableResult
    func addObserver(_ observer: @escaping (Snapshot) -> Void) -> UUID {
        let id = UUID()
        observers[id] = observer
        observer(snapshot())
        return id
    }

    func removeObserver(_ id: UUID) {
        observers[id] = nil
    }

    @discardableResult
    func startBackup() -> Bool {
        if state == .paused {
            return resumeFromPause()
        }
        if let selectedAssetIDs = backupScopeSelection.selectedAssetIDs {
            return startBackup(mode: .scoped(assetIDs: selectedAssetIDs))
        }
        return startBackup(mode: .full)
    }

    func currentScopeSelection() -> BackupScopeSelection {
        backupScopeSelection
    }

    @discardableResult
    func updateScopeSelection(_ selection: BackupScopeSelection) -> Bool {
        guard canAdjustScope() else { return false }
        backupScopeSelection = selection
        notifyObserversNow()
        return true
    }

    func ensureDefaultScopeSummaryLoaded() async {
        guard backupScopeSelection.totalAssetCount == 0 else { return }
        let totalCount = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true).count
        backupScopeSelection = BackupScopeSelection(
            selectedAssetIDs: nil,
            selectedAssetCount: totalCount,
            selectedEstimatedBytes: nil,
            totalAssetCount: totalCount,
            totalEstimatedBytes: nil
        )
        notifyObserversNow()
    }

    @discardableResult
    func retryFailedItems(assetIDs: Set<String>? = nil) -> Bool {
        let targetAssetIDs = assetIDs ?? Set(failedItems.map(\.assetLocalIdentifier))
        guard !targetAssetIDs.isEmpty else {
            statusText = "没有可重试项"
            appendLog("没有可重试的失败项")
            notifyObservers()
            return false
        }
        return startBackup(mode: .retry(assetIDs: targetAssetIDs))
    }

    func refreshFailedItems() {
        rebuildFailedItems()
        notifyObservers()
    }

    func loadPreviewImage(
        for item: FailedItem,
        targetSize: CGSize = CGSize(width: 1200, height: 1200)
    ) async -> UIImage? {
        let authStatus = photoLibraryService.authorizationStatus()
        guard authStatus == .authorized || authStatus == .limited else { return nil }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [item.assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return nil }

        return await withCheckedContinuation { continuation in
            let manager = PHCachingImageManager()
            let options = PHImageRequestOptions()
            options.deliveryMode = .highQualityFormat
            options.resizeMode = .exact
            options.isNetworkAccessAllowed = true
            options.isSynchronous = false

            manager.requestImage(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFit,
                options: options
            ) { image, info in
                let isDegraded = (info?[PHImageResultIsDegradedKey] as? Bool) ?? false
                if isDegraded { return }
                continuation.resume(returning: image)
            }
        }
    }

    @discardableResult
    private func startBackup(mode: BackupRunMode) -> Bool {
        guard state != .running else {
            appendLog("已有备份任务正在执行")
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            appendLog("控制命令处理中，请稍后")
            notifyObserversNow()
            return false
        }
        guard let profile = appSession.activeProfile else {
            state = .failed
            statusText = "请先连接远端存储"
            appendLog("错误: 未连接远端存储")
            notifyObserversNow()
            return false
        }

        let password: String
        guard let resolvedPassword = resolvePassword(for: profile) else {
            state = .failed
            statusText = "请先连接远端存储"
            appendLog("错误: 未提供远端存储凭据")
            notifyObserversNow()
            return false
        }
        password = resolvedPassword

        currentRunMode = mode
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        let workerCountMode = BackupWorkerCountMode.getValue()
        let workerCountOverride = workerCountMode.workerCountOverride
        let previousState = state
        let previousStatusText = statusText

        let shouldResetSessionItems =
            state == .idle ||
            state == .completed ||
            state == .failed ||
            (state == .stopped && !mode.isRetry)

        completedAssetIDsForResume.removeAll()
        if shouldResetSessionItems {
            clearProcessedItems()
            latestItemEvent = nil
            clearTransferStates()
            retryCountByAssetID.removeAll()
            failedItemsByAssetID.removeAll()
            failedItems.removeAll()
            logs.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        controlPhase = .starting
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
        appendLog(mode.isRetry ? "开始重试失败 Asset（\(mode.targetCount)）" : "开始备份任务（按 Asset 计数）")
        appendLog("上传并发模式：\(workerCountMode.getName())")
        notifyObserversNow()

        isStartCommandInFlight = true
        activeCommandRunToken = nil
        startCommandTask?.cancel()
        startCommandTask = Task { [weak self] in
            guard let self else { return }
            if Task.isCancelled {
                self.startCommandTask = nil
                return
            }
            let startedRunToken = await runCommandActor.startRun(
                profile: profile,
                password: password,
                mode: mode,
                workerCountOverride: workerCountOverride
            )
            self.startCommandTask = nil
            if Task.isCancelled {
                return
            }

            guard let startedRunToken else {
                self.isStartCommandInFlight = false
                self.controlPhase = .idle
                self.currentRunMode = .full
                self.state = previousState
                self.statusText = previousStatusText
                self.clearTransferStates()
                self.appendLog("备份引擎忙，请稍后重试")
                self.notifyObserversNow()
                return
            }
            self.activeCommandRunToken = startedRunToken
            self.isStartCommandInFlight = false
            self.controlPhase = .idle
            self.notifyObserversNow()
        }

        return true
    }

    func pauseBackup() {
        if controlPhase == .stopping {
            return
        }
        if isStartCommandInFlight {
            controlPhase = .pausing
            statusText = "正在暂停..."
            clearTransferStates()
            Task { [runCommandActor] in
                await runCommandActor.requestPause()
            }
            notifyObserversNow()
            return
        }
        if state != .running {
            controlPhase = .idle
            state = .paused
            statusText = "备份已暂停"
            clearTransferStates()
            notifyObserversNow()
            return
        }

        controlPhase = .pausing
        statusText = "正在暂停..."
        clearTransferStates()
        resumePreparationTask?.cancel()
        Task { [runCommandActor] in
            await runCommandActor.requestPause()
        }
        notifyObserversNow()
    }

    func stopBackup() {
        if controlPhase == .stopping {
            return
        }
        if isStartCommandInFlight {
            controlPhase = .stopping
            statusText = "正在停止..."
            clearTransferStates()
            Task { [runCommandActor] in
                await runCommandActor.requestStop()
            }
            notifyObserversNow()
            return
        }
        if state != .running {
            controlPhase = .idle
            state = .stopped
            statusText = "备份已停止"
            clearTransferStates()
            notifyObserversNow()
            return
        }

        controlPhase = .stopping
        statusText = "正在停止..."
        clearTransferStates()
        resumePreparationTask?.cancel()
        Task { [runCommandActor] in
            await runCommandActor.requestStop()
        }
        notifyObserversNow()
    }

    private func handleCommandSignal(_ signal: BackupEngineSignal) async {
        switch signal {
        case .runEvent(let runToken, let runMode, let displayMode, let intent, let event):
            if activeCommandRunToken == nil,
               controlPhase == .starting || controlPhase == .resuming {
                activeCommandRunToken = runToken
            }
            guard runToken == activeCommandRunToken else { return }
            _ = await handleEvent(event, runMode: runMode, displayMode: displayMode, terminalIntent: intent)
        case .runFailed(let failure):
            if activeCommandRunToken == nil,
               controlPhase == .starting || controlPhase == .resuming {
                activeCommandRunToken = failure.runToken
            }
            guard failure.runToken == activeCommandRunToken else { return }
            handleRunFailure(failure)
        }
    }

    private func handleRunFailure(_ failure: BackupRunFailureContext) {
        let phaseBeforeFailure = controlPhase
        activeCommandRunToken = nil
        isStartCommandInFlight = false
        controlPhase = .idle

        let effectiveIntent: BackupTerminationIntent
        if failure.intent != .none {
            effectiveIntent = failure.intent
        } else if failure.error is CancellationError {
            effectiveIntent = (phaseBeforeFailure == .stopping) ? .stop : .pause
        } else {
            effectiveIntent = .none
        }

        if effectiveIntent != .none || failure.error is CancellationError {
            if effectiveIntent == .stop {
                lastPausedRunMode = nil
                lastPausedDisplayRunMode = nil
                currentRunMode = .full
            } else {
                lastPausedRunMode = failure.runMode
                lastPausedDisplayRunMode = failure.displayMode
                currentRunMode = failure.displayMode
            }
            state = effectiveIntent == .stop ? .stopped : .paused
            statusText = effectiveIntent == .stop ? "备份已停止" : "备份已暂停"
            clearTransferStates()
            appendLog(effectiveIntent == .stop ? "任务已停止" : "任务已暂停")
            rebuildFailedItems()
            notifyObserversNow()
            return
        }

        let profile = failure.profile
        let userMessage = profile.userFacingStorageErrorMessage(failure.error)
        let externalUnavailable = profile.isExternalStorageUnavailableError(failure.error)
        if externalUnavailable,
           appSession.activeProfile?.id == profile.id {
            try? databaseManager.setActiveServerProfileID(nil)
            appSession.clear()
        }
        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .failed
        statusText = externalUnavailable ? "外接存储已断开" : "备份失败"
        clearTransferStates()
        appendLog("错误: \(userMessage)")
        rebuildFailedItems()
        notifyObserversNow()
    }

    private func handleEvent(
        _ event: BackupEvent,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) async -> Bool {
        isStartCommandInFlight = false
        currentRunMode = displayMode

        switch event {
        case .progress(let progress):
            succeeded = progress.succeeded
            failed = progress.failed
            skipped = progress.skipped
            total = progress.total
            statusText = progress.message
            if let state = progress.transferState {
                applyTransferState(state)
            }
            if let itemEvent = progress.itemEvent {
                applyProgressEvent(itemEvent)
            }
            scheduleObserverNotification()

        case .log(let message):
            appendLog(message)
            scheduleObserverNotification()

        case .transferState(let state):
            applyTransferState(state)
            scheduleObserverNotification()

        case .monthChanged(let change):
            let monthText = String(format: "%04d年%02d月", change.year, change.month)
            switch change.action {
            case .started:
                appendLog("Processing month \(monthText).")
            case .flushed:
                appendLog("Month \(monthText) manifest flushed.")
            case .flushFailed(let error):
                appendLog("Month \(monthText) manifest flush failed: \(error)")
            }
            scheduleObserverNotification()

        case .remoteIndexSynced(let syncEvent):
            appendLog("Remote index synced. \(syncEvent.resourceCount) resource(s), \(syncEvent.assetCount) asset(s).")
            scheduleObserverNotification()

        case .started(let totalAssets):
            total = totalAssets
            scheduleObserverNotification()

        case .finished(let result):
            finishRun(
                result: result,
                runMode: runMode,
                displayMode: displayMode,
                terminalIntent: terminalIntent
            )
            return true

        case .failed:
            return true
        }
        return false
    }

    private func finishRun(
        result: BackupExecutionResult,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) {
        activeCommandRunToken = nil
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
            clearTransferStates()
            appendLog("任务已停止")
            rebuildFailedItems()
            notifyObserversNow()
            return
        }

        if result.paused || terminalIntent == .pause {
            lastPausedRunMode = runMode
            lastPausedDisplayRunMode = displayMode
            currentRunMode = displayMode
            state = .paused
            statusText = "备份已暂停"
            clearTransferStates()
            appendLog("任务已暂停")
            rebuildFailedItems()
            notifyObserversNow()
            return
        }

        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .completed
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
        clearTransferStates()
        appendLog("完成: 成功\(succeeded) 失败\(result.failed) 跳过\(result.skipped)")
        rebuildFailedItems()
        notifyObserversNow()
    }

    private func rebuildFailedItems() {
        failedItems = failedItemsByAssetID.values.sorted { $0.updatedAt > $1.updatedAt }
    }

    private func notifyObservers() {
        let latest = snapshot()
        observers.values.forEach { $0(latest) }
    }

    private func scheduleObserverNotification() {
        guard !hasPendingObserverNotification else { return }
        hasPendingObserverNotification = true
        notifyThrottleTask?.cancel()
        notifyThrottleTask = Task { [weak self] in
            do {
                try await Task.sleep(nanoseconds: Self.observerNotificationIntervalNanos)
            } catch {
                return
            }
            guard let self else { return }
            self.hasPendingObserverNotification = false
            self.notifyObservers()
        }
    }

    private func notifyObserversNow() {
        notifyThrottleTask?.cancel()
        notifyThrottleTask = nil
        hasPendingObserverNotification = false
        notifyObservers()
    }

    private func controlsLocked() -> Bool {
        controlPhase != .idle
    }

    private func canAdjustScope() -> Bool {
        if controlsLocked() { return false }
        switch state {
        case .running, .paused:
            return false
        default:
            return true
        }
    }

    private func currentScopeSummary() -> BackupScopeSummary {
        let selection = backupScopeSelection
        let mode: BackupScopeSummary.Mode
        if selection.selectedAssetCount <= 0 || selection.totalAssetCount <= 0 {
            mode = .empty
        } else if selection.selectedAssetIDs == nil || selection.selectedAssetCount >= selection.totalAssetCount {
            mode = .all
        } else {
            mode = .partial
        }
        return BackupScopeSummary(
            mode: mode,
            selectedAssetCount: selection.selectedAssetCount,
            selectedEstimatedBytes: selection.selectedEstimatedBytes,
            totalAssetCount: selection.totalAssetCount,
            totalEstimatedBytes: selection.totalEstimatedBytes
        )
    }

    private func appendLog(_ line: String) {
        let timestamp = Self.timeFormatter.string(from: Date())
        logs.append("[\(timestamp)] \(line)")
        if logs.count > 800 {
            logs.removeFirst(logs.count - 800)
        }
    }

    private func applyProgressEvent(_ event: BackupItemEvent) {
        latestItemEvent = event
        let item = ProcessedItem(
            assetLocalIdentifier: event.assetLocalIdentifier,
            displayName: event.displayName,
            status: event.status,
            reason: event.reason,
            resourceSummary: event.resourceSummary,
            updatedAt: event.updatedAt
        )
        processedItemsByAssetID[event.assetLocalIdentifier] = item
        appendProcessedAssetTimeline(event.assetLocalIdentifier)

        var failedItemsChanged = false
        if event.status == .failed {
            completedAssetIDsForResume.remove(event.assetLocalIdentifier)
            retryCountByAssetID[event.assetLocalIdentifier, default: 0] += 1
            let retryCount = retryCountByAssetID[event.assetLocalIdentifier, default: 0]
            failedItemsByAssetID[event.assetLocalIdentifier] = FailedItem(
                jobID: 0,
                assetLocalIdentifier: event.assetLocalIdentifier,
                displayName: event.displayName,
                errorMessage: event.reason ?? "未知错误",
                retryCount: retryCount,
                updatedAt: event.updatedAt
            )
            failedItemsChanged = true
        } else {
            completedAssetIDsForResume.insert(event.assetLocalIdentifier)
            if failedItemsByAssetID.removeValue(forKey: event.assetLocalIdentifier) != nil {
                failedItemsChanged = true
            }
        }

        if failedItemsChanged {
            rebuildFailedItems()
        }
    }

    private func applyTransferState(_ state: BackupTransferState) {
        transferState = state
        transferStatesByWorkerID[state.workerID] = state
    }

    private func clearTransferStates() {
        transferState = nil
        transferStatesByWorkerID.removeAll()
    }

    private func clearProcessedItems() {
        processedItemsByAssetID.removeAll()
        processedAssetTimeline.removeAll()
        processedAssetTimelineHead = 0
        completedAssetIDsForResume.removeAll()
    }

    private func appendProcessedAssetTimeline(_ assetID: String) {
        processedAssetTimeline.append(assetID)
        let liveCount = processedAssetTimeline.count - processedAssetTimelineHead
        if liveCount > Self.processedItemsTimelineCapacity {
            processedAssetTimelineHead += (liveCount - Self.processedItemsTimelineCapacity)
        }
        if processedAssetTimelineHead >= Self.processedItemsTimelineCompactionThreshold,
           processedAssetTimelineHead * 2 >= processedAssetTimeline.count {
            processedAssetTimeline.removeFirst(processedAssetTimelineHead)
            processedAssetTimelineHead = 0
        }
        pruneProcessedItemsIfNeeded()
    }

    private func pruneProcessedItemsIfNeeded() {
        guard processedItemsByAssetID.count > Self.processedItemsMapSoftLimit else {
            return
        }
        let start = processedAssetTimelineHead
        guard start < processedAssetTimeline.count else {
            processedItemsByAssetID.removeAll()
            return
        }

        var liveAssetIDs = Set<String>()
        liveAssetIDs.reserveCapacity(
            min(Self.processedItemsTimelineCapacity, processedAssetTimeline.count - start)
        )
        for index in start ..< processedAssetTimeline.count {
            liveAssetIDs.insert(processedAssetTimeline[index])
        }

        processedItemsByAssetID = processedItemsByAssetID.filter { liveAssetIDs.contains($0.key) }
    }

    @discardableResult
    private func resumeFromPause() -> Bool {
        guard state != .running else {
            appendLog("已有备份任务正在执行")
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            appendLog("控制命令处理中，请稍后")
            notifyObserversNow()
            return false
        }
        guard let profile = appSession.activeProfile else {
            state = .failed
            statusText = "请先连接远端存储"
            appendLog("错误: 未连接远端存储")
            notifyObservers()
            return false
        }
        guard let password = resolvePassword(for: profile) else {
            state = .failed
            statusText = "请先连接远端存储"
            appendLog("错误: 未提供远端存储凭据")
            notifyObservers()
            return false
        }

        state = .running
        controlPhase = .resuming
        let pausedMode = lastPausedRunMode ?? .full
        let pausedDisplayMode = lastPausedDisplayRunMode ?? pausedMode
        currentRunMode = pausedDisplayMode
        statusText = "正在准备继续..."
        appendLog("计算剩余备份 Asset...")
        let workerCountMode = BackupWorkerCountMode.getValue()
        let workerCountOverride = workerCountMode.workerCountOverride
        appendLog("上传并发模式：\(workerCountMode.getName())")
        notifyObserversNow()

        resumePreparationTask = Task { [weak self] in
            guard let self else { return }
            do {
                let outcome = try await self.runCommandActor.resumeRun(
                    profile: profile,
                    password: password,
                    pausedMode: pausedMode,
                    pausedDisplayMode: pausedDisplayMode,
                    completedAssetIDs: self.completedAssetIDs(),
                    workerCountOverride: workerCountOverride
                )

                self.resumePreparationTask = nil
                guard self.state == .running else { return }

                switch outcome {
                case .started(let runToken, let pendingCount):
                    self.controlPhase = .idle
                    self.activeCommandRunToken = runToken
                    self.currentRunMode = pausedDisplayMode
                    self.appendLog("继续备份剩余 \(pendingCount) 个 Asset")
                    self.notifyObserversNow()

                case .noPending:
                    self.controlPhase = .idle
                    self.lastPausedRunMode = nil
                    self.lastPausedDisplayRunMode = nil
                    self.currentRunMode = .full
                    self.state = .completed
                    self.statusText = "备份完成"
                    self.clearTransferStates()
                    self.appendLog("无剩余 Asset，已完成")
                    self.rebuildFailedItems()
                    self.notifyObserversNow()
                case .interrupted(let intent):
                    self.controlPhase = .idle
                    if intent == .stop {
                        self.lastPausedRunMode = nil
                        self.lastPausedDisplayRunMode = nil
                        self.currentRunMode = .full
                        self.state = .stopped
                        self.statusText = "备份已停止"
                        self.appendLog("任务已停止")
                    } else {
                        self.lastPausedRunMode = pausedMode
                        self.lastPausedDisplayRunMode = pausedDisplayMode
                        self.currentRunMode = pausedDisplayMode
                        self.state = .paused
                        self.statusText = "备份已暂停"
                        self.appendLog("任务已暂停")
                    }
                    self.clearTransferStates()
                    self.rebuildFailedItems()
                    self.notifyObserversNow()

                case .busy:
                    self.controlPhase = .idle
                    self.currentRunMode = .full
                    self.state = .failed
                    self.statusText = "继续备份失败"
                    self.clearTransferStates()
                    self.appendLog("错误: 已有备份任务运行中")
                    self.rebuildFailedItems()
                    self.notifyObserversNow()
                }
            } catch is CancellationError {
                self.resumePreparationTask = nil
                let phaseBeforeCancel = self.controlPhase
                self.controlPhase = .idle
                let intent: BackupTerminationIntent = (phaseBeforeCancel == .stopping) ? .stop : .pause
                self.state = intent == .stop ? .stopped : .paused
                self.statusText = intent == .stop ? "备份已停止" : "备份已暂停"
                self.clearTransferStates()
                if intent == .stop {
                    self.lastPausedRunMode = nil
                    self.lastPausedDisplayRunMode = nil
                    self.currentRunMode = .full
                } else {
                    self.lastPausedRunMode = pausedMode
                    self.lastPausedDisplayRunMode = pausedDisplayMode
                    self.currentRunMode = pausedDisplayMode
                }
                self.appendLog(intent == .stop ? "任务已停止" : "任务已暂停")
                self.rebuildFailedItems()
                self.notifyObserversNow()
            } catch {
                self.resumePreparationTask = nil
                self.controlPhase = .idle
                self.lastPausedRunMode = nil
                self.lastPausedDisplayRunMode = nil
                self.currentRunMode = .full
                self.state = .failed
                self.statusText = "继续备份失败"
                self.clearTransferStates()
                self.appendLog("继续失败: \(error.localizedDescription)")
                self.rebuildFailedItems()
                self.notifyObserversNow()
            }
        }

        return true
    }

    private func completedAssetIDs() -> Set<String> {
        completedAssetIDsForResume
    }

    private func primaryActionTitle(for state: State) -> String {
        switch state {
        case .idle:
            return "开始备份"
        case .running:
            return currentRunMode.isRetry ? "重试中..." : "备份中..."
        case .paused, .stopped, .failed:
            return "继续备份"
        case .completed:
            return "开始备份"
        }
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()

    private static let observerNotificationIntervalNanos: UInt64 = 120_000_000
    private static let processedItemsTimelineCapacity = 8_000
    private static let processedItemsTimelineCompactionThreshold = 2_000
    private static let processedItemsSnapshotLimit = 2_000
    private static let processedItemsMapSoftLimit = 12_000

    private func resolvePassword(for profile: ServerProfileRecord) -> String? {
        if profile.storageProfile.requiresPassword {
            guard let activePassword = appSession.activePassword, !activePassword.isEmpty else {
                return nil
            }
            return activePassword
        }
        return appSession.activePassword ?? ""
    }

    private func buttonAvailability() -> (canStart: Bool, canPause: Bool, canStop: Bool) {
        switch controlPhase {
        case .starting, .resuming:
            return (false, true, true)
        case .pausing:
            return (false, false, true)
        case .stopping:
            return (false, false, false)
        case .idle:
            switch state {
            case .running:
                return (false, true, true)
            case .paused:
                return (true, false, true)
            case .idle, .stopped, .failed, .completed:
                return (true, false, false)
            }
        }
    }
}
