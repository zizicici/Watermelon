import Foundation
import Photos
import UIKit

@MainActor
final class BackupSessionController {
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
    private var pendingControlIntent: BackupTerminationIntent = .none
    private var currentRunMode: BackupRunMode = .full
    private var lastPausedRunMode: BackupRunMode?
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
    private var processedItemsQueue: [ProcessedItem] = []
    private var processedItemQueueIndexByAssetID: [String: Int] = [:]
    private(set) var latestItemEvent: BackupItemEvent?
    private(set) var transferState: BackupTransferState?
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
        Snapshot(
            state: state,
            primaryActionTitle: primaryActionTitle(for: state),
            controlsLocked: controlsLocked(),
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
            transferState: transferState
        )
    }

    private func processedItemsSnapshot() -> [ProcessedItem] {
        return processedItemsQueue
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
        guard state != .running, !isStartCommandInFlight, resumePreparationTask == nil else {
            appendLog("已有备份任务正在执行")
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
        if profile.storageProfile.requiresPassword {
            guard let activePassword = appSession.activePassword, !activePassword.isEmpty else {
                state = .failed
                statusText = "请先连接远端存储"
                appendLog("错误: 未提供远端存储凭据")
                notifyObserversNow()
                return false
            }
            password = activePassword
        } else {
            password = appSession.activePassword ?? ""
        }

        pendingControlIntent = .none
        currentRunMode = mode
        lastPausedRunMode = nil

        let shouldResetSessionItems =
            state == .idle ||
            state == .completed ||
            state == .failed ||
            (state == .stopped && !mode.isRetry)

        if shouldResetSessionItems {
            clearProcessedItems()
            latestItemEvent = nil
            transferState = nil
            retryCountByAssetID.removeAll()
            failedItemsByAssetID.removeAll()
            failedItems.removeAll()
            logs.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
        appendLog(mode.isRetry ? "开始重试失败 Asset（\(mode.targetCount)）" : "开始备份任务（按 Asset 计数）")
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
                mode: mode
            )
            self.startCommandTask = nil
            if Task.isCancelled {
                return
            }

            guard let startedRunToken else {
                self.isStartCommandInFlight = false
                self.pendingControlIntent = .none
                self.currentRunMode = .full
                self.state = .failed
                self.statusText = "已有备份任务运行中"
                self.transferState = nil
                self.appendLog("错误: 已有备份任务运行中")
                self.notifyObserversNow()
                return
            }
            self.activeCommandRunToken = startedRunToken
        }

        return true
    }

    func pauseBackup() {
        pendingControlIntent = .pause
        if isStartCommandInFlight {
            startCommandTask?.cancel()
            startCommandTask = nil
            isStartCommandInFlight = false
            pendingControlIntent = .none
            state = .paused
            statusText = "备份已暂停"
            transferState = nil
            Task { [runCommandActor] in
                await runCommandActor.cancelActive()
            }
            notifyObserversNow()
            return
        }
        if state != .running {
            Task { [runCommandActor] in
                await runCommandActor.requestPause()
            }
            pendingControlIntent = .none
            state = .paused
            statusText = "备份已暂停"
            transferState = nil
            notifyObserversNow()
            return
        }

        statusText = "正在暂停..."
        transferState = nil
        resumePreparationTask?.cancel()
        Task { [runCommandActor] in
            await runCommandActor.requestPause()
        }
        notifyObserversNow()
    }

    func stopBackup() {
        pendingControlIntent = .stop
        if isStartCommandInFlight {
            startCommandTask?.cancel()
            startCommandTask = nil
            isStartCommandInFlight = false
            pendingControlIntent = .none
            state = .stopped
            statusText = "备份已停止"
            transferState = nil
            Task { [runCommandActor] in
                await runCommandActor.cancelActive()
            }
            notifyObserversNow()
            return
        }
        if state != .running {
            Task { [runCommandActor] in
                await runCommandActor.requestStop()
            }
            pendingControlIntent = .none
            state = .stopped
            statusText = "备份已停止"
            transferState = nil
            notifyObserversNow()
            return
        }

        statusText = "正在停止..."
        transferState = nil
        resumePreparationTask?.cancel()
        Task { [runCommandActor] in
            await runCommandActor.requestStop()
        }
        notifyObserversNow()
    }

    private func handleCommandSignal(_ signal: BackupEngineSignal) async {
        switch signal {
        case .runEvent(let runToken, let runMode, let intent, let event):
            guard runToken == activeCommandRunToken else { return }
            _ = await handleEvent(event, runMode: runMode, terminalIntent: intent)
        case .runFailed(let failure):
            guard failure.runToken == activeCommandRunToken else { return }
            handleRunFailure(failure)
        }
    }

    private func handleRunFailure(_ failure: BackupRunFailureContext) {
        activeCommandRunToken = nil
        isStartCommandInFlight = false
        currentRunMode = .full

        let effectiveIntent: BackupTerminationIntent
        if failure.intent == .none, failure.error is CancellationError {
            effectiveIntent = pendingControlIntent
        } else {
            effectiveIntent = failure.intent
        }

        if effectiveIntent != .none || failure.error is CancellationError {
            pendingControlIntent = .none
            if effectiveIntent == .stop {
                lastPausedRunMode = nil
            } else {
                lastPausedRunMode = failure.runMode
            }
            state = effectiveIntent == .stop ? .stopped : .paused
            statusText = effectiveIntent == .stop ? "备份已停止" : "备份已暂停"
            transferState = nil
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
        pendingControlIntent = .none
        state = .failed
        statusText = externalUnavailable ? "外接存储已断开" : "备份失败"
        transferState = nil
        appendLog("错误: \(userMessage)")
        rebuildFailedItems()
        notifyObserversNow()
    }

    private func handleEvent(
        _ event: BackupEvent,
        runMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) async -> Bool {
        isStartCommandInFlight = false

        switch event {
        case .progress(let progress):
            succeeded = progress.succeeded
            failed = progress.failed
            skipped = progress.skipped
            total = progress.total
            statusText = progress.message
            transferState = progress.transferState
            if let itemEvent = progress.itemEvent {
                applyProgressEvent(itemEvent)
            }
            scheduleObserverNotification()

        case .log(let message):
            appendLog(message)
            scheduleObserverNotification()

        case .transferState(let state):
            transferState = state
            scheduleObserverNotification()

        case .assetCompleted(let completion):
            if isDuplicateAssetCompletion(completion) {
                break
            }

            let updatedAt = Date()
            let item = ProcessedItem(
                assetLocalIdentifier: completion.assetLocalIdentifier,
                displayName: completion.displayName,
                status: completion.status,
                reason: completion.reason,
                resourceSummary: completion.resourceSummary,
                updatedAt: updatedAt
            )
            processedItemsByAssetID[completion.assetLocalIdentifier] = item
            upsertProcessedItemInQueue(item)

            if completion.status == .failed {
                retryCountByAssetID[completion.assetLocalIdentifier, default: 0] += 1
                let retryCount = retryCountByAssetID[completion.assetLocalIdentifier, default: 0]
                failedItemsByAssetID[completion.assetLocalIdentifier] = FailedItem(
                    jobID: 0,
                    assetLocalIdentifier: completion.assetLocalIdentifier,
                    displayName: completion.displayName,
                    errorMessage: completion.reason ?? "未知错误",
                    retryCount: retryCount,
                    updatedAt: updatedAt
                )
            } else {
                failedItemsByAssetID[completion.assetLocalIdentifier] = nil
            }
            rebuildFailedItems()
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
            finishRun(result: result, runMode: runMode, terminalIntent: terminalIntent)
            return true

        case .failed:
            return true
        }
        return false
    }

    private func finishRun(
        result: BackupExecutionResult,
        runMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) {
        activeCommandRunToken = nil
        isStartCommandInFlight = false
        pendingControlIntent = .none
        currentRunMode = .full

        succeeded = result.succeeded
        failed = result.failed
        skipped = result.skipped
        total = result.total

        if terminalIntent == .stop {
            lastPausedRunMode = nil
            state = .stopped
            statusText = "备份已停止"
            transferState = nil
            appendLog("任务已停止")
            rebuildFailedItems()
            notifyObserversNow()
            return
        }

        if result.paused || terminalIntent == .pause {
            lastPausedRunMode = runMode
            state = .paused
            statusText = "备份已暂停"
            transferState = nil
            appendLog("任务已暂停")
            rebuildFailedItems()
            notifyObserversNow()
            return
        }

        lastPausedRunMode = nil
        state = .completed
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
        transferState = nil
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
        isStartCommandInFlight || resumePreparationTask != nil || pendingControlIntent != .none
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
        upsertProcessedItemInQueue(item)

        if event.status == .failed {
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
        } else {
            failedItemsByAssetID[event.assetLocalIdentifier] = nil
        }

        rebuildFailedItems()
    }

    private func isDuplicateAssetCompletion(_ completion: AssetCompletionEvent) -> Bool {
        guard let latest = latestItemEvent else { return false }
        return latest.assetLocalIdentifier == completion.assetLocalIdentifier &&
            latest.assetFingerprint == completion.assetFingerprint &&
            latest.displayName == completion.displayName &&
            latest.status == completion.status &&
            latest.reason == completion.reason &&
            latest.resourceSummary == completion.resourceSummary
    }

    private func clearProcessedItems() {
        processedItemsByAssetID.removeAll()
        processedItemsQueue.removeAll()
        processedItemQueueIndexByAssetID.removeAll()
    }

    private func upsertProcessedItemInQueue(_ item: ProcessedItem) {
        let assetID = item.assetLocalIdentifier

        if let oldIndex = processedItemQueueIndexByAssetID[assetID] {
            processedItemsQueue.remove(at: oldIndex)
            reindexProcessedItemsQueue(from: oldIndex)
        }

        processedItemsQueue.append(item)
        processedItemQueueIndexByAssetID[assetID] = processedItemsQueue.count - 1
    }

    private func reindexProcessedItemsQueue(from start: Int) {
        guard start < processedItemsQueue.count else { return }
        for index in start ..< processedItemsQueue.count {
            processedItemQueueIndexByAssetID[processedItemsQueue[index].assetLocalIdentifier] = index
        }
    }

    @discardableResult
    private func resumeFromPause() -> Bool {
        guard state != .running, !isStartCommandInFlight, resumePreparationTask == nil else {
            appendLog("已有备份任务正在执行")
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
        if profile.storageProfile.requiresPassword,
           (appSession.activePassword ?? "").isEmpty {
            state = .failed
            statusText = "请先连接远端存储"
            appendLog("错误: 未提供远端存储凭据")
            notifyObservers()
            return false
        }

        pendingControlIntent = .none
        state = .running
        statusText = "正在准备继续..."
        appendLog("计算剩余备份 Asset...")
        notifyObserversNow()

        let pausedMode = lastPausedRunMode ?? .full
        resumePreparationTask = Task { [weak self] in
            guard let self else { return }
            do {
                let pendingAssetIDs: Set<String>
                switch pausedMode {
                case .retry(let assetIDs):
                    pendingAssetIDs = assetIDs.subtracting(self.completedAssetIDs())
                case .scoped(let assetIDs):
                    pendingAssetIDs = assetIDs.subtracting(self.completedAssetIDs())
                case .full:
                    pendingAssetIDs = try await self.computePendingAssetIDsForFullRun()
                }

                self.resumePreparationTask = nil

                guard self.state == .running else { return }
                guard !pendingAssetIDs.isEmpty else {
                    self.lastPausedRunMode = nil
                    self.state = .completed
                    self.statusText = "备份完成"
                    self.transferState = nil
                    self.appendLog("无剩余 Asset，已完成")
                    self.rebuildFailedItems()
                    self.notifyObserversNow()
                    return
                }

                self.appendLog("继续备份剩余 \(pendingAssetIDs.count) 个 Asset")
                _ = self.startBackup(mode: .retry(assetIDs: pendingAssetIDs))
            } catch is CancellationError {
                self.resumePreparationTask = nil
                let intent = self.pendingControlIntent
                self.pendingControlIntent = .none
                self.currentRunMode = .full
                self.state = intent == .stop ? .stopped : .paused
                self.statusText = intent == .stop ? "备份已停止" : "备份已暂停"
                self.transferState = nil
                self.appendLog(intent == .stop ? "任务已停止" : "任务已暂停")
                self.rebuildFailedItems()
                self.notifyObserversNow()
            } catch {
                self.resumePreparationTask = nil
                self.pendingControlIntent = .none
                self.currentRunMode = .full
                self.state = .failed
                self.statusText = "继续备份失败"
                self.transferState = nil
                self.appendLog("继续失败: \(error.localizedDescription)")
                self.rebuildFailedItems()
                self.notifyObserversNow()
            }
        }

        return true
    }

    private func completedAssetIDs() -> Set<String> {
        Set(
            processedItemsByAssetID.values
                .filter { item in
                    item.status == .success || item.status == .skipped
                }
                .map(\.assetLocalIdentifier)
        )
    }

    private func computePendingAssetIDsForFullRun() async throws -> Set<String> {
        let status = photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }
        guard authorized else {
            throw BackupError.photoPermissionDenied
        }

        let completed = completedAssetIDs()
        let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var pending = Set<String>()

        for index in 0 ..< assets.count {
            try Task.checkCancellation()
            let asset = assets.object(at: index)
            if !completed.contains(asset.localIdentifier) {
                pending.insert(asset.localIdentifier)
            }
        }

        return pending
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
}
