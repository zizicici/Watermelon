import Foundation
import Photos

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
    }

    struct Snapshot {
        let state: State
        let statusText: String
        let succeeded: Int
        let failed: Int
        let skipped: Int
        let total: Int
        let startedMonths: Set<LibraryMonthKey>
        let flushedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
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
    private var completedAssetIDsForResume: Set<String> = []
    private(set) var startedMonths = Set<LibraryMonthKey>()
    private(set) var flushedMonths = Set<LibraryMonthKey>()
    private(set) var processedCountByMonth: [LibraryMonthKey: Int] = [:]

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
            statusText: statusText,
            succeeded: succeeded,
            failed: failed,
            skipped: skipped,
            total: total,
            startedMonths: startedMonths,
            flushedMonths: flushedMonths,
            processedCountByMonth: processedCountByMonth
        )
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

    @discardableResult
    func updateScopeSelection(_ selection: BackupScopeSelection) -> Bool {
        guard controlPhase == .idle else { return false }
        switch state {
        case .running, .paused:
            return false
        default:
            break
        }
        backupScopeSelection = selection
        notifyObserversNow()
        return true
    }

    @discardableResult
    private func startBackup(mode: BackupRunMode) -> Bool {
        guard state != .running else {
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            notifyObserversNow()
            return false
        }
        guard let profile = appSession.activeProfile else {
            state = .failed
            statusText = "请先连接远端存储"
            notifyObserversNow()
            return false
        }

        let password: String
        guard let resolvedPassword = resolvePassword(for: profile) else {
            state = .failed
            statusText = "请先连接远端存储"
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
            startedMonths.removeAll()
            flushedMonths.removeAll()
            processedCountByMonth.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        controlPhase = .starting
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
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
            notifyObserversNow()
            return
        }

        controlPhase = .pausing
        statusText = "正在暂停..."
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
            notifyObserversNow()
            return
        }

        controlPhase = .stopping
        statusText = "正在停止..."
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
            notifyObserversNow()
            return
        }

        let profile = failure.profile
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
            if let itemEvent = progress.itemEvent {
                applyProgressEvent(itemEvent)
            }
            scheduleObserverNotification()

        case .log:
            break

        case .transferState:
            scheduleObserverNotification()

        case .monthChanged(let change):
            let monthKey = LibraryMonthKey(year: change.year, month: change.month)
            switch change.action {
            case .started:
                startedMonths.insert(monthKey)
            case .flushed:
                flushedMonths.insert(monthKey)
            case .flushFailed:
                break
            }
            scheduleObserverNotification()

        case .remoteIndexSynced:
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
            notifyObserversNow()
            return
        }

        if result.paused || terminalIntent == .pause {
            lastPausedRunMode = runMode
            lastPausedDisplayRunMode = displayMode
            currentRunMode = displayMode
            state = .paused
            statusText = "备份已暂停"
            notifyObserversNow()
            return
        }

        lastPausedRunMode = nil
        lastPausedDisplayRunMode = nil
        currentRunMode = .full
        state = .completed
        completedAssetIDsForResume.removeAll()
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
        notifyObserversNow()
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

    private func applyProgressEvent(_ event: BackupItemEvent) {
        let monthKey = LibraryMonthKey.from(date: event.resourceDate)
        processedCountByMonth[monthKey, default: 0] += 1

        if event.status == .failed {
            completedAssetIDsForResume.remove(event.assetLocalIdentifier)
        } else {
            completedAssetIDsForResume.insert(event.assetLocalIdentifier)
        }
    }

    @discardableResult
    private func resumeFromPause() -> Bool {
        guard state != .running else {
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            notifyObserversNow()
            return false
        }
        guard let profile = appSession.activeProfile else {
            state = .failed
            statusText = "请先连接远端存储"
            notifyObservers()
            return false
        }
        guard let password = resolvePassword(for: profile) else {
            state = .failed
            statusText = "请先连接远端存储"
            notifyObservers()
            return false
        }

        state = .running
        controlPhase = .resuming
        let pausedMode = lastPausedRunMode ?? .full
        let pausedDisplayMode = lastPausedDisplayRunMode ?? pausedMode
        currentRunMode = pausedDisplayMode
        statusText = "正在准备继续..."
        let workerCountMode = BackupWorkerCountMode.getValue()
        let workerCountOverride = workerCountMode.workerCountOverride
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
                case .started(let runToken, _):
                    self.controlPhase = .idle
                    self.activeCommandRunToken = runToken
                    self.currentRunMode = pausedDisplayMode
                    self.notifyObserversNow()

                case .noPending:
                    self.controlPhase = .idle
                    self.lastPausedRunMode = nil
                    self.lastPausedDisplayRunMode = nil
                    self.currentRunMode = .full
                    self.state = .completed
                    self.statusText = "备份完成"
                    self.notifyObserversNow()
                case .interrupted(let intent):
                    self.controlPhase = .idle
                    if intent == .stop {
                        self.lastPausedRunMode = nil
                        self.lastPausedDisplayRunMode = nil
                        self.currentRunMode = .full
                        self.state = .stopped
                        self.statusText = "备份已停止"
                    } else {
                        self.lastPausedRunMode = pausedMode
                        self.lastPausedDisplayRunMode = pausedDisplayMode
                        self.currentRunMode = pausedDisplayMode
                        self.state = .paused
                        self.statusText = "备份已暂停"
                    }
                    self.notifyObserversNow()

                case .busy:
                    self.controlPhase = .idle
                    self.currentRunMode = .full
                    self.state = .failed
                    self.statusText = "继续备份失败"
                    self.notifyObserversNow()
                }
            } catch is CancellationError {
                self.resumePreparationTask = nil
                let phaseBeforeCancel = self.controlPhase
                self.controlPhase = .idle
                let intent: BackupTerminationIntent = (phaseBeforeCancel == .stopping) ? .stop : .pause
                self.state = intent == .stop ? .stopped : .paused
                self.statusText = intent == .stop ? "备份已停止" : "备份已暂停"
                if intent == .stop {
                    self.lastPausedRunMode = nil
                    self.lastPausedDisplayRunMode = nil
                    self.currentRunMode = .full
                } else {
                    self.lastPausedRunMode = pausedMode
                    self.lastPausedDisplayRunMode = pausedDisplayMode
                    self.currentRunMode = pausedDisplayMode
                }
                self.notifyObserversNow()
            } catch {
                self.resumePreparationTask = nil
                self.controlPhase = .idle
                self.lastPausedRunMode = nil
                self.lastPausedDisplayRunMode = nil
                self.currentRunMode = .full
                self.state = .failed
                self.statusText = "继续备份失败"
                self.notifyObserversNow()
            }
        }

        return true
    }

    private func completedAssetIDs() -> Set<String> {
        completedAssetIDsForResume
    }

    private static let observerNotificationIntervalNanos: UInt64 = 120_000_000

    private func resolvePassword(for profile: ServerProfileRecord) -> String? {
        if profile.storageProfile.requiresPassword {
            guard let activePassword = appSession.activePassword, !activePassword.isEmpty else {
                return nil
            }
            return activePassword
        }
        return appSession.activePassword ?? ""
    }

}
