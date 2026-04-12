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
        let completedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
        let failedCountByMonth: [LibraryMonthKey: Int]
    }

    private let backupCoordinator: BackupCoordinator
    private let appSession: AppSession
    private let databaseManager: DatabaseManager
    private let photoLibraryService: PhotoLibraryService

    private var observers: [UUID: @MainActor (Snapshot) -> Void] = [:]
    private var startCommandTask: Task<Void, Never>?
    private var resumePreparationTask: Task<Void, Never>?
    private var notifyThrottleTask: Task<Void, Never>?
    private var hasPendingObserverNotification = false
    private var controlPhase: ControlPhase = .idle
    private var currentRunMode: BackupRunMode = .full
    private var lastPausedRunMode: BackupRunMode?
    private var lastPausedDisplayRunMode: BackupRunMode?
    private var isStartCommandInFlight = false
    private var backupScopeSelection = BackupScopeSelection(
        selectedAssetIDs: nil,
        selectedAssetCount: 0,
        selectedEstimatedBytes: nil,
        totalAssetCount: 0,
        totalEstimatedBytes: nil
    )

    private var runTask: Task<Void, Never>?
    private var eventListenerTask: Task<Void, Never>?
    private var activeEventStream: BackupEventStream?
    private var activeWorkerCountOverride: Int?
    private var activeRunToken: UInt64 = 0
    private var activeTerminationIntent: BackupTerminationIntent = .none

    private(set) var state: State = .idle
    private(set) var statusText: String = "未开始"
    private(set) var succeeded: Int = 0
    private(set) var failed: Int = 0
    private(set) var skipped: Int = 0
    private(set) var total: Int = 0
    private var completedAssetIDsForResume: Set<String> = []
    private(set) var startedMonths = Set<LibraryMonthKey>()
    private(set) var flushedMonths = Set<LibraryMonthKey>()
    private(set) var completedMonths = Set<LibraryMonthKey>()
    private(set) var processedCountByMonth: [LibraryMonthKey: Int] = [:]
    private(set) var failedCountByMonth: [LibraryMonthKey: Int] = [:]

    init(
        backupCoordinator: BackupCoordinator,
        appSession: AppSession,
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService
    ) {
        self.backupCoordinator = backupCoordinator
        self.appSession = appSession
        self.databaseManager = databaseManager
        self.photoLibraryService = photoLibraryService
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
        runTask?.cancel()
        eventListenerTask?.cancel()
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
            completedMonths: completedMonths,
            processedCountByMonth: processedCountByMonth,
            failedCountByMonth: failedCountByMonth
        )
    }

    @discardableResult
    func addObserver(_ observer: @escaping @MainActor (Snapshot) -> Void) -> UUID {
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
            completedMonths.removeAll()
            processedCountByMonth.removeAll()
            failedCountByMonth.removeAll()
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
        startCommandTask?.cancel()
        startCommandTask = Task { [weak self] in
            guard let self else { return }
            if Task.isCancelled {
                self.startCommandTask = nil
                self.resolveStartCancellation(mode: mode)
                return
            }

            do {
                try await self.waitForPreviousRunToClear()
            } catch {
                self.startCommandTask = nil
                if error is CancellationError {
                    self.resolveStartCancellation(mode: mode)
                } else {
                    self.isStartCommandInFlight = false
                    self.controlPhase = .idle
                    self.state = previousState
                    self.statusText = previousStatusText
                    self.notifyObserversNow()
                }
                return
            }

            let runToken = self.startRunInternal(
                profile: profile,
                password: password,
                mode: mode,
                displayMode: mode,
                workerCountOverride: workerCountOverride
            )

            self.startCommandTask = nil
            if Task.isCancelled {
                if runToken != nil {
                    self.isStartCommandInFlight = false
                    self.controlPhase = .idle
                } else {
                    self.resolveStartCancellation(mode: mode)
                }
                return
            }

            guard runToken != nil else {
                self.isStartCommandInFlight = false
                self.controlPhase = .idle
                self.currentRunMode = .full
                self.state = previousState
                self.statusText = previousStatusText
                self.notifyObserversNow()
                return
            }
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
            applyIntent(.pause)
            notifyObserversNow()
            return
        }
        if state != .running {
            guard state == .idle || state == .paused else { return }
            controlPhase = .idle
            state = .paused
            statusText = "备份已暂停"
            notifyObserversNow()
            return
        }

        controlPhase = .pausing
        statusText = "正在暂停..."
        resumePreparationTask?.cancel()
        applyIntent(.pause)
        notifyObserversNow()
    }

    func stopBackup() {
        if controlPhase == .stopping {
            return
        }
        if isStartCommandInFlight {
            controlPhase = .stopping
            statusText = "正在停止..."
            applyIntent(.stop)
            notifyObserversNow()
            return
        }
        if state != .running {
            guard state == .idle || state == .paused || state == .stopped else { return }
            controlPhase = .idle
            state = .stopped
            statusText = "备份已停止"
            notifyObserversNow()
            return
        }

        controlPhase = .stopping
        statusText = "正在停止..."
        resumePreparationTask?.cancel()
        applyIntent(.stop)
        notifyObserversNow()
    }

    // MARK: - Run lifecycle

    private func waitForPreviousRunToClear() async throws {
        while runTask != nil || eventListenerTask != nil || activeEventStream != nil {
            try Task.checkCancellation()
            try await Task.sleep(nanoseconds: 50_000_000)
        }
    }

    private func startRunInternal(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        workerCountOverride: Int?
    ) -> UInt64? {
        guard runTask == nil, eventListenerTask == nil, activeEventStream == nil else {
            return nil
        }

        activeRunToken &+= 1
        let runToken = activeRunToken
        let eventStream = BackupEventStream()
        activeEventStream = eventStream
        activeWorkerCountOverride = workerCountOverride
        activeTerminationIntent = .none

        let capturedRunToken = runToken
        let capturedRunMode = mode
        let capturedDisplayMode = displayMode

        eventListenerTask = Task { [weak self] in
            for await event in eventStream.stream {
                guard let self else { return }
                guard capturedRunToken == self.activeRunToken else { return }
                let shouldStop = self.handleEvent(
                    event,
                    runMode: capturedRunMode,
                    displayMode: capturedDisplayMode,
                    terminalIntent: self.activeTerminationIntent
                )
                if shouldStop { break }
            }
        }

        runTask = Task.detached(priority: .userInitiated) { [weak self, eventStream] in
            guard let self else { return }
            defer {
                eventStream.finish()
            }
            do {
                let request = BackupRunRequest(
                    profile: profile,
                    password: password,
                    onlyAssetLocalIdentifiers: mode.targetAssetIdentifiers,
                    workerCountOverride: workerCountOverride
                )
                _ = try await self.backupCoordinator.runBackup(request: request, eventStream: eventStream)
            } catch {
                await self.handleRunError(
                    error,
                    runToken: capturedRunToken,
                    runMode: capturedRunMode,
                    displayMode: capturedDisplayMode,
                    profile: profile
                )
            }
        }

        if activeTerminationIntent != .none {
            runTask?.cancel()
        }

        return runToken
    }

    private func applyIntent(_ intent: BackupTerminationIntent) {
        activeTerminationIntent = intent
        if runTask != nil {
            runTask?.cancel()
            return
        }

        if isStartCommandInFlight {
            startCommandTask?.cancel()
        }
        resumePreparationTask?.cancel()
    }

    private func clearActiveRunState() {
        runTask = nil
        activeEventStream = nil
        eventListenerTask?.cancel()
        eventListenerTask = nil
        activeTerminationIntent = .none
    }

    private func resolveStartCancellation(mode: BackupRunMode) {
        let phaseBeforeCancel = controlPhase
        controlPhase = .idle
        isStartCommandInFlight = false
        activeTerminationIntent = .none

        let intent: BackupTerminationIntent = (phaseBeforeCancel == .stopping) ? .stop : .pause

        if intent == .stop {
            lastPausedRunMode = nil
            lastPausedDisplayRunMode = nil
            currentRunMode = .full
            state = .stopped
            statusText = "备份已停止"
        } else {
            lastPausedRunMode = mode
            lastPausedDisplayRunMode = mode
            currentRunMode = mode
            state = .paused
            statusText = "备份已暂停"
        }
        notifyObserversNow()
    }

    // MARK: - Event handling

    private func handleEvent(
        _ event: BackupEvent,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) -> Bool {
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
            break

        case .monthChanged(let change):
            let monthKey = LibraryMonthKey(year: change.year, month: change.month)
            switch change.action {
            case .started:
                startedMonths.insert(monthKey)
            case .completed:
                flushedMonths.insert(monthKey)
                completedMonths.insert(monthKey)
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
            clearActiveRunState()
            finishRun(
                result: result,
                runMode: runMode,
                displayMode: displayMode,
                terminalIntent: terminalIntent
            )
            return true
        }
        return false
    }

    @MainActor
    private func handleRunError(
        _ error: Error,
        runToken: UInt64,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        profile: ServerProfileRecord
    ) {
        guard runToken == activeRunToken else { return }

        let intent = activeTerminationIntent
        clearActiveRunState()

        let phaseBeforeFailure = controlPhase
        controlPhase = .idle
        isStartCommandInFlight = false

        let effectiveIntent: BackupTerminationIntent
        if intent != .none {
            effectiveIntent = intent
        } else if error is CancellationError {
            effectiveIntent = (phaseBeforeFailure == .stopping) ? .stop : .pause
        } else {
            effectiveIntent = .none
        }

        if effectiveIntent != .none || error is CancellationError {
            if effectiveIntent == .stop {
                lastPausedRunMode = nil
                lastPausedDisplayRunMode = nil
                currentRunMode = .full
            } else {
                lastPausedRunMode = runMode
                lastPausedDisplayRunMode = displayMode
                currentRunMode = displayMode
            }
            state = effectiveIntent == .stop ? .stopped : .paused
            statusText = effectiveIntent == .stop ? "备份已停止" : "备份已暂停"
            notifyObserversNow()
            return
        }

        let externalUnavailable = profile.isExternalStorageUnavailableError(error)
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

    private func finishRun(
        result: BackupExecutionResult,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) {
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

    // MARK: - Resume

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
                let completedIDs = self.completedAssetIDsForResume

                let pendingAssetIDs: Set<String>
                switch pausedMode {
                case .retry(let assetIDs):
                    try Task.checkCancellation()
                    pendingAssetIDs = assetIDs.subtracting(completedIDs)
                case .scoped(let assetIDs):
                    try Task.checkCancellation()
                    pendingAssetIDs = assetIDs.subtracting(completedIDs)
                case .full:
                    pendingAssetIDs = try await self.computePendingAssetIDsForFullRun(excluding: completedIDs)
                }

                try Task.checkCancellation()

                self.resumePreparationTask = nil
                guard self.state == .running else { return }

                guard !pendingAssetIDs.isEmpty else {
                    // No remaining work -- mark completed.
                    self.controlPhase = .idle
                    self.lastPausedRunMode = nil
                    self.lastPausedDisplayRunMode = nil
                    self.currentRunMode = .full
                    self.state = .completed
                    self.statusText = "备份完成"
                    self.notifyObserversNow()
                    return
                }

                try await self.waitForPreviousRunToClear()

                let resumedExecutionMode: BackupRunMode = pausedMode.isRetry
                    ? .retry(assetIDs: pendingAssetIDs)
                    : .scoped(assetIDs: pendingAssetIDs)

                let runToken = self.startRunInternal(
                    profile: profile,
                    password: password,
                    mode: resumedExecutionMode,
                    displayMode: pausedDisplayMode,
                    workerCountOverride: workerCountOverride ?? self.activeWorkerCountOverride
                )

                if runToken != nil {
                    self.controlPhase = .idle
                    self.currentRunMode = pausedDisplayMode
                    self.notifyObserversNow()
                } else {
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

    // MARK: - Pending asset computation for resume

    private func computePendingAssetIDsForFullRun(
        excluding completedAssetIDs: Set<String>
    ) async throws -> Set<String> {
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

        let assets = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var pending = Set<String>()

        for index in 0 ..< assets.count {
            try Task.checkCancellation()
            let asset = assets.object(at: index)
            if !completedAssetIDs.contains(asset.localIdentifier) {
                pending.insert(asset.localIdentifier)
            }
        }

        return pending
    }

    // MARK: - Observer notification

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
            failedCountByMonth[monthKey, default: 0] += 1
            completedAssetIDsForResume.remove(event.assetLocalIdentifier)
        } else {
            completedAssetIDsForResume.insert(event.assetLocalIdentifier)
        }
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
