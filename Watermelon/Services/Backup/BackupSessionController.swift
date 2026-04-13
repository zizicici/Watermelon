import Foundation
import Photos

@MainActor
final class BackupSessionController {
    private enum StartCommandKind {
        case newRun
        case resume
    }

    private struct StartCommandWaiter {
        let kind: StartCommandKind
        let continuation: CheckedContinuation<Void, Never>
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
        let controlPhase: BackupSessionControlPhase
        let statusText: String
        let succeeded: Int
        let failed: Int
        let skipped: Int
        let total: Int
        let startedMonths: Set<LibraryMonthKey>
        let checkpointedMonths: Set<LibraryMonthKey>
        let completedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
        let failedCountByMonth: [LibraryMonthKey: Int]
    }

    private let resumePlanner: BackupResumePlanner
    private let runDriver: BackupRunDriver
    private let appSession: AppSession
    private let databaseManager: DatabaseManager

    private var session = BackupSessionState()

    private var observers: [UUID: @MainActor (Snapshot) -> Void] = [:]
    private var startCommandTask: Task<Void, Never>?
    private var resumePreparationTask: Task<Void, Never>?
    private var notifyThrottleTask: Task<Void, Never>?
    private var hasPendingObserverNotification = false
    private var startCommandWaiters: [UUID: StartCommandWaiter] = [:]

    private var activeTerminationIntent: BackupTerminationIntent = .none

    private var controlPhase: BackupSessionControlPhase {
        get { session.controlPhase }
        set { session.controlPhase = newValue }
    }

    private var currentRunMode: BackupRunMode {
        get { session.currentRunMode }
        set { session.currentRunMode = newValue }
    }

    private var lastPausedRunMode: BackupRunMode? {
        get { session.lastPausedRunMode }
        set { session.lastPausedRunMode = newValue }
    }

    private var lastPausedDisplayRunMode: BackupRunMode? {
        get { session.lastPausedDisplayRunMode }
        set { session.lastPausedDisplayRunMode = newValue }
    }

    private var isStartCommandInFlight: Bool {
        get { session.isStartCommandInFlight }
        set { session.isStartCommandInFlight = newValue }
    }

    private var backupScopeSelection: BackupScopeSelection {
        get { session.backupScopeSelection }
        set { session.backupScopeSelection = newValue }
    }

    private var completedAssetIDsForResume: Set<String> {
        get { session.completedAssetIDsForResume }
        set { session.completedAssetIDsForResume = newValue }
    }

    private(set) var state: State {
        get { session.state }
        set { session.state = newValue }
    }

    private(set) var statusText: String {
        get { session.statusText }
        set { session.statusText = newValue }
    }

    private(set) var succeeded: Int {
        get { session.succeeded }
        set { session.succeeded = newValue }
    }

    private(set) var failed: Int {
        get { session.failed }
        set { session.failed = newValue }
    }

    private(set) var skipped: Int {
        get { session.skipped }
        set { session.skipped = newValue }
    }

    private(set) var total: Int {
        get { session.total }
        set { session.total = newValue }
    }

    private(set) var startedMonths: Set<LibraryMonthKey> {
        get { session.startedMonths }
        set { session.startedMonths = newValue }
    }

    private(set) var checkpointedMonths: Set<LibraryMonthKey> {
        get { session.checkpointedMonths }
        set { session.checkpointedMonths = newValue }
    }

    private(set) var completedMonths: Set<LibraryMonthKey> {
        get { session.completedMonths }
        set { session.completedMonths = newValue }
    }

    private(set) var processedCountByMonth: [LibraryMonthKey: Int] {
        get { session.processedCountByMonth }
        set { session.processedCountByMonth = newValue }
    }

    private(set) var failedCountByMonth: [LibraryMonthKey: Int] {
        get { session.failedCountByMonth }
        set { session.failedCountByMonth = newValue }
    }

    init(
        backupCoordinator: BackupCoordinator,
        appSession: AppSession,
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService
    ) {
        self.appSession = appSession
        self.databaseManager = databaseManager
        self.resumePlanner = BackupResumePlanner(photoLibraryService: photoLibraryService)
        self.runDriver = BackupRunDriver(backupCoordinator: backupCoordinator)
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
        let runDriver = self.runDriver
        Task { @MainActor in
            runDriver.cancelAll()
        }
        startCommandTask?.cancel()
        resumePreparationTask?.cancel()
        notifyThrottleTask?.cancel()
        for waiter in startCommandWaiters.values {
            waiter.continuation.resume()
        }
    }

    func snapshot() -> Snapshot {
        session.snapshot()
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
        guard session.canUpdateScopeSelection else { return false }
        session.setScopeSelection(selection)
        notifyObserversNow()
        return true
    }

    /// Waits for transient start/stop/pause transitions to settle, then issues the
    /// appropriate start command exactly once. This avoids helper-side polling and
    /// keeps the readiness rules inside BSC.
    @discardableResult
    func startBackupWhenReady(scope: BackupScopeSelection? = nil) async -> Bool {
        if let scope {
            if state == .paused {
                return false
            }
            if state == .running, controlPhase == .idle {
                return false
            }
            if controlPhase != .idle {
                await waitUntilReadyForStartCommand(.newRun)
            }
            guard !Task.isCancelled else { return false }
            guard updateScopeSelection(scope) else { return false }
            return startBackup()
        }

        if state == .paused {
            if controlPhase != .idle {
                await waitUntilReadyForStartCommand(.resume)
                guard !Task.isCancelled else { return false }
            }
            return startBackup()
        }

        if state == .running, controlPhase == .idle {
            return false
        }
        if controlPhase != .idle {
            await waitUntilReadyForStartCommand(.newRun)
        }
        guard !Task.isCancelled else { return false }
        return startBackup()
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
        guard let connection = resolveActiveConnection() else {
            session.failForMissingConnection()
            notifyObserversNow()
            return false
        }

        let workerCountMode = BackupWorkerCountMode.getValue()
        let workerCountOverride = workerCountMode.workerCountOverride
        let startContext = session.prepareForStart(mode: mode)
        notifyObserversNow()

        startCommandTask?.cancel()
        startCommandTask = Task { [weak self] in
            guard let self else { return }
            if Task.isCancelled {
                self.startCommandTask = nil
                self.session.resolveStartCancellation(mode: mode)
                self.activeTerminationIntent = .none
                self.notifyObserversNow()
                return
            }

            do {
                try await self.runDriver.waitForPreviousRunToClear()
            } catch {
                self.startCommandTask = nil
                if error is CancellationError {
                    self.session.resolveStartCancellation(mode: mode)
                    self.activeTerminationIntent = .none
                    self.notifyObserversNow()
                } else {
                    self.session.restoreRejectedStart(using: startContext)
                    self.notifyObserversNow()
                }
                return
            }

            let runToken = self.startRun(
                profile: connection.profile,
                password: connection.password,
                mode: mode,
                displayMode: mode,
                workerCountOverride: workerCountOverride
            )

            self.startCommandTask = nil
            if Task.isCancelled {
                if runToken != nil {
                    self.session.completeAcceptedStartLaunch()
                } else {
                    self.session.resolveStartCancellation(mode: mode)
                    self.activeTerminationIntent = .none
                    self.notifyObserversNow()
                }
                return
            }

            guard runToken != nil else {
                self.session.restoreRejectedStart(using: startContext)
                self.notifyObserversNow()
                return
            }
            self.session.completeAcceptedStartLaunch()
            self.notifyObserversNow()
        }

        return true
    }

    func pauseBackup() {
        if controlPhase == .stopping {
            return
        }
        if isStartCommandInFlight {
            session.beginPauseRequest()
            applyIntent(.pause)
            notifyObserversNow()
            return
        }
        if state != .running {
            guard state == .idle || state == .paused else { return }
            session.transitionToPausedWithoutRun()
            notifyObserversNow()
            return
        }

        session.beginPauseRequest()
        resumePreparationTask?.cancel()
        applyIntent(.pause)
        notifyObserversNow()
    }

    func stopBackup() {
        if controlPhase == .stopping {
            return
        }
        if isStartCommandInFlight {
            session.beginStopRequest()
            applyIntent(.stop)
            notifyObserversNow()
            return
        }
        if state != .running {
            guard state == .idle || state == .paused || state == .stopped else { return }
            session.transitionToStoppedWithoutRun()
            notifyObserversNow()
            return
        }

        session.beginStopRequest()
        resumePreparationTask?.cancel()
        applyIntent(.stop)
        notifyObserversNow()
    }

    // MARK: - Run lifecycle

    private func startRun(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        workerCountOverride: Int?
    ) -> UInt64? {
        activeTerminationIntent = .none
        let runToken = runDriver.startRun(
            profile: profile,
            password: password,
            mode: mode,
            displayMode: displayMode,
            workerCountOverride: workerCountOverride,
            terminalIntentProvider: { [weak self] in
                self?.activeTerminationIntent ?? .none
            },
            onEvent: { [weak self] event, runMode, displayMode, terminalIntent in
                self?.handleEvent(
                    event,
                    runMode: runMode,
                    displayMode: displayMode,
                    terminalIntent: terminalIntent
                ) ?? true
            },
            onError: { [weak self] error, runToken, runMode, displayMode, profile in
                self?.handleRunError(
                    error,
                    runToken: runToken,
                    runMode: runMode,
                    displayMode: displayMode,
                    profile: profile
                )
            }
        )

        if activeTerminationIntent != .none {
            runDriver.cancelRunTask()
        }

        return runToken
    }

    private func applyIntent(_ intent: BackupTerminationIntent) {
        activeTerminationIntent = intent
        if runDriver.hasActiveRunTask {
            runDriver.cancelRunTask()
            return
        }

        if isStartCommandInFlight {
            startCommandTask?.cancel()
        }
        resumePreparationTask?.cancel()
    }

    // MARK: - Event handling

    private func handleEvent(
        _ event: BackupEvent,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminalIntent: BackupTerminationIntent
    ) -> Bool {
        if case .finished = event {
            runDriver.clearActiveRunState()
            activeTerminationIntent = .none
        }

        let outcome = session.reduce(
            event: event,
            runMode: runMode,
            displayMode: displayMode,
            terminalIntent: terminalIntent
        )

        switch outcome.notification {
        case .none:
            break
        case .throttled:
            scheduleObserverNotification()
        case .immediate:
            notifyObserversNow()
        }

        return outcome.shouldStop
    }

    @MainActor
    private func handleRunError(
        _ error: Error,
        runToken: UInt64,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        profile: ServerProfileRecord
    ) {
        guard runDriver.matchesActiveRunToken(runToken) else { return }

        let intent = activeTerminationIntent
        runDriver.clearActiveRunState()
        activeTerminationIntent = .none

        let phaseBeforeFailure = controlPhase
        let externalUnavailable = profile.isExternalStorageUnavailableError(error)
        handleExternalStorageUnavailableIfNeeded(error, for: profile)
        session.applyRunError(
            error,
            runMode: runMode,
            displayMode: displayMode,
            externalUnavailable: externalUnavailable,
            intent: intent,
            phaseBeforeFailure: phaseBeforeFailure
        )
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
        guard let connection = resolveActiveConnection() else {
            session.failForMissingConnection()
            notifyObservers()
            return false
        }

        let resumeContext = session.prepareForResume()
        let workerCountMode = BackupWorkerCountMode.getValue()
        let workerCountOverride = workerCountMode.workerCountOverride
        notifyObserversNow()

        resumePreparationTask = Task { [weak self] in
            guard let self else { return }
            do {
                let resumePlan = try await self.resumePlanner.makePlan(
                    pausedMode: resumeContext.pausedMode,
                    completedAssetIDs: self.completedAssetIDsForResume
                )
                try Task.checkCancellation()

                self.resumePreparationTask = nil
                guard self.state == .running else { return }

                guard let resumedExecutionMode = resumePlan.resumedExecutionMode else {
                    self.session.completeResumeWithoutPendingWork()
                    self.notifyObserversNow()
                    return
                }

                try await self.runDriver.waitForPreviousRunToClear()

                let runToken = self.startRun(
                    profile: connection.profile,
                    password: connection.password,
                    mode: resumedExecutionMode,
                    displayMode: resumeContext.pausedDisplayMode,
                    workerCountOverride: workerCountOverride ?? self.runDriver.activeWorkerCountOverride
                )

                if runToken != nil {
                    self.session.completeResumeLaunchSucceeded(displayMode: resumeContext.pausedDisplayMode)
                    self.notifyObserversNow()
                } else {
                    self.session.completeResumeLaunchFailed()
                    self.notifyObserversNow()
                }

            } catch is CancellationError {
                self.resumePreparationTask = nil
                self.session.cancelResume(
                    pausedMode: resumeContext.pausedMode,
                    pausedDisplayMode: resumeContext.pausedDisplayMode
                )
                self.notifyObserversNow()
            } catch {
                self.resumePreparationTask = nil
                self.session.failResumePreparation()
                self.notifyObserversNow()
            }
        }

        return true
    }

    // MARK: - Observer notification

    private func notifyObservers() {
        resumeStartCommandWaitersIfNeeded()
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

    private func resolveActiveConnection() -> (profile: ServerProfileRecord, password: String)? {
        guard let profile = appSession.activeProfile,
              let password = resolvePassword(for: profile) else {
            return nil
        }
        return (profile, password)
    }

    private func handleExternalStorageUnavailableIfNeeded(
        _ error: Error,
        for profile: ServerProfileRecord
    ) {
        guard profile.isExternalStorageUnavailableError(error),
              appSession.activeProfile?.id == profile.id else { return }
        try? databaseManager.setActiveServerProfileID(nil)
        appSession.clear()
    }

    private func resolvePassword(for profile: ServerProfileRecord) -> String? {
        if profile.storageProfile.requiresPassword {
            guard let activePassword = appSession.activePassword, !activePassword.isEmpty else {
                return nil
            }
            return activePassword
        }
        return appSession.activePassword ?? ""
    }

    private func canProcessStartCommand(_ kind: StartCommandKind) -> Bool {
        guard controlPhase == .idle else { return false }
        switch kind {
        case .newRun:
            return state != .running && state != .paused
        case .resume:
            return state == .paused
        }
    }

    private func waitUntilReadyForStartCommand(_ kind: StartCommandKind) async {
        guard !canProcessStartCommand(kind) else { return }

        let waiterID = UUID()
        await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                if Task.isCancelled || self.canProcessStartCommand(kind) {
                    continuation.resume()
                    return
                }
                self.startCommandWaiters[waiterID] = StartCommandWaiter(kind: kind, continuation: continuation)
            }
        } onCancel: {
            Task { @MainActor [weak self] in
                guard let self,
                      let waiter = self.startCommandWaiters.removeValue(forKey: waiterID) else { return }
                waiter.continuation.resume()
            }
        }
    }

    private func resumeStartCommandWaitersIfNeeded() {
        let readyWaiters = startCommandWaiters.filter { canProcessStartCommand($0.value.kind) }
        guard !readyWaiters.isEmpty else { return }

        for (id, waiter) in readyWaiters {
            startCommandWaiters.removeValue(forKey: id)
            waiter.continuation.resume()
        }
    }

    private static let observerNotificationIntervalNanos: UInt64 = 120_000_000
}
