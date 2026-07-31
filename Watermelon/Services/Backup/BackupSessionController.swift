import Foundation
import MoreKit
import Photos

@MainActor
final class BackupSessionController {
    private enum StartCommandKind: Equatable {
        case newRun
        case resume
    }

    private struct PendingCommand {
        let id: UUID
        let kind: StartCommandKind
        let control: ExecutionTerminationControl
        let task: Task<Void, Never>
    }

    private struct StartCommandWaiter {
        let kind: StartCommandKind
        let continuation: CheckedContinuation<Void, Never>
    }

    enum State: Equatable {
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
        let completedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
        let failedCountByMonth: [LibraryMonthKey: Int]
    }

    private let resumePlanner: any BackupResumePlanning
    private let runDriver: any BackupRunDriving
    private let appSession: AppSession
    private let databaseManager: DatabaseManager

    private var session = BackupSessionState()

    private var observers: [UUID: @MainActor (Snapshot) -> Void] = [:]
    private var eventObservers: [UUID: @MainActor (BackupEvent) -> Void] = [:]
    private var pendingCommand: PendingCommand?
    private var hardCancellationTask: Task<Void, Never>?
    private var commandGeneration: UInt64 = 0
    private var notifyThrottleTask: Task<Void, Never>?
    private var hasPendingObserverNotification = false
    private var startCommandWaiters: [UUID: StartCommandWaiter] = [:]

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

    convenience init(
        backupCoordinator: BackupCoordinator,
        appSession: AppSession,
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService
    ) {
        self.init(
            runDriver: BackupRunDriver(backupCoordinator: backupCoordinator),
            appSession: appSession,
            databaseManager: databaseManager,
            photoLibraryService: photoLibraryService
        )
    }

    init(
        runDriver: any BackupRunDriving,
        appSession: AppSession,
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService,
        resumePlanner: (any BackupResumePlanning)? = nil
    ) {
        self.appSession = appSession
        self.databaseManager = databaseManager
        self.resumePlanner = resumePlanner
            ?? BackupResumePlanner(photoLibraryService: photoLibraryService)
        self.runDriver = runDriver
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
            _ = runDriver.cancelActiveRunImmediately()
        }
        pendingCommand?.control.request(.stop)
        pendingCommand?.task.cancel()
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
    func addEventObserver(_ observer: @escaping @MainActor (BackupEvent) -> Void) -> UUID {
        let id = UUID()
        eventObservers[id] = observer
        return id
    }

    func removeEventObserver(_ id: UUID) {
        eventObservers[id] = nil
    }

    @discardableResult
    func startBackup(
        runConfigurationOverride: BackupRunConfigurationOverride? = nil,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) -> Bool {
        if state == .paused {
            return resumeFromPause(onMonthUploaded: onMonthUploaded)
        }
        if let selectedAssetIDs = backupScopeSelection.selectedAssetIDs {
            return startBackup(
                mode: .scoped(assetIDs: selectedAssetIDs),
                runConfigurationOverride: runConfigurationOverride,
                onMonthUploaded: onMonthUploaded
            )
        }
        return startBackup(
            mode: .full,
            runConfigurationOverride: runConfigurationOverride,
            onMonthUploaded: onMonthUploaded
        )
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
    func startBackupWhenReady(
        scope: BackupScopeSelection? = nil,
        runConfigurationOverride: BackupRunConfigurationOverride? = nil,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) async -> Bool {
        guard hardCancellationTask == nil else { return false }
        let generation = commandGeneration

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
            guard !Task.isCancelled, generation == commandGeneration else { return false }
            guard updateScopeSelection(scope) else { return false }
            return startBackup(
                runConfigurationOverride: runConfigurationOverride,
                onMonthUploaded: onMonthUploaded
            )
        }

        if state == .paused {
            if controlPhase != .idle {
                await waitUntilReadyForStartCommand(.resume)
                guard !Task.isCancelled, generation == commandGeneration else { return false }
            }
            return startBackup(onMonthUploaded: onMonthUploaded)
        }

        if state == .running, controlPhase == .idle {
            return false
        }
        if controlPhase != .idle {
            await waitUntilReadyForStartCommand(.newRun)
        }
        guard !Task.isCancelled, generation == commandGeneration else { return false }
        return startBackup(
            runConfigurationOverride: runConfigurationOverride,
            onMonthUploaded: onMonthUploaded
        )
    }

    @discardableResult
    private func startBackup(
        mode: BackupRunMode,
        runConfigurationOverride configurationOverride: BackupRunConfigurationOverride?,
        onMonthUploaded: BackupMonthFinalizer?
    ) -> Bool {
        guard hardCancellationTask == nil else { return false }
        guard state != .running else {
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            notifyObserversNow()
            return false
        }
        guard pendingCommand == nil else {
            notifyObserversNow()
            return false
        }
        guard let connection = resolveActiveConnection() else {
            session.failForMissingConnection()
            notifyObserversNow()
            return false
        }

        let configuration = resolveRunConfiguration(
            profile: connection.profile,
            override: configurationOverride
        )
        let startContext = session.prepareForStart(mode: mode, configuration: configuration)
        let commandID = UUID()
        let commandControl = ExecutionTerminationControl()
        let commandTask = Task { [weak self] in
            guard let self,
                  self.matchesPendingCommand(commandID, kind: .newRun) else { return }
            do {
                try Task.checkCancellation()
                try commandControl.checkDrainRequested()
                try await self.runDriver.waitForPreviousRunToClear()
                try Task.checkCancellation()
                try commandControl.checkDrainRequested()
                guard self.matchesPendingCommand(commandID, kind: .newRun),
                      self.state == .running,
                      self.controlPhase == .starting else { throw CancellationError() }

                let runToken = self.startRun(
                    profile: connection.profile,
                    password: connection.password,
                    mode: mode,
                    displayMode: mode,
                    configuration: configuration,
                    onMonthUploaded: onMonthUploaded,
                    terminationControl: commandControl
                )

                guard self.clearPendingCommand(commandID, kind: .newRun) else { return }
                if runToken != nil {
                    self.session.completeAcceptedStartLaunch()
                } else {
                    self.session.restoreRejectedStart(using: startContext)
                }
                self.notifyObserversNow()
            } catch is CancellationError {
                guard self.clearPendingCommand(commandID, kind: .newRun) else { return }
                self.session.resolveStartCancellation(mode: mode)
                self.notifyObserversNow()
            } catch {
                guard self.clearPendingCommand(commandID, kind: .newRun) else { return }
                self.session.restoreRejectedStart(using: startContext)
                self.notifyObserversNow()
            }
        }
        pendingCommand = PendingCommand(
            id: commandID,
            kind: .newRun,
            control: commandControl,
            task: commandTask
        )
        notifyObserversNow()

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
        applyIntent(.stop)
        notifyObserversNow()
    }

    func markAssetIDsPendingForResume(_ assetIDs: Set<String>) {
        guard !assetIDs.isEmpty else { return }
        completedAssetIDsForResume.subtract(assetIDs)
    }

    func cancelBackupImmediately() -> Task<Void, Never> {
        if let hardCancellationTask {
            return hardCancellationTask
        }

        commandGeneration &+= 1
        let pendingCommand = self.pendingCommand
        pendingCommand?.control.request(.stop)
        pendingCommand?.task.cancel()
        let pendingRun = runDriver.cancelActiveRunImmediately()
        let runDriver = self.runDriver
        let settlement = Task { @MainActor [weak self] in
            if let pendingCommand {
                _ = await pendingCommand.task.value
            }
            if let pendingRun {
                _ = await pendingRun.value
            }
            if let racedRun = runDriver.cancelActiveRunImmediately() {
                _ = await racedRun.value
            }
            self?.hardCancellationTask = nil
        }
        hardCancellationTask = settlement
        invalidateStartCommandWaiters()
        return settlement
    }

    // MARK: - Run lifecycle

    private func startRun(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded: BackupMonthFinalizer? = nil,
        terminationControl: ExecutionTerminationControl
    ) -> UInt64? {
        guard !terminationControl.shouldDrain else { return nil }
        let runToken = runDriver.startRun(
            profile: profile,
            password: password,
            mode: mode,
            displayMode: displayMode,
            configuration: configuration,
            onMonthUploaded: onMonthUploaded,
            terminationControl: terminationControl,
            onEvent: { [weak self] event, runMode, displayMode, terminationControl in
                self?.handleEvent(
                    event,
                    runMode: runMode,
                    displayMode: displayMode,
                    terminationControl: terminationControl
                ) ?? true
            },
            onError: { [weak self] error, runToken, runMode, displayMode, profile in
                self?.handleRunError(
                    error,
                    runToken: runToken,
                    runMode: runMode,
                    displayMode: displayMode,
                    profile: profile,
                    terminalIntent: terminationControl.terminationIntent
                )
            }
        )

        if terminationControl.shouldDrain {
            runDriver.requestTermination(terminationControl.terminationIntent)
        }

        return runToken
    }

    private func resolveRunConfiguration(
        profile: ServerProfileRecord,
        override: BackupRunConfigurationOverride? = nil
    ) -> BackupRunConfigurationOverride {
        if let override { return override }
        return BackupRunConfigurationOverride(
            workerCountOverride: BackupWorkerCountResolver.workerCountOverride(for: profile),
            iCloudPhotoBackupMode: ICloudPhotoBackupMode.getValue(),
            monthGroupingTimeZone: .frozenCurrent()
        )
    }

    private func applyIntent(_ intent: ExecutionTerminationIntent) {
        pendingCommand?.control.request(intent)
        pendingCommand?.task.cancel()
        runDriver.requestTermination(intent)
    }

    // MARK: - Event handling

    private func handleEvent(
        _ event: BackupEvent,
        runMode: BackupRunMode,
        displayMode: BackupRunMode,
        terminationControl: ExecutionTerminationControl
    ) -> Bool {
        notifyEventObservers(event)

        if case .finished = event {
            runDriver.clearActiveRunState()
        }

        let outcome = session.reduce(
            event: event,
            runMode: runMode,
            displayMode: displayMode,
            terminalIntent: terminationControl.terminationIntent
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
        profile: ServerProfileRecord,
        terminalIntent: ExecutionTerminationIntent
    ) {
        guard runDriver.matchesActiveRunToken(runToken) else { return }

        runDriver.clearActiveRunState()

        let phaseBeforeFailure = controlPhase
        let externalUnavailable = profile.isExternalStorageUnavailableError(error)
        handleExternalStorageUnavailableIfNeeded(error, for: profile)
        session.applyRunError(
            error,
            runMode: runMode,
            displayMode: displayMode,
            externalUnavailable: externalUnavailable,
            intent: terminalIntent,
            phaseBeforeFailure: phaseBeforeFailure
        )
        notifyObserversNow()
    }

    // MARK: - Resume

    @discardableResult
    private func resumeFromPause(onMonthUploaded: BackupMonthFinalizer? = nil) -> Bool {
        guard hardCancellationTask == nil else { return false }
        guard state != .running else {
            notifyObserversNow()
            return false
        }
        guard controlPhase == .idle else {
            notifyObserversNow()
            return false
        }
        guard pendingCommand == nil else {
            notifyObserversNow()
            return false
        }
        guard let connection = resolveActiveConnection() else {
            session.failForMissingConnection()
            notifyObservers()
            return false
        }

        let resumeContext = session.prepareForResume()
        let runConfiguration = session.lastRunConfiguration
            ?? runDriver.activeRunConfiguration
            ?? resolveRunConfiguration(profile: connection.profile)
        let commandID = UUID()
        let commandControl = ExecutionTerminationControl()
        let commandTask = Task { [weak self] in
            guard let self,
                  self.matchesPendingCommand(commandID, kind: .resume) else { return }
            do {
                try Task.checkCancellation()
                try commandControl.checkDrainRequested()
                let resumePlan = try await self.resumePlanner.makePlan(
                    pausedMode: resumeContext.pausedMode,
                    completedAssetIDs: self.completedAssetIDsForResume
                )
                try Task.checkCancellation()
                try commandControl.checkDrainRequested()

                guard self.matchesPendingCommand(commandID, kind: .resume),
                      self.state == .running,
                      self.controlPhase == .resuming else { throw CancellationError() }

                guard let resumedExecutionMode = resumePlan.resumedExecutionMode else {
                    guard self.clearPendingCommand(commandID, kind: .resume) else { return }
                    self.session.completeResumeWithoutPendingWork()
                    self.notifyObserversNow()
                    return
                }

                try await self.runDriver.waitForPreviousRunToClear()
                try Task.checkCancellation()
                try commandControl.checkDrainRequested()
                guard self.matchesPendingCommand(commandID, kind: .resume),
                      self.state == .running,
                      self.controlPhase == .resuming else { throw CancellationError() }

                let runToken = self.startRun(
                    profile: connection.profile,
                    password: connection.password,
                    mode: resumedExecutionMode,
                    displayMode: resumeContext.pausedDisplayMode,
                    configuration: runConfiguration,
                    onMonthUploaded: onMonthUploaded,
                    terminationControl: commandControl
                )

                guard self.clearPendingCommand(commandID, kind: .resume) else { return }
                if runToken != nil {
                    self.session.completeResumeLaunchSucceeded(displayMode: resumeContext.pausedDisplayMode)
                    self.notifyObserversNow()
                } else {
                    self.session.completeResumeLaunchFailed()
                    self.notifyObserversNow()
                }

            } catch is CancellationError {
                guard self.clearPendingCommand(commandID, kind: .resume) else { return }
                self.session.cancelResume(
                    pausedMode: resumeContext.pausedMode,
                    pausedDisplayMode: resumeContext.pausedDisplayMode
                )
                self.notifyObserversNow()
            } catch {
                guard self.clearPendingCommand(commandID, kind: .resume) else { return }
                if Task.isCancelled || commandControl.shouldDrain {
                    self.session.cancelResume(
                        pausedMode: resumeContext.pausedMode,
                        pausedDisplayMode: resumeContext.pausedDisplayMode
                    )
                    self.notifyObserversNow()
                    return
                }
                self.notifyEventObservers(.log(
                    String.localizedStringWithFormat(
                        String(localized: "backup.session.resumePreparationFailed"),
                        connection.profile.userFacingStorageErrorMessage(error)
                    ),
                    level: .error
                ))
                self.session.failResumePreparation()
                self.notifyObserversNow()
            }
        }
        pendingCommand = PendingCommand(
            id: commandID,
            kind: .resume,
            control: commandControl,
            task: commandTask
        )
        notifyObserversNow()

        return true
    }

    private func matchesPendingCommand(_ id: UUID, kind: StartCommandKind) -> Bool {
        pendingCommand?.id == id && pendingCommand?.kind == kind
    }

    @discardableResult
    private func clearPendingCommand(_ id: UUID, kind: StartCommandKind) -> Bool {
        guard matchesPendingCommand(id, kind: kind) else { return false }
        pendingCommand = nil
        return true
    }

    // MARK: - Observer notification

    private func notifyObservers() {
        resumeStartCommandWaitersIfNeeded()
        let latest = snapshot()
        observers.values.forEach { $0(latest) }
    }

    private func notifyEventObservers(_ event: BackupEvent) {
        eventObservers.values.forEach { $0(event) }
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
        if profile.storageProfile.requiresStoredCredential {
            guard let activePassword = appSession.activePassword else {
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

    private func invalidateStartCommandWaiters() {
        let waiters = startCommandWaiters.values
        startCommandWaiters.removeAll()
        for waiter in waiters {
            waiter.continuation.resume()
        }
    }

    private static let observerNotificationIntervalNanos: UInt64 = 120_000_000
}
