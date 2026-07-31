import Foundation

@MainActor
protocol BackupRunDriving: AnyObject {
    var activeRunConfiguration: BackupRunConfigurationOverride? { get }
    var hasActiveRunTask: Bool { get }

    func matchesActiveRunToken(_ runToken: UInt64) -> Bool
    func waitForPreviousRunToClear() async throws
    func startRun(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded: BackupMonthFinalizer?,
        terminationControl: ExecutionTerminationControl,
        onEvent: @escaping BackupRunDriver.EventHandler,
        onError: @escaping BackupRunDriver.ErrorHandler
    ) -> UInt64?
    func requestTermination(_ intent: ExecutionTerminationIntent)
    func clearActiveRunState()
    @discardableResult
    func cancelActiveRunImmediately() -> Task<Void, Never>?
}

@MainActor
final class BackupRunDriver {
    typealias EventHandler = @MainActor (_ event: BackupEvent, _ runMode: BackupRunMode, _ displayMode: BackupRunMode, _ terminationControl: ExecutionTerminationControl) -> Bool
    typealias ErrorHandler = @MainActor (_ error: Error, _ runToken: UInt64, _ runMode: BackupRunMode, _ displayMode: BackupRunMode, _ profile: ServerProfileRecord) -> Void
    typealias RunOperation = @Sendable (_ request: BackupRunRequest, _ eventStream: BackupEventStream) async throws -> Void

    private let runOperation: RunOperation

    private var runTask: Task<Void, Never>?
    private var eventListenerTask: Task<Void, Never>?
    private var activeEventStream: BackupEventStream?
    private var activeRunControl: ExecutionTerminationControl?
    private var activeRunToken: UInt64 = 0

    private(set) var activeRunConfiguration: BackupRunConfigurationOverride?

    init(backupCoordinator: BackupCoordinator) {
        runOperation = { request, eventStream in
            _ = try await backupCoordinator.runBackup(request: request, eventStream: eventStream)
        }
    }

    init(runOperation: @escaping RunOperation) {
        self.runOperation = runOperation
    }

    var hasActiveRunTask: Bool {
        runTask != nil
    }

    func matchesActiveRunToken(_ runToken: UInt64) -> Bool {
        runToken == activeRunToken
    }

    func waitForPreviousRunToClear() async throws {
        while runTask != nil || eventListenerTask != nil || activeEventStream != nil {
            try Task.checkCancellation()
            try await Task.sleep(nanoseconds: 50_000_000)
        }
    }

    func startRun(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded: BackupMonthFinalizer? = nil,
        terminationControl: ExecutionTerminationControl = ExecutionTerminationControl(),
        onEvent: @escaping EventHandler,
        onError: @escaping ErrorHandler
    ) -> UInt64? {
        guard !terminationControl.shouldDrain,
              runTask == nil,
              eventListenerTask == nil,
              activeEventStream == nil else {
            return nil
        }

        activeRunToken &+= 1
        let runToken = activeRunToken
        let eventStream = BackupEventStream()
        activeEventStream = eventStream
        activeRunControl = terminationControl
        activeRunConfiguration = configuration

        let capturedRunToken = runToken
        let capturedRunMode = mode
        let capturedDisplayMode = displayMode
        let capturedConfiguration = configuration

        eventListenerTask = Task { [weak self] in
            for await event in eventStream.stream {
                guard let self else { return }
                guard capturedRunToken == self.activeRunToken else { return }
                let shouldStop = onEvent(
                    event,
                    capturedRunMode,
                    capturedDisplayMode,
                    terminationControl
                )
                if shouldStop { break }
            }
        }

        let runOperation = self.runOperation
        let operationTask = Task.detached(priority: .userInitiated) { [eventStream] in
            defer {
                eventStream.finish()
            }
            do {
                let request = BackupRunRequest(
                    profile: profile,
                    password: password,
                    onlyAssetLocalIdentifiers: mode.targetAssetIdentifiers,
                    workerCountOverride: capturedConfiguration.workerCountOverride,
                    iCloudPhotoBackupMode: capturedConfiguration.iCloudPhotoBackupMode,
                    monthGroupingTimeZone: capturedConfiguration.monthGroupingTimeZone,
                    onMonthUploaded: onMonthUploaded,
                    terminationControl: terminationControl
                )
                try await runOperation(request, eventStream)
            } catch {
                await onError(
                    error,
                    capturedRunToken,
                    capturedRunMode,
                    capturedDisplayMode,
                    profile
                )
            }
        }
        runTask = operationTask
        Task { @MainActor [weak self] in
            _ = await operationTask.value
            guard let self, self.activeRunToken == capturedRunToken else { return }
            self.runTask = nil
        }

        return runToken
    }

    func requestTermination(_ intent: ExecutionTerminationIntent) {
        activeRunControl?.request(intent)
    }

    func clearActiveRunState() {
        activeEventStream = nil
        activeRunControl = nil
        activeRunConfiguration = nil
        eventListenerTask?.cancel()
        eventListenerTask = nil
    }

    @discardableResult
    func cancelActiveRunImmediately() -> Task<Void, Never>? {
        let task = runTask
        activeRunToken &+= 1
        let cancellationToken = activeRunToken
        task?.cancel()
        eventListenerTask?.cancel()
        activeEventStream?.finish()
        eventListenerTask = nil
        activeEventStream = nil
        activeRunControl = nil
        activeRunConfiguration = nil
        if let task {
            Task { @MainActor [weak self] in
                _ = await task.value
                guard let self, self.activeRunToken == cancellationToken else { return }
                self.runTask = nil
            }
        } else {
            runTask = nil
        }
        return task
    }
}

extension BackupRunDriver: BackupRunDriving {}
