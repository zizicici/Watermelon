import Foundation

@MainActor
final class BackupRunDriver {
    typealias EventHandler = @MainActor (_ event: BackupEvent, _ runMode: BackupRunMode, _ displayMode: BackupRunMode, _ terminalIntent: BackupTerminationIntent) -> Bool
    typealias ErrorHandler = @MainActor (_ error: Error, _ runToken: UInt64, _ runMode: BackupRunMode, _ displayMode: BackupRunMode, _ profile: ServerProfileRecord) -> Void
    typealias TerminalIntentProvider = @MainActor () -> BackupTerminationIntent

    private let backupCoordinator: BackupCoordinator

    private var runTask: Task<Void, Never>?
    private var eventListenerTask: Task<Void, Never>?
    private var activeEventStream: BackupEventStream?
    private var activeRunToken: UInt64 = 0

    private(set) var activeWorkerCountOverride: Int?
    private(set) var activeICloudPhotoBackupMode: ICloudPhotoBackupMode = .disable

    init(backupCoordinator: BackupCoordinator) {
        self.backupCoordinator = backupCoordinator
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
        workerCountOverride: Int?,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        onMonthUploaded: BackupMonthFinalizer? = nil,
        terminalIntentProvider: @escaping TerminalIntentProvider,
        onEvent: @escaping EventHandler,
        onError: @escaping ErrorHandler
    ) -> UInt64? {
        guard runTask == nil, eventListenerTask == nil, activeEventStream == nil else {
            return nil
        }

        activeRunToken &+= 1
        let runToken = activeRunToken
        let eventStream = BackupEventStream()
        activeEventStream = eventStream
        activeWorkerCountOverride = workerCountOverride
        activeICloudPhotoBackupMode = iCloudPhotoBackupMode

        let capturedRunToken = runToken
        let capturedRunMode = mode
        let capturedDisplayMode = displayMode

        eventListenerTask = Task { [weak self] in
            for await event in eventStream.stream {
                guard let self else { return }
                guard capturedRunToken == self.activeRunToken else { return }
                let shouldStop = onEvent(
                    event,
                    capturedRunMode,
                    capturedDisplayMode,
                    terminalIntentProvider()
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
                    workerCountOverride: workerCountOverride,
                    iCloudPhotoBackupMode: iCloudPhotoBackupMode,
                    onMonthUploaded: onMonthUploaded
                )
                _ = try await self.backupCoordinator.runBackup(request: request, eventStream: eventStream)
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

        return runToken
    }

    func cancelRunTask() {
        runTask?.cancel()
    }

    func clearActiveRunState() {
        runTask = nil
        activeEventStream = nil
        eventListenerTask?.cancel()
        eventListenerTask = nil
    }

    func cancelAll() {
        runTask?.cancel()
        eventListenerTask?.cancel()
    }
}
