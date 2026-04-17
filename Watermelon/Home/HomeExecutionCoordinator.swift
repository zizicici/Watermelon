import Foundation
import MoreKit

struct HomeExecutionLogSnapshot {
    let statusText: String
    let entries: [ExecutionLogEntry]
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
    private var backupSessionController: BackupSessionController?
    private var backupBridge: BackupSessionAsyncBridge?
    private var downloadHelper: DownloadWorkflowHelper?
    private var executionSettingsSnapshot: ExecutionSettingsSnapshot?
    private var forcedUploadWorkerCountOverride: Int?
    private var currentStatusText = String(localized: "home.execution.notStarted")
    private var logEntries: [ExecutionLogEntry] = []
    private var logObservers: [UUID: @MainActor (HomeExecutionLogSnapshot) -> Void] = [:]
    private var backupEventObserverID: UUID?
    private(set) var currentSessionLogURL: URL?
    private var sessionLogStreamContinuation: AsyncStream<ExecutionLogEntry>.Continuation?
    private var sessionLogDrainTask: Task<Void, Never>?
    private var lastLogNotifyTime: CFAbsoluteTime = 0
    private var pendingLogNotifyTask: Task<Void, Never>?

    private static let syncThrottleInterval: CFAbsoluteTime = 2.0
    private static let logNotifyCoalesceInterval: CFAbsoluteTime = 0.5
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
        startSessionLogWriter(kind: .manual)
        PiPProgressManager.shared.taskDidStart(title: String(localized: "home.execution.log.preparingExecution"))
        session.enter(upload: upload, download: download, sync: sync, localAssetIDs: dataAccess.localAssetIDs)
        setStatusText(String(localized: "home.execution.log.preparingExecution"), notifyState: false)
        appendInfoLog(String(format: String(localized: "home.execution.log.startExecution"), upload.count, download.count, sync.count))
        let controller = BackupSessionController(dependencies: dependencies)
        backupSessionController = controller
        backupEventObserverID = controller.addEventObserver { [weak self] event in
            self?.handleBackupEvent(event)
        }
        backupBridge = BackupSessionAsyncBridge(backupSessionController: controller)
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
        setStatusText(String(localized: "home.execution.notStarted"), notifyState: false)
        logEntries.removeAll(keepingCapacity: true)
        finalizeSessionLogWriter()
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
            appendInfoLog(String(localized: "home.execution.log.requestPause"))
            setStatusText(String(localized: "home.execution.log.pausing"))
            backupBridge?.markAssetIDsPendingForResume(assetIDsAwaitingInlineSyncResume())
            dataRefresher.cancel()
            downloadHelper?.cancel()
            if shouldPauseBeforeUploadStart {
                let taskToAwait = executionTask
                executionTask?.cancel()
                executionTask = nil
                transientControlState = .pausing
                notifyStateChanged()
                settleUploadPause(after: taskToAwait)
                return
            }

            backupBridge?.requestPause()
            notifyStateChanged()
        case .download:
            appendInfoLog(String(localized: "home.execution.log.requestPause"))
            setStatusText(String(localized: "home.execution.log.pausing"))
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .pausing
            backupBridge?.cancel()
            downloadHelper?.cancel()
            notifyStateChanged()
            settleDownloadPause(after: taskToAwait)
        case nil:
            break
        }
    }

    func resume() {
        guard currentControlState == .idle else { return }
        guard session.resume() != nil else { return }
        appendInfoLog(String(localized: "home.execution.log.resuming"))
        setStatusText(String(localized: "home.execution.log.resumingStatus"))
        notifyStateChanged()
        startExecution()
    }

    func stop() {
        switch session.phase {
        case .uploading:
            appendWarningLog(String(localized: "home.execution.log.requestStop"))
            setStatusText(String(localized: "home.execution.log.stopping"))
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .stopping
            dataRefresher.cancel()
            downloadHelper?.cancel()
            notifyStateChanged()
            backupBridge?.requestStop()
            settleStop(after: taskToAwait)
        case .uploadPaused:
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            PiPProgressManager.shared.taskDidCancel()
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .downloading:
            appendWarningLog(String(localized: "home.execution.log.requestStop"))
            setStatusText(String(localized: "home.execution.log.stopping"))
            let taskToAwait = executionTask
            executionTask?.cancel()
            executionTask = nil
            transientControlState = .stopping
            backupBridge?.cancel()
            downloadHelper?.cancel()
            notifyStateChanged()
            settleStop(after: taskToAwait)
        case .downloadPaused:
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            PiPProgressManager.shared.taskDidCancel()
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
        setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.executionFailed"), alert.message))
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
                guard let backupBridge = self.backupBridge else { return }
                let scope = self.session.consumePendingUploadScope()
                let runConfigurationOverride = self.activeExecutionSettingsSnapshot().makeUploadRunConfiguration(
                    forcedWorkerCountOverride: self.forcedUploadWorkerCountOverride
                )
                let result = await backupBridge.runUpload(
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
            appendInfoLog(String(localized: "home.execution.log.uploadPhaseCompleteStartDownload"))
            setStatusText(String(localized: "home.execution.preparingDownload"))
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            notifyStateChanged()
            return true
        case .paused:
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            notifyStateChanged()
            return false
        case .failed(let alert):
            setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.uploadPhaseFailed"), alert.message))
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return false
        case .exit:
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            exit()
            return false
        case .finished:
            appendInfoLog(String(localized: "home.execution.log.executionPhaseDoneSyncing"))
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
            appendInfoLog(String(localized: "home.execution.log.allTasksComplete"))
            PiPProgressManager.shared.taskDidComplete()
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return
        }

        guard let context = makeDownloadContext() else {
            let alert = session.failForMissingConnection()
            setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.executionFailed"), alert.message))
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return
        }

        session.beginDownloadPhase()
        appendInfoLog(String(format: String(localized: "home.execution.log.startDownloadPhase"), remaining.count))
        setStatusText(String(localized: "home.execution.downloading"), notifyState: false)
        notifyStateChanged()

        for month in remaining {
            if Task.isCancelled { return }
            await runDownloadMonth(month, context: context, phaseLabel: session.phaseLabel(for: month))
        }

        if !Task.isCancelled {
            session.finishExecution()
            appendInfoLog(String(localized: "home.execution.log.allTasksComplete"))
            PiPProgressManager.shared.taskDidComplete()
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
        appendInfoLog(String(format: String(localized: "home.execution.log.startDownloadMonth"), phaseLabel, month.displayText))
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
            appendInfoLog(String(format: String(localized: "home.execution.log.icloudProbeStart"), assetIDs.count))
            setStatusText(String(localized: "home.execution.log.icloudProbeStatus"))
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
                appendWarningLog(String(format: String(localized: "home.execution.log.icloudProbeDegraded"), unavailableCount, failedCount))
            }
            appendInfoLog(String(localized: "home.execution.log.icloudProbeDone"))
            return true
        } catch is CancellationError {
            return false
        } catch {
            let errorMessage = UserFacingErrorLocalizer.message(
                for: error,
                profile: dependencies.appSession.activeProfile
            )
            let message = String(format: String(localized: "home.execution.log.icloudProbeFailed"), errorMessage)
            let alert = session.failExecution(reason: message)
            transientControlState = nil
            setErrorStatus(message, log: String(format: String(localized: "home.execution.log.executionFailed"), message))
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
            appendInfoLog(String(format: String(localized: "home.execution.log.startIndex"), assetIDs.count))
            setStatusText(String(localized: "home.execution.log.indexStatus"))
            let initialResult = try await dependencies.localHashIndexBuildService.buildIndex(
                for: assetIDs,
                workerCount: Self.localIndexPreflightWorkerCount,
                allowNetworkAccess: false,
                progressHandler: progressHandler
            )
            guard !Task.isCancelled else { return false }

            if !initialResult.readyAssetIDs.isEmpty {
                appendDebugLog(String(format: String(localized: "home.execution.log.indexWriteback"), initialResult.readyAssetIDs.count))
                await dataRefresher.refreshLocalIndexAndNotify(initialResult.readyAssetIDs)
                guard !Task.isCancelled else { return false }
                appendDebugLog(String(format: String(localized: "home.execution.log.indexRefreshDone"), initialResult.readyAssetIDs.count))
            }

            let result: LocalHashIndexBuildResult
            if session.requiresCompleteLocalIndexBeforeExecution,
               !initialResult.unavailableAssetIDs.isEmpty,
               settings.iCloudPhotoBackupMode == .enable {
                appendWarningLog(String(format: String(localized: "home.execution.log.icloudFound"), initialResult.unavailableAssetIDs.count))
                let iCloudResult = try await dependencies.localHashIndexBuildService.buildIndex(
                    for: initialResult.unavailableAssetIDs,
                    workerCount: Self.localIndexICloudPreflightWorkerCount,
                    allowNetworkAccess: true,
                    progressHandler: progressHandler
                )
                guard !Task.isCancelled else { return false }

                if !iCloudResult.readyAssetIDs.isEmpty {
                    appendDebugLog(String(format: String(localized: "home.execution.log.icloudWriteback"), iCloudResult.readyAssetIDs.count))
                    await dataRefresher.refreshLocalIndexAndNotify(iCloudResult.readyAssetIDs)
                    guard !Task.isCancelled else { return false }
                    appendDebugLog(String(format: String(localized: "home.execution.log.icloudRefreshDone"), iCloudResult.readyAssetIDs.count))
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
                String(format: String(localized: "home.execution.log.indexComplete"), result.readyAssetIDs.count, result.unavailableAssetIDs.count, result.failedAssetIDs.count),
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
                setErrorStatus(message, log: String(format: String(localized: "home.execution.log.executionFailed"), message))
                notifyStateChanged()
                onAlert?(alert.title, alert.message)
                return false
            }

            setStatusText(session.shouldRunUploadPhase ? String(localized: "home.execution.preparingUpload") : String(localized: "home.execution.preparingDownload"), notifyState: false)
            return true
        } catch is CancellationError {
            return false
        } catch {
            let errorMessage = UserFacingErrorLocalizer.message(
                for: error,
                profile: dependencies.appSession.activeProfile
            )
            let message = String(format: String(localized: "home.execution.log.indexFailed"), errorMessage)
            let alert = session.failExecution(reason: message)
            transientControlState = nil
            setErrorStatus(message, log: String(format: String(localized: "home.execution.log.executionFailed"), message))
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
            parts.append(String(format: String(localized: "home.execution.log.unavailableItems"), result.unavailableAssetIDs.count))
        }
        if !result.failedAssetIDs.isEmpty {
            parts.append(String(format: String(localized: "home.execution.log.failedItems"), result.failedAssetIDs.count))
        }
        let detail = parts.joined(separator: ", ")
        if !result.unavailableAssetIDs.isEmpty, iCloudPhotoBackupMode == .disable {
            return String(format: String(localized: "home.execution.log.indexIncompleteICloud"), detail)
        }
        return String(format: String(localized: "home.execution.log.indexIncomplete"), detail)
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
        appendInfoLog(String(format: String(localized: "home.execution.log.uploadDoneStartPhase"), phaseLabel, month.displayText))
        setStatusText("\(phaseLabel) \(month.displayText)", notifyState: false)
        notifyStateChanged()

        guard let context else {
            let message = String(localized: "home.execution.notConnected")
            session.failDownloadMonth(month, reason: message)
            setErrorStatus(message, log: String(format: String(localized: "home.execution.log.downloadFailed"), phaseLabel, month.displayText, message))
            notifyStateChanged()
            onAlert?(String(format: String(localized: "home.execution.log.phaseFailed"), phaseLabel), String(format: String(localized: "home.execution.log.phaseFailedDetail"), month.displayText, message))
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
        appendInfoLog(String(format: String(localized: "home.execution.log.syncRemoteIndex"), month.displayText))
        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled { return .cancelled }
        if !assetIDs.isEmpty {
            appendDebugLog(String(format: String(localized: "home.execution.log.refreshLocalIndex"), month.displayText))
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return .cancelled }
        }

        let remoteItems = dataAccess.remoteOnlyItems(month)
        appendDebugLog(String(format: String(localized: "home.execution.log.pendingDownload"), month.displayText, remoteItems.count))
        guard let downloadHelper else { return .cancelled }
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
            appendInfoLog(String(format: String(localized: "home.execution.log.downloadDone"), phaseLabel, month.displayText))
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return .success
        case .failed(let message):
            session.failDownloadMonth(month, reason: message)
            setErrorStatus(message, log: String(format: String(localized: "home.execution.log.downloadFailed"), phaseLabel, month.displayText, message))
            notifyStateChanged()
            onAlert?(String(format: String(localized: "home.execution.log.phaseFailed"), phaseLabel), String(format: String(localized: "home.execution.log.phaseFailedDetail"), month.displayText, message))
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
                appendInfoLog(String(format: String(localized: "home.execution.log.uploadStartMonth"), month.displayText))
            case .completed:
                appendInfoLog(String(format: String(localized: "home.execution.log.uploadDoneMonth"), month.displayText))
            }
        case .started(let totalAssets):
            setStatusText(String(localized: "home.execution.uploading"))
            appendInfoLog(String(format: String(localized: "home.execution.log.uploadPhaseStart"), totalAssets))
        case .finished(let result):
            appendLog(
                String(format: String(localized: "home.execution.log.uploadPhaseDone"), result.succeeded, result.failed, result.skipped),
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
        let entry = ExecutionLogEntry(timestamp: Date(), message: message, level: level)
        logEntries.append(entry)
        sessionLogStreamContinuation?.yield(entry)
        PiPProgressManager.shared.appendLog(entry)
        notifyLogObservers()
    }

    private func startSessionLogWriter(kind: ExecutionLogKind) {
        sessionLogStreamContinuation?.finish()
        sessionLogStreamContinuation = nil
        sessionLogDrainTask = nil

        let writer = ExecutionLogFileStore.beginSession(kind: kind)
        currentSessionLogURL = writer.fileURL
        let (stream, continuation) = AsyncStream.makeStream(of: ExecutionLogEntry.self)
        sessionLogStreamContinuation = continuation
        sessionLogDrainTask = Task.detached {
            for await entry in stream {
                await writer.appendLog(entry.message, level: entry.level, at: entry.timestamp)
            }
            await writer.finalize()
        }
    }

    private func finalizeSessionLogWriter() {
        sessionLogStreamContinuation?.finish()
        sessionLogStreamContinuation = nil
        sessionLogDrainTask = nil
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
        PiPProgressManager.shared.taskDidFail(message: statusText)
    }

    private func setStatusText(_ text: String, notifyState: Bool = true) {
        guard currentStatusText != text else { return }
        currentStatusText = text
        PiPProgressManager.shared.updateStatus(text)
        notifyLogObservers()
        if notifyState {
            onStateChanged?()
        }
    }

    private func refreshTerminalStatus(notifyState: Bool = true) {
        let text: String
        switch session.phase {
        case .completed:
            text = String(localized: "home.execution.completed")
        case .failed(let message):
            text = message
        case .uploadPaused, .downloadPaused:
            text = String(localized: "home.execution.paused")
        case .uploading:
            text = String(localized: "home.execution.uploading")
        case .downloading:
            text = String(localized: "home.execution.downloading")
        case nil:
            text = String(localized: "home.execution.notStarted")
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
        let now = CFAbsoluteTimeGetCurrent()
        let elapsed = now - lastLogNotifyTime
        if elapsed >= Self.logNotifyCoalesceInterval {
            pendingLogNotifyTask?.cancel()
            pendingLogNotifyTask = nil
            lastLogNotifyTime = now
            deliverLogSnapshot()
            return
        }
        if pendingLogNotifyTask != nil { return }
        let remaining = Self.logNotifyCoalesceInterval - elapsed
        pendingLogNotifyTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
            guard !Task.isCancelled, let self else { return }
            self.pendingLogNotifyTask = nil
            self.lastLogNotifyTime = CFAbsoluteTimeGetCurrent()
            self.deliverLogSnapshot()
        }
    }

    private func deliverLogSnapshot() {
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
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
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
                self.setStatusText(String(localized: "home.execution.paused"), notifyState: false)
                self.appendWarningLog(String(localized: "home.execution.log.executionPaused"))
                self.notifyStateChanged()
            }
        }
    }

    private func settleUploadPause(after task: Task<Void, Never>?) {
        guard let task else {
            transientControlState = nil
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
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
                self.setStatusText(String(localized: "home.execution.paused"), notifyState: false)
                self.appendWarningLog(String(localized: "home.execution.log.executionPaused"))
                self.notifyStateChanged()
            }
        }
    }

    private func settleStop(after task: Task<Void, Never>?) {
        guard let task else {
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            PiPProgressManager.shared.taskDidCancel()
            exit()
            return
        }

        Task { [weak self] in
            _ = await task.value
            await MainActor.run {
                guard let self,
                      self.transientControlState == .stopping,
                      self.session.isActive else { return }
                self.appendWarningLog(String(localized: "home.execution.log.stopped"))
                PiPProgressManager.shared.taskDidCancel()
                self.exit()
            }
        }
    }
}
