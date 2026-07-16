import Foundation
import MoreKit

struct HomeExecutionLogSnapshot {
    let statusText: String
    let transferMetrics: HomeExecutionTransferMetrics
    let entries: [ExecutionLogEntry]
}

struct HomeExecutionTransferMetrics: Equatable {
    let speedBytesPerSecond: Double?
    let remainingTimeSeconds: TimeInterval?

    static let inactive = HomeExecutionTransferMetrics(
        speedBytesPerSecond: nil,
        remainingTimeSeconds: nil
    )
}

struct HomeExecutionTransferTracker {
    private struct ResourceKey: Hashable {
        let kind: BackupTransferKind
        let assetLocalIdentifier: String
        let resourceDisplayName: String
        let resourcePosition: Int
        let totalResources: Int
    }

    private struct ResourceProgress {
        var committedBytes: Int64 = 0
        var lastAttemptBytes: Int64 = 0
    }

    private struct Sample {
        let timestamp: CFAbsoluteTime
        let bytes: Int64
    }

    private struct RateSnapshot {
        let bytesPerSecond: Double
        let timestamp: CFAbsoluteTime
    }

    private var totalBytes: Int64?
    private var progressByKey: [ResourceKey: ResourceProgress] = [:]
    private var actualTransferredBytes: Int64 = 0
    private var samples: [Sample] = []
    private var lastProgressAt: CFAbsoluteTime?
    private var smoothedRateBytesPerSecond: Double?
    private var smoothedRateSampleTimestamp: CFAbsoluteTime?

    private static let sampleWindow: CFAbsoluteTime = 10
    private static let minimumRateInterval: CFAbsoluteTime = 1
    private static let recentProgressWindow: CFAbsoluteTime = 10
    private static let rateSmoothingTimeConstant: CFAbsoluteTime = 6

    mutating func updateTotalBytes(_ totalBytes: Int64?) {
        self.totalBytes = totalBytes
    }

    mutating func clear() {
        totalBytes = nil
        progressByKey.removeAll(keepingCapacity: false)
        actualTransferredBytes = 0
        samples.removeAll(keepingCapacity: false)
        lastProgressAt = nil
        smoothedRateBytesPerSecond = nil
        smoothedRateSampleTimestamp = nil
    }

    mutating func record(_ state: BackupTransferState, now: CFAbsoluteTime) -> HomeExecutionTransferMetrics {
        let key = ResourceKey(
            kind: state.kind,
            assetLocalIdentifier: state.assetLocalIdentifier,
            resourceDisplayName: state.resourceDisplayName,
            resourcePosition: state.resourcePosition,
            totalResources: state.totalResources
        )
        let resolvedBytes = resolvedTransferredBytes(for: state)
        if let resolvedBytes {
            var progress = progressByKey[key] ?? ResourceProgress()
            let actualDelta: Int64
            if state.countsTowardTransferSpeed {
                actualDelta = resolvedBytes >= progress.lastAttemptBytes
                    ? resolvedBytes - progress.lastAttemptBytes
                    : resolvedBytes
            } else {
                actualDelta = 0
            }
            progress.lastAttemptBytes = resolvedBytes

            var committedBytes = max(progress.committedBytes, resolvedBytes)
            if state.resourceFraction >= 1, let total = state.resourceTotalBytes {
                committedBytes = max(committedBytes, total)
            }
            progress.committedBytes = committedBytes
            progressByKey[key] = progress

            if actualDelta > 0 {
                actualTransferredBytes += actualDelta
                lastProgressAt = now
                appendSample(now: now)
            }
        }
        return snapshot(now: now)
    }

    mutating func snapshot(now: CFAbsoluteTime) -> HomeExecutionTransferMetrics {
        trimSamples(referenceTime: samples.last?.timestamp ?? now)
        guard let lastProgressAt, now - lastProgressAt <= Self.recentProgressWindow else {
            smoothedRateBytesPerSecond = nil
            smoothedRateSampleTimestamp = nil
            return .inactive
        }
        guard let rateSnapshot = currentRate(), rateSnapshot.bytesPerSecond > 0 else {
            return HomeExecutionTransferMetrics(speedBytesPerSecond: nil, remainingTimeSeconds: nil)
        }
        let rate = smoothedRate(for: rateSnapshot)

        let remainingTimeSeconds: TimeInterval?
        if let totalBytes {
            let remainingBytes = max(0, totalBytes - currentAggregateBytes())
            remainingTimeSeconds = Double(remainingBytes) / rate
        } else {
            remainingTimeSeconds = nil
        }
        return HomeExecutionTransferMetrics(
            speedBytesPerSecond: rate,
            remainingTimeSeconds: remainingTimeSeconds
        )
    }

    private func resolvedTransferredBytes(for state: BackupTransferState) -> Int64? {
        if let resourceBytesTransferred = state.resourceBytesTransferred {
            if let total = state.resourceTotalBytes, total > 0 {
                return min(max(resourceBytesTransferred, 0), total)
            }
            return max(resourceBytesTransferred, 0)
        }
        guard let total = state.resourceTotalBytes, total > 0 else { return nil }
        return Int64((Double(total) * Double(state.resourceFraction)).rounded())
    }

    private func currentAggregateBytes() -> Int64 {
        progressByKey.values.reduce(Int64(0)) { $0 + $1.committedBytes }
    }

    private mutating func appendSample(now: CFAbsoluteTime) {
        samples.append(Sample(timestamp: now, bytes: actualTransferredBytes))
        trimSamples(referenceTime: now)
    }

    private mutating func trimSamples(referenceTime: CFAbsoluteTime) {
        samples.removeAll { referenceTime - $0.timestamp > Self.sampleWindow }
    }

    private func currentRate() -> RateSnapshot? {
        guard let last = samples.last else { return nil }
        guard let baseline = samples.dropLast().first(where: { last.timestamp - $0.timestamp >= Self.minimumRateInterval }) else {
            return nil
        }
        let elapsed = last.timestamp - baseline.timestamp
        guard elapsed >= Self.minimumRateInterval else { return nil }
        let delta = last.bytes - baseline.bytes
        guard delta > 0 else { return nil }
        return RateSnapshot(bytesPerSecond: Double(delta) / elapsed, timestamp: last.timestamp)
    }

    private mutating func smoothedRate(for rate: RateSnapshot) -> Double {
        guard let previousRate = smoothedRateBytesPerSecond,
              let previousTimestamp = smoothedRateSampleTimestamp else {
            smoothedRateBytesPerSecond = rate.bytesPerSecond
            smoothedRateSampleTimestamp = rate.timestamp
            return rate.bytesPerSecond
        }
        guard rate.timestamp > previousTimestamp else {
            return previousRate
        }
        let elapsed = rate.timestamp - previousTimestamp
        let alpha = min(max(1 - exp(-elapsed / Self.rateSmoothingTimeConstant), 0), 1)
        let nextRate = previousRate + (rate.bytesPerSecond - previousRate) * alpha
        smoothedRateBytesPerSecond = nextRate
        smoothedRateSampleTimestamp = rate.timestamp
        return nextRate
    }
}

@MainActor
final class HomeExecutionCoordinator {

    private struct ExecutionSettingsSnapshot {
        let uploadWorkerCountOverride: Int?
        let iCloudPhotoBackupMode: ICloudPhotoBackupMode
        let monthGroupingTimeZone: MonthGroupingTimeZonePreference

        static func fromCurrentSettings(
            monthGroupingTimeZone: MonthGroupingTimeZonePreference
        ) -> ExecutionSettingsSnapshot {
            ExecutionSettingsSnapshot(
                uploadWorkerCountOverride: BackupWorkerCountMode.getValue().workerCountOverride,
                iCloudPhotoBackupMode: ICloudPhotoBackupMode.getValue(),
                monthGroupingTimeZone: monthGroupingTimeZone
            )
        }

        func makeUploadRunConfiguration(
            forcedWorkerCountOverride: Int?
        ) -> BackupRunConfigurationOverride {
            BackupRunConfigurationOverride(
                workerCountOverride: forcedWorkerCountOverride ?? uploadWorkerCountOverride,
                iCloudPhotoBackupMode: iCloudPhotoBackupMode,
                monthGroupingTimeZone: monthGroupingTimeZone
            )
        }
    }

    // MARK: - Public State

    var phase: ExecutionPhase? { session.phase }
    var isActive: Bool { session.isActive }
    var isRunning: Bool {
        switch session.phase {
        case .some(.uploading), .some(.uploadPaused), .some(.downloading), .some(.downloadPaused):
            return true
        case .some(.completed), .some(.failed), nil:
            return false
        }
    }
    var currentState: HomeExecutionState? {
        session.currentState(
            controlState: currentControlState,
            statusText: currentStatusText
        )
    }
    var currentLogSnapshot: HomeExecutionLogSnapshot {
        HomeExecutionLogSnapshot(
            statusText: currentStatusText,
            transferMetrics: currentTransferMetrics,
            entries: logEntries
        )
    }

    // MARK: - Callbacks

    var onStateChanged: (() -> Void)?
    var onAlert: ((String, String) -> Void)?

    // MARK: - Data Access (provided by Store)

    struct DataAccess {
        let localAssetIDs: (LibraryMonthKey) -> Set<String>
        let localMonthGroupingTimeZone: () -> MonthGroupingTimeZonePreference
        let remoteOnlyItems: (LibraryMonthKey) async -> [RemoteAlbumItem]
        let syncRemoteData: () async -> Set<LibraryMonthKey>
        let refreshLocalIndex: (Set<String>) async -> Set<LibraryMonthKey>
    }

    // MARK: - Dependencies

    private let dependencies: DependencyContainer
    private let dataAccess: DataAccess
    // How this run's download phase treats incomplete remote records (chosen upfront in the UI). Default skip.
    private var incompleteDownloadPolicy: IncompleteDownloadPolicy = .skip

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
    private var transferTracker = HomeExecutionTransferTracker()
    private var currentTransferMetrics = HomeExecutionTransferMetrics.inactive
    private var transferMetricsRefreshTask: Task<Void, Never>?
    private var estimatedUploadTotalBytes: Int64?
    private var estimatedDownloadTotalBytes: Int64?
    private var downloadEstimateTask: Task<Void, Never>?
    private var transferPlanGeneration: UInt64 = 0
    private var transferMetricsActive = false
    private var logEntries: [ExecutionLogEntry] = []
    private var logObservers: [UUID: @MainActor (HomeExecutionLogSnapshot) -> Void] = [:]
    private var stateObservers: [UUID: @MainActor () -> Void] = [:]
    private var backupEventObserverID: UUID?
    private(set) var currentSessionLogURL: URL?
    private var sessionLogStreamContinuation: AsyncStream<ExecutionLogEntry>.Continuation?
    private var sessionLogDrainTask: Task<Void, Never>?
    private var memoryWatermarkTask: Task<Void, Never>?
    private var lastLogNotifyTime: CFAbsoluteTime = 0
    private var pendingLogNotifyTask: Task<Void, Never>?

    private static let syncThrottleInterval: CFAbsoluteTime = 2.0
    private static let logNotifyCoalesceInterval: CFAbsoluteTime = 0.5
    // Bound the live buffer; the full run log is durable on disk.
    nonisolated static let maxLiveLogEntries = 4000
    nonisolated static let liveLogTrimChunk = 1000
    private static let localIndexPreflightWorkerCount = 2
    private static let localIndexICloudPreflightWorkerCount = 1
    // Ride out a transient iCloud-fetch wobble in the network-allowed second pass so one blip doesn't fail an
    // otherwise-complete index; a sustained outage still settles incomplete after these attempts.
    private static let maxICloudPreflightAttempts = 3
    private static let iCloudPreflightRetryBackoffNanos: UInt64 = 3_000_000_000

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

    deinit {
        // Defensive cleanup for the app-root coordinator; AppRuntimeFlags clears only this container's lock.
        dependencies.appRuntimeFlags.exitExecution()
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

    @discardableResult
    func enter(backup: [LibraryMonthKey], download: [LibraryMonthKey], complement: [LibraryMonthKey], incompletePolicy: IncompleteDownloadPolicy = .skip) -> Bool {
        guard dependencies.appRuntimeFlags.tryEnterExecution() else { return false }
        incompleteDownloadPolicy = incompletePolicy
        executionTask = nil
        transientControlState = nil
        executionSettingsSnapshot = ExecutionSettingsSnapshot.fromCurrentSettings(
            monthGroupingTimeZone: dataAccess.localMonthGroupingTimeZone()
        )
        forcedUploadWorkerCountOverride = nil
        dataRefresher.reset()
        logEntries.removeAll(keepingCapacity: true)
        resetTransferMetricsForExecution(downloadMonths: download + complement)
        startTransferMetricsRefreshLoop()
        startSessionLogWriter(kind: .manual)
        session.enter(backup: backup, download: download, complement: complement, localAssetIDs: dataAccess.localAssetIDs)
        setStatusText(String(localized: "home.execution.log.preparingExecution"), notifyState: false)
        appendInfoLog(String(format: String(localized: "home.execution.log.startExecution"), backup.count, download.count, complement.count))
        for line in AppExitMetricsMonitor.consumeSummaryLines() {
            appendDebugLog(line)
        }
        startMemoryWatermarkLoop()
        let controller = BackupSessionController(dependencies: dependencies)
        backupSessionController = controller
        backupEventObserverID = controller.addEventObserver { [weak self] event in
            self?.handleBackupEvent(event)
        }
        backupBridge = BackupSessionAsyncBridge(backupSessionController: controller)
        downloadHelper = DownloadWorkflowHelper(dependencies: dependencies)
        notifyStateChanged()
        startExecution()
        return true
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
        deactivateTransferMetrics()
        finalizeSessionLogWriter()
        // Must precede `notifyStateChanged` — guards reading `isExecuting` need the cleared value.
        dependencies.appRuntimeFlags.exitExecution()
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
            deactivateTransferMetrics()
            appendInfoLog(String(localized: "home.execution.log.requestPause"))
            setStatusText(String(localized: "home.execution.log.pausing"))
            backupBridge?.markAssetIDsPendingForResume(assetIDsAwaitingInlineComplementResume())
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
            deactivateTransferMetrics()
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
        resetTransferMetricsForExecution(downloadMonths: plannedDownloadMonthsForTransferMetrics())
        startTransferMetricsRefreshLoop()
        startMemoryWatermarkLoop()
        appendInfoLog(String(localized: "home.execution.log.resuming"))
        setStatusText(String(localized: "home.execution.log.resumingStatus"))
        notifyStateChanged()
        startExecution()
    }

    func stop() {
        switch session.phase {
        case .uploading:
            deactivateTransferMetrics()
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
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .downloading:
            deactivateTransferMetrics()
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
            executionTask?.cancel()
            executionTask = nil
            exit()
        case .completed, .failed:
            exit()
        default:
            break
        }
    }

    func failForMissingConnection(message: String? = nil) {
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
        deactivateTransferMetrics()
        backupBridge?.requestStop()
        backupBridge?.cancel()
        downloadHelper?.cancel()

        let alert = session.failForMissingConnection(message: message)
        setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.executionFailed"), alert.message))
        notifyStateChanged()
        onAlert?(alert.title, alert.message)
    }

    // MARK: - Execution Task

    private func startExecution() {
        executionTask = Task { [weak self] in
            guard let self else { return }

            if self.session.needsLocalIndexPreflight,
               self.shouldRunLocalIndexPreflight() {
                await MainActor.run {
                    self.transientControlState = .starting
                    self.notifyStateChanged()
                }

                let prepared = await self.prepareLocalIndexIfNeeded()
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
        if transientControlState == nil, let text = phaseStatusText() {
            setStatusText(text, notifyState: false)
        }
        notifyStateChanged()
    }

    @discardableResult
    private func handleUploadResult(_ result: BackupSessionAsyncBridge.UploadResult) async -> Bool {
        if case .failed = session.phase {
            notifyStateChanged()
            return false
        }
        switch session.handleUploadResult(result) {
        case .continueToDownload:
            appendInfoLog(String(localized: "home.execution.log.uploadPhaseCompleteStartDownload"))
            setStatusText(String(localized: "home.execution.preparingDownload"))
            _ = await dataRefresher.syncRemoteDataAndWait()
            guard !Task.isCancelled else { return false }
            notifyStateChanged()
            return true
        case .paused:
            deactivateTransferMetrics(notify: false)
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            notifyStateChanged()
            return false
        case .failed(let alert):
            deactivateTransferMetrics(notify: false)
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
            appendInfoLog(String(localized: "home.execution.log.allTasksComplete"))
            deactivateTransferMetrics(notify: false)
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
            deactivateTransferMetrics(notify: false)
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
            return
        }

        guard let context = makeDownloadContext() else {
            let alert = session.failForMissingConnection()
            deactivateTransferMetrics(notify: false)
            setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.executionFailed"), alert.message))
            notifyStateChanged()
            onAlert?(alert.title, alert.message)
            return
        }

        session.beginDownloadPhase()
        appendInfoLog(String(format: String(localized: "home.execution.log.startDownloadPhase"), remaining.count))
        setStatusText(phaseStatusText() ?? String(localized: "home.execution.downloading"), notifyState: false)
        if estimatedDownloadTotalBytes == nil {
            updateEstimatedDownloadTotalBytes(await estimatedDownloadBytes(for: plannedDownloadMonthsForTransferMetrics()))
        }
        notifyStateChanged()

        let completed: Bool
        do {
            completed = try await dependencies.backupCoordinator.withDownloadVerificationPlan(
                profile: context.profile,
                password: context.password
            ) { verifier in
                await self.runDownloadMonths(
                    remaining,
                    context: context,
                    usesExistingTransferPlan: true,
                    verifyMonth: { month in try await verifier.verify(month: month) }
                )
            }
        } catch {
            if RemoteFaultLite.classify(error) == .cancelled { return }
            completed = await runDownloadMonths(remaining, context: context, usesExistingTransferPlan: true)
        }

        if completed, !Task.isCancelled {
            session.finishExecution()
            appendInfoLog(String(localized: "home.execution.log.allTasksComplete"))
            deactivateTransferMetrics(notify: false)
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
        }
    }

    private func runDownloadMonths(
        _ months: [LibraryMonthKey],
        context: DownloadWorkflowHelper.Context,
        usesExistingTransferPlan: Bool = false,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> Bool {
        for month in months {
            if Task.isCancelled { return false }
            let shouldContinue = await runDownloadMonth(
                month,
                context: context,
                phaseLabel: session.phaseLabel(for: month),
                usesExistingTransferPlan: usesExistingTransferPlan,
                verifyMonth: verifyMonth
            )
            if !shouldContinue { return false }
        }
        return true
    }

    private func estimatedDownloadBytes(for months: [LibraryMonthKey]) async -> Int64? {
        var totalBytes: Int64 = 0
        for month in months {
            guard !Task.isCancelled else { return nil }
            let items = await dataAccess.remoteOnlyItems(month)
            totalBytes += DownloadWorkflowHelper.estimatedDownloadBytes(for: items, incompletePolicy: incompleteDownloadPolicy) ?? 0
        }
        return totalBytes > 0 ? totalBytes : nil
    }

    private func runDownloadMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context,
        phaseLabel: String,
        usesExistingTransferPlan: Bool = false,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> Bool {
        session.beginDownloadMonth(month)
        appendInfoLog(String(format: String(localized: "home.execution.log.startDownloadMonth"), phaseLabel, month.displayText))
        let complementLabelOverride: String? = session.complementMonths.contains(month)
            ? String(localized: "home.execution.complementing")
            : nil
        let monthStatus = phaseStatusText(phaseLabelOverride: complementLabelOverride)
            ?? fallbackPhaseLabel()
        setStatusText(monthStatus, notifyState: false)
        notifyStateChanged()

        let assetIDs = dataAccess.localAssetIDs(month)
        let result = await downloadRemoteMonth(
            month,
            assetIDs: assetIDs,
            context: context,
            usesExistingTransferPlan: usesExistingTransferPlan,
            verifyMonth: verifyMonth
        )
        switch applyDownloadResult(result, month: month, phaseLabel: phaseLabel) {
        case .success, .failed:
            return true
        case .fatal, .cancelled:
            return false
        }
    }

    private func notifyStateChanged() {
        onStateChanged?()
        for observer in stateObservers.values {
            observer()
        }
    }

    @discardableResult
    func addStateObserver(_ observer: @escaping @MainActor () -> Void) -> UUID {
        let id = UUID()
        stateObservers[id] = observer
        return id
    }

    func removeStateObserver(_ id: UUID) {
        stateObservers[id] = nil
    }

    private func shouldRunLocalIndexPreflight() -> Bool {
        if session.requiresCompleteLocalIndexBeforeExecution { return true }
        return activeExecutionSettingsSnapshot().iCloudPhotoBackupMode != .disable
    }

    private func prepareLocalIndexIfNeeded() async -> Bool {
        forcedUploadWorkerCountOverride = nil

        let settings = activeExecutionSettingsSnapshot()
        let assetIDs = assetIDsForLocalHashIndexPreflight()
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

            if session.shouldRunUploadPhase,
               settings.iCloudPhotoBackupMode == .enable {
                let uploadScope = session.uploadScopeAssetIDs
                // Cache-valid offloaded assets (networkPending) need the network during upload just like
                // genuinely-unavailable ones, so both force the single-worker downgrade.
                let uploadNetworkPending = initialResult.unavailableAssetIDs
                    .union(initialResult.networkPendingAssetIDs)
                    .intersection(uploadScope)
                if !uploadNetworkPending.isEmpty {
                    let uploadFailed = initialResult.failedAssetIDs.intersection(uploadScope)
                    forcedUploadWorkerCountOverride = 1
                    appendWarningLog(String(format: String(localized: "home.execution.log.icloudUploadDegraded"), uploadNetworkPending.count, uploadFailed.count))
                }
            }

            let result: LocalHashIndexBuildResult
            if session.requiresCompleteLocalIndexBeforeExecution,
               !initialResult.unavailableAssetIDs.isEmpty,
               settings.iCloudPhotoBackupMode == .enable {
                appendWarningLog(String(format: String(localized: "home.execution.log.icloudFound"), initialResult.unavailableAssetIDs.count))
                var iCloudResult = try await dependencies.localHashIndexBuildService.buildIndex(
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

                // Ride out a transient iCloud-fetch wobble: retry the still-unavailable set within a bounded
                // number of passes so one blip doesn't fail an otherwise-complete index.
                var iCloudAttempt = 1
                while !iCloudResult.unavailableAssetIDs.isEmpty, iCloudAttempt < Self.maxICloudPreflightAttempts {
                    guard !Task.isCancelled else { return false }
                    try? await Task.sleep(nanoseconds: Self.iCloudPreflightRetryBackoffNanos)
                    guard !Task.isCancelled else { return false }
                    let retry = try await dependencies.localHashIndexBuildService.buildIndex(
                        for: iCloudResult.unavailableAssetIDs,
                        workerCount: Self.localIndexICloudPreflightWorkerCount,
                        allowNetworkAccess: true,
                        progressHandler: progressHandler
                    )
                    guard !Task.isCancelled else { return false }
                    if !retry.readyAssetIDs.isEmpty {
                        await dataRefresher.refreshLocalIndexAndNotify(retry.readyAssetIDs)
                        guard !Task.isCancelled else { return false }
                    }
                    iCloudResult = mergedLocalIndexBuildResult(initial: iCloudResult, iCloudRecovery: retry)
                    iCloudAttempt += 1
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
                deactivateTransferMetrics(notify: false)
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
            deactivateTransferMetrics(notify: false)
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

        let snapshot = ExecutionSettingsSnapshot.fromCurrentSettings(
            monthGroupingTimeZone: dataAccess.localMonthGroupingTimeZone()
        )
        executionSettingsSnapshot = snapshot
        return snapshot
    }

    /// Upload months read frozen IDs (the work plan is fixed at session.enter; PHChange
    /// additions mid-run shouldn't expand it). Pure-download months read live IDs so
    /// assets uploaded earlier in the same run are recognized and not re-downloaded.
    private func assetIDsForLocalHashIndexPreflight() -> Set<String> {
        var assetIDs = session.uploadScopeAssetIDs
        let uploadMonths = Set(session.backupMonths).union(session.complementMonths)
        for month in session.monthPlans.keys where !uploadMonths.contains(month) {
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
            parts.append(String.localizedStringWithFormat(String(localized: "home.execution.log.unavailableItems"), result.unavailableAssetIDs.count))
        }
        if !result.failedAssetIDs.isEmpty {
            parts.append(String.localizedStringWithFormat(String(localized: "home.execution.log.failedItems"), result.failedAssetIDs.count))
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
            missingAssetIDs: initial.missingAssetIDs.union(iCloudRecovery.missingAssetIDs),
            networkPendingAssetIDs: initial.networkPendingAssetIDs.union(iCloudRecovery.networkPendingAssetIDs)
        )
    }

    private func makeUploadMonthFinalizer() -> BackupMonthFinalizer? {
        guard session.hasComplementMonths else { return nil }
        let context = makeDownloadContext()
        return { [weak self] month, uploadContext in
            guard let self else { return .cancelled }
            return await self.finalizeUploadedMonth(month, context: context, uploadContext: uploadContext)
        }
    }

    private func makeDownloadContext() -> DownloadWorkflowHelper.Context? {
        guard let profile = dependencies.appSession.activeProfile,
              let password = profile.resolvedSessionCredential(from: dependencies.appSession) else {
            return nil
        }
        return DownloadWorkflowHelper.Context(profile: profile, password: password)
    }

    private func finalizeUploadedMonth(
        _ month: LibraryMonthKey,
        context: DownloadWorkflowHelper.Context?,
        uploadContext: BackupMonthUploadContext
    ) async -> BackupMonthFinalizationResult {
        guard session.monthPlans[month]?.needsUpload == true,
              session.monthPlans[month]?.needsDownload == true,
              session.monthPlans[month]?.isTerminal != true else {
            return .success
        }
        guard !Task.isCancelled else { return .cancelled }

        let phaseLabel = session.phaseLabel(for: month)
        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        appendInfoLog(String(format: String(localized: "home.execution.log.uploadDoneStartPhase"), phaseLabel, month.displayText))
        let complementLabel = String(localized: "home.execution.complementing")
        let monthStatus = phaseStatusText(phaseLabelOverride: complementLabel) ?? complementLabel
        setStatusText(monthStatus, notifyState: false)
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
        let result = await downloadRemoteMonth(month, assetIDs: assetIDs, context: context, uploadContext: uploadContext)
        return applyDownloadResult(result, month: month, phaseLabel: phaseLabel)
    }

    private func downloadRemoteMonth(
        _ month: LibraryMonthKey,
        assetIDs: Set<String>,
        context: DownloadWorkflowHelper.Context,
        uploadContext: BackupMonthUploadContext? = nil,
        usesExistingTransferPlan: Bool = false,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> DownloadMonthResult {
        appendInfoLog(String(format: String(localized: "home.execution.log.syncRemoteIndex"), month.displayText))
        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled { return .cancelled }
        if !assetIDs.isEmpty {
            appendDebugLog(String(format: String(localized: "home.execution.log.refreshLocalIndex"), month.displayText))
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled { return .cancelled }
        }

        do {
            if let verifyMonth {
                try await verifyMonth(month)
            } else {
                // In-run finalizer (uploadContext present, Lite) reuses the run's outer write lease.
                try await dependencies.backupCoordinator.verifyMonth(
                    profile: context.profile,
                    password: context.password,
                    month: month,
                    reusing: uploadContext
                )
            }
        } catch {
            if RemoteFaultLite.classify(error) == .cancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            appendWarningLog(String.localizedStringWithFormat(
                String(localized: "manifest.log.reconcileFailed"),
                month.displayText,
                message
            ))
            if let liteError = error as? LiteRepoError, liteError.isUploadFailFast {
                return .fatal(message, liteError)
            }
            if !Self.shouldContinueDownloadAfterVerifyFailure(error) {
                return .failed(message)
            }
        }
        if Task.isCancelled { return .cancelled }

        let remoteItems = await dataAccess.remoteOnlyItems(month)
        appendDebugLog(String(format: String(localized: "home.execution.log.pendingDownload"), month.displayText, remoteItems.count))
        guard let downloadHelper else { return .cancelled }
        if !usesExistingTransferPlan, estimatedDownloadTotalBytes == nil {
            let plannedMonths = plannedDownloadMonthsForTransferMetrics()
            if plannedMonths.isEmpty {
                updateEstimatedDownloadTotalBytes(DownloadWorkflowHelper.estimatedDownloadBytes(for: remoteItems))
            } else {
                updateEstimatedDownloadTotalBytes(await estimatedDownloadBytes(for: plannedMonths))
            }
        }
        return await downloadHelper.downloadItems(
            remoteItems,
            context: context,
            incompletePolicy: incompleteDownloadPolicy,
            onTransferState: { [weak self] state in
                self?.updateTransferMetrics(state)
            }
        ) { [weak self] assetID in
            guard let self else { return }
            await self.dataRefresher.refreshLocalIndexAndNotify([assetID])
        }
    }

    nonisolated static func shouldContinueDownloadAfterVerifyFailure(_ error: Error) -> Bool {
        if RemoteFaultLite.classify(error) == .retryable { return true }
        if let liteError = error as? LiteRepoError {
            // A whole-repo format failure (repoDamaged — e.g. a directory-only V1 candidate now routed
            // .damaged — and its siblings repoUnsupported / repoMaintenanceUnavailable) must fail the month
            // closed, never proceed to a stale-snapshot download that masks the damaged control state and
            // falsely completes the month. Per-month/transient verify failures stay continuable below.
            return liteError.shouldContinueDownloadVerify
        }
        let ns = error as NSError
        // Only the transient missing-manifest signal (-1, cache kept) is continuable. A confirmed-absent
        // (evicted, -2), a reconcile-pruned-but-flush-failed month (-3, cache still holds the un-pruned rows),
        // or a confirmed-corrupt (-34/-35) canonical must fail the month closed — never falsely complete from a
        // cache the verify either evicted or just proved invalid.
        if ns.domain == "RemoteIndexSyncService", ns.code == -1 { return true }
        return false
    }

    @discardableResult
    private func applyDownloadResult(
        _ result: DownloadMonthResult,
        month: LibraryMonthKey,
        phaseLabel: String
    ) -> BackupMonthFinalizationResult {
        switch result {
        case .success(_, let skippedIncompleteCount):
            if skippedIncompleteCount > 0 {
                // Mark month failed so finishExecution reports partial; skip the alert — informational, not a crash.
                let reason = String.localizedStringWithFormat(
                    String(localized: "restore.log.skippedIncomplete"),
                    month.displayText,
                    skippedIncompleteCount
                )
                session.failDownloadMonth(month, reason: reason)
                appendWarningLog(reason)
                refreshTerminalStatus(notifyState: false)
                notifyStateChanged()
                return .success
            }
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
        case .fatal(let message, let error):
            deactivateTransferMetrics(notify: false)
            _ = session.failExecution(reason: message)
            // Don't clear `transientControlState` here: settleStop's guard is `isActive` (true for `.failed`),
            // so a source-side clear would strand its auto-exit. The pause settles resolve a terminal-during-
            // settle themselves (settleDownloadPause), which keeps both pause and stop correct.
            setErrorStatus(message, log: String(format: String(localized: "home.execution.log.downloadFailed"), phaseLabel, month.displayText, message))
            notifyStateChanged()
            onAlert?(String(format: String(localized: "home.execution.log.phaseFailed"), phaseLabel), String(format: String(localized: "home.execution.log.phaseFailedDetail"), month.displayText, message))
            return .fatal(message, error)
        case .cancelled:
            return .cancelled
        }
    }

    private func assetIDsAwaitingInlineComplementResume() -> Set<String> {
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
            case .uploadFailed:
                // The executor already emitted a flush-failure error log; the month surfaces via partial-failure state.
                break
            }
        case .started(let totalAssets, let totalBytes):
            updateEstimatedUploadTotalBytes(totalBytes)
            setStatusText(phaseStatusText() ?? String(localized: "home.execution.uploading"))
            appendInfoLog(String(format: String(localized: "home.execution.log.uploadPhaseStart"), totalAssets))
        case .finished(let result):
            appendLog(
                String(format: String(localized: "home.execution.log.uploadPhaseDone"), result.succeeded, result.failed, result.skipped),
                level: result.failed > 0 ? .warning : .info
            )
        case .progress(let progress):
            appendLog(progress.effectiveLogMessage, level: progress.logLevel)
        case .transferState(let state):
            updateTransferMetrics(state)
        }
    }

    private func resetTransferMetricsForExecution(downloadMonths: [LibraryMonthKey]) {
        cancelDownloadEstimateTask()
        transferPlanGeneration &+= 1
        transferMetricsActive = true
        estimatedUploadTotalBytes = nil
        estimatedDownloadTotalBytes = downloadMonths.isEmpty ? 0 : nil
        transferTracker.clear()
        currentTransferMetrics = .inactive
        notifyLogObservers()
        scheduleDownloadEstimate(for: downloadMonths, generation: transferPlanGeneration)
    }

    private func plannedDownloadMonthsForTransferMetrics() -> [LibraryMonthKey] {
        session.downloadMonths + session.complementMonths
    }

    private func scheduleDownloadEstimate(for months: [LibraryMonthKey], generation: UInt64) {
        guard !months.isEmpty else {
            refreshExecutionTransferTotal()
            return
        }
        downloadEstimateTask = Task { [weak self] in
            guard let self else { return }
            let totalBytes = await self.estimatedDownloadBytes(for: months)
            guard !Task.isCancelled else { return }
            await MainActor.run {
                guard self.transferMetricsActive, self.transferPlanGeneration == generation else { return }
                self.estimatedDownloadTotalBytes = totalBytes ?? 0
                self.refreshExecutionTransferTotal()
            }
        }
    }

    private func cancelDownloadEstimateTask() {
        downloadEstimateTask?.cancel()
        downloadEstimateTask = nil
    }

    private func refreshExecutionTransferTotal() {
        guard transferMetricsActive else { return }
        let uploadBytes = estimatedUploadTotalBytes ?? 0
        let downloadBytes = estimatedDownloadTotalBytes ?? 0
        let totalBytes = uploadBytes + downloadBytes
        transferTracker.updateTotalBytes(totalBytes > 0 ? totalBytes : nil)
        refreshTransferMetrics()
    }

    private func updateEstimatedUploadTotalBytes(_ totalBytes: Int64?) {
        guard transferMetricsActive else { return }
        estimatedUploadTotalBytes = totalBytes
        refreshExecutionTransferTotal()
    }

    private func updateEstimatedDownloadTotalBytes(_ totalBytes: Int64?) {
        guard transferMetricsActive else { return }
        estimatedDownloadTotalBytes = totalBytes ?? 0
        refreshExecutionTransferTotal()
    }

    private func clearTransferMetrics(notify: Bool = true) {
        estimatedUploadTotalBytes = nil
        estimatedDownloadTotalBytes = nil
        transferTracker.clear()
        currentTransferMetrics = .inactive
        if notify {
            notifyLogObservers()
        }
    }

    private func deactivateTransferMetrics(notify: Bool = true) {
        transferPlanGeneration &+= 1
        transferMetricsActive = false
        clearTransferMetrics(notify: notify)
        cancelDownloadEstimateTask()
        stopTransferMetricsRefreshLoop()
        stopMemoryWatermarkLoop()
    }

    private func updateTransferMetrics(_ state: BackupTransferState) {
        guard transferMetricsActive else { return }
        let next = transferTracker.record(state, now: CFAbsoluteTimeGetCurrent())
        guard next != currentTransferMetrics else { return }
        currentTransferMetrics = next
        notifyLogObservers()
    }

    private func startTransferMetricsRefreshLoop() {
        stopTransferMetricsRefreshLoop()
        transferMetricsRefreshTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: 1_000_000_000)
                } catch {
                    return
                }
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    self?.refreshTransferMetrics()
                }
            }
        }
    }

    private func stopTransferMetricsRefreshLoop() {
        transferMetricsRefreshTask?.cancel()
        transferMetricsRefreshTask = nil
    }

    private func refreshTransferMetrics() {
        guard transferMetricsActive else { return }
        let next = transferTracker.snapshot(now: CFAbsoluteTimeGetCurrent())
        guard next != currentTransferMetrics else { return }
        currentTransferMetrics = next
        notifyLogObservers()
    }

    private func appendLog(
        _ message: String,
        level: ExecutionLogLevel = .info
    ) {
        let entry = ExecutionLogEntry(timestamp: Date(), message: message, level: level)
        logEntries.append(entry)
        Self.trimLiveLogEntries(&logEntries)
        sessionLogStreamContinuation?.yield(entry)
        notifyLogObservers()
    }

    // Chunked drop keeps the per-asset append amortized O(1).
    nonisolated static func trimLiveLogEntries(_ entries: inout [ExecutionLogEntry]) {
        guard entries.count > maxLiveLogEntries + liveLogTrimChunk else { return }
        entries.removeFirst(entries.count - maxLiveLogEntries)
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

    private func startMemoryWatermarkLoop() {
        stopMemoryWatermarkLoop()
        appendDebugLog(MemoryDiagnostics.watermarkLine())
        memoryWatermarkTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: MemoryDiagnostics.watermarkIntervalNanos)
                } catch {
                    return
                }
                guard let self, !Task.isCancelled else { return }
                self.appendDebugLog(MemoryDiagnostics.watermarkLine())
            }
        }
    }

    private func stopMemoryWatermarkLoop() {
        memoryWatermarkTask?.cancel()
        memoryWatermarkTask = nil
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
            text = String(localized: "home.execution.completed")
        case .failed(let message):
            text = message
        case .uploadPaused, .downloadPaused:
            text = String(localized: "home.execution.paused")
        case .uploading, .downloading:
            text = phaseStatusText() ?? fallbackPhaseLabel()
        case nil:
            text = String(localized: "home.execution.notStarted")
        }
        setStatusText(text, notifyState: notifyState)
    }

    private func phaseStatusText(phaseLabelOverride: String? = nil) -> String? {
        guard let counter = session.phaseProgressCounter, counter.current > 0 else { return nil }
        let label: String
        if let phaseLabelOverride {
            label = phaseLabelOverride
        } else {
            switch session.phase {
            case .uploading, .uploadPaused:
                label = String(localized: "home.execution.uploading")
            case .downloading, .downloadPaused:
                label = String(localized: "home.execution.downloading")
            default:
                return nil
            }
        }
        return "\(label) \(counter.current)/\(counter.total)"
    }

    private func fallbackPhaseLabel() -> String {
        switch session.phase {
        case .uploading, .uploadPaused:
            return String(localized: "home.execution.uploading")
        case .downloading, .downloadPaused:
            return String(localized: "home.execution.downloading")
        default:
            return ""
        }
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
                guard let self, self.transientControlState == .pausing else { return }
                if self.session.phase == .downloadPaused {
                    self.transientControlState = nil
                    self.setStatusText(String(localized: "home.execution.paused"), notifyState: false)
                    self.appendWarningLog(String(localized: "home.execution.log.executionPaused"))
                    self.notifyStateChanged()
                } else if self.sessionReachedTerminalPhase {
                    // A `.fatal` during the settle flipped the run terminal (`.failed`); the `.downloadPaused`
                    // guard above can never fire, so resolve the transient here (the error status/alert were
                    // already emitted by `applyDownloadResult(.fatal)`) instead of stranding the panel at
                    // `.pausing`. settleStop already tolerates the same case via its `isActive` guard.
                    self.transientControlState = nil
                    self.refreshTerminalStatus(notifyState: false)
                    self.notifyStateChanged()
                }
            }
        }
    }

    private var sessionReachedTerminalPhase: Bool {
        switch session.phase {
        case .some(.completed), .some(.failed):
            return true
        default:
            return false
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
                self.exit()
            }
        }
    }
}
