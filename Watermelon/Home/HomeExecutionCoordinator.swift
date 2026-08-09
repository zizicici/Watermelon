import Foundation
import MoreKit

struct HomeExecutionLogSnapshot {
    let statusText: String
    let transferMetrics: HomeExecutionTransferMetrics
    let entries: [ExecutionLogEntry]
}

struct HomeExecutionTransferMetrics: Equatable {
    let progressFraction: Double?
    let speedBytesPerSecond: Double?
    let remainingTimeSeconds: TimeInterval?

    static let inactive = HomeExecutionTransferMetrics(
        progressFraction: nil,
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
                let (nextTransferredBytes, overflowed) = actualTransferredBytes.addingReportingOverflow(actualDelta)
                actualTransferredBytes = overflowed ? .max : nextTransferredBytes
                lastProgressAt = now
                appendSample(now: now)
            }
        }
        return snapshot(now: now)
    }

    mutating func snapshot(now: CFAbsoluteTime) -> HomeExecutionTransferMetrics {
        trimSamples(referenceTime: samples.last?.timestamp ?? now)
        let progressFraction = currentProgressFraction()
        guard let lastProgressAt, now - lastProgressAt <= Self.recentProgressWindow else {
            smoothedRateBytesPerSecond = nil
            smoothedRateSampleTimestamp = nil
            return HomeExecutionTransferMetrics(
                progressFraction: progressFraction,
                speedBytesPerSecond: nil,
                remainingTimeSeconds: nil
            )
        }
        guard let rateSnapshot = currentRate(), rateSnapshot.bytesPerSecond > 0 else {
            return HomeExecutionTransferMetrics(
                progressFraction: progressFraction,
                speedBytesPerSecond: nil,
                remainingTimeSeconds: nil
            )
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
            progressFraction: progressFraction,
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
        let fraction = Double(state.resourceFraction)
        guard fraction.isFinite else { return nil }
        if fraction <= 0 { return 0 }
        if fraction >= 1 { return total }
        return Int64(exactly: (Double(total) * fraction).rounded())
    }

    private func currentAggregateBytes() -> Int64 {
        progressByKey.values.reduce(Int64(0)) { total, progress in
            let (nextTotal, overflowed) = total.addingReportingOverflow(progress.committedBytes)
            return overflowed ? .max : nextTotal
        }
    }

    private func currentProgressFraction() -> Double? {
        guard let totalBytes, totalBytes > 0 else { return nil }
        return min(max(Double(currentAggregateBytes()) / Double(totalBytes), 0), 1)
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
            profile: ServerProfileRecord?,
            monthGroupingTimeZone: MonthGroupingTimeZonePreference
        ) -> ExecutionSettingsSnapshot {
            ExecutionSettingsSnapshot(
                uploadWorkerCountOverride: profile.map {
                    BackupWorkerCountResolver.workerCountOverride(for: $0)
                } ?? BackupWorkerCountMode.getValue().workerCountOverride,
                iCloudPhotoBackupMode: ICloudPhotoBackupMode.getValue(),
                monthGroupingTimeZone: monthGroupingTimeZone
            )
        }

        func makeUploadRunConfiguration() -> BackupRunConfigurationOverride {
            BackupRunConfigurationOverride(
                workerCountOverride: uploadWorkerCountOverride,
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
    private nonisolated let appRuntimeFlags: AppRuntimeFlags
    private let dataAccess: DataAccess
    private let localHashIndexBuildService: any LocalHashIndexBuilding
    // How this run's download phase treats incomplete remote records (chosen upfront in the UI). Default skip.
    private var incompleteDownloadPolicy: IncompleteDownloadPolicy = .skip

    // MARK: - Runtime

    private var session = HomeExecutionSession()
    private let dataRefresher: HomeExecutionDataRefresher
    private var executionClaim: AppRuntimeFlags.ExecutionClaim?
    private struct ExecutionTaskHandle {
        let id: UUID
        let task: Task<Void, Never>
    }
    private enum ExecutionWorkStage {
        case preflight
        case upload
        case download
    }
    private var executionTask: ExecutionTaskHandle?
    private var executionWorkStage: (id: UUID, stage: ExecutionWorkStage)?
    private var hardCancellationTask: Task<Void, Never>?
    private var exitSettlementTask: Task<Void, Never>?
    private var executionTerminationBridge: HomeExecutionTerminationBridge?
    private var transientControlState: ExecutionControlState?
    private var backupSessionController: BackupSessionController?
    private var backupBridge: BackupSessionAsyncBridge?
    private var downloadHelper: DownloadWorkflowHelper?
    private var executionSettingsSnapshot: ExecutionSettingsSnapshot?
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

    init(
        dependencies: DependencyContainer,
        dataAccess: DataAccess,
        localHashIndexBuildService: (any LocalHashIndexBuilding)? = nil
    ) {
        self.dependencies = dependencies
        self.appRuntimeFlags = dependencies.appRuntimeFlags
        self.dataAccess = dataAccess
        self.localHashIndexBuildService = localHashIndexBuildService
            ?? dependencies.localHashIndexBuildService
        self.dataRefresher = HomeExecutionDataRefresher(
            syncRemoteData: dataAccess.syncRemoteData,
            refreshLocalIndex: dataAccess.refreshLocalIndex
        )
        self.dataRefresher.onStateChanged = { [weak self] in
            self?.notifyStateChanged()
        }
    }

    deinit {
        let pendingExecutionClaim = executionClaim
        let pendingExecutionTask = executionTask
        let pendingCancellationTask = hardCancellationTask
        let controller = backupSessionController
        let flags = appRuntimeFlags
        pendingExecutionTask?.task.cancel()
        Task { @MainActor in
            let cancellationTask = pendingCancellationTask ?? controller?.cancelBackupImmediately()
            if let pendingExecutionTask {
                _ = await pendingExecutionTask.task.value
            }
            if let cancellationTask {
                _ = await cancellationTask.value
            }
            if let pendingExecutionClaim {
                flags.exitExecution(pendingExecutionClaim)
            }
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

    @discardableResult
    func enter(backup: [LibraryMonthKey], download: [LibraryMonthKey], complement: [LibraryMonthKey], incompletePolicy: IncompleteDownloadPolicy = .skip) -> Bool {
        guard executionClaim == nil, exitSettlementTask == nil else { return false }
        guard let executionClaim = dependencies.appRuntimeFlags.tryEnterExecution() else { return false }
        self.executionClaim = executionClaim
        incompleteDownloadPolicy = incompletePolicy
        executionTask = nil
        executionWorkStage = nil
        hardCancellationTask = nil
        exitSettlementTask = nil
        executionTerminationBridge = nil
        transientControlState = nil
        executionSettingsSnapshot = ExecutionSettingsSnapshot.fromCurrentSettings(
            profile: dependencies.appSession.activeProfile,
            monthGroupingTimeZone: dataAccess.localMonthGroupingTimeZone()
        )
        dataRefresher.reset()
        logEntries.removeAll(keepingCapacity: true)
        resetTransferMetricsForExecution(
            expectsUpload: !(backup + complement).isEmpty,
            downloadMonths: download + complement
        )
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
        if exitSettlementTask != nil { return }
        let executionClaim = self.executionClaim
        self.executionClaim = nil
        let flags = dependencies.appRuntimeFlags
        let activeExecutionCancellation = executionTask?.task
        activeExecutionCancellation?.cancel()
        let activeUploadCancellation = hardCancellationTask ?? backupBridge?.hardCancel()
        executionTask = nil
        executionWorkStage = nil
        executionTerminationBridge?.request(.stop)
        executionTerminationBridge = nil
        transientControlState = nil
        executionSettingsSnapshot = nil
        dataRefresher.cancel()
        if let backupEventObserverID {
            backupSessionController?.removeEventObserver(backupEventObserverID)
            self.backupEventObserverID = nil
        }
        session.reset()
        setStatusText(String(localized: "home.execution.notStarted"), notifyState: false)
        logEntries.removeAll(keepingCapacity: true)
        deactivateTransferMetrics()
        finalizeSessionLogWriter()
        notifyLogObservers()
        if activeExecutionCancellation != nil || activeUploadCancellation != nil {
            hardCancellationTask = activeUploadCancellation
            exitSettlementTask = Task { [weak self, flags] in
                if let activeExecutionCancellation {
                    _ = await activeExecutionCancellation.value
                }
                if let activeUploadCancellation {
                    _ = await activeUploadCancellation.value
                }
                if let self {
                    self.hardCancellationTask = nil
                    self.exitSettlementTask = nil
                }
                if let executionClaim {
                    flags.exitExecution(executionClaim)
                }
                self?.notifyStateChanged()
            }
        } else {
            if let executionClaim {
                flags.exitExecution(executionClaim)
            }
            notifyStateChanged()
        }
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
            backupBridge?.markAssetIDsPendingForResume(session.assetIDsAwaitingInlineComplementResume())
            dataRefresher.cancel()
            executionTerminationBridge?.request(.pause)
            if shouldPauseBeforeUploadStart {
                let taskToAwait = executionTask
                executionTask?.task.cancel()
                transientControlState = .pausing
                notifyStateChanged()
                settleUploadPause(after: taskToAwait)
                return
            }

            transientControlState = .pausing
            backupBridge?.requestPause()
            notifyStateChanged()
        case .download:
            deactivateTransferMetrics()
            appendInfoLog(String(localized: "home.execution.log.requestPause"))
            setStatusText(String(localized: "home.execution.log.pausing"))
            let taskToAwait = executionTask
            transientControlState = .pausing
            executionTerminationBridge?.request(.pause)
            dataRefresher.cancel()
            notifyStateChanged()
            settleDownloadPause(after: taskToAwait)
        case nil:
            break
        }
    }

    func resume() {
        guard currentControlState == .idle else { return }
        guard session.resume() != nil else { return }
        resetTransferMetricsForExecution(
            expectsUpload: session.shouldRunUploadPhase,
            downloadMonths: plannedDownloadMonthsForTransferMetrics()
        )
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
            let uploadSnapshot = backupSessionController?.snapshot()
            if uploadSnapshot?.state == .idle || isExecutionInPreflight(taskToAwait) {
                taskToAwait?.task.cancel()
            }
            transientControlState = .stopping
            executionTerminationBridge?.request(.stop)
            dataRefresher.cancel()
            notifyStateChanged()
            backupBridge?.requestStop()
            settleStop(after: taskToAwait)
        case .uploadPaused:
            if transientControlState == .pausing {
                deactivateTransferMetrics()
                appendWarningLog(String(localized: "home.execution.log.requestStop"))
                setStatusText(String(localized: "home.execution.log.stopping"))
                let taskToAwait = executionTask
                if isExecutionInPreflight(taskToAwait) {
                    taskToAwait?.task.cancel()
                }
                transientControlState = .stopping
                executionTerminationBridge?.request(.stop)
                notifyStateChanged()
                backupBridge?.requestStop()
                settleStop(after: taskToAwait)
                return
            }
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            exit()
        case .downloading:
            deactivateTransferMetrics()
            appendWarningLog(String(localized: "home.execution.log.requestStop"))
            setStatusText(String(localized: "home.execution.log.stopping"))
            let taskToAwait = executionTask
            if isExecutionInPreflight(taskToAwait) {
                taskToAwait?.task.cancel()
            }
            transientControlState = .stopping
            executionTerminationBridge?.request(.stop)
            dataRefresher.cancel()
            notifyStateChanged()
            settleStop(after: taskToAwait)
        case .downloadPaused:
            if transientControlState == .pausing {
                deactivateTransferMetrics()
                appendWarningLog(String(localized: "home.execution.log.requestStop"))
                setStatusText(String(localized: "home.execution.log.stopping"))
                let taskToAwait = executionTask
                if isExecutionInPreflight(taskToAwait) {
                    taskToAwait?.task.cancel()
                }
                transientControlState = .stopping
                executionTerminationBridge?.request(.stop)
                dataRefresher.cancel()
                notifyStateChanged()
                settleStop(after: taskToAwait)
                return
            }
            appendWarningLog(String(localized: "home.execution.log.stopped"))
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

        executionTask?.task.cancel()
        transientControlState = nil
        dataRefresher.cancel()
        deactivateTransferMetrics()
        hardCancellationTask = backupBridge?.hardCancel() ?? hardCancellationTask

        let alert = session.failForMissingConnection(message: message)
        setErrorStatus(alert.message, log: String(format: String(localized: "home.execution.log.executionFailed"), alert.message))
        notifyStateChanged()
        onAlert?(alert.title, alert.message)
    }

    // MARK: - Execution Task

    private func startExecution() {
        let terminationBridge = HomeExecutionTerminationBridge()
        let executionID = UUID()
        executionTerminationBridge = terminationBridge
        let task = Task { [weak self] in
            guard let self else { return }
            guard !Task.isCancelled, !terminationBridge.shouldDrain else { return }
            self.setExecutionWorkStage(.preflight, for: executionID)

            if self.session.needsLocalIndexPreflight,
               self.shouldRunLocalIndexPreflight() {
                await MainActor.run {
                    self.transientControlState = .starting
                    self.notifyStateChanged()
                }

                let prepared = await self.prepareLocalIndexIfNeeded(
                    terminationControl: terminationBridge.control
                )
                guard !Task.isCancelled, !terminationBridge.shouldDrain else { return }
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
                self.setExecutionWorkStage(.upload, for: executionID)
                let scope = self.session.consumePendingUploadScope()
                let runConfigurationOverride = self.activeExecutionSettingsSnapshot()
                    .makeUploadRunConfiguration()
                let result = await backupBridge.runUpload(
                    scope: scope,
                    runConfigurationOverride: runConfigurationOverride,
                    onMonthUploaded: self.makeUploadMonthFinalizer(),
                    pendingAssetIDsOnPause: { [weak self] in
                        self?.session.assetIDsAwaitingInlineComplementResume() ?? []
                    }
                ) { [weak self] progress in
                    self?.handleUploadProgress(progress)
                }
                guard !Task.isCancelled else { return }
                guard await self.handleUploadResult(result, terminationBridge: terminationBridge) else { return }
                guard !Task.isCancelled else { return }
            }

            self.setExecutionWorkStage(.preflight, for: executionID)
            await self.runDownloadPhase(
                terminationControl: terminationBridge.control,
                executionID: executionID
            )
        }
        executionTask = ExecutionTaskHandle(id: executionID, task: task)
        executionWorkStage = (executionID, .preflight)
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
    private func handleUploadResult(
        _ result: BackupSessionAsyncBridge.UploadResult,
        terminationBridge: HomeExecutionTerminationBridge
    ) async -> Bool {
        if case .failed = session.phase {
            if transientControlState == .pausing {
                transientControlState = nil
            }
            notifyStateChanged()
            return false
        }
        let outcome = terminationBridge.resolveUploadResult(result, session: &session)
        if transientControlState == .pausing {
            transientControlState = nil
        }
        switch outcome {
        case .continueToDownload:
            if transientControlState == .stopping || terminationBridge.shouldDrain {
                return false
            }
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
            exitAfterExecutionTaskSettles()
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

    private func runDownloadPhase(
        terminationControl: ExecutionTerminationControl,
        executionID: UUID
    ) async {
        if terminationControl.shouldDrain { return }
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
                password: context.password,
                terminationControl: terminationControl
            ) { verifier in
                await self.runDownloadMonths(
                    remaining,
                    context: context,
                    terminationControl: terminationControl,
                    executionID: executionID,
                    usesExistingTransferPlan: true,
                    verifyMonth: { month in try await verifier.verify(month: month) }
                )
            }
        } catch {
            if RemoteFaultLite.classify(error) == .cancelled { return }
            completed = await runDownloadMonths(
                remaining,
                context: context,
                terminationControl: terminationControl,
                executionID: executionID,
                usesExistingTransferPlan: true
            )
        }

        if completed, !Task.isCancelled, !terminationControl.shouldDrain {
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
        terminationControl: ExecutionTerminationControl,
        executionID: UUID,
        usesExistingTransferPlan: Bool = false,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> Bool {
        for month in months {
            if Task.isCancelled || terminationControl.shouldDrain { return false }
            let shouldContinue = await runDownloadMonth(
                month,
                context: context,
                terminationControl: terminationControl,
                executionID: executionID,
                phaseLabel: session.phaseLabel(for: month),
                usesExistingTransferPlan: usesExistingTransferPlan,
                verifyMonth: verifyMonth
            )
            if !shouldContinue { return false }
            if terminationControl.shouldDrain { return false }
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
        terminationControl: ExecutionTerminationControl,
        executionID: UUID,
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
            terminationControl: terminationControl,
            executionID: executionID,
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

    private func prepareLocalIndexIfNeeded(
        terminationControl: ExecutionTerminationControl
    ) async -> Bool {

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
            let initialResult = try await localHashIndexBuildService.buildIndex(
                for: assetIDs,
                workerCount: Self.localIndexPreflightWorkerCount,
                allowNetworkAccess: false,
                progressHandler: progressHandler,
                tickHandler: nil
            )
            guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }

            if !initialResult.readyAssetIDs.isEmpty {
                appendDebugLog(String(format: String(localized: "home.execution.log.indexWriteback"), initialResult.readyAssetIDs.count))
                await dataRefresher.refreshLocalIndexAndNotify(initialResult.readyAssetIDs)
                guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
                appendDebugLog(String(format: String(localized: "home.execution.log.indexRefreshDone"), initialResult.readyAssetIDs.count))
            }

            let result: LocalHashIndexBuildResult
            if session.requiresCompleteLocalIndexBeforeExecution,
               !initialResult.unavailableAssetIDs.isEmpty,
               settings.iCloudPhotoBackupMode == .enable {
                appendWarningLog(String(format: String(localized: "home.execution.log.icloudFound"), initialResult.unavailableAssetIDs.count))
                var iCloudResult = try await localHashIndexBuildService.buildIndex(
                    for: initialResult.unavailableAssetIDs,
                    workerCount: Self.localIndexICloudPreflightWorkerCount,
                    allowNetworkAccess: true,
                    progressHandler: progressHandler,
                    tickHandler: nil
                )
                guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }

                if !iCloudResult.readyAssetIDs.isEmpty {
                    appendDebugLog(String(format: String(localized: "home.execution.log.icloudWriteback"), iCloudResult.readyAssetIDs.count))
                    await dataRefresher.refreshLocalIndexAndNotify(iCloudResult.readyAssetIDs)
                    guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
                    appendDebugLog(String(format: String(localized: "home.execution.log.icloudRefreshDone"), iCloudResult.readyAssetIDs.count))
                }

                // Ride out a transient iCloud-fetch wobble: retry the still-unavailable set within a bounded
                // number of passes so one blip doesn't fail an otherwise-complete index.
                var iCloudAttempt = 1
                while !iCloudResult.unavailableAssetIDs.isEmpty, iCloudAttempt < Self.maxICloudPreflightAttempts {
                    guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
                    try? await Task.sleep(nanoseconds: Self.iCloudPreflightRetryBackoffNanos)
                    guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
                    let retry = try await localHashIndexBuildService.buildIndex(
                        for: iCloudResult.unavailableAssetIDs,
                        workerCount: Self.localIndexICloudPreflightWorkerCount,
                        allowNetworkAccess: true,
                        progressHandler: progressHandler,
                        tickHandler: nil
                    )
                    guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
                    if !retry.readyAssetIDs.isEmpty {
                        await dataRefresher.refreshLocalIndexAndNotify(retry.readyAssetIDs)
                        guard !Task.isCancelled, !terminationControl.shouldDrain else { return false }
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
            if Task.isCancelled || terminationControl.shouldDrain {
                return false
            }
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
            profile: dependencies.appSession.activeProfile,
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
        guard !Task.isCancelled, uploadContext.terminationControl?.shouldDrain != true else {
            markComplementMonthPendingForResume(month)
            return .cancelled
        }

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
        let result = await downloadRemoteMonth(
            month,
            assetIDs: assetIDs,
            context: context,
            terminationControl: uploadContext.terminationControl,
            uploadContext: uploadContext
        )
        let finalization = applyDownloadResult(result, month: month, phaseLabel: phaseLabel)
        if case .cancelled = finalization {
            markComplementMonthPendingForResume(month)
        }
        return finalization
    }

    private func markComplementMonthPendingForResume(_ month: LibraryMonthKey) {
        backupBridge?.markAssetIDsPendingForResume(session.uploadAssetIDsByMonth[month] ?? [])
    }

    private func downloadRemoteMonth(
        _ month: LibraryMonthKey,
        assetIDs: Set<String>,
        context: DownloadWorkflowHelper.Context,
        terminationControl: ExecutionTerminationControl?,
        executionID: UUID? = nil,
        uploadContext: BackupMonthUploadContext? = nil,
        usesExistingTransferPlan: Bool = false,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> DownloadMonthResult {
        if let executionID {
            setExecutionWorkStage(.preflight, for: executionID)
        }
        appendInfoLog(String(format: String(localized: "home.execution.log.syncRemoteIndex"), month.displayText))
        _ = await dataRefresher.syncRemoteDataAndWait()
        if Task.isCancelled || terminationControl?.shouldDrain == true { return .cancelled }
        if !assetIDs.isEmpty {
            appendDebugLog(String(format: String(localized: "home.execution.log.refreshLocalIndex"), month.displayText))
            await dataRefresher.refreshLocalIndexAndNotify(assetIDs)
            if Task.isCancelled || terminationControl?.shouldDrain == true { return .cancelled }
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
        if Task.isCancelled || terminationControl?.shouldDrain == true { return .cancelled }

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
        if let executionID {
            setExecutionWorkStage(.download, for: executionID)
        }
        return await downloadHelper.downloadItems(
            remoteItems,
            context: context,
            incompletePolicy: incompleteDownloadPolicy,
            shouldDrain: { terminationControl?.shouldDrain == true },
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

    private func handleBackupEvent(_ event: BackupEvent) {
        switch event {
        case .log(let message, let level):
            appendLog(message, level: level)
        case .monthChanged(let change):
            let month = LibraryMonthKey(year: change.year, month: change.month)
            switch change.action {
            case .started, .iCloudUploadStarted:
                appendInfoLog(String(format: String(localized: "home.execution.log.uploadStartMonth"), month.displayText))
            case .completed:
                appendInfoLog(String(format: String(localized: "home.execution.log.uploadDoneMonth"), month.displayText))
            case .localUploadCompleted:
                appendInfoLog(String(format: String(localized: "home.execution.log.localUploadDoneMonth"), month.displayText))
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

    private func resetTransferMetricsForExecution(
        expectsUpload: Bool,
        downloadMonths: [LibraryMonthKey]
    ) {
        cancelDownloadEstimateTask()
        transferPlanGeneration &+= 1
        transferMetricsActive = true
        estimatedUploadTotalBytes = expectsUpload ? nil : 0
        estimatedDownloadTotalBytes = downloadMonths.isEmpty ? 0 : nil
        transferTracker.clear()
        currentTransferMetrics = .inactive
        notifyLogObservers()
        scheduleDownloadEstimate(for: downloadMonths, generation: transferPlanGeneration)
    }

    private func plannedDownloadMonthsForTransferMetrics() -> [LibraryMonthKey] {
        session.remainingDownloadMonths()
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
        let totalBytes = Self.resolvedTransferTotalBytes(
            uploadBytes: estimatedUploadTotalBytes,
            downloadBytes: estimatedDownloadTotalBytes
        )
        transferTracker.updateTotalBytes(totalBytes)
        refreshTransferMetrics()
    }

    nonisolated static func resolvedTransferTotalBytes(
        uploadBytes: Int64?,
        downloadBytes: Int64?
    ) -> Int64? {
        guard let uploadBytes, let downloadBytes else { return nil }
        let (totalBytes, overflowed) = uploadBytes.addingReportingOverflow(downloadBytes)
        guard !overflowed else { return nil }
        return totalBytes > 0 ? totalBytes : nil
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

    private func setExecutionWorkStage(_ stage: ExecutionWorkStage, for id: UUID) {
        guard executionTask?.id == id else { return }
        executionWorkStage = (id, stage)
    }

    private func isExecutionInPreflight(_ handle: ExecutionTaskHandle?) -> Bool {
        guard let handle,
              executionWorkStage?.id == handle.id else { return false }
        return executionWorkStage?.stage == .preflight
    }

    private func settleDownloadPause(after handle: ExecutionTaskHandle?) {
        guard let handle else {
            resolveDownloadPauseSettlement(expectedExecutionID: nil)
            return
        }

        Task { [weak self] in
            _ = await handle.task.value
            await MainActor.run {
                self?.resolveDownloadPauseSettlement(expectedExecutionID: handle.id)
            }
        }
    }

    private func resolveDownloadPauseSettlement(expectedExecutionID: UUID?) {
        if let expectedExecutionID {
            guard executionTask?.id == expectedExecutionID else { return }
        }
        guard transientControlState == .pausing else { return }
        executionTask = nil
        executionWorkStage = nil
        if session.phase == .downloadPaused,
           session.finishIfAllMonthsTerminal() {
            transientControlState = nil
            appendInfoLog(String(localized: "home.execution.log.allTasksComplete"))
            deactivateTransferMetrics(notify: false)
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
        } else if session.phase == .downloadPaused {
            transientControlState = nil
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
            notifyStateChanged()
        } else if sessionReachedTerminalPhase {
            transientControlState = nil
            refreshTerminalStatus(notifyState: false)
            notifyStateChanged()
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

    private func settleUploadPause(after handle: ExecutionTaskHandle?) {
        guard let handle else {
            transientControlState = nil
            setStatusText(String(localized: "home.execution.paused"), notifyState: false)
            appendWarningLog(String(localized: "home.execution.log.executionPaused"))
            notifyStateChanged()
            return
        }

        Task { [weak self] in
            _ = await handle.task.value
            await MainActor.run {
                guard let self,
                      self.executionTask?.id == handle.id,
                      self.transientControlState == .pausing,
                      self.session.phase == .uploadPaused else { return }
                self.executionTask = nil
                self.executionWorkStage = nil
                self.transientControlState = nil
                self.setStatusText(String(localized: "home.execution.paused"), notifyState: false)
                self.appendWarningLog(String(localized: "home.execution.log.executionPaused"))
                self.notifyStateChanged()
            }
        }
    }

    private func settleStop(after handle: ExecutionTaskHandle?) {
        guard let handle else {
            appendWarningLog(String(localized: "home.execution.log.stopped"))
            exit()
            return
        }

        Task { [weak self] in
            _ = await handle.task.value
            await MainActor.run {
                guard let self,
                      self.executionTask?.id == handle.id,
                      self.transientControlState == .stopping,
                      self.session.isActive else { return }
                self.appendWarningLog(String(localized: "home.execution.log.stopped"))
                self.exit()
            }
        }
    }

    private func exitAfterExecutionTaskSettles() {
        let handle = executionTask
        Task { [weak self] in
            if let handle {
                _ = await handle.task.value
            }
            await MainActor.run {
                guard let self else { return }
                if let handle {
                    guard self.executionTask?.id == handle.id else { return }
                }
                self.exit()
            }
        }
    }
}
