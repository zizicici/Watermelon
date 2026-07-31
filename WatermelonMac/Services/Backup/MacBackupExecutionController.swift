import Foundation
import Photos

struct MacBackupExecutionPlan: Sendable {
    let backupMonths: Set<LibraryMonthKey>
    let downloadMonths: Set<LibraryMonthKey>
    let complementMonths: Set<LibraryMonthKey>
    let localAssetIDsByMonth: [LibraryMonthKey: Set<String>]
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
    let incompleteDownloadPolicy: IncompleteDownloadPolicy

    var allMonths: Set<LibraryMonthKey> {
        backupMonths.union(downloadMonths).union(complementMonths)
    }

    var uploadMonths: Set<LibraryMonthKey> {
        backupMonths.union(complementMonths)
    }

    var requestedLocalAssetIDs: Set<String> {
        allMonths.reduce(into: Set<String>()) {
            $0.formUnion(localAssetIDsByMonth[$1] ?? [])
        }
    }

    var uploadAssetIDs: Set<String> {
        uploadMonths.reduce(into: Set<String>()) {
            $0.formUnion(localAssetIDsByMonth[$1] ?? [])
        }
    }

    var incompleteDownloadScanSignature:
        MacIncompleteDownloadScanSignature
    {
        let months = downloadMonths.union(complementMonths)
        return MacIncompleteDownloadScanSignature(
            months: months,
            localAssetIDsByMonth: Dictionary(
                uniqueKeysWithValues: months.map {
                    ($0, localAssetIDsByMonth[$0] ?? [])
                }
            )
        )
    }
}

struct MacIncompleteDownloadScanSignature: Equatable, Sendable {
    let months: Set<LibraryMonthKey>
    let localAssetIDsByMonth:
        [LibraryMonthKey: Set<String>]
}

protocol MacBackupCoordinating: Sendable {
    func runBackup(
        request: BackupRunRequest,
        eventStream: BackupEventStream
    ) async throws -> BackupExecutionResult

    func remoteMonthRawData(
        for month: LibraryMonthKey
    ) -> RemoteLibraryMonthDelta?

    func verifyMonth(
        profile: ServerProfileRecord,
        password: String,
        month: LibraryMonthKey,
        reusing uploadContext: BackupMonthUploadContext?
    ) async throws

    func withMacDownloadVerificationPlan(
        profile: ServerProfileRecord,
        password: String,
        body: @MainActor @escaping @Sendable (
            BackupDownloadVerificationPlan
        ) async -> Result<Int, Error>
    ) async throws -> Result<Int, Error>
}

private final class MacDownloadVerificationPlanBodyRunner: Sendable {
    private let body: @MainActor @Sendable (
        BackupDownloadVerificationPlan
    ) async -> Result<Int, Error>

    init(
        body: @MainActor @escaping @Sendable (
            BackupDownloadVerificationPlan
        ) async -> Result<Int, Error>
    ) {
        self.body = body
    }

    @MainActor
    func run(
        verificationPlan: BackupDownloadVerificationPlan
    ) async -> Result<Int, Error> {
        await body(verificationPlan)
    }
}

extension BackupCoordinator: MacBackupCoordinating {
    func withMacDownloadVerificationPlan(
        profile: ServerProfileRecord,
        password: String,
        body: @MainActor @escaping @Sendable (
            BackupDownloadVerificationPlan
        ) async -> Result<Int, Error>
    ) async throws -> Result<Int, Error> {
        let runner = MacDownloadVerificationPlanBodyRunner(
            body: body
        )
        return try await withDownloadVerificationPlan(
            profile: profile,
            password: password,
            terminationControl: nil
        ) { verificationPlan in
            let task = Task.detached {
                await runner.run(
                    verificationPlan: verificationPlan
                )
            }
            return await withTaskCancellationHandler {
                await task.value
            } onCancel: {
                task.cancel()
            }
        }
    }
}

protocol MacLocalHashIndexBuilding: Sendable {
    func buildIndex(
        for assetIDs: Set<String>,
        workerCount: Int,
        allowNetworkAccess: Bool,
        progressHandler: LocalHashIndexProgressHandler?,
        tickHandler: LocalHashIndexProgressTickHandler?
    ) async throws -> LocalHashIndexBuildResult
}

extension LocalHashIndexBuildService: MacLocalHashIndexBuilding {}

@MainActor
protocol MacDownloadWorkflowHelping: AnyObject {
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: DownloadWorkflowHelper.Context,
        incompletePolicy: IncompleteDownloadPolicy,
        onTransferState:
            @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored:
            @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult
}

extension DownloadWorkflowHelper: MacDownloadWorkflowHelping {
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        incompletePolicy: IncompleteDownloadPolicy,
        onTransferState:
            @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored:
            @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        await downloadItems(
            remoteItems,
            context: context,
            incompletePolicy: incompletePolicy,
            shouldDrain: { false },
            onTransferState: onTransferState,
            onItemRestored: onItemRestored
        )
    }
}

struct MacBackupExecutionSettingsSnapshot: Equatable, Sendable {
    let uploadWorkerCountOverride: Int?
    let iCloudMode: ICloudPhotoBackupMode

    static func capture(
        profile: ServerProfileRecord?,
        globalWorkerCountMode:
            BackupWorkerCountMode = .persistedValue,
        iCloudMode:
            ICloudPhotoBackupMode = .persistedValue
    ) -> MacBackupExecutionSettingsSnapshot {
        MacBackupExecutionSettingsSnapshot(
            uploadWorkerCountOverride: profile.map {
                BackupWorkerCountResolver.workerCountOverride(
                    for: $0,
                    globalDefault: globalWorkerCountMode
                )
            } ?? globalWorkerCountMode.workerCountOverride,
            iCloudMode: iCloudMode
        )
    }

    func uploadWorkerCountOverride(
        requiresSingleWorker: Bool
    ) -> Int? {
        requiresSingleWorker ? 1 : uploadWorkerCountOverride
    }
}

struct MacBackupExecutionSummary: Sendable {
    let upload: BackupExecutionResult?
    let restoredCount: Int
    let skippedIncompleteCount: Int
    let failedDownloadMonths: Int
}

enum MacBackupExecutionStage: Sendable {
    case preflight
    case upload
    case download
}

enum MacBackupExecutionState: Sendable {
    case idle
    case preflighting(processed: Int, total: Int, message: String)
    case uploading(BackupProgress?)
    case downloading(
        month: LibraryMonthKey,
        itemPosition: Int,
        totalItems: Int
    )
    case pausing
    case paused(MacBackupExecutionStage)
    case resuming(MacBackupExecutionStage)
    case stopping
    case completed(MacBackupExecutionSummary)
    case failed(String)
    case cancelled

    var isActive: Bool {
        switch self {
        case .preflighting, .uploading, .downloading,
             .pausing, .paused, .resuming, .stopping,
             .completed, .failed:
            return true
        case .idle, .cancelled:
            return false
        }
    }

    var requiresSafeStopBeforeTermination: Bool {
        requiresExecutionLease
    }

    var acceptsStopRequest: Bool {
        switch self {
        case .preflighting, .uploading, .downloading,
             .pausing, .paused, .resuming:
            return true
        case .idle, .stopping, .completed, .failed, .cancelled:
            return false
        }
    }

    var requiresExecutionLease: Bool {
        switch self {
        case .preflighting, .uploading, .downloading,
             .pausing, .paused, .resuming, .stopping:
            return true
        case .idle, .completed, .failed, .cancelled:
            return false
        }
    }

    var isAwaitingResultDismissal: Bool {
        switch self {
        case .completed, .failed:
            return true
        case .idle, .preflighting, .uploading, .downloading,
             .pausing, .paused, .resuming, .stopping, .cancelled:
            return false
        }
    }
}

struct MacExecutionLogLiveSnapshot {
    let sessionURL: URL
    let statusText: String
    let transferMetrics: HomeExecutionTransferMetrics
}

enum MacBackupExecutionLogPresentation {
    static func statusText(
        for state: MacBackupExecutionState
    ) -> String {
        switch state {
        case .idle:
            return String(
                localized: "home.execution.notStarted"
            )
        case .preflighting(_, _, let message):
            return message
        case .uploading(let progress):
            guard let progress else {
                return String(
                    localized: "home.execution.uploading"
                )
            }
            return String.localizedStringWithFormat(
                String(
                    localized: "mac.execution.uploadProgress"
                ),
                Int64(
                    progress.succeeded
                        + progress.failed
                        + progress.skipped
                ),
                Int64(progress.total)
            )
        case .downloading(
            let month,
            let itemPosition,
            let totalItems
        ):
            guard totalItems > 0 else {
                return String(
                    localized:
                        "home.execution.preparingDownload"
                )
            }
            return String.localizedStringWithFormat(
                String(
                    localized: "mac.execution.downloadProgress"
                ),
                month.displayText,
                Int64(itemPosition),
                Int64(totalItems)
            )
        case .pausing:
            return String(
                localized: "home.execution.log.pausing"
            )
        case .paused:
            return String(
                localized: "home.execution.paused"
            )
        case .resuming:
            return String(
                localized:
                    "home.execution.log.resumingStatus"
            )
        case .stopping:
            return String(
                localized: "home.execution.log.stopping"
            )
        case .completed:
            return String(
                localized: "home.execution.completed"
            )
        case .failed(let message):
            return message
        case .cancelled:
            return String(
                localized: "home.execution.log.stopped"
            )
        }
    }
}

enum MacExecutionTerminationPolicy {
    static func isBlocking(
        manualBackupState: MacBackupExecutionState,
        runtimeExecuting: Bool
    ) -> Bool {
        if manualBackupState.requiresSafeStopBeforeTermination {
            return true
        }
        return runtimeExecuting
    }
}

enum MacBackupExecutionStartContextPolicy {
    static func allowsStart(
        profileID: Int64?,
        expectedSessionGeneration: UInt64?,
        session: AppSession.Snapshot
    ) -> Bool {
        guard let profileID,
              let profile = session.activeProfile,
              profile.id == profileID else {
            return false
        }
        if let expectedSessionGeneration,
           session.generation != expectedSessionGeneration {
            return false
        }
        return !profile.storageProfile.requiresStoredCredential
            || session.activePassword != nil
    }
}

private enum MacBackupExecutionError: LocalizedError {
    case sessionUnavailable
    case noAssets
    case localIndexIncomplete(
        unavailable: Int,
        failed: Int,
        iCloudMode: ICloudPhotoBackupMode
    )

    var errorDescription: String? {
        switch self {
        case .sessionUnavailable:
            return String(
                localized: "backup.session.missingConnection"
            )
        case .noAssets:
            return String(
                localized: "mac.execution.noAssets",
                defaultValue: "None of the selected photos are still available."
            )
        case .localIndexIncomplete(
            let unavailable,
            let failed,
            let iCloudMode
        ):
            var parts: [String] = []
            if unavailable > 0 {
                parts.append(
                    String.localizedStringWithFormat(
                        String(
                            localized:
                                "home.execution.log.unavailableItems"
                        ),
                        unavailable
                    )
                )
            }
            if failed > 0 {
                parts.append(
                    String.localizedStringWithFormat(
                        String(
                            localized:
                                "home.execution.log.failedItems"
                        ),
                        failed
                    )
                )
            }
            let detail = parts.joined(separator: ", ")
            if unavailable > 0 && iCloudMode == .disable {
                return String(
                    format: String(
                        localized:
                            "home.execution.log.indexIncompleteICloud"
                    ),
                    detail
                )
            }
            return String(
                format: String(
                    localized:
                        "home.execution.log.indexIncomplete"
                ),
                detail
            )
        }
    }
}

private struct MacBackupPreparedUpload {
    let index: LocalHashIndexBuildResult
    let requiresSingleWorker: Bool
}

private struct MacBackupExecutionContext {
    let profileID: Int64?
    let sessionGeneration: UInt64
    let plan: MacBackupExecutionPlan
    let settings: MacBackupExecutionSettingsSnapshot
}

@MainActor
final class MacBackupExecutionController {
    private static let localWorkerCount = 2
    private static let iCloudWorkerCount = 1
    private static let maxICloudAttempts = 3
    private static let iCloudRetryDelayNanos: UInt64 = 3_000_000_000

    private let appSession: AppSession
    private let photoLibraryService: PhotoLibraryService
    private let localHashIndexBuildService:
        any MacLocalHashIndexBuilding
    private let backupCoordinator: any MacBackupCoordinating
    private let remoteLibraryReadService: RemoteLibraryReadService
    private let hashIndexRepository: ContentHashIndexRepository
    private let downloadWorkflowHelper:
        any MacDownloadWorkflowHelping
    private let appRuntimeFlags: AppRuntimeFlags
    private let makeManualLogWriter:
        @MainActor () -> ExecutionLogSessionWriter
    private var runTask: Task<Void, Never>?
    private var eventTask: Task<Void, Never>?
    private var generation: UInt64 = 0
    private var restoredCount = 0
    private var skippedIncompleteCount = 0
    private var completedComplementMonths = Set<LibraryMonthKey>()
    private var completedUploadMonths = Set<LibraryMonthKey>()
    private var completedDownloadMonths = Set<LibraryMonthKey>()
    private var monthExecutionTracker =
        MacMonthExecutionTracker()
    private var restoredAssetIDsByMonth:
        [LibraryMonthKey: Set<String>] = [:]
    private var activeDownloadCount = 0
    private var localIndexPreflightCompleted = false
    private var forcedSingleUploadWorker = false
    private var activeContext: MacBackupExecutionContext?
    private var activeLogWriter: ExecutionLogSessionWriter?
    private var terminationIntent: ExecutionTerminationIntent = .none
    private var pauseRequestedStage: MacBackupExecutionStage = .preflight
    private var executionClaim: AppRuntimeFlags.ExecutionClaim?
    private var holdsExecutionLease: Bool {
        executionClaim != nil
    }
    private var transferTracker = HomeExecutionTransferTracker()
    private var currentTransferMetrics =
        HomeExecutionTransferMetrics.inactive
    private var transferMetricsRefreshTask: Task<Void, Never>?
    private let downloadEstimateScheduler =
        MacDownloadEstimateScheduler()
    private var estimatedUploadTotalBytes: Int64?
    private var estimatedDownloadTotalBytes: Int64?
    private var transferMetricsActive = false
    private(set) var currentSessionLogURL: URL?
    var activeSessionLogURL: URL? {
        activeLogWriter == nil ? nil : currentSessionLogURL
    }
    var currentLogLiveSnapshot: MacExecutionLogLiveSnapshot? {
        guard let sessionURL = activeSessionLogURL else {
            return nil
        }
        return MacExecutionLogLiveSnapshot(
            sessionURL: sessionURL,
            statusText:
                MacBackupExecutionLogPresentation.statusText(
                    for: state
                ),
            transferMetrics: currentTransferMetrics
        )
    }

    private(set) var state: MacBackupExecutionState = .idle {
        didSet {
            if holdsExecutionLease && !state.requiresExecutionLease {
                releaseExecutionLease()
            }
            onChange?(state)
        }
    }

    var onChange: ((MacBackupExecutionState) -> Void)?
    var onMonthExecutionChange: (() -> Void)?
    var onRemoteSnapshot: ((
        RemoteLibrarySnapshotState,
        UInt64
    ) -> Void)?
    var onLocalLibraryChanged: (() -> Void)?

    var monthExecutionPhases:
        [LibraryMonthKey: MacMonthExecutionPhase]
    {
        monthExecutionTracker.phases
    }

    var monthExecutionProgress:
        [LibraryMonthKey: MacMonthExecutionProgress]
    {
        monthExecutionTracker.progress
    }

    init(
        appSession: AppSession,
        photoLibraryService: PhotoLibraryService,
        localHashIndexBuildService:
            any MacLocalHashIndexBuilding,
        backupCoordinator: any MacBackupCoordinating,
        remoteLibraryReadService: RemoteLibraryReadService,
        hashIndexRepository: ContentHashIndexRepository,
        downloadWorkflowHelper:
            any MacDownloadWorkflowHelping,
        appRuntimeFlags: AppRuntimeFlags,
        makeManualLogWriter:
            @escaping @MainActor () -> ExecutionLogSessionWriter = {
                ExecutionLogFileStore.beginSession(
                    kind: .manual
                )
            }
    ) {
        self.appSession = appSession
        self.photoLibraryService = photoLibraryService
        self.localHashIndexBuildService = localHashIndexBuildService
        self.backupCoordinator = backupCoordinator
        self.remoteLibraryReadService = remoteLibraryReadService
        self.hashIndexRepository = hashIndexRepository
        self.downloadWorkflowHelper = downloadWorkflowHelper
        self.appRuntimeFlags = appRuntimeFlags
        self.makeManualLogWriter = makeManualLogWriter
    }

    deinit {
        runTask?.cancel()
        eventTask?.cancel()
        transferMetricsRefreshTask?.cancel()
        if let executionClaim {
            appRuntimeFlags.exitExecution(executionClaim)
        }
    }

    @discardableResult
    func start(
        profileID: Int64?,
        expectedSessionGeneration: UInt64? = nil,
        plan: MacBackupExecutionPlan
    ) -> Bool {
        guard !state.isActive,
              !plan.allMonths.isEmpty else {
            return false
        }
        let session = appSession.snapshot
        guard MacBackupExecutionStartContextPolicy.allowsStart(
                  profileID: profileID,
                  expectedSessionGeneration:
                    expectedSessionGeneration,
                  session: session
              ),
              let profile = session.activeProfile,
              let executionClaim =
                appRuntimeFlags.tryEnterExecution() else {
            return false
        }
        self.executionClaim = executionClaim
        appRuntimeFlags.setExecutionCancellationHandler(
            for: self,
            claim: executionClaim
        ) {
            $0.cancel()
        }
        let context = MacBackupExecutionContext(
            profileID: profileID,
            sessionGeneration: session.generation,
            plan: plan,
            settings: .capture(profile: profile)
        )
        let logWriter = beginManualLogSession()
        activeContext = context
        activeLogWriter = logWriter
        currentSessionLogURL = logWriter.fileURL
        restoredCount = 0
        skippedIncompleteCount = 0
        completedComplementMonths.removeAll()
        completedUploadMonths.removeAll()
        completedDownloadMonths.removeAll()
        monthExecutionTracker = MacMonthExecutionTracker(
            plan: plan
        )
        restoredAssetIDsByMonth.removeAll()
        activeDownloadCount = 0
        localIndexPreflightCompleted = false
        forcedSingleUploadWorker = false
        terminationIntent = .none
        resetTransferMetrics(for: plan)
        state = .preflighting(
            processed: 0,
            total: plan.requestedLocalAssetIDs.count,
            message: String(
                localized: "home.execution.log.indexStatus"
            )
        )
        launchRun(context: context, logWriter: logWriter)
        return true
    }

    func incompleteDownloadItemCount(
        for plan: MacBackupExecutionPlan
    ) async -> Int {
        let months = plan.downloadMonths.union(
            plan.complementMonths
        )
        let inputs = months.compactMap { month -> (
            RemoteLibraryMonthDelta,
            Set<String>
        )? in
            guard let delta = backupCoordinator.remoteMonthRawData(
                for: month
            ) else {
                return nil
            }
            return (
                delta,
                plan.localAssetIDsByMonth[month] ?? []
            )
        }
        guard !inputs.isEmpty else { return 0 }
        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        return await withCancellableDetachedValue(
            priority: .userInitiated
        ) {
            guard !Task.isCancelled else { return 0 }
            let allLocalIDs = inputs.reduce(into: Set<String>()) {
                $0.formUnion($1.1)
            }
            let rows = (
                try? repository.fetchValidIndexedRows(
                    assetIDs: allLocalIDs
                )
            ) ?? [:]
            guard !Task.isCancelled else { return 0 }
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: Set(rows.keys)
            )
            guard !Task.isCancelled else { return 0 }
            let fingerprintRecords = rows.mapValues {
                LocalAssetFingerprintRecord(
                    fingerprint: $0.assetFingerprint,
                    updatedAt: $0.updatedAt
                )
            }
            let currentFingerprintsByID =
                MacDownloadLocalFingerprintPolicy.freshRecords(
                    snapshots: assets.map(snapshot),
                    records: fingerprintRecords
                ).mapValues(\.fingerprint)
            return inputs.reduce(into: 0) { count, input in
                let localFingerprints = Set(
                    input.1.compactMap {
                        currentFingerprintsByID[$0]
                    }
                )
                count += HomeAlbumMatching.buildRemoteItems(
                    assets: input.0.assets,
                    resources: input.0.resources,
                    links: input.0.assetResourceLinks
                ).lazy.filter {
                    $0.isIncomplete
                        && !localFingerprints.contains(
                            $0.assetFingerprint
                        )
                }.count
            }
        }
    }

    private func launchRun(
        context: MacBackupExecutionContext,
        logWriter: ExecutionLogSessionWriter
    ) {
        generation &+= 1
        let expectedGeneration = generation
        let eventStream = BackupEventStream()
        eventTask?.cancel()
        let eventConsumer = Task { [weak self] in
            for await event in eventStream.stream {
                guard let self,
                      self.generation == expectedGeneration else {
                    return
                }
                self.apply(event: event)
                switch event {
                case .log(let message, let level):
                    await logWriter.appendLog(message, level: level)
                case .progress(let progress):
                    await logWriter.appendLog(
                        progress.effectiveLogMessage,
                        level: progress.logLevel
                    )
                case .started(let totalAssets, let totalBytes):
                    self.updateEstimatedUploadTotalBytes(
                        totalBytes
                    )
                    await logWriter.appendLog(
                        String.localizedStringWithFormat(
                            String(
                                localized:
                                    "home.execution.log.uploadPhaseStart"
                            ),
                            Int64(totalAssets)
                        ),
                        level: .info
                    )
                case .finished(let result):
                    await logWriter.appendLog(
                        String.localizedStringWithFormat(
                            String(
                                localized:
                                    "home.execution.log.uploadPhaseDone"
                            ),
                            Int64(result.succeeded),
                            Int64(result.failed),
                            Int64(result.skipped)
                        ),
                        level: result.failed == 0 ? .info : .warning
                    )
                case .writeBoundaryReached,
                     .transferState, .monthChanged:
                    break
                }
            }
        }
        eventTask = eventConsumer

        runTask = Task { [weak self] in
            await self?.run(
                context: context,
                eventStream: eventStream,
                eventConsumer: eventConsumer,
                logWriter: logWriter,
                generation: expectedGeneration
            )
        }
    }

    private func run(
        context: MacBackupExecutionContext,
        eventStream: BackupEventStream,
        eventConsumer: Task<Void, Never>,
        logWriter: ExecutionLogSessionWriter,
        generation expectedGeneration: UInt64
    ) async {
        let plan = context.plan
        var remoteSnapshotPublicationPending = false
        var uploadResultForSummary: BackupExecutionResult?
        defer {
            eventStream.finish()
            if generation == expectedGeneration {
                runTask = nil
            }
        }

        do {
            await logWriter.appendLog(
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "home.execution.log.startExecution"
                    ),
                    Int64(plan.backupMonths.count),
                    Int64(plan.downloadMonths.count),
                    Int64(plan.complementMonths.count)
                ),
                level: .info
            )
            let session = appSession.snapshot
            guard let profile = session.activeProfile,
                  profile.id == context.profileID,
                  session.generation == context.sessionGeneration,
                  let credential = profile.resolvedSessionCredential(
                    from: appSession
                  ) else {
                throw MacBackupExecutionError.sessionUnavailable
            }

            let requiresCompleteLocalIndex =
                !plan.downloadMonths.isEmpty
                || !plan.complementMonths.isEmpty
            let shouldRunLocalIndexPreflight =
                requiresCompleteLocalIndex
                || context.settings.iCloudMode != .disable
            let preflight: MacBackupPreparedUpload
            if localIndexPreflightCompleted {
                preflight = MacBackupPreparedUpload(
                    index: Self.emptyPreparedUpload.index,
                    requiresSingleWorker: forcedSingleUploadWorker
                )
            } else if plan.requestedLocalAssetIDs.isEmpty
                        || !shouldRunLocalIndexPreflight {
                preflight = Self.emptyPreparedUpload
            } else {
                preflight = try await prepareLocalIndex(
                    assetIDs: plan.requestedLocalAssetIDs,
                    uploadAssetIDs: plan.uploadAssetIDs,
                    requiresCompleteIndex: requiresCompleteLocalIndex,
                    iCloudMode: context.settings.iCloudMode,
                    logWriter: logWriter,
                    generation: expectedGeneration
                )
            }
            try Task.checkCancellation()
            guard generation == expectedGeneration else { return }
            localIndexPreflightCompleted = true
            forcedSingleUploadWorker =
                preflight.requiresSingleWorker
            remoteSnapshotPublicationPending =
                !plan.uploadAssetIDs.isEmpty
            let uploadResult = try await runUploadIfNeeded(
                assetIDs: plan.uploadAssetIDs,
                plan: plan,
                preflight: preflight,
                profile: profile,
                credential: credential,
                settings: context.settings,
                eventStream: eventStream,
                logWriter: logWriter
            )
            uploadResultForSummary = uploadResult
            await finishAndDrainEvents(
                eventStream,
                consumer: eventConsumer,
                generation: expectedGeneration
            )
            guard generation == expectedGeneration else { return }
            publishRemoteSnapshotIfNeeded(
                &remoteSnapshotPublicationPending,
                sessionGeneration: context.sessionGeneration
            )
            try Task.checkCancellation()
            if uploadResult?.paused == true {
                if interruptionDisposition(for: plan) == .finish {
                    await completeRun(
                        uploadResult: uploadResult,
                        failedDownloadMonths: 0,
                        logWriter: logWriter
                    )
                } else {
                    await pauseForRecoverableInterruption(
                        stage: .upload,
                        logWriter: logWriter
                    )
                }
                return
            }

            let remainingDownloads = plan.downloadMonths
                .union(
                    plan.complementMonths.subtracting(
                        completedComplementMonths
                    )
                )
                .sorted()
            remoteSnapshotPublicationPending =
                !remainingDownloads.isEmpty
            let failedDownloadMonths = try await downloadMonths(
                remainingDownloads,
                localAssetIDsByMonth: plan.localAssetIDsByMonth,
                profile: profile,
                credential: credential,
                incompletePolicy:
                    plan.incompleteDownloadPolicy,
                logWriter: logWriter
            )
            guard generation == expectedGeneration else { return }
            publishRemoteSnapshotIfNeeded(
                &remoteSnapshotPublicationPending,
                sessionGeneration: context.sessionGeneration
            )
            try Task.checkCancellation()

            if uploadResult == nil, remainingDownloads.isEmpty {
                throw MacBackupExecutionError.noAssets
            }
            await completeRun(
                uploadResult: uploadResult,
                failedDownloadMonths: failedDownloadMonths,
                logWriter: logWriter
            )
        } catch is CancellationError {
            await finishAndDrainEvents(
                eventStream,
                consumer: eventConsumer,
                generation: expectedGeneration
            )
            guard generation == expectedGeneration else { return }
            publishRemoteSnapshotIfNeeded(
                &remoteSnapshotPublicationPending,
                sessionGeneration: context.sessionGeneration
            )
            let disposition = MacBackupRunFailurePolicy.disposition(
                intent: terminationIntent,
                source: .taskCancellation,
                allOperationsCommitted:
                    interruptionDisposition(for: plan) == .finish
            )
            switch disposition {
            case .complete:
                await completeRun(
                    uploadResult: uploadResultForSummary,
                    failedDownloadMonths: 0,
                    logWriter: logWriter
                )
            case .pause:
                terminationIntent = .none
                if restoredCount > 0 {
                    onLocalLibraryChanged?()
                }
                await logWriter.appendLog(
                    String(
                        localized:
                            "home.execution.log.executionPaused"
                    ),
                    level: .info
                )
                updateMonthExecution {
                    $0.pause(pauseRequestedStage)
                }
                state = .paused(pauseRequestedStage)
            case .cancel:
                if restoredCount > 0 {
                    onLocalLibraryChanged?()
                }
                await logWriter.appendLog(
                    String(
                        localized:
                            "home.execution.log.stopped"
                    ),
                    level: .warning
                )
                await endSession(logWriter: logWriter)
                releaseExecutionLease()
                clearMonthExecution()
                state = .cancelled
            case .fail:
                preconditionFailure(
                    "Task cancellation cannot produce a fatal disposition"
                )
            }
        } catch {
            await finishAndDrainEvents(
                eventStream,
                consumer: eventConsumer,
                generation: expectedGeneration
            )
            guard generation == expectedGeneration else { return }
            publishRemoteSnapshotIfNeeded(
                &remoteSnapshotPublicationPending,
                sessionGeneration: context.sessionGeneration
            )
            let faultCategory = RemoteFaultLite.classify(error)
            let repoTransient =
                (error as? LiteRepoError)?
                    .isRetryableTransportFault ?? false
            let resumable =
                error is BackupNetworkRecoveryExhausted
                || faultCategory == .retryable
                || repoTransient
            let source: MacBackupRunFailureSource
            if faultCategory == .cancelled {
                source = .classifiedCancellation
            } else if resumable {
                source = .recoverable
            } else {
                source = .fatal
            }
            let disposition = MacBackupRunFailurePolicy.disposition(
                intent: terminationIntent,
                source: source,
                allOperationsCommitted:
                    interruptionDisposition(for: plan) == .finish
            )
            switch disposition {
            case .complete:
                await completeRun(
                    uploadResult: uploadResultForSummary,
                    failedDownloadMonths: 0,
                    logWriter: logWriter
                )
            case .pause:
                if restoredCount > 0 {
                    onLocalLibraryChanged?()
                }
                await pauseForRecoverableInterruption(
                    stage: currentRunningStage
                        ?? pauseRequestedStage,
                    logWriter: logWriter
                )
            case .cancel:
                if restoredCount > 0 {
                    onLocalLibraryChanged?()
                }
                await logWriter.appendLog(
                    String(
                        localized:
                            "home.execution.log.stopped"
                    ),
                    level: .warning
                )
                await endSession(logWriter: logWriter)
                releaseExecutionLease()
                clearMonthExecution()
                state = .cancelled
            case .fail:
                if restoredCount > 0 {
                    onLocalLibraryChanged?()
                }
                let message = UserFacingErrorLocalizer.message(
                    for: error,
                    profile: appSession.activeProfile
                )
                await logWriter.appendLog(message, level: .error)
                await endSession(logWriter: logWriter)
                updateMonthExecution {
                    $0.failRemaining()
                }
                state = .failed(message)
            }
        }
    }

    private func completeRun(
        uploadResult: BackupExecutionResult?,
        failedDownloadMonths: Int,
        logWriter: ExecutionLogSessionWriter
    ) async {
        updateMonthExecution {
            $0.completeRemaining()
        }
        let summary = MacBackupExecutionSummary(
            upload: uploadResult,
            restoredCount: restoredCount,
            skippedIncompleteCount: skippedIncompleteCount,
            failedDownloadMonths: failedDownloadMonths
        )
        if (uploadResult?.failed ?? 0) == 0,
           failedDownloadMonths == 0,
           skippedIncompleteCount == 0 {
            await logWriter.appendLog(
                String(
                    localized:
                        "home.execution.log.allTasksComplete"
                ),
                level: .info
            )
        }
        await endSession(logWriter: logWriter)
        state = .completed(summary)
        onLocalLibraryChanged?()
    }

    private func publishRemoteSnapshotIfNeeded(
        _ isNeeded: inout Bool,
        sessionGeneration: UInt64
    ) {
        guard isNeeded else { return }
        isNeeded = false
        onRemoteSnapshot?(
            remoteLibraryReadService.currentSnapshotState(),
            sessionGeneration
        )
    }

    private func finishAndDrainEvents(
        _ eventStream: BackupEventStream,
        consumer: Task<Void, Never>,
        generation expectedGeneration: UInt64
    ) async {
        await eventStream.finishAndDrain(consumer)
        if generation == expectedGeneration {
            eventTask = nil
        }
    }

    func pause() {
        guard let stage = currentRunningStage else { return }
        pauseRequestedStage = stage
        terminationIntent = .pause
        deactivateTransferMetrics()
        state = .pausing
        runTask?.cancel()
    }

    func resume() {
        guard case .paused(let stage) = state,
              let context = activeContext,
              let logWriter = activeLogWriter else {
            return
        }
        let remainingContext = MacBackupExecutionContext(
            profileID: context.profileID,
            sessionGeneration: context.sessionGeneration,
            plan: remainingPlan(from: context.plan),
            settings: context.settings
        )
        activeContext = remainingContext
        terminationIntent = .none
        resetTransferMetrics(for: remainingContext.plan)
        updateMonthExecution {
            $0.resume(stage)
        }
        state = .resuming(stage)
        Task {
            await logWriter.appendLog(
                String(
                    localized:
                        "home.execution.log.resuming"
                ),
                level: .info
            )
        }
        launchRun(context: remainingContext, logWriter: logWriter)
    }

    func stop() {
        guard state.acceptsStopRequest else { return }
        terminationIntent = .stop
        deactivateTransferMetrics()
        state = .stopping
        if runTask != nil {
            runTask?.cancel()
            return
        }
        guard let logWriter = activeLogWriter else {
            releaseExecutionLease()
            clearMonthExecution()
            state = .cancelled
            return
        }
        Task { [weak self] in
            guard let self else { return }
            await logWriter.appendLog(
                String(
                    localized:
                        "home.execution.log.stopped"
                ),
                level: .warning
            )
            await self.endSession(logWriter: logWriter)
            self.releaseExecutionLease()
            self.clearMonthExecution()
            self.state = .cancelled
        }
    }

    func cancel() {
        stop()
    }

    func resetPresentation() {
        switch state {
        case .completed, .failed:
            releaseExecutionLease()
            clearMonthExecution()
            state = .idle
        case .cancelled:
            clearMonthExecution()
            state = .idle
        default:
            break
        }
    }

    #if DEBUG
    func showDemoProgress() {
        guard !state.isActive,
              let executionClaim =
                appRuntimeFlags.tryEnterExecution() else {
            return
        }
        self.executionClaim = executionClaim
        appRuntimeFlags.setExecutionCancellationHandler(
            for: self,
            claim: executionClaim
        ) {
            $0.cancel()
        }
        let logWriter = beginManualLogSession()
        activeLogWriter = logWriter
        currentSessionLogURL = logWriter.fileURL
        transferMetricsActive = true
        transferTracker.clear()
        transferTracker.updateTotalBytes(84 * 1_024 * 1_024)
        let now = CFAbsoluteTimeGetCurrent()
        _ = transferTracker.record(
            Self.demoTransferState(
                bytesTransferred: 20 * 1_024 * 1_024,
                fraction: 0.25
            ),
            now: now - 2
        )
        currentTransferMetrics = transferTracker.record(
            Self.demoTransferState(
                bytesTransferred: 42 * 1_024 * 1_024,
                fraction: 0.5
            ),
            now: now
        )
        Task {
            await logWriter.appendLog(
                String(
                    localized:
                        "home.execution.log.preparingExecution"
                ),
                level: .info
            )
            await logWriter.appendLog(
                String(
                    localized:
                        "home.execution.log.resuming"
                ),
                level: .info
            )
        }
        state = .downloading(
            month: LibraryMonthKey(year: 2026, month: 7),
            itemPosition: 18,
            totalItems: 42
        )
    }
    #endif

    private func beginManualLogSession() -> ExecutionLogSessionWriter {
        let writer = makeManualLogWriter()
        _ = FileManager.default.createFile(
            atPath: writer.fileURL.path,
            contents: nil
        )
        return writer
    }

    private static func demoTransferState(
        bytesTransferred: Int64,
        fraction: Float
    ) -> BackupTransferState {
        BackupTransferState(
            kind: .download,
            workerID: 1,
            assetLocalIdentifier: "demo",
            assetDisplayName: "IMG_2048.HEIC",
            resourceDate: nil,
            assetPosition: 18,
            totalAssets: 42,
            resourceDisplayName: "IMG_2048.HEIC",
            resourcePosition: 1,
            totalResources: 1,
            resourceFraction: fraction,
            resourceBytesTransferred: bytesTransferred,
            resourceTotalBytes: 84 * 1_024 * 1_024,
            countsTowardTransferSpeed: true,
            stageDescription: ""
        )
    }

    private func resetTransferMetrics(
        for plan: MacBackupExecutionPlan
    ) {
        stopTransferMetricsRefreshLoop()
        transferMetricsActive = true
        estimatedUploadTotalBytes =
            plan.uploadAssetIDs.isEmpty ? 0 : nil
        estimatedDownloadTotalBytes =
            plan.downloadMonths.isEmpty
                && plan.complementMonths.isEmpty
                ? 0
                : nil
        transferTracker.clear()
        currentTransferMetrics = .inactive
        refreshTransferTotal()
        scheduleDownloadEstimate(for: plan)
        startTransferMetricsRefreshLoop()
    }

    private func deactivateTransferMetrics() {
        downloadEstimateScheduler.cancel()
        transferMetricsActive = false
        estimatedUploadTotalBytes = nil
        estimatedDownloadTotalBytes = nil
        transferTracker.clear()
        currentTransferMetrics = .inactive
        stopTransferMetricsRefreshLoop()
    }

    private func updateEstimatedUploadTotalBytes(
        _ totalBytes: Int64?
    ) {
        guard transferMetricsActive else { return }
        estimatedUploadTotalBytes = totalBytes
        refreshTransferTotal()
    }

    private func updateEstimatedDownloadTotalBytes(
        _ totalBytes: Int64?
    ) {
        guard transferMetricsActive else { return }
        estimatedDownloadTotalBytes = totalBytes ?? 0
        refreshTransferTotal()
    }

    private func refreshTransferTotal() {
        guard transferMetricsActive else { return }
        transferTracker.updateTotalBytes(
            HomeExecutionTransferTracker.resolvedTotalBytes(
                uploadBytes: estimatedUploadTotalBytes,
                downloadBytes: estimatedDownloadTotalBytes
            )
        )
        refreshTransferMetrics()
    }

    private func updateTransferMetrics(
        _ transferState: BackupTransferState
    ) {
        guard transferMetricsActive else { return }
        currentTransferMetrics = transferTracker.record(
            transferState,
            now: CFAbsoluteTimeGetCurrent()
        )
    }

    private func refreshTransferMetrics() {
        guard transferMetricsActive else { return }
        currentTransferMetrics = transferTracker.snapshot(
            now: CFAbsoluteTimeGetCurrent()
        )
    }

    private func startTransferMetricsRefreshLoop() {
        transferMetricsRefreshTask = Task {
            [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(
                        nanoseconds: 1_000_000_000
                    )
                } catch {
                    return
                }
                guard let self, !Task.isCancelled else { return }
                self.refreshTransferMetrics()
            }
        }
    }

    private func stopTransferMetricsRefreshLoop() {
        transferMetricsRefreshTask?.cancel()
        transferMetricsRefreshTask = nil
    }

    private func scheduleDownloadEstimate(
        for plan: MacBackupExecutionPlan
    ) {
        guard !plan.downloadMonths.isEmpty
                || !plan.complementMonths.isEmpty else {
            return
        }
        downloadEstimateScheduler.schedule(
            operation: { [weak self] in
                guard let self else { return nil }
                return await estimatedDownloadBytes(for: plan)
            },
            onValue: { [weak self] value in
                self?.updateEstimatedDownloadTotalBytes(value)
            }
        )
    }

    private func estimatedDownloadBytes(
        for plan: MacBackupExecutionPlan
    ) async -> Int64? {
        let months = plan.downloadMonths.union(
            plan.complementMonths
        )
        guard !months.isEmpty else { return nil }
        let inputs = months.compactMap { month -> (
            RemoteLibraryMonthDelta,
            Set<String>
        )? in
            guard let delta = backupCoordinator
                .remoteMonthRawData(for: month) else {
                return nil
            }
            return (
                delta,
                (
                    plan.localAssetIDsByMonth[month] ?? []
                ).union(
                    restoredAssetIDsByMonth[month] ?? []
                )
            )
        }
        guard !inputs.isEmpty else { return nil }
        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        let incompletePolicy = plan.incompleteDownloadPolicy
        return await withCancellableDetachedValue(priority: .utility) {
            () -> Int64? in
            guard !Task.isCancelled else { return nil }
            let localIDs = inputs.reduce(into: Set<String>()) {
                $0.formUnion($1.1)
            }
            guard let records = try? repository
                .fetchAssetFingerprintRecords(
                    assetIDs: localIDs
                ) else {
                return nil
            }
            guard !Task.isCancelled else { return nil }
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: Set(records.keys)
            )
            guard !Task.isCancelled else { return nil }
            let freshRecords =
                MacDownloadLocalFingerprintPolicy.freshRecords(
                    snapshots: assets.map(snapshot),
                    records: records
                )
            var totalBytes: Int64 = 0
            for input in inputs {
                guard !Task.isCancelled else { return nil }
                let localFingerprints = Set(
                    input.1.compactMap {
                        freshRecords[$0]?.fingerprint
                    }
                )
                let remoteItems = HomeAlbumMatching
                    .buildRemoteItems(
                        assets: input.0.assets,
                        resources: input.0.resources,
                        links: input.0.assetResourceLinks
                    )
                    .filter {
                        !localFingerprints.contains(
                            $0.assetFingerprint
                        )
                    }
                let monthBytes = DownloadWorkflowHelper
                    .estimatedDownloadBytes(
                        for: remoteItems,
                        incompletePolicy: incompletePolicy
                    ) ?? 0
                let (nextTotal, overflowed) =
                    totalBytes.addingReportingOverflow(monthBytes)
                guard !overflowed else { return nil }
                totalBytes = nextTotal
            }
            return totalBytes > 0 ? totalBytes : nil
        }
    }

    private var currentRunningStage: MacBackupExecutionStage? {
        switch state {
        case .preflighting:
            return .preflight
        case .uploading:
            return .upload
        case .downloading:
            return .download
        case .idle, .pausing, .paused, .stopping,
             .resuming, .completed, .failed, .cancelled:
            return nil
        }
    }

    private func remainingPlan(
        from plan: MacBackupExecutionPlan
    ) -> MacBackupExecutionPlan {
        MacBackupResumePlanPolicy.remainingPlan(
            from: plan,
            completedUploadMonths: completedUploadMonths,
            completedDownloadMonths: completedDownloadMonths
        )
    }

    private func interruptionDisposition(
        for plan: MacBackupExecutionPlan
    ) -> MacBackupInterruptionDisposition {
        MacBackupResumePlanPolicy.interruptionDisposition(
            for: plan,
            completedUploadMonths: completedUploadMonths,
            completedDownloadMonths: completedDownloadMonths
        )
    }

    private func endSession(
        logWriter: ExecutionLogSessionWriter
    ) async {
        deactivateTransferMetrics()
        await logWriter.finalize()
        activeContext = nil
        activeLogWriter = nil
        terminationIntent = .none
    }

    private func pauseForRecoverableInterruption(
        stage: MacBackupExecutionStage,
        logWriter: ExecutionLogSessionWriter
    ) async {
        deactivateTransferMetrics()
        terminationIntent = .none
        pauseRequestedStage = stage
        await logWriter.appendLog(
            String(
                localized:
                    "home.execution.log.executionPaused"
            ),
            level: .warning
        )
        updateMonthExecution {
            $0.pause(stage)
        }
        state = .paused(stage)
    }

    private func releaseExecutionLease() {
        guard let executionClaim else { return }
        self.executionClaim = nil
        appRuntimeFlags.exitExecution(executionClaim)
    }

    private func updateMonthExecution(
        _ body: (inout MacMonthExecutionTracker) -> Void
    ) {
        let previousPhases = monthExecutionTracker.phases
        let previousProgress = monthExecutionTracker.progress
        body(&monthExecutionTracker)
        guard monthExecutionTracker.phases != previousPhases
                || monthExecutionTracker.progress != previousProgress else {
            return
        }
        onMonthExecutionChange?()
    }

    private func clearMonthExecution() {
        updateMonthExecution {
            $0.clear()
        }
    }

    private static let emptyPreparedUpload = MacBackupPreparedUpload(
        index: LocalHashIndexBuildResult(
            requestedAssetIDs: [],
            readyAssetIDs: [],
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: [],
            networkPendingAssetIDs: []
        ),
        requiresSingleWorker: false
    )

    private func runUploadIfNeeded(
        assetIDs: Set<String>,
        plan: MacBackupExecutionPlan,
        preflight: MacBackupPreparedUpload,
        profile: ServerProfileRecord,
        credential: String,
        settings: MacBackupExecutionSettingsSnapshot,
        eventStream: BackupEventStream,
        logWriter: ExecutionLogSessionWriter
    ) async throws -> BackupExecutionResult? {
        guard !assetIDs.isEmpty else { return nil }
        let context = DownloadWorkflowHelper.Context(
            profile: profile,
            password: credential
        )
        let finalizer = makeComplementFinalizer(
            plan: plan,
            context: context,
            logWriter: logWriter
        )
        let request = BackupRunRequest(
            profile: profile,
            password: credential,
            onlyAssetLocalIdentifiers: assetIDs,
            workerCountOverride:
                settings.uploadWorkerCountOverride(
                    requiresSingleWorker:
                        preflight.requiresSingleWorker
                ),
            iCloudPhotoBackupMode: settings.iCloudMode,
            monthGroupingTimeZone: plan.monthGroupingTimeZone,
            onMonthUploaded: finalizer
        )
        state = .uploading(nil)
        let result = try await backupCoordinator.runBackup(
            request: request,
            eventStream: eventStream
        )
        return result
    }

    private func makeComplementFinalizer(
        plan: MacBackupExecutionPlan,
        context: DownloadWorkflowHelper.Context,
        logWriter: ExecutionLogSessionWriter
    ) -> BackupMonthFinalizer? {
        guard !plan.complementMonths.isEmpty else { return nil }
        return { [weak self] month, uploadContext in
            guard let self else { return .cancelled }
            guard plan.complementMonths.contains(month) else {
                return .success
            }
            let result = await self.downloadMonth(
                month,
                localAssetIDs:
                    plan.localAssetIDsByMonth[month] ?? [],
                context: context,
                uploadContext: uploadContext,
                incompletePolicy:
                    plan.incompleteDownloadPolicy,
                logWriter: logWriter
            )
            return self.applyInlineDownloadResult(
                result,
                month: month
            )
        }
    }

    private func downloadMonths(
        _ months: [LibraryMonthKey],
        localAssetIDsByMonth: [LibraryMonthKey: Set<String>],
        profile: ServerProfileRecord,
        credential: String,
        incompletePolicy: IncompleteDownloadPolicy,
        logWriter: ExecutionLogSessionWriter
    ) async throws -> Int {
        guard !months.isEmpty else { return 0 }
        let context = DownloadWorkflowHelper.Context(
            profile: profile,
            password: credential
        )
        let batchResult: Result<Int, Error>
        do {
            batchResult = try await backupCoordinator
                .withMacDownloadVerificationPlan(
                    profile: profile,
                    password: credential
                ) { [weak self] verificationPlan in
                    guard let self else {
                        return .failure(CancellationError())
                    }
                    do {
                        return .success(
                            try await self.downloadMonths(
                                months,
                                localAssetIDsByMonth:
                                    localAssetIDsByMonth,
                                context: context,
                                incompletePolicy:
                                    incompletePolicy,
                                logWriter: logWriter,
                                verifyMonth: { month in
                                    let verificationTask =
                                        Task.detached {
                                            try await verificationPlan
                                                .verify(month: month)
                                        }
                                    try await
                                        withTaskCancellationHandler {
                                            try await verificationTask
                                                .value
                                        } onCancel: {
                                            verificationTask.cancel()
                                        }
                                }
                            )
                        )
                    } catch {
                        return .failure(error)
                    }
                }
        } catch {
            if RemoteFaultLite.classify(error) == .cancelled {
                throw error
            }
            return try await downloadMonths(
                months,
                localAssetIDsByMonth: localAssetIDsByMonth,
                context: context,
                incompletePolicy: incompletePolicy,
                logWriter: logWriter
            )
        }
        return try batchResult.get()
    }

    private func downloadMonths(
        _ months: [LibraryMonthKey],
        localAssetIDsByMonth: [LibraryMonthKey: Set<String>],
        context: DownloadWorkflowHelper.Context,
        incompletePolicy: IncompleteDownloadPolicy,
        logWriter: ExecutionLogSessionWriter,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async throws -> Int {
        var failedMonths = 0
        for month in months {
            try Task.checkCancellation()
            let result = await downloadMonth(
                month,
                localAssetIDs: (
                    localAssetIDsByMonth[month] ?? []
                ).union(
                    restoredAssetIDsByMonth[month] ?? []
                ),
                context: context,
                incompletePolicy: incompletePolicy,
                logWriter: logWriter,
                verifyMonth: verifyMonth
            )
            switch result {
            case .success(
                let restoredCount,
                let skippedIncompleteCount
            ):
                self.skippedIncompleteCount +=
                    skippedIncompleteCount
                completedDownloadMonths.insert(month)
                updateMonthExecution {
                    $0.complete(
                        month,
                        hasIssues: skippedIncompleteCount > 0
                    )
                }
                if restoredCount > 0 || skippedIncompleteCount > 0 {
                    await logWriter.appendLog(
                        String(
                            format: String(
                                localized:
                                    "home.execution.log.downloadDone"
                            ),
                            String(
                                localized:
                                    "home.execution.phaseDownload"
                            ),
                            month.displayText
                        ),
                        level: .info
                    )
                }
                if skippedIncompleteCount > 0 {
                    await logWriter.appendLog(
                        String.localizedStringWithFormat(
                            String(
                                localized:
                                    "restore.log.skippedIncomplete"
                            ),
                            month.displayText,
                            Int64(skippedIncompleteCount)
                        ),
                        level: .warning
                    )
                }
            case .failed(let message):
                failedMonths += 1
                updateMonthExecution {
                    $0.fail(month)
                }
                await logWriter.appendLog(
                    String(
                        format: String(
                            localized:
                                "home.execution.log.downloadFailed"
                        ),
                        String(
                            localized:
                                "home.execution.phaseDownload"
                        ),
                        month.displayText,
                        message
                    ),
                    level: .error
                )
            case .fatal(_, let error):
                updateMonthExecution {
                    $0.fail(month)
                }
                throw error
            case .cancelled:
                throw CancellationError()
            }
        }
        return failedMonths
    }

    private func downloadMonth(
        _ month: LibraryMonthKey,
        localAssetIDs: Set<String>,
        context: DownloadWorkflowHelper.Context,
        uploadContext: BackupMonthUploadContext? = nil,
        incompletePolicy: IncompleteDownloadPolicy,
        logWriter: ExecutionLogSessionWriter,
        verifyMonth: ((LibraryMonthKey) async throws -> Void)? = nil
    ) async -> DownloadMonthResult {
        activeDownloadCount += 1
        defer { activeDownloadCount -= 1 }
        updateMonthExecution {
            $0.beginDownload(month)
        }
        state = .downloading(
            month: month,
            itemPosition: 0,
            totalItems: 0
        )
        do {
            if let verifyMonth {
                try await verifyMonth(month)
            } else {
                try await backupCoordinator.verifyMonth(
                    profile: context.profile,
                    password: context.password,
                    month: month,
                    reusing: uploadContext
                )
            }
        } catch {
            if RemoteFaultLite.classify(error) == .cancelled {
                return .cancelled
            }
            let message = context.profile.userFacingStorageErrorMessage(error)
            await logWriter.appendLog(
                String.localizedStringWithFormat(
                    String(localized: "manifest.log.reconcileFailed"),
                    month.displayText,
                    message
                ),
                level: .warning
            )
            if let liteError = error as? LiteRepoError,
               liteError.isUploadFailFast {
                return .fatal(message, liteError)
            }
            if !DownloadVerifyFailurePolicy.canUseCachedSnapshot(
                after: error
            ) {
                return .failed(message)
            }
        }
        guard !Task.isCancelled else { return .cancelled }

        guard let delta = backupCoordinator.remoteMonthRawData(
            for: month
        ) else {
            return .success(restoredCount: 0, skippedIncompleteCount: 0)
        }
        let allRemoteItems = HomeAlbumMatching.buildRemoteItems(
            assets: delta.assets,
            resources: delta.resources,
            links: delta.assetResourceLinks
        )
        let localFingerprints: Set<Data>
        do {
            let currentLocalAssetIDs = localAssetIDs.union(
                restoredAssetIDsByMonth[month] ?? []
            )
            let records = try hashIndexRepository
                .fetchAssetFingerprintRecords(
                    assetIDs: currentLocalAssetIDs
                )
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: Set(records.keys)
            )
            localFingerprints =
                MacDownloadLocalFingerprintPolicy.freshFingerprints(
                    snapshots: assets.map(snapshot),
                    records: records
                )
        } catch {
            return .failed(
                UserFacingErrorLocalizer.message(
                    for: error,
                    profile: context.profile
                )
            )
        }
        let remoteItems = allRemoteItems
            .filter {
                !localFingerprints.contains($0.assetFingerprint)
            }
            .sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate < $1.creationDate
                }
                return $0.id < $1.id
            }
        guard !remoteItems.isEmpty else {
            return .success(restoredCount: 0, skippedIncompleteCount: 0)
        }

        await logWriter.appendLog(
            String.localizedStringWithFormat(
                String(
                    localized:
                        "home.execution.log.pendingDownload"
                ),
                month.displayText,
                Int64(remoteItems.count)
            ),
            level: .info
        )
        state = .downloading(
            month: month,
            itemPosition: 0,
            totalItems: remoteItems.count
        )
        return await downloadWorkflowHelper.downloadItems(
            remoteItems,
            context: context,
            incompletePolicy: incompletePolicy,
            onTransferState: { [weak self] transfer in
                guard let self else { return }
                self.updateTransferMetrics(transfer)
                self.updateMonthExecution {
                    $0.recordDownloadTransfer(
                        transfer,
                        month: month
                    )
                }
                self.state = .downloading(
                    month: month,
                    itemPosition: transfer.assetPosition,
                    totalItems: transfer.totalAssets
                )
            },
            onItemRestored: { [weak self] localID in
                guard let self else { return }
                let inserted = self.restoredAssetIDsByMonth[
                    month,
                    default: []
                ].insert(localID).inserted
                if inserted {
                    self.restoredCount += 1
                }
            }
        )
    }

    private func applyInlineDownloadResult(
        _ result: DownloadMonthResult,
        month: LibraryMonthKey
    ) -> BackupMonthFinalizationResult {
        // The inline finalizer starts only after the uploaded manifest is durable.
        completedUploadMonths.insert(month)
        switch result {
        case .success(_, let skippedIncompleteCount):
            self.skippedIncompleteCount += skippedIncompleteCount
            completedComplementMonths.insert(month)
            completedDownloadMonths.insert(month)
            updateMonthExecution {
                $0.complete(
                    month,
                    hasIssues: skippedIncompleteCount > 0
                )
            }
            return .success
        case .failed(let message):
            updateMonthExecution {
                $0.fail(month)
            }
            return .failed(message)
        case .fatal(let message, let error):
            updateMonthExecution {
                $0.fail(month)
            }
            return .fatal(message, error)
        case .cancelled:
            return .cancelled
        }
    }

    private func prepareLocalIndex(
        assetIDs: Set<String>,
        uploadAssetIDs: Set<String>,
        requiresCompleteIndex: Bool,
        iCloudMode: ICloudPhotoBackupMode,
        logWriter: ExecutionLogSessionWriter,
        generation expectedGeneration: UInt64
    ) async throws -> MacBackupPreparedUpload {
        guard !assetIDs.isEmpty else {
            throw MacBackupExecutionError.noAssets
        }
        state = .preflighting(
            processed: 0,
            total: assetIDs.count,
            message: String(
                localized: "home.execution.log.indexStatus",
                defaultValue: "Building local index"
            )
        )
        await logWriter.appendLog(
            String.localizedStringWithFormat(
                String(
                    localized:
                        "home.execution.log.startIndex"
                ),
                Int64(assetIDs.count)
            ),
            level: .info
        )

        let progressHandler: LocalHashIndexProgressHandler = {
            [weak self] message, _ in
            Task { @MainActor [weak self] in
                guard let self,
                      self.generation == expectedGeneration,
                      case .preflighting(let processed, let total, _) = self.state else {
                    return
                }
                self.state = .preflighting(
                    processed: processed,
                    total: total,
                    message: message
                )
            }
        }
        let tickHandler: LocalHashIndexProgressTickHandler = {
            [weak self] processed, total in
            Task { @MainActor [weak self] in
                guard let self,
                      self.generation == expectedGeneration,
                      case .preflighting(_, _, let message) = self.state else {
                    return
                }
                self.state = .preflighting(
                    processed: processed,
                    total: total,
                    message: message
                )
            }
        }

        let initial = try await localHashIndexBuildService.buildIndex(
            for: assetIDs,
            workerCount: Self.localWorkerCount,
            allowNetworkAccess: false,
            progressHandler: progressHandler,
            tickHandler: tickHandler
        )
        await logWriter.appendLog(
            String.localizedStringWithFormat(
                String(
                    localized:
                        "home.execution.log.indexComplete"
                ),
                Int64(initial.readyAssetIDs.count),
                Int64(initial.unavailableAssetIDs.count),
                Int64(initial.failedAssetIDs.count)
            ),
            level: initial.incompleteAssetIDs.isEmpty ? .info : .warning
        )
        if !initial.readyAssetIDs.isEmpty {
            onLocalLibraryChanged?()
        }
        try Task.checkCancellation()
        let requiresSingleWorker = LocalHashIndexPreflightPolicy
            .requiresSingleUploadWorker(
                initialResult: initial,
                uploadAssetIDs: uploadAssetIDs,
                iCloudMode: iCloudMode
            )

        var result = initial
        if requiresCompleteIndex,
           iCloudMode == .enable,
           !initial.unavailableAssetIDs.isEmpty {
            var pending = initial.unavailableAssetIDs
            for attempt in 0 ..< Self.maxICloudAttempts where !pending.isEmpty {
                if attempt > 0 {
                    try await Task.sleep(
                        nanoseconds: Self.iCloudRetryDelayNanos
                    )
                }
                let recovery = try await localHashIndexBuildService.buildIndex(
                    for: pending,
                    workerCount: Self.iCloudWorkerCount,
                    allowNetworkAccess: true,
                    progressHandler: progressHandler,
                    tickHandler: tickHandler
                )
                await logWriter.appendLog(
                    String.localizedStringWithFormat(
                        String(
                            localized:
                                "home.execution.log.indexComplete"
                        ),
                        Int64(recovery.readyAssetIDs.count),
                        Int64(recovery.unavailableAssetIDs.count),
                        Int64(recovery.failedAssetIDs.count)
                    ),
                    level: recovery.incompleteAssetIDs.isEmpty
                        ? .info
                        : .warning
                )
                if !recovery.readyAssetIDs.isEmpty {
                    onLocalLibraryChanged?()
                }
                result = LocalHashIndexPreflightPolicy.merging(
                    result,
                    recovery: recovery
                )
                pending = recovery.unavailableAssetIDs
                try Task.checkCancellation()
            }
        }

        if requiresCompleteIndex,
           !result.incompleteAssetIDs.isEmpty {
            throw MacBackupExecutionError.localIndexIncomplete(
                unavailable: result.unavailableAssetIDs.count,
                failed: result.failedAssetIDs.count,
                iCloudMode: iCloudMode
            )
        }
        return MacBackupPreparedUpload(
            index: result,
            requiresSingleWorker: requiresSingleWorker
        )
    }

    private func apply(event: BackupEvent) {
        updateMonthExecution {
            $0.apply(event)
        }
        switch event {
        case .progress(let progress):
            if activeDownloadCount == 0 {
                state = .uploading(progress)
            }
        case .started:
            if activeDownloadCount == 0 {
                state = .uploading(nil)
            }
        case .transferState(let transferState):
            updateTransferMetrics(transferState)
        case .monthChanged(let change):
            let month = LibraryMonthKey(
                year: change.year,
                month: change.month
            )
            switch change.action {
            case .started:
                break
            case .completed:
                completedUploadMonths.insert(month)
                if activeContext?.plan.complementMonths
                    .contains(month) == true {
                    completedDownloadMonths.insert(month)
                    completedComplementMonths.insert(month)
                }
            case .uploadFailed:
                break
            }
        case .log, .writeBoundaryReached, .finished:
            break
        }
    }
}
