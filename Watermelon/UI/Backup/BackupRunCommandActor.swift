import Foundation
import Photos

enum BackupRunMode: Sendable {
    case full
    case scoped(assetIDs: Set<String>)
    case retry(assetIDs: Set<String>)

    var isRetry: Bool {
        if case .retry = self {
            return true
        }
        return false
    }

    var isScoped: Bool {
        if case .scoped = self {
            return true
        }
        return false
    }

    var targetCount: Int {
        switch self {
        case .full:
            return 0
        case .scoped(let assetIDs), .retry(let assetIDs):
            return assetIDs.count
        }
    }

    var displayModeText: String {
        switch self {
        case .full:
            return "全量"
        case .scoped:
            return "范围"
        case .retry:
            return "重试"
        }
    }

    var targetAssetIdentifiers: Set<String>? {
        switch self {
        case .full:
            return nil
        case .retry(let assetIDs):
            return assetIDs
        case .scoped(let assetIDs):
            return assetIDs
        }
    }
}

enum BackupTerminationIntent: Sendable {
    case none
    case pause
    case stop
}

enum BackupResumeOutcome: Sendable {
    case started(runToken: UInt64, pendingCount: Int)
    case noPending
    case interrupted(intent: BackupTerminationIntent)
    case busy
}

struct BackupRunFailureContext: @unchecked Sendable {
    let runToken: UInt64
    let runMode: BackupRunMode
    let profile: ServerProfileRecord
    let error: Error
    let intent: BackupTerminationIntent
}

enum BackupEngineSignal: @unchecked Sendable {
    case runEvent(
        runToken: UInt64,
        runMode: BackupRunMode,
        intent: BackupTerminationIntent,
        event: BackupEvent
    )
    case runFailed(BackupRunFailureContext)
}

actor BackupRunCommandActor {
    private let backupCoordinator: BackupCoordinatorProtocol
    private let photoLibraryService: PhotoLibraryServiceProtocol

    private var runTask: Task<Void, Never>?
    private var eventListenerTask: Task<Void, Never>?
    private var activeRunToken: UInt64 = 0
    private var terminationIntent: BackupTerminationIntent = .none
    private var queuedIntent: BackupTerminationIntent = .none
    private var isPreparingResume = false
    private var preparationIntent: BackupTerminationIntent = .none

    private var signalContinuations: [UUID: AsyncStream<BackupEngineSignal>.Continuation] = [:]

    init(
        backupCoordinator: BackupCoordinatorProtocol,
        photoLibraryService: PhotoLibraryServiceProtocol
    ) {
        self.backupCoordinator = backupCoordinator
        self.photoLibraryService = photoLibraryService
    }

    func makeSignalStream() -> AsyncStream<BackupEngineSignal> {
        AsyncStream { continuation in
            let id = UUID()
            Task { [weak self] in
                await self?.addSignalContinuation(continuation, id: id)
            }
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.removeSignalContinuation(id: id)
                }
            }
        }
    }

    func startRun(profile: ServerProfileRecord, password: String, mode: BackupRunMode) -> UInt64? {
        startRunInternal(profile: profile, password: password, mode: mode)
    }

    func resumeRun(
        profile: ServerProfileRecord,
        password: String,
        pausedMode: BackupRunMode,
        completedAssetIDs: Set<String>
    ) async throws -> BackupResumeOutcome {
        guard runTask == nil, eventListenerTask == nil, !isPreparingResume else {
            return .busy
        }

        isPreparingResume = true
        preparationIntent = .none
        defer {
            isPreparingResume = false
            preparationIntent = .none
        }

        let pendingAssetIDs: Set<String>
        switch pausedMode {
        case .retry(let assetIDs):
            try throwIfResumePreparationInterrupted()
            pendingAssetIDs = assetIDs.subtracting(completedAssetIDs)
        case .scoped(let assetIDs):
            try throwIfResumePreparationInterrupted()
            pendingAssetIDs = assetIDs.subtracting(completedAssetIDs)
        case .full:
            pendingAssetIDs = try await computePendingAssetIDsForFullRun(excluding: completedAssetIDs)
        }

        if preparationIntent != .none {
            queuedIntent = preparationIntent
            return .interrupted(intent: preparationIntent)
        }

        guard !pendingAssetIDs.isEmpty else {
            return .noPending
        }

        // Resume preparation has completed; hand over to normal run-start path.
        isPreparingResume = false

        let resumedMode: BackupRunMode = pausedMode.isRetry ? .retry(assetIDs: pendingAssetIDs) : .scoped(assetIDs: pendingAssetIDs)
        guard let runToken = startRunInternal(
            profile: profile,
            password: password,
            mode: resumedMode
        ) else {
            return .busy
        }

        return .started(runToken: runToken, pendingCount: pendingAssetIDs.count)
    }

    func requestPause() {
        applyIntent(.pause)
    }

    func requestStop() {
        applyIntent(.stop)
    }

    func cancelActive() {
        applyIntent(.stop)
    }

    private func applyIntent(_ intent: BackupTerminationIntent) {
        if runTask != nil {
            terminationIntent = intent
            runTask?.cancel()
            return
        }

        if isPreparingResume {
            preparationIntent = intent
            return
        }

        queuedIntent = intent
    }

    private func startRunInternal(
        profile: ServerProfileRecord,
        password: String,
        mode: BackupRunMode
    ) -> UInt64? {
        guard runTask == nil, eventListenerTask == nil, !isPreparingResume else {
            return nil
        }

        activeRunToken &+= 1
        let runToken = activeRunToken

        terminationIntent = queuedIntent
        queuedIntent = .none

        eventListenerTask = Task.detached(priority: .userInitiated) { [weak self] in
            guard let self else { return }
            for await event in self.backupCoordinator.eventStream {
                let shouldStop = await self.handleRunEvent(
                    event,
                    runToken: runToken,
                    runMode: mode
                )
                if shouldStop {
                    break
                }
            }
        }

        runTask = Task.detached(priority: .userInitiated) { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.backupCoordinator.runBackup(
                    profile: profile,
                    password: password,
                    onlyAssetLocalIdentifiers: mode.targetAssetIdentifiers
                )
            } catch {
                await self.handleRunError(
                    error,
                    runToken: runToken,
                    runMode: mode,
                    profile: profile
                )
            }
        }

        if terminationIntent != .none {
            runTask?.cancel()
        }

        return runToken
    }

    private func handleRunEvent(
        _ event: BackupEvent,
        runToken: UInt64,
        runMode: BackupRunMode
    ) -> Bool {
        guard runToken == activeRunToken else { return false }
        let intentSnapshot = terminationIntent
        emit(.runEvent(
            runToken: runToken,
            runMode: runMode,
            intent: intentSnapshot,
            event: event
        ))

        if event.isTerminal {
            clearActiveRunState(resetIntent: true)
            return true
        }

        return false
    }

    private func handleRunError(
        _ error: Error,
        runToken: UInt64,
        runMode: BackupRunMode,
        profile: ServerProfileRecord
    ) {
        guard runToken == activeRunToken else { return }

        let intent = terminationIntent
        clearActiveRunState(resetIntent: true)

        emit(.runFailed(BackupRunFailureContext(
            runToken: runToken,
            runMode: runMode,
            profile: profile,
            error: error,
            intent: intent
        )))
    }

    private func clearActiveRunState(resetIntent: Bool) {
        runTask = nil
        eventListenerTask?.cancel()
        eventListenerTask = nil
        if resetIntent {
            terminationIntent = .none
        }
    }

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
            try throwIfResumePreparationInterrupted()
            try Task.checkCancellation()
            let asset = assets.object(at: index)
            if !completedAssetIDs.contains(asset.localIdentifier) {
                pending.insert(asset.localIdentifier)
            }
        }

        return pending
    }

    private func throwIfResumePreparationInterrupted() throws {
        if preparationIntent != .none {
            throw CancellationError()
        }
    }

    private func emit(_ signal: BackupEngineSignal) {
        for continuation in signalContinuations.values {
            continuation.yield(signal)
        }
    }

    private func addSignalContinuation(
        _ continuation: AsyncStream<BackupEngineSignal>.Continuation,
        id: UUID
    ) {
        signalContinuations[id] = continuation
    }

    private func removeSignalContinuation(id: UUID) {
        signalContinuations[id] = nil
    }
}
