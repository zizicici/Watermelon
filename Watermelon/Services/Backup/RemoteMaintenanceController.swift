import Foundation

/// Terminal outcome of a leftover scan/delete, delivered to the driving modal so it can render the next
/// state without racing the `.RemoteMaintenanceDidChange` notification. `failed` carries the localized reason.
enum LeftoverScanOutcome {
    case completed(LeftoverScanResult)
    case cancelled
    case failed(String)
}

enum LeftoverDeleteOutcome {
    case completed(LeftoverDeleteResult)
    case cancelled
    case failed(String)
}

/// Failures land in `lastError`, not a `.failed` phase, so retries aren't blocked
/// by the `case .idle` guard in the start methods. Verify, leftover scan, and leftover
/// delete are mutually exclusive — only one runs at a time.
@MainActor
final class RemoteMaintenanceController {
    enum Phase {
        case idle
        case verifying(profileID: Int64, progress: RemoteSyncProgress)
        case scanningLeftover(profileID: Int64, progress: RemoteSyncProgress)
        case deletingLeftover(profileID: Int64, progress: RemoteSyncProgress)
    }

    struct LastError {
        let profileID: Int64
        let message: String
    }

    private(set) var phase: Phase = .idle
    private(set) var lastError: LastError?

    var isVerifying: Bool {
        if case .verifying = phase { return true }
        return false
    }

    /// Any maintenance op holds the profile; the detail page disables profile mutations while busy.
    var isBusy: Bool {
        if case .idle = phase { return false }
        return true
    }

    func isVerifying(profileID: Int64) -> Bool {
        if case .verifying(let pid, _) = phase { return pid == profileID }
        return false
    }

    /// True while any maintenance op (verify / leftover scan / leftover delete) holds this specific profile.
    func isBusy(profileID: Int64) -> Bool {
        switch phase {
        case .idle:
            return false
        case .verifying(let pid, _), .scanningLeftover(let pid, _), .deletingLeftover(let pid, _):
            return pid == profileID
        }
    }

    var currentProgress: RemoteSyncProgress? {
        switch phase {
        case .idle:
            return nil
        case .verifying(_, let progress),
             .scanningLeftover(_, let progress),
             .deletingLeftover(_, let progress):
            return progress
        }
    }

    private let backupCoordinator: BackupCoordinator
    private let appRuntimeFlags: AppRuntimeFlags
    private let databaseManager: DatabaseManager

    private var runningTask: Task<Void, Never>?
    private var lastNotifyAt: CFAbsoluteTime = 0
    private var pendingNotifyTask: Task<Void, Never>?

    private static let throttleInterval: CFAbsoluteTime = 0.5

    /// `nonisolated` so the container can construct it from any context (foreground
    /// scene start, background-task launch handler) without `MainActor.assumeIsolated`.
    /// All mutating methods stay MainActor-bound — only init is open.
    nonisolated init(
        backupCoordinator: BackupCoordinator,
        appRuntimeFlags: AppRuntimeFlags,
        databaseManager: DatabaseManager
    ) {
        self.backupCoordinator = backupCoordinator
        self.appRuntimeFlags = appRuntimeFlags
        self.databaseManager = databaseManager
    }

    @discardableResult
    func startFullVerify(profile: ServerProfileRecord, password: String) -> Bool {
        guard case .idle = phase else { return false }
        guard !appRuntimeFlags.isExecuting else { return false }
        guard let profileID = profile.id else { return false }

        lastError = nil
        phase = .verifying(profileID: profileID, progress: RemoteSyncProgress(current: 0, total: 0))
        postNow()

        runningTask = Task { [weak self] in
            guard let self else { return }
            do {
                try await self.backupCoordinator.verifyAllMonths(
                    profile: profile,
                    password: password
                ) { [weak self] progress in
                    self?.handleProgress(profileID: profileID, progress: progress)
                }
                self.handleVerifySuccess(profileID: profileID)
            } catch {
                // The Lite layer wraps a cancelled lock-acquire/classify as LiteRepoError.lockFault/probeFault(.cancelled),
                // which is not a CancellationError — classify covers both so a user cancel never surfaces as lastError.
                if RemoteFaultLite.classify(error) == .cancelled {
                    self.handleCancellation()
                } else {
                    self.handleFailure(
                        profileID: profileID,
                        message: UserFacingErrorLocalizer.message(for: error, profile: profile)
                    )
                }
            }
        }
        return true
    }

    @discardableResult
    func startScanLeftover(
        profile: ServerProfileRecord,
        password: String,
        onComplete: @escaping @MainActor (LeftoverScanOutcome) -> Void
    ) -> Bool {
        guard case .idle = phase else { return false }
        guard !appRuntimeFlags.isExecuting else { return false }
        guard let profileID = profile.id else { return false }

        lastError = nil
        phase = .scanningLeftover(profileID: profileID, progress: RemoteSyncProgress(current: 0, total: 0))
        postNow()

        runningTask = Task { [weak self] in
            guard let self else { return }
            do {
                let result = try await self.backupCoordinator.scanLeftoverFiles(
                    profile: profile,
                    password: password
                ) { [weak self] progress in
                    self?.handleProgress(profileID: profileID, progress: progress)
                }
                self.resetToIdle()
                onComplete(.completed(result))
            } catch {
                if RemoteFaultLite.classify(error) == .cancelled {
                    self.handleCancellation()
                    onComplete(.cancelled)
                } else {
                    let message = UserFacingErrorLocalizer.message(for: error, profile: profile)
                    self.handleFailure(profileID: profileID, message: message)
                    onComplete(.failed(message))
                }
            }
        }
        return true
    }

    @discardableResult
    func startDeleteLeftover(
        profile: ServerProfileRecord,
        password: String,
        targets: [LeftoverFile],
        onComplete: @escaping @MainActor (LeftoverDeleteOutcome) -> Void
    ) -> Bool {
        guard case .idle = phase else { return false }
        guard !appRuntimeFlags.isExecuting else { return false }
        guard let profileID = profile.id else { return false }
        guard !targets.isEmpty else { return false }

        lastError = nil
        phase = .deletingLeftover(profileID: profileID, progress: RemoteSyncProgress(current: 0, total: targets.count))
        postNow()

        runningTask = Task { [weak self] in
            guard let self else { return }
            do {
                let result = try await self.backupCoordinator.deleteLeftoverFiles(
                    profile: profile,
                    password: password,
                    targets: targets
                ) { [weak self] progress in
                    self?.handleProgress(profileID: profileID, progress: progress)
                }
                self.resetToIdle()
                onComplete(.completed(result))
            } catch {
                if RemoteFaultLite.classify(error) == .cancelled {
                    self.handleCancellation()
                    onComplete(.cancelled)
                } else {
                    let message = UserFacingErrorLocalizer.message(for: error, profile: profile)
                    self.handleFailure(profileID: profileID, message: message)
                    onComplete(.failed(message))
                }
            }
        }
        return true
    }

    func cancel() {
        // State cleanup happens in `handleCancellation` once the task observes
        // the cancellation; clearing here would race with the in-flight task.
        runningTask?.cancel()
    }

    func dismissLastError() {
        guard lastError != nil else { return }
        lastError = nil
        postNow()
    }

    private func handleProgress(profileID: Int64, progress: RemoteSyncProgress) {
        switch phase {
        case .verifying(let pid, _) where pid == profileID:
            phase = .verifying(profileID: profileID, progress: progress)
        case .scanningLeftover(let pid, _) where pid == profileID:
            phase = .scanningLeftover(profileID: profileID, progress: progress)
        case .deletingLeftover(let pid, _) where pid == profileID:
            phase = .deletingLeftover(profileID: profileID, progress: progress)
        default:
            return
        }
        postThrottled()
    }

    private func handleVerifySuccess(profileID: Int64) {
        try? databaseManager.setRemoteVerifiedAt(Date(), profileID: profileID)
        resetToIdle()
    }

    private func handleCancellation() {
        resetToIdle()
    }

    private func resetToIdle() {
        runningTask = nil
        phase = .idle
        postNow()
    }

    private func handleFailure(profileID: Int64, message: String) {
        runningTask = nil
        lastError = LastError(profileID: profileID, message: message)
        phase = .idle
        postNow()
    }

    private func postNow() {
        pendingNotifyTask?.cancel()
        pendingNotifyTask = nil
        lastNotifyAt = CFAbsoluteTimeGetCurrent()
        NotificationCenter.default.post(
            name: .RemoteMaintenanceDidChange,
            object: self
        )
    }

    private func postThrottled() {
        let now = CFAbsoluteTimeGetCurrent()
        let elapsed = now - lastNotifyAt
        if elapsed >= Self.throttleInterval {
            postNow()
            return
        }
        if pendingNotifyTask != nil { return }
        let remaining = Self.throttleInterval - elapsed
        pendingNotifyTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
            guard !Task.isCancelled, let self else { return }
            self.pendingNotifyTask = nil
            self.postNow()
        }
    }
}
