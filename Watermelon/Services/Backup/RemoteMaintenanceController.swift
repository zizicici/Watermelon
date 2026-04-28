import Foundation

/// Failures land in `lastError`, not a `.failed` phase, so retries aren't blocked
/// by the `case .idle` guard in `startFullVerify`.
@MainActor
final class RemoteMaintenanceController {
    enum Phase {
        case idle
        case verifying(profileID: Int64, progress: RemoteSyncProgress)
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

    func isVerifying(profileID: Int64) -> Bool {
        if case .verifying(let pid, _) = phase { return pid == profileID }
        return false
    }

    var currentProgress: RemoteSyncProgress? {
        if case .verifying(_, let progress) = phase { return progress }
        return nil
    }

    private let backupCoordinator: BackupCoordinator
    private let appRuntimeFlags: AppRuntimeFlags
    private let databaseManager: DatabaseManager

    private var verifyTask: Task<Void, Never>?
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

        verifyTask = Task { [weak self] in
            guard let self else { return }
            do {
                try await self.backupCoordinator.verifyAllMonths(
                    profile: profile,
                    password: password
                ) { [weak self] progress in
                    self?.handleProgress(profileID: profileID, progress: progress)
                }
                self.handleSuccess(profileID: profileID)
            } catch is CancellationError {
                self.handleCancellation()
            } catch {
                self.handleFailure(profileID: profileID, profile: profile, error: error)
            }
        }
        return true
    }

    func cancel() {
        // State cleanup happens in `handleCancellation` once the task observes
        // the cancellation; clearing here would race with the in-flight task.
        verifyTask?.cancel()
    }

    func dismissLastError() {
        guard lastError != nil else { return }
        lastError = nil
        postNow()
    }

    private func handleProgress(profileID: Int64, progress: RemoteSyncProgress) {
        guard case .verifying(let pid, _) = phase, pid == profileID else { return }
        phase = .verifying(profileID: profileID, progress: progress)
        postThrottled()
    }

    private func handleSuccess(profileID: Int64) {
        try? databaseManager.setRemoteVerifiedAt(Date(), profileID: profileID)
        verifyTask = nil
        phase = .idle
        postNow()
    }

    private func handleCancellation() {
        verifyTask = nil
        phase = .idle
        postNow()
    }

    private func handleFailure(profileID: Int64, profile: ServerProfileRecord, error: Error) {
        verifyTask = nil
        lastError = LastError(
            profileID: profileID,
            message: UserFacingErrorLocalizer.message(for: error, profile: profile)
        )
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
