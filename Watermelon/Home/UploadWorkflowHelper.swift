import Foundation

/// Pure executor for upload operations.
/// Wraps BackupSessionController's observer-based API into a structured async interface.
/// Does NOT know about Home's data cache (syncRemoteData/refreshLocalIndex).
/// The coordinator decides when and how to refresh caches via the onProgress callback.
@MainActor
final class UploadWorkflowHelper {

    struct UploadProgress {
        let startedMonths: Set<LibraryMonthKey>
        let completedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
    }

    enum UploadResult {
        case completed(failedCountByMonth: [LibraryMonthKey: Int])
        case paused
        case stopped
        case failed(String)
        case startFailed
    }

    private let backupSessionController: BackupSessionController
    private var observerID: UUID?
    private var pendingContinuation: CheckedContinuation<UploadResult, Never>?

    init(backupSessionController: BackupSessionController) {
        self.backupSessionController = backupSessionController
    }

    // MARK: - Public Operations

    /// Runs upload via BSC for the configured scope.
    /// Scope application and start readiness are handled inside BSC.
    /// Calls onProgress on every non-terminal BSC snapshot (~120ms throttle).
    /// Returns a terminal UploadResult when BSC reaches completed/paused/stopped/failed.
    func runUpload(
        scope: BackupScopeSelection? = nil,
        onProgress: @escaping (UploadProgress) -> Void
    ) async -> UploadResult {
        let started = await backupSessionController.startBackupWhenReady(scope: scope)
        guard started else {
            return Task.isCancelled ? .paused : .startFailed
        }

        return await withCheckedContinuation { (continuation: CheckedContinuation<UploadResult, Never>) in
            pendingContinuation = continuation

            let id = backupSessionController.addObserver { [weak self] snapshot in
                self?.handleSnapshot(snapshot, onProgress: onProgress)
            }
            observerID = id
        }
    }

    /// Requests BSC to pause. Cooperative — BSC will eventually report .paused,
    /// which resolves runUpload with .paused.
    func pause() {
        backupSessionController.pauseBackup()
    }

    /// Requests BSC to stop. Cooperative — BSC will eventually report .stopped,
    /// which resolves runUpload with .stopped.
    func stop() {
        backupSessionController.stopBackup()
    }

    /// Force cleanup without waiting for BSC terminal state.
    /// Used by coordinator's exit() for immediate teardown.
    func cancel() {
        removeObserver()
        if let cont = pendingContinuation {
            pendingContinuation = nil
            cont.resume(returning: .stopped)
        }
    }

    // MARK: - Private

    private func handleSnapshot(
        _ snapshot: BackupSessionController.Snapshot,
        onProgress: (UploadProgress) -> Void
    ) {
        guard let continuation = pendingContinuation else { return }

        let progress = UploadProgress(
            startedMonths: snapshot.startedMonths,
            completedMonths: snapshot.completedMonths,
            processedCountByMonth: snapshot.processedCountByMonth
        )

        let result: UploadResult?
        switch snapshot.state {
        case .completed: result = .completed(failedCountByMonth: snapshot.failedCountByMonth)
        case .paused:    result = .paused
        case .stopped:   result = .stopped
        case .failed:    result = .failed(snapshot.statusText)
        default:         result = nil
        }

        if let result {
            onProgress(progress)
            pendingContinuation = nil
            removeObserver()
            continuation.resume(returning: result)
        } else {
            onProgress(progress)
        }
    }

    private func removeObserver() {
        if let id = observerID {
            backupSessionController.removeObserver(id)
            observerID = nil
        }
    }
}
