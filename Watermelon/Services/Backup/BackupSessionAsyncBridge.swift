import Foundation

/// Async bridge over BackupSessionController.
/// Exposes structured upload/scoped-backup operations while keeping BSC as the single owner
/// of start/pause/stop readiness rules and session snapshots.
@MainActor
final class BackupSessionAsyncBridge {

    struct UploadProgress {
        let newlyStartedMonths: Set<LibraryMonthKey>
        let newlyUploadCompletedMonths: Set<LibraryMonthKey>
        let processedCountByMonth: [LibraryMonthKey: Int]
    }

    enum UploadResult {
        case completed(
            failedCountByMonth: [LibraryMonthKey: Int],
            incompleteSummaryByMonth: [LibraryMonthKey: BackupMonthIncompleteSummary],
            uploadSnapshotDeferredMessageByMonth: [LibraryMonthKey: String]
        )
        case paused
        case stopped
        case failed(
            String,
            failedCountByMonth: [LibraryMonthKey: Int],
            incompleteSummaryByMonth: [LibraryMonthKey: BackupMonthIncompleteSummary],
            uploadSnapshotDeferredMessageByMonth: [LibraryMonthKey: String]
        )
        case startFailed
    }

    private let backupSessionController: BackupSessionController
    private var observerID: UUID?
    private var pendingUploadContinuation: CheckedContinuation<UploadResult, Never>?
    private var pendingScopedContinuation: CheckedContinuation<Bool, Never>?
    private var reportedStartedMonths = Set<LibraryMonthKey>()
    private var reportedUploadCompletedMonths = Set<LibraryMonthKey>()

    init(backupSessionController: BackupSessionController) {
        self.backupSessionController = backupSessionController
    }

    func runUpload(
        scope: BackupScopeSelection? = nil,
        runConfigurationOverride: BackupRunConfigurationOverride? = nil,
        onMonthUploaded: BackupMonthFinalizer? = nil,
        onProgress: @escaping (UploadProgress) -> Void
    ) async -> UploadResult {
        resetUploadReporting()
        removeObserver()

        let started = await backupSessionController.startBackupWhenReady(
            scope: scope,
            runConfigurationOverride: runConfigurationOverride,
            onMonthUploaded: onMonthUploaded
        )
        guard started else {
            return Task.isCancelled ? .paused : .startFailed
        }

        return await withCheckedContinuation { (continuation: CheckedContinuation<UploadResult, Never>) in
            pendingUploadContinuation = continuation

            let id = backupSessionController.addObserver { [weak self] snapshot in
                self?.handleUploadSnapshot(snapshot, onProgress: onProgress)
            }
            observerID = id
        }
    }

    func requestPause() {
        backupSessionController.pauseBackup()
    }

    func requestStop() {
        backupSessionController.stopBackup()
    }

    func markAssetIDsPendingForResume(_ assetIDs: Set<String>) {
        backupSessionController.markAssetIDsPendingForResume(assetIDs)
    }

    func runScopedBackup(
        assetIDs: Set<String>,
        onProgress: @escaping () -> Void
    ) async -> Bool {
        let selection = BackupScopeSelection(
            selectedAssetIDs: assetIDs,
            selectedAssetCount: assetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: assetIDs.count,
            totalEstimatedBytes: nil
        )

        removeObserver()

        let started = await backupSessionController.startBackupWhenReady(scope: selection)
        guard started else { return false }

        return await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            pendingScopedContinuation = continuation

            let id = backupSessionController.addObserver { [weak self] snapshot in
                self?.handleScopedSnapshot(snapshot, onProgress: onProgress)
            }
            observerID = id
        }
    }

    func cancel() {
        let snapshot = backupSessionController.snapshot()
        if snapshot.state == .running || snapshot.controlPhase != .idle {
            backupSessionController.stopBackup()
        }

        removeObserver()

        if let continuation = pendingUploadContinuation {
            pendingUploadContinuation = nil
            continuation.resume(returning: .stopped)
        }

        if let continuation = pendingScopedContinuation {
            pendingScopedContinuation = nil
            continuation.resume(returning: false)
        }

        resetUploadReporting()
    }

    private func handleUploadSnapshot(
        _ snapshot: BackupSessionController.Snapshot,
        onProgress: (UploadProgress) -> Void
    ) {
        guard let continuation = pendingUploadContinuation else { return }

        let newlyStartedMonths = snapshot.startedMonths.subtracting(reportedStartedMonths)
        let newlyUploadCompletedMonths = snapshot.uploadCompletedMonths.subtracting(reportedUploadCompletedMonths)
        reportedStartedMonths.formUnion(snapshot.startedMonths)
        reportedUploadCompletedMonths.formUnion(snapshot.uploadCompletedMonths)

        let progress = UploadProgress(
            newlyStartedMonths: newlyStartedMonths,
            newlyUploadCompletedMonths: newlyUploadCompletedMonths,
            processedCountByMonth: snapshot.processedCountByMonth
        )

        let result: UploadResult?
        switch snapshot.state {
        case .completed:
            result = .completed(
                failedCountByMonth: snapshot.failedCountByMonth,
                incompleteSummaryByMonth: snapshot.incompleteSummaryByMonth,
                uploadSnapshotDeferredMessageByMonth: snapshot.uploadSnapshotDeferredMessageByMonth
            )
        case .paused:    result = .paused
        case .stopped:   result = .stopped
        case .failed:    result = .failed(
                snapshot.statusText,
                failedCountByMonth: snapshot.failedCountByMonth,
                incompleteSummaryByMonth: snapshot.incompleteSummaryByMonth,
                uploadSnapshotDeferredMessageByMonth: snapshot.uploadSnapshotDeferredMessageByMonth
            )
        default:         result = nil
        }

        onProgress(progress)

        guard let result else { return }
        pendingUploadContinuation = nil
        removeObserver()
        continuation.resume(returning: result)
    }

    private func handleScopedSnapshot(
        _ snapshot: BackupSessionController.Snapshot,
        onProgress: () -> Void
    ) {
        guard let continuation = pendingScopedContinuation else { return }

        onProgress()

        let resolved: Bool?
        switch snapshot.state {
        case .completed:        resolved = true
        case .failed, .stopped: resolved = false
        default:                resolved = nil
        }

        guard let resolved else { return }
        pendingScopedContinuation = nil
        removeObserver()
        continuation.resume(returning: resolved)
    }

    private func removeObserver() {
        if let id = observerID {
            backupSessionController.removeObserver(id)
            observerID = nil
        }
    }

    private func resetUploadReporting() {
        reportedStartedMonths.removeAll()
        reportedUploadCompletedMonths.removeAll()
    }
}
