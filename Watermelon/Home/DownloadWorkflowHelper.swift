import Foundation
import os.log

private let downloadLog = Logger(subsystem: "com.zizicici.watermelon", category: "DownloadHelper")

/// Pure domain executor for download operations.
/// Knows only how to run scoped backups and download remote items.
/// Does NOT know about Home's data cache (syncRemoteData/refreshLocalIndex).
/// The coordinator decides when and how to refresh caches between steps.
@MainActor
final class DownloadWorkflowHelper {

    struct Context {
        let profile: ServerProfileRecord
        let password: String
    }

    private let dependencies: DependencyContainer
    private let backupSessionController: BackupSessionController
    private var backupObserverID: UUID?
    private var pendingScopedContinuation: CheckedContinuation<Bool, Never>?

    init(dependencies: DependencyContainer, backupSessionController: BackupSessionController) {
        self.dependencies = dependencies
        self.backupSessionController = backupSessionController
    }

    // MARK: - Public Operations

    /// Runs a scoped backup to populate the local hash index for the given assets.
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

        // Wait for BSC to accept scope + start. During rapid pause→resume,
        // BSC may still be in a stop transition. Poll both updateScopeSelection
        // and startBackup to avoid using stale scope from a previous run.
        // startBackup must succeed before addObserver (addObserver immediately
        // replays the current snapshot, which may be a stale terminal state).
        var ready = false
        for _ in 0..<20 {
            if backupSessionController.updateScopeSelection(selection),
               backupSessionController.startBackup() {
                ready = true
                break
            }
            try? await Task.sleep(nanoseconds: 100_000_000)
            if Task.isCancelled { return false }
        }
        guard ready else {
            downloadLog.warning("[DownloadHelper] BSC did not become ready after 20 attempts")
            return false
        }

        return await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            pendingScopedContinuation = continuation

            let observerID = backupSessionController.addObserver { [weak self] snapshot in
                guard let self, let cont = self.pendingScopedContinuation else { return }
                onProgress()

                let resolved: Bool?
                switch snapshot.state {
                case .completed:        resolved = true
                case .failed, .stopped: resolved = false
                default:                resolved = nil
                }

                if let resolved {
                    self.pendingScopedContinuation = nil
                    self.removeObserver()
                    cont.resume(returning: resolved)
                }
            }
            backupObserverID = observerID
        }
    }

    /// Downloads remote-only items via RestoreService and writes hash index per item.
    /// Calls `onItemRestored` with the asset local identifier after each successful restore.
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        onItemRestored: @MainActor @escaping (String) -> Void
    ) async -> DownloadMonthResult {
        guard !remoteItems.isEmpty else { return .success }

        do {
            _ = try await dependencies.restoreService.restoreItems(
                items: remoteItems.map(\.resources),
                profile: context.profile,
                password: context.password,
                onItemCompleted: { [weak self] _, _, restoredAsset in
                    guard let self else { return }
                    if let restoredAsset {
                        self.writeHashIndex(for: restoredAsset, remoteItems: remoteItems)
                        onItemRestored(restoredAsset.asset.localIdentifier)
                    }
                }
            )
            return Task.isCancelled ? .cancelled : .success
        } catch {
            if Task.isCancelled { return .cancelled }
            return .failed(error.localizedDescription)
        }
    }

    func cancel() {
        backupSessionController.stopBackup()
        removeObserver()
        if let continuation = pendingScopedContinuation {
            pendingScopedContinuation = nil
            continuation.resume(returning: false)
        }
    }

    // MARK: - Private

    private func removeObserver() {
        if let id = backupObserverID {
            backupSessionController.removeObserver(id)
            backupObserverID = nil
        }
    }

    private func writeHashIndex(for result: RestoreService.IndexedRestoredAsset, remoteItems: [RemoteAlbumItem]) {
        guard result.itemIndex < remoteItems.count else { return }
        let remoteItem = remoteItems[result.itemIndex]
        dependencies.hashIndexRepository.writeHashIndex(
            assetLocalIdentifier: result.asset.localIdentifier,
            remoteAssetFingerprint: remoteItem.assetFingerprint,
            resourceLinks: remoteItem.resourceLinks,
            resources: remoteItem.resources
        )
    }
}

enum DownloadMonthResult {
    case success
    case failed(String)
    case cancelled
}
