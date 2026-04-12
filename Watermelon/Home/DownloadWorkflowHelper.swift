import Foundation

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
    /// Start readiness is handled inside BSC instead of helper-side polling.
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
        onItemRestored: @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        guard !remoteItems.isEmpty else { return .success }
        let hashIndexRepository = dependencies.hashIndexRepository

        do {
            _ = try await dependencies.restoreService.restoreItems(
                items: remoteItems.map(\.resources),
                profile: context.profile,
                password: context.password,
                onItemCompleted: { _, _, restoredAsset in
                    if let restoredAsset {
                        try await Self.writeHashIndex(
                            for: restoredAsset,
                            remoteItems: remoteItems,
                            repository: hashIndexRepository
                        )
                        await onItemRestored(restoredAsset.asset.localIdentifier)
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

    private static func writeHashIndex(
        for result: RestoreService.IndexedRestoredAsset,
        remoteItems: [RemoteAlbumItem],
        repository: ContentHashIndexRepository
    ) async throws {
        guard result.itemIndex < remoteItems.count else { return }
        let remoteItem = remoteItems[result.itemIndex]

        try await Task.detached(priority: .utility) {
            try repository.writeHashIndex(
                assetLocalIdentifier: result.asset.localIdentifier,
                remoteAssetFingerprint: remoteItem.assetFingerprint,
                resourceLinks: remoteItem.resourceLinks,
                resources: remoteItem.resources
            )
        }.value
    }
}

enum DownloadMonthResult {
    case success
    case failed(String)
    case cancelled
}
