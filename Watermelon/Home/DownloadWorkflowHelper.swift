import Foundation

/// Pure domain executor for download operations.
/// Knows only how to restore remote items and write hash index entries.
/// Does NOT know about Home's data cache (syncRemoteData/refreshLocalIndex) or BSC upload/scoped-backup control.
/// The coordinator decides when and how to sequence scoped backup, remote sync and local refresh.
@MainActor
final class DownloadWorkflowHelper {

    struct Context {
        let profile: ServerProfileRecord
        let password: String
    }

    private let dependencies: DependencyContainer

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
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
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(remoteItems.count), reason=\(error.localizedDescription)")
            return .failed(error.localizedDescription)
        }
    }

    func cancel() {}

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
