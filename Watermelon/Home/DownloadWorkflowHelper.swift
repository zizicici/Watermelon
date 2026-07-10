import Foundation

/// Pure domain executor for download operations.
/// Knows only how to restore remote items and write hash index entries.
/// Does NOT know about Home's data cache (syncRemoteData/refreshLocalIndex) or BSC upload/scoped-backup control.
/// The coordinator decides when and how to sequence scoped backup, remote sync and local refresh.
@MainActor
final class DownloadWorkflowHelper {

    struct Context: Sendable {
        let profile: ServerProfileRecord
        let password: String
    }

    private let dependencies: DependencyContainer

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
    }

    /// Downloads remote-only items via RestoreService and writes hash index per item.
    /// `isIncomplete` items are filtered out (would 404) and reported via `skippedIncompleteCount`.
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        incompletePolicy: IncompleteDownloadPolicy,
        onTransferState: @MainActor @escaping (BackupTransferState) -> Void,
        onItemRestored: @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        // Incomplete records can only import their resolvable subset — a new, differently-fingerprinted asset.
        // `.createNewAsset` downloads them anyway (informed consent given upfront); `.skip` leaves them.
        let toRestore: [RemoteAlbumItem]
        let skippedIncompleteCount: Int
        switch incompletePolicy {
        case .createNewAsset:
            toRestore = remoteItems
            skippedIncompleteCount = 0
        case .skip:
            toRestore = remoteItems.filter { !$0.isIncomplete }
            skippedIncompleteCount = remoteItems.count - toRestore.count
        }

        guard !toRestore.isEmpty else {
            return .success(restoredCount: 0, skippedIncompleteCount: skippedIncompleteCount)
        }

        let hashIndexRepository = dependencies.hashIndexRepository

        do {
            let descriptors = toRestore.map { item in
                RestoreService.RestoreItemDescriptor(
                    instances: item.instances,
                    identity: item.assetFingerprint
                )
            }
            let restored = try await dependencies.restoreService.restoreItems(
                items: descriptors,
                profile: context.profile,
                password: context.password,
                onTransferState: { state in
                    await onTransferState(state)
                },
                onItemCompleted: { _, _, restoredItem in
                    if let restoredItem {
                        try await Self.writeHashIndex(
                            assetLocalIdentifier: restoredItem.asset.localIdentifier,
                            remoteAssetFingerprint: restoredItem.identity,
                            instances: restoredItem.asset.importedInstances,
                            repository: hashIndexRepository
                        )
                        await onItemRestored(restoredItem.asset.localIdentifier)
                    }
                }
            )
            if Task.isCancelled { return .cancelled }
            return .success(
                restoredCount: restored.count,
                skippedIncompleteCount: skippedIncompleteCount
            )
        } catch {
            if Task.isCancelled || RemoteFaultLite.classify(error) == .cancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(toRestore.count), reason=\(message)")
            return .failed(message)
        }
    }

    static func estimatedDownloadBytes(for remoteItems: [RemoteAlbumItem], incompletePolicy: IncompleteDownloadPolicy = .skip) -> Int64? {
        let toRestore = incompletePolicy == .createNewAsset ? remoteItems : remoteItems.filter { !$0.isIncomplete }
        var totalBytes: Int64 = 0
        for item in toRestore {
            var seenFileNames = Set<String>()
            for instance in item.instances where seenFileNames.insert(instance.fileName).inserted {
                totalBytes += max(instance.storedFileSize ?? instance.fileSize, 0)
            }
        }
        return totalBytes > 0 ? totalBytes : nil
    }

    func cancel() {}

    private static func writeHashIndex(
        assetLocalIdentifier: String,
        remoteAssetFingerprint: Data,
        instances: [RemoteAssetResourceInstance],
        repository: ContentHashIndexRepository
    ) async throws {
        try await Task.detached(priority: .utility) {
            try repository.writeHashIndex(
                assetLocalIdentifier: assetLocalIdentifier,
                remoteAssetFingerprint: remoteAssetFingerprint,
                instances: instances
            )
        }.value
    }
}

enum DownloadMonthResult {
    case success(restoredCount: Int, skippedIncompleteCount: Int)
    case failed(String)
    case fatal(String, LiteRepoError)
    case cancelled
}

// How a download treats incomplete remote records (only their resolvable subset is importable → a new asset).
enum IncompleteDownloadPolicy: Sendable {
    case createNewAsset   // download them too, importing the subset as a new (differently-fingerprinted) asset
    case skip             // leave them undownloaded (reported via skippedIncompleteCount)
}
