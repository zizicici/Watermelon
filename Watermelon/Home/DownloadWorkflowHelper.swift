import Foundation

protocol RestoreItemsServing: Sendable {
    func restoreItems(
        items: [RestoreService.RestoreItemDescriptor],
        profile: ServerProfileRecord,
        password: String,
        downloadPolicy: RestoreDownloadPolicy,
        shouldDrain: @escaping @Sendable () -> Bool,
        onTransferState: (@Sendable (BackupTransferState) async -> Void)?,
        onItemFailed: (@Sendable (RestoreItemFailure) async -> Void)?,
        onItemCompleted: @escaping @Sendable (Int, Int, RestoreService.RestoredItem?) async throws -> Void
    ) async throws -> [RestoreService.RestoredItem]
}

extension RestoreService: RestoreItemsServing {}

@MainActor
final class DownloadWorkflowHelper {
    struct Context: Sendable {
        let profile: ServerProfileRecord
        let password: String
        let downloadPolicy = RestoreDownloadPolicy()
    }

    private let restoreService: any RestoreItemsServing

    init(dependencies: DependencyContainer) {
        restoreService = dependencies.restoreService
    }

    init(
        restoreService: any RestoreItemsServing
    ) {
        self.restoreService = restoreService
    }

    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        incompletePolicy: IncompleteDownloadPolicy,
        shouldDrain: @escaping @Sendable () -> Bool = { false },
        onTransferState: @MainActor @escaping (BackupTransferState) -> Void,
        onItemFailed: (@Sendable (RestoreItemFailure) async -> Void)? = nil,
        onItemRestored: @MainActor @escaping (RestoreService.RestoredItem) async -> Void
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

        do {
            let descriptors = toRestore.map { item in
                RestoreService.RestoreItemDescriptor(
                    instances: item.instances,
                    identity: item.assetFingerprint,
                    creationDate: item.creationDate
                )
            }
            let restored = try await restoreService.restoreItems(
                items: descriptors,
                profile: context.profile,
                password: context.password,
                downloadPolicy: context.downloadPolicy,
                shouldDrain: shouldDrain,
                onTransferState: { state in
                    await onTransferState(state)
                },
                onItemFailed: onItemFailed,
                onItemCompleted: { _, _, restoredItem in
                    if let restoredItem {
                        await onItemRestored(restoredItem)
                    }
                }
            )
            if Task.isCancelled { return .cancelled }
            return .success(restoredCount: restored.count, skippedIncompleteCount: skippedIncompleteCount)
        } catch {
            if Task.isCancelled || RemoteFaultLite.classify(error) == .cancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(toRestore.count), reason=\(message)")
            print("[DownloadWorkflowHelper] error detail: \(RestoreService.diagnosticDescription(of: error))")
            return .failed(message)
        }
    }

    static func estimatedDownloadBytes(for remoteItems: [RemoteAlbumItem], incompletePolicy: IncompleteDownloadPolicy = .skip) -> Int64? {
        let toRestore = incompletePolicy == .createNewAsset ? remoteItems : remoteItems.filter { !$0.isIncomplete }
        var totalBytes: Int64 = 0
        for item in toRestore {
            totalBytes = RestoreDownloadPolicy.adding(totalBytes, RestoreDownloadPolicy.estimatedBytes(for: item.instances))
        }
        return totalBytes > 0 ? totalBytes : nil
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
