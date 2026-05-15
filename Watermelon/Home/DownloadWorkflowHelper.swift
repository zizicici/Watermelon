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
    /// Filters on `isRestorable` so post-save verification can compare full fingerprints.
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        onItemRestored: @MainActor @escaping (String) async -> Void
    ) async -> DownloadMonthResult {
        let toRestore = remoteItems.filter(\.isRestorable)
        let skippedIncompleteCount = remoteItems.count - toRestore.count

        guard !toRestore.isEmpty else {
            return .success(restoredCount: 0, skippedIncompleteCount: skippedIncompleteCount)
        }

        let hashIndexBuildService = dependencies.localHashIndexBuildService
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
                onItemCompleted: { _, _, restoredItem in
                    if let restoredItem {
                        try await Self.rebuildVerifiedLocalHashIndex(
                            assetLocalIdentifier: restoredItem.asset.localIdentifier,
                            remoteAssetFingerprint: restoredItem.identity,
                            buildService: hashIndexBuildService,
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
            if Task.isCancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(toRestore.count), reason=\(message)")
            return .failed(DownloadMonthFailure(message: message, underlyingError: error))
        }
    }

    func cancel() {}

    private nonisolated static func rebuildVerifiedLocalHashIndex(
        assetLocalIdentifier: String,
        remoteAssetFingerprint: Data,
        buildService: LocalHashIndexBuildService,
        repository: ContentHashIndexRepository
    ) async throws {
        for attempt in 0..<3 {
            try Task.checkCancellation()
            let result = try await buildService.buildIndex(
                for: [assetLocalIdentifier],
                workerCount: 1,
                allowNetworkAccess: false
            )
            if result.readyAssetIDs.contains(assetLocalIdentifier) {
                let records = try await Task.detached(priority: .utility) {
                    try repository.fetchAssetFingerprintRecords(assetIDs: [assetLocalIdentifier])
                }.value
                guard records[assetLocalIdentifier]?.fingerprint == remoteAssetFingerprint else {
                    throw NSError(domain: "DownloadWorkflowHelper", code: -2, userInfo: [
                        NSLocalizedDescriptionKey: "restored asset bytes do not match the remote asset fingerprint"
                    ])
                }
                return
            }
            if attempt + 1 < 3 {
                try await Task.sleep(for: .milliseconds(500))
            }
        }
        throw NSError(domain: "DownloadWorkflowHelper", code: -1, userInfo: [
            NSLocalizedDescriptionKey: "restored asset local hash verification did not complete"
        ])
    }
}

enum DownloadMonthResult {
    case success(restoredCount: Int, skippedIncompleteCount: Int)
    case failed(DownloadMonthFailure)
    case cancelled
}

struct DownloadMonthFailure {
    let message: String
    let underlyingError: Error?
}
