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
            return .success(restoredCount: 0, skippedIncompleteCount: skippedIncompleteCount, unverifiedFingerprintCount: 0)
        }

        let hashIndexBuildService = dependencies.localHashIndexBuildService
        let hashIndexRepository = dependencies.hashIndexRepository
        let unverifiedTracker = UnverifiedFingerprintTracker()

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
                        // Throw on failure so RestoreService's retry loop runs; if every retry
                        // still fails the caller logs and we surface a partial-failure count
                        // upstream — restoring the asset without a durable fingerprint would
                        // re-download it as a "duplicate" on the next session.
                        do {
                            let verified = try await Self.rebuildVerifiedLocalHashIndex(
                                assetLocalIdentifier: restoredItem.asset.localIdentifier,
                                remoteAssetFingerprint: restoredItem.identity,
                                buildService: hashIndexBuildService,
                                repository: hashIndexRepository
                            )
                            if !verified {
                                throw NSError(
                                    domain: "DownloadWorkflowHelper",
                                    code: -1,
                                    userInfo: [NSLocalizedDescriptionKey:
                                        "local fingerprint not durable for assetID=\(restoredItem.asset.localIdentifier)"]
                                )
                            }
                            // A later retry succeeding must clear the record left by an earlier attempt;
                            // otherwise a transient first-attempt failure is reported as a permanent miss.
                            await unverifiedTracker.clear(restoredItem.asset.localIdentifier)
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {
                            await unverifiedTracker.record(restoredItem.asset.localIdentifier)
                            print("[DownloadWorkflowHelper] restored asset hash verification failed: assetID=\(restoredItem.asset.localIdentifier), reason=\(error.localizedDescription)")
                            throw error
                        }
                        await onItemRestored(restoredItem.asset.localIdentifier)
                    }
                }
            )
            if Task.isCancelled { return .cancelled }
            return .success(
                restoredCount: restored.count,
                skippedIncompleteCount: skippedIncompleteCount,
                unverifiedFingerprintCount: await unverifiedTracker.count
            )
        } catch {
            if Task.isCancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(toRestore.count), reason=\(message)")
            return .failed(DownloadMonthFailure(message: message, underlyingError: error))
        }
    }

    private actor UnverifiedFingerprintTracker {
        private var ids: Set<String> = []
        func record(_ id: String) { ids.insert(id) }
        func clear(_ id: String) { ids.remove(id) }
        var count: Int { ids.count }
    }

    func cancel() {}

    private nonisolated static func rebuildVerifiedLocalHashIndex(
        assetLocalIdentifier: String,
        remoteAssetFingerprint: Data,
        buildService: LocalHashIndexBuildService,
        repository: ContentHashIndexRepository
    ) async throws -> Bool {
        let delays: [Duration] = [
            .milliseconds(500),
            .milliseconds(750),
            .milliseconds(1_000),
            .milliseconds(1_500),
            .milliseconds(2_000),
            .milliseconds(2_500),
            .milliseconds(3_000)
        ]
        for attempt in 0...delays.count {
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
                return records[assetLocalIdentifier]?.fingerprint == remoteAssetFingerprint
            }
            if attempt < delays.count {
                try await Task.sleep(for: delays[attempt])
            }
        }
        return false
    }
}

enum DownloadMonthResult {
    case success(restoredCount: Int, skippedIncompleteCount: Int, unverifiedFingerprintCount: Int)
    case failed(DownloadMonthFailure)
    case cancelled
}

struct DownloadMonthFailure {
    let message: String
    let underlyingError: Error?
}
