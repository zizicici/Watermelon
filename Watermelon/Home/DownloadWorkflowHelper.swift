import Foundation

/// Pure domain executor for download operations.
/// Restores remote items and delegates per-item durability verification to RestoredAssetFingerprintVerifier.
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

    /// Downloads remote-only items via RestoreService and verifies durable fingerprint binding per item.
    /// Filters on `isRestorable` so post-save verification can compare full fingerprints.
    func downloadItems(
        _ remoteItems: [RemoteAlbumItem],
        context: Context,
        onItemRestored: @MainActor @escaping (PhotoKitLocalIdentifier) async -> Void
    ) async -> DownloadMonthResult {
        let toRestore = remoteItems.filter(\.isRestorable)
        let fingerprintMismatchCount = remoteItems.filter(\.isFingerprintMismatch).count
        let skippedIncompleteCount = remoteItems.count - toRestore.count - fingerprintMismatchCount

        guard !toRestore.isEmpty else {
            return .success(DownloadMonthOutcome(
                restoredCount: 0,
                issues: DownloadIssueSummary(
                    skippedIncompleteCount: skippedIncompleteCount,
                    fingerprintMismatchCount: fingerprintMismatchCount,
                    localFingerprintVerificationIncompleteCount: 0
                )
            ))
        }

        let verifier = dependencies.restoredAssetFingerprintVerifier
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
                        // RestoredAsset.localIdentifier is the newly-created PhotoKit asset id (S3 raw String).
                        // Wrap once at the restore boundary so the rest of the helper carries a typed asset id.
                        let assetID = PhotoKitLocalIdentifier(rawValue: restoredItem.asset.localIdentifier)
                        // Throw on failure so RestoreService's retry loop runs; if every retry
                        // still fails the caller logs and we surface a partial-failure count
                        // upstream — restoring the asset without a durable fingerprint would
                        // re-download it as a "duplicate" on the next session.
                        do {
                            let verified = try await verifier.verifyDurableBinding(
                                assetLocalIdentifier: assetID,
                                expectedFingerprint: restoredItem.identity
                            )
                            if !verified {
                                throw NSError(
                                    domain: "DownloadWorkflowHelper",
                                    code: -1,
                                    userInfo: [NSLocalizedDescriptionKey:
                                        "local fingerprint not durable for assetID=\(assetID)"]
                                )
                            }
                            // A later retry succeeding must clear the record left by an earlier attempt;
                            // otherwise a transient first-attempt failure is reported as a permanent miss.
                            await unverifiedTracker.clear(assetID)
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {
                            await unverifiedTracker.record(assetID)
                            print("[DownloadWorkflowHelper] restored asset hash verification failed: assetID=\(assetID), reason=\(error.localizedDescription)")
                            throw error
                        }
                        await onItemRestored(assetID)
                    }
                }
            )
            if Task.isCancelled { return .cancelled }
            return .success(DownloadMonthOutcome(
                restoredCount: restored.count,
                issues: DownloadIssueSummary(
                    skippedIncompleteCount: skippedIncompleteCount,
                    fingerprintMismatchCount: fingerprintMismatchCount,
                    localFingerprintVerificationIncompleteCount: await unverifiedTracker.count
                )
            ))
        } catch {
            if Task.isCancelled { return .cancelled }
            let message = context.profile.userFacingStorageErrorMessage(error)
            print("[DownloadWorkflowHelper] download FAILED: itemCount=\(toRestore.count), reason=\(message)")
            return .failed(DownloadMonthFailure(message: message, underlyingError: error))
        }
    }

    private actor UnverifiedFingerprintTracker {
        private var ids: Set<PhotoKitLocalIdentifier> = []
        func record(_ id: PhotoKitLocalIdentifier) { ids.insert(id) }
        func clear(_ id: PhotoKitLocalIdentifier) { ids.remove(id) }
        var count: Int { ids.count }
    }

    func cancel() {}
}

enum DownloadMonthResult: Sendable {
    case success(DownloadMonthOutcome)
    case failed(DownloadMonthFailure)
    case cancelled
}

struct DownloadMonthFailure: @unchecked Sendable {
    let message: String
    let underlyingError: Error?
}
