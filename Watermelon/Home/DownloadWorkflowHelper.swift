import Foundation
import os.log

private let downloadLog = Logger(subsystem: "com.zizicici.watermelon", category: "DownloadWorkflow")

/// Encapsulates the download/sync workflow for a single month:
/// scoped backup (to build hash index) → sync remote data → download remote-only items.
///
/// Owns no execution state — the coordinator drives it and updates monthPlans/phase
/// based on the results.
@MainActor
final class DownloadWorkflowHelper {

    struct Context {
        let profile: ServerProfileRecord
        let password: String
    }

    struct Callbacks {
        let localAssetIDs: (LibraryMonthKey) -> Set<String>
        let remoteOnlyItems: (LibraryMonthKey) -> [RemoteAlbumItem]
        let syncRemoteData: () -> Void
        let refreshLocalIndex: (Set<String>) -> Void
        let onProgress: () -> Void
    }

    private let dependencies: DependencyContainer
    private let backupSessionController: BackupSessionController
    private let callbacks: Callbacks
    private var backupObserverID: UUID?
    private var pendingScopedContinuation: CheckedContinuation<Bool, Never>?

    init(
        dependencies: DependencyContainer,
        backupSessionController: BackupSessionController,
        callbacks: Callbacks
    ) {
        self.dependencies = dependencies
        self.backupSessionController = backupSessionController
        self.callbacks = callbacks
    }

    // MARK: - Public

    /// Runs the full download workflow for a single month:
    /// 1. Scoped backup to populate local hash index
    /// 2. Sync remote data + refresh local index
    /// 3. Download remote-only items via RestoreService
    ///
    /// Returns `true` if the month was fully processed, `false` on failure or cancellation.
    func downloadMonth(
        _ month: LibraryMonthKey,
        context: Context,
        phaseLabel: String
    ) async -> DownloadMonthResult {
        let assetIDs = callbacks.localAssetIDs(month)
        if !assetIDs.isEmpty {
            let uploadCompleted = await runScopedBackup(assetIDs: assetIDs)
            if Task.isCancelled { return .cancelled }
            if !uploadCompleted {
                return .failed("\(month.displayText): 备份索引失败")
            }
        }

        callbacks.syncRemoteData()
        if !assetIDs.isEmpty {
            callbacks.refreshLocalIndex(assetIDs)
        }

        return await downloadRemoteItems(month: month, context: context, phaseLabel: phaseLabel)
    }

    func cancel() {
        if let id = backupObserverID {
            backupSessionController.removeObserver(id)
            backupObserverID = nil
        }
        if let continuation = pendingScopedContinuation {
            pendingScopedContinuation = nil
            continuation.resume(returning: false)
        }
    }

    // MARK: - Internal

    private func runScopedBackup(assetIDs: Set<String>) async -> Bool {
        await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            pendingScopedContinuation = continuation

            let selection = BackupScopeSelection(
                selectedAssetIDs: assetIDs,
                selectedAssetCount: assetIDs.count,
                selectedEstimatedBytes: nil,
                totalAssetCount: assetIDs.count,
                totalEstimatedBytes: nil
            )
            backupSessionController.updateScopeSelection(selection)

            if let id = backupObserverID {
                backupSessionController.removeObserver(id)
                backupObserverID = nil
            }

            backupSessionController.startBackup()

            let observerID = backupSessionController.addObserver { [weak self] snapshot in
                guard let self, let cont = self.pendingScopedContinuation else { return }
                self.callbacks.onProgress()

                func resolve(_ value: Bool) {
                    self.pendingScopedContinuation = nil
                    self.backupSessionController.removeObserver(observerID)
                    self.backupObserverID = nil
                    cont.resume(returning: value)
                }

                switch snapshot.state {
                case .completed:      resolve(true)
                case .failed, .stopped: resolve(false)
                default:              break
                }
            }
            backupObserverID = observerID
        }
    }

    private func downloadRemoteItems(
        month: LibraryMonthKey,
        context: Context,
        phaseLabel: String
    ) async -> DownloadMonthResult {
        let remoteItems = callbacks.remoteOnlyItems(month)
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
                        self.callbacks.refreshLocalIndex([restoredAsset.asset.localIdentifier])
                    }
                    self.callbacks.onProgress()
                }
            )
            return Task.isCancelled ? .cancelled : .success
        } catch {
            if Task.isCancelled { return .cancelled }
            return .failed("\(month.displayText): \(error.localizedDescription)")
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
