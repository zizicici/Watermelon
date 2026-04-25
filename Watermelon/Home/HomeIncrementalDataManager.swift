import Foundation
@preconcurrency import Photos

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    struct Hooks {
        let remoteMonthSnapshot: @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
        let currentScope: @MainActor () -> HomeLocalLibraryScope
    }

    private let processingWorker: HomeDataProcessingWorker
    let fileSizeCoordinator: HomeFileSizeScanCoordinator
    private let hooks: Hooks

    private var isObservingPhotoLibrary = false
    // Reentrancy guard for `photoLibraryDidChange`. While the manager has an in-flight
    // worker mutation (a refresh/sync/apply), incoming PHChanges queue into
    // `deferredPhotoChanges` instead of racing into the worker. Not a mutex — actor
    // reentrancy across `await` is what makes this necessary.
    private var processingMutationCount = 0
    private var deferredPhotoChanges: [PHChange] = []
    private var isDrainingDeferredPhotoChanges = false

    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        hooks: Hooks
    ) {
        let worker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository,
            remoteMonthSnapshot: hooks.remoteMonthSnapshot
        )
        self.processingWorker = worker
        self.fileSizeCoordinator = HomeFileSizeScanCoordinator(
            worker: worker,
            contentHashIndexRepository: contentHashIndexRepository
        )
        self.hooks = hooks
        super.init()
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingWorker.remoteSnapshotRevisionForQuery(hasActiveConnection: hasActiveConnection)
    }

    @discardableResult
    func ensureLocalIndexLoaded() async -> Bool {
        await loadLocalIndex(forceReload: false)
    }

    @discardableResult
    func reloadLocalIndex() async -> Bool {
        await loadLocalIndex(forceReload: true)
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) async -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        let scope = hooks.currentScope()
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.refreshLocalIndex(
            forAssetIDs: assetIDs,
            expectedScope: scope
        )
        finishProcessingMutation()
        return reconciledMonths
    }

    @discardableResult
    func syncRemoteSnapshotOnProcessingQueue(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> Set<LibraryMonthKey> {
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.syncRemoteSnapshot(
            state: state,
            hasActiveConnection: hasActiveConnection
        )
        finishProcessingMutation()
        return reconciledMonths
    }

    func monthRow(for month: LibraryMonthKey) -> HomeMonthRow {
        processingWorker.monthRow(for: month)
    }

    func allMonthRows() -> [LibraryMonthKey: HomeMonthRow] {
        processingWorker.allMonthRows()
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        processingWorker.localAssetIDs(for: month, expectedScope: hooks.currentScope())
    }

    func remoteOnlyItems(for month: LibraryMonthKey) async -> [RemoteAlbumItem] {
        await processingWorker.remoteOnlyItems(for: month, expectedScope: hooks.currentScope())
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        processingWorker.matchedCount(for: month)
    }

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            guard let self else { return }

            if self.processingMutationCount > 0 || self.isDrainingDeferredPhotoChanges {
                self.deferredPhotoChanges.append(changeInstance)
                return
            }

            await self.applyPhotoLibraryChangeNow(changeInstance)
        }
    }

    private func beginProcessingMutation() {
        processingMutationCount += 1
    }

    private func finishProcessingMutation() {
        if processingMutationCount > 0 {
            processingMutationCount -= 1
        }

        scheduleDeferredPhotoChangeDrainIfNeeded()
    }

    @discardableResult
    private func loadLocalIndex(forceReload: Bool) async -> Bool {
        let result = await processingWorker.loadLocalIndex(
            forceReload: forceReload,
            scope: hooks.currentScope()
        )
        if result.isAuthorized {
            registerPhotoLibraryObserverIfNeeded()
            if result.didReload {
                fileSizeCoordinator.startFullScan()
            }
        } else {
            unregisterPhotoLibraryObserverIfNeeded()
            fileSizeCoordinator.reset()
        }
        return !result.changedMonths.isEmpty
    }

    private func applyPhotoLibraryChangeNow(_ changeInstance: PHChange) async {
        let scope = hooks.currentScope()
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.applyPhotoLibraryChange(
            changeInstance,
            scope: scope
        )
        finishProcessingMutation()
        if !reconciledMonths.isEmpty {
            fileSizeCoordinator.enqueueRescan(for: reconciledMonths)
            onMonthsChanged?(reconciledMonths)
        }
    }

    private func registerPhotoLibraryObserverIfNeeded() {
        guard !isObservingPhotoLibrary else { return }
        PHPhotoLibrary.shared().register(self)
        isObservingPhotoLibrary = true
    }

    private func unregisterPhotoLibraryObserverIfNeeded() {
        guard isObservingPhotoLibrary else { return }
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
        isObservingPhotoLibrary = false
    }

    private func scheduleDeferredPhotoChangeDrainIfNeeded() {
        guard processingMutationCount == 0,
              !deferredPhotoChanges.isEmpty,
              !isDrainingDeferredPhotoChanges else { return }
        Task { @MainActor [weak self] in
            await self?.drainDeferredPhotoChangesIfNeeded()
        }
    }

    private func drainDeferredPhotoChangesIfNeeded() async {
        guard processingMutationCount == 0,
              !deferredPhotoChanges.isEmpty,
              !isDrainingDeferredPhotoChanges else { return }

        isDrainingDeferredPhotoChanges = true
        defer {
            isDrainingDeferredPhotoChanges = false
            scheduleDeferredPhotoChangeDrainIfNeeded()
        }

        while processingMutationCount == 0, !deferredPhotoChanges.isEmpty {
            let deferred = deferredPhotoChanges.removeFirst()
            await applyPhotoLibraryChangeNow(deferred)
        }
    }
}
