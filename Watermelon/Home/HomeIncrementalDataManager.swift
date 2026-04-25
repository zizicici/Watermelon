import Foundation
@preconcurrency import Photos

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    private let processingWorker: HomeDataProcessingWorker
    private let fileSizeCoordinator: HomeFileSizeScanCoordinator
    private let currentScope: @MainActor () -> HomeLocalLibraryScope

    private var isObservingPhotoLibrary = false
    private var processingMutationCount = 0
    private var deferredPhotoChanges: [PHChange] = []
    private var isDrainingDeferredPhotoChanges = false

    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?
    var onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        remoteMonthSnapshot: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?,
        currentScope: @escaping @MainActor () -> HomeLocalLibraryScope
    ) {
        let worker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository,
            remoteMonthSnapshot: remoteMonthSnapshot
        )
        self.processingWorker = worker
        self.fileSizeCoordinator = HomeFileSizeScanCoordinator(
            hooks: HomeFileSizeScanCoordinator.Hooks(
                localMonthsForScan: { worker.localMonthsForFileSizeScan() },
                updateFileSize: { month, cache in
                    await worker.updateFileSize(for: month, sizeCache: cache)
                }
            ),
            contentHashIndexRepository: contentHashIndexRepository
        )
        self.currentScope = currentScope
        super.init()
        // Closure-fan-out: keep `onFileSizesUpdated` as a manager property so
        // HomeScreenStore's `bind()` does not need to know about the coordinator.
        // Setting after super.init lets the closure capture `self` weakly.
        fileSizeCoordinator.onFileSizesUpdated = { [weak self] months in
            self?.onFileSizesUpdated?(months)
        }
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
        let scope = currentScope()
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
        processingWorker.localAssetIDs(for: month, expectedScope: currentScope())
    }

    func remoteOnlyItems(for month: LibraryMonthKey) async -> [RemoteAlbumItem] {
        await processingWorker.remoteOnlyItems(for: month, expectedScope: currentScope())
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
            scope: currentScope()
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
        let scope = currentScope()
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
