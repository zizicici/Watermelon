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
    nonisolated let browserLocalSeedChanges = ChangeSignal<Void>()
    private let hooks: Hooks

    private var isObservingPhotoLibrary = false
    private var cachedMonthGroupingTimeZone: MonthGroupingTimeZonePreference = .frozenCurrent()

    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?
    var onFingerprintValidationNeeded: ((Set<String>) -> Void)?

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

    func monthGroupingTimeZoneForLocalIndex() -> MonthGroupingTimeZonePreference {
        cachedMonthGroupingTimeZone
    }

    func browserLocalSeed(expectedScope: HomeLocalLibraryScope) async -> HomeBrowserLocalSeed? {
        await processingWorker.browserLocalSeed(expectedScope: expectedScope)
    }

    @discardableResult
    func ensureLocalIndexLoaded() async -> Bool {
        await loadLocalIndex(forceReload: false)
    }

    @discardableResult
    func reloadLocalIndex() async -> Bool {
        await loadLocalIndex(forceReload: true)
    }

    func refreshLocalFingerprints() async -> Set<LibraryMonthKey> {
        let changedMonths = await processingWorker.refreshLocalFingerprints()
        if !changedMonths.isEmpty {
            notifyBrowserLocalSeedChanged()
        }
        return changedMonths
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) async -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        let changedMonths = await processingWorker.refreshLocalIndex(
            forAssetIDs: assetIDs,
            expectedScope: hooks.currentScope()
        )
        if !changedMonths.isEmpty {
            notifyBrowserLocalSeedChanged()
        }
        return changedMonths
    }

    @discardableResult
    func syncRemoteSnapshotOnProcessingQueue(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> Set<LibraryMonthKey> {
        await processingWorker.syncRemoteSnapshot(
            state: state,
            hasActiveConnection: hasActiveConnection
        )
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

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        processingWorker.handlePhotoLibraryChange(changeInstance) { [weak self] result in
            MainActor.assumeIsolated {
                guard let self else { return }
                self.notifyBrowserLocalSeedChanged()
                if !result.changedMonths.isEmpty {
                    self.fileSizeCoordinator.enqueueRescan(for: result.changedMonths)
                    self.onMonthsChanged?(result.changedMonths)
                }
                if !result.fingerprintValidationAssetIDs.isEmpty {
                    self.onFingerprintValidationNeeded?(result.fingerprintValidationAssetIDs)
                }
            }
        }
    }

    @discardableResult
    private func loadLocalIndex(forceReload: Bool) async -> Bool {
        let result = await processingWorker.loadLocalIndex(
            forceReload: forceReload,
            scope: hooks.currentScope()
        )
        if let monthGroupingTimeZone = result.monthGroupingTimeZone {
            cachedMonthGroupingTimeZone = monthGroupingTimeZone
        }
        if result.isAuthorized {
            registerPhotoLibraryObserverIfNeeded()
            if result.didReload {
                fileSizeCoordinator.startFullScan()
            }
        } else {
            unregisterPhotoLibraryObserverIfNeeded()
            fileSizeCoordinator.reset()
        }
        if !result.fingerprintValidationAssetIDs.isEmpty {
            onFingerprintValidationNeeded?(result.fingerprintValidationAssetIDs)
        }
        if result.didReload {
            notifyBrowserLocalSeedChanged()
        }
        return !result.changedMonths.isEmpty
    }

    private func notifyBrowserLocalSeedChanged() {
        browserLocalSeedChanges.publish()
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
}
