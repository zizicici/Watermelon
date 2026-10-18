import Foundation
@preconcurrency import Photos

struct HomeLocalIndexReadError: LocalizedError {
    var errorDescription: String? { String(localized: "home.localIndex.error.title") }
}

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
    private(set) var needsLocalIndexReload = true

    var onReloadNeeded: (() -> Void)?
    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?
    var onFingerprintValidationNeeded: ((Set<String>) -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        hooks: Hooks,
        fetchLocalLibrary: (@Sendable (PhotoLibraryQuery) -> HomeDataProcessingWorker.LocalLibraryInput?)? = nil
    ) {
        let worker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository,
            remoteMonthSnapshot: hooks.remoteMonthSnapshot,
            fetchLocalLibrary: fetchLocalLibrary
        )
        self.processingWorker = worker
        self.fileSizeCoordinator = HomeFileSizeScanCoordinator(
            worker: worker,
            contentHashIndexRepository: contentHashIndexRepository
        )
        self.hooks = hooks
        super.init()
    }

    func setAppActive(_ active: Bool) {
        processingWorker.setAppActive(active)
        fileSizeCoordinator.setActive(active)
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingWorker.remoteSnapshotRevisionForQuery(hasActiveConnection: hasActiveConnection)
    }

    func monthGroupingTimeZoneForLocalIndex() -> MonthGroupingTimeZonePreference {
        cachedMonthGroupingTimeZone
    }

    func allAccessibleAssetIDs() async -> Set<String> {
        await processingWorker.allAccessibleAssetIDs()
    }

    func browserLocalSeed(expectedScope: HomeLocalLibraryScope) async -> HomeBrowserLocalSeed? {
        let result = await processingWorker.refreshLocalIndex(
            forAssetIDs: [], expectedScope: expectedScope, allowFullReload: false
        )
        applyRefreshResult(result)
        if !result.changedMonths.isEmpty { onMonthsChanged?(result.changedMonths) }
        return await processingWorker.browserLocalSeed(expectedScope: expectedScope)
    }

    func browserFileSizes(for identifiers: Set<String>) async -> [String: Int64] {
        await processingWorker.browserFileSizes(for: identifiers)
    }

    func reloadLocalIndex() async {
        let result = await processingWorker.reloadLocalIndex(scope: hooks.currentScope())
        applyLoadResult(result)
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>, allowFullReload: Bool = true) async -> Set<LibraryMonthKey> {
        let result = await processingWorker.refreshLocalIndex(
            forAssetIDs: assetIDs,
            expectedScope: hooks.currentScope(),
            allowFullReload: allowFullReload
        )
        applyRefreshResult(result)
        return result.changedMonths
    }

    private func applyRefreshResult(_ result: HomeLocalIndexRefreshResult) {
        switch result {
        case .requiresReload:
            needsLocalIndexReload = true
            onReloadNeeded?()
        case .failed:
            localIndexReadFailed()
        case .reloaded(let load):
            applyLoadResult(load)
        case .incremental(let changedMonths, let membershipChangedMonths):
            if !changedMonths.isEmpty { notifyBrowserLocalSeedChanged() }
            fileSizeCoordinator.enqueueRescan(for: membershipChangedMonths)
        }
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

    func remoteOnlyItems(for month: LibraryMonthKey) async throws -> [RemoteAlbumItem] {
        let months = await refreshLocalIndex(forAssetIDs: [])
        if !months.isEmpty { onMonthsChanged?(months) }
        guard !needsLocalIndexReload else { throw HomeLocalIndexReadError() }
        return try await processingWorker.remoteOnlyItems(for: month, expectedScope: hooks.currentScope())
    }

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        processingWorker.handlePhotoLibraryChange(changeInstance) { [weak self] result in
            MainActor.assumeIsolated {
                guard let self else { return }
                if result.readFailed { self.localIndexReadFailed() }
                else { self.notifyBrowserLocalSeedChanged() }
                if result.requiresReload {
                    self.needsLocalIndexReload = true
                    self.onReloadNeeded?()
                }
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

    private func applyLoadResult(_ result: HomeDataLoadResult) {
        guard !result.readFailed else {
            localIndexReadFailed()
            return
        }
        needsLocalIndexReload = false
        if let monthGroupingTimeZone = result.monthGroupingTimeZone {
            cachedMonthGroupingTimeZone = monthGroupingTimeZone
        }
        if result.isAuthorized {
            registerPhotoLibraryObserverIfNeeded()
            fileSizeCoordinator.startFullScan()
        } else {
            unregisterPhotoLibraryObserverIfNeeded()
            fileSizeCoordinator.reset()
        }
        if !result.fingerprintValidationAssetIDs.isEmpty {
            onFingerprintValidationNeeded?(result.fingerprintValidationAssetIDs)
        }
        notifyBrowserLocalSeedChanged()
    }

    private func localIndexReadFailed() {
        needsLocalIndexReload = true
        fileSizeCoordinator.reset()
        notifyBrowserLocalSeedChanged()
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

#if DEBUG
extension HomeIncrementalDataManager {
    var _testIsObservingPhotoLibrary: Bool { isObservingPhotoLibrary }
}
#endif
