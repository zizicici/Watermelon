import Foundation
@preconcurrency import Photos

@MainActor
final class MacPhotoLibraryIndexController: NSObject {
    enum State {
        case idle
        case loading
        case unavailable(PhotoLibraryAccessState)
        case loaded(PhotoLibraryMonthlyIndexSnapshot)
        case failed(String)
    }

    private struct RemoteApplication {
        let state: RemoteLibrarySnapshotState?
        let hasActiveConnection: Bool
        let completion: (() -> Void)?
    }

    private let worker: PhotoLibraryMonthlyIndexWorker
    private let localIndexBuildCoordinator:
        LocalIndexBuildCoordinator
    private let localIndexChangePublisher:
        LocalIndexChangePublisher
    private var loadTask: Task<Void, Never>?
    private var remoteTask: Task<Void, Never>?
    private var fileSizeScanTask: Task<Void, Never>?
    private var fingerprintRefreshTask: Task<Void, Never>?
    private var localReloadEpoch: UInt64 = 0
    private var photoChangeEpoch: UInt64 = 0
    private var fileSizeScanEpoch: UInt64 = 0
    private var remoteEpoch: UInt64 = 0
    private var activeRemoteApplication: RemoteApplication?
    private var pendingRemoteApplication: RemoteApplication?
    private var isObservingPhotoLibrary = false
    private var localIndexChangeObserverID: UUID?
    private var pendingFingerprintRefreshAssetIDs = Set<String>()
    private var photoLibraryAccessState: PhotoLibraryAccessState = .unknown
    private(set) var scope: HomeLocalLibraryScope = .allPhotos

    private(set) var state: State = .idle {
        didSet {
            onChange?(state)
        }
    }

    var onChange: ((State) -> Void)?

    init(
        worker: PhotoLibraryMonthlyIndexWorker,
        localIndexBuildCoordinator: LocalIndexBuildCoordinator,
        localIndexChangePublisher: LocalIndexChangePublisher
    ) {
        self.worker = worker
        self.localIndexBuildCoordinator =
            localIndexBuildCoordinator
        self.localIndexChangePublisher =
            localIndexChangePublisher
        super.init()
        localIndexChangeObserverID =
            localIndexChangePublisher.addObserver {
                [weak self] change in
                Task { @MainActor [weak self] in
                    self?.handleLocalIndexChange(change)
                }
            }
    }

    deinit {
        loadTask?.cancel()
        remoteTask?.cancel()
        fileSizeScanTask?.cancel()
        fingerprintRefreshTask?.cancel()
        if let localIndexChangeObserverID {
            localIndexChangePublisher.removeObserver(
                localIndexChangeObserverID
            )
        }
        if isObservingPhotoLibrary {
            PHPhotoLibrary.shared().unregisterChangeObserver(self)
        }
    }

    func reload() {
        loadTask?.cancel()
        if pendingRemoteApplication == nil {
            pendingRemoteApplication = activeRemoteApplication
        }
        remoteTask?.cancel()
        remoteTask = nil
        activeRemoteApplication = nil
        remoteEpoch &+= 1
        fileSizeScanTask?.cancel()
        fingerprintRefreshTask?.cancel()
        pendingFingerprintRefreshAssetIDs.removeAll()
        localReloadEpoch &+= 1
        photoChangeEpoch &+= 1
        let expectedReloadEpoch = localReloadEpoch
        fileSizeScanEpoch &+= 1
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-no-destination"
        ) {
            unregisterPhotoLibraryObserverIfNeeded()
            state = .loaded(.macDemoLocalOnly)
            return
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-photo-access-denied"
        ) {
            unregisterPhotoLibraryObserverIfNeeded()
            state = .loaded(.macDemoRemoteOnly)
            return
        }
        if MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
            arguments: ProcessInfo.processInfo.arguments
        ) {
            unregisterPhotoLibraryObserverIfNeeded()
            state = .loaded(
                ProcessInfo.processInfo.arguments.contains("--demo-connected")
                    ? .macDemo
                    : .macDemoLocalOnly
            )
            return
        }
        #endif
        state = .loading
        let expectedScope = scope
        loadTask = Task { [weak self] in
            guard let self else { return }
            let result = await worker.load(scope: expectedScope)
            guard !Task.isCancelled,
                  localReloadEpoch == expectedReloadEpoch,
                  scope == expectedScope else {
                return
            }
            loadTask = nil
            scope = result.loadedScope
            photoLibraryAccessState = result.accessState
            localIndexBuildCoordinator
                .scheduleFingerprintRevalidation(
                    for: result.fingerprintValidationAssetIDs
                )
            if result.accessState.canReadLibrary {
                registerPhotoLibraryObserverIfNeeded()
                state = .loaded(result.snapshot)
                startFileSizeScan()
            } else if result.snapshot.remoteAssetCount > 0 {
                unregisterPhotoLibraryObserverIfNeeded()
                state = .loaded(result.snapshot)
            } else {
                unregisterPhotoLibraryObserverIfNeeded()
                state = .unavailable(result.accessState)
            }
            applyPendingRemoteApplicationIfNeeded()
        }
    }

    func setScope(_ scope: HomeLocalLibraryScope) {
        guard self.scope != scope else { return }
        self.scope = scope
        reload()
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        worker.localAssetIDs(for: month, expectedScope: scope)
    }

    func applyRemoteSnapshot(
        _ remoteState: RemoteLibrarySnapshotState?,
        hasActiveConnection: Bool,
        completion: (() -> Void)? = nil
    ) {
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-no-destination"
        ) {
            state = .loaded(.macDemoLocalOnly)
            completion?()
            return
        }
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-photo-access-denied"
        ) {
            state = .loaded(.macDemoRemoteOnly)
            completion?()
            return
        }
        if MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
            arguments: ProcessInfo.processInfo.arguments
        ) {
            state = .loaded(
                ProcessInfo.processInfo.arguments.contains("--demo-connected")
                    ? .macDemo
                    : .macDemoLocalOnly
            )
            completion?()
            return
        }
        #endif
        let application = RemoteApplication(
            state: remoteState,
            hasActiveConnection: hasActiveConnection,
            completion: completion
        )
        guard loadTask == nil else {
            pendingRemoteApplication = application
            return
        }
        startRemoteApplication(application)
    }

    private func startRemoteApplication(
        _ application: RemoteApplication
    ) {
        remoteTask?.cancel()
        remoteEpoch &+= 1
        let expectedEpoch = remoteEpoch
        activeRemoteApplication = application
        remoteTask = Task { [weak self] in
            guard let self else { return }
            let snapshot: PhotoLibraryMonthlyIndexSnapshot
            if let remoteState = application.state {
                snapshot = await worker.applyRemoteSnapshot(
                    remoteState,
                    hasActiveConnection:
                        application.hasActiveConnection
                )
            } else {
                snapshot = await worker.clearRemoteSnapshot()
            }
            guard !Task.isCancelled,
                  remoteEpoch == expectedEpoch else {
                return
            }
            remoteTask = nil
            activeRemoteApplication = nil
            if !photoLibraryAccessState.canReadLibrary,
               snapshot.remoteAssetCount == 0 {
                state = .unavailable(photoLibraryAccessState)
            } else {
                state = .loaded(snapshot)
            }
            application.completion?()
        }
    }

    private func applyPendingRemoteApplicationIfNeeded() {
        guard let application = pendingRemoteApplication else {
            return
        }
        pendingRemoteApplication = nil
        startRemoteApplication(application)
    }

    private func startFileSizeScan() {
        fileSizeScanTask?.cancel()
        fileSizeScanEpoch &+= 1
        let epoch = fileSizeScanEpoch
        fileSizeScanTask = Task { [weak self] in
            guard let self else { return }
            await worker.scanFileSizes { [weak self] snapshot in
                Task { @MainActor [weak self] in
                    guard self?.fileSizeScanEpoch == epoch else {
                        return
                    }
                    self?.state = .loaded(snapshot)
                }
            }
        }
    }

    private func handleLocalIndexChange(
        _ change: LocalIndexChangePublisher.Change
    ) {
        switch change {
        case .touched(let assetIDs):
            pendingFingerprintRefreshAssetIDs.formUnion(assetIDs)
            startFingerprintRefreshIfNeeded()
        case .bulkInvalidation:
            reload()
        }
    }

    private func startFingerprintRefreshIfNeeded() {
        guard fingerprintRefreshTask == nil,
              !pendingFingerprintRefreshAssetIDs.isEmpty else {
            return
        }
        fingerprintRefreshTask = Task { [weak self] in
            await self?.drainFingerprintRefreshes()
        }
    }

    private func drainFingerprintRefreshes() async {
        defer {
            fingerprintRefreshTask = nil
            startFingerprintRefreshIfNeeded()
        }
        while !pendingFingerprintRefreshAssetIDs.isEmpty {
            guard !Task.isCancelled else { return }
            let assetIDs = pendingFingerprintRefreshAssetIDs
            pendingFingerprintRefreshAssetIDs.removeAll()
            guard let snapshot = await worker
                .refreshLocalFingerprints(for: assetIDs),
                  !Task.isCancelled else {
                continue
            }
            state = .loaded(snapshot)
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

}

extension MacPhotoLibraryIndexController: PHPhotoLibraryChangeObserver {
    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            guard let self else { return }
            photoChangeEpoch &+= 1
            let expectedPhotoChangeEpoch = photoChangeEpoch
            let expectedReloadEpoch = localReloadEpoch
            let scopeAtChange = scope
            let reconciledScope = await worker.reconciledScope(
                for: scopeAtChange
            )
            guard photoChangeEpoch == expectedPhotoChangeEpoch,
                  localReloadEpoch == expectedReloadEpoch,
                  scope == scopeAtChange else {
                return
            }
            if reconciledScope != scopeAtChange {
                scope = reconciledScope
                reload()
                return
            }
            worker.handlePhotoLibraryChange(changeInstance) { [weak self] snapshot in
                Task { @MainActor [weak self] in
                    guard let self,
                          self.photoChangeEpoch
                            == expectedPhotoChangeEpoch,
                          self.localReloadEpoch
                            == expectedReloadEpoch,
                          self.scope == scopeAtChange else {
                        return
                    }
                    self.localIndexBuildCoordinator
                        .scheduleFingerprintRevalidation(
                            for: snapshot
                                .fingerprintValidationAssetIDs
                        )
                    self.state = .loaded(snapshot.snapshot)
                    self.startFileSizeScan()
                }
            }
        }
    }
}

#if DEBUG
private extension PhotoLibraryMonthlyIndexSnapshot {
    static let macDemoLocalOnly: PhotoLibraryMonthlyIndexSnapshot = {
        let demo = macDemo
        let sections: [HomeMergedYearSection] = demo.sections.compactMap {
            section -> HomeMergedYearSection? in
            let rows = section.rows.compactMap { row -> HomeMonthRow? in
                guard row.local != nil else { return nil }
                return HomeMonthRow(
                    month: row.month,
                    local: row.local,
                    remote: nil
                )
            }
            guard !rows.isEmpty else { return nil }
            return HomeMergedYearSection(year: section.year, rows: rows)
        }
        return PhotoLibraryMonthlyIndexSnapshot(
            sections: sections,
            totalAssetCount: demo.totalAssetCount,
            totalPhotoCount: demo.totalPhotoCount,
            totalVideoCount: demo.totalVideoCount,
            totalSizeBytes: demo.totalSizeBytes,
            remoteAssetCount: 0,
            remotePhotoCount: 0,
            remoteVideoCount: 0,
            remoteSizeBytes: nil,
            monthGroupingTimeZone: demo.monthGroupingTimeZone
        )
    }()

    static let macDemoRemoteOnly: PhotoLibraryMonthlyIndexSnapshot = {
        let demo = macDemo
        let sections: [HomeMergedYearSection] = demo.sections.compactMap {
            section -> HomeMergedYearSection? in
            let rows = section.rows.compactMap { row -> HomeMonthRow? in
                guard row.remote != nil else { return nil }
                return HomeMonthRow(
                    month: row.month,
                    local: nil,
                    remote: row.remote
                )
            }
            guard !rows.isEmpty else { return nil }
            return HomeMergedYearSection(year: section.year, rows: rows)
        }
        return PhotoLibraryMonthlyIndexSnapshot(
            sections: sections,
            totalAssetCount: 0,
            totalPhotoCount: 0,
            totalVideoCount: 0,
            totalSizeBytes: nil,
            remoteAssetCount: demo.remoteAssetCount,
            remotePhotoCount: demo.remotePhotoCount,
            remoteVideoCount: demo.remoteVideoCount,
            remoteSizeBytes: demo.remoteSizeBytes,
            monthGroupingTimeZone: demo.monthGroupingTimeZone
        )
    }()

    static let macDemo: PhotoLibraryMonthlyIndexSnapshot = {
        let summaries = [
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 7),
                assetCount: 428,
                photoCount: 391,
                videoCount: 37,
                backedUpCount: 385,
                totalSizeBytes: 18_942_115_840
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 6),
                assetCount: 173,
                photoCount: 162,
                videoCount: 11,
                backedUpCount: 173,
                totalSizeBytes: 5_832_847_360
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 4),
                assetCount: 89,
                photoCount: 83,
                videoCount: 6,
                backedUpCount: 71,
                totalSizeBytes: 2_231_287_808
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2025, month: 12),
                assetCount: 612,
                photoCount: 570,
                videoCount: 42,
                backedUpCount: 612,
                totalSizeBytes: 31_457_280_000
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2025, month: 8),
                assetCount: 241,
                photoCount: 219,
                videoCount: 22,
                backedUpCount: 219,
                totalSizeBytes: 9_124_675_584
            )
        ]
        let remoteSummaries = [
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 7),
                assetCount: 401,
                photoCount: 370,
                videoCount: 31,
                backedUpCount: nil,
                totalSizeBytes: 17_908_121_600
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 6),
                assetCount: 173,
                photoCount: 162,
                videoCount: 11,
                backedUpCount: nil,
                totalSizeBytes: 5_832_847_360
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2026, month: 5),
                assetCount: 94,
                photoCount: 88,
                videoCount: 6,
                backedUpCount: nil,
                totalSizeBytes: 3_402_563_584
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2025, month: 12),
                assetCount: 612,
                photoCount: 570,
                videoCount: 42,
                backedUpCount: nil,
                totalSizeBytes: 31_457_280_000
            ),
            HomeMonthSummary(
                month: LibraryMonthKey(year: 2025, month: 10),
                assetCount: 130,
                photoCount: 122,
                videoCount: 8,
                backedUpCount: nil,
                totalSizeBytes: 4_639_473_664
            )
        ]
        let localByMonth = Dictionary(
            uniqueKeysWithValues: summaries.map { ($0.month, $0) }
        )
        let remoteByMonth = Dictionary(
            uniqueKeysWithValues: remoteSummaries.map { ($0.month, $0) }
        )
        let rows = Set(localByMonth.keys).union(remoteByMonth.keys).map {
            HomeMonthRow(
                month: $0,
                local: localByMonth[$0],
                remote: remoteByMonth[$0]
            )
        }
        let grouped = Dictionary(grouping: rows, by: { $0.month.year })
        let sections = grouped.map { year, yearRows in
            HomeMergedYearSection(
                year: year,
                rows: yearRows
                    .sorted { $0.month > $1.month }
            )
        }
        .sorted { $0.year > $1.year }
        return PhotoLibraryMonthlyIndexSnapshot(
            sections: sections,
            totalAssetCount: summaries.reduce(0) { $0 + $1.assetCount },
            totalPhotoCount: summaries.reduce(0) { $0 + $1.photoCount },
            totalVideoCount: summaries.reduce(0) { $0 + $1.videoCount },
            totalSizeBytes: summaries.compactMap(\.totalSizeBytes).reduce(0, +),
            remoteAssetCount: remoteSummaries.reduce(0) {
                $0 + $1.assetCount
            },
            remotePhotoCount: remoteSummaries.reduce(0) {
                $0 + $1.photoCount
            },
            remoteVideoCount: remoteSummaries.reduce(0) {
                $0 + $1.videoCount
            },
            remoteSizeBytes: remoteSummaries
                .compactMap(\.totalSizeBytes)
                .reduce(0, +),
            monthGroupingTimeZone: .frozenCurrent()
        )
    }()
}
#endif
