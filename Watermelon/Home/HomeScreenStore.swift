import Foundation
import os.log

private let homeLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeSync")

@MainActor
final class HomeScreenStore {
    // MARK: - Sub-controllers

    let dataManager: HomeIncrementalDataManager
    let executionCoordinator: HomeExecutionCoordinator
    let connectionController: HomeConnectionController
    private let pipBridge: PiPExecutionBridge
    private let scopeController = HomeScopeController()
    private let sectionBuilder: HomeSectionBuilder
    private let photoAccessGate: HomePhotoAccessGate
    private let scopeNormalizer: HomeScopeNormalizer
    // `lazy var` rather than `let`: the hooks closures need `[weak self]`, which
    // Swift definite-initialization rejects inside `init` until self is fully
    // constructed. First access happens after init returns (via `scheduleRefresh`
    // / `selectionController.clear()` etc.), so lazy init is safe.
    private lazy var refreshScheduler = makeRefreshScheduler()
    private lazy var selectionController = makeSelectionController()

    private let dependencies: DependencyContainer

    // MARK: - State

    private(set) var albumDisplayCache: [String: LocalAlbumDescriptor] = [:]

    var sections: [HomeMergedYearSection] { sectionBuilder.sections }
    var rowLookup: [LibraryMonthKey: HomeMonthRow] { sectionBuilder.rowLookup }
    var selection: SelectionState { selectionController.state }
    var localPhotoAccessState: LocalPhotoAccessState { photoAccessGate.state }

    var localLibraryScope: HomeLocalLibraryScope { scopeController.activeScope }
    var isReloadingScope: Bool { scopeController.isReloading }

    var connectionState: ConnectionState { connectionController.state }
    var remoteSyncProgress: RemoteSyncProgress? { connectionController.syncProgress }
    var executionState: HomeExecutionState? { executionCoordinator.currentState }

    private(set) var isRemoteMaintenanceActive: Bool = false

    var isSelectable: Bool {
        connectionState.isConnected
            && localPhotoAccessState.isAuthorized
            && executionState == nil
            && !isReloadingScope
            && !isRemoteMaintenanceActive
    }

    /// Read live so a verify started after a confirm dialog opened still blocks
    /// the action that dialog gates.
    var isMaintenanceBlocked: Bool {
        dependencies.remoteMaintenanceController.isVerifying
    }

    var isRemoteSelectionAllowed: Bool {
        !localLibraryScope.isSpecificAlbums
    }

    var savedProfiles: [ServerProfileRecord] { connectionController.savedProfiles }

    // MARK: - Notification

    var onChange: ((HomeChangeKind) -> Void)?
    var onAlert: ((String, String) -> Void)?
    var onNeedsPasswordPrompt: ((ServerProfileRecord, _ completion: @escaping (String) -> Void) -> Void)?
    var onConnectFailed: ((ServerProfileRecord, Error) -> Void)?

    // MARK: - Private

    private var wasExecutionActive = false
    private var lastMonthPhases: [LibraryMonthKey: MonthPlan.Phase] = [:]
    private var bootstrapTask: Task<Void, Never>?
    private var maintenanceObserver: NSObjectProtocol?

    // MARK: - Init

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.photoAccessGate = HomePhotoAccessGate(photoLibraryService: dependencies.photoLibraryService)
        let photoLib = dependencies.photoLibraryService
        self.scopeNormalizer = HomeScopeNormalizer(hooks: HomeScopeNormalizer.Hooks(
            authorizationStatus: { photoLib.authorizationStatus() },
            existingUserAlbumIdentifiers: { ids in photoLib.existingUserAlbumIdentifiers(in: ids) }
        ))
        let backupCoordinator = dependencies.backupCoordinator
        let scopeController = self.scopeController
        self.dataManager = HomeIncrementalDataManager(
            photoLibraryService: dependencies.photoLibraryService,
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: dependencies.databaseManager),
            hooks: HomeIncrementalDataManager.Hooks(
                remoteMonthSnapshot: { backupCoordinator.remoteMonthRawData(for: $0) },
                currentScope: { scopeController.activeScope }
            )
        )
        self.connectionController = HomeConnectionController(dependencies: dependencies)
        let connectionCtrl = self.connectionController
        self.executionCoordinator = HomeExecutionCoordinator(
            dependencies: dependencies,
            dataAccess: HomeExecutionCoordinator.DataAccess(
                localAssetIDs: { [dataManager] month in dataManager.localAssetIDs(for: month) },
                remoteOnlyItems: { [dataManager] month in await dataManager.remoteOnlyItems(for: month) },
                syncRemoteData: { [dataManager, dependencies, weak connectionCtrl] in
                    let active = connectionCtrl?.state.isConnected ?? false
                    let revision = dataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
                    let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(since: revision)
                    return await dataManager.syncRemoteSnapshotOnProcessingQueue(
                        state: snapshotState,
                        hasActiveConnection: active
                    )
                },
                refreshLocalIndex: { [dataManager] assetIDs in
                    await dataManager.refreshLocalIndex(forAssetIDs: assetIDs)
                }
            )
        )
        self.pipBridge = PiPExecutionBridge(coordinator: self.executionCoordinator)
        let dataManagerRef = self.dataManager
        self.sectionBuilder = HomeSectionBuilder(hooks: HomeSectionBuilder.Hooks(
            allMonthRows: { dataManagerRef.allMonthRows() },
            monthRow: { dataManagerRef.monthRow(for: $0) }
        ))
        bind()
        pipBridge.attach()
        observeMaintenance()
    }

    private func observeMaintenance() {
        maintenanceObserver = NotificationCenter.default.addObserver(
            forName: .RemoteMaintenanceDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                guard let self else { return }
                let active = self.dependencies.remoteMaintenanceController.isVerifying
                guard self.isRemoteMaintenanceActive != active else { return }
                let wasActive = self.isRemoteMaintenanceActive
                self.isRemoteMaintenanceActive = active
                // Sync first: verify's `replaceMonth` won't reach the grid until HomeIncrementalDataManager pulls the new revision.
                if wasActive && !active {
                    self.scheduleRefresh([.syncRemote, .notifyStructural])
                } else {
                    self.onChange?(.structural)
                }
            }
        }
    }

    private func makeRefreshScheduler() -> HomeRefreshScheduler {
        HomeRefreshScheduler(hooks: HomeRefreshScheduler.Hooks(
            normalizeBeforeReload: { [weak self] in
                self?.normalizeLocalLibraryScopeIfNeeded(shouldAlert: true) ?? false
            },
            reloadLocal: { [weak self] in
                _ = await self?.dataManager.reloadLocalIndex()
            },
            refreshAccessState: { [weak self] in
                self?.refreshLocalPhotoAccessState() ?? false
            },
            afterReload: { [weak self] scopeChanged, accessChanged, hasMoreReloadPending in
                guard let self else { return }
                if accessChanged || scopeChanged { self.selectionController.clear() }
                self.scopeController.completeReload(
                    loaded: self.scopeController.activeScope,
                    hasMoreReloadPending: hasMoreReloadPending
                )
            },
            syncRemote: { [weak self] in
                await self?.syncRemoteDataIfNeeded()
            },
            postProcess: { [weak self] in
                self?.sectionBuilder.rebuildAll()
            },
            onIterationComplete: { [weak self] work, scopeChanged, accessChanged in
                guard let self else { return }
                if work.contains(.notifyConnection) {
                    self.onChange?(.connection)
                } else if work.contains(.notifyStructural) || accessChanged || scopeChanged {
                    self.onChange?(.structural)
                }
            }
        ))
    }

    private func makeSelectionController() -> HomeSelectionController {
        HomeSelectionController(hooks: HomeSelectionController.Hooks(
            isSelectable: { [weak self] in self?.isSelectable ?? false },
            isRemoteSelectionAllowed: { [weak self] in self?.isRemoteSelectionAllowed ?? false },
            sections: { [weak self] in self?.sections ?? [] }
        ))
    }

    deinit {
        bootstrapTask?.cancel()
        if let maintenanceObserver {
            NotificationCenter.default.removeObserver(maintenanceObserver)
        }
    }

    // MARK: - Bind Sub-controllers

    private func bind() {
        scopeController.onChange = { [weak self] in
            self?.onChange?(.selection)
        }
        scopeNormalizer.onAlert = { [weak self] title, message in
            self?.onAlert?(title, message)
        }
        dataManager.onMonthsChanged = { [weak self] months in
            self?.handleDataChange(months)
        }
        dataManager.fileSizeCoordinator.onFileSizesUpdated = { [weak self] months in
            self?.handleFileSizeChange(months)
        }
        executionCoordinator.onStateChanged = { [weak self] in
            self?.handleExecutionChange()
        }
        executionCoordinator.onAlert = { [weak self] title, message in
            self?.onAlert?(title, message)
        }
        connectionController.onStateChanged = { [weak self] in
            self?.handleConnectionChange()
        }
        connectionController.onSyncProgressChanged = { [weak self] in
            self?.onChange?(.connectionProgress)
        }
        // Remote snapshot data accumulates in snapshotCache on the connection thread;
        // processing happens in handleConnectionChange once connected.
        connectionController.onNeedsPasswordPrompt = { [weak self] profile, completion in
            self?.onNeedsPasswordPrompt?(profile, completion)
        }
        connectionController.onConnectFailed = { [weak self] profile, error in
            self?.onConnectFailed?(profile, error)
        }
    }

    // MARK: - Lifecycle

    func load() {
        connectionController.loadProfiles()
        bootstrapTask?.cancel()
        bootstrapTask = Task { [weak self] in
            guard let self else { return }
            await self.dataManager.ensureLocalIndexLoaded()
            guard !Task.isCancelled else { return }
            _ = self.refreshLocalPhotoAccessState()
            self.connectionController.attemptAutoConnect()
            // Don't call syncRemoteDataIfNeeded here — if auto-connect is in progress,
            // connectionState is .connecting and hasActiveConnection would be false,
            // which would clear remote data and advance revision, corrupting the bootstrap.
            // Connection callbacks handle sync when ready.
            self.refreshAllAndNotify()
        }
    }

    func reloadProfiles() {
        connectionController.loadProfiles()
        onChange?(.connection)
    }

    func setLocalLibraryScope(_ scope: HomeLocalLibraryScope, descriptors: [LocalAlbumDescriptor] = []) {
        for descriptor in descriptors {
            albumDisplayCache[descriptor.localIdentifier] = descriptor
        }
        let isExecuting = executionState != nil
        let normalized: (scope: HomeLocalLibraryScope, alert: HomeScopeNormalizer.Alert?) = isExecuting
            ? (scope, nil)
            : scopeNormalizer.normalize(scope)
        switch scopeController.setActive(normalized.scope, isExecuting: isExecuting) {
        case .applied:
            selectionController.clear()
            if let alert = normalized.alert { scopeNormalizer.emitAlertIfNotDebounced(alert) }
            onChange?(.selection)
            scheduleRefresh([.reloadLocal, .notifyStructural])
        case .noChange:
            // Identity unchanged but descriptors may have refreshed (e.g., rename); fan
            // out so the header re-reads the cache.
            if !descriptors.isEmpty {
                onChange?(.selection)
            }
        case .deferred:
            break
        }
    }

    // MARK: - Selection Actions

    func toggleMonth(_ month: LibraryMonthKey, side: SelectionSide) {
        if selectionController.toggleMonth(month, side: side) {
            onChange?(.selection)
        }
    }

    func toggleYear(sectionIndex: Int, side: SelectionSide) {
        if selectionController.toggleYear(sectionIndex: sectionIndex, side: side) {
            onChange?(.selection)
        }
    }

    func toggleAll(side: SelectionSide) {
        if selectionController.toggleAll(side: side) {
            onChange?(.selection)
        }
    }

    // MARK: - Execution Actions

    func startExecution(backup: [LibraryMonthKey], download: [LibraryMonthKey], complement: [LibraryMonthKey]) {
        guard !rejectIfMaintaining() else { return }
        executionCoordinator.enter(backup: backup, download: download, complement: complement)
    }

    func pauseExecution() { executionCoordinator.pause() }
    func resumeExecution() { executionCoordinator.resume() }
    func stopExecution() { executionCoordinator.stop() }
    func exitExecution() { executionCoordinator.exit() }

    // MARK: - Connection Actions

    func connectProfile(_ profile: ServerProfileRecord) {
        guard !rejectIfMaintaining() else { return }
        connectionController.promptAndConnect(profile: profile)
    }

    func disconnect() {
        guard !rejectIfMaintaining() else { return }
        connectionController.disconnect()
    }

    private func rejectIfMaintaining() -> Bool {
        guard isMaintenanceBlocked else { return false }
        onAlert?(
            String(localized: "common.error"),
            String(localized: "home.alert.maintenanceInProgress")
        )
        return true
    }

    func requestLocalPhotoAccessIfNeeded() {
        Task { [weak self] in
            guard let self else { return }

            let access = LocalPhotoAccessState(authorizationStatus: self.photoAccessGate.currentSystemAuthorizationStatus())
            switch access {
            case .authorized:
                self.scheduleRefresh([.reloadLocal, .notifyStructural])
            case .notDetermined:
                _ = await self.photoAccessGate.requestAuthorization()
                guard !Task.isCancelled else { return }
                self.scheduleRefresh([.reloadLocal, .notifyStructural])
            case .denied:
                break
            }
        }
    }

    func refreshLocalPhotoAccessIfNeeded() {
        let scopeChanged = normalizeLocalLibraryScopeIfNeeded(shouldAlert: true)
        let accessChanged = photoAccessGate.hasSystemStateDiverged()
        guard scopeChanged || accessChanged else { return }
        scheduleRefresh([.reloadLocal, .notifyStructural])
    }

    // MARK: - Derived State

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        executionState?.intent(for: month) ?? selection.intent(for: month)
    }

    func progressPercent(for month: LibraryMonthKey) -> Double? {
        let row = rowLookup[month]
        let monthIntent = intent(for: month)
        let matched = dataManager.matchedCount(for: month)

        if let exec = executionState {
            return exec.progressPercent(for: month, row: row, intent: monthIntent, matchedCount: matched)
        }

        return HomeProgressCalculator.basePercent(
            row: row,
            intent: monthIntent,
            matchedCount: matched
        )
    }

    // MARK: - Change Handlers

    private func handleDataChange(_ months: Set<LibraryMonthKey>) {
        if normalizeLocalLibraryScopeIfNeeded(shouldAlert: true) {
            scheduleRefresh([.reloadLocal, .notifyStructural])
            return
        }

        let previousMonths = Set(sectionBuilder.rowLookup.keys)
        sectionBuilder.rebuildAll()
        let currentMonths = Set(sectionBuilder.rowLookup.keys)

        selectionController.intersect(with: currentMonths)

        if previousMonths != currentMonths {
            onChange?(.structural)
        } else {
            onChange?(.data(months))
        }
    }

    private func handleFileSizeChange(_ months: Set<LibraryMonthKey>) {
        let changed = sectionBuilder.refreshFileSizeRows(for: months)
        guard !changed.isEmpty else { return }
        onChange?(.fileSizes(changed))
    }

    private func handleExecutionChange() {
        pipBridge.observeStateChange()

        let isNowActive = executionCoordinator.isActive
        homeLog.info("[HomeSync] handleExecutionChange: active=\(isNowActive), wasActive=\(self.wasExecutionActive)")

        if wasExecutionActive && !isNowActive {
            // Execution ended
            selectionController.clear()
            wasExecutionActive = false
            let allPrevious = Set(lastMonthPhases.keys)
            lastMonthPhases.removeAll()

            if let pendingScope = scopeController.resumeFromDeferred() {
                let normalized = scopeNormalizer.normalize(pendingScope)
                scopeController.setActiveFromNormalize(normalized.scope)
                if let alert = normalized.alert { scopeNormalizer.emitAlertIfNotDebounced(alert) }
            }

            scheduleRefresh([.reloadLocal, .syncRemote, .notifyStructural])

            onChange?(.execution(allPrevious))
            return
        }

        if !wasExecutionActive && isNowActive {
            wasExecutionActive = true
        }

        // Compute which months actually need UI update:
        // - months whose phase changed
        // - months that are active (progress bar updates)
        var changedMonths = executionCoordinator.consumePendingDataChangedMonths()
        if let state = executionCoordinator.currentState {
            for (month, plan) in state.monthPlans {
                if lastMonthPhases[month] != plan.phase || plan.isActive {
                    changedMonths.insert(month)
                }
            }
            // Months removed from execution
            for month in lastMonthPhases.keys where state.monthPlans[month] == nil {
                changedMonths.insert(month)
            }
            lastMonthPhases = state.monthPlans.mapValues(\.phase)

            sectionBuilder.updateRowsAndRebuild(for: changedMonths)
        }

        onChange?(.execution(changedMonths))
    }

    private func handleConnectionChange() {
        let stateDesc: String
        switch connectionState {
        case .connected: stateDesc = "connected"
        case .connecting: stateDesc = "connecting"
        case .disconnected: stateDesc = "disconnected"
        }
        homeLog.info("[HomeSync] handleConnectionChange: state=\(stateDesc)")

        if case .disconnected = connectionState, executionCoordinator.isActive {
            executionCoordinator.failForMissingConnection()
        }

        selectionController.clear()

        switch connectionState {
        case .connecting:
            // Don't sync remote during connecting — reloadRemoteIndex is still rebuilding
            // the shared snapshotCache. A sync here with hasActiveConnection=false would clear
            // the remote engine and record a stale revision, causing the subsequent .connected
            // refresh to get an empty delta.
            scheduleRefresh([.notifyConnection])
        case .connected, .disconnected:
            scheduleRefresh([.syncRemote, .notifyConnection])
        }
    }

    // MARK: - Data Refresh

    func syncRemoteDataIfNeeded() async {
        let start = CFAbsoluteTimeGetCurrent()
        let active = connectionState.isConnected
        let revision = dataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(
            since: revision
        )
        await dataManager.syncRemoteSnapshotOnProcessingQueue(state: snapshotState, hasActiveConnection: active)
        let elapsed = CFAbsoluteTimeGetCurrent() - start
        homeLog.info("[HomeSync] syncRemoteDataIfNeeded: revision=\(revision.map { String($0) } ?? "nil"), connected=\(active), deltaMonths=\(snapshotState.monthDeltas.count), \(String(format: "%.3f", elapsed))s")
    }

    private func refreshAllAndNotify() {
        sectionBuilder.rebuildAll()
        onChange?(.structural)
    }

    @discardableResult
    private func refreshLocalPhotoAccessState() -> Bool {
        photoAccessGate.refresh()
    }

    @discardableResult
    private func normalizeLocalLibraryScopeIfNeeded(shouldAlert: Bool) -> Bool {
        guard executionState == nil else {
            // Defer normalization until execution ends. Without this, a downgrade
            // detected mid-execution (album deleted while uploading) would not run
            // again until the next user-driven reload.
            scopeController.requestPostExecutionRenormalization()
            return false
        }

        let result = scopeNormalizer.normalize(scopeController.activeScope)
        let changed = scopeController.setActiveFromNormalize(result.scope)
        if changed {
            selectionController.clear()
            if shouldAlert, let alert = result.alert {
                scopeNormalizer.emitAlertIfNotDebounced(alert)
            }
        }
        return changed
    }

    private func scheduleRefresh(_ work: HomeRefreshScheduler.Work) {
        refreshScheduler.enqueue(work)
    }
}
