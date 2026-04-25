import Foundation
import os.log

private let homeLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeSync")

@MainActor
final class HomeScreenStore {
    // MARK: - Sub-controllers

    let dataManager: HomeIncrementalDataManager
    private(set) var executionCoordinator: HomeExecutionCoordinator
    private(set) var connectionController: HomeConnectionController
    private let pipBridge: PiPExecutionBridge
    private let scopeController = HomeScopeController()
    private var refreshScheduler: HomeRefreshScheduler!
    private var selectionController: HomeSelectionController!

    private let dependencies: DependencyContainer

    // MARK: - State

    private(set) var sections: [HomeMergedYearSection] = []
    private(set) var rowLookup: [LibraryMonthKey: HomeMonthRow] = [:]
    private(set) var localPhotoAccessState: LocalPhotoAccessState
    private(set) var albumDisplayCache: [String: LocalAlbumDescriptor] = [:]

    var selection: SelectionState { selectionController.state }

    var localLibraryScope: HomeLocalLibraryScope { scopeController.activeScope }
    var isReloadingScope: Bool { scopeController.isReloading }

    var connectionState: ConnectionState { connectionController.state }
    var remoteSyncProgress: RemoteSyncProgress? { connectionController.syncProgress }
    var executionState: HomeExecutionState? { executionCoordinator.currentState }

    var isSelectable: Bool {
        connectionState.isConnected && localPhotoAccessState.isAuthorized && executionState == nil && !isReloadingScope
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
    private var lastScopeAlertTime: CFAbsoluteTime = 0

    private static let scopeAlertDebounceInterval: CFAbsoluteTime = 2.0

    private enum ScopeNormalizationAlert {
        case albumsUnavailable
        case albumsUpdated
    }

    // MARK: - Init

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.localPhotoAccessState = LocalPhotoAccessState(
            authorizationStatus: dependencies.photoLibraryService.authorizationStatus()
        )
        let backupCoordinator = dependencies.backupCoordinator
        let scopeController = self.scopeController
        self.dataManager = HomeIncrementalDataManager(
            photoLibraryService: dependencies.photoLibraryService,
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: dependencies.databaseManager),
            remoteMonthSnapshot: { month in
                backupCoordinator.remoteMonthRawData(for: month)
            },
            currentScope: { scopeController.activeScope }
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
        self.selectionController = HomeSelectionController(dependencies: HomeSelectionController.Dependencies(
            isSelectable: { [weak self] in self?.isSelectable ?? false },
            isRemoteSelectionAllowed: { [weak self] in self?.isRemoteSelectionAllowed ?? false },
            sections: { [weak self] in self?.sections ?? [] }
        ))
        self.refreshScheduler = HomeRefreshScheduler(hooks: HomeRefreshScheduler.Hooks(
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
                guard let self else { return }
                self.refreshRowLookup()
                self.rebuildSections()
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
        bind()
        pipBridge.attach()
    }

    deinit {
        bootstrapTask?.cancel()
    }

    // MARK: - Bind Sub-controllers

    private func bind() {
        scopeController.onChange = { [weak self] in
            self?.onChange?(.selection)
        }
        dataManager.onMonthsChanged = { [weak self] months in
            self?.handleDataChange(months)
        }
        dataManager.onFileSizesUpdated = { [weak self] months in
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
        let normalized = isExecuting
            ? (scope: scope, alert: ScopeNormalizationAlert?.none)
            : normalizedLocalLibraryScope(scope)
        switch scopeController.setActive(normalized.scope, isExecuting: isExecuting) {
        case .applied:
            selectionController.clear()
            if let alert = normalized.alert { emitScopeAlertIfNotDebounced(alert) }
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
        executionCoordinator.enter(backup: backup, download: download, complement: complement)
    }

    func pauseExecution() { executionCoordinator.pause() }
    func resumeExecution() { executionCoordinator.resume() }
    func stopExecution() { executionCoordinator.stop() }
    func exitExecution() { executionCoordinator.exit() }

    // MARK: - Connection Actions

    func connectProfile(_ profile: ServerProfileRecord) {
        connectionController.promptAndConnect(profile: profile)
    }

    func disconnect() {
        connectionController.disconnect()
    }

    func requestLocalPhotoAccessIfNeeded() {
        Task { [weak self] in
            guard let self else { return }

            let accessState = LocalPhotoAccessState(
                authorizationStatus: self.dependencies.photoLibraryService.authorizationStatus()
            )

            switch accessState {
            case .authorized:
                self.scheduleRefresh([.reloadLocal, .notifyStructural])
            case .notDetermined:
                _ = await self.dependencies.photoLibraryService.requestAuthorization()
                guard !Task.isCancelled else { return }
                self.scheduleRefresh([.reloadLocal, .notifyStructural])
            case .denied:
                break
            }
        }
    }

    func refreshLocalPhotoAccessIfNeeded() {
        let accessState = LocalPhotoAccessState(
            authorizationStatus: dependencies.photoLibraryService.authorizationStatus()
        )
        let scopeChanged = normalizeLocalLibraryScopeIfNeeded(shouldAlert: true)
        guard accessState != localPhotoAccessState || scopeChanged else { return }
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

        let previousMonths = Set(rowLookup.keys)
        refreshRowLookup()
        rebuildSections()
        let currentMonths = Set(rowLookup.keys)

        selectionController.intersect(with: currentMonths)

        if previousMonths != currentMonths {
            onChange?(.structural)
        } else {
            onChange?(.data(months))
        }
    }

    private func handleFileSizeChange(_ months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }

        var changedMonths = Set<LibraryMonthKey>()
        for month in months where rowLookup[month] != nil {
            let updatedRow = dataManager.monthRow(for: month)
            guard updatedRow.local != nil || updatedRow.remote != nil else { continue }
            rowLookup[month] = updatedRow
            changedMonths.insert(month)
        }

        guard !changedMonths.isEmpty else { return }
        refreshSections(for: changedMonths)
        onChange?(.fileSizes(changedMonths))
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
                let normalized = normalizedLocalLibraryScope(pendingScope)
                scopeController.setActiveFromNormalize(normalized.scope)
                if let alert = normalized.alert { emitScopeAlertIfNotDebounced(alert) }
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

            // Refresh rowLookup only for changed months
            for month in changedMonths {
                rowLookup[month] = dataManager.monthRow(for: month)
            }
            rebuildSections()
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

    private func refreshRowLookup() {
        rowLookup = dataManager.allMonthRows()
    }

    private func rebuildSections() {
        var rowsByYear: [Int: [HomeMonthRow]] = [:]
        for (_, row) in rowLookup {
            rowsByYear[row.month.year, default: []].append(row)
        }
        sections = rowsByYear
            .map { HomeMergedYearSection(year: $0.key, rows: $0.value.sorted { $0.month > $1.month }) }
            .sorted { $0.year > $1.year }
    }

    private func refreshSections(for months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }

        sections = sections.map { section in
            let updatedRows = section.rows.map { row in
                guard months.contains(row.month),
                      let updatedRow = rowLookup[row.month] else { return row }
                return updatedRow
            }
            return HomeMergedYearSection(year: section.year, rows: updatedRows)
        }
    }

    private func refreshAllAndNotify() {
        refreshRowLookup()
        rebuildSections()
        onChange?(.structural)
    }

    @discardableResult
    private func refreshLocalPhotoAccessState() -> Bool {
        let newState = LocalPhotoAccessState(
            authorizationStatus: dependencies.photoLibraryService.authorizationStatus()
        )
        guard newState != localPhotoAccessState else { return false }
        localPhotoAccessState = newState
        return true
    }

    private func normalizedLocalLibraryScope(
        _ scope: HomeLocalLibraryScope
    ) -> (scope: HomeLocalLibraryScope, alert: ScopeNormalizationAlert?) {
        guard case .albums(let ids) = scope else { return (scope, nil) }
        let accessState = LocalPhotoAccessState(
            authorizationStatus: dependencies.photoLibraryService.authorizationStatus()
        )
        guard accessState.isAuthorized else { return (scope, nil) }

        guard !ids.isEmpty else { return (.allPhotos, nil) }

        let existingIDs = dependencies.photoLibraryService.existingUserAlbumIdentifiers(in: ids)
        guard existingIDs != ids else { return (scope, nil) }

        if existingIDs.isEmpty {
            return (.allPhotos, .albumsUnavailable)
        }
        return (.albums(existingIDs), .albumsUpdated)
    }

    private func emitScopeAlertIfNotDebounced(_ alert: ScopeNormalizationAlert) {
        let now = CFAbsoluteTimeGetCurrent()
        guard now - lastScopeAlertTime >= Self.scopeAlertDebounceInterval else { return }
        lastScopeAlertTime = now
        switch alert {
        case .albumsUnavailable:
            onAlert?(
                String(localized: "home.alert.localAlbumsUnavailable"),
                String(localized: "home.alert.localAlbumsUnavailableMessage")
            )
        case .albumsUpdated:
            onAlert?(
                String(localized: "home.alert.localAlbumsUpdated"),
                String(localized: "home.alert.localAlbumsUpdatedMessage")
            )
        }
    }

    @discardableResult
    private func normalizeLocalLibraryScopeIfNeeded(shouldAlert: Bool) -> Bool {
        guard executionState == nil else {
            // Defer normalization until execution ends. Without this, a downgrade
            // detected mid-execution (album deleted while uploading) would not run
            // again until the next user-driven reload.
            scopeController.deferActiveScopeForReevaluation()
            return false
        }

        let result = normalizedLocalLibraryScope(scopeController.activeScope)
        let changed = scopeController.setActiveFromNormalize(result.scope)
        if changed {
            selectionController.clear()
            if shouldAlert, let alert = result.alert {
                emitScopeAlertIfNotDebounced(alert)
            }
        }
        return changed
    }

    private func scheduleRefresh(_ work: HomeRefreshScheduler.Work) {
        refreshScheduler.enqueue(work)
    }
}
