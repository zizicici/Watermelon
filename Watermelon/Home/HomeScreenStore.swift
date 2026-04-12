import Foundation
import os.log

private let homeLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeSync")

@MainActor
final class HomeScreenStore {

    // MARK: - Sub-controllers

    let dataManager: HomeIncrementalDataManager
    private(set) var executionCoordinator: HomeExecutionCoordinator
    private(set) var connectionController: HomeConnectionController

    private let dependencies: DependencyContainer

    // MARK: - State

    private(set) var sections: [HomeMergedYearSection] = []
    private(set) var rowLookup: [LibraryMonthKey: HomeMonthRow] = [:]
    private(set) var selection = SelectionState()

    var connectionState: ConnectionState { connectionController.state }
    var executionState: HomeExecutionState? { executionCoordinator.currentState }

    var isSelectable: Bool {
        connectionState.isConnected && executionState == nil
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
    private var reloadTask: Task<Void, Never>?

    // MARK: - Init

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.dataManager = HomeIncrementalDataManager(
            photoLibraryService: dependencies.photoLibraryService,
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: dependencies.databaseManager)
        )
        self.connectionController = HomeConnectionController(dependencies: dependencies)
        let connectionCtrl = self.connectionController
        self.executionCoordinator = HomeExecutionCoordinator(
            dependencies: dependencies,
            dataAccess: HomeExecutionCoordinator.DataAccess(
                localAssetIDs: { [dataManager] month in dataManager.localAssetIDs(for: month) },
                remoteOnlyItems: { [dataManager] month in dataManager.remoteOnlyItems(for: month) },
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
        bind()
    }

    deinit {
        reloadTask?.cancel()
    }

    // MARK: - Bind Sub-controllers

    private func bind() {
        dataManager.onMonthsChanged = { [weak self] months in
            self?.handleDataChange(months)
        }
        dataManager.onFileSizesUpdated = { [weak self] months in
            self?.handleDataChange(months)
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
        reloadTask?.cancel()
        reloadTask = Task { [weak self] in
            guard let self else { return }
            await self.dataManager.ensureLocalIndexLoaded()
            self.connectionController.attemptAutoConnect()
            // Don't call syncRemoteDataIfNeeded here — if auto-connect is in progress,
            // connectionState is .connecting and hasActiveConnection would be false,
            // which would clear remote data and advance revision, corrupting the bootstrap.
            // Connection callbacks handle sync when ready.
            self.refreshAllAndNotify()
        }
    }

    // MARK: - Selection Actions

    func toggleMonth(_ month: LibraryMonthKey, side: SelectionSide) {
        guard isSelectable else { return }
        switch side {
        case .local:
            if selection.localMonths.contains(month) { selection.localMonths.remove(month) }
            else { selection.localMonths.insert(month) }
        case .remote:
            if selection.remoteMonths.contains(month) { selection.remoteMonths.remove(month) }
            else { selection.remoteMonths.insert(month) }
        }
        onChange?(.selection)
    }

    func toggleYear(sectionIndex: Int, side: SelectionSide) {
        guard isSelectable, sectionIndex < sections.count else { return }
        let allMonths = Set(sections[sectionIndex].rows.map(\.month))
        switch side {
        case .local:
            if allMonths.isSubset(of: selection.localMonths) { selection.localMonths.subtract(allMonths) }
            else { selection.localMonths.formUnion(allMonths) }
        case .remote:
            if allMonths.isSubset(of: selection.remoteMonths) { selection.remoteMonths.subtract(allMonths) }
            else { selection.remoteMonths.formUnion(allMonths) }
        }
        onChange?(.selection)
    }

    func toggleAll(side: SelectionSide) {
        guard isSelectable else { return }
        let allMonths = Set(sections.flatMap { $0.rows.map(\.month) })
        switch side {
        case .local:
            if allMonths.isSubset(of: selection.localMonths) { selection.localMonths.removeAll() }
            else { selection.localMonths = allMonths }
        case .remote:
            if allMonths.isSubset(of: selection.remoteMonths) { selection.remoteMonths.removeAll() }
            else { selection.remoteMonths = allMonths }
        }
        onChange?(.selection)
    }

    // MARK: - Execution Actions

    func startExecution(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        executionCoordinator.enter(upload: upload, download: download, sync: sync)
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

    // MARK: - Derived State

    func arrowDirection(for month: LibraryMonthKey) -> HomeArrowDirection? {
        executionState?.direction(for: month) ?? selection.arrowDirection(for: month)
    }

    func progressPercent(for month: LibraryMonthKey) -> Double? {
        let row = rowLookup[month]
        let direction = arrowDirection(for: month)
        let matched = dataManager.matchedCount(for: month)

        if let exec = executionState {
            return exec.progressPercent(for: month, row: row, direction: direction, matchedCount: matched)
        }

        guard let row, let direction else { return nil }
        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0
        switch direction {
        case .toRemote:
            return localCount > 0 ? Double(matched) / Double(localCount) * 100 : nil
        case .toLocal:
            return remoteCount > 0 ? Double(matched) / Double(remoteCount) * 100 : nil
        case .sync:
            let remoteOnly = max(0, remoteCount - matched)
            let total = localCount + remoteOnly
            return total > 0 ? Double(matched) / Double(total) * 100 : nil
        }
    }

    // MARK: - Change Handlers

    private func handleDataChange(_ months: Set<LibraryMonthKey>) {
        let previousMonths = Set(rowLookup.keys)
        refreshRowLookup()
        rebuildSections()
        let currentMonths = Set(rowLookup.keys)

        selection.localMonths.formIntersection(currentMonths)
        selection.remoteMonths.formIntersection(currentMonths)

        if previousMonths != currentMonths {
            onChange?(.structural)
        } else {
            onChange?(.data(months))
        }
    }

    private func handleExecutionChange() {
        let isNowActive = executionCoordinator.isActive
        homeLog.info("[HomeSync] handleExecutionChange: active=\(isNowActive), wasActive=\(self.wasExecutionActive)")

        if wasExecutionActive && !isNowActive {
            // Execution ended
            selection.clear()
            wasExecutionActive = false
            let allPrevious = Set(lastMonthPhases.keys)
            lastMonthPhases.removeAll()

            reloadTask?.cancel()
            reloadTask = Task { [weak self] in
                guard let self else { return }
                await self.dataManager.reloadLocalIndex()
                await self.syncRemoteDataIfNeeded()
                self.refreshAllAndNotify()
            }

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

        reloadTask?.cancel()
        selection.clear()

        reloadTask = Task { [weak self] in
            guard let self else { return }
            let start = CFAbsoluteTimeGetCurrent()
            await self.syncRemoteDataIfNeeded()
            guard !Task.isCancelled else { return }
            self.refreshRowLookup()
            self.rebuildSections()
            let elapsed = CFAbsoluteTimeGetCurrent() - start
            homeLog.info("[HomeSync] handleConnectionChange: done, \(String(format: "%.3f", elapsed))s")
            self.onChange?(.connection)
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

    private func refreshAllAndNotify() {
        refreshRowLookup()
        rebuildSections()
        onChange?(.structural)
    }
}
