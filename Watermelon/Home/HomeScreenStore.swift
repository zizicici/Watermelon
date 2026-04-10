import Foundation

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
    private var reloadTask: Task<Void, Never>?

    // MARK: - Init

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.dataManager = HomeIncrementalDataManager(
            photoLibraryService: dependencies.photoLibraryService,
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: dependencies.databaseManager)
        )
        self.connectionController = HomeConnectionController(dependencies: dependencies)
        self.executionCoordinator = HomeExecutionCoordinator(
            dependencies: dependencies,
            homeDataManager: dataManager
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
        connectionController.onMonthSynced = { [weak self] in
            self?.syncRemoteDataIfNeeded()
            self?.refreshAllAndNotify()
        }
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
            self.syncRemoteDataIfNeeded()
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

    func progressPercent(for month: LibraryMonthKey) -> Double? {
        let row = rowLookup[month]
        let direction = selection.arrowDirection(for: month)
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

        if wasExecutionActive && !isNowActive {
            // Execution ended
            selection.clear()
            wasExecutionActive = false

            reloadTask?.cancel()
            reloadTask = Task { [weak self] in
                guard let self else { return }
                await self.dataManager.reloadLocalIndex()
                self.syncRemoteDataIfNeeded()
                self.refreshAllAndNotify()
            }

            onChange?(.structural)
            return
        }

        if !wasExecutionActive && isNowActive {
            wasExecutionActive = true
        }

        onChange?(.execution)
    }

    private func handleConnectionChange() {
        if !connectionState.isConnected {
            selection.clear()
        }
        syncRemoteDataIfNeeded()
        refreshRowLookup()
        rebuildSections()
        onChange?(.connection)
    }

    // MARK: - Data Refresh

    func syncRemoteDataIfNeeded() {
        let active = connectionState.isConnected
        let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(
            since: dataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        )
        dataManager.syncRemoteSnapshot(state: snapshotState, hasActiveConnection: active)
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
