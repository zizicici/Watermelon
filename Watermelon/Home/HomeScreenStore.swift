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
    // Lazily binds hooks after stored properties are initialized.
    private lazy var monthGroupingTimeZoneChangeObserver = makeMonthGroupingTimeZoneChangeObserver()
    private lazy var localIndexReloadCoordinator = makeLocalIndexReloadCoordinator()
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
    var isLocalIndexReloading: Bool {
        scopeController.isReloading
            || localIndexReloadCoordinator.isReloading
    }
    // Excludes reloads deferred behind execution/maintenance — nothing is scanning then.
    var isLocalIndexReloadUnderway: Bool { refreshScheduler.hasQueuedOrRunningReloadLocal }

    var connectionState: ConnectionState { connectionController.state }
    var remoteSyncProgress: RemoteSyncProgress? { connectionController.syncProgress }
    var executionState: HomeExecutionState? { executionCoordinator.currentState }
    var isExecutionActive: Bool { executionState != nil || dependencies.appRuntimeFlags.isExecuting }

    private(set) var isRemoteMaintenanceActive: Bool = false
    private var isBrowserLinkPairingActive = false
    private var isBrowserLinkTransportActive = false
    private var isBootstrapReadyForAutoConnect = false

    // False between connect/switch and the sync that applies the new connection's snapshot.
    private(set) var isRemoteReady: Bool = false

    var isSelectable: Bool {
        connectionState.isConnected
            && localPhotoAccessState.isAuthorized
            && !isExecutionActive
            && !isLocalIndexReloading
            && !isMaintenanceBlocked
    }

    var canChangeLocalSource: Bool {
        !isExecutionActive && !isLocalIndexReloading && !isMaintenanceBlocked
    }

    var canInteractWithRemoteNode: Bool {
        !isExecutionActive && !isMaintenanceBlocked
    }

    var isMaintenanceBlocked: Bool {
        isRemoteMaintenanceActive || dependencies.remoteMaintenanceController.isBusy
    }

    var isRemoteSelectionAllowed: Bool {
        !localLibraryScope.isSpecificAlbums
    }

    var savedProfiles: [ServerProfileRecord] { connectionController.savedProfiles }

    func setBrowserLinkSessionActive(_ isActive: Bool) {
        guard isBrowserLinkPairingActive != isActive else { return }
        isBrowserLinkPairingActive = isActive
    }

    func setBrowserLinkTransportActive(_ isActive: Bool) {
        guard isBrowserLinkTransportActive != isActive else { return }
        isBrowserLinkTransportActive = isActive
    }

    private func attemptAutoConnectIfAllowed() {
        guard isBootstrapReadyForAutoConnect,
              !isBrowserLinkPairingActive,
              !isBrowserLinkTransportActive else { return }
        connectionController.attemptAutoConnect()
    }

    func reachability(for profileID: Int64) -> ProfileReachabilityService.Reachability {
        dependencies.profileReachabilityService.reachability(for: profileID)
    }

    // MARK: - Notification

    var onChange: ((HomeChangeKind) -> Void)?
    var onAlert: ((String, String) -> Void)?
    var onDisconnecting: (() -> Void)?
    var onNeedsPasswordPrompt: ((ServerProfileRecord, _ completion: @escaping (String) -> Void) -> Void)?
    var onNeedsSFTPHostKeyTrust: ((ServerProfileRecord, SFTPHostKeyPromptPolicy.Decision, String) async -> Bool)?
    var onConnectFailed: ((ServerProfileRecord, Error) -> Void)?

    // MARK: - Private

    private var wasExecutionActive = false
    // A remote-snapshot change (e.g. a browser delete) can arrive while an execution/maintenance owns the remote
    // view — the browser delete even posts it while still holding the execution lease. Remember it and flush the
    // sync once the blocker clears, so Home's remote rows/counts don't stay stale until the next sync.
    private var pendingRemoteSnapshotRefresh = false
    private var lastMonthPhases: [LibraryMonthKey: MonthPlan.Phase] = [:]
    private var bootstrapTask: Task<Void, Never>?
    private var notificationObservers: [NSObjectProtocol] = []
    private var indexChangeObserverID: UUID?
    // Global bg-run watermarks, baselined at init (cold-launch load already read the DB), never re-baselined
    // on connect (that would risk suppressing a reload when a same-profile bg run raced the connect scan).
    // Remote advances only after a successful reload (skipped/failed → retries next foreground). Local advances
    // on detection: a transient DB-read failure is a non-destructive no-op (empty-guard in refreshFingerprintsFromDB),
    // not auto-retried for that run but healed by the next reload / PHChange / bg run.
    private var lastBackgroundRunWatermark: Date?
    private var remoteReloadWatermark: Date?

    // MARK: - Init

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.isRemoteMaintenanceActive = dependencies.remoteMaintenanceController.isBusy
        let backgroundRunBaseline = Self.latestBackgroundRun(dependencies.databaseManager)
        self.lastBackgroundRunWatermark = backgroundRunBaseline
        self.remoteReloadWatermark = backgroundRunBaseline
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
                localMonthGroupingTimeZone: { [dataManager] in dataManager.monthGroupingTimeZoneForLocalIndex() },
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
        _ = monthGroupingTimeZoneChangeObserver
        _ = localIndexReloadCoordinator
        observeExecutionLifecycle()
        observeMaintenance()
        observeRemoteSnapshotChange()
        observeBackgroundRunMarkers()
        observeLocalIndexChanges()
    }

    // The background runner lives in a separate container; its marker write can land after the one activation
    // pickup already ran. Re-run pickup on that signal so an active foreground Home reconciles promptly.
    private func observeBackgroundRunMarkers() {
        observeNotification(.BackgroundBackupRunMarkerDidChange) { [weak self] in
            self?.pickUpBackgroundFingerprintsIfNeeded()
        }
    }

    private func observeLocalIndexChanges() {
        indexChangeObserverID = dependencies.localIndexChangePublisher.addObserver { [weak self] change in
            Task { @MainActor [weak self] in
                self?.handleLocalIndexChange(change)
            }
        }
    }

    private func observeExecutionLifecycle() {
        observeNotification(.ExecutionLifecycleDidChange) { [weak self] in
            self?.handleRuntimeExecutionLifecycleChange()
        }
    }

    private func handleLocalIndexChange(_ change: LocalIndexChangePublisher.Change) {
        switch change {
        case .touched(let assetIDs):
            Task { [weak self] in
                guard let self else { return }
                let reconciledMonths = await self.dataManager.refreshLocalIndex(forAssetIDs: assetIDs)
                guard !reconciledMonths.isEmpty else { return }
                self.handleDataChange(reconciledMonths)
            }
        case .bulkInvalidation:
            scheduleRefresh([.reloadLocal, .notifyStructural])
        }
    }

    private func observeMaintenance() {
        observeNotification(.RemoteMaintenanceDidChange) { [weak self] in
            self?.handleRemoteMaintenanceChange()
        }
    }

    // A browser-initiated remote delete (or any out-of-band snapshot mutation) posts this AFTER updating the
    // shared cache. Home isn't otherwise told, so its remote rows/counts would stay stale until the next reload.
    private func observeRemoteSnapshotChange() {
        observeNotification(.RemoteLibrarySnapshotDidChange) { [weak self] in
            self?.handleRemoteSnapshotChange()
        }
    }

    private func handleRemoteSnapshotChange() {
        // Disconnected: irrelevant (no remote view; reconnect reconciles). Otherwise re-sync from the
        // already-updated cache (no network) — but if an execution/maintenance currently owns the remote view
        // (the browser delete posts this while still holding the execution lease), defer to when it clears
        // instead of silently dropping the refresh.
        guard connectionState.isConnected else { return }
        guard !isExecutionActive, !isMaintenanceBlocked else {
            pendingRemoteSnapshotRefresh = true
            return
        }
        scheduleRefresh([.syncRemote, .notifyStructural])
    }

    // Ordering-independent: `exitExecution` clears the runtime flag BEFORE it posts the lifecycle notification, so
    // whichever handler runs first, the refresh still happens — either the snapshot handler sets the latch and
    // this flush clears it, or the snapshot handler runs after the flag cleared and schedules directly.
    private func flushPendingRemoteSnapshotRefreshIfNeeded() {
        guard pendingRemoteSnapshotRefresh, !isExecutionActive, !isMaintenanceBlocked, connectionState.isConnected else { return }
        pendingRemoteSnapshotRefresh = false
        scheduleRefresh([.syncRemote, .notifyStructural])
    }

    private func handleRemoteMaintenanceChange() {
        let active = dependencies.remoteMaintenanceController.isBusy
        let changed = isRemoteMaintenanceActive != active
        isRemoteMaintenanceActive = active

        guard !active else {
            if changed { onChange?(.structural) }
            return
        }

        localIndexReloadCoordinator.replayIfPossible()
        // Maintenance just ended — flush a remote-snapshot refresh deferred while it was active (coalesces with the sync below).
        flushPendingRemoteSnapshotRefreshIfNeeded()
        guard changed else { return }
        // Sync first: verify's `replaceMonth` won't reach the grid until HomeIncrementalDataManager pulls the new revision.
        scheduleRefresh([.syncRemote, .notifyStructural])
        // Catch up a bg run whose remote reload was deferred while maintenance was active.
        pickUpBackgroundFingerprintsIfNeeded()
    }

    private func observeNotification(_ name: Notification.Name, handler: @escaping @MainActor () -> Void) {
        let observer = NotificationCenter.default.addObserver(
            forName: name,
            object: nil,
            queue: .main
        ) { _ in
            MainActor.assumeIsolated {
                handler()
            }
        }
        notificationObservers.append(observer)
    }

    private func makeMonthGroupingTimeZoneChangeObserver() -> HomeMonthGroupingTimeZoneChangeObserver {
        HomeMonthGroupingTimeZoneChangeObserver(hooks: .init(
            requestLocalIndexReload: { [weak self] in
                guard let self else { return }
                self.localIndexReloadCoordinator.schedule(
                    [.reloadLocal, .notifyStructural],
                    onEnqueued: { [weak self] in
                        guard let self else { return }
                        self.selectionController.clear()
                        self.onChange?(.structural)
                    }
                )
            }
        ))
    }

    private func makeLocalIndexReloadCoordinator() -> HomeLocalIndexReloadCoordinator {
        HomeLocalIndexReloadCoordinator(hooks: .init(
            isBlocked: { [weak self] in
                self?.isLocalIndexReloadBlocked ?? false
            },
            hasQueuedOrRunningReload: { [weak self] in
                self?.refreshScheduler.hasQueuedOrRunningReloadLocal ?? false
            },
            enqueue: { [weak self] work in
                self?.refreshScheduler.enqueue(work)
            },
            notifyAvailabilityChanged: { [weak self] in
                self?.onChange?(.structural)
            }
        ))
    }

    private func makeRefreshScheduler() -> HomeRefreshScheduler {
        HomeRefreshScheduler(hooks: HomeRefreshScheduler.Hooks(
            normalizeBeforeReload: { [weak self] in
                self?.normalizeLocalLibraryScopeIfNeeded(shouldAlert: true) ?? false
            },
            deferIfNeeded: { [weak self] work in
                self?.localIndexReloadCoordinator.deferIfBlocked(work) ?? false
            },
            reloadLocal: { [weak self] in
                _ = await self?.dataManager.reloadLocalIndex()
            },
            refreshFingerprints: { [weak self] in
                _ = await self?.dataManager.refreshLocalFingerprints()
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
                self.sectionBuilder.rebuildAll()
                // handleDataChange only reconciles selection on local changes; remote-sync/reload rebuilds (maintenance verify, bg-run reload) can evict a selected month or one of its sides too.
                self.reconcileSelectionToVisibleSides()
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
            isRemoteReady: { [weak self] in self?.isRemoteReady ?? false },
            sections: { [weak self] in self?.sections ?? [] }
        ))
    }

    deinit {
        bootstrapTask?.cancel()
        for observer in notificationObservers {
            NotificationCenter.default.removeObserver(observer)
        }
        if let id = indexChangeObserverID {
            dependencies.localIndexChangePublisher.removeObserver(id)
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
        connectionController.onNeedsSFTPHostKeyTrust = { [weak self] profile, decision, actual in
            guard let self, let confirm = self.onNeedsSFTPHostKeyTrust else { return false }
            return await confirm(profile, decision, actual)
        }
        connectionController.onConnectFailed = { [weak self] profile, error in
            self?.onConnectFailed?(profile, error)
        }
        dependencies.profileReachabilityService.onChange = { [weak self] in
            self?.scheduleRefresh([.notifyConnection])
        }
    }

    private func pushProfilesToReachabilityService() {
        if connectionState.activeProfile?.isBrowserLinkProfile == true {
            dependencies.profileReachabilityService.setProfiles([], activeProfileID: nil)
            return
        }
        dependencies.profileReachabilityService.setProfiles(
            connectionController.savedProfiles,
            activeProfileID: connectionState.activeProfile?.id
        )
    }

    // MARK: - Lifecycle

    func load() {
        connectionController.loadProfiles()
        pushProfilesToReachabilityService()
        bootstrapTask?.cancel()
        isBootstrapReadyForAutoConnect = false
        bootstrapTask = Task { [weak self] in
            guard let self else { return }
            if !self.localIndexReloadCoordinator.deferIfBlocked([.reloadLocal, .notifyStructural]) {
                await self.dataManager.ensureLocalIndexLoaded()
            }
            guard !Task.isCancelled else { return }
            _ = self.refreshLocalPhotoAccessState()
            self.isBootstrapReadyForAutoConnect = true
            self.attemptAutoConnectIfAllowed()
            // Don't call syncRemoteDataIfNeeded here — if auto-connect is in progress,
            // connectionState is .connecting and hasActiveConnection would be false,
            // which would clear remote data and advance revision, corrupting the bootstrap.
            // Connection callbacks handle sync when ready.
            self.refreshAllAndNotify()
        }
    }

    func reloadProfiles() {
        connectionController.loadProfiles()
        pushProfilesToReachabilityService()
        onChange?(.connection)
    }

    func setLocalLibraryScope(_ scope: HomeLocalLibraryScope, descriptors: [LocalAlbumDescriptor] = []) {
        guard !isExecutionActive,
              !isLocalIndexReloading,
              !isMaintenanceBlocked else { return }
        for descriptor in descriptors {
            albumDisplayCache[descriptor.localIdentifier] = descriptor
        }
        let normalized = scopeNormalizer.normalize(scope)
        switch scopeController.setActive(normalized.scope, isExecuting: false) {
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

    func startExecution(backup: [LibraryMonthKey], download: [LibraryMonthKey], complement: [LibraryMonthKey], incompletePolicy: IncompleteDownloadPolicy = .skip) {
        guard !isExecutionActive, !isLocalIndexReloading else { return }
        guard !rejectIfMaintaining() else { return }
        guard !rejectIfLocalIndexBuilding() else { return }
        // The confirm dialog captured these months before it opened; a reconcile/clear meanwhile may have
        // dropped a side's truth, so re-validate against the live selection before entering execution.
        let live = selection.revalidated(backup: backup, download: download, complement: complement)
        guard !live.backup.isEmpty || !live.download.isEmpty || !live.complement.isEmpty else { return }
        executionCoordinator.enter(backup: live.backup, download: live.download, complement: live.complement, incompletePolicy: incompletePolicy)
    }

    // Total incomplete remote records across the given download/complement months — lets the UI prompt once,
    // upfront, before starting a restore that would otherwise silently skip them.
    func incompleteDownloadItemCount(download: [LibraryMonthKey], complement: [LibraryMonthKey]) async -> Int {
        var count = 0
        for month in Set(download).union(complement) {
            count += await dataManager.remoteOnlyItems(for: month).lazy.filter(\.isIncomplete).count
        }
        return count
    }

    func pauseExecution() { executionCoordinator.pause() }
    func resumeExecution() { executionCoordinator.resume() }
    func stopExecution() { executionCoordinator.stop() }
    func exitExecution() { executionCoordinator.exit() }

    // MARK: - Connection Actions

    func connectProfile(_ profile: ServerProfileRecord) {
        guard !isExecutionActive else { return }
        guard !rejectIfMaintaining() else { return }
        guard !isBrowserLinkPairingActive, !isBrowserLinkTransportActive else { return }
        guard connectionState.activeProfile?.isBrowserLinkProfile != true else { return }
        connectionController.promptAndConnect(profile: profile)
    }

    func disconnect() {
        guard !isExecutionActive else { return }
        guard !rejectIfMaintaining() else { return }
        let wasBrowserLink = connectionState.activeProfile?.isBrowserLinkProfile == true
        onDisconnecting?()
        if wasBrowserLink {
            connectionController.disconnectBrowserLink()
        } else {
            connectionController.disconnect()
        }
    }

    func connectBrowserLink(
        profile: ServerProfileRecord,
        completion: @escaping (Result<Void, Error>) -> Void
    ) {
        connectionController.connectBrowserLink(profile: profile, completion: completion)
    }

    func cancelBrowserLinkConnection(sessionID: String) {
        connectionController.cancelBrowserLinkConnection(sessionID: sessionID)
    }

    func browserLinkTransportDidClose(message: String) {
        let hadRunningExecution = executionCoordinator.isRunning
        if hadRunningExecution {
            executionCoordinator.failForMissingConnection(message: message)
        }
        setBrowserLinkTransportActive(false)
        if !hadRunningExecution {
            onAlert?(
                String(localized: "common.error"),
                message
            )
        }
    }

    private func rejectIfMaintaining() -> Bool {
        guard isMaintenanceBlocked else { return false }
        onAlert?(
            String(localized: "common.error"),
            String(localized: "home.alert.maintenanceInProgress")
        )
        return true
    }

    private func rejectIfLocalIndexBuilding() -> Bool {
        guard dependencies.localIndexBuildCoordinator.isRunning else { return false }
        onAlert?(
            String(localized: "common.error"),
            String(localized: "home.alert.localIndexBuildInProgress")
        )
        return true
    }

    func requestLocalPhotoAccessIfNeeded() {
        Task { [weak self] in
            guard let self else { return }

            let access = LocalPhotoAccessState(authorizationStatus: self.photoAccessGate.currentSystemAuthorizationStatus())
            switch access {
            case .authorized:
                self.scheduleRefresh([.reloadLocal, .notifyStructural], onEnqueued: { [weak self] in
                    self?.onChange?(.structural)
                })
            case .notDetermined:
                _ = await self.photoAccessGate.requestAuthorization()
                guard !Task.isCancelled else { return }
                // Begin-time .structural lets the overlay show its scanning state during the first index load.
                self.scheduleRefresh([.reloadLocal, .notifyStructural], onEnqueued: { [weak self] in
                    self?.onChange?(.structural)
                })
            case .denied:
                break
            }
        }
    }

    func refreshLocalPhotoAccessIfNeeded() {
        let scopeChanged = normalizeLocalLibraryScopeIfNeeded(shouldAlert: true)
        let accessChanged = photoAccessGate.hasSystemStateDiverged()
        let timeZoneChanged = dataManager.monthGroupingTimeZoneForLocalIndex() != .frozenCurrent()
        if scopeChanged || accessChanged || timeZoneChanged {
            scheduleRefresh([.reloadLocal, .notifyStructural], onEnqueued: { [weak self] in
                self?.onChange?(.structural)
            })
        }
        pickUpBackgroundFingerprintsIfNeeded()
    }

    /// On foreground, re-read DB fingerprints and reload the connected remote when a background backup (which
    /// runs in a separate DependencyContainer) advanced the global bg-run watermark since our last pickup.
    private func pickUpBackgroundFingerprintsIfNeeded() {
        guard let current = Self.latestBackgroundRun(dependencies.databaseManager) else { return }
        if current > (lastBackgroundRunWatermark ?? .distantPast) {
            lastBackgroundRunWatermark = current
            scheduleRefresh([.refreshFingerprints, .notifyStructural])
        }

        guard !isExecutionActive, !isMaintenanceBlocked, connectionState.isConnected,
              current > (remoteReloadWatermark ?? .distantPast) else { return }
        connectionController.refreshActiveRemoteIndex { [weak self] in
            guard let self, !self.isExecutionActive, !self.isMaintenanceBlocked else { return }
            self.remoteReloadWatermark = current  // advance only after a successful reload
            self.scheduleRefresh([.syncRemote, .notifyStructural])
        }
    }

    private static func latestBackgroundRun(_ db: DatabaseManager) -> Date? {
        // All saved profiles, not just background-enabled ones: a run that already mutated a profile stamps its
        // marker gated by destination identity (not the enabled flag), so disabling background backup mid-run
        // must not hide that completed run from foreground pickup. Future-run eligibility is unaffected.
        guard let profiles = try? db.fetchServerProfiles() else { return nil }
        return profiles.compactMap { $0.id.flatMap { Self.latestBackgroundRun(db, profileID: $0) } }.max()
    }

    private static func latestBackgroundRun(_ db: DatabaseManager, profileID: Int64) -> Date? {
        let ran = try? db.backgroundBackupLastRanAt(profileID: profileID)
        let completed = try? db.backgroundBackupLastCompletedAt(profileID: profileID)
        return [ran, completed].compactMap { $0 }.max()
    }

    #if DEBUG
    static func _testLatestBackgroundRun(_ db: DatabaseManager) -> Date? {
        latestBackgroundRun(db)
    }
    #endif

    // MARK: - Derived State

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        executionState?.intent(for: month) ?? selection.intent(for: month)
    }

    func progressPercent(for month: LibraryMonthKey) -> Double? {
        let row = rowLookup[month]
        let monthIntent = intent(for: month)
        let matched = row?.local?.backedUpCount ?? 0

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
        // A queued reload's postProcess subsumes this rebuild. Not isLocalIndexReloading: that is also
        // true while a reload is deferred behind an execution, which must keep row updates flowing.
        guard !refreshScheduler.hasQueuedOrRunningReloadLocal else { return }
        if normalizeLocalLibraryScopeIfNeeded(shouldAlert: true) {
            scheduleRefresh([.reloadLocal, .notifyStructural])
            return
        }

        let previousMonths = Set(sectionBuilder.rowLookup.keys)
        sectionBuilder.rebuildAll()
        let currentMonths = Set(sectionBuilder.rowLookup.keys)

        let selectionChanged = reconcileSelectionToVisibleSides()

        // A side eviction that keeps the month key (survives via the opposite side) leaves the key set
        // unchanged but trims the selection; emit .structural so the action panel and top toggles re-render
        // (the .data path refreshes neither).
        if previousMonths != currentMonths || selectionChanged {
            onChange?(.structural)
        } else {
            onChange?(.data(months))
        }
    }

    // Trim each selection side to months that still carry that side's summary, so a month surviving via the
    // opposite side can't keep a stale local/remote selection (which would execute as no-op backup/download).
    @discardableResult
    private func reconcileSelectionToVisibleSides() -> Bool {
        var localMonths = Set<LibraryMonthKey>()
        var remoteMonths = Set<LibraryMonthKey>()
        for (month, row) in sectionBuilder.rowLookup {
            if row.local != nil { localMonths.insert(month) }
            if row.remote != nil { remoteMonths.insert(month) }
        }
        return selectionController.intersect(localMonths: localMonths, remoteMonths: remoteMonths)
    }

    private func handleFileSizeChange(_ months: Set<LibraryMonthKey>) {
        guard !refreshScheduler.hasQueuedOrRunningReloadLocal else { return }
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

            applyDeferredScopeNormalizationIfNeeded(shouldAlert: true)
            localIndexReloadCoordinator.replayIfPossible()

            // This full refresh subsumes any snapshot change deferred during the run, so clear the latch too.
            pendingRemoteSnapshotRefresh = false
            scheduleRefresh([.reloadLocal, .syncRemote, .notifyStructural])
            // Catch up a bg run whose remote reload was deferred while execution was active.
            pickUpBackgroundFingerprintsIfNeeded()

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

    private func handleRuntimeExecutionLifecycleChange() {
        guard !dependencies.appRuntimeFlags.isExecuting else {
            onChange?(.structural)
            return
        }
        // Execution (incl. a browser delete's lease) just ended — flush a remote-snapshot refresh deferred while it held.
        flushPendingRemoteSnapshotRefreshIfNeeded()
        guard !executionCoordinator.isActive, !wasExecutionActive else { return }
        localIndexReloadCoordinator.replayIfPossible()
        pickUpBackgroundFingerprintsIfNeeded()
        onChange?(.structural)
    }

    private func handleConnectionChange() {
        let stateDesc: String
        switch connectionState {
        case .connected: stateDesc = "connected"
        case .connecting: stateDesc = "connecting"
        case .disconnected: stateDesc = "disconnected"
        }
        homeLog.info("[HomeSync] handleConnectionChange: state=\(stateDesc)")
        pushProfilesToReachabilityService()

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
            isRemoteReady = false
            scheduleRefresh([.notifyConnection])
        case .connected, .disconnected:
            // Pure remote sync; bg-backup fingerprint/remote pickup is owned by the foreground-activation handler.
            scheduleRefresh([.syncRemote, .notifyConnection])
        }
    }

    // MARK: - Data Refresh

    func syncRemoteDataIfNeeded() async {
        let start = CFAbsoluteTimeGetCurrent()
        let active = connectionState.isConnected
        let activeProfileIdentity = connectionState.activeProfile?.runtimeConnectionIdentity
        let revision = dataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(
            since: revision
        )
        await dataManager.syncRemoteSnapshotOnProcessingQueue(state: snapshotState, hasActiveConnection: active)
        // Only mark ready if this sync still matches the live connection — a switch may have landed
        // mid-await, and a stale prior-connection sync must not lift the overlay over the new remote.
        if active,
           connectionState.isConnected,
           connectionState.activeProfile?.runtimeConnectionIdentity == activeProfileIdentity {
            isRemoteReady = true
        }
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
        guard !isExecutionActive else {
            // Re-run scope normalization after the frozen execution window closes.
            scopeController.requestPostExecutionRenormalization()
            return false
        }

        if scopeController.pendingScope != nil {
            return applyDeferredScopeNormalizationIfNeeded(shouldAlert: shouldAlert)
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

    @discardableResult
    private func applyDeferredScopeNormalizationIfNeeded(shouldAlert: Bool) -> Bool {
        guard let pendingScope = scopeController.resumeFromDeferred() else { return false }
        let normalized = scopeNormalizer.normalize(pendingScope)
        let changed = scopeController.setActiveFromNormalize(normalized.scope)
        if changed {
            selectionController.clear()
            if shouldAlert, let alert = normalized.alert {
                scopeNormalizer.emitAlertIfNotDebounced(alert)
            }
        }
        return changed
    }

    private var isLocalIndexReloadBlocked: Bool {
        isExecutionActive || isMaintenanceBlocked
    }

    private func scheduleRefresh(
        _ work: HomeRefreshScheduler.Work,
        onEnqueued: (@MainActor () -> Void)? = nil
    ) {
        localIndexReloadCoordinator.schedule(work, onEnqueued: onEnqueued)
    }
}
