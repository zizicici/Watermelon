import AppKit
import Combine

@MainActor
final class MainWindowController: NSWindowController {
  private let container: MacDependencyContainer
  private let homeViewController: MacBackupHomeViewController
  private let remoteConnectionController: MacRemoteConnectionController
  private let backupExecutionController: MacBackupExecutionController
  private var auxiliaryWindowControllers: [NSWindowController] = []
  private weak var profileManagementWindowController: NSWindowController?
  private weak var settingsWindowController: NSWindowController?
  private weak var maintenanceWindowController: NSWindowController?
  private weak var logHistoryWindowController: NSWindowController?
  private weak var localIndexWindowController: NSWindowController?
  private weak var duplicatesWindowController: NSWindowController?
  private weak var onboardingWindow: NSWindow?
  private var autoConnectProfileID: Int64?
  private var selectedProfileID: Int64?
  private var cancellables = Set<AnyCancellable>()
  nonisolated(unsafe) private var executionLifecycleObserver: NSObjectProtocol?
  private var monthGroupingTimeZoneChangeObserver: HomeMonthGroupingTimeZoneChangeObserver?
  private var externalVolumeLifecycleMonitor: MacExternalVolumeLifecycleMonitor?

  private var isExecutionActive: Bool {
    container.appRuntimeFlags.isExecuting
  }

  private var hasTerminationBlockingActivity: Bool {
    MacExecutionTerminationPolicy.isBlocking(
      manualBackupState: backupExecutionController.state,
      runtimeExecuting:
        container.appRuntimeFlags.isExecuting
    )
  }

  init(container: MacDependencyContainer) {
    self.container = container
    let activeProfileID =
      (try? container.databaseManager
        .activeServerProfileID()) ?? nil
    self.autoConnectProfileID = activeProfileID
    self.selectedProfileID = activeProfileID
    let photoLibraryIndexController = MacPhotoLibraryIndexController(
      worker: container.photoLibraryMonthlyIndexWorker,
      localIndexBuildCoordinator:
        container.localIndexBuildCoordinator,
      localIndexChangePublisher:
        container.localIndexChangePublisher
    )
    let remoteConnectionController = MacRemoteConnectionController(
      databaseManager: container.databaseManager,
      keychainService: container.keychainService,
      appSession: container.appSession,
      appRuntimeFlags: container.appRuntimeFlags,
      profileStore: container.profileStore,
      connectionService: container.storageProfileConnectionService,
      remoteLibraryReadService: container.remoteLibraryReadService,
      backupCoordinator: container.backupCoordinator
    )
    self.remoteConnectionController = remoteConnectionController
    let backupExecutionController = MacBackupExecutionController(
      appSession: container.appSession,
      photoLibraryService: container.photoLibraryService,
      localHashIndexBuildService:
        container.localHashIndexBuildService,
      backupCoordinator: container.backupCoordinator,
      remoteLibraryReadService: container.remoteLibraryReadService,
      hashIndexRepository: container.hashIndexRepository,
      downloadWorkflowHelper: container.downloadWorkflowHelper,
      appRuntimeFlags: container.appRuntimeFlags
    )
    self.backupExecutionController = backupExecutionController
    self.homeViewController = MacBackupHomeViewController(
      photoLibraryAuthorizationProvider: container.photoLibraryAuthorizationProvider,
      photoLibraryService: container.photoLibraryService,
      photoLibraryIndexController: photoLibraryIndexController,
      remoteConnectionController: remoteConnectionController,
      appSession: container.appSession,
      profileReachabilityService:
        container.profileReachabilityService,
      backupExecutionController: backupExecutionController
    )

    let window = NSWindow(contentViewController: homeViewController)
    window.title = AppName.localized
    window.setContentSize(NSSize(width: 920, height: 780))
    window.minSize = NSSize(width: 760, height: 700)
    window.styleMask.insert(.fullSizeContentView)
    window.titlebarAppearsTransparent = false
    window.titleVisibility = .visible
    window.tabbingMode = .preferred
    window.setFrameAutosaveName("WatermelonBackupMainWindow")

    super.init(window: window)
    window.delegate = self
    shouldCascadeWindows = true
    monthGroupingTimeZoneChangeObserver =
      HomeMonthGroupingTimeZoneChangeObserver(
        hooks: .init(
          requestLocalIndexReload: { [weak self] in
            self?.homeViewController.reloadPhotoLibrary()
          }
        ))

    executionLifecycleObserver = NotificationCenter.default
      .addObserver(
        forName: .ExecutionLifecycleDidChange,
        object: nil,
        queue: .main
      ) { [weak self] _ in
        MainActor.assumeIsolated {
          self?.applyGlobalExecutionState()
        }
      }
    let externalVolumeLifecycleMonitor =
      MacExternalVolumeLifecycleMonitor(
        activeProfile: { [weak self] in
          self?.container.appSession.snapshot.activeProfile
        },
        disconnect: { [weak self] in
          self?.remoteConnectionController.disconnect()
        },
        refreshReachability: { [weak self] in
          self?.container.profileReachabilityService
            .sweep(force: true)
        }
      )
    self.externalVolumeLifecycleMonitor =
      externalVolumeLifecycleMonitor
    externalVolumeLifecycleMonitor.start()
    homeViewController.onConnectDestination = {
      [weak self] profile in
      self?.connectDestination(profile)
    }
    homeViewController.onConfigureDestination = {
      [weak self] profileID in
      self?.openProfileManagement(selecting: profileID)
    }
    remoteConnectionController.onSelectionReverted = {
      [weak self] profile in
      self?.restoreSelectedDestination(profile)
    }
    homeViewController.onManageDestinations = { [weak self] in
      self?.openProfileManagement()
    }
    homeViewController.onCreateDestination = {
      [weak self] type in
      self?.openProfileManagement(adding: type)
    }
    homeViewController.onOpenPhotoBrowser = {
      [weak self] request in
      self?.openPhotoBrowser(request: request)
    }
    homeViewController.onOpenExecutionLog = {
      [weak self] url in
      self?.openExecutionLogHistory(selecting: url)
    }
    homeViewController.onExecutionActivityChanged = {
      [weak self] _ in
      self?.applyGlobalExecutionState()
    }
    homeViewController
      .onMonthGroupingTimeZoneAvailabilityChange = {
        [weak self] in
        self?.refreshSettingsAvailability()
      }
    container.profileStore.$profiles
      .sink { [weak self] profiles in
        self?.applyDestinations(profiles)
      }
      .store(in: &cancellables)
    container.profileStore.$loadError
      .compactMap { $0 }
      .sink { [weak self] error in
        self?.presentProfileLoadError(error)
      }
      .store(in: &cancellables)
    #if DEBUG
      if ProcessInfo.processInfo.arguments.contains("--demo-executing") {
        backupExecutionController.showDemoProgress()
      }
    #endif
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  deinit {
    if let executionLifecycleObserver {
      NotificationCenter.default.removeObserver(
        executionLifecycleObserver
      )
    }
  }

  private func applyDestinations(
    _ profiles: [ServerProfileRecord]
  ) {
    #if DEBUG
      let displayedProfiles =
        ProcessInfo.processInfo.arguments.contains(
          "--demo-no-destination"
        ) ? [] : profiles
    #else
      let displayedProfiles = profiles
    #endif
    let selectedProfile =
      selectedProfileID.flatMap { profileID in
        displayedProfiles.first { $0.id == profileID }
      } ?? displayedProfiles.first
    selectedProfileID = selectedProfile?.id
    homeViewController.setDestinations(
      displayedProfiles,
      selectedProfileID: selectedProfileID
    )
    applySelectedDestination(selectedProfile)
  }

  private func restoreSelectedDestination(
    _ profile: ServerProfileRecord
  ) {
    selectedProfileID = profile.id
    homeViewController.setDestinations(
      container.profileStore.profiles,
      selectedProfileID: profile.id
    )
    homeViewController.show(profile: profile)
  }

  private func connectDestination(_ profile: ServerProfileRecord) {
    selectedProfileID = profile.id
    homeViewController.setDestinations(
      container.profileStore.profiles,
      selectedProfileID: profile.id
    )
    homeViewController.show(profile: profile)
    remoteConnectionController.connect(profile: profile)
  }

  private func applySelectedDestination(
    _ profile: ServerProfileRecord?
  ) {
    homeViewController.show(profile: profile)
    remoteConnectionController.select(profile: profile)
    guard let profile,
      profile.id == autoConnectProfileID
    else {
      autoConnectProfileID = nil
      return
    }
    autoConnectProfileID = nil
    #if DEBUG
      guard
        !MacDemoPhotoLibraryPolicy.usesSyntheticLibrary(
          arguments: ProcessInfo.processInfo.arguments
        )
      else { return }
    #endif
    remoteConnectionController.attemptAutoConnect(profile: profile)
  }

  private func presentProfileLoadError(_ error: Error) {
    guard let window else { return }
    let alert = NSAlert(error: error)
    alert.beginSheetModal(for: window) {
      [weak self] _ in
      self?.container.profileStore.loadError = nil
    }
  }

  private func applyGlobalExecutionState() {
    let executionActive = isExecutionActive
    container.localIndexBuildCoordinator
      .handleExecutionLifecycleChange()
    homeViewController.setDestinationInteractionEnabled(
      !executionActive
    )
    homeViewController.setExternalExecutionActive(
      executionActive
        && !backupExecutionController.state.isActive
    )
    refreshSettingsAvailability()
  }

  private func refreshSettingsAvailability() {
    guard
      let settings =
        settingsWindowController?.window?
        .contentViewController
        as? MacSettingsViewController
    else {
      return
    }
    settings.refreshAvailability()
  }

  override func showWindow(_ sender: Any?) {
    super.showWindow(sender)
    window?.makeKeyAndOrderFront(sender)
  }

  func presentOnboarding(isFirstLaunch: Bool) {
    guard let parentWindow = window else { return }
    if let onboardingWindow {
      onboardingWindow.makeKeyAndOrderFront(nil)
      return
    }
    let onboarding = MacOnboardingViewController(
      isFirstLaunch: isFirstLaunch
    )
    let sheet = NSWindow(contentViewController: onboarding)
    sheet.title = String(
      localized: "mac.menu.welcome",
      defaultValue: "Welcome to Watermelon Backup"
    )
    sheet.titleVisibility = .hidden
    sheet.setContentSize(NSSize(width: 660, height: 590))
    sheet.styleMask = [.titled]
    sheet.isMovable = false
    sheet.standardWindowButton(.closeButton)?.isHidden = true
    onboardingWindow = sheet

    let finish: () -> Void = { [weak self, weak sheet] in
      guard let self, let sheet else { return }
      MacOnboardingCompletionGate.markCompleted()
      parentWindow.endSheet(sheet)
      self.onboardingWindow = nil
    }
    onboarding.onContinue = {
      finish()
    }
    parentWindow.beginSheet(sheet)
  }

  #if DEBUG
    func showDemoAlbumPicker() {
      homeViewController.showDemoAlbumPicker()
    }
  #endif

  func openProfileManagement() {
    openProfileManagement(adding: nil, selecting: nil)
  }

  private func openProfileManagement(selecting profileID: Int64) {
    openProfileManagement(adding: nil, selecting: profileID)
  }

  private func openProfileManagement(adding storageType: StorageType?) {
    openProfileManagement(adding: storageType, selecting: nil)
  }

  private func openProfileManagement(
    adding storageType: StorageType?,
    selecting profileID: Int64?
  ) {
    if storageType != nil,
      isExecutionActive
    {
      return
    }
    if let controller = profileManagementWindowController,
      controller.window?.isVisible == true
    {
      controller.window?.makeKeyAndOrderFront(nil)
      if controller.window?.attachedSheet == nil,
        let manager = controller.window?
          .contentViewController
          as? MacProfileManagementViewController
      {
        if let storageType {
          beginAddingDestination(
            storageType,
            in: manager,
            windowController: controller
          )
        } else if let profileID {
          manager.selectProfile(id: profileID)
        }
      }
      return
    }
    let manager = MacProfileManagementViewController(
      store: container.profileStore,
      storageClientFactory: container.storageClientFactory,
      databaseManager: container.databaseManager,
      appSession: container.appSession,
      appRuntimeFlags: container.appRuntimeFlags,
      isConnectionActive: {
        [weak remoteConnectionController] in
        remoteConnectionController?.state.isConnecting
          ?? false
      },
      remoteMaintenanceController:
        container.remoteMaintenanceController,
      backupCoordinator: container.backupCoordinator,
      remoteThumbnailMaintenanceService:
        container.remoteThumbnailMaintenanceService,
      oneDriveProfileSetupCoordinator:
        container.oneDriveProfileSetupCoordinator,
      selectedProfileID: profileID
    )
    manager.onOpenLegacyMigration = { [weak self] profile in
      self?.openLegacyMigration(for: profile)
    }
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "more.item.manageStorage",
        defaultValue: "Manage Nodes"
      ),
      contentViewController: manager,
      size: NSSize(width: 900, height: 560)
    )
    profileManagementWindowController = controller
    retainAndShow(controller)
    if let storageType {
      DispatchQueue.main.async { [weak self] in
        self?.beginAddingDestination(
          storageType,
          in: manager,
          windowController: controller
        )
      }
    }
    #if DEBUG
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-local-profile"
      ) {
        DispatchQueue.main.async {
          manager.beginAdding(.externalVolume)
        }
      }
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-smb-profile"
      ) {
        DispatchQueue.main.async {
          manager.beginAdding(.smb)
        }
      }
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-webdav-profile"
      ) {
        DispatchQueue.main.async {
          manager.beginAdding(.webdav)
        }
      }
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-s3-profile"
      ) {
        DispatchQueue.main.async {
          manager.beginAdding(.s3)
        }
      }
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-sftp-profile"
      ) {
        DispatchQueue.main.async {
          manager.showDemoSFTPProfileSheet()
        }
      }
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-onedrive-profile"
      ) {
        DispatchQueue.main.async {
          manager.showDemoOneDriveProfileSheet()
        }
      }
    #endif
  }

  private func beginAddingDestination(
    _ storageType: StorageType,
    in manager: MacProfileManagementViewController,
    windowController: NSWindowController
  ) {
    manager.beginAdding(storageType) {
      [weak self, weak windowController] profile in
      DispatchQueue.main.async {
        self?.connectDestination(profile)
        windowController?.close()
        self?.showWindow(nil)
      }
    }
  }

  func openSettings() {
    if let settingsWindowController,
      settingsWindowController.window?.isVisible == true
    {
      settingsWindowController.window?.makeKeyAndOrderFront(nil)
      refreshSettingsAvailability()
      return
    }
    let settings = MacSettingsViewController(
      canChangeMonthGroupingTimeZone: {
        [weak self] in
        guard let self else { return false }
        return !self.isExecutionActive
          && self.homeViewController
            .canChangeMonthGroupingTimeZone
      },
      openLocalPhotoBrowser: { [weak self] in
        self?.openPhotoBrowser(
          request: MacPhotoBrowserRequest(
            initialMonth: nil,
            initialSide: .local,
            localQuery: .allAssets,
            title: String(
              localized: "home.photoLibrary"
            ),
            monthGroupingTimeZone:
              .frozenCurrent()
          )
        )
      },
      openProfileManagement: { [weak self] in
        self?.openProfileManagement()
      }
    )
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "controller.more.title",
        defaultValue: "Settings"
      ),
      contentViewController: settings,
      size: NSSize(width: 680, height: 420)
    )
    controller.window?.styleMask.remove(.resizable)
    controller.window?.minSize = NSSize(width: 680, height: 420)
    controller.window?.maxSize = NSSize(width: 680, height: 420)
    controller.window?.toolbarStyle = .preference
    controller.window?.tabbingMode = .disallowed
    settingsWindowController = controller
    retainAndShow(controller)
    #if DEBUG
      if ProcessInfo.processInfo.arguments.contains("--demo-timezone") {
        DispatchQueue.main.async {
          settings.showDemoTimeZonePicker()
        }
      }
    #endif
  }

  func openExecutionLogHistory() {
    openExecutionLogHistory(selecting: nil)
  }

  func openCurrentExecutionLog() {
    guard
      let url = backupExecutionController
        .currentSessionLogURL
    else {
      return
    }
    openExecutionLogHistory(selecting: url)
  }

  private func openExecutionLogHistory(selecting sessionURL: URL?) {
    if let logHistoryWindowController,
      logHistoryWindowController.window?.isVisible == true
    {
      if let sessionURL,
        let history = logHistoryWindowController
          .contentViewController
          as? MacExecutionLogHistoryViewController
      {
        history.selectSession(url: sessionURL)
      }
      logHistoryWindowController.window?.makeKeyAndOrderFront(nil)
      return
    }
    let executionController = backupExecutionController
    let history = MacExecutionLogHistoryViewController(
      preferredSessionURL: sessionURL,
      activeSessionURLProvider: {
        [weak executionController] in
        executionController?.activeSessionLogURL
      },
      liveSnapshotProvider: {
        [weak executionController] in
        executionController?.currentLogLiveSnapshot
      }
    )
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "log.history.title",
        defaultValue: "Execution Log History"
      ),
      contentViewController: history,
      size: NSSize(width: 1_000, height: 640)
    )
    controller.window?.minSize = NSSize(width: 820, height: 520)
    logHistoryWindowController = controller
    retainAndShow(controller)
  }

  func isToolsMenuCommandEnabled(
    _ command: MacToolsMenuCommand
  ) -> Bool {
    let hasVisibleWindow: Bool
    switch command {
    case .manageProfiles:
      hasVisibleWindow =
        profileManagementWindowController?
        .window?.isVisible == true
    case .localIndex:
      hasVisibleWindow =
        localIndexWindowController?.window?.isVisible == true
    case .duplicates:
      hasVisibleWindow =
        duplicatesWindowController?.window?.isVisible == true
    case .repositoryMaintenance:
      hasVisibleWindow =
        maintenanceWindowController?.window?.isVisible == true
    case .logs:
      hasVisibleWindow =
        logHistoryWindowController?.window?.isVisible == true
    }
    return MacToolsMenuAvailabilityPolicy.isEnabled(
      command: command,
      executionActive:
        isExecutionActive,
      hasVisibleWindow: hasVisibleWindow
    )
  }

  func openLocalIndex() {
    if let localIndexWindowController,
      localIndexWindowController.window?.isVisible == true
    {
      localIndexWindowController.window?
        .makeKeyAndOrderFront(nil)
      return
    }
    guard !isExecutionActive else { return }
    let localIndex = MacLocalIndexViewController(
      coordinator: container.localIndexBuildCoordinator,
      photoLibraryService: container.photoLibraryService,
      hashIndexRepository: container.hashIndexRepository,
      appRuntimeFlags: container.appRuntimeFlags,
      isExecutionActive: { [weak self] in
        self?.isExecutionActive ?? true
      }
    )
    localIndex.onIndexChanged = { [weak self] in
      self?.homeViewController.reloadPhotoLibrary()
    }
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "home.localIndex.title",
        defaultValue: "Local Photo Index"
      ),
      contentViewController: localIndex,
      size: NSSize(width: 620, height: 320)
    )
    controller.window?.minSize = NSSize(
      width: 580,
      height: 290
    )
    localIndexWindowController = controller
    retainAndShow(controller)
  }

  func openDuplicates() {
    if let duplicatesWindowController,
      duplicatesWindowController.window?.isVisible == true
    {
      duplicatesWindowController.window?
        .makeKeyAndOrderFront(nil)
      return
    }
    guard !isExecutionActive else { return }
    let duplicates = MacDuplicatesViewController(
      coordinator: container.localIndexBuildCoordinator,
      hashIndexRepository: container.hashIndexRepository,
      photoLibraryService: container.photoLibraryService,
      changePublisher: container.localIndexChangePublisher,
      appRuntimeFlags: container.appRuntimeFlags,
      isExecutionActive: { [weak self] in
        self?.isExecutionActive ?? true
      }
    )
    duplicates.onOpenLocalIndex = { [weak self] in
      self?.openLocalIndex()
    }
    duplicates.onLibraryChanged = { [weak self] in
      self?.homeViewController.reloadPhotoLibrary()
    }
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "home.duplicates.title",
        defaultValue: "Duplicate Photos"
      ),
      contentViewController: duplicates,
      size: NSSize(width: 760, height: 650)
    )
    controller.window?.minSize = NSSize(
      width: 680,
      height: 540
    )
    duplicatesWindowController = controller
    retainAndShow(controller)
  }

  func openRepositoryMaintenance() {
    if let maintenanceWindowController,
      maintenanceWindowController.window?.isVisible == true
    {
      maintenanceWindowController.window?.makeKeyAndOrderFront(nil)
      return
    }
    guard !isExecutionActive else { return }
    let session = container.appSession.snapshot
    guard let profile = session.activeProfile,
      MacRepositoryMaintenanceContext.capture(
        representedProfile: profile,
        representedGeneration: session.generation,
        current: session
      ) != nil
    else {
      let alert = NSAlert()
      alert.alertStyle = .informational
      alert.messageText = String(
        localized: "mediaBrowser.action.notConnected",
        defaultValue: "Connect to a backup first."
      )
      alert.addButton(
        withTitle: String(
          localized: "common.ok",
          defaultValue: "OK"
        )
      )
      alert.runModal()
      return
    }
    let maintenance = MacRepositoryMaintenanceViewController(
      profile: profile,
      sessionGeneration: session.generation,
      appSession: container.appSession,
      controller: container.remoteMaintenanceController,
      backupCoordinator: container.backupCoordinator,
      databaseManager: container.databaseManager
    )
    #if DEBUG
      if ProcessInfo.processInfo.arguments.contains(
        "--demo-maintenance-review"
      ) {
        maintenance.showDemoReview()
      }
    #endif
    maintenance.onBusyChanged = { [weak self] busy in
      self?.homeViewController.setDestinationInteractionEnabled(
        !busy
      )
    }
    maintenance.onRepositoryChanged = { [weak self] in
      guard let self else { return }
      self.homeViewController.applyCurrentRemoteSnapshot(
        self.container.remoteLibraryReadService
          .currentSnapshotState(),
        sessionGeneration: session.generation
      )
    }
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "mac.maintenance.title",
        defaultValue: "Repository Maintenance"
      ),
      contentViewController: maintenance,
      size: NSSize(width: 900, height: 560)
    )
    maintenanceWindowController = controller
    retainAndShow(controller)
  }

  func openPhotoBrowser(request: MacPhotoBrowserRequest) {
    let browser = MacPhotoBrowserViewController(
      request: request,
      photoLibraryService: container.photoLibraryService,
      backupCoordinator: container.backupCoordinator,
      hashIndexRepository: container.hashIndexRepository,
      downloadWorkflowHelper: container.downloadWorkflowHelper,
      storageClientFactory: container.storageClientFactory,
      appSession: container.appSession,
      appRuntimeFlags: container.appRuntimeFlags
    )
    browser.onBackUpItems = {
      [weak self] localAssetIDsByMonth in
      self?.confirmBrowserBackup(
        localAssetIDsByMonth: localAssetIDsByMonth,
        monthGroupingTimeZone:
          request.monthGroupingTimeZone
      ) ?? false
    }
    browser.onRemoteLibraryChanged = {
      [weak self] sessionGeneration in
      guard let self,
        self.container.appSession.activeProfile != nil
      else { return }
      self.homeViewController.applyCurrentRemoteSnapshot(
        self.container.remoteLibraryReadService
          .currentSnapshotState(),
        sessionGeneration: sessionGeneration
      )
    }
    browser.onLocalLibraryChanged = { [weak self] in
      self?.homeViewController.reloadPhotoLibrary()
    }
    let controller = makeAuxiliaryWindow(
      title: request.title,
      contentViewController: browser,
      size: NSSize(width: 1_080, height: 700)
    )
    controller.window?.minSize = NSSize(width: 920, height: 620)
    retainAndShow(controller)
  }

  private func confirmBrowserBackup(
    localAssetIDsByMonth:
      [LibraryMonthKey: Set<String>],
    monthGroupingTimeZone:
      MonthGroupingTimeZonePreference
  ) -> Bool {
    let session = container.appSession.snapshot
    guard let profile = session.activeProfile else {
      showConnectionRequiredAlert()
      return false
    }
    let localIdentifierCount = localAssetIDsByMonth.values.reduce(
      0
    ) {
      $0 + $1.count
    }
    guard localIdentifierCount > 0 else { return false }
    let alert = NSAlert()
    alert.alertStyle = .warning
    alert.messageText =
      localIdentifierCount == 1
      ? String(
        localized: "mac.browser.backupConfirmTitle.one",
        defaultValue: "Back up this item to \(profile.name)?"
      )
      : String(
        localized: "mac.browser.backupConfirmTitle.many",
        defaultValue: "Back up \(localIdentifierCount) items to \(profile.name)?"
      )
    alert.addButton(
      withTitle: String(
        localized: "panel.backup",
        defaultValue: "Back Up"
      )
    )
    alert.addButton(
      withTitle: String(
        localized: "common.cancel",
        defaultValue: "Cancel"
      )
    )
    guard alert.runModal() == .alertFirstButtonReturn else {
      return false
    }
    return backupExecutionController.start(
      profileID: profile.id,
      expectedSessionGeneration: session.generation,
      plan: MacBackupExecutionPlan(
        backupMonths: Set(localAssetIDsByMonth.keys),
        downloadMonths: [],
        complementMonths: [],
        localAssetIDsByMonth: localAssetIDsByMonth,
        monthGroupingTimeZone: monthGroupingTimeZone,
        incompleteDownloadPolicy: .skip
      )
    )
  }

  private func showConnectionRequiredAlert() {
    let alert = NSAlert()
    alert.alertStyle = .informational
    alert.messageText = String(
      localized: "home.overlay.notConnected",
      defaultValue: "No node connected"
    )
    alert.addButton(
      withTitle: String(
        localized: "common.ok",
        defaultValue: "OK"
      )
    )
    alert.runModal()
  }

  @discardableResult
  func requestSafeStopForTermination() -> Bool {
    guard hasTerminationBlockingActivity else { return false }
    let initialBackupState = backupExecutionController.state
    if initialBackupState.requiresSafeStopBeforeTermination,
      !initialBackupState.acceptsStopRequest
    {
      return true
    }
    showWindow(nil)
    window?.makeKeyAndOrderFront(nil)

    let alert = NSAlert()
    alert.alertStyle = .warning
    alert.messageText = String(
      localized: "mac.execution.quitTitle",
      defaultValue: "A task is still running"
    )
    alert.informativeText = String(
      localized: "mac.execution.quitMessage",
      defaultValue: "Stop the task safely before quitting Watermelon."
    )
    alert.addButton(
      withTitle: String(
        localized: "common.stop",
        defaultValue: "Stop"
      )
    )
    alert.addButton(
      withTitle: String(
        localized: "common.cancel",
        defaultValue: "Cancel"
      )
    )
    #if DEBUG
      let confirmed =
        ProcessInfo.processInfo.arguments.contains(
          "--demo-safe-quit"
        ) || alert.runModal() == .alertFirstButtonReturn
    #else
      let confirmed = alert.runModal() == .alertFirstButtonReturn
    #endif
    guard confirmed else {
      return false
    }
    guard hasTerminationBlockingActivity else {
      return true
    }
    let backupState = backupExecutionController.state
    if backupState.requiresSafeStopBeforeTermination {
      if backupState.acceptsStopRequest {
        backupExecutionController.cancel()
      }
      return true
    } else if container.appRuntimeFlags
      .requestExecutionCancellation()
    {
      return true
    } else if container.remoteMaintenanceController.isBusy {
      container.remoteMaintenanceController.cancel()
      return true
    }
    return false
  }

  var hasActiveExecution: Bool {
    hasTerminationBlockingActivity
  }

  private func openLegacyMigration(for profile: ServerProfileRecord) {
    guard !isExecutionActive else { return }
    let migrationProfile =
      (try? container.databaseManager
        .profileWithBackfilledWriterID(profile)) ?? profile
    let migrationController =
      MacLegacyMigrationViewController(
        profile: migrationProfile,
        storageClientFactory: container.storageClientFactory,
        profileStore: container.profileStore,
        appRuntimeFlags: container.appRuntimeFlags
      )
    migrationController.onRepositoryChanged = { [weak self] in
      guard let profileID = migrationProfile.id else { return }
      self?.remoteConnectionController
        .refreshConnectedProfile(profileID: profileID)
    }
    let controller = makeAuxiliaryWindow(
      title: String(
        localized: "mac.migration.windowTitle",
        defaultValue:
          "Legacy Migration — \(migrationProfile.name)"
      ),
      contentViewController: migrationController,
      size: NSSize(width: 960, height: 680)
    )
    retainAndShow(controller)
  }

  private func makeAuxiliaryWindow(
    title: String,
    contentViewController: NSViewController,
    size: NSSize
  ) -> NSWindowController {
    let window = NSWindow(contentViewController: contentViewController)
    window.title = title
    window.setContentSize(size)
    window.minSize = NSSize(width: 720, height: 480)
    window.styleMask.insert(.resizable)
    return NSWindowController(window: window)
  }

  private func retainAndShow(_ controller: NSWindowController) {
    auxiliaryWindowControllers.removeAll {
      $0.window == nil || $0.window?.isVisible == false
    }
    auxiliaryWindowControllers.append(controller)
    controller.showWindow(nil)
    controller.window?.makeKeyAndOrderFront(nil)
  }
}

extension MainWindowController: NSWindowDelegate {
  func windowWillClose(_ notification: Notification) {
    guard
      backupExecutionController.state
        .isAwaitingResultDismissal
    else {
      return
    }
    backupExecutionController.resetPresentation()
  }
}
