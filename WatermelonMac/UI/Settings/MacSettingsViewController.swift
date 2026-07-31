import AppKit
import MoreKit

@MainActor
final class MacSettingsViewController: NSTabViewController {
  private let backupController: MacBackupSettingsViewController

  init(
    canChangeMonthGroupingTimeZone: @escaping () -> Bool,
    openLocalPhotoBrowser: @escaping () -> Void = {},
    openProfileManagement: @escaping () -> Void = {}
  ) {
    backupController = MacBackupSettingsViewController(
      canChangeMonthGroupingTimeZone:
        canChangeMonthGroupingTimeZone
    )
    super.init(nibName: nil, bundle: nil)

    tabStyle = .toolbar
    transitionOptions = []
    addPage(
      MacGeneralSettingsViewController(),
      title: String(
        localized: "more.section.general",
        defaultValue: "General"
      ),
      symbolName: "gearshape"
    )
    addPage(
      backupController,
      title: String(
        localized: "more.section.backup",
        defaultValue: "Backup"
      ),
      symbolName: "arrow.up.circle"
    )
    addPage(
      MacBrowserSettingsViewController(
        openLocalPhotoBrowser: openLocalPhotoBrowser,
        openProfileManagement: openProfileManagement
      ),
      title: String(
        localized: "more.section.imageBrowser",
        defaultValue: "Photo Browser"
      ),
      symbolName: "photo.on.rectangle"
    )
    addPage(
      MacMoreSettingsViewController(),
      title: String(
        localized: "more.title",
        defaultValue: "More"
      ),
      symbolName: "ellipsis.circle"
    )
    selectedTabViewItemIndex = 0
    #if DEBUG
      let arguments = ProcessInfo.processInfo.arguments
      if arguments.contains("--demo-settings-backup") {
        selectedTabViewItemIndex = 1
      } else if arguments.contains("--demo-settings-browser") {
        selectedTabViewItemIndex = 2
      } else if arguments.contains("--demo-settings-more")
        || arguments.contains("--demo-settings-about")
      {
        selectedTabViewItemIndex = 3
      }
    #endif
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  override func loadView() {
    super.loadView()
    view.frame = NSRect(
      x: 0,
      y: 0,
      width: 680,
      height: 420
    )
  }

  override func viewWillAppear() {
    super.viewWillAppear()
    view.window?.toolbarStyle = .preference
    view.window?.toolbar?.displayMode = .iconAndLabel
    view.window?.toolbar?.sizeMode = .regular
    view.window?.tabbingMode = .disallowed
    refreshAvailability()
  }

  func refreshAvailability() {
    backupController.refreshAvailability()
  }

  var isTimeZoneSelectionEnabled: Bool {
    backupController.isTimeZoneSelectionEnabled
  }

  private func addPage(
    _ controller: NSViewController,
    title: String,
    symbolName: String
  ) {
    let item = NSTabViewItem(viewController: controller)
    item.label = title
    item.image = NSImage(
      systemSymbolName: symbolName,
      accessibilityDescription: title
    )
    addTabViewItem(item)
  }

  #if DEBUG
    func showDemoTimeZonePicker() {
      selectedTabViewItemIndex = 1
      backupController.showDemoTimeZonePicker()
    }
  #endif
}

@MainActor
private final class MacBackupSettingsViewController:
  MacSettingsPageViewController
{
  private let iCloudSwitch = NSSwitch()
  private let workerCountPopup = NSPopUpButton()
  private let timeZonePopup = NSPopUpButton()
  private let canChangeMonthGroupingTimeZone: () -> Bool

  init(
    canChangeMonthGroupingTimeZone:
      @escaping () -> Bool
  ) {
    self.canChangeMonthGroupingTimeZone =
      canChangeMonthGroupingTimeZone
    super.init()
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  override func makeRows() -> [FormRow] {
    configureICloudSwitch()
    configureWorkerCountPopup()
    configureTimeZonePopup()

    return [
      makeFormRow(
        title: String(
          localized: "settings.worker.default.title",
          defaultValue: "Default Concurrency"
        ),
        control: workerCountPopup,
        fillsControlColumn: true
      ),
      makeFormRow(
        title: String(
          localized: "settings.icloud.header",
          defaultValue: "Allow iCloud Photo Access"
        ),
        control: iCloudSwitch
      ),
      makeFormRow(
        title: String(
          localized:
            "settings.monthGroupingTimeZone.section.timeZone",
          defaultValue: "Time Zone"
        ),
        control: timeZonePopup,
        fillsControlColumn: true
      ),
    ]
  }

  func refreshAvailability() {
    guard isViewLoaded else { return }
    timeZonePopup.isEnabled =
      canChangeMonthGroupingTimeZone()
  }

  var isTimeZoneSelectionEnabled: Bool {
    loadViewIfNeeded()
    return timeZonePopup.isEnabled
  }

  private func configureICloudSwitch() {
    iCloudSwitch.state =
      ICloudPhotoBackupMode.persistedValue == .enable ? .on : .off
    iCloudSwitch.target = self
    iCloudSwitch.action = #selector(changeICloudMode(_:))
  }

  private func configureWorkerCountPopup() {
    workerCountPopup.removeAllItems()
    for mode in BackupWorkerCountMode.allCases {
      workerCountPopup.addItem(
        withTitle: Self.workerCountTitle(mode)
      )
      workerCountPopup.lastItem?.representedObject = mode.rawValue
    }
    if let selectedItem = workerCountPopup.itemArray.first(where: {
      ($0.representedObject as? Int)
        == BackupWorkerCountMode.persistedValue.rawValue
    }) {
      workerCountPopup.select(selectedItem)
    }
    workerCountPopup.target = self
    workerCountPopup.action = #selector(changeWorkerCount(_:))
  }

  private func configureTimeZonePopup() {
    timeZonePopup.removeAllItems()
    addTimeZoneItem(
      title: String(
        localized: "settings.monthGroupingTimeZone.system",
        defaultValue: "Follow System Time Zone"
      ),
      value: "system"
    )
    addTimeZoneItem(
      title: String(
        localized: "settings.monthGroupingTimeZone.fixedCurrent",
        defaultValue: "Fix to Current Time Zone"
      ),
      value: "current"
    )
    addTimeZoneItem(title: "UTC", value: "utc")
    let current = MonthGroupingTimeZonePreference.current
    let systemIdentifier =
      MonthGroupingTimeZonePreference.currentSystemTimeZone()
      .identifier
    if current.mode == .fixedIana,
      let identifier = current.identifier,
      identifier != systemIdentifier
    {
      addTimeZoneItem(
        title: identifier,
        value: "zone:\(identifier)"
      )
    }
    addTimeZoneItem(
      title: String(
        localized: "settings.monthGroupingTimeZone.chooseOther",
        defaultValue: "Custom Time Zone"
      ) + "…",
      value: "choose"
    )
    let selectedValue: String
    if current.mode == .system {
      selectedValue = "system"
    } else if current == .fixedUTC() {
      selectedValue = "utc"
    } else if let identifier = current.identifier,
      identifier != systemIdentifier
    {
      selectedValue = "zone:\(identifier)"
    } else {
      selectedValue = "current"
    }
    if let item = timeZonePopup.itemArray.first(
      where: { $0.representedObject as? String == selectedValue }
    ) {
      timeZonePopup.select(item)
    }
    timeZonePopup.target = self
    timeZonePopup.action = #selector(changeTimeZone(_:))
  }

  private func addTimeZoneItem(title: String, value: String) {
    timeZonePopup.addItem(withTitle: title)
    timeZonePopup.lastItem?.representedObject = value
  }

  @objc private func changeICloudMode(_ sender: NSSwitch) {
    ICloudPhotoBackupMode.setPersistedValue(
      sender.state == .on ? .enable : .disable
    )
  }

  @objc private func changeWorkerCount(_ sender: NSPopUpButton) {
    guard let rawValue = sender.selectedItem?.representedObject as? Int,
      let mode = BackupWorkerCountMode(rawValue: rawValue)
    else {
      configureWorkerCountPopup()
      return
    }
    BackupWorkerCountMode.setPersistedValue(mode)
  }

  @objc private func changeTimeZone(_ sender: NSPopUpButton) {
    guard let value = sender.selectedItem?.representedObject as? String
    else {
      return
    }
    let preference: MonthGroupingTimeZonePreference?
    switch value {
    case "system":
      preference = .defaultPreference
    case "utc":
      preference = .fixedUTC()
    case "current":
      preference = .fixedCurrent()
    case "choose":
      preference = nil
      configureTimeZonePopup()
      guard canChangeMonthGroupingTimeZone() else {
        presentTimeZoneChangeBlockedAlert()
        return
      }
      presentTimeZonePicker()
    default:
      if value.hasPrefix("zone:") {
        let identifier = String(value.dropFirst(5))
        let timeZone = TimeZone(identifier: identifier)
        preference = timeZone.map {
          MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: identifier,
            fallbackOffsetSeconds: $0.secondsFromGMT(
              for: Date()
            )
          )
        }
      } else {
        preference = nil
      }
    }
    guard let preference else { return }
    setMonthGroupingTimeZone(preference)
  }

  private func presentTimeZonePicker() {
    let picker = MacTimeZonePickerViewController()
    picker.onSelect = { [weak self] timeZone in
      self?.setMonthGroupingTimeZone(
        MonthGroupingTimeZonePreference(
          mode: .fixedIana,
          identifier: timeZone.identifier,
          fallbackOffsetSeconds: timeZone.secondsFromGMT(
            for: Date()
          )
        )
      )
      self?.configureTimeZonePopup()
    }
    presentAsSheet(picker)
  }

  private func setMonthGroupingTimeZone(
    _ preference: MonthGroupingTimeZonePreference
  ) {
    let normalized = preference.normalized()
    guard
      normalized
        != MonthGroupingTimeZonePreference.current
    else {
      configureTimeZonePopup()
      return
    }
    guard canChangeMonthGroupingTimeZone() else {
      configureTimeZonePopup()
      presentTimeZoneChangeBlockedAlert()
      return
    }
    MonthGroupingTimeZonePreference.setCurrent(normalized)
    configureTimeZonePopup()
  }

  private func presentTimeZoneChangeBlockedAlert() {
    let alert = NSAlert()
    alert.alertStyle = .informational
    alert.messageText = String(
      localized: "settings.monthGroupingTimeZone.title"
    )
    alert.informativeText = String(
      localized:
        "settings.monthGroupingTimeZone.blockedDuringExecution"
    )
    alert.addButton(
      withTitle: String(localized: "common.ok")
    )
    if let window = view.window {
      alert.beginSheetModal(for: window)
    } else {
      alert.runModal()
    }
  }

  private static func workerCountTitle(
    _ mode: BackupWorkerCountMode
  ) -> String {
    guard let count = mode.workerCountOverride else {
      return String(
        localized: "settings.worker.automatic",
        defaultValue: "Automatic"
      )
    }
    return String.localizedStringWithFormat(
      String(
        localized: "settings.worker.count",
        defaultValue: "%lld workers"
      ),
      Int64(count)
    )
  }

  #if DEBUG
    func showDemoTimeZonePicker() {
      presentTimeZonePicker()
    }
  #endif
}

@MainActor
private final class MacBrowserSettingsViewController:
  MacSettingsPageViewController
{
  private let openLocalPhotoBrowser: () -> Void
  private let openProfileManagement: () -> Void

  init(
    openLocalPhotoBrowser: @escaping () -> Void,
    openProfileManagement: @escaping () -> Void
  ) {
    self.openLocalPhotoBrowser = openLocalPhotoBrowser
    self.openProfileManagement = openProfileManagement
    super.init()
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  override func makeRows() -> [FormRow] {
    let openButton = makeActionButton(
      title: String(
        localized: "common.open",
        defaultValue: "Open"
      ),
      action: #selector(openBrowser(_:))
    )
    let manageButton = makeActionButton(
      title: String(
        localized: "common.edit",
        defaultValue: "Edit"
      ),
      action: #selector(manageBrowserThumbnails(_:))
    )
    return [
      makeFormRow(
        title: String(
          localized: "home.photoLibrary",
          defaultValue: "Photo Library"
        ),
        control: openButton
      ),
      makeFormRow(
        title: String(
          localized: "remoteThumbnails.title",
          defaultValue: "Remote Thumbnails"
        ),
        control: manageButton
      ),
    ]
  }

  @objc private func openBrowser(_ sender: Any?) {
    openLocalPhotoBrowser()
  }

  @objc private func manageBrowserThumbnails(_ sender: Any?) {
    openProfileManagement()
  }
}

@MainActor
private final class MacGeneralSettingsViewController:
  MacSettingsPageViewController
{
  private let languagePopup = NSPopUpButton()
  private let purchaseButton = NSButton()
  private let restoreButton = NSButton()
  private var storeOperationTask: Task<Void, Never>?

  override init() {
    super.init()
    for name in [
      Notification.Name.StoreInfoLoaded,
      .StoreProductsLoaded,
      .LifetimeMembership,
    ] {
      NotificationCenter.default.addObserver(
        self,
        selector: #selector(storeStateDidChange(_:)),
        name: name,
        object: nil
      )
    }
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  override func makeRows() -> [FormRow] {
    configureLanguagePopup()

    purchaseButton.bezelStyle = .rounded
    purchaseButton.target = self
    purchaseButton.action = #selector(purchase(_:))

    restoreButton.title = String(
      localized: "store.restorePurchases",
      defaultValue: "Restore Purchases"
    )
    restoreButton.bezelStyle = .rounded
    restoreButton.target = self
    restoreButton.action = #selector(restore(_:))

    renderStoreState()

    return [
      makeFormRow(
        title: String(
          localized: "more.item.settings.language",
          defaultValue: "Language"
        ),
        control: languagePopup,
        fillsControlColumn: true
      ),
      makeFormRow(
        title: String(
          localized: "store.promotion.title",
          defaultValue: "Upgrade to Pro"
        ),
        control: purchaseButton
      ),
      makeFormRow(
        title: String(
          localized: "store.restorePurchases",
          defaultValue: "Restore Purchases"
        ),
        control: restoreButton
      ),
    ]
  }

  override func viewWillAppear() {
    super.viewWillAppear()
    renderStoreState()
    Store.shared.retryRequestProducts()
    storeOperationTask?.cancel()
    storeOperationTask = Task { [weak self] in
      await Store.shared.updateCustomerProductStatus()
      guard !Task.isCancelled else { return }
      self?.renderStoreState()
    }
  }

  private func configureLanguagePopup() {
    languagePopup.removeAllItems()
    for identifier in Self.supportedLanguageIdentifiers {
      languagePopup.addItem(
        withTitle: Self.languageName(identifier)
      )
      languagePopup.lastItem?.representedObject = identifier
    }
    let selectedIdentifier =
      UserDefaults.standard.stringArray(
        forKey: "AppleLanguages"
      )?.first
      ?? Bundle.main.preferredLocalizations.first
      ?? "en"
    let selectedItem = languagePopup.itemArray.first {
      ($0.representedObject as? String)
        == Self.supportedLanguageIdentifier(
          matching: selectedIdentifier
        )
    }
    if let selectedItem {
      languagePopup.select(selectedItem)
    }
    languagePopup.target = self
    languagePopup.action = #selector(changeLanguage(_:))
  }

  @objc private func changeLanguage(_ sender: NSPopUpButton) {
    guard
      let identifier =
        sender.selectedItem?.representedObject as? String
    else {
      configureLanguagePopup()
      return
    }
    UserDefaults.standard.set(
      [identifier],
      forKey: "AppleLanguages"
    )
  }

  private static let supportedLanguageIdentifiers = [
    "en",
    "zh-Hans",
    "zh-Hant",
    "zh-HK",
    "de",
    "es",
    "es-419",
    "fr",
    "ja",
    "ko",
    "pt-BR",
    "pt-PT",
    "ru",
    "uk",
  ]

  private static func supportedLanguageIdentifier(
    matching identifier: String
  ) -> String {
    if supportedLanguageIdentifiers.contains(identifier) {
      return identifier
    }
    let normalized = identifier.replacingOccurrences(
      of: "_",
      with: "-"
    )
    if let exact = supportedLanguageIdentifiers.first(
      where: {
        $0.compare(
          normalized,
          options: .caseInsensitive
        ) == .orderedSame
      }
    ) {
      return exact
    }
    let languageCode = Locale(
      identifier: normalized
    ).language.languageCode?.identifier
    return supportedLanguageIdentifiers.first {
      Locale(identifier: $0).language.languageCode?.identifier
        == languageCode
    } ?? "en"
  }

  private static func languageName(
    _ identifier: String
  ) -> String {
    Locale(identifier: identifier)
      .localizedString(forIdentifier: identifier)?
      .capitalized(with: Locale(identifier: identifier))
      ?? identifier
  }

  private func renderStoreState(isOperating: Bool = false) {
    guard isViewLoaded else { return }
    if Store.shared.hasValidMembership() {
      purchaseButton.title = String(
        localized: "store.grateful.title",
        defaultValue: "Thank You for Your Support"
      )
      purchaseButton.isEnabled = false
    } else if let displayPrice =
      Store.shared.membershipDisplayPrice()
    {
      purchaseButton.title = [
        String(
          localized: "store.promotion.buttonTitle",
          defaultValue: "Get Pro"
        ),
        displayPrice,
      ].joined(separator: " · ")
      purchaseButton.isEnabled = !isOperating
    } else {
      purchaseButton.title = String(
        localized: "store.promotion.buttonTitle",
        defaultValue: "Get Pro"
      )
      purchaseButton.isEnabled = false
    }
    restoreButton.isEnabled = !isOperating
  }

  @objc private func purchase(_ sender: Any?) {
    runStoreOperation {
      _ = try await Store.shared.purchaseLifetimeMembership()
    }
  }

  @objc private func restore(_ sender: Any?) {
    runStoreOperation {
      try await Store.shared.sync()
    }
  }

  @objc private func storeStateDidChange(
    _ notification: Notification
  ) {
    renderStoreState()
  }

  private func runStoreOperation(
    _ operation: @escaping @MainActor () async throws -> Void
  ) {
    storeOperationTask?.cancel()
    renderStoreState(isOperating: true)
    storeOperationTask = Task { [weak self] in
      do {
        try await operation()
      } catch {
        guard !Task.isCancelled else { return }
        self?.present(error: error)
      }
      guard !Task.isCancelled else { return }
      self?.renderStoreState()
    }
  }

  private func present(error: Error) {
    let alert = NSAlert(error: error)
    if let window = view.window {
      alert.beginSheetModal(for: window)
    } else {
      alert.runModal()
    }
  }
}

@MainActor
private final class MacMoreSettingsViewController:
  MacSettingsPageViewController
{
  private static let appStoreURL = URL(
    string: "https://apps.apple.com/app/id6762260596"
  )!
  private static let reviewURL = URL(
    string:
      "https://apps.apple.com/app/id6762260596?action=write-review"
  )!
  private static let eulaURL = URL(
    string:
      "https://www.apple.com/legal/internet-services/itunes/dev/stdeula/"
  )!
  override var formRowSpacing: CGFloat {
    12
  }

  override func loadView() {
    view = NSView(
      frame: NSRect(x: 0, y: 0, width: 680, height: 350)
    )
    let grid = makeForm(rows: makeRows())
    let showcase = AppShowcaseView(
      apps: AppInfo.App.allCases.filter { $0 != .watermelon },
      visibleIconCount: 5
    )
    showcase.identifier = NSUserInterfaceItemIdentifier(
      "settings.appShowcase"
    )
    showcase.translatesAutoresizingMaskIntoConstraints = false
    view.addSubview(grid)
    view.addSubview(showcase)

    NSLayoutConstraint.activate([
      grid.topAnchor.constraint(
        equalTo: view.topAnchor,
        constant: 44
      ),
      grid.centerXAnchor.constraint(
        equalTo: view.centerXAnchor
      ),
      grid.bottomAnchor.constraint(
        lessThanOrEqualTo: showcase.topAnchor,
        constant: -14
      ),
      showcase.centerXAnchor.constraint(
        equalTo: view.centerXAnchor
      ),
      showcase.widthAnchor.constraint(
        equalToConstant: 320
      ),
      showcase.bottomAnchor.constraint(
        equalTo: view.bottomAnchor,
        constant: -10
      ),
      showcase.heightAnchor.constraint(
        equalToConstant: 74
      ),
    ])
  }

  override func makeRows() -> [FormRow] {
    return [
      makeFormRow(
        title: String(
          localized: "more.contact.email",
          defaultValue: "Email"
        ),
        control: makeActionButton(
          title: "watermelon@zi.ci",
          action: #selector(sendEmail(_:))
        )
      ),
      makeFormRow(
        title: String(
          localized: "more.about.specifications",
          defaultValue: "Specifications"
        ),
        control: makeDisclosureButton(
          title: Self.versionDescription,
          action: #selector(showSpecifications(_:))
        )
      ),
      makeFormRow(
        title: "App Store",
        control: makeAppStoreActionsControl()
      ),
      makeFormRow(
        title: MacHelpDestination.privacyPolicy.title,
        control: makeActionButton(
          title: "watermelonbackup.com",
          action: #selector(openPrivacyPolicy(_:))
        )
      ),
    ]
  }

  private func makeAppStoreActionsControl() -> NSView {
    let shareButton = makeActionButton(
      title: String(
        localized: "mediaBrowser.action.share",
        defaultValue: "Share"
      ),
      action: #selector(shareApp(_:))
    )
    let reviewButton = makeActionButton(
      title: String(
        localized: "more.about.review",
        defaultValue: "Write Review"
      ),
      action: #selector(writeReview(_:))
    )
    let eulaButton = makeActionButton(
      title: String(
        localized: "more.about.eula",
        defaultValue: "EULA"
      ),
      action: #selector(openEULA(_:))
    )
    let stack = NSStackView(
      views: [
        shareButton,
        reviewButton,
        eulaButton,
      ]
    )
    stack.orientation = .horizontal
    stack.spacing = 6
    return stack
  }

  private static var versionDescription: String {
    let dictionary = Bundle.main.infoDictionary
    let version =
      dictionary?["CFBundleShortVersionString"] as? String ?? "—"
    let build = dictionary?["CFBundleVersion"] as? String ?? "—"
    return "\(version) (\(build))"
  }

  private func makeDisclosureButton(
    title: String,
    action: Selector
  ) -> NSButton {
    let button = makeActionButton(
      title: title,
      action: action
    )
    button.image = NSImage(
      systemSymbolName: "chevron.forward",
      accessibilityDescription: nil
    )
    button.imagePosition = .imageTrailing
    return button
  }

  @objc private func sendEmail(_ sender: Any?) {
    NSWorkspace.shared.open(
      MacHelpDestination.contactSupport.url
    )
  }

  @objc private func showSpecifications(_ sender: Any?) {
    presentAsSheet(
      SpecificationsViewController(
        configuration: MacSpecifications.current()
      )
    )
  }

  @objc private func shareApp(_ sender: NSButton) {
    NSSharingServicePicker(
      items: [Self.appStoreURL]
    ).show(
      relativeTo: sender.bounds,
      of: sender,
      preferredEdge: .minY
    )
  }

  @objc private func writeReview(_ sender: Any?) {
    NSWorkspace.shared.open(Self.reviewURL)
  }

  @objc private func openEULA(_ sender: Any?) {
    NSWorkspace.shared.open(Self.eulaURL)
  }

  @objc private func openPrivacyPolicy(_ sender: Any?) {
    NSWorkspace.shared.open(
      MacHelpDestination.privacyPolicy.url
    )
  }
}

@MainActor
private class MacSettingsPageViewController: NSViewController {
  static let columnSpacing: CGFloat = 16

  var formRowSpacing: CGFloat {
    14
  }

  var verticalInset: CGFloat {
    32
  }

  struct FormRow {
    let label: NSTextField
    let control: NSView
    let fillsControlColumn: Bool
  }

  init() {
    super.init(nibName: nil, bundle: nil)
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }

  override func loadView() {
    view = NSView(
      frame: NSRect(x: 0, y: 0, width: 680, height: 350)
    )
    let grid = makeForm(rows: makeRows())
    view.addSubview(grid)

    NSLayoutConstraint.activate([
      grid.topAnchor.constraint(
        equalTo: view.topAnchor,
        constant: verticalInset
      ),
      grid.centerXAnchor.constraint(
        equalTo: view.centerXAnchor
      ),
      grid.bottomAnchor.constraint(
        lessThanOrEqualTo: view.bottomAnchor,
        constant: -verticalInset
      ),
    ])
  }

  func makeRows() -> [FormRow] {
    []
  }

  func makeForm(
    rows: [FormRow]
  ) -> NSGridView {
    let grid = NSGridView(
      views: rows.map { [$0.label, $0.control] }
    )
    grid.identifier = NSUserInterfaceItemIdentifier("settings.grid")
    grid.rowSpacing = formRowSpacing
    grid.columnSpacing = Self.columnSpacing
    grid.column(at: 0).xPlacement = .trailing
    grid.column(at: 1).xPlacement = .leading
    for (index, row) in rows.enumerated() {
      grid.row(at: index).yPlacement = .center
      grid.cell(
        atColumnIndex: 1,
        rowIndex: index
      ).xPlacement = row.fillsControlColumn ? .fill : .leading
    }
    grid.translatesAutoresizingMaskIntoConstraints = false
    return grid
  }

  func makeFormRow(
    title: String,
    control: NSView,
    fillsControlColumn: Bool = false
  ) -> FormRow {
    let label = NSTextField(labelWithString: title)
    label.alignment = .right
    label.lineBreakMode = .byTruncatingTail
    label.usesSingleLineMode = true
    label.setContentCompressionResistancePriority(
      .required,
      for: .horizontal
    )
    label.widthAnchor.constraint(
      greaterThanOrEqualToConstant:
        ceil(label.fittingSize.width) + 1
    ).isActive = true
    label.identifier = NSUserInterfaceItemIdentifier(
      "settings.rowLabel"
    )
    control.identifier = NSUserInterfaceItemIdentifier(
      "settings.rowControl"
    )
    return FormRow(
      label: label,
      control: control,
      fillsControlColumn: fillsControlColumn
    )
  }

  func makeActionButton(
    title: String,
    action: Selector
  ) -> NSButton {
    let button = NSButton(
      title: title,
      target: self,
      action: action
    )
    button.bezelStyle = .rounded
    return button
  }
}
