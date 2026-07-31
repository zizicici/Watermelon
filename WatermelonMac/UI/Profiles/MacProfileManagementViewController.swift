import AppKit
import Combine

enum MacRemoteThumbnailSettingsPolicy {
    static func shouldOfferBackfill(
        wasEnabled: Bool,
        isEnabled: Bool,
        profileIsActive: Bool
    ) -> Bool {
        !wasEnabled && isEnabled && profileIsActive
    }
}

enum MacRemoteThumbnailMaintenanceAvailabilityPolicy {
    static func canStart(
        taskRunning: Bool,
        executionActive: Bool,
        remoteMaintenanceActive: Bool,
        connectionActive: Bool,
        profileIsActive: Bool
    ) -> Bool {
        !taskRunning
            && !executionActive
            && !remoteMaintenanceActive
            && !connectionActive
            && profileIsActive
    }
}

struct MacRemoteThumbnailMaintenanceContext: Sendable {
    let profile: ServerProfileRecord
    let credential: String
    let sessionGeneration: UInt64

    static func capture(
        selectedProfile: ServerProfileRecord,
        current: AppSession.Snapshot
    ) -> Self? {
        guard profileMatches(
            selectedProfile,
            current: current
        ),
        let credential = resolvedCredential(
            for: selectedProfile,
            current: current
        ) else {
            return nil
        }
        return Self(
            profile: selectedProfile,
            credential: credential,
            sessionGeneration: current.generation
        )
    }

    static func profileMatches(
        _ profile: ServerProfileRecord,
        current: AppSession.Snapshot
    ) -> Bool {
        guard let activeProfile = current.activeProfile else {
            return false
        }
        return activeProfile.runtimeConnectionIdentity
                == profile.runtimeConnectionIdentity
            && RemoteIndexSyncService.remoteProfileKey(activeProfile)
                == RemoteIndexSyncService.remoteProfileKey(profile)
    }

    func isCurrent(_ current: AppSession.Snapshot) -> Bool {
        current.generation == sessionGeneration
            && Self.profileMatches(profile, current: current)
            && Self.resolvedCredential(
                for: profile,
                current: current
            ) == credential
    }

    private static func resolvedCredential(
        for profile: ServerProfileRecord,
        current: AppSession.Snapshot
    ) -> String? {
        if profile.storageProfile.requiresStoredCredential {
            return current.activePassword
        }
        return current.activePassword ?? ""
    }
}

@MainActor
final class MacProfileManagementViewController: NSViewController {
    var onOpenLegacyMigration: ((ServerProfileRecord) -> Void)?

    private static let profileRowPasteboardType =
        NSPasteboard.PasteboardType(
            "com.zizicici.watermelon-mac.profile-row"
        )

    private let store: ProfileStore
    private let storageClientFactory: StorageClientFactory
    private let databaseManager: DatabaseManager
    private let appSession: AppSession
    private let appRuntimeFlags: AppRuntimeFlags
    private let isConnectionActive: () -> Bool
    private let remoteMaintenanceController:
        RemoteMaintenanceController
    private let backupCoordinator: BackupCoordinator
    private let remoteThumbnailMaintenanceService:
        MacRemoteThumbnailMaintenanceService
    private let oneDriveProfileSetupCoordinator:
        OneDriveProfileSetupCoordinator
    private let tableView = NSTableView()
    private let detailTitle = NSTextField(labelWithString: "")
    private let migrationButton = NSButton()
    private let editButton = NSButton()
    private let renameButton = NSButton()
    private let deleteButton = NSButton()
    private let addButton = NSButton()
    private let workerPopup = NSPopUpButton()
    private let thumbnailCheckbox = NSButton()
    private let thumbnailProgressIndicator =
        NSProgressIndicator()
    private let thumbnailProgressLabel =
        NSTextField(labelWithString: "")
    private let settingsGrid = NSGridView()
    private var profiles: [ServerProfileRecord] = []
    private var selectedProfileID: Int64?
    private var cancellables = Set<AnyCancellable>()
    private var thumbnailMaintenanceTask:
        Task<Void, Never>?
    private var closeWhenThumbnailMaintenanceFinishes = false

    init(
        store: ProfileStore,
        storageClientFactory: StorageClientFactory,
        databaseManager: DatabaseManager,
        appSession: AppSession,
        appRuntimeFlags: AppRuntimeFlags,
        isConnectionActive: @escaping () -> Bool,
        remoteMaintenanceController:
            RemoteMaintenanceController,
        backupCoordinator: BackupCoordinator,
        remoteThumbnailMaintenanceService:
            MacRemoteThumbnailMaintenanceService,
        oneDriveProfileSetupCoordinator:
            OneDriveProfileSetupCoordinator,
        selectedProfileID: Int64? = nil
    ) {
        self.store = store
        self.storageClientFactory = storageClientFactory
        self.databaseManager = databaseManager
        self.appSession = appSession
        self.appRuntimeFlags = appRuntimeFlags
        self.isConnectionActive = isConnectionActive
        self.remoteMaintenanceController =
            remoteMaintenanceController
        self.backupCoordinator = backupCoordinator
        self.remoteThumbnailMaintenanceService =
            remoteThumbnailMaintenanceService
        self.oneDriveProfileSetupCoordinator =
            oneDriveProfileSetupCoordinator
        self.selectedProfileID = selectedProfileID
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        thumbnailMaintenanceTask?.cancel()
    }

    override func loadView() {
        view = NSView()

        configureTable()
        configureDetail()
        configureNodeSettings()

        addButton.title = ""
        addButton.target = self
        addButton.action = #selector(showAddMenu(_:))
        addButton.image = NSImage(
            systemSymbolName: "plus",
            accessibilityDescription: nil
        )
        addButton.imagePosition = .imageLeading
        addButton.bezelStyle = .rounded
        addButton.toolTip = String(
            localized: "home.menu.addStorage",
            defaultValue: "Add Destination"
        )

        renameButton.title = String(
            localized: "auth.section.name",
            defaultValue: "Name"
        ) + "…"
        renameButton.bezelStyle = .rounded
        renameButton.target = self
        renameButton.action = #selector(renameProfile(_:))

        editButton.title = String(
            localized: "storage.detail.editConnection",
            defaultValue: "Connection Parameters"
        ) + "…"
        editButton.bezelStyle = .rounded
        editButton.target = self
        editButton.action = #selector(editConnection(_:))

        deleteButton.title = String(
            localized: "common.delete",
            defaultValue: "Delete"
        )
        deleteButton.bezelStyle = .rounded
        deleteButton.contentTintColor = .wmMaterialError
        deleteButton.target = self
        deleteButton.action = #selector(deleteProfile(_:))

        let listButtons = NSStackView(
            views: [addButton, NSView(), renameButton, deleteButton]
        )
        listButtons.orientation = .horizontal
        listButtons.alignment = .centerY
        listButtons.spacing = 8

        let listScroll = NSScrollView()
        listScroll.documentView = tableView
        listScroll.hasVerticalScroller = true
        listScroll.autohidesScrollers = true
        listScroll.borderType = .bezelBorder

        let listStack = NSStackView(views: [listScroll, listButtons])
        listStack.orientation = .vertical
        listStack.alignment = .width
        listStack.spacing = 10

        let detailActions = NSStackView(
            views: [editButton, migrationButton]
        )
        detailActions.orientation = .horizontal
        detailActions.alignment = .centerY
        detailActions.spacing = 10

        let detailStack = NSStackView(
            views: [
                detailTitle,
                settingsGrid,
                NSView(),
                detailActions
            ]
        )
        detailStack.orientation = .vertical
        detailStack.alignment = .centerX
        detailStack.spacing = 12
        detailStack.edgeInsets = NSEdgeInsets(
            top: 30,
            left: 34,
            bottom: 28,
            right: 34
        )
        detailStack.wantsLayer = true
        detailStack.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor
        detailStack.layer?.borderColor = NSColor.separatorColor.cgColor
        detailStack.layer?.borderWidth = 1
        detailStack.layer?.cornerRadius = 10

        let split = NSStackView(views: [listStack, detailStack])
        split.orientation = .horizontal
        split.alignment = .height
        split.spacing = 18

        let root = NSStackView(
            views: [split]
        )
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 10
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor, constant: 26),
            root.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 28
            ),
            root.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -28
            ),
            root.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -26
            ),
            split.widthAnchor.constraint(equalTo: root.widthAnchor),
            split.heightAnchor.constraint(greaterThanOrEqualToConstant: 420),
            listStack.widthAnchor.constraint(equalToConstant: 350),
            detailStack.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 390
            ),
        ])

        store.$profiles
            .sink { [weak self] profiles in
                self?.apply(profiles)
            }
            .store(in: &cancellables)
        store.$loadError
            .compactMap { $0 }
            .sink { [weak self] error in
                self?.present(error: error)
            }
            .store(in: &cancellables)
        NotificationCenter.default.publisher(
            for: .ExecutionLifecycleDidChange
        )
        .merge(
            with: NotificationCenter.default.publisher(
                for: .ConnectionLifecycleDidChange
            )
        )
        .merge(
            with: NotificationCenter.default.publisher(
                for: .RemoteMaintenanceDidChange
            )
        )
        .receive(on: RunLoop.main)
        .sink { [weak self] _ in
            self?.renderDetail()
        }
        .store(in: &cancellables)
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
    }

    private var selectedProfile: ServerProfileRecord? {
        selectedProfileID.flatMap { id in
            profiles.first { $0.id == id }
        }
    }

    private var executionActive: Bool {
        appRuntimeFlags.isExecuting
    }

    private var canCreateProfile: Bool {
        MacProfileMutationAvailabilityPolicy.canMutate(
            executionActive: executionActive,
            maintenanceActive: remoteMaintenanceController.isBusy,
            connectionActive: isConnectionActive()
        )
    }

    func selectProfile(id: Int64) {
        selectedProfileID = id
        guard isViewLoaded else { return }
        apply(store.profiles)
    }

    func beginAdding(
        _ storageType: StorageType,
        onCreated: ((ServerProfileRecord) -> Void)? = nil
    ) {
        loadViewIfNeeded()
        guard canCreateProfile else {
            present(error: MacProfileMutationError.taskInProgress)
            return
        }
        switch storageType {
        case .externalVolume:
            presentAddLocal(onCreated: onCreated)
        case .smb:
            presentAddSMB(onCreated: onCreated)
        case .webdav:
            presentAddWebDAV(onCreated: onCreated)
        case .sftp:
            presentAddSFTP(onCreated: onCreated)
        case .s3:
            presentAddS3(onCreated: onCreated)
        case .onedrive:
            presentAddOneDrive(onCreated: onCreated)
        }
    }

    private func configureTable() {
        let column = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("profile")
        )
        tableView.addTableColumn(column)
        tableView.headerView = nil
        tableView.rowHeight = 56
        tableView.intercellSpacing = NSSize(width: 0, height: 2)
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.dataSource = self
        tableView.delegate = self
        tableView.registerForDraggedTypes([
            Self.profileRowPasteboardType
        ])
        tableView.setDraggingSourceOperationMask(
            .move,
            forLocal: true
        )
    }

    private func configureDetail() {
        detailTitle.font = .systemFont(ofSize: 17, weight: .semibold)
        detailTitle.alignment = .center

        migrationButton.title = ""
        migrationButton.image = NSImage(
            systemSymbolName: "ellipsis",
            accessibilityDescription: nil
        )
        migrationButton.bezelStyle = .rounded
        migrationButton.target = self
        migrationButton.action = #selector(showProfileActions(_:))
    }

    private func configureNodeSettings() {
        workerPopup.removeAllItems()
        for selection in NodeBackupWorkerCountSelection.allCases {
            workerPopup.addItem(
                withTitle: selection.localizedText()
            )
        }
        workerPopup.target = self
        workerPopup.action = #selector(changeWorkerCount(_:))

        thumbnailCheckbox.setButtonType(.switch)
        thumbnailCheckbox.title = String(
            localized: "remoteThumbnails.enable.label",
            defaultValue: "Generate shared browser thumbnails"
        )
        thumbnailCheckbox.target = self
        thumbnailCheckbox.action = #selector(changeThumbnailSetting(_:))

        let workerLabel = NSTextField(
            labelWithString: String(
                localized: "more.item.workerCount",
                defaultValue: "Concurrency"
            )
        )
        workerLabel.font = .systemFont(ofSize: 12, weight: .medium)
        workerLabel.alignment = .right

        settingsGrid.addRow(with: [workerLabel, workerPopup])
        settingsGrid.addRow(
            with: [NSView(), thumbnailCheckbox]
        )

        thumbnailProgressIndicator.minValue = 0
        thumbnailProgressIndicator.maxValue = 1
        thumbnailProgressIndicator.isIndeterminate = false
        thumbnailProgressIndicator.controlSize = .small
        thumbnailProgressIndicator.isHidden = true
        thumbnailProgressIndicator.widthAnchor.constraint(
            equalToConstant: 210
        ).isActive = true
        thumbnailProgressLabel.font = .systemFont(ofSize: 11)
        thumbnailProgressLabel.textColor =
            .secondaryLabelColor
        thumbnailProgressLabel.isHidden = true
        let progress = NSStackView(
            views: [
                thumbnailProgressIndicator,
                thumbnailProgressLabel,
            ]
        )
        progress.orientation = .vertical
        progress.alignment = .leading
        progress.spacing = 4
        settingsGrid.addRow(with: [NSView(), progress])
        settingsGrid.rowSpacing = 12
        settingsGrid.columnSpacing = 14
        settingsGrid.column(at: 0).xPlacement = .trailing
        settingsGrid.column(at: 1).xPlacement = .leading
    }

    private func apply(_ profiles: [ServerProfileRecord]) {
        self.profiles = profiles
        if let selectedProfileID,
           !profiles.contains(where: { $0.id == selectedProfileID }) {
            self.selectedProfileID = nil
        }
        if selectedProfileID == nil {
            selectedProfileID = profiles.first?.id
        }
        tableView.reloadData()
        if let index = profiles.firstIndex(
            where: { $0.id == selectedProfileID }
        ) {
            tableView.selectRowIndexes(
                IndexSet(integer: index),
                byExtendingSelection: false
            )
        } else {
            tableView.deselectAll(nil)
        }
        renderDetail()
    }

    private func renderDetail() {
        addButton.isEnabled = canCreateProfile
        guard let profile = selectedProfile else {
            detailTitle.stringValue = ""
            settingsGrid.isHidden = true
            migrationButton.isHidden = true
            editButton.isHidden = true
            editButton.isEnabled = false
            renameButton.isEnabled = false
            deleteButton.isEnabled = false
            updateThumbnailMaintenanceControls()
            return
        }

        let canMutate = canMutate(profile)
        detailTitle.stringValue = profile.name
        let workerSelection = NodeBackupWorkerCountSelection(
            persistedMode: profile.uploadWorkerCountMode
        )
        workerPopup.selectItem(
            at: NodeBackupWorkerCountSelection.allCases.firstIndex(
                of: workerSelection
            ) ?? 0
        )
        thumbnailCheckbox.state = profile.generateRemoteThumbnails
            ? .on
            : .off
        settingsGrid.isHidden = false
        migrationButton.isHidden = false
        editButton.isHidden = false
        editButton.isEnabled = canMutate
        renameButton.isEnabled = canMutate
        deleteButton.isEnabled = canMutate
        workerPopup.isEnabled = canMutate
        thumbnailCheckbox.isEnabled = canMutate
        migrationButton.isEnabled =
            thumbnailMaintenanceTask != nil || canMutate
        updateThumbnailMaintenanceControls()
    }

    private func canMutate(_ profile: ServerProfileRecord) -> Bool {
        MacProfileMutationAvailabilityPolicy.canMutate(
            executionActive: executionActive,
            maintenanceActive:
                remoteMaintenanceController.isBusy,
            connectionActive: isConnectionActive()
        )
    }

    @objc private func showAddMenu(_ sender: NSButton) {
        guard canCreateProfile else {
            present(error: MacProfileMutationError.taskInProgress)
            return
        }
        let menu = NSMenu()
        addMenuItem(
            to: menu,
            title: String(localized: "profile.add.local.title"),
            symbol: StorageType.externalVolume.symbolName,
            action: #selector(addLocal(_:))
        )
        addMenuItem(
            to: menu,
            title: StorageType.smb.sectionHeaderText,
            symbol: StorageType.smb.symbolName,
            action: #selector(addSMB(_:))
        )
        addMenuItem(
            to: menu,
            title: String(localized: "auth.webdav.title"),
            symbol: StorageType.webdav.symbolName,
            action: #selector(addWebDAV(_:))
        )
        addMenuItem(
            to: menu,
            title: String(localized: "auth.sftp.title"),
            symbol: StorageType.sftp.symbolName,
            action: #selector(addSFTP(_:))
        )
        addMenuItem(
            to: menu,
            title: String(localized: "auth.s3.title"),
            symbol: StorageType.s3.symbolName,
            action: #selector(addS3(_:))
        )
        addMenuItem(
            to: menu,
            title: String(localized: "auth.onedrive.title"),
            symbol: StorageType.onedrive.symbolName,
            action: #selector(addOneDrive(_:))
        )
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: sender.bounds.maxY + 4),
            in: sender
        )
    }

    private func addMenuItem(
        to menu: NSMenu,
        title: String,
        symbol: String,
        action: Selector
    ) {
        let item = NSMenuItem(
            title: title,
            action: action,
            keyEquivalent: ""
        )
        item.target = self
        item.image = NSImage(
            systemSymbolName: symbol,
            accessibilityDescription: nil
        )
        menu.addItem(item)
    }

    @objc private func addLocal(_ sender: Any?) {
        presentAddLocal(onCreated: nil)
    }

    private func presentAddLocal(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        chooseExternalFolder(
            title: String(
                localized: "profile.add.local.title",
                defaultValue: "Add External Folder"
            ),
            message: String(
                localized: "profile.add.local.pickerMessage",
                defaultValue: "Choose a folder for Watermelon backup data."
            )
        ) { [weak self] url in
            guard let self else { return }
            let record = try self.withNewProfileMutation {
                try self.store.saveLocalProfile(folderURL: url)
            }
            self.didCreateProfile(record, onCreated: onCreated)
        }
    }

    @objc private func addSMB(_ sender: Any?) {
        presentAddSMB(onCreated: nil)
    }

    private func presentAddSMB(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        presentAsSheet(
            MacSMBProfileViewController(
                canPerformActions: { [weak self] in
                    self?.canCreateProfile == true
                }
            ) { [weak self] context in
                guard let self else { return }
                let record = try self.withNewProfileMutation {
                    try self.store.saveSMBProfile(
                        name: context.auth.name,
                        host: context.auth.host,
                        port: context.auth.port,
                        shareName: context.shareName,
                        basePath: context.basePath,
                        username: context.auth.username,
                        domain: context.auth.domain,
                        password: context.auth.password
                    )
                }
                self.didCreateProfile(record, onCreated: onCreated)
            }
        )
    }

    @objc private func addWebDAV(_ sender: Any?) {
        presentAddWebDAV(onCreated: nil)
    }

    private func presentAddWebDAV(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        presentAsSheet(
            MacWebDAVProfileViewController(
                storageClientFactory: storageClientFactory,
                canPerformActions: { [weak self] in
                    self?.canCreateProfile == true
                }
            ) { [weak self] snapshot in
                guard let self else { return }
                let record = try self.withNewProfileMutation {
                    try self.store.saveWebDAVProfile(
                        name: snapshot.name,
                        scheme: snapshot.scheme,
                        host: snapshot.host,
                        port: snapshot.port,
                        mountPath: snapshot.mountPath,
                        basePath: snapshot.basePath,
                        username: snapshot.username,
                        password: snapshot.password
                    )
                }
                self.didCreateProfile(record, onCreated: onCreated)
            }
        )
    }

    @objc private func addSFTP(_ sender: Any?) {
        presentAddSFTP(onCreated: nil)
    }

    private func presentAddSFTP(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        presentAsSheet(
            MacSFTPProfileViewController(
                canPerformActions: { [weak self] in
                    self?.canCreateProfile == true
                }
            ) {
                [weak self]
                name,
                host,
                port,
                basePath,
                username,
                credential,
                testedHostKey in
                guard let self else { return }
                let record = try self.withNewProfileMutation {
                    let fingerprint =
                        try MacSFTPProfileSaveFingerprintResolver.resolve(
                            editingSnapshot: nil,
                            proposedHost: host,
                            proposedPort: port,
                            testedHostKey: testedHostKey,
                            databaseManager: self.databaseManager
                        )
                    return try self.store.saveSFTPProfile(
                        name: name,
                        host: host,
                        port: port,
                        basePath: basePath,
                        username: username,
                        credential: credential,
                        hostKeyFingerprintSHA256: fingerprint
                    )
                }
                self.didCreateProfile(record, onCreated: onCreated)
            }
        )
    }

    @objc private func addS3(_ sender: Any?) {
        presentAddS3(onCreated: nil)
    }

    private func presentAddS3(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        presentAsSheet(
            MacS3ProfileViewController(
                storageClientFactory: storageClientFactory,
                appRuntimeFlags: appRuntimeFlags,
                canPerformActions: { [weak self] in
                    self?.canCreateProfile == true
                }
            ) { [weak self] snapshot in
                guard let self else { return }
                let record = try self.withNewProfileMutation {
                    try self.store.saveS3Profile(
                        name: snapshot.name,
                        scheme: snapshot.scheme,
                        host: snapshot.host,
                        port: snapshot.port,
                        region: snapshot.region,
                        bucket: snapshot.bucket,
                        basePath: snapshot.basePath,
                        usePathStyle: snapshot.usePathStyle,
                        accessKeyID: snapshot.accessKeyID,
                        secretAccessKey: snapshot.secretAccessKey
                    )
                }
                self.didCreateProfile(record, onCreated: onCreated)
            }
        )
    }

    @objc private func addOneDrive(_ sender: Any?) {
        presentAddOneDrive(onCreated: nil)
    }

    private func presentAddOneDrive(
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        guard canPresentNewProfileEditor() else { return }
        let controller =
            MacOneDriveProfileSetupViewController(
                setupCoordinator:
                    oneDriveProfileSetupCoordinator,
                store: store,
                appRuntimeFlags: appRuntimeFlags,
                canPerformActions: { [weak self] in
                    self?.canCreateProfile == true
                }
            ) { [weak self] record in
                self?.didCreateProfile(
                    record,
                    onCreated: onCreated
                )
            }
        presentAsSheet(controller)
    }

    private func canPresentNewProfileEditor() -> Bool {
        guard canCreateProfile else {
            present(error: MacProfileMutationError.taskInProgress)
            return false
        }
        return true
    }

    private func didCreateProfile(
        _ profile: ServerProfileRecord,
        onCreated: ((ServerProfileRecord) -> Void)?
    ) {
        selectedProfileID = profile.id
        onCreated?(profile)
    }

    @objc private func editConnection(_ sender: Any?) {
        guard let profile = selectedProfile else { return }
        switch profile.resolvedStorageType {
            case .externalVolume:
                chooseExternalFolder(
                    title: String(
                        localized: "auth.external.selectDir",
                        defaultValue: "Select Directory"
                    ),
                    message: String(
                        localized: "auth.external.noDirMessage",
                        defaultValue: "Please select an external storage directory for backups first."
                    )
                ) { [weak self] url in
                    guard let self else { return }
                    _ = try withProfileMutation(profile) {
                        try self.store.updateLocalProfile(
                            profile,
                            folderURL: url
                        )
                    }
                }
            case .smb:
                let password = MacProfileEditingCredentialPolicy
                    .plainCredential(
                        storedCredential:
                            try? store.password(for: profile)
                    )
                let initial = SMBServerPathContext(
                    auth: SMBServerAuthContext(
                        name: profile.name,
                        host: profile.host,
                        port: SMBEndpoint.effectivePort(profile.port),
                        username: profile.username,
                        password: password,
                        domain: profile.domain
                    ),
                    shareName: profile.shareName,
                    basePath: profile.basePath
                )
                presentAsSheet(
                    MacSMBProfileViewController(
                        initial: initial,
                        canPerformActions: { [weak self] in
                            self?.canMutate(profile) == true
                        }
                    ) {
                        [weak self] context in
                        guard let self else { return }
                        _ = try withProfileMutation(profile) {
                            try self.store.updateSMBProfile(
                                profile,
                                context: context
                            )
                        }
                    }
                )
            case .webdav:
                let password = MacProfileEditingCredentialPolicy
                    .plainCredential(
                        storedCredential:
                            try? store.password(for: profile)
                    )
                let initial = WebDAVProfileSnapshot(
                    name: profile.name,
                    scheme: profile.webDAVParams?.scheme ?? "https",
                    host: profile.host,
                    port: profile.port,
                    mountPath: profile.shareName,
                    basePath: profile.basePath,
                    username: profile.username,
                    password: password
                )
                presentAsSheet(
                    MacWebDAVProfileViewController(
                        storageClientFactory: storageClientFactory,
                        title: String(
                            localized: "auth.webdav.editTitle",
                            defaultValue: "Edit WebDAV Destination"
                        ),
                        initial: initial,
                        canPerformActions: { [weak self] in
                            self?.canMutate(profile) == true
                        }
                    ) { [weak self] snapshot in
                        guard let self else { return }
                        _ = try withProfileMutation(profile) {
                            try self.store.updateWebDAVProfile(
                                profile,
                                snapshot: snapshot
                            )
                        }
                    }
                )
            case .s3:
                let secret = MacProfileEditingCredentialPolicy
                    .plainCredential(
                        storedCredential:
                            try? store.password(for: profile)
                    )
                let initial = S3ProfileSnapshot(
                    name: profile.name,
                    scheme: profile.s3Params?.scheme ?? "https",
                    host: profile.host,
                    port: profile.port,
                    region: profile.s3Params?.region ?? "",
                    bucket: profile.shareName,
                    basePath: profile.basePath,
                    usePathStyle:
                        profile.s3Params?.usePathStyle ?? true,
                    accessKeyID: profile.username,
                    secretAccessKey: secret
                )
                presentAsSheet(
                    MacS3ProfileViewController(
                        storageClientFactory: storageClientFactory,
                        appRuntimeFlags: appRuntimeFlags,
                        mutationProfileID: profile.id,
                        title: String(
                            localized: "auth.s3.editTitle",
                            defaultValue: "Edit S3 Destination"
                        ),
                        initial: initial,
                        canPerformActions: { [weak self] in
                            self?.canMutate(profile) == true
                        }
                    ) { [weak self] snapshot in
                        guard let self else { return }
                        _ = try withProfileMutation(profile) {
                            try self.store.updateS3Profile(
                                profile,
                                snapshot: snapshot
                            )
                        }
                    }
                )
            case .sftp:
                let credential = MacProfileEditingCredentialPolicy
                    .sftpCredential(
                        storedCredential:
                            try? store.password(for: profile),
                        authMethod:
                            profile.sftpParams?.authMethod
                                ?? .password
                )
                presentAsSheet(
                    MacSFTPProfileViewController(
                        title: String(
                            localized: "auth.sftp.editTitle",
                            defaultValue: "Edit SFTP Destination"
                        ),
                        initialProfile: profile,
                        initialCredential: credential,
                        canPerformActions: { [weak self] in
                            self?.canMutate(profile) == true
                        }
                    ) {
                        [weak self]
                        _,
                        host,
                        port,
                        basePath,
                        username,
                        credential,
                        testedHostKey in
                        guard let self else { return }
                        _ = try withProfileMutation(profile) {
                            let fingerprint =
                                try MacSFTPProfileSaveFingerprintResolver
                                    .resolve(
                                        editingSnapshot: profile,
                                        proposedHost: host,
                                        proposedPort: port,
                                        testedHostKey: testedHostKey,
                                        databaseManager:
                                            self.databaseManager
                                    )
                            return try self.store.updateSFTPProfile(
                                profile,
                                host: host,
                                port: port,
                                basePath: basePath,
                                username: username,
                                credential: credential,
                                hostKeyFingerprintSHA256:
                                    fingerprint
                            )
                        }
                    }
                )
            case .onedrive:
                let controller =
                    MacOneDriveProfileSetupViewController(
                        setupCoordinator:
                            oneDriveProfileSetupCoordinator,
                        store: store,
                        appRuntimeFlags: appRuntimeFlags,
                        editingProfile: profile,
                        canPerformActions: { [weak self] in
                            self?.canMutate(profile) == true
                        }
                    ) { [weak self] record in
                        self?.selectedProfileID = record.id
                    }
                presentAsSheet(controller)
        }
    }

    private func chooseExternalFolder(
        title: String,
        message: String,
        save: @escaping (URL) throws -> Void
    ) {
        let panel = NSOpenPanel()
        panel.title = title
        panel.message = message
        panel.prompt = String(
            localized: "common.choose",
            defaultValue: "Choose"
        )
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true

        let complete: (NSApplication.ModalResponse) -> Void = {
            [weak self, weak panel] response in
            guard response == .OK, let url = panel?.url else {
                return
            }
            do {
                try save(url)
            } catch {
                self?.renderDetail()
                self?.present(error: error)
            }
        }

        if let window = view.window {
            panel.beginSheetModal(
                for: window,
                completionHandler: complete
            )
        } else {
            complete(panel.runModal())
        }
    }

    private func withProfileMutation<T>(
        _ profile: ServerProfileRecord,
        _ body: () throws -> T
    ) throws -> T {
        guard canMutate(profile) else {
            throw MacProfileMutationError.taskInProgress
        }
        guard let value = try appRuntimeFlags
            .withProfileMutationLease(
                profileID: profile.id,
                body
            ) else {
            throw MacProfileMutationError.taskInProgress
        }
        return value
    }

    private func withNewProfileMutation<T>(
        _ body: () throws -> T
    ) throws -> T {
        guard canCreateProfile else {
            throw MacProfileMutationError.taskInProgress
        }
        guard let value = try appRuntimeFlags
            .withProfileMutationLease(
                profileID: nil,
                body
            ) else {
            throw MacProfileMutationError.taskInProgress
        }
        return value
    }

    @objc private func renameProfile(_ sender: Any?) {
        guard let profile = selectedProfile, let id = profile.id else { return }
        let alert = NSAlert()
        alert.messageText = String(
            localized: "auth.section.name",
            defaultValue: "Name"
        )
        let field = NSTextField(string: profile.name)
        field.frame.size = NSSize(width: 300, height: 24)
        alert.accessoryView = field
        alert.addButton(
            withTitle: String(
                localized: "common.save",
                defaultValue: "Save"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        guard alert.runModal() == .alertFirstButtonReturn else { return }
        do {
            try withProfileMutation(profile) {
                try store.renameProfile(
                    id: id,
                    newName: field.stringValue
                )
            }
        } catch {
            renderDetail()
            present(error: error)
        }
    }

    @objc private func deleteProfile(_ sender: Any?) {
        guard let profile = selectedProfile, let id = profile.id else { return }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized: "storage.detail.deleteConfirm.title",
            defaultValue: "Delete this storage?"
        )
        alert.informativeText = String(
            localized: "storage.detail.deleteConfirm.message",
            defaultValue: "The saved connection and credentials will be removed. Remote backup files are not deleted."
        )
        alert.addButton(
            withTitle: String(
                localized: "common.delete",
                defaultValue: "Delete"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        guard alert.runModal() == .alertFirstButtonReturn else { return }
        do {
            try withProfileMutation(profile) {
                try store.deleteProfile(id: id)
            }
        } catch {
            renderDetail()
            present(error: error)
        }
    }

    @objc private func openMigration(_ sender: Any?) {
        guard let selectedProfile else { return }
        onOpenLegacyMigration?(selectedProfile)
    }

    @objc private func showProfileActions(_ sender: NSButton) {
        guard let selectedProfile else { return }
        let menu = NSMenu()
        if thumbnailMaintenanceTask != nil {
            addMenuItem(
                to: menu,
                title: String(
                    localized: "common.cancel",
                    defaultValue: "Cancel"
                ),
                symbol: "xmark",
                action: #selector(cancelThumbnailMaintenance(_:))
            )
        } else {
            addMenuItem(
                to: menu,
                title: String(
                    localized: "mac.profiles.legacyMigration",
                    defaultValue: "Open Legacy Migration…"
                ),
                symbol: "externaldrive.badge.timemachine",
                action: #selector(openMigration(_:))
            )
            menu.items.last?.isEnabled = canMutate(selectedProfile)
            menu.addItem(.separator())
            addMenuItem(
                to: menu,
                title: String(
                    localized: "remoteThumbnails.maintenance.backfill",
                    defaultValue: "Backfill…"
                ),
                symbol: "photo.badge.plus",
                action: #selector(backfillThumbnails(_:))
            )
            menu.items.last?.isEnabled =
                canStartThumbnailMaintenance
            addMenuItem(
                to: menu,
                title: String(
                    localized: "remoteThumbnails.maintenance.purge",
                    defaultValue: "Purge…"
                ),
                symbol: "trash",
                action: #selector(purgeThumbnails(_:))
            )
            menu.items.last?.isEnabled =
                canStartThumbnailMaintenance
        }
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: sender.bounds.maxY + 4),
            in: sender
        )
    }

    @objc private func changeWorkerCount(_ sender: NSPopUpButton) {
        guard let profile = selectedProfile,
              let profileID = profile.id,
              NodeBackupWorkerCountSelection.allCases.indices.contains(
                  sender.indexOfSelectedItem
              ) else {
            return
        }
        let selection =
            NodeBackupWorkerCountSelection.allCases[
                sender.indexOfSelectedItem
            ]
        do {
            try withProfileMutation(profile) {
                try databaseManager.setUploadWorkerCountMode(
                    selection.persistedMode,
                    profileID: profileID
                )
                appSession.setActiveUploadWorkerCountMode(
                    selection.persistedMode,
                    profileID: profileID
                )
            }
            store.reload()
        } catch {
            renderDetail()
            present(error: error)
        }
    }

    @objc private func changeThumbnailSetting(_ sender: NSButton) {
        guard let profile = selectedProfile,
              let profileID = profile.id else {
            return
        }
        let enabled = sender.state == .on
        let shouldOfferBackfill =
            MacRemoteThumbnailSettingsPolicy.shouldOfferBackfill(
                wasEnabled: profile.generateRemoteThumbnails,
                isEnabled: enabled,
                profileIsActive: profileIsActive(profile)
            )
        do {
            try withProfileMutation(profile) {
                try databaseManager.setGenerateRemoteThumbnails(
                    enabled,
                    profileID: profileID
                )
                appSession.setActiveGenerateRemoteThumbnails(
                    enabled,
                    profileID: profileID
                )
            }
            store.reload()
            if shouldOfferBackfill {
                backfillThumbnails(nil)
            }
        } catch {
            renderDetail()
            present(error: error)
        }
    }

    private func updateThumbnailMaintenanceControls() {
        let running = thumbnailMaintenanceTask != nil
        thumbnailProgressIndicator.isHidden = !running
        thumbnailProgressLabel.isHidden = !running
    }

    private var canStartThumbnailMaintenance: Bool {
        MacRemoteThumbnailMaintenanceAvailabilityPolicy.canStart(
            taskRunning: thumbnailMaintenanceTask != nil,
            executionActive: executionActive,
            remoteMaintenanceActive:
                remoteMaintenanceController.isBusy,
            connectionActive: isConnectionActive(),
            profileIsActive: selectedProfile.map(
                profileIsActive
            ) ?? false
        )
    }

    private func profileIsActive(
        _ profile: ServerProfileRecord
    ) -> Bool {
        MacRemoteThumbnailMaintenanceContext.profileMatches(
            profile,
            current: appSession.snapshot
        )
    }

    private func thumbnailMaintenanceContext()
        -> MacRemoteThumbnailMaintenanceContext?
    {
        guard let profile = selectedProfile else {
            presentThumbnailConnectionRequired()
            return nil
        }
        guard canStartThumbnailMaintenance else {
            presentThumbnailBusy()
            return nil
        }
        guard let context =
                MacRemoteThumbnailMaintenanceContext.capture(
                    selectedProfile: profile,
                    current: appSession.snapshot
                ) else {
            presentThumbnailConnectionRequired()
            return nil
        }
        return context
    }

    private func presentThumbnailConnectionRequired() {
        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = String(
            localized: "remoteThumbnails.needConnection",
            defaultValue: "Connect this destination first"
        )
        alert.runModal()
    }

    private func validateThumbnailMaintenanceContext(
        _ context: MacRemoteThumbnailMaintenanceContext
    ) -> Bool {
        guard !isConnectionActive() else {
            presentThumbnailBusy()
            return false
        }
        guard context.isCurrent(appSession.snapshot) else {
            presentThumbnailConnectionRequired()
            return false
        }
        return true
    }

    @objc
    private func backfillThumbnails(_ sender: Any?) {
        guard let context = thumbnailMaintenanceContext() else {
            return
        }
        let snapshot = backupCoordinator
            .currentRemoteSnapshotState(since: nil)
        let expectedProfileKey = RemoteIndexSyncService
            .remoteProfileKey(context.profile)
        guard RemoteSnapshotOwnership.matches(
            ownerProfileKey: snapshot.profileKey,
            expectedProfileKey: expectedProfileKey
        ) else {
            presentThumbnailMessage(
                title: String(
                    localized: "common.error",
                    defaultValue: "Remote index unavailable"
                ),
                message: String(
                    localized: "remoteThumbnails.needConnection",
                    defaultValue: "Connect to this node first."
                )
            )
            return
        }
        let fingerprints = Array(
            Set(
                snapshot.monthDeltas.flatMap {
                    $0.assets.map(\.assetFingerprint)
                }
            )
        )
        guard !fingerprints.isEmpty else {
            presentThumbnailMessage(
                title: String(
                    localized: "remoteThumbnails.backfill.doneTitle",
                    defaultValue: "Nothing to backfill"
                ),
                message: String(
                    localized: "remoteThumbnails.backfill.empty",
                    defaultValue: "The connected backup does not contain indexed assets."
                )
            )
            return
        }

        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = String(
            localized: "remoteThumbnails.backfill.promptTitle",
            defaultValue: "Backfill shared thumbnails?"
        )
        alert.informativeText = String(
            localized: "remoteThumbnails.backfill.promptMessage",
            defaultValue: "The switch only applies to future backups. Generate thumbnails for content already backed up to this node?"
        )
        alert.addButton(
            withTitle: String(
                localized: "remoteThumbnails.maintenance.backfill",
                defaultValue: "Backfill"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        guard alert.runModal() == .alertFirstButtonReturn,
              validateThumbnailMaintenanceContext(context) else {
            return
        }
        startThumbnailBackfill(
            context: context,
            fingerprints: fingerprints
        )
    }

    private func startThumbnailBackfill(
        context: MacRemoteThumbnailMaintenanceContext,
        fingerprints: [Data]
    ) {
        guard context.isCurrent(appSession.snapshot),
              !isConnectionActive(),
              !executionActive,
              let executionClaim =
                appRuntimeFlags.tryEnterExecution() else {
            presentThumbnailBusy()
            return
        }
        appRuntimeFlags.setExecutionCancellationHandler(
            for: self,
            claim: executionClaim
        ) {
            $0.thumbnailMaintenanceTask?.cancel()
        }
        let flags = appRuntimeFlags
        let service = remoteThumbnailMaintenanceService
        thumbnailProgressIndicator.doubleValue = 0
        thumbnailProgressLabel.stringValue = String(
            localized: "remoteThumbnails.backfill.progressTitle",
            defaultValue: "Preparing thumbnails…"
        )
        thumbnailMaintenanceTask = Task { [weak self] in
            let result = await service.backfill(
                profile: context.profile,
                credential: context.credential,
                fingerprints: fingerprints
            ) { [weak self] completed, total in
                guard let self else { return }
                self.thumbnailProgressIndicator.doubleValue =
                    Double(completed) / Double(max(total, 1))
                self.thumbnailProgressLabel.stringValue =
                    "\(completed) / \(total)"
            }
            flags.exitExecution(executionClaim)
            guard let self else { return }
            let wasCancelled = Task.isCancelled
            self.finishThumbnailMaintenance()
            guard !wasCancelled else { return }
            if result.failed > 0 {
                self.presentThumbnailMessage(
                    title: String(
                        localized: "common.error",
                        defaultValue: "Thumbnail backfill incomplete"
                    ),
                    message: String.localizedStringWithFormat(
                        String(
                            localized:
                                "remoteThumbnails.backfill.failedMessage",
                            defaultValue: "Generated %1$lld, skipped %2$lld, failed %3$lld."
                        ),
                        Int64(result.generated),
                        Int64(result.skipped),
                        Int64(result.failed)
                    )
                )
            } else {
                self.presentThumbnailMessage(
                    title: String(
                        localized: "remoteThumbnails.backfill.doneTitle",
                        defaultValue: "Thumbnail backfill complete"
                    ),
                    message: String.localizedStringWithFormat(
                        String(
                            localized:
                                "remoteThumbnails.backfill.doneMessage",
                            defaultValue: "Generated %1$lld, skipped %2$lld."
                        ),
                        Int64(result.generated),
                        Int64(result.skipped)
                    )
                )
            }
        }
        updateThumbnailMaintenanceControls()
    }

    @objc
    private func purgeThumbnails(_ sender: Any?) {
        guard let context = thumbnailMaintenanceContext() else {
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized: "remoteThumbnails.purge.confirmTitle",
            defaultValue: "Purge all shared thumbnails?"
        )
        alert.informativeText = String(
            localized: "remoteThumbnails.purge.confirmMessage",
            defaultValue: "This removes the regenerable thumbnail sidecar tree from the selected destination. Backup originals and manifests are not changed."
        )
        alert.addButton(
            withTitle: String(
                localized: "remoteThumbnails.maintenance.purge",
                defaultValue: "Purge"
            )
        )
        alert.addButton(
            withTitle: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            )
        )
        guard alert.runModal() == .alertFirstButtonReturn,
              validateThumbnailMaintenanceContext(context) else {
            return
        }
        guard context.isCurrent(appSession.snapshot),
              !isConnectionActive(),
              !executionActive,
              let executionClaim =
                appRuntimeFlags.tryEnterExecution() else {
            presentThumbnailBusy()
            return
        }
        appRuntimeFlags.setExecutionCancellationHandler(
            for: self,
            claim: executionClaim
        ) {
            $0.thumbnailMaintenanceTask?.cancel()
        }
        let flags = appRuntimeFlags
        let service = remoteThumbnailMaintenanceService
        thumbnailProgressIndicator.doubleValue = 0
        thumbnailProgressIndicator.isIndeterminate = true
        thumbnailProgressIndicator.startAnimation(nil)
        thumbnailProgressLabel.stringValue = String(
            localized: "remoteThumbnails.purge.progressTitle",
            defaultValue: "Purging shared thumbnails…"
        )
        thumbnailMaintenanceTask = Task { [weak self] in
            let outcome = await service.purge(
                profile: context.profile,
                credential: context.credential
            )
            flags.exitExecution(executionClaim)
            guard let self else { return }
            self.thumbnailProgressIndicator.stopAnimation(nil)
            self.thumbnailProgressIndicator.isIndeterminate = false
            self.finishThumbnailMaintenance()
            guard outcome == .failed else { return }
            self.presentThumbnailMessage(
                title: String(
                    localized: "common.error",
                    defaultValue: "Purge incomplete"
                ),
                message: String(
                    localized: "remoteThumbnails.purge.failed",
                    defaultValue: "Some thumbnail files could not be removed. Check the connection and try again."
                )
            )
        }
        updateThumbnailMaintenanceControls()
    }

    @objc
    private func cancelThumbnailMaintenance(_ sender: Any?) {
        thumbnailMaintenanceTask?.cancel()
        thumbnailProgressLabel.stringValue = String(
            localized: "backup.session.stopping",
            defaultValue: "Stopping safely…"
        )
    }

    private func finishThumbnailMaintenance() {
        thumbnailMaintenanceTask = nil
        updateThumbnailMaintenanceControls()
        guard closeWhenThumbnailMaintenanceFinishes else { return }
        closeWhenThumbnailMaintenanceFinishes = false
        view.window?.performClose(nil)
    }

    private func presentThumbnailBusy() {
        presentThumbnailMessage(
            title: String(
                localized: "common.error"
            ),
            message: String(
                localized: "mediaBrowser.action.taskInProgress",
                defaultValue: "A task is already running. Try again later."
            )
        )
    }

    private func presentThumbnailMessage(
        title: String,
        message: String
    ) {
        let alert = NSAlert()
        alert.messageText = title
        alert.informativeText = message
        alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        if let window = view.window {
            alert.beginSheetModal(for: window)
        } else {
            alert.runModal()
        }
    }

    private func present(error: Error) {
        guard view.window != nil else { return }
        NSAlert(error: error).beginSheetModal(for: view.window!)
    }

    #if DEBUG
    func showDemoSFTPProfileSheet() {
        addSFTP(nil)
    }

    func showDemoOneDriveProfileSheet() {
        addOneDrive(nil)
    }
    #endif
}

extension MacProfileManagementViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard thumbnailMaintenanceTask != nil else { return true }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mediaBrowser.action.taskInProgress",
            defaultValue: "A task is already running. Try again later."
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
        let stopConfirmed =
            alert.runModal() == .alertFirstButtonReturn
        switch MacRunningTaskWindowClosePolicy.action(
            stopConfirmed: stopConfirmed,
            isTaskRunning: thumbnailMaintenanceTask != nil
        ) {
        case .keepOpen:
            return false
        case .close:
            return true
        case .stopThenClose:
            closeWhenThumbnailMaintenanceFinishes = true
            cancelThumbnailMaintenance(nil)
            return false
        }
    }
}

extension MacProfileManagementViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        profiles.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard profiles.indices.contains(row) else { return nil }
        let profile = profiles[row]
        let identifier = NSUserInterfaceItemIdentifier("profileCell")
        let cell = tableView.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? NSTableCellView ?? NSTableCellView()
        cell.identifier = identifier

        let symbol: NSImageView
        let labels: NSStackView
        if let imageView = cell.imageView,
           let existingLabels = cell.subviews.first(
               where: { $0 is NSStackView }
           ) as? NSStackView {
            symbol = imageView
            labels = existingLabels
        } else {
            symbol = NSImageView()
            symbol.translatesAutoresizingMaskIntoConstraints = false
            symbol.symbolConfiguration = NSImage.SymbolConfiguration(
                pointSize: 18,
                weight: .medium
            )
            symbol.contentTintColor = .secondaryLabelColor
            cell.imageView = symbol
            cell.addSubview(symbol)

            let title = NSTextField(labelWithString: "")
            title.font = .systemFont(ofSize: 13, weight: .medium)
            title.lineBreakMode = .byTruncatingTail
            let subtitle = NSTextField(labelWithString: "")
            subtitle.font = .systemFont(ofSize: 11)
            subtitle.textColor = .secondaryLabelColor
            subtitle.lineBreakMode = .byTruncatingMiddle
            labels = NSStackView(views: [title, subtitle])
            labels.translatesAutoresizingMaskIntoConstraints = false
            labels.orientation = .vertical
            labels.alignment = .leading
            labels.spacing = 2
            cell.addSubview(labels)
            NSLayoutConstraint.activate([
                symbol.leadingAnchor.constraint(
                    equalTo: cell.leadingAnchor,
                    constant: 8
                ),
                symbol.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                ),
                symbol.widthAnchor.constraint(equalToConstant: 26),
                symbol.heightAnchor.constraint(equalToConstant: 26),
                labels.leadingAnchor.constraint(
                    equalTo: symbol.trailingAnchor,
                    constant: 8
                ),
                labels.trailingAnchor.constraint(
                    equalTo: cell.trailingAnchor,
                    constant: -8
                ),
                labels.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                )
            ])
        }
        symbol.image = NSImage(
            systemSymbolName: profile.resolvedStorageType.symbolName,
            accessibilityDescription: nil
        )
        (labels.arrangedSubviews[0] as? NSTextField)?.stringValue =
            profile.name
        (labels.arrangedSubviews[1] as? NSTextField)?.stringValue =
            StorageProfile(record: profile).displaySubtitle
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        let row = tableView.selectedRow
        selectedProfileID = profiles.indices.contains(row)
            ? profiles[row].id
            : nil
        renderDetail()
    }

    func tableView(
        _ tableView: NSTableView,
        pasteboardWriterForRow row: Int
    ) -> NSPasteboardWriting? {
        guard profiles.indices.contains(row),
              canMutate(profiles[row]),
              let profileID = profiles[row].id else {
            return nil
        }
        let item = NSPasteboardItem()
        item.setString(
            String(profileID),
            forType: Self.profileRowPasteboardType
        )
        return item
    }

    func tableView(
        _ tableView: NSTableView,
        validateDrop info: NSDraggingInfo,
        proposedRow row: Int,
        proposedDropOperation dropOperation:
            NSTableView.DropOperation
    ) -> NSDragOperation {
        guard dropOperation == .above,
              let sourceIndex = draggedProfileIndex(from: info),
              profiles.indices.contains(sourceIndex),
              canMutate(profiles[sourceIndex]),
              row != sourceIndex,
              row != sourceIndex + 1,
              isDropPosition(
                row,
                inStorageType:
                    profiles[sourceIndex].resolvedStorageType
              ) else {
            return []
        }
        return .move
    }

    func tableView(
        _ tableView: NSTableView,
        acceptDrop info: NSDraggingInfo,
        row: Int,
        dropOperation: NSTableView.DropOperation
    ) -> Bool {
        guard dropOperation == .above,
              let sourceIndex = draggedProfileIndex(from: info),
              profiles.indices.contains(sourceIndex),
              canMutate(profiles[sourceIndex]),
              isDropPosition(
                row,
                inStorageType:
                    profiles[sourceIndex].resolvedStorageType
              ) else {
            return false
        }
        var reorderedProfiles = profiles
        let moved = reorderedProfiles.remove(at: sourceIndex)
        let insertionIndex = min(
            max(0, row > sourceIndex ? row - 1 : row),
            reorderedProfiles.count
        )
        reorderedProfiles.insert(moved, at: insertionIndex)
        do {
            try withNewProfileMutation {
                try databaseManager.saveServerProfileSortOrder(
                    profileIDs:
                        reorderedProfiles.compactMap(\.id)
                )
            }
            profiles = reorderedProfiles
            selectedProfileID = moved.id
            store.reload()
            return true
        } catch {
            store.reload()
            present(error: error)
            return false
        }
    }

    private func draggedProfileIndex(
        from info: NSDraggingInfo
    ) -> Int? {
        guard info.draggingSource as? NSTableView === tableView,
              let value = info.draggingPasteboard.string(
                forType: Self.profileRowPasteboardType
              ),
              let profileID = Int64(value) else {
            return nil
        }
        return profiles.firstIndex { $0.id == profileID }
    }

    private func isDropPosition(
        _ row: Int,
        inStorageType storageType: StorageType
    ) -> Bool {
        guard (0...profiles.count).contains(row) else {
            return false
        }
        let previousType = row > 0
            ? profiles[row - 1].resolvedStorageType
            : nil
        let nextType = row < profiles.count
            ? profiles[row].resolvedStorageType
            : nil
        return previousType == storageType
            || nextType == storageType
    }
}
