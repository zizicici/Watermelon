import AppKit

@MainActor
final class MacSMBProfileViewController:
    NSViewController,
    NSTextFieldDelegate,
    NSTableViewDataSource,
    NSTableViewDelegate
{
    private enum Step {
        case discovery
        case editor
        case share
        case folder
    }

    private enum FolderState {
        case loading
        case loaded([RemoteStorageEntry])
        case failed(String)
    }

    private enum FolderRow {
        case selectCurrent
        case parent
        case directory(RemoteStorageEntry)
        case status(
            title: String,
            detail: String?,
            retryable: Bool
        )
    }

    private let initial: SMBServerPathContext?
    private let editorMode: MacProfileConnectionEditorMode
    private let canPerformActions: () -> Bool
    private let save: (_ context: SMBServerPathContext) throws -> Void
    private let setupService = SMBSetupService()
    private let discoveryModel = SMBDiscoveryViewModel()

    private let titleLabel = NSTextField(labelWithString: "")
    private let backButton = NSButton()
    private let progressIndicator = NSProgressIndicator()
    private let contentContainer = NSView()
    private let errorLabel = NSTextField(wrappingLabelWithString: "")
    private let actionStack = NSStackView()

    private let nameField = NSTextField()
    private let hostField = NSTextField()
    private let portField = NSTextField()
    private let usernameField = NSTextField()
    private let passwordField = NSSecureTextField()
    private let domainField = NSTextField()
    private let shareButton = NSButton()
    private let folderButton = NSButton()
    private let saveButton = NSButton()

    private let discoveryTable = NSTableView()
    private let shareTable = NSTableView()
    private let folderTable = NSTableView()
    private let folderPathLabel = NSTextField(labelWithString: "")

    private var step: Step
    private var selectedShareName: String?
    private var selectedBasePath: String
    private var selectionBinding = SMBSelectionContextBinding()
    private var availableShares: [SMBShareInfo] = []
    private var folderAuth: SMBServerAuthContext?
    private var folderShareName: String?
    private var folderCurrentPath = "/"
    private var folderState: FolderState = .loading
    private var shareTask: Task<Void, Never>?
    private var shareRequestID: UUID?
    private var folderTask: Task<Void, Never>?
    private var folderRequestID: UInt64 = 0
    private var discoveryStarted = false
    private var isWorking = false
    private var activityObserver:
        MacProfileMutationActivityObserver?

    init(
        initial: SMBServerPathContext? = nil,
        canPerformActions: @escaping () -> Bool = { true },
        save: @escaping (_ context: SMBServerPathContext) throws -> Void
    ) {
        self.initial = initial
        editorMode = MacProfileConnectionEditorMode(
            hasEditingProfile: initial != nil
        )
        self.canPerformActions = canPerformActions
        self.save = save
        step = initial == nil ? .discovery : .editor
        selectedShareName = initial?.shareName
        selectedBasePath =
            (try? SMBPathCanonicalizer.canonicalRawPath(
                initial?.basePath ?? "/"
            )) ?? "/"
        super.init(nibName: nil, bundle: nil)

        nameField.stringValue = initial?.auth.name ?? ""
        hostField.stringValue = initial?.auth.host ?? ""
        portField.stringValue = initial.map {
            String(SMBEndpoint.effectivePort($0.auth.port))
        } ?? String(SMBEndpoint.defaultPort)
        usernameField.stringValue = initial?.auth.username ?? ""
        passwordField.stringValue = initial?.auth.password ?? ""
        domainField.stringValue = initial?.auth.domain ?? ""

        if selectedShareName != nil,
           let auth = try? buildAuthContext() {
            selectionBinding.bind(
                to: SMBSelectionContextSignature(auth: auth)
            )
        }

        preferredContentSize = NSSize(width: 620, height: 520)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        shareTask?.cancel()
        folderTask?.cancel()
    }

    override func loadView() {
        view = NSView()

        titleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        backButton.image = NSImage(
            systemSymbolName: "chevron.left",
            accessibilityDescription: String(localized: "common.back")
        )
        backButton.bezelStyle = .accessoryBarAction
        backButton.isBordered = false
        backButton.target = self
        backButton.action = #selector(goBack(_:))

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .small
        progressIndicator.isDisplayedWhenStopped = false

        let header = NSStackView(
            views: [backButton, titleLabel, NSView(), progressIndicator]
        )
        header.orientation = .horizontal
        header.alignment = .centerY
        header.spacing = 8

        errorLabel.textColor = .wmMaterialError
        errorLabel.font = .systemFont(ofSize: 12)
        errorLabel.isSelectable = true
        errorLabel.isHidden = true

        actionStack.orientation = .horizontal
        actionStack.alignment = .centerY
        actionStack.spacing = 8

        let root = NSStackView(
            views: [
                header,
                contentContainer,
                errorLabel,
                actionStack
            ]
        )
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 14
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        header.translatesAutoresizingMaskIntoConstraints = false
        contentContainer.translatesAutoresizingMaskIntoConstraints = false
        actionStack.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor, constant: 22),
            root.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 24
            ),
            root.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -24
            ),
            root.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -20
            ),
            header.widthAnchor.constraint(equalTo: root.widthAnchor),
            contentContainer.widthAnchor.constraint(
                equalTo: root.widthAnchor
            ),
            actionStack.widthAnchor.constraint(equalTo: root.widthAnchor)
        ])

        configureTables()
        configureFields()
        discoveryModel.onChange = { [weak self] in
            self?.applyDiscoveryState()
        }
        activityObserver = MacProfileMutationActivityObserver {
            [weak self] in
            self?.updateEditorActions()
        }
        renderStep()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        if step == .discovery, !discoveryStarted {
            discoveryStarted = true
            discoveryModel.startDiscovery()
        } else if step == .editor {
            focusEditor()
        }
    }

    override func viewWillDisappear() {
        super.viewWillDisappear()
        cancelAsyncWork()
    }

    func controlTextDidChange(_ notification: Notification) {
        guard let field = notification.object as? NSTextField else {
            return
        }
        if field === hostField
            || field === portField
            || field === usernameField
            || field === passwordField
            || field === domainField {
            connectionFieldsDidChange()
        }
        updateEditorActions()
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView === discoveryTable {
            return max(discoveryModel.rows.count, 1)
        }
        if tableView === shareTable {
            return availableShares.count
        }
        if tableView === folderTable {
            return folderRows.count
        }
        return 0
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        if tableView === discoveryTable {
            return discoveryCell(row: row)
        }
        if tableView === shareTable {
            return shareCell(row: row)
        }
        if tableView === folderTable {
            return folderCell(row: row)
        }
        return nil
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard let tableView = notification.object as? NSTableView else {
            return
        }
        let row = tableView.selectedRow
        guard row >= 0 else { return }
        tableView.deselectRow(row)

        if tableView === discoveryTable {
            selectDiscoveredService(at: row)
        } else if tableView === shareTable {
            selectShare(at: row)
        } else if tableView === folderTable {
            selectFolderRow(at: row)
        }
    }

    private func configureFields() {
        configure(
            nameField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.name"
            )
        )
        configure(
            hostField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.host"
            )
        )
        configure(
            portField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.port"
            )
        )
        configure(
            usernameField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.username"
            )
        )
        configure(
            passwordField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.password"
            )
        )
        configure(
            domainField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.domain"
            )
        )

        shareButton.bezelStyle = .rounded
        shareButton.target = self
        shareButton.action = #selector(openShareSelection(_:))

        folderButton.bezelStyle = .rounded
        folderButton.target = self
        folderButton.action = #selector(openFolderSelection(_:))

        saveButton.title = String(localized: "common.save")
        saveButton.bezelStyle = .rounded
        saveButton.bezelColor = .wmMaterialPrimary
        saveButton.target = self
        saveButton.action = #selector(commit(_:))
        saveButton.keyEquivalent = "\r"
    }

    private func configure(
        _ field: NSTextField,
        placeholder: String
    ) {
        field.placeholderString = placeholder
        field.delegate = self
        field.lineBreakMode = .byTruncatingTail
    }

    private func configureTables() {
        configureTable(discoveryTable, rowHeight: 50)
        configureTable(shareTable, rowHeight: 50)
        configureTable(folderTable, rowHeight: 44)
    }

    private func configureTable(
        _ tableView: NSTableView,
        rowHeight: CGFloat
    ) {
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(
            "main"
        ))
        column.resizingMask = .autoresizingMask
        tableView.addTableColumn(column)
        tableView.headerView = nil
        tableView.rowHeight = rowHeight
        tableView.intercellSpacing = NSSize(width: 0, height: 1)
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.selectionHighlightStyle = .regular
        tableView.dataSource = self
        tableView.delegate = self
    }

    private func renderStep() {
        clearError()
        contentContainer.subviews.forEach { $0.removeFromSuperview() }
        actionStack.arrangedSubviews.forEach {
            actionStack.removeArrangedSubview($0)
            $0.removeFromSuperview()
        }

        let content: NSView
        switch step {
        case .discovery:
            titleLabel.stringValue = String(
                localized: "auth.smb.discovery.title"
            )
            backButton.isHidden = true
            content = makeDiscoveryView()
            preferredContentSize = NSSize(width: 620, height: 520)
        case .editor:
            titleLabel.stringValue =
                initial == nil
                ? String(localized: "auth.smb.login.title")
                : String(localized: "auth.smb.login.editTitle")
            backButton.isHidden = initial != nil
            content = makeEditorView()
            preferredContentSize = NSSize(
                width: 620,
                height: editorMode.showsNameField ? 500 : 460
            )
        case .share:
            titleLabel.stringValue = String(
                localized: "auth.smb.share.title"
            )
            backButton.isHidden = false
            content = makeShareView()
            preferredContentSize = NSSize(width: 620, height: 520)
        case .folder:
            titleLabel.stringValue = String(
                localized: "auth.smb.folder.title"
            )
            backButton.isHidden = false
            content = makeFolderView()
            preferredContentSize = NSSize(width: 620, height: 540)
        }

        content.translatesAutoresizingMaskIntoConstraints = false
        contentContainer.addSubview(content)
        NSLayoutConstraint.activate([
            content.topAnchor.constraint(
                equalTo: contentContainer.topAnchor
            ),
            content.leadingAnchor.constraint(
                equalTo: contentContainer.leadingAnchor
            ),
            content.trailingAnchor.constraint(
                equalTo: contentContainer.trailingAnchor
            ),
            content.bottomAnchor.constraint(
                equalTo: contentContainer.bottomAnchor
            )
        ])

        rebuildActions()
        applyDiscoveryState()
        updateEditorActions()
        if step == .editor {
            DispatchQueue.main.async { [weak self] in
                self?.focusEditor()
            }
        }
    }

    private func makeDiscoveryView() -> NSView {
        let refreshButton = NSButton(
            image: NSImage(
                systemSymbolName: "arrow.clockwise",
                accessibilityDescription: String(localized: "common.refresh")
            ) ?? NSImage(),
            target: self,
            action: #selector(refreshDiscovery(_:))
        )
        refreshButton.bezelStyle = .accessoryBarAction
        refreshButton.isBordered = false
        refreshButton.toolTip = String(localized: "common.refresh")

        let sectionLabel = NSTextField(
            labelWithString: String(
                localized: "auth.smb.discovery.sectionTitle"
            )
        )
        sectionLabel.font = .systemFont(ofSize: 12, weight: .medium)
        sectionLabel.textColor = .secondaryLabelColor

        let heading = NSStackView(
            views: [sectionLabel, NSView(), refreshButton]
        )
        heading.orientation = .horizontal
        heading.alignment = .centerY

        let scrollView = makeScrollView(for: discoveryTable)
        let stack = NSStackView(views: [heading, scrollView])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            heading.widthAnchor.constraint(equalTo: stack.widthAnchor),
            scrollView.widthAnchor.constraint(equalTo: stack.widthAnchor),
            scrollView.heightAnchor.constraint(greaterThanOrEqualToConstant: 330)
        ])
        return stack
    }

    private func makeEditorView() -> NSView {
        var rows: [[NSView]] = []
        if editorMode.showsNameField {
            rows.append([
                makeFieldLabel(String(localized: "auth.section.name")),
                nameField
            ])
        }
        rows.append(contentsOf: [
            [
                makeFieldLabel(String(localized: "auth.field.host")),
                hostField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.port")),
                portField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.username")),
                usernameField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.password")),
                passwordField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.domain")),
                domainField
            ],
            [
                makeFieldLabel(String(localized: "auth.smb.share.title")),
                shareButton
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.smb.folder.sectionTitle")
                ),
                folderButton
            ]
        ])
        let grid = NSGridView(views: rows)
        grid.rowSpacing = 10
        grid.columnSpacing = 14
        grid.column(at: 0).xPlacement = .trailing
        grid.column(at: 1).xPlacement = .fill
        grid.translatesAutoresizingMaskIntoConstraints = false
        return grid
    }

    private func makeShareView() -> NSView {
        let scrollView = makeScrollView(for: shareTable)
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        scrollView.heightAnchor.constraint(
            greaterThanOrEqualToConstant: 350
        ).isActive = true
        return scrollView
    }

    private func makeFolderView() -> NSView {
        folderPathLabel.stringValue = String.localizedStringWithFormat(
            String(localized: "auth.smb.share.currentPath"),
            folderCurrentPath
        )
        folderPathLabel.font = .monospacedSystemFont(
            ofSize: 12,
            weight: .regular
        )
        folderPathLabel.textColor = .secondaryLabelColor
        folderPathLabel.lineBreakMode = .byTruncatingMiddle

        let scrollView = makeScrollView(for: folderTable)
        let stack = NSStackView(views: [folderPathLabel, scrollView])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            folderPathLabel.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            scrollView.widthAnchor.constraint(equalTo: stack.widthAnchor),
            scrollView.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 350
            )
        ])
        return stack
    }

    private func rebuildActions() {
        if step == .discovery {
            let manualButton = NSButton(
                title: String(localized: "home.menu.smbManual"),
                target: self,
                action: #selector(useManualEntry(_:))
            )
            manualButton.bezelStyle = .rounded
            actionStack.addArrangedSubview(manualButton)
        }

        actionStack.addArrangedSubview(NSView())
        let cancelButton = NSButton(
            title: String(localized: "common.cancel"),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded
        cancelButton.keyEquivalent = "\u{1b}"
        actionStack.addArrangedSubview(cancelButton)

        if step == .editor {
            actionStack.addArrangedSubview(saveButton)
        }
    }

    private func makeFieldLabel(_ text: String) -> NSTextField {
        let label = NSTextField(labelWithString: text)
        label.textColor = .secondaryLabelColor
        label.alignment = .right
        return label
    }

    private func makeScrollView(for tableView: NSTableView) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.borderType = .bezelBorder
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.documentView = tableView
        return scrollView
    }

    private func discoveryCell(row: Int) -> NSView {
        if discoveryModel.rows.isEmpty {
            let title: String
            let detail: String?
            if discoveryModel.isShowingLoading {
                title = String(localized: "auth.smb.discovery.searching")
                detail = nil
            } else if let error = discoveryModel.browserError {
                title = String(
                    localized: "auth.smb.discovery.discoveryFailed"
                )
                detail = error
            } else {
                title = String(
                    localized: "auth.smb.discovery.noServices"
                )
                detail = String(
                    localized: "auth.smb.discovery.noServicesHint"
                )
            }
            return makeCell(
                in: discoveryTable,
                title: title,
                detail: detail,
                symbol: nil,
                enabled: false
            )
        }

        guard discoveryModel.rows.indices.contains(row) else {
            return NSView()
        }
        let service = discoveryModel.rows[row]
        let detail: String
        if let host = service.host, let port = service.port {
            detail = "\(host):\(port)"
        } else if let error = service.error {
            detail = error
        } else {
            detail = String(
                localized: "auth.smb.discovery.resolving"
            )
        }
        return makeCell(
            in: discoveryTable,
            title: service.name,
            detail: detail,
            symbol: StorageType.smb.symbolName,
            enabled: service.isReady
        )
    }

    private func shareCell(row: Int) -> NSView {
        guard availableShares.indices.contains(row) else {
            return NSView()
        }
        let share = availableShares[row]
        return makeCell(
            in: shareTable,
            title: share.name,
            detail: share.comment.isEmpty ? nil : share.comment,
            symbol: share.name == selectedShareName
                ? "checkmark"
                : "externaldrive.connected.to.line.below",
            enabled: true
        )
    }

    private func folderCell(row: Int) -> NSView {
        guard folderRows.indices.contains(row) else {
            return NSView()
        }
        switch folderRows[row] {
        case .selectCurrent:
            return makeCell(
                in: folderTable,
                title: String(
                    localized: "auth.smb.folder.selectCurrent"
                ),
                detail: nil,
                symbol: "checkmark",
                enabled: folderIsLoaded
            )
        case .parent:
            return makeCell(
                in: folderTable,
                title: String(localized: "auth.smb.share.parentDir"),
                detail: nil,
                symbol: "arrow.up",
                enabled: true
            )
        case .directory(let entry):
            return makeCell(
                in: folderTable,
                title: entry.name,
                detail: entry.path,
                symbol: "folder",
                enabled: true
            )
        case .status(let title, let detail, let retryable):
            return makeCell(
                in: folderTable,
                title: title,
                detail: detail,
                symbol: nil,
                enabled: retryable
            )
        }
    }

    private func makeCell(
        in tableView: NSTableView,
        title: String,
        detail: String?,
        symbol: String?,
        enabled: Bool
    ) -> NSView {
        let identifier = NSUserInterfaceItemIdentifier("SMBTableCell")
        let cell =
            tableView.makeView(
                withIdentifier: identifier,
                owner: self
            ) as? MacSMBTableCellView
            ?? MacSMBTableCellView(identifier: identifier)
        cell.apply(
            title: title,
            detail: detail,
            symbol: symbol,
            enabled: enabled
        )
        return cell
    }

    private var folderRows: [FolderRow] {
        var rows: [FolderRow] = [.selectCurrent]
        if folderCurrentPath != "/" {
            rows.append(.parent)
        }
        switch folderState {
        case .loading:
            rows.append(
                .status(
                    title: String(localized: "smb.path.loading"),
                    detail: nil,
                    retryable: false
                )
            )
        case .failed(let message):
            rows.append(
                .status(
                    title: String(
                        localized: "auth.smb.share.readFailed"
                    ),
                    detail: message,
                    retryable: true
                )
            )
        case .loaded(let entries):
            if entries.isEmpty {
                rows.append(
                    .status(
                        title: String(localized: "common.none"),
                        detail: nil,
                        retryable: false
                    )
                )
            } else {
                rows.append(contentsOf: entries.map(FolderRow.directory))
            }
        }
        return rows
    }

    private var folderIsLoaded: Bool {
        if case .loaded = folderState {
            return true
        }
        return false
    }

    private func selectDiscoveredService(at row: Int) {
        guard discoveryModel.rows.indices.contains(row) else { return }
        let service = discoveryModel.rows[row]
        guard let host = service.host, let port = service.port else {
            showError(
                String(localized: "auth.smb.discovery.notReadyMessage")
            )
            return
        }
        if nameField.stringValue.trimmed.isEmpty {
            nameField.stringValue = service.name
        }
        hostField.stringValue = host
        portField.stringValue = String(port)
        discoveryModel.stopDiscovery(clearRows: false)
        step = .editor
        renderStep()
    }

    private func selectShare(at row: Int) {
        guard availableShares.indices.contains(row),
              let auth = try? buildAuthContext() else {
            return
        }
        let name = availableShares[row].name
        if selectedShareName != name {
            selectedBasePath = "/"
        }
        selectedShareName = name
        selectionBinding.bind(
            to: SMBSelectionContextSignature(auth: auth)
        )
        step = .editor
        renderStep()
    }

    private func selectFolderRow(at row: Int) {
        guard folderRows.indices.contains(row) else { return }
        switch folderRows[row] {
        case .selectCurrent:
            guard folderIsLoaded,
                  let auth = try? buildAuthContext(),
                  selectionBinding.matches(
                    SMBSelectionContextSignature(auth: auth)
                  ) else {
                return
            }
            selectedBasePath = folderCurrentPath
            cancelFolderLoading()
            step = .editor
            renderStep()
        case .parent:
            navigateFolder(to: parentPath(of: folderCurrentPath))
        case .directory(let entry):
            navigateFolder(to: entry.path)
        case .status(_, _, let retryable):
            if retryable {
                loadFolders()
            }
        }
    }

    private func connectionFieldsDidChange() {
        guard selectionBinding.isBound else { return }
        let signature = (try? buildAuthContext()).map {
            SMBSelectionContextSignature(auth: $0)
        }
        if selectionBinding.invalidateIfMismatched(signature) {
            selectedShareName = nil
            selectedBasePath = "/"
            updateEditorActions()
        }
    }

    private func invalidateSelection() {
        selectionBinding.clear()
        selectedShareName = nil
        selectedBasePath = "/"
        updateEditorActions()
    }

    private func buildAuthContext() throws -> SMBServerAuthContext {
        guard let host = RemoteHostEndpoint.socketHost(
            hostField.stringValue,
            strippingSMBScheme: true
        ) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let username = usernameField.stringValue.trimmed
        let domain = domainField.stringValue.trimmed
        guard !host.isEmpty, !username.isEmpty else {
            throw NSError(
                domain: "MacSMBProfile",
                code: 1,
                userInfo: [
                    NSLocalizedDescriptionKey: String(
                        localized: "auth.smb.login.validation"
                    )
                ]
            )
        }

        let portText = portField.stringValue.trimmed
        let port: Int
        if portText.isEmpty {
            port = SMBEndpoint.defaultPort
        } else {
            guard let parsed = Int(portText),
                  (1 ... 65_535).contains(parsed) else {
                throw NSError(
                    domain: "MacSMBProfile",
                    code: 2,
                    userInfo: [
                        NSLocalizedDescriptionKey: String(
                            localized: "auth.webdav.validationPort"
                        )
                    ]
                )
            }
            port = parsed
        }

        let name = nameField.stringValue.trimmed
        return SMBServerAuthContext(
            name: name.isEmpty ? host : name,
            host: host,
            port: port,
            username: username,
            password: passwordField.stringValue,
            domain: domain.isEmpty ? nil : domain
        )
    }

    private func updateEditorActions() {
        let actionsAllowed = canPerformActions()
        shareButton.title =
            selectedShareName
            ?? String(localized: "auth.smb.share.noShareSelected")
        folderButton.title = selectedBasePath
        folderButton.isEnabled =
            actionsAllowed && !isWorking && selectedShareName != nil
        shareButton.isEnabled = actionsAllowed && !isWorking

        let signature = (try? buildAuthContext()).map {
            SMBSelectionContextSignature(auth: $0)
        }
        saveButton.isEnabled =
            actionsAllowed
            && !isWorking
            && selectedShareName != nil
            && signature.map(selectionBinding.matches) == true
    }

    private func applyDiscoveryState() {
        discoveryTable.reloadData()
        if step == .discovery, discoveryModel.isShowingLoading {
            progressIndicator.startAnimation(nil)
        } else if !isWorking && step != .folder {
            progressIndicator.stopAnimation(nil)
        }
    }

    private func focusEditor() {
        guard step == .editor else { return }
        view.window?.makeFirstResponder(
            editorMode.showsNameField
                && !hostField.stringValue.isEmpty
                ? nameField
                : hostField
        )
    }

    private func setWorking(_ working: Bool) {
        isWorking = working
        [
            nameField,
            hostField,
            portField,
            usernameField,
            passwordField,
            domainField
        ].forEach { $0.isEnabled = !working }
        backButton.isEnabled = !working
        if working {
            progressIndicator.startAnimation(nil)
        } else if !discoveryModel.isShowingLoading {
            progressIndicator.stopAnimation(nil)
        }
        updateEditorActions()
    }

    private func showError(_ message: String) {
        errorLabel.stringValue = message
        errorLabel.isHidden = false
    }

    private func clearError() {
        errorLabel.stringValue = ""
        errorLabel.isHidden = true
    }

    private func uniqueShares(
        _ shares: [SMBShareInfo]
    ) -> [SMBShareInfo] {
        var seen = Set<String>()
        return shares.filter { seen.insert($0.name).inserted }
    }

    private func navigateFolder(to path: String) {
        guard let canonical = try? SMBPathCanonicalizer.canonicalRawPath(
            path
        ) else {
            return
        }
        folderCurrentPath = canonical
        folderPathLabel.stringValue = String.localizedStringWithFormat(
            String(localized: "auth.smb.share.currentPath"),
            canonical
        )
        loadFolders()
    }

    private func loadFolders() {
        guard let auth = folderAuth,
              let shareName = folderShareName else {
            return
        }
        folderTask?.cancel()
        folderRequestID &+= 1
        let requestID = folderRequestID
        let requestedPath = folderCurrentPath
        folderState = .loading
        folderTable.reloadData()
        progressIndicator.startAnimation(nil)
        let setupService = setupService
        folderTask = Task { [weak self] in
            do {
                let entries = try await setupService.listDirectories(
                    auth: auth,
                    shareName: shareName,
                    path: requestedPath
                )
                try Task.checkCancellation()
                guard let self,
                      self.folderRequestID == requestID,
                      self.folderCurrentPath == requestedPath else {
                    return
                }
                self.folderTask = nil
                self.folderState = .loaded(entries)
                self.progressIndicator.stopAnimation(nil)
                self.folderTable.reloadData()
            } catch is CancellationError {
            } catch {
                guard let self,
                      self.folderRequestID == requestID,
                      self.folderCurrentPath == requestedPath else {
                    return
                }
                self.folderTask = nil
                self.folderState = .failed(
                    UserFacingErrorLocalizer.message(
                        for: error,
                        storageType: .smb
                    )
                )
                self.progressIndicator.stopAnimation(nil)
                self.folderTable.reloadData()
            }
        }
    }

    private func cancelFolderLoading() {
        folderRequestID &+= 1
        folderTask?.cancel()
        folderTask = nil
        progressIndicator.stopAnimation(nil)
    }

    private func parentPath(of path: String) -> String {
        guard let normalized =
            try? SMBPathCanonicalizer.canonicalRawPath(path),
            normalized != "/" else {
            return "/"
        }
        let parent = (normalized as NSString).deletingLastPathComponent
        return parent.isEmpty
            ? "/"
            : ((try? SMBPathCanonicalizer.canonicalRawPath(parent)) ?? "/")
    }

    private func cancelAsyncWork() {
        shareRequestID = nil
        shareTask?.cancel()
        shareTask = nil
        cancelFolderLoading()
        discoveryModel.stopDiscovery(clearRows: false)
    }

    @objc private func refreshDiscovery(_ sender: Any?) {
        discoveryStarted = true
        discoveryModel.startDiscovery()
    }

    @objc private func useManualEntry(_ sender: Any?) {
        discoveryModel.stopDiscovery(clearRows: false)
        step = .editor
        renderStep()
    }

    @objc private func goBack(_ sender: Any?) {
        clearError()
        switch step {
        case .discovery:
            return
        case .editor:
            guard initial == nil else { return }
            step = .discovery
            renderStep()
            discoveryStarted = true
            discoveryModel.startDiscovery()
        case .share:
            step = .editor
            renderStep()
        case .folder:
            cancelFolderLoading()
            step = .editor
            renderStep()
        }
    }

    @objc private func openShareSelection(_ sender: Any?) {
        clearError()
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress,
                    storageType: .smb
                )
            )
            updateEditorActions()
            return
        }
        let auth: SMBServerAuthContext
        do {
            auth = try buildAuthContext()
        } catch {
            showError(
                UserFacingErrorLocalizer.message(
                    for: error,
                    storageType: .smb
                )
            )
            return
        }

        shareTask?.cancel()
        let requestID = UUID()
        shareRequestID = requestID
        setWorking(true)
        let setupService = setupService
        shareTask = Task { [weak self] in
            do {
                let shares = try await setupService.listShares(auth: auth)
                try Task.checkCancellation()
                guard let self, self.shareRequestID == requestID else {
                    return
                }
                self.shareTask = nil
                self.shareRequestID = nil
                self.setWorking(false)
                let unique = self.uniqueShares(shares)
                guard !unique.isEmpty else {
                    self.showError(
                        String(
                            localized: "auth.smb.login.noSharesMessage"
                        )
                    )
                    return
                }
                self.availableShares = unique
                self.shareTable.reloadData()
                self.step = .share
                self.renderStep()
            } catch is CancellationError {
            } catch {
                guard let self, self.shareRequestID == requestID else {
                    return
                }
                self.shareTask = nil
                self.shareRequestID = nil
                self.setWorking(false)
                self.showError(
                    UserFacingErrorLocalizer.message(
                        for: error,
                        storageType: .smb
                    )
                )
            }
        }
    }

    @objc private func openFolderSelection(_ sender: Any?) {
        clearError()
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress,
                    storageType: .smb
                )
            )
            updateEditorActions()
            return
        }
        guard let shareName = selectedShareName else {
            showError(
                String(localized: "auth.smb.share.selectShareFirst")
            )
            return
        }

        do {
            let auth = try buildAuthContext()
            let signature = SMBSelectionContextSignature(auth: auth)
            guard selectionBinding.matches(signature) else {
                invalidateSelection()
                showError(
                    String(localized: "auth.smb.share.selectShareFirst")
                )
                return
            }
            folderAuth = auth
            folderShareName = shareName
            folderCurrentPath = selectedBasePath
            folderState = .loading
            step = .folder
            renderStep()
            loadFolders()
        } catch {
            showError(
                UserFacingErrorLocalizer.message(
                    for: error,
                    storageType: .smb
                )
            )
        }
    }

    @objc private func commit(_ sender: Any?) {
        clearError()
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress,
                    storageType: .smb
                )
            )
            updateEditorActions()
            return
        }
        guard !isWorking, let shareName = selectedShareName else {
            showError(
                String(localized: "auth.smb.share.selectShareFirst")
            )
            return
        }

        do {
            let auth = try buildAuthContext()
            guard selectionBinding.matches(
                SMBSelectionContextSignature(auth: auth)
            ) else {
                invalidateSelection()
                showError(
                    String(localized: "auth.smb.share.selectShareFirst")
                )
                return
            }
            let basePath = try SMBPathCanonicalizer.canonicalRawPath(
                selectedBasePath
            )
            setWorking(true)
            defer { setWorking(false) }
            try save(
                SMBServerPathContext(
                    auth: auth,
                    shareName: shareName,
                    basePath: basePath
                )
            )
            dismiss(self)
        } catch {
            showError(
                UserFacingErrorLocalizer.message(
                    for: error,
                    storageType: .smb
                )
            )
        }
    }

    @objc private func cancel(_ sender: Any?) {
        cancelAsyncWork()
        dismiss(self)
    }
}

private final class MacSMBTableCellView: NSTableCellView {
    private let symbolView = NSImageView()
    private let titleLabel = NSTextField(labelWithString: "")
    private let detailLabel = NSTextField(labelWithString: "")

    init(identifier: NSUserInterfaceItemIdentifier) {
        super.init(frame: .zero)
        self.identifier = identifier

        symbolView.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 14,
            weight: .regular
        )
        symbolView.contentTintColor = .secondaryLabelColor
        symbolView.setContentHuggingPriority(.required, for: .horizontal)

        titleLabel.lineBreakMode = .byTruncatingTail
        detailLabel.font = .systemFont(ofSize: 11)
        detailLabel.textColor = .secondaryLabelColor
        detailLabel.lineBreakMode = .byTruncatingMiddle

        let textStack = NSStackView(views: [titleLabel, detailLabel])
        textStack.orientation = .vertical
        textStack.alignment = .leading
        textStack.spacing = 2

        let stack = NSStackView(views: [symbolView, textStack])
        stack.orientation = .horizontal
        stack.alignment = .centerY
        stack.spacing = 10
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 10
            ),
            stack.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -10
            ),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor),
            textStack.widthAnchor.constraint(
                lessThanOrEqualTo: stack.widthAnchor,
                constant: -26
            )
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func apply(
        title: String,
        detail: String?,
        symbol: String?,
        enabled: Bool
    ) {
        titleLabel.stringValue = title
        titleLabel.textColor = enabled
            ? .labelColor
            : .secondaryLabelColor
        detailLabel.stringValue = detail ?? ""
        detailLabel.isHidden = detail == nil
        if let symbol {
            symbolView.image = NSImage(
                systemSymbolName: symbol,
                accessibilityDescription: nil
            )
            symbolView.isHidden = false
        } else {
            symbolView.image = nil
            symbolView.isHidden = true
        }
    }
}
