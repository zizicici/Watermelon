import AppKit

@MainActor
final class MacLegacyMigrationViewController: NSViewController {
    var onRepositoryChanged: (() -> Void)? {
        didSet {
            viewModel.onRepositoryChanged = onRepositoryChanged
        }
    }

    private enum PendingAction {
        case browse
        case scan
        case commit
    }

    private let profile: ServerProfileRecord
    private let profileStore: ProfileStore
    private let viewModel: LegacyMigrationViewModel
    private let pathLabel = NSTextField(labelWithString: "")
    private let browseButton = NSButton()
    private let skipPerceptualCheckbox = NSButton()
    private let replaceSubsetsCheckbox = NSButton()
    private let scanButton = NSButton()
    private let commitButton = NSButton()
    private let scanSpinner = NSProgressIndicator()
    private let scanningLabel = NSTextField(labelWithString: "")
    private let cancelScanButton = NSButton()
    private let contentSeparator = NSBox()
    private let contentContainer = NSView()
    private let progressView = NSView()
    private let progressSpinner = NSProgressIndicator()
    private let progressIcon = NSImageView()
    private let progressTitle = NSTextField(labelWithString: "")
    private let progressActionButton = NSButton()
    private let progressIndicator = NSProgressIndicator()
    private let progressContext = NSTextField(labelWithString: "")
    private let progressCount = NSTextField(labelWithString: "")
    private let importedValue = NSTextField(labelWithString: "")
    private let skippedValue = NSTextField(labelWithString: "")
    private let failedValue = NSTextField(labelWithString: "")
    private let uploadedValue = NSTextField(labelWithString: "")
    private var scanResultController:
        MacLegacyScanResultViewController?
    private var pendingAction: PendingAction?
    private var connectionTask: Task<Void, Never>?
    private var closeWhenFinished = false

    init(
        profile: ServerProfileRecord,
        storageClientFactory: StorageClientFactory,
        profileStore: ProfileStore,
        appRuntimeFlags: AppRuntimeFlags
    ) {
        self.profile = profile
        self.profileStore = profileStore
        viewModel = LegacyMigrationViewModel(
            profile: profile,
            storageClientFactory: storageClientFactory,
            profileStore: profileStore,
            appRuntimeFlags: appRuntimeFlags
        )
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        connectionTask?.cancel()
    }

    override func loadView() {
        view = NSView()

        let header = makeProfileHeader()
        let source = makeSourceSection()
        configureProgressView()

        let headerSeparator = NSBox()
        headerSeparator.boxType = .separator
        contentSeparator.boxType = .separator

        contentContainer.addSubview(progressView)
        progressView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            progressView.topAnchor.constraint(
                equalTo: contentContainer.topAnchor
            ),
            progressView.leadingAnchor.constraint(
                equalTo: contentContainer.leadingAnchor
            ),
            progressView.trailingAnchor.constraint(
                equalTo: contentContainer.trailingAnchor
            ),
            progressView.bottomAnchor.constraint(
                lessThanOrEqualTo: contentContainer.bottomAnchor
            ),
        ])

        let root = NSStackView(
            views: [
                header,
                headerSeparator,
                source,
                contentSeparator,
                contentContainer,
            ]
        )
        root.orientation = .vertical
        root.alignment = .width
        root.spacing = 14
        root.translatesAutoresizingMaskIntoConstraints = false
        root.edgeInsets = NSEdgeInsets(
            top: 18,
            left: 20,
            bottom: 16,
            right: 20
        )
        view.addSubview(root)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor),
            root.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            root.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            root.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            contentContainer.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 0
            ),
        ])

        viewModel.onChange = { [weak self] in
            self?.render()
            self?.closeIfFinished()
        }
        render()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
    }

    private func makeProfileHeader() -> NSView {
        let display = StorageProfile(record: profile)
        let symbol = NSImageView()
        symbol.image = NSImage(
            systemSymbolName: display.storageType.symbolName,
            accessibilityDescription: nil
        )
        symbol.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 22,
            weight: .medium
        )
        symbol.contentTintColor = .secondaryLabelColor
        symbol.translatesAutoresizingMaskIntoConstraints = false

        let title = NSTextField(labelWithString: profile.name)
        title.font = .systemFont(ofSize: 17, weight: .semibold)

        let subtitle = NSTextField(
            labelWithString: display.displaySubtitle
        )
        subtitle.font = .systemFont(ofSize: 13)
        subtitle.textColor = .secondaryLabelColor
        subtitle.lineBreakMode = .byTruncatingMiddle

        let labels = NSStackView(views: [title, subtitle])
        labels.orientation = .vertical
        labels.alignment = .leading
        labels.spacing = 2

        let header = NSStackView(
            views: [symbol, labels, NSView()]
        )
        header.orientation = .horizontal
        header.alignment = .centerY
        header.spacing = 12
        NSLayoutConstraint.activate([
            symbol.widthAnchor.constraint(equalToConstant: 28),
            symbol.heightAnchor.constraint(equalToConstant: 28),
        ])
        return header
    }

    private func makeSourceSection() -> NSView {
        let title = NSTextField(
            labelWithString: String(
                localized: "migration.section.legacyFolder"
            )
        )
        title.font = .systemFont(ofSize: 14, weight: .semibold)

        pathLabel.font = .monospacedSystemFont(
            ofSize: 13,
            weight: .regular
        )
        pathLabel.lineBreakMode = .byTruncatingMiddle
        pathLabel.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        browseButton.title = String(
            localized: "legacy.folder.browse"
        )
        browseButton.bezelStyle = .rounded
        browseButton.target = self
        browseButton.action = #selector(browse(_:))

        let pathRow = NSStackView(
            views: [pathLabel, browseButton]
        )
        pathRow.orientation = .horizontal
        pathRow.alignment = .centerY
        pathRow.spacing = 10
        pathRow.edgeInsets = NSEdgeInsets(
            top: 6,
            left: 8,
            bottom: 6,
            right: 8
        )

        let pathBox = NSBox()
        pathBox.boxType = .custom
        pathBox.borderWidth = 0
        pathBox.fillColor = .controlBackgroundColor
        pathBox.cornerRadius = 6
        pathBox.contentView = pathRow

        configureCheckbox(
            skipPerceptualCheckbox,
            title: String(
                localized:
                    "migration.options.skipPerceptual.label"
            ),
            tooltip: String(
                localized:
                    "migration.options.skipPerceptual.description"
            ),
            action: #selector(changeSkipPerceptual(_:))
        )
        configureCheckbox(
            replaceSubsetsCheckbox,
            title: String(
                localized:
                    "migration.options.replaceSubsets.label"
            ),
            tooltip: String(
                localized:
                    "migration.options.replaceSubsets.description"
            ),
            action: #selector(changeReplaceSubsets(_:))
        )

        scanSpinner.style = .spinning
        scanSpinner.controlSize = .small
        scanningLabel.stringValue = String(
            localized: "migration.scanning"
        )
        scanningLabel.textColor = .secondaryLabelColor

        cancelScanButton.title = String(localized: "common.cancel")
        cancelScanButton.bezelStyle = .rounded
        cancelScanButton.target = self
        cancelScanButton.action = #selector(cancelScan(_:))

        scanButton.title = String(
            localized: "migration.button.scan"
        )
        scanButton.image = NSImage(
            systemSymbolName: "magnifyingglass",
            accessibilityDescription: nil
        )
        scanButton.imagePosition = .imageLeading
        scanButton.bezelStyle = .rounded
        scanButton.keyEquivalent = "r"
        scanButton.keyEquivalentModifierMask = [.command]
        scanButton.target = self
        scanButton.action = #selector(scan(_:))

        commitButton.title = String(
            localized: "migration.button.commit"
        )
        commitButton.image = NSImage(
            systemSymbolName: "tray.and.arrow.down",
            accessibilityDescription: nil
        )
        commitButton.imagePosition = .imageLeading
        commitButton.bezelStyle = .rounded
        commitButton.bezelColor = .wmMaterialPrimary
        commitButton.keyEquivalent = "\r"
        commitButton.keyEquivalentModifierMask = [.command]
        commitButton.target = self
        commitButton.action = #selector(commit(_:))

        let actionRow = NSStackView(
            views: [
                scanSpinner,
                scanningLabel,
                cancelScanButton,
                NSView(),
                scanButton,
                commitButton,
            ]
        )
        actionRow.orientation = .horizontal
        actionRow.alignment = .centerY
        actionRow.spacing = 8

        let source = NSStackView(
            views: [
                title,
                pathBox,
                skipPerceptualCheckbox,
                replaceSubsetsCheckbox,
                actionRow,
            ]
        )
        source.orientation = .vertical
        source.alignment = .width
        source.spacing = 10
        return source
    }

    private func configureCheckbox(
        _ checkbox: NSButton,
        title: String,
        tooltip: String,
        action: Selector
    ) {
        checkbox.setButtonType(.switch)
        checkbox.title = title
        checkbox.toolTip = tooltip
        checkbox.target = self
        checkbox.action = action
    }

    private func configureProgressView() {
        progressSpinner.style = .spinning
        progressSpinner.controlSize = .small
        progressIcon.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 16,
            weight: .medium
        )
        progressTitle.font = .systemFont(
            ofSize: 14,
            weight: .semibold
        )

        progressActionButton.bezelStyle = .rounded
        progressActionButton.target = self
        progressActionButton.action = #selector(progressAction(_:))

        let header = NSStackView(
            views: [
                progressSpinner,
                progressIcon,
                progressTitle,
                NSView(),
                progressActionButton,
            ]
        )
        header.orientation = .horizontal
        header.alignment = .centerY
        header.spacing = 8

        progressIndicator.style = .bar
        progressIndicator.isIndeterminate = false
        progressIndicator.minValue = 0

        progressContext.font = .systemFont(ofSize: 13)
        progressCount.font = .monospacedDigitSystemFont(
            ofSize: 13,
            weight: .regular
        )
        progressCount.textColor = .secondaryLabelColor

        let progressLabels = NSStackView(
            views: [progressContext, NSView(), progressCount]
        )
        progressLabels.orientation = .horizontal
        progressLabels.alignment = .centerY
        progressLabels.spacing = 8

        let stats = NSStackView(
            views: [
                makeStat(
                    label: String(
                        localized: "migration.progress.imported"
                    ),
                    valueField: importedValue
                ),
                makeStat(
                    label: String(
                        localized: "migration.scan.action.skip"
                    ),
                    valueField: skippedValue
                ),
                makeStat(
                    label: String(localized: "home.execution.failed"),
                    valueField: failedValue
                ),
                makeStat(
                    label: String(
                        localized:
                            "migration.progress.stat.uploaded"
                    ),
                    valueField: uploadedValue
                ),
                NSView(),
            ]
        )
        stats.orientation = .horizontal
        stats.alignment = .top
        stats.spacing = 20

        let content = NSStackView(
            views: [
                header,
                progressLabels,
                progressIndicator,
                stats,
            ]
        )
        content.orientation = .vertical
        content.alignment = .width
        content.spacing = 10
        content.translatesAutoresizingMaskIntoConstraints = false
        progressView.addSubview(content)
        NSLayoutConstraint.activate([
            content.topAnchor.constraint(
                equalTo: progressView.topAnchor
            ),
            content.leadingAnchor.constraint(
                equalTo: progressView.leadingAnchor
            ),
            content.trailingAnchor.constraint(
                equalTo: progressView.trailingAnchor
            ),
            content.bottomAnchor.constraint(
                equalTo: progressView.bottomAnchor
            ),
        ])
    }

    private func makeStat(
        label: String,
        valueField: NSTextField
    ) -> NSView {
        let labelField = NSTextField(labelWithString: label)
        labelField.font = .systemFont(ofSize: 11)
        labelField.textColor = .secondaryLabelColor
        valueField.font = .monospacedDigitSystemFont(
            ofSize: 13,
            weight: .regular
        )

        let stack = NSStackView(views: [labelField, valueField])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 0
        return stack
    }

    private func render() {
        let path = viewModel.legacyFolderPath
        pathLabel.stringValue =
            path ?? String(localized: "common.notSelected")
        pathLabel.textColor =
            path == nil ? .secondaryLabelColor : .labelColor
        browseButton.isEnabled =
            viewModel.phase != .scanning
            && viewModel.phase != .committing

        let showsSkip =
            viewModel.phase == .idle
            || viewModel.phase == .scanned
        skipPerceptualCheckbox.isHidden = !showsSkip
        skipPerceptualCheckbox.state =
            viewModel.skipPerceptualDuplicates ? .on : .off

        let hasPlans = !(viewModel.report?.plans.isEmpty ?? true)
        replaceSubsetsCheckbox.isHidden =
            viewModel.phase != .scanned || !hasPlans
        replaceSubsetsCheckbox.state =
            viewModel.replaceSubsetAssets ? .on : .off

        let isScanning = viewModel.phase == .scanning
        scanSpinner.isHidden = !isScanning
        scanningLabel.isHidden = !isScanning
        cancelScanButton.isHidden = !isScanning
        if isScanning {
            scanSpinner.startAnimation(nil)
        } else {
            scanSpinner.stopAnimation(nil)
        }

        scanButton.isHidden = isScanning
        scanButton.isEnabled =
            path != nil
            && viewModel.phase != .committing
            && viewModel.phase != .committed
        commitButton.isHidden =
            viewModel.phase != .scanned || !hasPlans

        let showsContent =
            viewModel.phase != .idle
            && viewModel.phase != .scanning
        contentSeparator.isHidden = !showsContent
        contentContainer.isHidden = !showsContent

        if viewModel.phase == .scanned,
           let report = viewModel.report {
            installScanResultIfNeeded(report)
            scanResultController?.view.isHidden = false
            progressView.isHidden = true
        } else if showsContent {
            scanResultController?.view.isHidden = true
            progressView.isHidden = false
            renderProgress()
        }

        if viewModel.phase == .idle,
           viewModel.report == nil {
            removeScanResult()
        }
    }

    private func installScanResultIfNeeded(
        _ report: LegacyScanReport
    ) {
        guard scanResultController == nil else { return }
        let controller = MacLegacyScanResultViewController(
            report: report
        )
        addChild(controller)
        contentContainer.addSubview(controller.view)
        controller.view.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            controller.view.topAnchor.constraint(
                equalTo: contentContainer.topAnchor
            ),
            controller.view.leadingAnchor.constraint(
                equalTo: contentContainer.leadingAnchor
            ),
            controller.view.trailingAnchor.constraint(
                equalTo: contentContainer.trailingAnchor
            ),
            controller.view.bottomAnchor.constraint(
                equalTo: contentContainer.bottomAnchor
            ),
        ])
        scanResultController = controller
    }

    private func removeScanResult() {
        scanResultController?.view.removeFromSuperview()
        scanResultController?.removeFromParent()
        scanResultController = nil
    }

    private func renderProgress() {
        let isCommitting = viewModel.phase == .committing
        progressSpinner.isHidden = !isCommitting
        if isCommitting {
            progressSpinner.startAnimation(nil)
            progressIcon.isHidden = true
            progressTitle.stringValue = String(
                localized: "migration.progress.importing"
            )
            progressActionButton.title = String(
                localized: "common.cancel"
            )
        } else {
            progressSpinner.stopAnimation(nil)
            progressIcon.isHidden = false
            progressActionButton.title = String(
                localized: "common.done"
            )
            switch viewModel.phase {
            case .committed:
                progressIcon.image = NSImage(
                    systemSymbolName: "checkmark.seal.fill",
                    accessibilityDescription: nil
                )
                progressIcon.contentTintColor = .wmMaterialPrimary
                progressTitle.stringValue = String(
                    localized: "migration.progress.imported"
                )
            case .error:
                progressIcon.image = NSImage(
                    systemSymbolName: "xmark.octagon.fill",
                    accessibilityDescription: nil
                )
                progressIcon.contentTintColor = .wmMaterialError
                progressTitle.stringValue = String(
                    localized: "home.execution.failed"
                )
            default:
                progressIcon.image = nil
                progressTitle.stringValue = ""
            }
        }

        let totals = viewModel.totals
        progressIndicator.maxValue = Double(
            max(totals.bundlesPlanned, 1)
        )
        progressIndicator.doubleValue = Double(
            totals.bundlesProcessed
        )
        if isCommitting {
            if let month = viewModel.currentMonth {
                progressContext.stringValue = String(
                    format: String(
                        localized:
                            "migration.progress.month.format"
                    ),
                    month.text
                )
            } else {
                progressContext.stringValue = String(
                    localized: "link.connection.connecting"
                )
            }
        } else {
            progressContext.stringValue = ""
        }
        progressCount.stringValue =
            "\(totals.bundlesProcessed) / \(totals.bundlesPlanned)"
        importedValue.stringValue = Self.formatCount(
            totals.bundlesImported
        )
        skippedValue.stringValue = Self.formatCount(
            totals.bundlesSkippedFingerprintExists
                + totals.resourcesSkippedHashExists
        )
        failedValue.stringValue = Self.formatCount(
            totals.bundlesFailed
        )
        uploadedValue.stringValue =
            ByteCountFormatter.fileSizeString(totals.bytesUploaded)
    }

    private func handle(_ action: PendingAction) {
        if viewModel.client != nil {
            execute(action)
            return
        }
        pendingAction = action
        if profile.resolvedStorageType == .externalVolume {
            connectAndExecute(action: action, password: "")
            return
        }
        if let stored = try? profileStore.password(for: profile) {
            connectAndExecute(action: action, password: stored)
            return
        }
        let prompt = MacStoragePasswordPromptViewController(
            profileName: profile.name,
            username: profile.username,
            storageType: profile.resolvedStorageType
        ) { [weak self] password in
            guard let self else { return }
            self.connectAndExecute(
                action: self.pendingAction,
                password: password
            )
        }
        presentAsSheet(prompt)
    }

    private func connectAndExecute(
        action: PendingAction?,
        password: String
    ) {
        guard let action else { return }
        connectionTask?.cancel()
        connectionTask = Task { [weak self] in
            guard let self else { return }
            do {
                try await viewModel.connect(password: password)
                try Task.checkCancellation()
                pendingAction = nil
                connectionTask = nil
                closeIfFinished()
                execute(action)
            } catch is CancellationError {
                connectionTask = nil
                closeIfFinished()
            } catch {
                let wasClosing = closeWhenFinished
                pendingAction = nil
                connectionTask = nil
                closeIfFinished()
                guard !wasClosing else { return }
                presentConnectionError(error)
            }
        }
    }

    private func execute(_ action: PendingAction) {
        switch action {
        case .browse:
            guard let client = viewModel.client else { return }
            let picker = MacLegacyFolderPickerViewController(
                client: client,
                initialPath: viewModel.legacyFolderPath ?? "/"
            ) { [weak self] path in
                self?.viewModel.setLegacyPath(path)
            }
            presentAsSheet(picker)
        case .scan:
            removeScanResult()
            viewModel.startScan()
        case .commit:
            viewModel.startCommit()
        }
    }

    private func presentConnectionError(_ error: Error) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "link.connection.failed"
        )
        alert.informativeText = error.localizedDescription
        alert.addButton(
            withTitle: String(localized: "common.ok")
        )
        if let window = view.window {
            alert.beginSheetModal(for: window)
        } else {
            alert.runModal()
        }
    }

    @objc private func browse(_ sender: Any?) {
        handle(.browse)
    }

    @objc private func scan(_ sender: Any?) {
        handle(.scan)
    }

    @objc private func commit(_ sender: Any?) {
        handle(.commit)
    }

    @objc private func cancelScan(_ sender: Any?) {
        viewModel.cancelScan()
    }

    @objc private func changeSkipPerceptual(_ sender: NSButton) {
        viewModel.skipPerceptualDuplicates = sender.state == .on
    }

    @objc private func changeReplaceSubsets(_ sender: NSButton) {
        viewModel.replaceSubsetAssets = sender.state == .on
    }

    @objc private func progressAction(_ sender: Any?) {
        if viewModel.phase == .committing {
            viewModel.cancelCommit()
        } else {
            viewModel.resetForNewScan()
        }
    }

    private static let numberFormatter: NumberFormatter = {
        let formatter = NumberFormatter()
        formatter.numberStyle = .decimal
        return formatter
    }()

    private static func formatCount(_ value: Int) -> String {
        numberFormatter.string(from: NSNumber(value: value))
            ?? String(value)
    }

    private func closeIfFinished() {
        guard closeWhenFinished,
              connectionTask == nil,
              !viewModel.isRunning else {
            return
        }
        closeWhenFinished = false
        view.window?.performClose(nil)
    }
}

extension MacLegacyMigrationViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard connectionTask != nil || viewModel.isRunning else {
            return true
        }
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
            isTaskRunning:
                connectionTask != nil || viewModel.isRunning
        ) {
        case .keepOpen:
            return false
        case .close:
            return true
        case .stopThenClose:
            closeWhenFinished = true
            pendingAction = nil
            connectionTask?.cancel()
            viewModel.cancelScan()
            viewModel.cancelCommit()
            return false
        }
    }
}
