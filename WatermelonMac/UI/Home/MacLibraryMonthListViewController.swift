import AppKit

@MainActor
final class MacLibraryMonthListViewController: NSViewController {
    enum RemoteOverlayMode {
        case hidden
        case emptySetup
        case profileSelection
        case progress(message: String, actionTitle: String)
    }

    var onRefresh: (() -> Void)?
    var onToggleMonth: ((LibraryMonthKey, SelectionSide) -> Void)?
    var onToggleYear: ((Int, SelectionSide) -> Void)?
    var onToggleAll: ((SelectionSide) -> Void)?
    var onStart: (() -> Void)?
    var onPauseExecution: (() -> Void)?
    var onResumeExecution: (() -> Void)?
    var onCompleteExecution: (() -> Void)?
    var onStopExecution: (() -> Void)?
    var onOpenExecutionLog: (() -> Void)?
    var onOpenYear: ((Int, SelectionSide) -> Void)?
    var onOpenMonth: ((LibraryMonthKey, SelectionSide) -> Void)?
    var onLocalAccessAction: (() -> Void)?
    var onCreateDestination: ((StorageType) -> Void)?
    var onConnectDestination: ((ServerProfileRecord) -> Void)?
    var onRemoteOverlayAction: (() -> Void)?
    var destinationTitleProvider: ((
        ServerProfileRecord
    ) -> String)?

    private enum DisplayRow {
        case year(index: Int, section: HomeMergedYearSection)
        case month(HomeMonthRow)
    }

    private let tableView = NSTableView()
    private let localHeader = LibrarySideHeaderView()
    private let remoteHeader = LibrarySideHeaderView()
    private let refreshButton = NSButton()
    private let startButton = NSButton()
    private let executionStatusLabel = NSTextField(labelWithString: "")
    private let executionProgressIndicator = NSProgressIndicator()
    private let executionLogButton = NSButton()
    private let pauseResumeExecutionButton = NSButton()
    private let stopExecutionButton = NSButton()
    private let executionRow = NSStackView()
    private let stateLabel = NSTextField(labelWithString: "")
    private let progressIndicator = NSProgressIndicator()
    private let localOverlay = NSView()
    private let localOverlayLabel = NSTextField(labelWithString: "")
    private let localOverlayProgressIndicator = NSProgressIndicator()
    private let localOverlayButton = NSButton()
    private let remoteSetupOverlay = NSView()
    private let externalStorageButton = NSButton()
    private let otherStorageButton = NSButton()
    private let remoteConnectButton = NSButton()
    private let remoteAddStorageButton = NSButton()
    private let remoteProgressIndicator = NSProgressIndicator()
    private let remoteProgressLabel = NSTextField(labelWithString: "")
    private let remoteProgressActionButton = NSButton()
    private let remoteOverlayStack = NSStackView()
    private var rows: [DisplayRow] = []
    private var snapshot = PhotoLibraryMonthlyIndexSnapshot.empty
    private var localAccessState: PhotoLibraryAccessState = .unknown
    private var destinationProfiles: [ServerProfileRecord] = []
    private var selectionState = SelectionState()
    private var monthExecutionPhases:
        [LibraryMonthKey: MacMonthExecutionPhase] = [:]
    private var monthExecutionProgress:
        [LibraryMonthKey: MacMonthExecutionProgress] = [:]
    private var selectionEnabled = false
    private var remoteSelectionEnabled = false
    private var executionCanResume = false
    private var executionCanComplete = false

    override func loadView() {
        view = NSView()
        view.wantsLayer = true
        view.layer?.backgroundColor = NSColor.controlBackgroundColor.cgColor
        view.layer?.borderColor = NSColor.separatorColor.cgColor
        view.layer?.borderWidth = 1
        view.layer?.cornerRadius = 10
        view.layer?.masksToBounds = true

        localHeader.configure(
            title: String(
                localized: "mediaBrowser.mode.local",
                defaultValue: "Local"
            ),
            onToggle: { [weak self] in
                self?.onToggleAll?(.local)
            }
        )
        remoteHeader.configure(
            title: String(
                localized: "mediaBrowser.mode.remote",
                defaultValue: "Remote"
            ),
            trailingInset: 52,
            onToggle: { [weak self] in
                self?.onToggleAll?(.remote)
            }
        )

        let headerContainer = NSView()
        headerContainer.wantsLayer = true
        headerContainer.layer?.backgroundColor =
            NSColor.wmMaterialPrimarySurface.cgColor

        let headerDivider = makeDivider()
        for subview in [localHeader, headerDivider, remoteHeader] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            headerContainer.addSubview(subview)
        }
        NSLayoutConstraint.activate([
            headerDivider.centerXAnchor.constraint(
                equalTo: headerContainer.centerXAnchor
            ),
            headerDivider.topAnchor.constraint(
                equalTo: headerContainer.topAnchor
            ),
            headerDivider.bottomAnchor.constraint(
                equalTo: headerContainer.bottomAnchor
            ),
            headerDivider.widthAnchor.constraint(equalToConstant: 2),

            localHeader.leadingAnchor.constraint(
                equalTo: headerContainer.leadingAnchor
            ),
            localHeader.trailingAnchor.constraint(
                equalTo: headerDivider.leadingAnchor
            ),
            localHeader.topAnchor.constraint(
                equalTo: headerContainer.topAnchor
            ),
            localHeader.bottomAnchor.constraint(
                equalTo: headerContainer.bottomAnchor
            ),

            remoteHeader.leadingAnchor.constraint(
                equalTo: headerDivider.trailingAnchor
            ),
            remoteHeader.trailingAnchor.constraint(
                equalTo: headerContainer.trailingAnchor
            ),
            remoteHeader.topAnchor.constraint(
                equalTo: headerContainer.topAnchor
            ),
            remoteHeader.bottomAnchor.constraint(
                equalTo: headerContainer.bottomAnchor
            )
        ])

        refreshButton.image = NSImage(
            systemSymbolName: "arrow.clockwise",
            accessibilityDescription: nil
        )
        refreshButton.toolTip = String(
            localized: "common.refresh",
            defaultValue: "Refresh"
        )
        refreshButton.bezelStyle = .inline
        refreshButton.target = self
        refreshButton.action = #selector(refresh(_:))
        refreshButton.translatesAutoresizingMaskIntoConstraints = false
        headerContainer.addSubview(refreshButton)
        NSLayoutConstraint.activate([
            refreshButton.trailingAnchor.constraint(
                equalTo: headerContainer.trailingAnchor,
                constant: -12
            ),
            refreshButton.centerYAnchor.constraint(
                equalTo: headerContainer.centerYAnchor
            ),
            refreshButton.widthAnchor.constraint(equalToConstant: 30),
            refreshButton.heightAnchor.constraint(equalToConstant: 30)
        ])

        configureTableView()
        let scrollView = NSScrollView()
        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
        scrollView.borderType = .noBorder

        stateLabel.font = .systemFont(ofSize: 13)
        stateLabel.textColor = .secondaryLabelColor
        stateLabel.alignment = .center
        stateLabel.maximumNumberOfLines = 0
        stateLabel.lineBreakMode = .byWordWrapping

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .small
        progressIndicator.isDisplayedWhenStopped = false

        let stateStack = NSStackView(views: [progressIndicator, stateLabel])
        stateStack.orientation = .vertical
        stateStack.alignment = .centerX
        stateStack.spacing = 10

        localOverlay.wantsLayer = true
        localOverlay.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor

        localOverlayLabel.font = .systemFont(ofSize: 15, weight: .medium)
        localOverlayLabel.textColor = .secondaryLabelColor
        localOverlayLabel.alignment = .center
        localOverlayLabel.maximumNumberOfLines = 0
        localOverlayLabel.lineBreakMode = .byWordWrapping

        localOverlayProgressIndicator.style = .spinning
        localOverlayProgressIndicator.controlSize = .small
        localOverlayProgressIndicator.isDisplayedWhenStopped = false

        localOverlayButton.bezelStyle = .inline
        localOverlayButton.isBordered = false
        localOverlayButton.font = .systemFont(ofSize: 13, weight: .medium)
        localOverlayButton.contentTintColor = .wmMaterialPrimary
        localOverlayButton.target = self
        localOverlayButton.action = #selector(localAccessAction(_:))

        let localOverlayStack = NSStackView(
            views: [
                localOverlayProgressIndicator,
                localOverlayLabel,
                localOverlayButton
            ]
        )
        localOverlayStack.orientation = .vertical
        localOverlayStack.alignment = .centerX
        localOverlayStack.spacing = 12
        localOverlayStack.translatesAutoresizingMaskIntoConstraints = false
        localOverlay.addSubview(localOverlayStack)
        NSLayoutConstraint.activate([
            localOverlayStack.centerXAnchor.constraint(
                equalTo: localOverlay.centerXAnchor
            ),
            localOverlayStack.centerYAnchor.constraint(
                equalTo: localOverlay.centerYAnchor
            ),
            localOverlayStack.leadingAnchor.constraint(
                greaterThanOrEqualTo: localOverlay.leadingAnchor,
                constant: 28
            ),
            localOverlayStack.trailingAnchor.constraint(
                lessThanOrEqualTo: localOverlay.trailingAnchor,
                constant: -28
            )
        ])
        localOverlay.isHidden = true

        remoteSetupOverlay.wantsLayer = true
        remoteSetupOverlay.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor

        configureRemoteSetupButton(
            externalStorageButton,
            title: String(
                localized: "home.menu.externalStorage",
                defaultValue: "External Storage"
            ),
            symbolName: StorageType.externalVolume.symbolName,
            action: #selector(addExternalStorage(_:))
        )
        configureRemoteSetupButton(
            otherStorageButton,
            title: String(
                localized: "home.menu.addStorage",
                defaultValue: "Add Destination"
            ),
            symbolName: "plus.circle",
            action: #selector(showOtherStorageMenu(_:))
        )
        configureRemoteSetupButton(
            remoteConnectButton,
            title: String(
                localized: "home.overlay.connectNode",
                defaultValue: "Connect Node"
            ),
            symbolName: "link",
            action: #selector(showConnectDestinationMenu(_:))
        )
        configureRemoteSetupButton(
            remoteAddStorageButton,
            title: String(
                localized: "home.menu.addStorage",
                defaultValue: "Add Destination"
            ),
            symbolName: "plus.circle",
            action: #selector(showAddStorageMenu(_:))
        )

        remoteProgressIndicator.style = .spinning
        remoteProgressIndicator.controlSize = .small
        remoteProgressIndicator.isDisplayedWhenStopped = false

        configureRemoteSetupLabel(remoteProgressLabel, text: "")
        remoteProgressLabel.maximumNumberOfLines = 0
        remoteProgressLabel.lineBreakMode = .byWordWrapping

        remoteProgressActionButton.isBordered = false
        remoteProgressActionButton.target = self
        remoteProgressActionButton.action = #selector(
            remoteOverlayAction(_:)
        )

        remoteOverlayStack.orientation = .vertical
        remoteOverlayStack.alignment = .centerX
        remoteOverlayStack.spacing = 12
        remoteOverlayStack.translatesAutoresizingMaskIntoConstraints = false
        remoteSetupOverlay.addSubview(remoteOverlayStack)
        NSLayoutConstraint.activate([
            remoteOverlayStack.centerXAnchor.constraint(
                equalTo: remoteSetupOverlay.centerXAnchor
            ),
            remoteOverlayStack.centerYAnchor.constraint(
                equalTo: remoteSetupOverlay.centerYAnchor
            ),
            remoteOverlayStack.leadingAnchor.constraint(
                greaterThanOrEqualTo: remoteSetupOverlay.leadingAnchor,
                constant: 28
            ),
            remoteOverlayStack.trailingAnchor.constraint(
                lessThanOrEqualTo: remoteSetupOverlay.trailingAnchor,
                constant: -28
            )
        ])
        replaceRemoteOverlayViews(
            [
                externalStorageButton,
                otherStorageButton
            ]
        )
        remoteSetupOverlay.isHidden = true

        startButton.title = String(
            localized: "common.start",
            defaultValue: "Start"
        )
        startButton.bezelStyle = .rounded
        startButton.target = self
        startButton.action = #selector(start(_:))

        executionStatusLabel.font = .systemFont(ofSize: 12)
        executionStatusLabel.textColor = .secondaryLabelColor
        executionStatusLabel.lineBreakMode = .byTruncatingMiddle

        executionProgressIndicator.style = .bar
        executionProgressIndicator.isIndeterminate = false
        executionProgressIndicator.minValue = 0
        executionProgressIndicator.maxValue = 1
        executionProgressIndicator.doubleValue = 0

        executionLogButton.image = NSImage(
            systemSymbolName: "doc.text",
            accessibilityDescription: String(
                localized: "log.title"
            )
        )
        executionLogButton.toolTip = String(localized: "log.title")
        executionLogButton.setAccessibilityLabel(
            String(localized: "log.title")
        )
        executionLogButton.bezelStyle = .inline
        executionLogButton.target = self
        executionLogButton.action = #selector(openExecutionLog(_:))

        pauseResumeExecutionButton.title = String(
            localized: "mac.execution.pause",
            defaultValue: "Pause"
        )
        pauseResumeExecutionButton.bezelStyle = .rounded
        pauseResumeExecutionButton.target = self
        pauseResumeExecutionButton.action = #selector(togglePauseResume(_:))

        stopExecutionButton.title = String(
            localized: "common.stop",
            defaultValue: "Stop"
        )
        stopExecutionButton.bezelStyle = .rounded
        stopExecutionButton.target = self
        stopExecutionButton.action = #selector(stopExecution(_:))

        executionRow.setViews(
            [
                executionStatusLabel,
                executionProgressIndicator,
                executionLogButton,
                pauseResumeExecutionButton,
                stopExecutionButton
            ],
            in: .leading
        )
        executionRow.orientation = .horizontal
        executionRow.alignment = .centerY
        executionRow.spacing = 10
        executionRow.isHidden = true

        let footer = NSStackView(
            views: [
                NSView(),
                startButton
            ]
        )
        footer.orientation = .horizontal
        footer.alignment = .centerY
        footer.spacing = 10

        let footerContainer = NSStackView(views: [executionRow, footer])
        footerContainer.orientation = .vertical
        footerContainer.alignment = .width
        footerContainer.spacing = 8

        for subview in [
            headerContainer,
            scrollView,
            localOverlay,
            remoteSetupOverlay,
            stateStack,
            footerContainer
        ] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            view.addSubview(subview)
        }

        NSLayoutConstraint.activate([
            headerContainer.topAnchor.constraint(equalTo: view.topAnchor),
            headerContainer.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            headerContainer.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            headerContainer.heightAnchor.constraint(equalToConstant: 82),

            scrollView.topAnchor.constraint(
                equalTo: headerContainer.bottomAnchor,
                constant: 2
            ),
            scrollView.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 1
            ),
            scrollView.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -1
            ),
            scrollView.bottomAnchor.constraint(
                equalTo: footerContainer.topAnchor,
                constant: -8
            ),

            localOverlay.topAnchor.constraint(
                equalTo: scrollView.topAnchor
            ),
            localOverlay.leadingAnchor.constraint(
                equalTo: scrollView.leadingAnchor
            ),
            localOverlay.trailingAnchor.constraint(
                equalTo: view.centerXAnchor,
                constant: -1
            ),
            localOverlay.bottomAnchor.constraint(
                equalTo: scrollView.bottomAnchor
            ),

            remoteSetupOverlay.topAnchor.constraint(
                equalTo: scrollView.topAnchor
            ),
            remoteSetupOverlay.leadingAnchor.constraint(
                equalTo: view.centerXAnchor,
                constant: 1
            ),
            remoteSetupOverlay.trailingAnchor.constraint(
                equalTo: scrollView.trailingAnchor
            ),
            remoteSetupOverlay.bottomAnchor.constraint(
                equalTo: scrollView.bottomAnchor
            ),

            stateStack.centerXAnchor.constraint(
                equalTo: scrollView.centerXAnchor
            ),
            stateStack.centerYAnchor.constraint(
                equalTo: scrollView.centerYAnchor
            ),
            stateStack.leadingAnchor.constraint(
                greaterThanOrEqualTo: scrollView.leadingAnchor,
                constant: 24
            ),
            stateStack.trailingAnchor.constraint(
                lessThanOrEqualTo: scrollView.trailingAnchor,
                constant: -24
            ),

            footerContainer.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 20
            ),
            footerContainer.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -16
            ),
            footerContainer.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -12
            ),
            footer.heightAnchor.constraint(greaterThanOrEqualToConstant: 28)
        ])
        executionProgressIndicator.widthAnchor.constraint(
            equalToConstant: 190
        ).isActive = true

        applySelection(
            SelectionState(),
            selectionEnabled: false,
            remoteSelectionEnabled: false
        )
        showUnavailable("")
    }

    func applyLocalAccessState(_ state: PhotoLibraryAccessState) {
        localAccessState = state
        switch state {
        case .authorized, .limited:
            localOverlay.isHidden = true
            localOverlayProgressIndicator.stopAnimation(nil)
        case .notDetermined:
            showLocalAccessMessage(
                String(
                    localized: "home.overlay.authRequired",
                    defaultValue: "Photo library access required"
                ),
                actionTitle: String(
                    localized: "home.overlay.allowAccess",
                    defaultValue: "Continue"
                )
            )
        case .denied, .restricted, .unknown:
            showLocalAccessMessage(
                String(
                    localized: "home.overlay.noAuth",
                    defaultValue: "Photo library access denied"
                ),
                actionTitle: String(
                    localized: "home.overlay.goToSettings",
                    defaultValue: "Go to Settings"
                )
            )
        }
    }

    func showLocalAccessRequestInProgress() {
        showLocalAccessLoading()
    }

    func applyRemoteOverlay(
        mode: RemoteOverlayMode,
        profiles: [ServerProfileRecord],
        interactionEnabled: Bool
    ) {
        destinationProfiles = profiles
        remoteProgressIndicator.stopAnimation(nil)
        externalStorageButton.isEnabled = interactionEnabled
        otherStorageButton.isEnabled = interactionEnabled
        remoteConnectButton.isEnabled =
            interactionEnabled && !profiles.isEmpty
        remoteAddStorageButton.isEnabled = interactionEnabled
        remoteProgressActionButton.isEnabled = interactionEnabled

        switch mode {
        case .hidden:
            remoteSetupOverlay.isHidden = true
        case .emptySetup:
            replaceRemoteOverlayViews(
                [
                    externalStorageButton,
                    otherStorageButton
                ]
            )
            remoteSetupOverlay.isHidden = false
        case .profileSelection:
            replaceRemoteOverlayViews(
                [
                    remoteConnectButton,
                    remoteAddStorageButton
                ]
            )
            remoteSetupOverlay.isHidden = false
        case .progress(let message, let actionTitle):
            remoteProgressLabel.stringValue = message
            configureRemoteActionButton(
                remoteProgressActionButton,
                title: actionTitle
            )
            replaceRemoteOverlayViews(
                [
                    remoteProgressIndicator,
                    remoteProgressLabel,
                    remoteProgressActionButton
                ]
            )
            remoteProgressIndicator.startAnimation(nil)
            remoteSetupOverlay.isHidden = false
        }
    }

    func showLoading() {
        refreshButton.isEnabled = false
        if !localAccessState.canReadLibrary {
            stateLabel.isHidden = true
            progressIndicator.stopAnimation(nil)
            tableView.isHidden = false
            showLocalAccessLoading()
            return
        }
        localOverlay.isHidden = true
        stateLabel.stringValue = String(
            localized: "home.overlay.scanningLibrary",
            defaultValue: "Scanning photo library…"
        )
        stateLabel.isHidden = false
        progressIndicator.startAnimation(nil)
        tableView.isHidden = true
    }

    func showUnavailable(_ message: String) {
        refreshButton.isEnabled = true
        progressIndicator.stopAnimation(nil)
        stateLabel.stringValue = message
        stateLabel.isHidden = message.isEmpty
        snapshot = .empty
        rows = []
        updateHeaderSummaries()
        tableView.reloadData()
        tableView.isHidden = localOverlay.isHidden
    }

    func showError(_ message: String) {
        showUnavailable(message)
    }

    func apply(snapshot: PhotoLibraryMonthlyIndexSnapshot) {
        refreshButton.isEnabled = true
        progressIndicator.stopAnimation(nil)
        self.snapshot = snapshot
        rows = snapshot.sections.enumerated().flatMap { index, section in
            [.year(index: index, section: section)]
                + section.rows.map(DisplayRow.month)
        }
        updateHeaderSummaries()
        tableView.reloadData()

        let isEmpty = snapshot.totalAssetCount == 0
            && snapshot.remoteAssetCount == 0
        tableView.isHidden = isEmpty
        let showsEmptyState = isEmpty && localOverlay.isHidden
        stateLabel.isHidden = !showsEmptyState
        if showsEmptyState {
            stateLabel.stringValue = String(
                localized: "mediaBrowser.empty.message",
                defaultValue: "There are no photos to show here."
            )
        }
    }

    func applySelection(
        _ state: SelectionState,
        selectionEnabled: Bool,
        remoteSelectionEnabled: Bool
    ) {
        self.selectionState = state
        self.selectionEnabled = selectionEnabled
        self.remoteSelectionEnabled = remoteSelectionEnabled

        let monthRows = snapshot.sections.flatMap(\.rows)
        localHeader.applySelection(
            state: state.selectionState(
                forRows: monthRows,
                side: .local
            ),
            enabled: selectionEnabled
                && monthRows.contains { $0.local != nil }
        )
        remoteHeader.applySelection(
            state: state.selectionState(
                forRows: monthRows,
                side: .remote
            ),
            enabled: remoteSelectionEnabled
                && monthRows.contains { $0.remote != nil }
        )

        let counts = state.counts()
        let selectedCount = counts.backup
            + counts.download
            + counts.complement
        startButton.isEnabled = selectionEnabled && selectedCount > 0
        tableView.reloadData()
    }

    func setStartButton(title: String, enabled: Bool) {
        startButton.title = title
        startButton.isEnabled = enabled
    }

    func applyMonthExecution(
        _ phases: [LibraryMonthKey: MacMonthExecutionPhase],
        progress: [LibraryMonthKey: MacMonthExecutionProgress]
    ) {
        let allMonths = Set(monthExecutionPhases.keys)
            .union(phases.keys)
            .union(monthExecutionProgress.keys)
            .union(progress.keys)
        let changedMonths = allMonths.filter {
            monthExecutionPhases[$0] != phases[$0]
                || monthExecutionProgress[$0] != progress[$0]
        }
        guard !changedMonths.isEmpty else { return }
        monthExecutionPhases = phases
        monthExecutionProgress = progress
        guard isViewLoaded else { return }
        reloadMonthRows(Set(changedMonths))
    }

    func applyExecution(
        status: String?,
        progress: Double?,
        active: Bool,
        canPause: Bool,
        canResume: Bool,
        canComplete: Bool,
        canStop: Bool,
        canOpenLog: Bool
    ) {
        executionCanResume = canResume
        executionCanComplete = canComplete
        executionRow.isHidden = !active && status == nil
        executionStatusLabel.stringValue = status ?? ""
        executionProgressIndicator.doubleValue = min(
            max(progress ?? 0, 0),
            1
        )
        executionProgressIndicator.isHidden = progress == nil
        executionLogButton.isHidden = !canOpenLog
        executionLogButton.isEnabled = canOpenLog
        if canComplete {
            pauseResumeExecutionButton.title = String(
                localized: "common.done",
                defaultValue: "Done"
            )
        } else if canResume {
            pauseResumeExecutionButton.title = String(
                localized: "mac.execution.resume",
                defaultValue: "Resume"
            )
        } else {
            pauseResumeExecutionButton.title = String(
                localized: "mac.execution.pause",
                defaultValue: "Pause"
            )
        }
        pauseResumeExecutionButton.isEnabled =
            active && (canPause || canResume || canComplete)
        pauseResumeExecutionButton.isHidden =
            !active || (!canPause && !canResume && !canComplete)
        stopExecutionButton.isEnabled = active && canStop
        stopExecutionButton.isHidden = !active
    }

    @objc private func refresh(_ sender: Any?) {
        onRefresh?()
    }

    @objc private func localAccessAction(_ sender: Any?) {
        onLocalAccessAction?()
    }

    @objc private func addExternalStorage(_ sender: Any?) {
        onCreateDestination?(.externalVolume)
    }

    @objc private func showOtherStorageMenu(_ sender: NSButton) {
        showStorageMenu(
            from: sender,
            types: StorageType.nodeTypeDisplayOrder.filter {
                $0 != .externalVolume
            }
        )
    }

    @objc private func showConnectDestinationMenu(_ sender: NSButton) {
        let menu = NSMenu()
        for profile in destinationProfiles {
            let item = NSMenuItem(
                title: destinationTitleProvider?(profile)
                    ?? profile.name,
                action: #selector(connectDestination(_:)),
                keyEquivalent: ""
            )
            item.target = self
            if let profileID = profile.id {
                item.representedObject = NSNumber(value: profileID)
            }
            item.image = NSImage(
                systemSymbolName: profile.resolvedStorageType.symbolName,
                accessibilityDescription: nil
            )
            menu.addItem(item)
        }
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: sender.bounds.maxY + 4),
            in: sender
        )
    }

    @objc private func connectDestination(_ sender: NSMenuItem) {
        guard let profileID = (
            sender.representedObject as? NSNumber
        )?.int64Value,
              let profile = destinationProfiles.first(where: {
                  $0.id == profileID
              }) else {
            return
        }
        onConnectDestination?(profile)
    }

    @objc private func showAddStorageMenu(_ sender: NSButton) {
        showStorageMenu(
            from: sender,
            types: StorageType.nodeTypeDisplayOrder
        )
    }

    private func showStorageMenu(
        from sender: NSButton,
        types: [StorageType]
    ) {
        let menu = NSMenu()
        for type in types {
            let item = NSMenuItem(
                title: type.sectionHeaderText,
                action: #selector(addStorage(_:)),
                keyEquivalent: ""
            )
            item.target = self
            item.representedObject = type.rawValue
            item.image = NSImage(
                systemSymbolName: type.symbolName,
                accessibilityDescription: nil
            )
            menu.addItem(item)
        }
        menu.popUp(
            positioning: nil,
            at: NSPoint(x: 0, y: sender.bounds.maxY + 4),
            in: sender
        )
    }

    @objc private func addStorage(_ sender: NSMenuItem) {
        guard let rawValue = sender.representedObject as? String,
              let type = StorageType(rawValue: rawValue) else {
            return
        }
        onCreateDestination?(type)
    }

    @objc private func remoteOverlayAction(_ sender: Any?) {
        onRemoteOverlayAction?()
    }

    @objc private func start(_ sender: Any?) {
        onStart?()
    }

    @objc private func togglePauseResume(_ sender: Any?) {
        if executionCanComplete {
            onCompleteExecution?()
        } else if executionCanResume {
            onResumeExecution?()
        } else {
            onPauseExecution?()
        }
    }

    @objc private func stopExecution(_ sender: Any?) {
        onStopExecution?()
    }

    @objc private func openExecutionLog(_ sender: Any?) {
        onOpenExecutionLog?()
    }

    private func configureTableView() {
        let localColumn = NSTableColumn(identifier: .localPaneColumn)
        localColumn.width = 360
        localColumn.minWidth = 210
        localColumn.resizingMask = .autoresizingMask

        let remoteColumn = NSTableColumn(identifier: .remotePaneColumn)
        remoteColumn.width = 360
        remoteColumn.minWidth = 210
        remoteColumn.resizingMask = .autoresizingMask

        tableView.addTableColumn(localColumn)
        tableView.addTableColumn(remoteColumn)
        tableView.headerView = nil
        tableView.rowHeight = 72
        tableView.intercellSpacing = NSSize(width: 2, height: 2)
        tableView.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        tableView.style = .plain
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.selectionHighlightStyle = .none
        tableView.allowsEmptySelection = true
        tableView.backgroundColor = .separatorColor
        tableView.delegate = self
        tableView.dataSource = self
    }

    private func updateHeaderSummaries() {
        localHeader.applySummary(
            photoCount: snapshot.totalPhotoCount,
            videoCount: snapshot.totalVideoCount,
            sizeBytes: snapshot.totalSizeBytes
        )
        remoteHeader.applySummary(
            photoCount: snapshot.remotePhotoCount,
            videoCount: snapshot.remoteVideoCount,
            sizeBytes: snapshot.remoteSizeBytes
        )
    }

    private func showLocalAccessMessage(
        _ message: String,
        actionTitle: String
    ) {
        localOverlay.isHidden = false
        localOverlayProgressIndicator.stopAnimation(nil)
        localOverlayLabel.stringValue = message
        localOverlayButton.attributedTitle = NSAttributedString(
            string: actionTitle,
            attributes: [
                .font: NSFont.systemFont(ofSize: 13, weight: .medium),
                .foregroundColor: NSColor.wmMaterialPrimary,
            ]
        )
        localOverlayButton.isEnabled = true
        localOverlayButton.isHidden = false
    }

    private func showLocalAccessLoading() {
        localOverlay.isHidden = false
        localOverlayLabel.stringValue = String(
            localized: "home.overlay.scanningLibrary",
            defaultValue: "Scanning photo library…"
        )
        localOverlayButton.isHidden = true
        localOverlayProgressIndicator.startAnimation(nil)
    }

    private func configureRemoteSetupLabel(
        _ label: NSTextField,
        text: String
    ) {
        label.stringValue = text
        label.font = .systemFont(ofSize: 15, weight: .medium)
        label.textColor = .secondaryLabelColor
        label.alignment = .center
    }

    private func configureRemoteSetupButton(
        _ button: NSButton,
        title: String,
        symbolName: String,
        action: Selector
    ) {
        button.attributedTitle = NSAttributedString(
            string: title,
            attributes: [
                .font: NSFont.systemFont(ofSize: 13, weight: .medium),
                .foregroundColor: NSColor.wmMaterialPrimary,
            ]
        )
        button.image = NSImage(
            systemSymbolName: symbolName,
            accessibilityDescription: nil
        )
        button.imagePosition = .imageLeading
        button.isBordered = false
        button.font = .systemFont(ofSize: 13, weight: .medium)
        button.contentTintColor = .wmMaterialPrimary
        button.target = self
        button.action = action
    }

    private func configureRemoteActionButton(
        _ button: NSButton,
        title: String
    ) {
        button.attributedTitle = NSAttributedString(
            string: title,
            attributes: [
                .font: NSFont.systemFont(ofSize: 13, weight: .medium),
                .foregroundColor: NSColor.wmMaterialPrimary,
            ]
        )
    }

    private func replaceRemoteOverlayViews(_ views: [NSView]) {
        for arrangedSubview in remoteOverlayStack.arrangedSubviews {
            remoteOverlayStack.removeArrangedSubview(arrangedSubview)
            arrangedSubview.removeFromSuperview()
        }
        for view in views {
            remoteOverlayStack.addArrangedSubview(view)
        }
    }

    private func makeDivider() -> NSView {
        let divider = NSView()
        divider.wantsLayer = true
        divider.layer?.backgroundColor = NSColor.separatorColor.cgColor
        return divider
    }

    private func reloadMonthRows(
        _ months: Set<LibraryMonthKey>
    ) {
        let rowIndexes = IndexSet(
            rows.enumerated().compactMap { index, row in
                guard case .month(let monthRow) = row,
                      months.contains(monthRow.month) else {
                    return nil
                }
                return index
            }
        )
        guard !rowIndexes.isEmpty else { return }
        tableView.reloadData(
            forRowIndexes: rowIndexes,
            columnIndexes: IndexSet(
                integersIn: 0 ..< tableView.numberOfColumns
            )
        )
    }
}

extension MacLibraryMonthListViewController: NSTableViewDataSource {
    func numberOfRows(in tableView: NSTableView) -> Int {
        rows.count
    }
}

extension MacLibraryMonthListViewController: NSTableViewDelegate {
    func tableView(
        _ tableView: NSTableView,
        heightOfRow row: Int
    ) -> CGFloat {
        guard rows.indices.contains(row) else { return 72 }
        switch rows[row] {
        case .year:
            return 58
        case .month:
            return 72
        }
    }

    func tableView(
        _ tableView: NSTableView,
        shouldSelectRow row: Int
    ) -> Bool {
        false
    }

    func tableView(
        _ tableView: NSTableView,
        rowViewForRow row: Int
    ) -> NSTableRowView? {
        let rowView = tableView.makeView(
            withIdentifier: .libraryRowView,
            owner: self
        ) as? LibraryTableRowView ?? LibraryTableRowView()
        rowView.identifier = .libraryRowView

        guard rows.indices.contains(row),
              case .month(let month) = rows[row] else {
            rowView.configure(intent: nil, percent: nil)
            return rowView
        }
        let intent = selectionState.intent(for: month.month)
        rowView.configure(
            intent: intent,
            percent: MacHomeMonthProgressCalculator.percent(
                row: month,
                intent: intent,
                phase: monthExecutionPhases[month.month],
                executionProgress:
                    monthExecutionProgress[month.month]
            )
        )
        return rowView
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard rows.indices.contains(row), let tableColumn else {
            return nil
        }
        let side: SelectionSide = tableColumn.identifier == .remotePaneColumn
            ? .remote
            : .local

        switch rows[row] {
        case .year(let sectionIndex, let section):
            let cell = tableView.makeView(
                withIdentifier: .yearPaneCell,
                owner: self
            ) as? LibraryYearPaneCell ?? LibraryYearPaneCell()
            cell.identifier = .yearPaneCell
            let summaries = side == .local
                ? section.rows.compactMap(\.local)
                : section.rows.compactMap(\.remote)
            let state = selectionState.selectionState(
                forRows: section.rows,
                side: side
            )
            let enabled = side == .local
                ? selectionEnabled && !summaries.isEmpty
                : remoteSelectionEnabled && !summaries.isEmpty
            cell.configure(
                title: section.title,
                photoCount: side == .local
                    ? section.localPhotoCount
                    : section.remotePhotoCount,
                videoCount: side == .local
                    ? section.localVideoCount
                    : section.remoteVideoCount,
                sizeBytes: side == .local
                    ? section.localSizeBytes
                    : section.remoteSizeBytes,
                selectionState: state,
                selectionEnabled: enabled,
                onToggle: { [weak self] in
                    self?.onToggleYear?(sectionIndex, side)
                },
                onOpen: { [weak self] in
                    self?.onOpenYear?(sectionIndex, side)
                }
            )
            return cell

        case .month(let monthRow):
            let cell = tableView.makeView(
                withIdentifier: .monthPaneCell,
                owner: self
            ) as? LibraryMonthPaneCell ?? LibraryMonthPaneCell()
            cell.identifier = .monthPaneCell
            let summary = side == .local
                ? monthRow.local
                : monthRow.remote
            let isSelected = side == .local
                ? selectionState.localMonths.contains(monthRow.month)
                : selectionState.remoteMonths.contains(monthRow.month)
            let enabled = side == .local
                ? selectionEnabled
                : remoteSelectionEnabled
            cell.configure(
                month: monthRow.month,
                summary: summary,
                selected: isSelected,
                selectionEnabled: enabled,
                executionPhase:
                    monthExecutionPhases[monthRow.month],
                side: side,
                onToggle: { [weak self] in
                    self?.onToggleMonth?(monthRow.month, side)
                },
                onOpen: { [weak self] in
                    self?.onOpenMonth?(monthRow.month, side)
                }
            )
            return cell
        }
    }
}

@MainActor
private final class LibraryTableRowView: NSTableRowView {
    private let badge = LibraryDirectionBadgeView()
    private let imageView = NSImageView()
    private let percentLabel = NSTextField(labelWithString: "")
    private var centeredImageConstraint: NSLayoutConstraint!
    private var topImageConstraint: NSLayoutConstraint!

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        wantsLayer = true

        badge.wantsLayer = true
        badge.layer?.backgroundColor = NSColor.controlBackgroundColor.cgColor
        badge.layer?.borderColor = NSColor.separatorColor.cgColor
        badge.layer?.borderWidth = 1
        badge.layer?.cornerRadius = 13
        badge.layer?.zPosition = 100
        badge.setAccessibilityElement(true)

        imageView.imageScaling = .scaleProportionallyDown
        percentLabel.font = .monospacedDigitSystemFont(
            ofSize: 9,
            weight: .regular
        )
        percentLabel.alignment = .center
        percentLabel.lineBreakMode = .byClipping

        badge.translatesAutoresizingMaskIntoConstraints = false
        imageView.translatesAutoresizingMaskIntoConstraints = false
        percentLabel.translatesAutoresizingMaskIntoConstraints = false
        addSubview(badge)
        badge.addSubview(imageView)
        badge.addSubview(percentLabel)
        centeredImageConstraint = imageView.centerYAnchor.constraint(
            equalTo: badge.centerYAnchor
        )
        topImageConstraint = imageView.topAnchor.constraint(
            equalTo: badge.topAnchor,
            constant: 4
        )
        NSLayoutConstraint.activate([
            badge.centerXAnchor.constraint(equalTo: centerXAnchor),
            badge.centerYAnchor.constraint(equalTo: centerYAnchor),
            badge.widthAnchor.constraint(equalToConstant: 44),
            badge.heightAnchor.constraint(equalToConstant: 40),

            imageView.centerXAnchor.constraint(equalTo: badge.centerXAnchor),
            imageView.widthAnchor.constraint(equalToConstant: 17),
            imageView.heightAnchor.constraint(equalToConstant: 17),
            centeredImageConstraint,

            percentLabel.centerXAnchor.constraint(
                equalTo: badge.centerXAnchor
            ),
            percentLabel.topAnchor.constraint(
                equalTo: imageView.bottomAnchor,
                constant: -1
            ),
            percentLabel.widthAnchor.constraint(equalToConstant: 40),
            percentLabel.heightAnchor.constraint(equalToConstant: 12)
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(intent: MonthIntent?, percent: Double?) {
        guard let intent else {
            badge.isHidden = true
            setShowsPercent(false)
            return
        }
        badge.isHidden = false
        imageView.image = NSImage(
            systemSymbolName: intent.iconSymbolName,
            accessibilityDescription: nil
        )?.withSymbolConfiguration(
            NSImage.SymbolConfiguration(
                pointSize: 13,
                weight: .bold
            )
        )
        switch intent {
        case .backup:
            imageView.contentTintColor = .wmMaterialBackup
            percentLabel.textColor = .wmMaterialBackup
        case .download:
            imageView.contentTintColor = .wmMaterialDownload
            percentLabel.textColor = .wmMaterialDownload
        case .complement:
            imageView.contentTintColor = .wmMaterialComplement
            percentLabel.textColor = .wmMaterialComplement
        }
        if let percent {
            percentLabel.stringValue = String(
                format: "%.1f%%",
                min(max(percent, 0), 100)
            )
            setShowsPercent(true)
            badge.setAccessibilityLabel(
                "\(intent.panelSubtitle), \(percentLabel.stringValue)"
            )
        } else {
            setShowsPercent(false)
            badge.setAccessibilityLabel(intent.panelSubtitle)
        }
        badge.toolTip = intent.panelSubtitle
    }

    private func setShowsPercent(_ visible: Bool) {
        percentLabel.isHidden = !visible
        if !visible {
            percentLabel.stringValue = ""
        }
        if visible {
            centeredImageConstraint.isActive = false
            topImageConstraint.isActive = true
        } else {
            topImageConstraint.isActive = false
            centeredImageConstraint.isActive = true
        }
    }
}

@MainActor
private final class LibraryDirectionBadgeView: NSView {
    override func hitTest(_ point: NSPoint) -> NSView? {
        nil
    }
}

@MainActor
private final class LibrarySideHeaderView: NSView {
    private let selectionButton = NSButton()
    private let titleLabel = NSTextField(labelWithString: "")
    private let summaryLabel = NSTextField(labelWithString: "")
    private var onToggle: (() -> Void)?
    private var trailingInset: CGFloat = 14
    private var trailingConstraint: NSLayoutConstraint?

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        wantsLayer = true
        layer?.backgroundColor = NSColor.wmMaterialPrimarySurface.cgColor

        configureSelectionButton(selectionButton)
        selectionButton.target = self
        selectionButton.action = #selector(toggle(_:))

        titleLabel.font = .systemFont(ofSize: 16, weight: .semibold)
        titleLabel.textColor = .wmMaterialOnPrimaryContainer
        titleLabel.lineBreakMode = .byTruncatingTail

        summaryLabel.font = .monospacedDigitSystemFont(
            ofSize: 12,
            weight: .regular
        )
        summaryLabel.textColor = .wmMaterialPrimaryDetail
        summaryLabel.lineBreakMode = .byTruncatingTail

        for subview in [selectionButton, titleLabel, summaryLabel] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            addSubview(subview)
        }
        trailingConstraint = titleLabel.trailingAnchor.constraint(
            lessThanOrEqualTo: trailingAnchor,
            constant: -trailingInset
        )
        NSLayoutConstraint.activate([
            selectionButton.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 14
            ),
            selectionButton.centerYAnchor.constraint(equalTo: centerYAnchor),
            selectionButton.widthAnchor.constraint(equalToConstant: 28),
            selectionButton.heightAnchor.constraint(equalToConstant: 32),

            titleLabel.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 52
            ),
            titleLabel.bottomAnchor.constraint(
                equalTo: centerYAnchor,
                constant: -3
            ),
            trailingConstraint!,

            summaryLabel.leadingAnchor.constraint(
                equalTo: titleLabel.leadingAnchor
            ),
            summaryLabel.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -14
            ),
            summaryLabel.topAnchor.constraint(
                equalTo: centerYAnchor,
                constant: 5
            )
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(
        title: String,
        trailingInset: CGFloat = 14,
        onToggle: @escaping () -> Void
    ) {
        titleLabel.stringValue = title
        self.onToggle = onToggle
        self.trailingInset = trailingInset
        trailingConstraint?.constant = -trailingInset
    }

    func applySummary(
        photoCount: Int,
        videoCount: Int,
        sizeBytes: Int64?
    ) {
        summaryLabel.attributedStringValue =
            MacMediaCountPresentation.summaryAttributedString(
                photoCount: photoCount,
                videoCount: videoCount,
                sizeBytes: sizeBytes,
                fontSize: 12,
                color: .wmMaterialPrimaryDetail
            )
    }

    func applySelection(
        state: HomeSelectionState,
        enabled: Bool
    ) {
        selectionButton.image = selectionImage(for: state)
        selectionButton.contentTintColor = enabled
            ? .wmMaterialOnPrimaryContainer
            : .quaternaryLabelColor
        selectionButton.isEnabled = enabled
    }

    @objc private func toggle(_ sender: Any?) {
        onToggle?()
    }
}

@MainActor
private final class LibraryYearPaneCell: NSTableCellView {
    private let openButton = NSButton()
    private let selectionButton = NSButton()
    private let titleLabel = NSTextField(labelWithString: "")
    private let sizeLabel = NSTextField(labelWithString: "")
    private let countLabel = NSTextField(labelWithString: "")
    private var onToggle: (() -> Void)?
    private var onOpen: (() -> Void)?

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        wantsLayer = true
        layer?.backgroundColor = NSColor.controlBackgroundColor.cgColor

        configureSelectionButton(selectionButton)
        selectionButton.target = self
        selectionButton.action = #selector(toggle(_:))

        openButton.isBordered = false
        openButton.title = ""
        openButton.focusRingType = .none
        openButton.target = self
        openButton.action = #selector(open(_:))

        titleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        titleLabel.textColor = .secondaryLabelColor
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)

        sizeLabel.font = .monospacedDigitSystemFont(
            ofSize: 12,
            weight: .regular
        )
        sizeLabel.textColor = .tertiaryLabelColor
        sizeLabel.lineBreakMode = .byTruncatingTail

        countLabel.font = .monospacedDigitSystemFont(
            ofSize: 11,
            weight: .regular
        )
        countLabel.textColor = .tertiaryLabelColor

        for subview in [
            selectionButton,
            titleLabel,
            sizeLabel,
            countLabel,
            openButton
        ] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            addSubview(subview)
        }
        NSLayoutConstraint.activate([
            selectionButton.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 14
            ),
            selectionButton.centerYAnchor.constraint(equalTo: centerYAnchor),
            selectionButton.widthAnchor.constraint(equalToConstant: 28),
            selectionButton.heightAnchor.constraint(equalToConstant: 32),

            titleLabel.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 52
            ),
            titleLabel.bottomAnchor.constraint(
                equalTo: centerYAnchor,
                constant: -2
            ),

            sizeLabel.leadingAnchor.constraint(
                equalTo: titleLabel.trailingAnchor,
                constant: 7
            ),
            sizeLabel.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -14
            ),
            sizeLabel.centerYAnchor.constraint(
                equalTo: titleLabel.centerYAnchor
            ),

            countLabel.leadingAnchor.constraint(
                equalTo: titleLabel.leadingAnchor
            ),
            countLabel.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -14
            ),
            countLabel.topAnchor.constraint(
                equalTo: centerYAnchor,
                constant: 4
            ),

            openButton.leadingAnchor.constraint(equalTo: leadingAnchor),
            openButton.trailingAnchor.constraint(equalTo: trailingAnchor),
            openButton.topAnchor.constraint(equalTo: topAnchor),
            openButton.bottomAnchor.constraint(equalTo: bottomAnchor),
        ])
        addSubview(selectionButton)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(
        title: String,
        photoCount: Int,
        videoCount: Int,
        sizeBytes: Int64?,
        selectionState: HomeSelectionState,
        selectionEnabled: Bool,
        onToggle: @escaping () -> Void,
        onOpen: @escaping () -> Void
    ) {
        self.onToggle = onToggle
        self.onOpen = onOpen
        titleLabel.stringValue = title
        sizeLabel.stringValue = sizeBytes.map {
            ByteCountFormatter.string(fromByteCount: $0, countStyle: .file)
        } ?? ""
        countLabel.attributedStringValue =
            MacMediaCountPresentation.attributedString(
                photoCount: photoCount,
                videoCount: videoCount,
                fontSize: 11,
                color: .tertiaryLabelColor
            )
        selectionButton.image = selectionImage(for: selectionState)
        selectionButton.contentTintColor = selectionEnabled
            ? .secondaryLabelColor
            : .quaternaryLabelColor
        selectionButton.isEnabled = selectionEnabled
        openButton.setAccessibilityLabel(title)
    }

    @objc private func toggle(_ sender: Any?) {
        onToggle?()
    }

    @objc private func open(_ sender: Any?) {
        onOpen?()
    }
}

@MainActor
private final class LibraryMonthPaneCell: NSTableCellView {
    private let openButton = NSButton()
    private let selectionButton = NSButton()
    private let statusIndicator = NSProgressIndicator()
    private let titleLabel = NSTextField(labelWithString: "")
    private let sizeLabel = NSTextField(labelWithString: "")
    private let countLabel = NSTextField(labelWithString: "")
    private var onToggle: (() -> Void)?
    private var onOpen: (() -> Void)?

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        wantsLayer = true

        titleLabel.font = .systemFont(ofSize: 15, weight: .medium)
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)

        sizeLabel.font = .monospacedDigitSystemFont(
            ofSize: 13,
            weight: .regular
        )
        sizeLabel.lineBreakMode = .byTruncatingTail

        countLabel.font = .monospacedDigitSystemFont(
            ofSize: 12,
            weight: .regular
        )

        openButton.isBordered = false
        openButton.title = ""
        openButton.focusRingType = .none
        openButton.target = self
        openButton.action = #selector(open(_:))

        configureSelectionButton(selectionButton)
        selectionButton.target = self
        selectionButton.action = #selector(toggle(_:))

        statusIndicator.style = .spinning
        statusIndicator.controlSize = .small
        statusIndicator.isDisplayedWhenStopped = false
        statusIndicator.isHidden = true

        for subview in [
            titleLabel,
            sizeLabel,
            countLabel,
            openButton,
            selectionButton,
            statusIndicator
        ] {
            subview.translatesAutoresizingMaskIntoConstraints = false
            addSubview(subview)
        }
        NSLayoutConstraint.activate([
            titleLabel.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 52
            ),
            titleLabel.bottomAnchor.constraint(
                equalTo: centerYAnchor,
                constant: -3
            ),

            sizeLabel.leadingAnchor.constraint(
                equalTo: titleLabel.trailingAnchor,
                constant: 7
            ),
            sizeLabel.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -14
            ),
            sizeLabel.centerYAnchor.constraint(
                equalTo: titleLabel.centerYAnchor
            ),

            countLabel.leadingAnchor.constraint(
                equalTo: titleLabel.leadingAnchor
            ),
            countLabel.trailingAnchor.constraint(
                lessThanOrEqualTo: trailingAnchor,
                constant: -14
            ),
            countLabel.topAnchor.constraint(
                equalTo: centerYAnchor,
                constant: 5
            ),

            openButton.leadingAnchor.constraint(equalTo: leadingAnchor),
            openButton.trailingAnchor.constraint(equalTo: trailingAnchor),
            openButton.topAnchor.constraint(equalTo: topAnchor),
            openButton.bottomAnchor.constraint(equalTo: bottomAnchor),

            selectionButton.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 14
            ),
            selectionButton.centerYAnchor.constraint(equalTo: centerYAnchor),
            selectionButton.widthAnchor.constraint(equalToConstant: 28),
            selectionButton.heightAnchor.constraint(equalToConstant: 40),

            statusIndicator.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 20
            ),
            statusIndicator.centerYAnchor.constraint(
                equalTo: centerYAnchor
            ),
            statusIndicator.widthAnchor.constraint(equalToConstant: 16),
            statusIndicator.heightAnchor.constraint(equalToConstant: 16)
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(
        month: LibraryMonthKey,
        summary: HomeMonthSummary?,
        selected: Bool,
        selectionEnabled: Bool,
        executionPhase: MacMonthExecutionPhase?,
        side: SelectionSide,
        onToggle: @escaping () -> Void,
        onOpen: @escaping () -> Void
    ) {
        self.onToggle = onToggle
        self.onOpen = onOpen

        let isTerminal = executionPhase == .completed
            || executionPhase == .failed
        let background = isTerminal
            ? NSColor.controlBackgroundColor
            : NSColor.wmMaterialMonthSurface(for: month.month)
        layer?.backgroundColor = background.cgColor

        let isAvailable = summary != nil
        let summary = summary ?? .empty(month: month)
        let titleColor = isTerminal
            ? NSColor.secondaryLabelColor
            : NSColor.wmMaterialMonthTitle(for: month.month)
        let detailColor = isTerminal
            ? NSColor.tertiaryLabelColor
            : NSColor.wmMaterialMonthDetail(for: month.month)
        titleLabel.stringValue = summary.monthTitle
        titleLabel.textColor = titleColor
        sizeLabel.stringValue = summary.sizeText ?? ""
        sizeLabel.textColor = detailColor
        countLabel.attributedStringValue =
            MacMediaCountPresentation.attributedString(
                photoCount: summary.photoCount,
                videoCount: summary.videoCount,
                fontSize: 12,
                color: detailColor
            )
        countLabel.textColor = detailColor

        let sideTitle = side == .local
            ? String(
                localized: "mediaBrowser.mode.local",
                defaultValue: "Local"
            )
            : String(
                localized: "mediaBrowser.mode.remote",
                defaultValue: "Remote"
            )
        configureLeadingStatus(
            executionPhase,
            selected: selected,
            selectionEnabled: selectionEnabled && isAvailable,
            titleColor: titleColor,
            detailColor: detailColor,
            accessibilityPrefix:
                "\(sideTitle), \(summary.monthTitle)"
        )
        openButton.isEnabled = true
        openButton.setAccessibilityLabel(
            "\(sideTitle), \(summary.monthTitle)"
        )
    }

    private func configureLeadingStatus(
        _ phase: MacMonthExecutionPhase?,
        selected: Bool,
        selectionEnabled: Bool,
        titleColor: NSColor,
        detailColor: NSColor,
        accessibilityPrefix: String
    ) {
        statusIndicator.stopAnimation(nil)
        statusIndicator.isHidden = true
        selectionButton.isHidden = false
        selectionButton.isEnabled = false

        switch phase {
        case .uploading, .downloading:
            selectionButton.isHidden = true
            statusIndicator.isHidden = false
            statusIndicator.startAnimation(nil)
            statusIndicator.setAccessibilityLabel(
                "\(accessibilityPrefix), \(executionAccessibilityText(phase))"
            )
        case .uploadPaused, .downloadPaused:
            selectionButton.image = statusImage("pause.circle.fill")
            selectionButton.contentTintColor = titleColor
            selectionButton.setAccessibilityLabel(
                "\(accessibilityPrefix), \(executionAccessibilityText(phase))"
            )
        case .completed:
            selectionButton.image = statusImage(
                "checkmark.circle.fill"
            )
            selectionButton.contentTintColor = .systemGreen
            selectionButton.setAccessibilityLabel(
                "\(accessibilityPrefix), \(executionAccessibilityText(phase))"
            )
        case .partiallyFailed:
            selectionButton.image = statusImage(
                "exclamationmark.triangle.fill"
            )
            selectionButton.contentTintColor = .systemOrange
            selectionButton.setAccessibilityLabel(
                "\(accessibilityPrefix), \(executionAccessibilityText(phase))"
            )
        case .failed:
            selectionButton.image = statusImage(
                "exclamationmark.circle.fill"
            )
            selectionButton.contentTintColor = .systemRed
            selectionButton.setAccessibilityLabel(
                "\(accessibilityPrefix), \(executionAccessibilityText(phase))"
            )
        case .pending, nil:
            selectionButton.image = selectionImage(
                for: selected ? .all : .none
            )
            selectionButton.contentTintColor = selectionEnabled
                ? (selected ? titleColor : detailColor)
                : (phase == .pending && selected
                    ? titleColor
                    : .quaternaryLabelColor)
            selectionButton.isEnabled = selectionEnabled
            selectionButton.setAccessibilityLabel(
                accessibilityPrefix
            )
        }
    }

    @objc private func toggle(_ sender: Any?) {
        onToggle?()
    }

    @objc private func open(_ sender: Any?) {
        onOpen?()
    }
}

@MainActor
private func statusImage(_ symbolName: String) -> NSImage? {
    NSImage(
        systemSymbolName: symbolName,
        accessibilityDescription: nil
    )?.withSymbolConfiguration(
        NSImage.SymbolConfiguration(
            pointSize: 16,
            weight: .medium
        )
    )
}

@MainActor
private func executionAccessibilityText(
    _ phase: MacMonthExecutionPhase?
) -> String {
    switch phase {
    case .uploading:
        return String(localized: "home.execution.uploading")
    case .downloading:
        return String(localized: "home.execution.phaseDownload")
    case .uploadPaused, .downloadPaused:
        return String(localized: "home.execution.paused")
    case .completed:
        return String(localized: "home.execution.completed")
    case .partiallyFailed:
        return String(localized: "home.execution.partialFailed")
    case .failed:
        return String(localized: "home.execution.failed")
    case .pending, nil:
        return ""
    }
}

@MainActor
private func configureSelectionButton(_ button: NSButton) {
    button.isBordered = false
    button.title = ""
    button.imagePosition = .imageOnly
    button.focusRingType = .none
    button.imageScaling = .scaleProportionallyDown
}

@MainActor
private func selectionImage(
    for state: HomeSelectionState
) -> NSImage? {
    let symbolName: String
    switch state {
    case .none:
        symbolName = "circle"
    case .partial:
        symbolName = "minus.circle.fill"
    case .all:
        symbolName = "checkmark.circle.fill"
    }
    let image = NSImage(
        systemSymbolName: symbolName,
        accessibilityDescription: nil
    )
    return image?.withSymbolConfiguration(
        NSImage.SymbolConfiguration(
            pointSize: 16,
            weight: .medium
        )
    )
}

private extension NSUserInterfaceItemIdentifier {
    static let localPaneColumn = NSUserInterfaceItemIdentifier(
        "LibraryLocalPaneColumn"
    )
    static let remotePaneColumn = NSUserInterfaceItemIdentifier(
        "LibraryRemotePaneColumn"
    )
    static let yearPaneCell = NSUserInterfaceItemIdentifier(
        "LibraryYearPaneCell"
    )
    static let monthPaneCell = NSUserInterfaceItemIdentifier(
        "LibraryMonthPaneCell"
    )
    static let libraryRowView = NSUserInterfaceItemIdentifier(
        "LibraryRowView"
    )
}
