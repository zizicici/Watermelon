import AppKit

@MainActor
final class MacExecutionLogHistoryViewController: NSViewController {
    private struct Session {
        let info: ExecutionLogSessionInfo?
        let title: String
        let subtitle: String
        let entries: [ExecutionLogEntry]?
    }

    private let sessionTable = NSTableView()
    private let entryTable = NSTableView()
    private let searchField = NSSearchField()
    private let levelPopup = NSPopUpButton()
    private let revealButton = NSButton()
    private let liveStatusStack = NSStackView()
    private let liveStatusValueLabel = NSTextField(
        labelWithString: ""
    )
    private let liveSpeedLabel = NSTextField(
        labelWithString: ""
    )
    private let liveRemainingLabel = NSTextField(
        labelWithString: ""
    )
    private let sessionMenu = NSMenu()
    private let deleteMenuItem = NSMenuItem()
    private let entryEmptyLabel = NSTextField(
        wrappingLabelWithString: String(localized: "log.empty")
    )
    private var sessions: [Session] = []
    private var entries: [ExecutionLogEntry] = []
    private var visibleEntries: [ExecutionLogEntry] = []
    private var preferredSessionURL: URL?
    private let activeSessionURLProvider: () -> URL?
    private let liveSnapshotProvider:
        () -> MacExecutionLogLiveSnapshot?
    private var liveRefreshTimer: Timer?

    init(
        preferredSessionURL: URL? = nil,
        activeSessionURLProvider: @escaping () -> URL? = { nil },
        liveSnapshotProvider:
            @escaping () -> MacExecutionLogLiveSnapshot? = { nil }
    ) {
        self.preferredSessionURL = preferredSessionURL
        self.activeSessionURLProvider = activeSessionURLProvider
        self.liveSnapshotProvider = liveSnapshotProvider
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        view = NSView()

        configureTables()
        configureControls()
        configureLiveStatus()

        let sessionScroll = NSScrollView()
        sessionScroll.documentView = sessionTable
        sessionScroll.hasVerticalScroller = true
        sessionScroll.autohidesScrollers = true
        sessionScroll.borderType = .bezelBorder

        let entryScroll = NSScrollView()
        entryScroll.documentView = entryTable
        entryScroll.hasVerticalScroller = true
        entryScroll.hasHorizontalScroller = true
        entryScroll.autohidesScrollers = true
        entryScroll.borderType = .bezelBorder

        let content = NSStackView(views: [sessionScroll, entryScroll])
        content.orientation = .horizontal
        content.alignment = .height
        content.spacing = 14

        let refreshButton = NSButton(
            image: NSImage(
                systemSymbolName: "arrow.clockwise",
                accessibilityDescription: nil
            ) ?? NSImage(),
            target: self,
            action: #selector(refresh(_:))
        )
        refreshButton.bezelStyle = .texturedRounded
        refreshButton.toolTip = String(
            localized: "common.refresh",
            defaultValue: "Refresh"
        )

        let toolbar = NSStackView(
            views: [
                NSView(),
                searchField,
                levelPopup,
                revealButton,
                refreshButton
            ]
        )
        toolbar.orientation = .horizontal
        toolbar.alignment = .centerY
        toolbar.spacing = 10

        let root = NSStackView(
            views: [liveStatusStack, toolbar, content]
        )
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 10
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)
        entryEmptyLabel.alignment = .center
        entryEmptyLabel.textColor = .secondaryLabelColor
        entryEmptyLabel.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(entryEmptyLabel)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor, constant: 24),
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
                constant: -22
            ),
            toolbar.widthAnchor.constraint(equalTo: root.widthAnchor),
            liveStatusStack.widthAnchor.constraint(
                equalTo: root.widthAnchor
            ),
            content.widthAnchor.constraint(equalTo: root.widthAnchor),
            content.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 440
            ),
            sessionScroll.widthAnchor.constraint(equalToConstant: 260),
            entryScroll.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 540
            ),
            searchField.widthAnchor.constraint(equalToConstant: 210),
            entryEmptyLabel.centerXAnchor.constraint(
                equalTo: entryScroll.centerXAnchor
            ),
            entryEmptyLabel.centerYAnchor.constraint(
                equalTo: entryScroll.centerYAnchor
            ),
            entryEmptyLabel.widthAnchor.constraint(
                lessThanOrEqualTo: entryScroll.widthAnchor,
                constant: -40
            )
        ])
        reloadSessions()
    }

    override func viewWillAppear() {
        super.viewWillAppear()
        startLiveRefresh()
    }

    override func viewDidDisappear() {
        super.viewDidDisappear()
        liveRefreshTimer?.invalidate()
        liveRefreshTimer = nil
    }

    private func configureTables() {
        let sessionColumn = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("session")
        )
        sessionTable.addTableColumn(sessionColumn)
        sessionTable.headerView = nil
        sessionTable.rowHeight = 46
        sessionTable.dataSource = self
        sessionTable.delegate = self
        deleteMenuItem.title = String(
            localized: "log.history.delete"
        )
        deleteMenuItem.target = self
        deleteMenuItem.action = #selector(deleteSelectedLog(_:))
        sessionMenu.addItem(deleteMenuItem)
        sessionMenu.delegate = self
        sessionMenu.autoenablesItems = false
        sessionTable.menu = sessionMenu

        let definitions: [(String, String, CGFloat)] = [
            (
                "time",
                String(localized: "mac.logs.column.time"),
                86
            ),
            (
                "level",
                String(localized: "log.levelSection"),
                66
            ),
            (
                "message",
                String(localized: "mac.logs.column.message"),
                620
            )
        ]
        for definition in definitions {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier(definition.0)
            )
            column.title = definition.1
            column.width = definition.2
            column.minWidth = definition.0 == "message"
                ? 300
                : definition.2
            entryTable.addTableColumn(column)
        }
        entryTable.headerView = NSTableHeaderView()
        entryTable.rowHeight = 27
        entryTable.usesAlternatingRowBackgroundColors = true
        entryTable.dataSource = self
        entryTable.delegate = self
    }

    private func configureControls() {
        searchField.placeholderString = String(
            localized: "mac.logs.search",
            defaultValue: "Search messages"
        )
        searchField.target = self
        searchField.action = #selector(changeFilter(_:))
        searchField.sendsSearchStringImmediately = true

        levelPopup.addItems(
            withTitles: [
                String(localized: "log.showAll"),
                String(localized: "mac.logs.filter.warnings"),
                String(localized: "mac.logs.filter.errors")
            ]
        )
        levelPopup.selectItem(at: 0)
        levelPopup.target = self
        levelPopup.action = #selector(changeFilter(_:))

        revealButton.title = String(
            localized: "mac.logs.reveal",
            defaultValue: "Reveal Log"
        )
        revealButton.bezelStyle = .rounded
        revealButton.target = self
        revealButton.action = #selector(revealLog(_:))
        revealButton.isEnabled = false
    }

    private func configureLiveStatus() {
        let title = NSTextField(
            labelWithString: String(localized: "log.status")
        )
        title.font = .systemFont(ofSize: 12, weight: .semibold)
        title.textColor = .secondaryLabelColor

        liveStatusValueLabel.font = .systemFont(
            ofSize: 13,
            weight: .medium
        )
        liveStatusValueLabel.lineBreakMode = .byTruncatingTail
        liveStatusValueLabel.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        for label in [liveSpeedLabel, liveRemainingLabel] {
            label.font = .monospacedDigitSystemFont(
                ofSize: 12,
                weight: .medium
            )
            label.textColor = .secondaryLabelColor
            label.setContentHuggingPriority(
                .required,
                for: .horizontal
            )
        }

        liveStatusStack.setViews(
            [
                title,
                liveStatusValueLabel,
                NSView(),
                liveSpeedLabel,
                liveRemainingLabel
            ],
            in: .leading
        )
        liveStatusStack.orientation = .horizontal
        liveStatusStack.alignment = .centerY
        liveStatusStack.spacing = 10
        liveStatusStack.edgeInsets = NSEdgeInsets(
            top: 8,
            left: 10,
            bottom: 8,
            right: 10
        )
        liveStatusStack.isHidden = true
    }

    private func reloadSessions() {
        let selectedURL = selectedSessionURL
        ExecutionLogFileStore.purgeExpired()
        sessions = ExecutionLogFileStore.listSessions().map { info in
            let kind = info.kind == .manual
                ? String(localized: "log.history.section.manual")
                : String(localized: "log.history.section.auto")
            return Session(
                info: info,
                title: Self.sessionDateFormatter.string(
                    from: info.startedAt
                ),
                subtitle: kind,
                entries: nil
            )
        }
        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains("--demo-logs") {
            let now = Date()
            sessions.insert(
                Session(
                    info: nil,
                    title: Self.sessionDateFormatter.string(from: now),
                    subtitle: String(
                        localized: "log.history.section.manual"
                    ),
                    entries: [
                        ExecutionLogEntry(
                            timestamp: now.addingTimeInterval(-22),
                            message: String.localizedStringWithFormat(
                                String(
                                    localized:
                                        "home.execution.log.startExecution"
                                ),
                                Int64(2),
                                Int64(1),
                                Int64(0)
                            ),
                            level: .info
                        ),
                        ExecutionLogEntry(
                            timestamp: now.addingTimeInterval(-19),
                            message: String.localizedStringWithFormat(
                                String(
                                    localized:
                                        "home.execution.log.startIndex"
                                ),
                                Int64(148)
                            ),
                            level: .info
                        ),
                        ExecutionLogEntry(
                            timestamp: now.addingTimeInterval(-12),
                            message: String.localizedStringWithFormat(
                                String(
                                    localized:
                                        "home.execution.log.indexComplete"
                                ),
                                Int64(145),
                                Int64(3),
                                Int64(0)
                            ),
                            level: .warning
                        ),
                        ExecutionLogEntry(
                            timestamp: now.addingTimeInterval(-7),
                            message: String.localizedStringWithFormat(
                                String(
                                    localized:
                                        "home.execution.log.indexComplete"
                                ),
                                Int64(3),
                                Int64(0),
                                Int64(0)
                            ),
                            level: .info
                        ),
                        ExecutionLogEntry(
                            timestamp: now,
                            message: String.localizedStringWithFormat(
                                String(
                                    localized:
                                        "home.execution.log.uploadPhaseDone"
                                ),
                                Int64(148),
                                Int64(0),
                                Int64(0)
                            ),
                            level: .info
                        )
                    ]
                ),
                at: 0
            )
        }
        #endif
        sessionTable.reloadData()
        if sessions.isEmpty {
            entries = []
            visibleEntries = []
            entryTable.reloadData()
            revealButton.isEnabled = false
            refreshLivePresentation()
            return
        }
        let preferredURL = preferredSessionURL ?? selectedURL
        preferredSessionURL = nil
        guard let row = MacExecutionLogSessionPolicy.preferredIndex(
            sessionURLs: sessions.map(\.info?.url),
            preferredURL: preferredURL
        ) else {
            return
        }
        sessionTable.selectRowIndexes(
            IndexSet(integer: row),
            byExtendingSelection: false
        )
        loadSelectedSession()
    }

    func selectSession(url: URL) {
        preferredSessionURL = url
        if isViewLoaded {
            reloadSessions()
        }
    }

    private var selectedSessionURL: URL? {
        let row = sessionTable.selectedRow
        guard sessions.indices.contains(row) else { return nil }
        return sessions[row].info?.url
    }

    private func startLiveRefresh() {
        guard liveRefreshTimer == nil else { return }
        let timer = Timer(
            timeInterval: 1,
            target: self,
            selector: #selector(refreshLiveSession(_:)),
            userInfo: nil,
            repeats: true
        )
        liveRefreshTimer = timer
        RunLoop.main.add(timer, forMode: .common)
    }

    @objc private func refreshLiveSession(_ timer: Timer) {
        refreshLivePresentation()
        guard let activeURL = activeSessionURLProvider() else { return }
        if let preferredSessionURL,
           preferredSessionURL.standardizedFileURL
                == activeURL.standardizedFileURL {
            reloadSessions()
            return
        }
        guard selectedSessionURL?.standardizedFileURL
                == activeURL.standardizedFileURL else {
            return
        }
        loadSelectedSession()
    }

    private func loadSelectedSession() {
        let row = sessionTable.selectedRow
        guard sessions.indices.contains(row) else {
            entries = []
            applyEntryFilter()
            revealButton.isEnabled = false
            refreshLivePresentation()
            return
        }
        let session = sessions[row]
        do {
            entries = try session.entries
                ?? session.info?.readEntries()
                ?? []
            revealButton.isEnabled = session.info != nil
        } catch {
            entries = [
                ExecutionLogEntry(
                    timestamp: Date(),
                    message: error.localizedDescription,
                    level: .error
                )
            ]
            revealButton.isEnabled = session.info != nil
        }
        applyEntryFilter()
        refreshLivePresentation()
    }

    private func refreshLivePresentation() {
        guard let snapshot = liveSnapshotProvider(),
              selectedSessionURL?.standardizedFileURL
                == snapshot.sessionURL.standardizedFileURL else {
            liveStatusStack.isHidden = true
            return
        }
        liveStatusValueLabel.stringValue = snapshot.statusText
        liveSpeedLabel.stringValue =
            HomeExecutionTransferFormatter.speed(
                snapshot.transferMetrics.speedBytesPerSecond
            ) ?? String(localized: "log.transfer.waiting")
        liveRemainingLabel.stringValue =
            HomeExecutionTransferFormatter.remainingTime(
                snapshot.transferMetrics.remainingTimeSeconds
            ) ?? String(
                localized: "log.transfer.estimating"
            )
        liveStatusStack.isHidden = false
    }

    private func applyEntryFilter() {
        let query = searchField.stringValue.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        visibleEntries = entries.filter { entry in
            guard Self.isIncluded(
                entry.level,
                popupIndex: levelPopup.indexOfSelectedItem
            ) else {
                return false
            }
            return query.isEmpty
                || entry.message.localizedCaseInsensitiveContains(query)
        }
        entryTable.reloadData()
        entryEmptyLabel.isHidden = !visibleEntries.isEmpty
    }

    private static func isIncluded(
        _ level: ExecutionLogLevel,
        popupIndex: Int
    ) -> Bool {
        switch popupIndex {
        case 1:
            return level == .warning || level == .error
        case 2:
            return level == .error
        default:
            return true
        }
    }

    @objc private func refresh(_ sender: Any?) {
        reloadSessions()
    }

    @objc private func changeFilter(_ sender: Any?) {
        applyEntryFilter()
    }

    @objc private func revealLog(_ sender: Any?) {
        let row = sessionTable.selectedRow
        guard sessions.indices.contains(row),
              let url = sessions[row].info?.url else {
            return
        }
        NSWorkspace.shared.activateFileViewerSelecting([url])
    }

    @objc private func deleteSelectedLog(_ sender: Any?) {
        let row = sessionTable.clickedRow >= 0
            ? sessionTable.clickedRow
            : sessionTable.selectedRow
        guard sessions.indices.contains(row),
              let url = sessions[row].info?.url,
              MacExecutionLogSessionPolicy.canDelete(
                sessionURL: url,
                activeSessionURL: activeSessionURLProvider()
              ) else {
            return
        }
        try? FileManager.default.removeItem(at: url)
        reloadSessions()
    }

    private static func color(
        for level: ExecutionLogLevel
    ) -> NSColor {
        switch level {
        case .debug:
            return .tertiaryLabelColor
        case .info:
            return .labelColor
        case .warning:
            return .wmMaterialWarningDetail
        case .error:
            return .wmMaterialError
        }
    }

    private static let sessionDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()

    private static let lineDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()
}

extension MacExecutionLogHistoryViewController: NSMenuDelegate {
    func menuWillOpen(_ menu: NSMenu) {
        let row = sessionTable.clickedRow >= 0
            ? sessionTable.clickedRow
            : sessionTable.selectedRow
        deleteMenuItem.isEnabled =
            sessions.indices.contains(row)
            && MacExecutionLogSessionPolicy.canDelete(
                sessionURL: sessions[row].info?.url,
                activeSessionURL: activeSessionURLProvider()
            )
    }
}

extension MacExecutionLogHistoryViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        tableView === sessionTable ? sessions.count : visibleEntries.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        if tableView === sessionTable {
            guard sessions.indices.contains(row) else { return nil }
            return sessionCell(sessions[row])
        }
        guard visibleEntries.indices.contains(row),
              let tableColumn else {
            return nil
        }
        return entryCell(
            visibleEntries[row],
            column: tableColumn.identifier.rawValue
        )
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard notification.object as? NSTableView === sessionTable else {
            return
        }
        loadSelectedSession()
    }

    private func sessionCell(_ session: Session) -> NSView {
        let identifier = NSUserInterfaceItemIdentifier("sessionCell")
        let cell = sessionTable.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? NSTableCellView ?? NSTableCellView()
        cell.identifier = identifier
        let stack: NSStackView
        if let existing = cell.subviews.first(
            where: { $0 is NSStackView }
        ) as? NSStackView {
            stack = existing
        } else {
            let title = NSTextField(labelWithString: "")
            title.font = .systemFont(ofSize: 13, weight: .medium)
            let subtitle = NSTextField(labelWithString: "")
            subtitle.font = .systemFont(ofSize: 11)
            subtitle.textColor = .secondaryLabelColor
            stack = NSStackView(views: [title, subtitle])
            stack.orientation = .vertical
            stack.alignment = .leading
            stack.spacing = 2
            stack.translatesAutoresizingMaskIntoConstraints = false
            cell.addSubview(stack)
            NSLayoutConstraint.activate([
                stack.leadingAnchor.constraint(
                    equalTo: cell.leadingAnchor,
                    constant: 8
                ),
                stack.trailingAnchor.constraint(
                    equalTo: cell.trailingAnchor,
                    constant: -8
                ),
                stack.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                )
            ])
        }
        (stack.arrangedSubviews[0] as? NSTextField)?.stringValue =
            session.title
        (stack.arrangedSubviews[1] as? NSTextField)?.stringValue =
            session.subtitle
        return cell
    }

    private func entryCell(
        _ entry: ExecutionLogEntry,
        column: String
    ) -> NSView {
        let identifier = NSUserInterfaceItemIdentifier(
            "entry.\(column)"
        )
        let cell = entryTable.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? NSTableCellView ?? NSTableCellView()
        cell.identifier = identifier
        let field: NSTextField
        if let existing = cell.textField {
            field = existing
        } else {
            field = NSTextField(labelWithString: "")
            field.translatesAutoresizingMaskIntoConstraints = false
            field.lineBreakMode = .byTruncatingTail
            cell.textField = field
            cell.addSubview(field)
            NSLayoutConstraint.activate([
                field.leadingAnchor.constraint(
                    equalTo: cell.leadingAnchor,
                    constant: 4
                ),
                field.trailingAnchor.constraint(
                    equalTo: cell.trailingAnchor,
                    constant: -4
                ),
                field.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                )
            ])
        }
        switch column {
        case "time":
            field.stringValue = Self.lineDateFormatter.string(
                from: entry.timestamp
            )
            field.font = .monospacedDigitSystemFont(
                ofSize: 11,
                weight: .regular
            )
            field.textColor = .secondaryLabelColor
        case "level":
            field.stringValue = Self.localizedTitle(
                for: entry.level
            ).uppercased()
            field.font = .systemFont(ofSize: 10, weight: .semibold)
            field.textColor = Self.color(for: entry.level)
        default:
            field.stringValue = entry.message
            field.font = .systemFont(ofSize: 12)
            field.textColor = Self.color(for: entry.level)
            field.toolTip = entry.message
        }
        return cell
    }

    private static func localizedTitle(
        for level: ExecutionLogLevel
    ) -> String {
        switch level {
        case .debug:
            return String(localized: "log.level.debug")
        case .info:
            return String(localized: "log.level.info")
        case .warning:
            return String(localized: "log.level.warning")
        case .error:
            return String(localized: "log.level.error")
        }
    }
}
