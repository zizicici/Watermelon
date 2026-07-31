import AppKit

@MainActor
final class MacRepositoryMaintenanceViewController: NSViewController {
    var onBusyChanged: ((Bool) -> Void)?
    var onRepositoryChanged: (() -> Void)?

    private enum Operation {
        case verify
        case scan
        case hashCheck
        case delete
    }

    private let profile: ServerProfileRecord
    private let sessionGeneration: UInt64
    private let appSession: AppSession
    private let controller: RemoteMaintenanceController
    private let backupCoordinator: BackupCoordinator
    private let databaseManager: DatabaseManager
    private var operation: Operation?
    private var closeWhenOperationFinishes = false
    private var scanResult: LeftoverScanResult?
    private var selectedPaths = Set<String>()
    private var hashStatusByPath: [String: LeftoverHashCheckStatus] = [:]
    nonisolated(unsafe) private var observer: NSObjectProtocol?
    nonisolated(unsafe) private var snapshotObserver: NSObjectProtocol?
    nonisolated(unsafe) private var sessionObserver: NSObjectProtocol?

    private let destinationLabel = NSTextField(labelWithString: "")
    private let overviewLabel =
        NSTextField(wrappingLabelWithString: "")
    private let incompleteAssetsButton = NSButton()
    private let statusLabel = NSTextField(wrappingLabelWithString: "")
    private let progressIndicator = NSProgressIndicator()
    private let stopButton = NSButton()
    private let verifyButton = NSButton()
    private let scanButton = NSButton()
    private let tableView = NSTableView()
    private let scrollView = NSScrollView()
    private let emptyLabel = NSTextField(wrappingLabelWithString: "")
    private let selectAllButton = NSButton()
    private let thumbnailCheckbox = NSButton()
    private let hashButton = NSButton()
    private let deleteButton = NSButton()
    private let selectionLabel = NSTextField(labelWithString: "")
    private let progressStack = NSStackView()
    private let reviewTopStack = NSStackView()
    private let reviewBottomStack = NSStackView()

    init(
        profile: ServerProfileRecord,
        sessionGeneration: UInt64,
        appSession: AppSession,
        controller: RemoteMaintenanceController,
        backupCoordinator: BackupCoordinator,
        databaseManager: DatabaseManager
    ) {
        self.profile = profile
        self.sessionGeneration = sessionGeneration
        self.appSession = appSession
        self.controller = controller
        self.backupCoordinator = backupCoordinator
        self.databaseManager = databaseManager
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let observer {
            NotificationCenter.default.removeObserver(observer)
        }
        if let snapshotObserver {
            NotificationCenter.default.removeObserver(
                snapshotObserver
            )
        }
        if let sessionObserver {
            NotificationCenter.default.removeObserver(sessionObserver)
        }
    }

    override func loadView() {
        view = NSView()

        destinationLabel.stringValue = profile.name
        destinationLabel.font = .systemFont(ofSize: 13, weight: .medium)
        destinationLabel.textColor = .secondaryLabelColor

        configureOverview()
        configureActionButtons()
        configureProgressViews()
        configureTable()
        configureReviewControls()

        let actionStack = NSStackView(views: [verifyButton, scanButton])
        actionStack.orientation = .horizontal
        actionStack.spacing = 10

        let overviewStack = NSStackView(
            views: [overviewLabel, incompleteAssetsButton]
        )
        overviewStack.orientation = .horizontal
        overviewStack.alignment = .centerY
        overviewStack.spacing = 12

        progressStack.setViews(
            [statusLabel, progressIndicator, stopButton],
            in: .leading
        )
        progressStack.orientation = .horizontal
        progressStack.alignment = .centerY
        progressStack.spacing = 10
        progressStack.translatesAutoresizingMaskIntoConstraints = false

        emptyLabel.alignment = .center
        emptyLabel.textColor = .secondaryLabelColor
        emptyLabel.font = .systemFont(ofSize: 14)
        emptyLabel.translatesAutoresizingMaskIntoConstraints = false

        reviewTopStack.setViews(
            [selectAllButton, thumbnailCheckbox, selectionLabel],
            in: .leading
        )
        reviewTopStack.orientation = .horizontal
        reviewTopStack.alignment = .centerY
        reviewTopStack.spacing = 12

        let flexibleSpace = NSView()
        reviewBottomStack.setViews(
            [hashButton, flexibleSpace, deleteButton],
            in: .leading
        )
        reviewBottomStack.orientation = .horizontal
        reviewBottomStack.alignment = .centerY
        reviewBottomStack.spacing = 10

        let stack = NSStackView(
            views: [
                destinationLabel,
                overviewStack,
                actionStack,
                progressStack,
                reviewTopStack,
                scrollView,
                emptyLabel,
                reviewBottomStack
            ]
        )
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: view.topAnchor, constant: 28),
            stack.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 28
            ),
            stack.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -28
            ),
            stack.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -24
            ),
            progressStack.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            overviewStack.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            statusLabel.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 260
            ),
            progressIndicator.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 180
            ),
            reviewTopStack.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            scrollView.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            scrollView.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 250
            ),
            emptyLabel.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            reviewBottomStack.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            ),
            flexibleSpace.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 20
            )
        ])

        observer = NotificationCenter.default.addObserver(
            forName: .RemoteMaintenanceDidChange,
            object: controller,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.maintenanceDidChange()
            }
        }
        snapshotObserver = NotificationCenter.default.addObserver(
            forName: .RemoteLibrarySnapshotDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.refreshOverview()
            }
        }
        sessionObserver = NotificationCenter.default.addObserver(
            forName: .AppSessionChanged,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.sessionDidChange()
            }
        }
        refreshOverview()
        render()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
    }

    #if DEBUG
    func showDemoReview() {
        loadViewIfNeeded()
        let july = LibraryMonthKey(year: 2026, month: 7)
        let june = LibraryMonthKey(year: 2026, month: 6)
        let files = [
            LeftoverFile(
                month: july,
                fileName: "IMG_2048 copy.HEIC",
                path: "2026/07/IMG_2048 copy.HEIC",
                size: 4_812_443
            ),
            LeftoverFile(
                month: july,
                fileName: "VID_0912.MOV",
                path: "2026/07/VID_0912.MOV",
                size: 48_442_112
            ),
            LeftoverFile(
                month: june,
                fileName: "DSC_7710.JPG",
                path: "2026/06/DSC_7710.JPG",
                size: 7_201_554
            )
        ]
        scanResult = LeftoverScanResult(
            groups: [
                LeftoverMonthGroup(month: july, files: Array(files[0...1])),
                LeftoverMonthGroup(month: june, files: [files[2]])
            ],
            orphanThumbnailCount: 12,
            orphanThumbnailBytes: 1_244_320,
            probableMatchesByPath: [
                files[0].path: LeftoverProbableMatchSummary(
                    matches: [],
                    totalCount: 1
                )
            ]
        )
        hashStatusByPath[files[1].path] = .noMatch(
            hashHex: String(repeating: "a", count: 64)
        )
        selectedPaths = [files[0].path]
        emptyLabel.stringValue = ""
        render()
    }
    #endif

    private var files: [LeftoverFile] {
        scanResult?.groups
            .sorted { $0.month > $1.month }
            .flatMap { $0.files.sorted { $0.fileName < $1.fileName } } ?? []
    }

    private var selectedFiles: [LeftoverFile] {
        files.filter { selectedPaths.contains($0.path) }
    }

    private var includesThumbnails: Bool {
        thumbnailCheckbox.state == .on
    }

    private func configureOverview() {
        overviewLabel.font = .systemFont(ofSize: 12)
        overviewLabel.textColor = .secondaryLabelColor
        overviewLabel.maximumNumberOfLines = 2
        overviewLabel.lineBreakMode = .byWordWrapping
        overviewLabel.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        incompleteAssetsButton.bezelStyle = .inline
        incompleteAssetsButton.target = self
        incompleteAssetsButton.action = #selector(
            showIncompleteAssets(_:)
        )
    }

    private func refreshOverview() {
        let digest = backupCoordinator.healthDigest()
        let firstLine = [
            overviewValue(
                title: String(
                    localized:
                        "storage.detail.overview.assetCount"
                ),
                value: Self.formatCount(digest.totalAssets)
            ),
            overviewValue(
                title: String(
                    localized:
                        "storage.detail.overview.resourceCount"
                ),
                value: Self.formatCount(digest.totalResources)
            ),
            overviewValue(
                title: String(
                    localized:
                        "storage.detail.overview.diskUsage"
                ),
                value: ByteCountFormatter.string(
                    fromByteCount: digest.totalSizeBytes,
                    countStyle: .file
                )
            )
        ].joined(separator: "  ·  ")
        let lastVerified = profile.id.flatMap {
            try? databaseManager.remoteVerifiedAt(profileID: $0)
        }
        let secondLine = [
            overviewValue(
                title: String(
                    localized:
                        "storage.detail.overview.lastIndexSyncedAt"
                ),
                value: Self.formatDate(
                    digest.lastIndexSyncedAt
                )
            ),
            overviewValue(
                title: String(
                    localized:
                        "storage.detail.overview.lastVerifiedAt"
                ),
                value: Self.formatDate(lastVerified)
            )
        ].joined(separator: "  ·  ")
        overviewLabel.stringValue = firstLine + "\n" + secondLine

        incompleteAssetsButton.title =
            String(
                localized:
                    "storage.detail.overview.incompleteAssets"
            )
            + " "
            + Self.formatCount(digest.incompleteAssets.count)
        incompleteAssetsButton.isEnabled =
            !digest.incompleteAssets.isEmpty
        incompleteAssetsButton.contentTintColor =
            digest.incompleteAssets.isEmpty
            ? .secondaryLabelColor
            : .wmMaterialWarningDetail
    }

    private func sessionDidChange() {
        let snapshot = appSession.snapshot
        if MacRepositoryMaintenanceSessionPolicy.shouldClose(
            representedProfile: profile,
            representedGeneration: sessionGeneration,
            current: snapshot,
            isBusy: operation != nil
        ) {
            view.window?.performClose(nil)
            return
        }
        guard MacRepositoryMaintenanceSessionPolicy.matches(
            representedProfile: profile,
            representedGeneration: sessionGeneration,
            current: snapshot
        ) else {
            return
        }
        refreshOverview()
    }

    private func overviewValue(
        title: String,
        value: String
    ) -> String {
        "\(title): \(value)"
    }

    @objc private func showIncompleteAssets(_ sender: Any?) {
        let entries = backupCoordinator
            .healthDigest()
            .incompleteAssets
        guard !entries.isEmpty else { return }
        let controller = MacIncompleteAssetsViewController(
            entries: entries
        )
        controller.title = String(
            localized:
                "storage.detail.incompleteAssets.title"
        )
        presentAsSheet(controller)
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

    private static func formatDate(_ date: Date?) -> String {
        guard let date else {
            return String(
                localized:
                    "storage.detail.overview.notAvailable"
            )
        }
        return date.formatted(
            date: .abbreviated,
            time: .shortened
        )
    }

    private func configureActionButtons() {
        verifyButton.title = String(
            localized: "storage.detail.overview.refreshButton",
            defaultValue: "Refresh and Verify"
        )
        verifyButton.bezelStyle = .rounded
        verifyButton.target = self
        verifyButton.action = #selector(startVerify(_:))

        scanButton.title = String(
            localized: "storage.detail.overview.checkLeftover",
            defaultValue: "Check Leftover Files"
        )
        scanButton.bezelStyle = .rounded
        scanButton.target = self
        scanButton.action = #selector(startScan(_:))
    }

    private func configureProgressViews() {
        statusLabel.textColor = .secondaryLabelColor
        statusLabel.lineBreakMode = .byTruncatingMiddle
        progressIndicator.style = .bar
        progressIndicator.controlSize = .small

        stopButton.title = String(
            localized: "common.stop",
            defaultValue: "Stop"
        )
        stopButton.bezelStyle = .rounded
        stopButton.contentTintColor = .wmMaterialError
        stopButton.target = self
        stopButton.action = #selector(stopOperation(_:))
    }

    private func configureTable() {
        let columns: [(String, String, CGFloat)] = [
            ("selected", "", 38),
            (
                "month",
                String(
                    localized: "mac.library.month"
                ),
                92
            ),
            (
                "file",
                String(
                    localized: "mediaMetadata.section.file"
                ),
                260
            ),
            (
                "size",
                String(
                    localized: "mac.maintenance.column.size"
                ),
                92
            ),
            (
                "status",
                String(
                    localized:
                        "mac.maintenance.column.assessment"
                ),
                250
            )
        ]
        for (identifier, title, width) in columns {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier(identifier)
            )
            column.title = title
            column.width = width
            column.minWidth = identifier == "file" ? 150 : width
            tableView.addTableColumn(column)
        }
        tableView.headerView = NSTableHeaderView()
        tableView.rowHeight = 30
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.allowsMultipleSelection = false
        tableView.dataSource = self
        tableView.delegate = self

        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.borderType = .bezelBorder
    }

    private func configureReviewControls() {
        selectAllButton.title = String(
            localized: "common.selectAll",
            defaultValue: "Select All"
        )
        selectAllButton.bezelStyle = .rounded
        selectAllButton.target = self
        selectAllButton.action = #selector(toggleSelectAll(_:))

        thumbnailCheckbox.setButtonType(.switch)
        thumbnailCheckbox.title = String(
            localized: "storage.detail.leftover.thumbnails.label",
            defaultValue: "Orphan thumbnails"
        )
        thumbnailCheckbox.target = self
        thumbnailCheckbox.action = #selector(selectionChanged(_:))

        selectionLabel.textColor = .secondaryLabelColor
        selectionLabel.alignment = .right

        hashButton.title = String(
            localized: "storage.detail.leftover.hash.action",
            defaultValue: "Verify with SHA-256"
        )
        hashButton.bezelStyle = .rounded
        hashButton.target = self
        hashButton.action = #selector(startHashCheck(_:))

        deleteButton.title = String(
            localized: "common.delete"
        )
        deleteButton.bezelStyle = .rounded
        deleteButton.contentTintColor = .wmMaterialError
        deleteButton.target = self
        deleteButton.action = #selector(confirmDelete(_:))
    }

    private func render() {
        let busy = operation != nil
        verifyButton.isEnabled = !busy
        scanButton.isEnabled = !busy
        statusLabel.isHidden = !busy
        progressIndicator.isHidden = !busy
        stopButton.isHidden = !busy

        if busy {
            renderProgress()
        } else {
            progressIndicator.stopAnimation(nil)
        }

        let hasReview = scanResult?.hasAnythingToClean == true
        progressStack.isHidden = !busy
        reviewTopStack.isHidden = !hasReview
        reviewBottomStack.isHidden = !hasReview
        scrollView.isHidden = !hasReview
        selectAllButton.isHidden = !hasReview
        thumbnailCheckbox.isHidden =
            !hasReview || (scanResult?.orphanThumbnailCount ?? 0) == 0
        selectionLabel.isHidden = !hasReview
        hashButton.isHidden = !hasReview || files.isEmpty
        deleteButton.isHidden = !hasReview
        emptyLabel.isHidden =
            hasReview || busy || emptyLabel.stringValue.isEmpty

        updateSelectionPresentation()
        tableView.reloadData()
    }

    private func renderProgress() {
        guard let operation else { return }
        let progress = controller.currentProgress
        statusLabel.stringValue = progressText(
            operation: operation,
            progress: progress
        )
        if let progress, progress.total > 0 {
            progressIndicator.isIndeterminate = false
            progressIndicator.minValue = 0
            progressIndicator.maxValue = Double(progress.total)
            progressIndicator.doubleValue = Double(progress.current)
        } else {
            progressIndicator.isIndeterminate = true
            progressIndicator.startAnimation(nil)
        }
    }

    private func progressText(
        operation: Operation,
        progress: RemoteSyncProgress?
    ) -> String {
        switch operation {
        case .verify:
            guard let progress, progress.total > 0 else {
                return String(
                    localized:
                        "storage.detail.overview.placeholder.verifyingStarting"
                )
            }
            return String.localizedStringWithFormat(
                String(
                    localized:
                        "storage.detail.overview.placeholder.verifying"
                ),
                progress.current,
                progress.total
            )
        case .scan:
            guard let progress else {
                return String(
                    localized:
                        "storage.detail.overview.placeholder.scanningLeftoverStarting"
                )
            }
            switch progress.kind {
            case .leftoverMaintenance(.scanningThumbnails):
                return String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.leftover.progress.thumbnails"
                    ),
                    progress.current,
                    progress.total
                )
            case .leftoverMaintenance(.finalizingScan):
                return String(
                    localized:
                        "storage.detail.leftover.progress.finalizingScan"
                )
            default:
                guard progress.total > 0 else {
                    return String(
                        localized:
                            "storage.detail.overview.placeholder.scanningLeftoverStarting"
                    )
                }
                return String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.overview.placeholder.scanningLeftover"
                    ),
                    progress.current,
                    progress.total
                )
            }
        case .hashCheck:
            guard let progress else {
                return String(
                    localized:
                        "storage.detail.leftover.progress.finalizingHashes"
                )
            }
            if case .leftoverMaintenance(.finalizingHashCheck) =
                progress.kind {
                return String(
                    localized:
                        "storage.detail.leftover.progress.finalizingHashes"
                )
            }
            return String.localizedStringWithFormat(
                String(
                    localized:
                        "storage.detail.leftover.progress.hashes"
                ),
                progress.current,
                progress.total
            )
        case .delete:
            guard let progress else {
                return String(
                    localized:
                        "storage.detail.overview.placeholder.deletingLeftoverStarting"
                )
            }
            switch progress.kind {
            case .leftoverMaintenance(
                .preparingThumbnailDeletion
            ):
                return String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.leftover.progress.preparingThumbnailDeletion"
                    ),
                    progress.current,
                    progress.total
                )
            case .leftoverMaintenance(
                .scanningThumbnailsForDeletion
            ):
                return String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.leftover.progress.thumbnails"
                    ),
                    progress.current,
                    progress.total
                )
            case .leftoverMaintenance(.finalizingDelete):
                return String(
                    localized:
                        "storage.detail.leftover.progress.finalizingDelete"
                )
            default:
                guard progress.total > 0 else {
                    return String(
                        localized:
                            "storage.detail.overview.placeholder.deletingLeftoverStarting"
                    )
                }
                return String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.overview.placeholder.deletingLeftover"
                    ),
                    progress.current,
                    progress.total
                )
            }
        }
    }

    private func updateSelectionPresentation() {
        guard let result = scanResult else { return }
        let dataBytes = selectedFiles.reduce(Int64(0)) { $0 + $1.size }
        let thumbnailCount = includesThumbnails
            ? result.orphanThumbnailCount
            : 0
        let thumbnailBytes = includesThumbnails
            ? result.orphanThumbnailBytes
            : 0
        let count = selectedFiles.count + thumbnailCount
        let bytes = dataBytes + thumbnailBytes
        selectionLabel.stringValue =
            String.localizedStringWithFormat(
            String(
                localized:
                    "storage.detail.leftover.selectionSummary"
            ),
            count,
            ByteCountFormatter.string(
                fromByteCount: bytes,
                countStyle: .file
            )
        )
        hashButton.isEnabled = !selectedFiles.isEmpty && operation == nil
        deleteButton.isEnabled = count > 0 && operation == nil
        selectAllButton.title = selectedPaths.count == files.count
            && !files.isEmpty
            ? String(
                localized: "mac.maintenance.deselectAll",
                defaultValue: "Deselect All"
            )
            : String(
                localized: "common.selectAll",
                defaultValue: "Select All"
            )
        if result.orphanThumbnailCount > 0 {
            thumbnailCheckbox.title = String(
                localized: "storage.detail.leftover.thumbnails.label",
                defaultValue: "Orphan thumbnails"
            )
        }
    }

    private func maintenanceContext()
        -> MacRepositoryMaintenanceContext? {
        guard let context = MacRepositoryMaintenanceContext.capture(
            representedProfile: profile,
            representedGeneration: sessionGeneration,
            current: appSession.snapshot
        ) else {
            presentConnectionError()
            return nil
        }
        return context
    }

    private func presentConnectionError() {
        presentError(
            String(
                localized: "remoteThumbnails.needConnection",
                defaultValue: "Connect to this node first."
            )
        )
    }

    @objc private func startVerify(_ sender: Any?) {
        guard operation == nil,
              let context = maintenanceContext() else {
            return
        }
        guard controller.startFullVerify(
            profile: context.profile,
            password: context.credential
        ) else {
            presentBusyError()
            return
        }
        begin(.verify)
    }

    @objc private func startScan(_ sender: Any?) {
        guard operation == nil,
              let context = maintenanceContext() else {
            return
        }
        guard controller.startScanLeftover(
            profile: context.profile,
            password: context.credential,
            onComplete: { [weak self] outcome in
                self?.finishScan(outcome)
            }
        ) else {
            presentBusyError()
            return
        }
        begin(.scan)
    }

    @objc private func startHashCheck(_ sender: Any?) {
        guard operation == nil,
              let result = scanResult,
              !selectedFiles.isEmpty,
              let context = maintenanceContext() else {
            return
        }
        guard controller.startCheckLeftoverHashes(
            profile: context.profile,
            password: context.credential,
            targets: selectedFiles,
            knownResourceCatalog: result.knownResourceCatalog,
            onComplete: { [weak self] outcome in
                self?.finishHashCheck(outcome)
            }
        ) else {
            presentBusyError()
            return
        }
        begin(.hashCheck)
    }

    @objc private func confirmDelete(_ sender: Any?) {
        guard operation == nil, let result = scanResult else { return }
        let targets = selectedFiles
        let thumbnailCount = includesThumbnails
            ? result.orphanThumbnailCount
            : 0
        let count = targets.count + thumbnailCount
        guard count > 0 else { return }
        guard let context = maintenanceContext() else { return }

        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized: "storage.detail.leftover.confirm.title"
        )
        alert.informativeText = String.localizedStringWithFormat(
            String(
                localized:
                    "storage.detail.leftover.confirm.message"
            ),
            count
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
        guard alert.runModal() == .alertFirstButtonReturn else {
            return
        }
        guard context.isCurrent(appSession.snapshot) else {
            presentConnectionError()
            view.window?.performClose(nil)
            return
        }
        let includeThumbnails = includesThumbnails
        guard controller.startDeleteLeftover(
            profile: context.profile,
            password: context.credential,
            targets: targets,
            includeThumbnails: includeThumbnails,
            onComplete: { [weak self] outcome in
                self?.finishDelete(outcome)
            }
        ) else {
            presentBusyError()
            return
        }
        begin(.delete)
    }

    @objc private func stopOperation(_ sender: Any?) {
        guard operation != nil else { return }
        statusLabel.stringValue = String(
            localized: "backup.session.stopping",
            defaultValue: "Stopping safely…"
        )
        stopButton.isEnabled = false
        controller.cancel()
    }

    @objc private func toggleSelectAll(_ sender: Any?) {
        if selectedPaths.count == files.count {
            selectedPaths.removeAll()
        } else {
            selectedPaths = Set(files.map(\.path))
        }
        updateSelectionPresentation()
        tableView.reloadData()
    }

    @objc private func selectionChanged(_ sender: Any?) {
        updateSelectionPresentation()
    }

    @objc private func toggleFile(_ sender: NSButton) {
        guard files.indices.contains(sender.tag) else { return }
        let path = files[sender.tag].path
        if sender.state == .on {
            selectedPaths.insert(path)
        } else {
            selectedPaths.remove(path)
        }
        updateSelectionPresentation()
    }

    private func begin(_ operation: Operation) {
        self.operation = operation
        stopButton.isEnabled = true
        onBusyChanged?(true)
        render()
    }

    private func finishOperation(
        message: String,
        cancelledDelete: Bool = false
    ) {
        let terminalAction = MacRepositoryMaintenanceClosePolicy.terminalAction(
            closeRequested: closeWhenOperationFinishes,
            cancelledDelete: cancelledDelete
        )
        closeWhenOperationFinishes = false
        operation = nil
        stopButton.isEnabled = true
        emptyLabel.stringValue = message
        onBusyChanged?(false)

        let sessionIsCurrent =
            MacRepositoryMaintenanceSessionPolicy.matches(
                representedProfile: profile,
                representedGeneration: sessionGeneration,
                current: appSession.snapshot
            )
        if terminalAction == .close || !sessionIsCurrent {
            view.window?.performClose(nil)
            return
        }

        refreshOverview()
        render()

        switch terminalAction {
        case .stay:
            break
        case .close:
            break
        case .rescan:
            startScan(nil)
        }
    }

    private func finishScan(_ outcome: LeftoverScanOutcome) {
        switch outcome {
        case .completed(let result):
            scanResult = result
            selectedPaths.removeAll()
            hashStatusByPath.removeAll()
            thumbnailCheckbox.state = .off
            finishOperation(
                message: result.hasAnythingToClean
                    ? ""
                    : String(
                        localized: "storage.detail.leftover.empty.message",
                        defaultValue: "No leftover files were found in the months this app manages."
                    )
            )
        case .cancelled:
            finishOperation(message: "")
        case .failed(let message):
            finishOperation(message: message)
        }
    }

    private func finishHashCheck(_ outcome: LeftoverHashCheckOutcome) {
        switch outcome {
        case .completed(let result):
            hashStatusByPath.merge(result.statusByPath) { _, new in new }
            finishOperation(message: "")
        case .cancelled:
            finishOperation(message: "")
        case .failed(let message):
            finishOperation(message: message)
        }
    }

    private func finishDelete(_ outcome: LeftoverDeleteOutcome) {
        switch outcome {
        case .completed(let result):
            scanResult = nil
            selectedPaths.removeAll()
            hashStatusByPath.removeAll()
            finishOperation(message: deleteSummaryText(result))
            onRepositoryChanged?()
        case .cancelled:
            scanResult = nil
            selectedPaths.removeAll()
            hashStatusByPath.removeAll()
            finishOperation(message: "", cancelledDelete: true)
        case .failed(let message):
            scanResult = nil
            selectedPaths.removeAll()
            hashStatusByPath.removeAll()
            finishOperation(message: message)
        }
    }

    private func deleteSummaryText(
        _ result: LeftoverDeleteResult
    ) -> String {
        var parts: [String] = []
        let hadDataWork =
            result.deletedCount > 0 || result.failedCount > 0
        if hadDataWork || result.deletedThumbnailCount == 0 {
            if result.failedCount > 0 {
                parts.append(
                    String.localizedStringWithFormat(
                        String(
                            localized:
                                "storage.detail.leftover.summary.withFailures"
                        ),
                        result.deletedCount,
                        result.failedCount
                    )
                )
            } else {
                parts.append(
                    String.localizedStringWithFormat(
                        String(
                            localized:
                                "storage.detail.leftover.summary.deleted"
                        ),
                        result.deletedCount
                    )
                )
            }
        }
        if result.deletedThumbnailCount > 0 {
            parts.append(
                String.localizedStringWithFormat(
                    String(
                        localized:
                            "storage.detail.leftover.summary.thumbnails"
                    ),
                    result.deletedThumbnailCount
                )
            )
        }
        return parts.joined(separator: " ")
    }

    private func maintenanceDidChange() {
        if operation == .verify, !controller.isBusy {
            if let error = controller.lastError,
               error.profileID == profile.id {
                controller.dismissLastError()
                finishOperation(message: error.message)
            } else {
                finishOperation(message: "")
            }
            onRepositoryChanged?()
            return
        }
        if operation != nil {
            renderProgress()
        }
    }

    private func assessment(for file: LeftoverFile) -> String {
        if let status = hashStatusByPath[file.path] {
            switch status {
            case .matched(_, let resources):
                return String(
                    format: String(
                        localized:
                            "mac.maintenance.assessment.matched"
                    ),
                    resources.count
                )
            case .noMatch:
                return String(
                    localized:
                        "storage.detail.leftover.hash.noMatch"
                )
            case .failed:
                return String(
                    localized:
                        "storage.detail.leftover.hash.failed"
                )
            }
        }
        if let probable = scanResult?.probableMatchesByPath[file.path] {
            return String(
                format: String(
                    localized:
                        "mac.maintenance.assessment.possible"
                ),
                probable.totalCount
            )
        }
        return ""
    }

    private func presentBusyError() {
        presentError(
            String(
                localized: "home.alert.maintenanceInProgress",
                defaultValue: "Another backup or maintenance operation is already in progress."
            )
        )
    }

    private func presentError(_ message: String) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "common.error",
            defaultValue: "Unable to Continue"
        )
        alert.informativeText = message
        alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        alert.runModal()
    }
}

extension MacRepositoryMaintenanceViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        files.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard files.indices.contains(row), let tableColumn else { return nil }
        let file = files[row]
        if tableColumn.identifier.rawValue == "selected" {
            let checkbox = NSButton(checkboxWithTitle: "", target: self, action: #selector(toggleFile(_:)))
            checkbox.tag = row
            checkbox.state = selectedPaths.contains(file.path) ? .on : .off
            checkbox.alignment = .center
            return checkbox
        }

        let identifier = NSUserInterfaceItemIdentifier(
            "maintenance.\(tableColumn.identifier.rawValue)"
        )
        let cell = tableView.makeView(
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
            field.lineBreakMode = .byTruncatingMiddle
            cell.addSubview(field)
            cell.textField = field
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

        switch tableColumn.identifier.rawValue {
        case "month":
            field.stringValue = file.month.displayText
        case "file":
            field.stringValue = file.fileName
            field.toolTip = file.path
        case "size":
            field.stringValue = ByteCountFormatter.string(
                fromByteCount: file.size,
                countStyle: .file
            )
        case "status":
            field.stringValue = assessment(for: file)
            field.textColor = hashStatusByPath[file.path] == .failed
                ? .wmMaterialError
                : .secondaryLabelColor
        default:
            field.stringValue = ""
        }
        return cell
    }
}

extension MacRepositoryMaintenanceViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard operation != nil else { return true }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mac.maintenance.title",
            defaultValue: "Repository Maintenance"
        )
        alert.informativeText = String(
            localized: "mac.maintenance.busyMessage",
            defaultValue: "Stop the operation safely before closing this window."
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
            isTaskRunning: operation != nil
        ) {
        case .keepOpen:
            return false
        case .close:
            return true
        case .stopThenClose:
            closeWhenOperationFinishes = true
            stopOperation(nil)
            return false
        }
    }
}
