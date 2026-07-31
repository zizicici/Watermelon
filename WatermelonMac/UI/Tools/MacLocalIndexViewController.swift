import AppKit
import Photos

@MainActor
final class MacLocalIndexViewController: NSViewController {
    var onIndexChanged: (() -> Void)?

    private struct ScopeSnapshot {
        let total: Int
        let indexed: Int
        let totalSizeBytes: Int64
        let lastUpdatedAt: Date?
    }

    private let coordinator: LocalIndexBuildCoordinator
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    private let appRuntimeFlags: AppRuntimeFlags
    private let isExecutionActive: @MainActor () -> Bool

    private let indexedValueLabel = NSTextField(labelWithString: "—")
    private let sizeValueLabel = NSTextField(labelWithString: "—")
    private let updatedValueLabel = NSTextField(labelWithString: "—")
    private let progressIndicator = NSProgressIndicator()
    private let progressLabel = NSTextField(labelWithString: "")
    private let progressStack = NSStackView()
    private let incrementalButton = NSButton()
    private let rebuildButton = NSButton()
    private let stopButton = NSButton()
    private let accessButton = NSButton()

    private var indexedCount = 0
    private var hasLoadedStats = false
    private var photoLibraryAccessState: PhotoLibraryAccessState = .unknown
    private var lastObservedRunning = false
    private var ownsExecutionLease = false
    private var closeWhenFinished = false
    private var coordinatorObserverID: UUID?
    private var statsTask: Task<Void, Never>?
    private var accessTask: Task<Void, Never>?
    nonisolated(unsafe) private var activationObserver: NSObjectProtocol?
    nonisolated(unsafe) private var executionLifecycleObserver:
        NSObjectProtocol?

    init(
        coordinator: LocalIndexBuildCoordinator,
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        appRuntimeFlags: AppRuntimeFlags,
        isExecutionActive: @escaping @MainActor () -> Bool
    ) {
        self.coordinator = coordinator
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.appRuntimeFlags = appRuntimeFlags
        self.isExecutionActive = isExecutionActive
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        statsTask?.cancel()
        accessTask?.cancel()
        if let activationObserver {
            NotificationCenter.default.removeObserver(activationObserver)
        }
        if let executionLifecycleObserver {
            NotificationCenter.default.removeObserver(
                executionLifecycleObserver
            )
        }
        let coordinator = coordinator
        let observerID = coordinatorObserverID
        Task { @MainActor in
            if let observerID {
                coordinator.removeObserver(observerID)
            }
        }
        if ownsExecutionLease {
            appRuntimeFlags.exitExecution()
        }
    }

    override func loadView() {
        let root = NSView()
        view = root

        configureValueLabel(indexedValueLabel)
        configureValueLabel(sizeValueLabel)
        configureValueLabel(updatedValueLabel)
        let stats = NSGridView(views: [
            [
                makeStat(
                    title: String(
                        localized: "home.localIndex.indexedCount",
                        defaultValue: "Indexed"
                    ),
                    valueLabel: indexedValueLabel
                ),
                makeStat(
                    title: String(
                        localized: "home.localIndex.totalSize"
                    ),
                    valueLabel: sizeValueLabel
                ),
                makeStat(
                    title: String(
                        localized: "home.localIndex.lastUpdated",
                        defaultValue: "Last Updated"
                    ),
                    valueLabel: updatedValueLabel
                ),
            ],
        ])
        stats.columnSpacing = 16
        for index in 0..<3 {
            stats.column(at: index).width = 150
        }
        let statsCard = makeCard(stats, fill: .wmMaterialPrimarySurface)

        progressIndicator.minValue = 0
        progressIndicator.maxValue = 1
        progressIndicator.isIndeterminate = false
        progressIndicator.controlSize = .small
        progressLabel.font = .systemFont(ofSize: 12.5)
        progressLabel.textColor = .secondaryLabelColor
        progressLabel.lineBreakMode = .byTruncatingMiddle
        progressStack.setViews(
            [progressIndicator, progressLabel],
            in: .leading
        )
        progressStack.orientation = .vertical
        progressStack.alignment = .leading
        progressStack.spacing = 6
        progressStack.isHidden = true

        configureButton(
            incrementalButton,
            title: String(
                localized: "home.localIndex.incrementalTitle",
                defaultValue: "Update Index"
            ),
            symbol: "arrow.clockwise",
            action: #selector(updateIndex(_:))
        )
        configureButton(
            rebuildButton,
            title: String(
                localized: "home.localIndex.rebuildTitle",
                defaultValue: "Rebuild Index…"
            ),
            symbol: "arrow.triangle.2.circlepath",
            action: #selector(rebuildIndex(_:))
        )
        configureButton(
            stopButton,
            title: String(
                localized: "common.stop",
                defaultValue: "Stop"
            ),
            symbol: "stop.fill",
            action: #selector(stopIndex(_:))
        )
        stopButton.contentTintColor = .wmMaterialError
        stopButton.isHidden = true
        configureButton(
            accessButton,
            title: "",
            symbol: "photo.on.rectangle",
            action: #selector(handlePhotoAccess(_:))
        )
        accessButton.isHidden = true

        let buttons = NSStackView(
            views: [
                incrementalButton,
                rebuildButton,
                stopButton,
                accessButton,
            ]
        )
        buttons.orientation = .horizontal
        buttons.alignment = .centerY
        buttons.spacing = 10

        let content = NSStackView(
            views: [
                statsCard,
                progressStack,
                buttons,
            ]
        )
        content.orientation = .vertical
        content.alignment = .leading
        content.spacing = 18
        content.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(content)

        NSLayoutConstraint.activate([
            statsCard.widthAnchor.constraint(equalTo: content.widthAnchor),
            progressStack.widthAnchor.constraint(
                equalTo: content.widthAnchor
            ),
            progressIndicator.widthAnchor.constraint(
                equalTo: progressStack.widthAnchor
            ),
            content.leadingAnchor.constraint(
                equalTo: root.leadingAnchor,
                constant: 28
            ),
            content.trailingAnchor.constraint(
                equalTo: root.trailingAnchor,
                constant: -28
            ),
            content.topAnchor.constraint(
                equalTo: root.topAnchor,
                constant: 28
            ),
            content.bottomAnchor.constraint(
                lessThanOrEqualTo: root.bottomAnchor,
                constant: -26
            ),
        ])
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
        if coordinatorObserverID == nil {
            lastObservedRunning = coordinator.isRunning
            coordinatorObserverID = coordinator.addObserver {
                [weak self] in
                self?.applyCoordinatorState()
            }
        }
        if activationObserver == nil {
            activationObserver = NotificationCenter.default.addObserver(
                forName: NSApplication.didBecomeActiveNotification,
                object: nil,
                queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.refreshPhotoLibraryAccess()
                }
            }
        }
        if executionLifecycleObserver == nil {
            executionLifecycleObserver = NotificationCenter.default
                .addObserver(
                    forName: .ExecutionLifecycleDidChange,
                    object: nil,
                    queue: .main
                ) { [weak self] _ in
                    MainActor.assumeIsolated {
                        self?.updateActionAvailability()
                    }
                }
        }
        refreshPhotoLibraryAccess(reloadIfReadable: true)
        applyCoordinatorState()
    }

    private func configureValueLabel(_ label: NSTextField) {
        label.font = .systemFont(ofSize: 17, weight: .semibold)
        label.textColor = .wmMaterialOnPrimaryContainer
        label.lineBreakMode = .byTruncatingTail
    }

    private func makeStat(
        title: String,
        valueLabel: NSTextField
    ) -> NSView {
        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = .systemFont(ofSize: 12)
        titleLabel.textColor = .wmMaterialPrimaryDetail
        let stack = NSStackView(views: [titleLabel, valueLabel])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 5
        return stack
    }

    private func makeCard(
        _ content: NSView,
        fill: NSColor
    ) -> NSView {
        let box = NSBox()
        box.boxType = .custom
        box.borderWidth = 1
        box.borderColor = NSColor.wmMaterialPrimaryDetail
            .withAlphaComponent(0.25)
        box.fillColor = fill
        box.cornerRadius = 11
        content.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(content)
        NSLayoutConstraint.activate([
            content.leadingAnchor.constraint(
                equalTo: box.leadingAnchor,
                constant: 18
            ),
            content.trailingAnchor.constraint(
                equalTo: box.trailingAnchor,
                constant: -18
            ),
            content.topAnchor.constraint(
                equalTo: box.topAnchor,
                constant: 16
            ),
            content.bottomAnchor.constraint(
                equalTo: box.bottomAnchor,
                constant: -16
            ),
        ])
        return box
    }

    private func configureButton(
        _ button: NSButton,
        title: String,
        symbol: String,
        action: Selector
    ) {
        button.title = title
        button.image = NSImage(
            systemSymbolName: symbol,
            accessibilityDescription: nil
        )
        button.imagePosition = .imageLeading
        button.bezelStyle = .rounded
        button.controlSize = .large
        button.target = self
        button.action = action
    }

    private func reloadStats() {
        guard photoLibraryAccessState.canReadLibrary else {
            applyUnavailablePhotoLibrary()
            return
        }
        statsTask?.cancel()
        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        statsTask = Task { [weak self] in
            let snapshot = await Self.loadSnapshot(
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            guard !Task.isCancelled else { return }
            self?.apply(snapshot)
        }
    }

    private nonisolated static func loadSnapshot(
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> ScopeSnapshot {
        await withCancellableDetachedValue(
            priority: .userInitiated
        ) {
            let allIDs = photoLibraryService.collectAssetIDs(
                query: .allAssets
            )
            let rows = (
                try? repository.fetchValidIndexedRows(
                    assetIDs: allIDs
                )
            ) ?? [:]
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: Set(rows.keys)
            )
            var indexed = 0
            var totalSizeBytes: Int64 = 0
            var newest: Date?
            for asset in assets {
                guard let row = rows[asset.localIdentifier] else {
                    continue
                }
                if let modified = asset.modificationDate,
                   modified > row.updatedAt {
                    continue
                }
                indexed += 1
                totalSizeBytes += row.totalFileSizeBytes
                newest = max(newest ?? .distantPast, row.updatedAt)
            }
            return ScopeSnapshot(
                total: allIDs.count,
                indexed: indexed,
                totalSizeBytes: totalSizeBytes,
                lastUpdatedAt: newest
            )
        }
    }

    private func apply(_ snapshot: ScopeSnapshot) {
        indexedCount = snapshot.indexed
        hasLoadedStats = true
        indexedValueLabel.stringValue =
            String.localizedStringWithFormat(
                String(
                    localized:
                        "home.localIndex.indexedCountValue"
                ),
                Int64(snapshot.indexed),
                Int64(snapshot.total)
            )
        sizeValueLabel.stringValue = snapshot.totalSizeBytes > 0
            ? Self.byteFormatter.string(
                fromByteCount: snapshot.totalSizeBytes
            )
            : String(localized: "home.localIndex.never")
        if let lastUpdatedAt = snapshot.lastUpdatedAt {
            updatedValueLabel.stringValue = Self.dateFormatter.string(
                from: lastUpdatedAt
            )
        } else {
            updatedValueLabel.stringValue = String(
                localized: "home.localIndex.never",
                defaultValue: "Not yet"
            )
        }
        updateActionAvailability()
    }

    private func applyCoordinatorState() {
        let running = coordinator.isRunning
        let didFinish = lastObservedRunning && !running
        lastObservedRunning = running
        if let state = coordinator.state {
            let total = max(state.totalCount, 1)
            progressIndicator.doubleValue =
                Double(state.processedInRun) / Double(total)
            let verb: String
            switch state.mode {
            case .incremental:
                verb = String(
                    localized: "mac.localIndex.updating",
                    defaultValue: "Updating index"
                )
            case .rebuild:
                verb = String(
                    localized: "mac.localIndex.rebuilding",
                    defaultValue: "Rebuilding index"
                )
            }
            progressLabel.stringValue =
                "\(verb)… \(state.displayedIndexed) / \(state.totalCount)"
        } else {
            progressIndicator.doubleValue = 0
            progressLabel.stringValue = ""
        }
        progressStack.isHidden = !running
        let canReadLibrary = photoLibraryAccessState.canReadLibrary
        incrementalButton.isHidden = running || !canReadLibrary
        rebuildButton.isHidden = running || !canReadLibrary
        stopButton.isHidden = !running
        accessButton.isHidden = running || canReadLibrary
        updateActionAvailability()

        guard didFinish else { return }
        releaseExecutionLease()
        reloadStats()
        onIndexChanged?()
        if let error = coordinator.lastError {
            showError(error)
        }
        if closeWhenFinished {
            closeWhenFinished = false
            view.window?.performClose(nil)
        }
    }

    private func updateActionAvailability() {
        let enabled = !coordinator.isRunning
            && !isExecutionActive()
        incrementalButton.isEnabled =
            enabled && hasLoadedStats
                && photoLibraryAccessState.canReadLibrary
        rebuildButton.isEnabled =
            enabled && photoLibraryAccessState.canReadLibrary
        stopButton.isEnabled = coordinator.isRunning
        accessButton.isEnabled = enabled && accessTask == nil
    }

    private func refreshPhotoLibraryAccess(
        reloadIfReadable: Bool = false
    ) {
        let state = PhotoLibraryAccessState(
            photoLibraryService.authorizationStatus()
        )
        guard state != photoLibraryAccessState
                || reloadIfReadable else {
            return
        }
        photoLibraryAccessState = state
        if state.canReadLibrary {
            if !coordinator.isRunning {
                reloadStats()
            }
        } else {
            applyUnavailablePhotoLibrary()
        }
        applyCoordinatorState()
    }

    private func applyUnavailablePhotoLibrary() {
        statsTask?.cancel()
        statsTask = nil
        hasLoadedStats = false
        indexedValueLabel.stringValue = photoLibraryAccessState
            == .notDetermined
            ? String(
                localized: "home.overlay.authRequired",
                defaultValue: "Photo library access required"
            )
            : String(
                localized: "home.overlay.noAuth",
                defaultValue: "Photo library access denied"
            )
        sizeValueLabel.stringValue = "—"
        updatedValueLabel.stringValue = "—"
        accessButton.title = photoLibraryAccessState == .notDetermined
            ? String(
                localized: "home.overlay.allowAccess",
                defaultValue: "Continue"
            )
            : String(
                localized: "home.overlay.goToSettings",
                defaultValue: "Go to Settings"
            )
    }

    @objc
    private func handlePhotoAccess(_ sender: Any?) {
        guard !photoLibraryAccessState.canReadLibrary else { return }
        if photoLibraryAccessState == .notDetermined {
            guard accessTask == nil else { return }
            accessTask = Task { [weak self] in
                guard let self else { return }
                let state = PhotoLibraryAccessState(
                    await photoLibraryService.requestAuthorization()
                )
                guard !Task.isCancelled else { return }
                accessTask = nil
                photoLibraryAccessState = state
                if state.canReadLibrary {
                    reloadStats()
                } else {
                    applyUnavailablePhotoLibrary()
                }
                applyCoordinatorState()
            }
            updateActionAvailability()
            return
        }
        MacPhotoLibrarySettings.open()
    }

    private func begin(_ mode: LocalIndexBuildCoordinator.Mode) {
        guard photoLibraryAccessState.canReadLibrary else {
            handlePhotoAccess(nil)
            return
        }
        guard !coordinator.isRunning,
              !isExecutionActive(),
              appRuntimeFlags.tryEnterExecution() else {
            showBusyAlert()
            return
        }
        ownsExecutionLease = true
        appRuntimeFlags.setExecutionCancellationHandler(for: self) {
            $0.coordinator.cancel()
        }
        coordinator.start(
            mode: mode,
            initialIndexed: indexedCount
        )
        applyCoordinatorState()
    }

    private func releaseExecutionLease() {
        guard ownsExecutionLease else { return }
        ownsExecutionLease = false
        appRuntimeFlags.exitExecution()
    }

    @objc
    private func updateIndex(_ sender: Any?) {
        begin(.incremental)
    }

    @objc
    private func rebuildIndex(_ sender: Any?) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "home.localIndex.confirmRebuildTitle",
            defaultValue: "Rebuild the local index?"
        )
        alert.informativeText = String(
            localized: "home.localIndex.confirmRebuildMessage",
            defaultValue: "Existing local fingerprints will be removed and rebuilt. Your Photos library and backup files are not changed."
        )
        alert.addButton(
            withTitle: String(
                localized: "home.localIndex.confirmRebuildAction",
                defaultValue: "Rebuild"
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
        begin(.rebuild)
    }

    @objc
    private func stopIndex(_ sender: Any?) {
        coordinator.cancel()
    }

    private func showBusyAlert() {
        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = String(
            localized: "common.error"
        )
        alert.informativeText = String(
            localized: "mediaBrowser.action.taskInProgress"
        )
        alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        alert.runModal()
    }

    private func showError(_ error: Error) {
        let alert = NSAlert(error: error)
        alert.messageText = String(
            localized: "home.localIndex.error.title",
            defaultValue: "Local index could not finish"
        )
        alert.runModal()
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()

    private static let byteFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        return formatter
    }()
}

extension MacLocalIndexViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard coordinator.isRunning else { return true }
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = String(
            localized: "mac.localIndex.closeTitle",
            defaultValue: "Indexing is still running"
        )
        alert.informativeText = String(
            localized: "mac.localIndex.closeMessage",
            defaultValue: "Stop the index update safely before closing this window."
        )
        alert.addButton(
            withTitle: String(
                localized: "mac.localIndex.stopAndClose",
                defaultValue: "Stop and Close"
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
            isTaskRunning: coordinator.isRunning
        ) {
        case .keepOpen:
            return false
        case .close:
            return true
        case .stopThenClose:
            closeWhenFinished = true
            coordinator.cancel()
            return false
        }
    }
}
