import AppKit
import Photos

@MainActor
final class MacDuplicatesViewController: NSViewController {
    var onOpenLocalIndex: (() -> Void)?
    var onLibraryChanged: (() -> Void)?

    fileprivate struct DuplicateEntry: Sendable {
        let assetLocalIdentifier: String
        let creationDate: Date?
        let mediaType: PHAssetMediaType
    }

    private struct DuplicateGroup: Sendable {
        let fingerprint: Data
        let entries: [DuplicateEntry]
    }

    private struct DuplicatesData: Sendable {
        let scopeTotal: Int
        let scopeIndexed: Int
        let groups: [DuplicateGroup]
    }

    private struct KeepDeletePair: Sendable {
        let keep: String
        let delete: String
    }

    private enum Row {
        case group(Int)
        case entry(group: Int, entry: Int)
    }

    private let coordinator: LocalIndexBuildCoordinator
    private let hashIndexRepository: ContentHashIndexRepository
    private let photoLibraryService: PhotoLibraryService
    private let changePublisher: LocalIndexChangePublisher
    private let appRuntimeFlags: AppRuntimeFlags
    private let isExecutionActive: @MainActor () -> Bool

    private let summaryLabel = NSTextField(labelWithString: "")
    private let gateBox = NSBox()
    private let gateDetailLabel = NSTextField(
        wrappingLabelWithString: ""
    )
    private let gateButton = NSButton()
    private let scrollView = NSScrollView()
    private let tableView = NSTableView()
    private let emptyView = NSStackView()
    private let progressIndicator = NSProgressIndicator()
    private let deleteButton = NSButton()
    private var gateHeightConstraint: NSLayoutConstraint!

    private var groups: [DuplicateGroup] = []
    private var rows: [Row] = []
    private var processedGroups = Set<Int>()
    private var keepIndexByGroup: [Int: Int] = [:]
    private var scopeTotal = 0
    private var scopeIndexed = 0
    private var isLoading = false
    private var isDeleting = false
    private var executionClaim: AppRuntimeFlags.ExecutionClaim?
    private var photoLibraryAccessState: PhotoLibraryAccessState = .unknown
    private var loadTask: Task<Void, Never>?
    private var deleteTask: Task<Void, Never>?
    private var accessTask: Task<Void, Never>?
    private var changeObserverID: UUID?
    private var coordinatorObserverID: UUID?
    nonisolated(unsafe) private var activationObserver: NSObjectProtocol?
    nonisolated(unsafe) private var executionLifecycleObserver:
        NSObjectProtocol?
    private var lastObservedRunning = false

    init(
        coordinator: LocalIndexBuildCoordinator,
        hashIndexRepository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService,
        changePublisher: LocalIndexChangePublisher,
        appRuntimeFlags: AppRuntimeFlags,
        isExecutionActive: @escaping @MainActor () -> Bool
    ) {
        self.coordinator = coordinator
        self.hashIndexRepository = hashIndexRepository
        self.photoLibraryService = photoLibraryService
        self.changePublisher = changePublisher
        self.appRuntimeFlags = appRuntimeFlags
        self.isExecutionActive = isExecutionActive
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        deleteTask?.cancel()
        accessTask?.cancel()
        if let activationObserver {
            NotificationCenter.default.removeObserver(activationObserver)
        }
        if let executionLifecycleObserver {
            NotificationCenter.default.removeObserver(
                executionLifecycleObserver
            )
        }
        let publisher = changePublisher
        let observerID = changeObserverID
        if let observerID {
            publisher.removeObserver(observerID)
        }
        let coordinator = coordinator
        let coordinatorObserverID = coordinatorObserverID
        Task { @MainActor in
            if let coordinatorObserverID {
                coordinator.removeObserver(coordinatorObserverID)
            }
        }
        if let executionClaim {
            appRuntimeFlags.exitExecution(executionClaim)
        }
    }

    override func loadView() {
        let root = NSView()
        view = root

        summaryLabel.font = .systemFont(ofSize: 13)
        summaryLabel.textColor = .secondaryLabelColor
        summaryLabel.maximumNumberOfLines = 2

        configureGate()
        configureTable()
        configureEmptyView()

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .small
        progressIndicator.isDisplayedWhenStopped = false

        deleteButton.title = String(
            localized: "home.duplicates.execute",
            defaultValue: "Delete Duplicates…"
        )
        deleteButton.image = NSImage(
            systemSymbolName: "trash",
            accessibilityDescription: nil
        )
        deleteButton.imagePosition = .imageLeading
        deleteButton.bezelStyle = .rounded
        deleteButton.controlSize = .large
        deleteButton.contentTintColor = .wmMaterialError
        deleteButton.target = self
        deleteButton.action = #selector(deleteDuplicates(_:))

        let footerSpacer = NSView()
        let footer = NSStackView(
            views: [
                progressIndicator,
                footerSpacer,
                deleteButton,
            ]
        )
        footer.orientation = .horizontal
        footer.alignment = .centerY
        footer.spacing = 10

        summaryLabel.translatesAutoresizingMaskIntoConstraints = false
        gateBox.translatesAutoresizingMaskIntoConstraints = false
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        emptyView.translatesAutoresizingMaskIntoConstraints = false
        footer.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(summaryLabel)
        root.addSubview(gateBox)
        root.addSubview(scrollView)
        root.addSubview(emptyView)
        root.addSubview(footer)

        gateHeightConstraint = gateBox.heightAnchor.constraint(
            equalToConstant: 0
        )
        NSLayoutConstraint.activate([
            summaryLabel.leadingAnchor.constraint(
                equalTo: root.leadingAnchor,
                constant: 26
            ),
            summaryLabel.trailingAnchor.constraint(
                equalTo: root.trailingAnchor,
                constant: -26
            ),
            summaryLabel.topAnchor.constraint(
                equalTo: root.topAnchor,
                constant: 24
            ),
            gateBox.leadingAnchor.constraint(
                equalTo: summaryLabel.leadingAnchor
            ),
            gateBox.trailingAnchor.constraint(
                equalTo: summaryLabel.trailingAnchor
            ),
            gateBox.topAnchor.constraint(
                equalTo: summaryLabel.bottomAnchor,
                constant: 12
            ),
            gateHeightConstraint,
            scrollView.leadingAnchor.constraint(
                equalTo: root.leadingAnchor
            ),
            scrollView.trailingAnchor.constraint(
                equalTo: root.trailingAnchor
            ),
            scrollView.topAnchor.constraint(
                equalTo: gateBox.bottomAnchor,
                constant: 14
            ),
            scrollView.bottomAnchor.constraint(
                equalTo: footer.topAnchor,
                constant: -12
            ),
            emptyView.centerXAnchor.constraint(
                equalTo: scrollView.centerXAnchor
            ),
            emptyView.centerYAnchor.constraint(
                equalTo: scrollView.centerYAnchor
            ),
            emptyView.widthAnchor.constraint(
                lessThanOrEqualToConstant: 420
            ),
            footer.leadingAnchor.constraint(
                equalTo: summaryLabel.leadingAnchor
            ),
            footer.trailingAnchor.constraint(
                equalTo: summaryLabel.trailingAnchor
            ),
            footer.bottomAnchor.constraint(
                equalTo: root.bottomAnchor,
                constant: -18
            ),
        ])
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.delegate = self
        if changeObserverID == nil {
            changeObserverID = changePublisher.addObserver {
                [weak self] _ in
                Task { @MainActor [weak self] in
                    self?.loadDuplicates()
                }
            }
        }
        if coordinatorObserverID == nil {
            lastObservedRunning = coordinator.isRunning
            coordinatorObserverID = coordinator.addObserver {
                [weak self] in
                self?.handleCoordinatorChange()
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
                        self?.updatePresentation()
                    }
                }
        }
        refreshPhotoLibraryAccess(reloadIfReadable: true)
    }

    private func handleCoordinatorChange() {
        let isRunning = coordinator.isRunning
        let didFinish = lastObservedRunning && !isRunning
        lastObservedRunning = isRunning
        if didFinish {
            loadDuplicates()
        } else {
            updatePresentation()
        }
    }

    private func configureGate() {
        gateBox.boxType = .custom
        gateBox.borderWidth = 1
        gateBox.cornerRadius = 10
        gateBox.fillColor = .wmMaterialWarningContainer
        gateBox.borderColor = NSColor.wmMaterialWarningDetail
            .withAlphaComponent(0.4)

        let gateIcon = NSImageView(
            image: NSImage(
                systemSymbolName: "exclamationmark.triangle.fill",
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        gateIcon.contentTintColor = .wmMaterialOnWarningContainer
        gateIcon.translatesAutoresizingMaskIntoConstraints = false
        gateDetailLabel.font = .systemFont(ofSize: 12.5)
        gateDetailLabel.textColor = .wmMaterialOnWarningContainer

        gateButton.title = String(
            localized: "home.duplicates.gateTitle",
            defaultValue: "Complete Local Index…"
        )
        gateButton.bezelStyle = .rounded
        gateButton.target = self
        gateButton.action = #selector(handleGateAction(_:))

        let labelsAndButton = NSStackView(
            views: [gateDetailLabel, gateButton]
        )
        labelsAndButton.orientation = .vertical
        labelsAndButton.alignment = .leading
        labelsAndButton.spacing = 9
        let content = NSStackView(
            views: [gateIcon, labelsAndButton]
        )
        content.orientation = .horizontal
        content.alignment = .top
        content.spacing = 12
        content.translatesAutoresizingMaskIntoConstraints = false
        gateBox.addSubview(content)
        NSLayoutConstraint.activate([
            gateIcon.widthAnchor.constraint(equalToConstant: 24),
            gateIcon.heightAnchor.constraint(equalToConstant: 24),
            content.leadingAnchor.constraint(
                equalTo: gateBox.leadingAnchor,
                constant: 14
            ),
            content.trailingAnchor.constraint(
                equalTo: gateBox.trailingAnchor,
                constant: -14
            ),
            content.topAnchor.constraint(
                equalTo: gateBox.topAnchor,
                constant: 13
            ),
            content.bottomAnchor.constraint(
                equalTo: gateBox.bottomAnchor,
                constant: -13
            ),
        ])
    }

    private func configureTable() {
        let column = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier(
                "duplicates"
            )
        )
        column.resizingMask = .autoresizingMask
        tableView.addTableColumn(column)
        tableView.headerView = nil
        tableView.rowSizeStyle = .custom
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.selectionHighlightStyle = .regular
        tableView.intercellSpacing = NSSize(width: 0, height: 1)
        tableView.delegate = self
        tableView.dataSource = self

        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
    }

    private func configureEmptyView() {
        let icon = NSImageView(
            image: NSImage(
                systemSymbolName: "checkmark.seal",
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        icon.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 34,
            weight: .regular
        )
        icon.contentTintColor = .wmMaterialPrimary
        let title = NSTextField(
            labelWithString: String(
                localized: "home.duplicates.emptyTitle",
                defaultValue: "No duplicates found"
            )
        )
        title.font = .systemFont(ofSize: 17, weight: .semibold)
        emptyView.addArrangedSubview(icon)
        emptyView.addArrangedSubview(title)
        emptyView.orientation = .vertical
        emptyView.alignment = .centerX
        emptyView.spacing = 8
        emptyView.isHidden = true
    }

    private func loadDuplicates() {
        guard !isDeleting else { return }
        guard photoLibraryAccessState.canReadLibrary else {
            applyUnavailablePhotoLibrary()
            return
        }
        loadTask?.cancel()
        isLoading = true
        progressIndicator.startAnimation(nil)
        updatePresentation()

        #if DEBUG
        if ProcessInfo.processInfo.arguments.contains(
            "--demo-duplicates"
        ) {
            DispatchQueue.main.async { [weak self] in
                self?.apply(Self.demoData())
            }
            return
        }
        #endif

        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        loadTask = Task { [weak self] in
            let data = await Self.computeData(
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            guard !Task.isCancelled else { return }
            self?.apply(data)
        }
    }

    private func apply(_ data: DuplicatesData) {
        isLoading = false
        progressIndicator.stopAnimation(nil)
        scopeTotal = data.scopeTotal
        scopeIndexed = data.scopeIndexed
        groups = data.groups
        processedGroups = Set(groups.indices)
        keepIndexByGroup = Dictionary(
            uniqueKeysWithValues: groups.indices.map { ($0, 0) }
        )
        rebuildRows()
        updatePresentation()
    }

    private func rebuildRows() {
        rows.removeAll(keepingCapacity: true)
        for groupIndex in groups.indices {
            rows.append(.group(groupIndex))
            for entryIndex in groups[groupIndex].entries.indices {
                rows.append(
                    .entry(
                        group: groupIndex,
                        entry: entryIndex
                    )
                )
            }
        }
        tableView.reloadData()
    }

    private func updatePresentation() {
        let isBuilding = coordinator.isRunning
        let executionActive = isExecutionActive()
        let needsPhotoAccess =
            !photoLibraryAccessState.canReadLibrary
        let incomplete = scopeIndexed < scopeTotal
        let showsGate = needsPhotoAccess || isBuilding || incomplete
        gateBox.isHidden = !showsGate
        gateHeightConstraint.constant = showsGate ? 92 : 0
        if needsPhotoAccess {
            gateDetailLabel.stringValue = photoLibraryAccessState
                == .notDetermined
                ? String(
                    localized: "home.overlay.authRequired",
                    defaultValue: "Photo library access required"
                )
                : String(
                    localized: "home.overlay.noAuth",
                    defaultValue: "Photo library access denied"
                )
            gateButton.title = photoLibraryAccessState == .notDetermined
                ? String(
                    localized: "home.overlay.allowAccess",
                    defaultValue: "Continue"
                )
                : String(
                    localized: "home.overlay.goToSettings",
                    defaultValue: "Go to Settings"
                )
        } else {
            gateDetailLabel.stringValue = String(
                localized: "home.duplicates.gateExplanation"
            )
            gateButton.title = String(
                localized: "home.duplicates.gateTitle",
                defaultValue: "Complete Local Index…"
            )
        }
        gateButton.isEnabled = !isDeleting
            && accessTask == nil
            && (needsPhotoAccess || !executionActive)

        if needsPhotoAccess {
            summaryLabel.stringValue = ""
        } else if isBuilding {
            summaryLabel.stringValue = String(
                localized: "home.duplicates.summaryBuilding"
            )
        } else if isLoading {
            summaryLabel.stringValue = String(
                localized: "home.duplicates.loading",
                defaultValue: "Checking local fingerprints…"
            )
        } else if groups.isEmpty {
            summaryLabel.stringValue = ""
        } else {
            summaryLabel.stringValue = String.localizedStringWithFormat(
                String(
                    localized: "home.duplicates.summary",
                    defaultValue: "%lld items will be deleted."
                ),
                Int64(deletionCount())
            )
        }
        scrollView.isHidden =
            needsPhotoAccess || groups.isEmpty || isLoading || isBuilding
        emptyView.isHidden =
            needsPhotoAccess || !groups.isEmpty || isLoading || showsGate
        deleteButton.isEnabled = !isLoading
            && !isDeleting
            && deletionCount() > 0
            && !coordinator.isRunning
            && !executionActive
        deleteButton.isHidden = groups.isEmpty || isLoading
        deleteButton.title = String(
            localized: "common.delete",
            defaultValue: "Delete"
        ) + "…"
    }

    private func deletionCount() -> Int {
        processedGroups.reduce(into: 0) { count, index in
            guard groups.indices.contains(index) else { return }
            count += max(groups[index].entries.count - 1, 0)
        }
    }

    private func pairsForDeletion() -> [KeepDeletePair] {
        var pairs: [KeepDeletePair] = []
        for groupIndex in processedGroups.sorted() {
            guard groups.indices.contains(groupIndex) else { continue }
            let entries = groups[groupIndex].entries
            let keepIndex = keepIndexByGroup[groupIndex] ?? 0
            guard entries.indices.contains(keepIndex) else { continue }
            let keep = entries[keepIndex].assetLocalIdentifier
            for index in entries.indices where index != keepIndex {
                pairs.append(
                    KeepDeletePair(
                        keep: keep,
                        delete: entries[index]
                            .assetLocalIdentifier
                    )
                )
            }
        }
        return pairs
    }

    @objc
    private func toggleGroup(_ sender: NSButton) {
        let index = sender.tag
        if sender.state == .on {
            processedGroups.insert(index)
        } else {
            processedGroups.remove(index)
        }
        reloadRows(forGroup: index)
        updatePresentation()
    }

    @objc
    private func keepEntry(_ sender: NSButton) {
        guard rows.indices.contains(sender.tag),
              case .entry(let group, let entry) = rows[sender.tag],
              processedGroups.contains(group) else {
            return
        }
        keepIndexByGroup[group] = entry
        reloadRows(forGroup: group)
        updatePresentation()
    }

    private func reloadRows(forGroup group: Int) {
        let rowIndexes = IndexSet(
            rows.indices.filter { index in
                switch rows[index] {
                case .group(let candidate):
                    return candidate == group
                case .entry(let candidate, _):
                    return candidate == group
                }
            }
        )
        tableView.reloadData(
            forRowIndexes: rowIndexes,
            columnIndexes: IndexSet(integer: 0)
        )
    }

    @objc
    private func handleGateAction(_ sender: Any?) {
        if photoLibraryAccessState.canReadLibrary {
            onOpenLocalIndex?()
            return
        }
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
                    loadDuplicates()
                } else {
                    applyUnavailablePhotoLibrary()
                }
            }
            updatePresentation()
            return
        }
        MacPhotoLibrarySettings.open()
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
            loadDuplicates()
        } else {
            applyUnavailablePhotoLibrary()
        }
    }

    private func applyUnavailablePhotoLibrary() {
        loadTask?.cancel()
        loadTask = nil
        isLoading = false
        progressIndicator.stopAnimation(nil)
        scopeTotal = 0
        scopeIndexed = 0
        groups = []
        processedGroups = []
        keepIndexByGroup = [:]
        rebuildRows()
        updatePresentation()
    }

    @objc
    private func deleteDuplicates(_ sender: Any?) {
        let pairs = pairsForDeletion()
        guard !pairs.isEmpty else { return }
        guard !isExecutionActive() else {
            showBusyAlert()
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = String(
            localized: "home.duplicates.confirmTitle",
            defaultValue: "Delete selected duplicates from Photos?"
        )
        alert.informativeText = String.localizedStringWithFormat(
            String(
                localized: "home.duplicates.confirmMessage",
                defaultValue: "%lld items will be moved to Recently Deleted."
            ),
            Int64(pairs.count)
        )
        alert.addButton(
            withTitle: String(
                localized: "home.duplicates.execute",
                defaultValue: "Delete Duplicates"
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
        guard !isExecutionActive(),
              let executionClaim =
                appRuntimeFlags.tryEnterExecution() else {
            showBusyAlert()
            return
        }
        self.executionClaim = executionClaim
        appRuntimeFlags.setExecutionCancellationHandler(
            for: self,
            claim: executionClaim
        ) {
            $0.deleteTask?.cancel()
        }
        isDeleting = true
        progressIndicator.startAnimation(nil)
        updatePresentation()

        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        let publisher = changePublisher
        deleteTask = Task { [weak self] in
            guard let self else { return }
            let stillValid = await Self.revalidate(
                pairs: pairs,
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            switch MacPhotoLibraryDeletionPreparationPolicy.disposition(
                isCancelled: Task.isCancelled,
                isStillValid: stillValid
            ) {
            case .cancel:
                self.finishDeletion(
                    result: .failure(CancellationError())
                )
                return
            case .stale:
                self.finishDeletion(
                    result: .failure(
                        MacDuplicateError.staleIndex
                    )
                )
                return
            case .proceed:
                break
            }
            let ids = pairs.map(\.delete)
            let deleted: Bool
            do {
                deleted = try await Self.deleteAssets(
                    photoLibraryService: photoLibraryService,
                    assetLocalIdentifiers: ids
                )
            } catch is CancellationError {
                self.finishDeletion(
                    result: .failure(CancellationError())
                )
                return
            } catch {
                self.finishDeletion(result: .failure(error))
                return
            }
            guard deleted else {
                self.finishDeletion(
                    result: .failure(
                        MacDuplicateError.photosDeleteFailed
                    )
                )
                return
            }
            // A committed Photos change still needs matching index cleanup.
            await withCancellableDetachedValue(
                priority: .userInitiated
            ) {
                try? repository.deleteIndexEntries(assetIDs: ids)
            }
            publisher.publish(.touched(assetIDs: Set(ids)))
            self.finishDeletion(result: .success(()))
        }
    }

    private func finishDeletion(
        result: Result<Void, Error>
    ) {
        isDeleting = false
        deleteTask = nil
        if let executionClaim {
            self.executionClaim = nil
            appRuntimeFlags.exitExecution(executionClaim)
        }
        progressIndicator.stopAnimation(nil)
        switch result {
        case .success:
            onLibraryChanged?()
            loadDuplicates()
        case .failure(let error):
            guard !(error is CancellationError) else { return }
            let alert = NSAlert(error: error)
            alert.runModal()
            loadDuplicates()
        }
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

    private nonisolated static func computeData(
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> DuplicatesData {
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
            var fingerprintsByID: [String: Data] = [:]
            var assetsByID: [String: PHAsset] = [:]
            for asset in assets {
                guard let row = rows[asset.localIdentifier] else {
                    continue
                }
                if let modified = asset.modificationDate,
                   modified > row.updatedAt {
                    continue
                }
                fingerprintsByID[asset.localIdentifier] =
                    row.assetFingerprint
                assetsByID[asset.localIdentifier] = asset
            }

            var idsByFingerprint: [Data: [String]] = [:]
            for (assetID, fingerprint) in fingerprintsByID {
                idsByFingerprint[
                    fingerprint,
                    default: []
                ].append(assetID)
            }
            var groups: [DuplicateGroup] = []
            for (fingerprint, assetIDs) in idsByFingerprint
                where assetIDs.count > 1 {
                var entries = assetIDs.compactMap { assetID in
                    assetsByID[assetID].map {
                        DuplicateEntry(
                            assetLocalIdentifier: assetID,
                            creationDate: $0.creationDate,
                            mediaType: $0.mediaType
                        )
                    }
                }
                entries.sort {
                    let left = $0.creationDate ?? .distantFuture
                    let right = $1.creationDate ?? .distantFuture
                    if left != right { return left < right }
                    return $0.assetLocalIdentifier
                        < $1.assetLocalIdentifier
                }
                if entries.count > 1 {
                    groups.append(
                        DuplicateGroup(
                            fingerprint: fingerprint,
                            entries: entries
                        )
                    )
                }
            }
            groups.sort {
                $0.fingerprint.lexicographicallyPrecedes(
                    $1.fingerprint
                )
            }
            return DuplicatesData(
                scopeTotal: allIDs.count,
                scopeIndexed: fingerprintsByID.count,
                groups: groups
            )
        }
    }

    private nonisolated static func revalidate(
        pairs: [KeepDeletePair],
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> Bool {
        await withCancellableDetachedValue(
            priority: .userInitiated
        ) {
            guard !Task.isCancelled else { return false }
            let ids = Set(
                pairs.flatMap { [$0.keep, $0.delete] }
            )
            guard let rows = try? repository
                .fetchValidIndexedRows(assetIDs: ids) else {
                return false
            }
            guard !Task.isCancelled else { return false }
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: ids
            )
            guard !Task.isCancelled else { return false }
            let assetsByID = Dictionary(
                uniqueKeysWithValues: assets.map {
                    ($0.localIdentifier, $0)
                }
            )
            for pair in pairs {
                guard !Task.isCancelled else { return false }
                guard let keepRow = rows[pair.keep],
                      let deleteRow = rows[pair.delete],
                      let keepAsset = assetsByID[pair.keep],
                      let deleteAsset = assetsByID[pair.delete],
                      keepRow.assetFingerprint
                        == deleteRow.assetFingerprint else {
                    return false
                }
                if let modified = keepAsset.modificationDate,
                   modified > keepRow.updatedAt {
                    return false
                }
                if let modified = deleteAsset.modificationDate,
                   modified > deleteRow.updatedAt {
                    return false
                }
            }
            return true
        }
    }

    private nonisolated static func deleteAssets(
        photoLibraryService: PhotoLibraryService,
        assetLocalIdentifiers: [String]
    ) async throws -> Bool {
        try MacPhotoLibraryDeletionPreparationPolicy
            .ensureCommitAllowed(
                isCancelled: Task.isCancelled
            )
        let assets = photoLibraryService.fetchAssets(
            localIdentifiers: Set(assetLocalIdentifiers)
        )
        guard !assets.isEmpty else { return true }
        try MacPhotoLibraryDeletionPreparationPolicy
            .ensureCommitAllowed(
                isCancelled: Task.isCancelled
            )
        return await withCheckedContinuation { continuation in
            PHPhotoLibrary.shared().performChanges {
                PHAssetChangeRequest.deleteAssets(
                    assets as NSFastEnumeration
                )
            } completionHandler: { success, _ in
                continuation.resume(returning: success)
            }
        }
    }

    #if DEBUG
    private static func demoData() -> DuplicatesData {
        let base = Date().addingTimeInterval(-86_400 * 18)
        let counts = [3, 2, 4]
        let groups = counts.enumerated().map {
            groupIndex, count in
            DuplicateGroup(
                fingerprint: Data(
                    repeating: UInt8(groupIndex + 1),
                    count: 32
                ),
                entries: (0..<count).map { entryIndex in
                    DuplicateEntry(
                        assetLocalIdentifier:
                            "demo-\(groupIndex)-\(entryIndex)",
                        creationDate: base.addingTimeInterval(
                            TimeInterval(
                                groupIndex * 7_200
                                    + entryIndex * 90
                            )
                        ),
                        mediaType: entryIndex == count - 1
                            && groupIndex == 2 ? .video : .image
                    )
                }
            )
        }
        return DuplicatesData(
            scopeTotal: 4_286,
            scopeIndexed: 4_286,
            groups: groups
        )
    }
    #endif

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }()
}

extension MacDuplicatesViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        rows.count
    }

    func tableView(
        _ tableView: NSTableView,
        heightOfRow row: Int
    ) -> CGFloat {
        guard rows.indices.contains(row) else { return 54 }
        switch rows[row] {
        case .group:
            return 46
        case .entry:
            return 66
        }
    }

    func tableView(
        _ tableView: NSTableView,
        shouldSelectRow row: Int
    ) -> Bool {
        guard rows.indices.contains(row),
              case .entry(let group, let entry) = rows[row],
              processedGroups.contains(group) else {
            return false
        }
        keepIndexByGroup[group] = entry
        reloadRows(forGroup: group)
        updatePresentation()
        return true
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard rows.indices.contains(row) else { return nil }
        switch rows[row] {
        case .group(let groupIndex):
            return groupCell(index: groupIndex)
        case .entry(let groupIndex, let entryIndex):
            return entryCell(
                row: row,
                groupIndex: groupIndex,
                entryIndex: entryIndex
            )
        }
    }

    private func groupCell(index: Int) -> NSView {
        let cell = NSTableCellView()
        cell.wantsLayer = true
        cell.layer?.backgroundColor = NSColor
            .wmMaterialPrimarySurface.cgColor
        let count = groups.indices.contains(index)
            ? groups[index].entries.count : 0
        let checkbox = NSButton(
            checkboxWithTitle: String.localizedStringWithFormat(
                String(
                    localized: "home.localAlbums.assetCount",
                    defaultValue: "%d items"
                ),
                count
            ),
            target: self,
            action: #selector(toggleGroup(_:))
        )
        checkbox.state = processedGroups.contains(index)
            ? .on : .off
        checkbox.tag = index
        let spacer = NSView()
        let row = NSStackView(
            views: [checkbox, spacer]
        )
        row.orientation = .horizontal
        row.alignment = .centerY
        row.translatesAutoresizingMaskIntoConstraints = false
        cell.addSubview(row)
        NSLayoutConstraint.activate([
            row.leadingAnchor.constraint(
                equalTo: cell.leadingAnchor,
                constant: 16
            ),
            row.trailingAnchor.constraint(
                equalTo: cell.trailingAnchor,
                constant: -16
            ),
            row.centerYAnchor.constraint(
                equalTo: cell.centerYAnchor
            ),
        ])
        return cell
    }

    private func entryCell(
        row: Int,
        groupIndex: Int,
        entryIndex: Int
    ) -> NSView {
        guard groups.indices.contains(groupIndex),
              groups[groupIndex].entries.indices.contains(
                entryIndex
              ) else {
            return NSTableCellView()
        }
        let entry = groups[groupIndex].entries[entryIndex]
        let identifier = NSUserInterfaceItemIdentifier(
            "MacDuplicateEntryCell"
        )
        let cell = tableView.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? MacDuplicateEntryCell ?? MacDuplicateEntryCell()
        cell.identifier = identifier
        let isProcessed = processedGroups.contains(groupIndex)
        let isKeep = isProcessed
            && keepIndexByGroup[groupIndex] == entryIndex
        cell.configure(
            entry: entry,
            isKeep: isKeep,
            isEnabled: isProcessed,
            dateFormatter: Self.dateFormatter,
            actionTarget: self,
            action: #selector(keepEntry(_:)),
            row: row
        )
        return cell
    }
}

extension MacDuplicatesViewController: NSWindowDelegate {
    func windowShouldClose(_ sender: NSWindow) -> Bool {
        guard !isDeleting else {
            let alert = NSAlert()
            alert.alertStyle = .warning
            alert.messageText = String(
                localized: "mac.duplicates.deletingTitle",
                defaultValue: "Photos is updating the library"
            )
            alert.addButton(
                withTitle: String(
                    localized: "common.ok",
                    defaultValue: "OK"
                )
            )
            alert.runModal()
            return false
        }
        return true
    }
}

private enum MacDuplicateError: LocalizedError {
    case staleIndex
    case photosDeleteFailed

    var errorDescription: String? {
        switch self {
        case .staleIndex:
            return String(
                localized: "home.duplicates.staleSnapshotMessage",
                defaultValue: "The Photos library or local index changed. Review the duplicate groups again."
            )
        case .photosDeleteFailed:
            return String(
                localized: "home.duplicates.deleteFailed",
                defaultValue: "Photos could not delete the selected items."
            )
        }
    }
}

@MainActor
private final class MacDuplicateEntryCell: NSTableCellView {
    private var imageRequestID:
        PHImageRequestID = PHInvalidImageRequestID
    private var representedAssetID: String?
    private let thumbnail = NSImageView()
    private let dateLabel = NSTextField(labelWithString: "")
    private let keepButton = NSButton(
        radioButtonWithTitle: "",
        target: nil,
        action: nil
    )

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        thumbnail.imageScaling = .scaleAxesIndependently
        thumbnail.wantsLayer = true
        thumbnail.layer?.cornerRadius = 7
        thumbnail.layer?.masksToBounds = true
        thumbnail.layer?.backgroundColor =
            NSColor.controlBackgroundColor.cgColor
        thumbnail.translatesAutoresizingMaskIntoConstraints = false

        dateLabel.font = .systemFont(ofSize: 13, weight: .medium)

        let content = NSStackView(
            views: [thumbnail, dateLabel, NSView(), keepButton]
        )
        content.orientation = .horizontal
        content.alignment = .centerY
        content.spacing = 12
        content.translatesAutoresizingMaskIntoConstraints = false
        addSubview(content)
        NSLayoutConstraint.activate([
            thumbnail.widthAnchor.constraint(equalToConstant: 48),
            thumbnail.heightAnchor.constraint(equalToConstant: 48),
            content.leadingAnchor.constraint(
                equalTo: leadingAnchor,
                constant: 18
            ),
            content.trailingAnchor.constraint(
                equalTo: trailingAnchor,
                constant: -18
            ),
            content.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(
        entry: MacDuplicatesViewController.DuplicateEntry,
        isKeep: Bool,
        isEnabled: Bool,
        dateFormatter: DateFormatter,
        actionTarget: AnyObject,
        action: Selector,
        row: Int
    ) {
        cancelImageRequest()
        thumbnail.image = NSImage(
            systemSymbolName: entry.mediaType == .video
                ? "video.fill" : "photo.fill",
            accessibilityDescription: nil
        )
        thumbnail.contentTintColor = .tertiaryLabelColor

        let dateText = entry.creationDate.map(
            dateFormatter.string(from:)
        ) ?? String(
            localized: "home.duplicates.unknownDate",
            defaultValue: "Unknown date"
        )
        dateLabel.stringValue = dateText
        keepButton.title = String(
            localized: "home.duplicates.statusKeep",
            defaultValue: "Keep"
        )
        keepButton.target = actionTarget
        keepButton.action = action
        keepButton.state = isKeep ? .on : .off
        keepButton.isEnabled = isEnabled
        keepButton.tag = row
        alphaValue = isEnabled ? 1 : 0.45

        representedAssetID = entry.assetLocalIdentifier
        guard !entry.assetLocalIdentifier.hasPrefix("demo-") else {
            thumbnail.contentTintColor = isKeep
                ? .wmMaterialPrimary
                : .secondaryLabelColor
            return
        }
        let result = PHAsset.fetchAssets(
            withLocalIdentifiers: [entry.assetLocalIdentifier],
            options: nil
        )
        guard let asset = result.firstObject else { return }
        let options = PHImageRequestOptions()
        options.deliveryMode = .opportunistic
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = false
        imageRequestID = PHImageManager.default().requestImage(
            for: asset,
            targetSize: NSSize(width: 96, height: 96),
            contentMode: .aspectFill,
            options: options
        ) { [weak self, weak thumbnail = self.thumbnail] image, _ in
            guard self?.representedAssetID
                    == entry.assetLocalIdentifier,
                  let image else { return }
            thumbnail?.image = image
        }
    }

    private func cancelImageRequest() {
        guard imageRequestID != PHInvalidImageRequestID else {
            return
        }
        PHImageManager.default().cancelImageRequest(imageRequestID)
        imageRequestID = PHInvalidImageRequestID
    }
}
