import Photos
import UIKit
import UniformTypeIdentifiers

private final class MediaBrowserSourceLease: Sendable {
    let source: MediaBrowserSource

    init(_ source: MediaBrowserSource) {
        self.source = source
    }

    deinit {
        let source = source
        Task { await source.shutdown() }
    }
}

struct MediaBrowserThumbnailReloadTracker: Sendable {
    private(set) var requestedGeneration: UInt64 = 0
    private(set) var appliedGeneration: UInt64 = 0

    mutating func requestReload() {
        requestedGeneration &+= 1
    }

    func shouldApply(_ generation: UInt64) -> Bool {
        generation > appliedGeneration
    }

    mutating func markApplied(_ generation: UInt64) {
        appliedGeneration = max(appliedGeneration, generation)
    }
}

// Source-driven media grid (local / remote / merged). Month sections, date-descending. Cells show a
// thumbnail plus type (video/live) and presence badges. Tapping opens the full-screen paging viewer.
// Modes are switchable via a segmented control whose availability tracks the remote connection live.
@MainActor
final class MediaBrowserGridViewController: UIViewController {
    // One selectable mode. `isAvailable` is re-evaluated on connection changes to enable/disable its tab.
    struct ModeSpec {
        let mode: MediaBrowserMode
        let isAvailable: () -> Bool
        let makeSource: () -> MediaBrowserSource
    }

    struct SelectionAction {
        let buttonTitle: String
        let symbolName: String
        let initialOptions: InboxTransferOptions
        let accessPolicy: @MainActor () -> InboxTransferAccessPolicy
        let canChooseDestination: @MainActor () -> Bool
        let makeMenu: (@escaping (InboxTransferDestination) -> Void) -> UIMenu
        let perform: @MainActor (
            InboxTransferDestination,
            [InboxTransferItem],
            UIViewController,
            InboxTransferOptions,
            InboxTransferActivity
        ) async -> Bool
    }

    private enum Layout {
        static let spacing: CGFloat = 2
        static let maximumItemWidth: CGFloat = 132
        static let minimumColumnCount = 3
        static let headerHeight: CGFloat = 44

        static func metrics(for availableWidth: CGFloat) -> (columnCount: Int, itemWidth: CGFloat) {
            guard availableWidth > 0 else { return (minimumColumnCount, maximumItemWidth) }
            let rawColumnCount = Int(ceil((availableWidth + spacing) / (maximumItemWidth + spacing)))
            let columnCount = max(minimumColumnCount, rawColumnCount)
            let itemWidth = floor((availableWidth - CGFloat(columnCount - 1) * spacing) / CGFloat(columnCount))
            return (columnCount, itemWidth)
        }
    }

    private enum SelectionActionUIState {
        enum PausePhase: Equatable {
            case active
            case pausing
            case paused
        }

        enum TerminalKind: Equatable {
            case completed
            case failed
        }

        case idle
        case running(status: String, pause: PausePhase)
        case stopping(status: String)
        case terminal(status: String, kind: TerminalKind)

        var isPerforming: Bool {
            switch self {
            case .running, .stopping: true
            case .idle, .terminal: false
            }
        }

        var showsPanel: Bool {
            switch self {
            case .idle: false
            case .running, .stopping, .terminal: true
            }
        }
    }

    private enum TransferSelectionGroup: Equatable {
        case photos
        case videos
        case files
    }

    private struct TransferPhotoSummaryRequestKey: Hashable {
        let localIdentifiers: [String]
        let options: InboxTransferOptions
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<LibraryMonthKey, MediaBrowserItemID>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<LibraryMonthKey, MediaBrowserItemID>
    private static let headerKind = "month-header"
    private static let fallbackCellReuseID = "media-browser-fallback-cell"

    private let specs: [ModeSpec]
    private let navTitle: String
    private let remoteStorageImage: () -> UIImage?
    private let actionRunner: MediaBrowserActionRunner
    private let selectionAction: SelectionAction?
    private var isMediaDrop: Bool { selectionAction != nil }
    private let onTransferModeSwitchAvailabilityChanged: ((Bool) -> Void)?
    private let onTransferPanelVisibilityChanged: ((Bool) -> Void)?
    private var inboxTransferOptions: InboxTransferOptions
    // The one presence authority for this browser session. Its invalidation signal drives a source reload;
    // the old UI snapshot remains visible until a current replacement commits.
    private let presenceIndex: LibraryPresenceIndex
    // Identifies the active remote session/profile (nil = disconnected). A change means a remote-backed
    // source is now stale (disconnect, or profile A→B while still connected).
    private let sessionToken: () -> AnyHashable?
    private var currentMode: MediaBrowserMode
    private var sourceLease: MediaBrowserSourceLease
    private var source: MediaBrowserSource { sourceLease.source }
    private var sourceToken: AnyHashable?
    private var pendingScrollMonth: LibraryMonthKey?

    private let browserSession = MediaBrowserSession()
    private var browserSnapshot: MediaBrowserSnapshot { browserSession.snapshot }
    private var months: [LibraryMonthKey] { browserSession.snapshot.months }
    private var dataSource: DataSource?
    private var loadTask: Task<Void, Never>?
    private var loadGeneration = 0
    private var thumbnailReloadTracker = MediaBrowserThumbnailReloadTracker()
    private weak var segmentedControl: UISegmentedControl?

    private var isSelecting = false
    private var selectedItemIDs: Set<MediaBrowserItemID> = []
    private var isNativeMultipleSelectionInteraction = false
    private var selectionDragSelects: Bool?
    private var selectionDragAnchorIndex: Int?
    private var selectionDragBaselineIDs: Set<MediaBrowserItemID> = []
    private weak var activeSelectionDragGesture: UILongPressGestureRecognizer?
    private var selectionDragDisplayLink: CADisplayLink?
    private var selectionDragDisplayLinkTimestamp: CFTimeInterval?
    private let batchBarContainer = UIView()
    private let batchBar = MediaActionBar()
    private let selectionActionButton = UIButton(configuration: .filled())
    private let transferSelectionSummaryScrollView = UIScrollView()
    private let transferPhotoSummaryButton = UIButton(type: .system)
    private let transferVideoSummaryButton = UIButton(type: .system)
    private let transferFileSummaryButton = UIButton(type: .system)
    private let transferTopBar = UIView()
    private let transferOptionsButton = UIButton(type: .system)
    private let transferChooseFilesButton = UIButton(type: .system)
    private lazy var transferFileSelection = MediaDropFileSelectionController()
    private let selectionActivityPanel = UIView()
    private let selectionActivityStatusButton = UIButton(type: .system)
    private let selectionActivityPhotoButton = UIButton(type: .system)
    private let selectionActivityVideoButton = UIButton(type: .system)
    private let selectionActivityFileButton = UIButton(type: .system)
    private let selectionActivityPauseButton = UIButton(type: .system)
    private let selectionActivityStopButton = UIButton(type: .system)
    private var transferPanelShownConstraint: NSLayoutConstraint?
    private var transferPanelHiddenConstraint: NSLayoutConstraint?
    private var isTransferPanelVisible = false
    private var selectionActionActivity: InboxTransferActivity?
    private var selectionActionTask: Task<Void, Never>?
    private var selectionActionUIState = SelectionActionUIState.idle
    private var showsSelectionActivityPanel = false
    private var selectionActivityTransitionAnimator: UIViewPropertyAnimator?
    private static let batchBarHeight: CGFloat = 72
    private var libraryChangeReload: DispatchWorkItem?
    private var deferredReloadTrigger: String?
    private var lastTransferPhotoAuthorizationStatus: PHAuthorizationStatus?
    private var transferPhotoSummaryRequestKey: TransferPhotoSummaryRequestKey?
    private var transferPhotoSelectionDetails: [String: InboxTransferPhotoSelectionDetail] = [:]
    private var transferPhotoSummaryTask: Task<Void, Never>?
    private var isRuntimeObservationActive = false
    private var isMediaDropActive = false
    private var wasTransferLimitExceeded = false

    private var isAnyActionRunning: Bool {
        actionRunner.isActionRunning || selectionActionUIState.showsPanel
    }

    private var defersSnapshotReload: Bool {
        isAnyActionRunning || activeSelectionDragGesture != nil || isNativeMultipleSelectionInteraction
    }

    private var isContentActive: Bool {
        !isMediaDrop || isMediaDropActive
    }

    var mediaDropPanelTopAnchor: NSLayoutYAxisAnchor {
        batchBarContainer.topAnchor
    }

    private lazy var collectionView = UICollectionView(frame: .zero, collectionViewLayout: makeLayout())

    // Empty-state copy depends on the mode: "nothing backed up" only makes sense for remote.
    private func makeEmptyState() -> UIView {
        if isMediaDrop {
            switch PHPhotoLibrary.authorizationStatus(for: .readWrite) {
            case .notDetermined:
                return makeTransferPhotoAccessStateView(
                    title: String(localized: "home.overlay.authRequired"),
                    actionTitle: String(localized: "home.overlay.allowAccess"),
                    action: UIAction { [weak self] _ in self?.requestTransferPhotoAccess() }
                )
            case .denied, .restricted:
                return makeTransferPhotoAccessStateView(
                    title: String(localized: "home.overlay.noAuth"),
                    actionTitle: String(localized: "home.overlay.goToSettings"),
                    action: UIAction { _ in
                        guard let url = URL(string: UIApplication.openSettingsURLString) else { return }
                        UIApplication.shared.open(url)
                    }
                )
            case .authorized, .limited:
                break
            @unknown default:
                break
            }
        }

        let title: String
        let message: String
        switch currentMode {
        case .remote:
            title = String(localized: "remoteBrowser.empty.title")
            message = String(localized: "remoteBrowser.empty.message")
        case .local, .merged:
            title = String(localized: "mediaBrowser.empty.title")
            message = String(localized: "mediaBrowser.empty.message")
        }
        return makeAlbumEmptyStateView(title: title, message: message)
    }

    private func makeTransferPhotoAccessStateView(
        title: String,
        actionTitle: String,
        action: UIAction
    ) -> UIView {
        MediaDropEmptyActionView(
            title: title,
            actionTitle: actionTitle,
            action: action
        )
    }

    private func requestTransferPhotoAccess() {
        PHPhotoLibrary.requestAuthorization(for: .readWrite) { [weak self] status in
            DispatchQueue.main.async {
                guard let self else { return }
                self.lastTransferPhotoAuthorizationStatus = status
                self.load(trigger: "photoAuthorization")
            }
        }
    }

    private func makeLoadingState() -> UIView {
        let view = UIView()

        let spinner = UIActivityIndicatorView(style: .large)
        spinner.startAnimating()

        let titleLabel = UILabel()
        titleLabel.text = String(localized: "mediaBrowser.loading.title")
        titleLabel.textColor = .secondaryLabel
        titleLabel.font = .preferredFont(forTextStyle: .headline)
        titleLabel.textAlignment = .center
        titleLabel.adjustsFontForContentSizeCategory = true

        let messageLabel = UILabel()
        messageLabel.text = String(localized: "mediaBrowser.loading.message")
        messageLabel.textColor = .tertiaryLabel
        messageLabel.font = .preferredFont(forTextStyle: .subheadline)
        messageLabel.textAlignment = .center
        messageLabel.numberOfLines = 0
        messageLabel.adjustsFontForContentSizeCategory = true

        let stackView = UIStackView(arrangedSubviews: [spinner, titleLabel, messageLabel])
        stackView.axis = .vertical
        stackView.alignment = .center
        stackView.spacing = 10

        view.addSubview(stackView)
        stackView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            stackView.centerXAnchor.constraint(equalTo: view.centerXAnchor),
            stackView.centerYAnchor.constraint(equalTo: view.centerYAnchor),
            stackView.leadingAnchor.constraint(greaterThanOrEqualTo: view.leadingAnchor, constant: 32),
            stackView.trailingAnchor.constraint(lessThanOrEqualTo: view.trailingAnchor, constant: -32),
        ])
        return view
    }

    init(
        specs: [ModeSpec],
        initialMode: MediaBrowserMode,
        initialMonth: LibraryMonthKey?,
        remoteStorageImage: @escaping () -> UIImage?,
        sessionToken: @escaping () -> AnyHashable?,
        actionRunner: MediaBrowserActionRunner,
        presenceIndex: LibraryPresenceIndex,
        title: String,
        selectionAction: SelectionAction? = nil,
        onTransferModeSwitchAvailabilityChanged: ((Bool) -> Void)? = nil,
        onTransferPanelVisibilityChanged: ((Bool) -> Void)? = nil
    ) {
        self.specs = specs
        self.navTitle = title
        self.remoteStorageImage = remoteStorageImage
        self.sessionToken = sessionToken
        self.actionRunner = actionRunner
        self.presenceIndex = presenceIndex
        self.selectionAction = selectionAction
        self.onTransferModeSwitchAvailabilityChanged = onTransferModeSwitchAvailabilityChanged
        self.onTransferPanelVisibilityChanged = onTransferPanelVisibilityChanged
        self.inboxTransferOptions = selectionAction?.initialOptions ?? .defaultOption
        self.pendingScrollMonth = initialMonth
        let initialSpec = specs.first(where: { $0.mode == initialMode }) ?? specs[0]
        self.currentMode = initialSpec.mode
        self.sourceLease = MediaBrowserSourceLease(initialSpec.makeSource())
        self.sourceToken = sessionToken()
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        selectionActionTask?.cancel()
        transferPhotoSummaryTask?.cancel()
        selectionDragDisplayLink?.invalidate()
        NotificationCenter.default.removeObserver(self)
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        configureNavigationTitle()
        configureModeSwitcher()
        configureTransferTopBar()
        configureBatchBar()
        configureUI()
        view.bringSubviewToFront(batchBarContainer)
        configureTransferFileSelection()
        configureDataSource()
        actionRunner.onActionStateChanged = { [weak self] isRunning in
            guard !isRunning else { return }
            self?.flushDeferredReloadIfNeeded()
        }
        updateSelectBarButton()
        if isMediaDrop {
            enterSelection()
        }
        if isContentActive {
            startRuntimeObservation()
            load(trigger: "initial")
        }
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        guard isContentActive else { return }
        flushDeferredReloadIfNeeded()
        refreshVisibleThumbnails()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        guard isContentActive else { return }
        refreshTransferAccessPresentation()
    }

    func setMediaDropActive(_ active: Bool) {
        guard isMediaDrop, isMediaDropActive != active else { return }
        isMediaDropActive = active
        guard isViewLoaded else { return }
        if active {
            startRuntimeObservation()
            sourceToken = sessionToken()
            lastTransferPhotoAuthorizationStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
            thumbnailReloadTracker.requestReload()
            refreshTransferAccessPresentation()
            reloadRespectingActionGate(trigger: "mediaDropActivated")
            return
        }
        stopRuntimeObservation()
        libraryChangeReload?.cancel()
        libraryChangeReload = nil
        deferredReloadTrigger = nil
        loadTask?.cancel()
        loadTask = nil
        loadGeneration += 1
        transferPhotoSummaryTask?.cancel()
        transferPhotoSummaryTask = nil
        transferPhotoSummaryRequestKey = nil
        endSelectionDrag()
        for case let cell as MediaBrowserGridCell in collectionView.visibleCells {
            cell.cancelLoading()
        }
    }

    private func startRuntimeObservation() {
        guard !isRuntimeObservationActive else { return }
        isRuntimeObservationActive = true
        let center = NotificationCenter.default
        center.addObserver(self, selector: #selector(sessionChanged), name: .AppSessionChanged, object: nil)
        center.addObserver(self, selector: #selector(thumbnailStored(_:)), name: .MediaBrowserThumbnailDidStore, object: nil)
        if isMediaDrop {
            center.addObserver(self, selector: #selector(transferAvailabilityChanged), name: .ExecutionLifecycleDidChange, object: nil)
            center.addObserver(self, selector: #selector(transferAvailabilityChanged), name: .RemoteMaintenanceDidChange, object: nil)
            center.addObserver(self, selector: #selector(transferAvailabilityChanged), name: .ConnectionLifecycleDidChange, object: nil)
            center.addObserver(self, selector: #selector(transferAvailabilityChanged), name: .LocalIndexBuildStateDidChange, object: nil)
            center.addObserver(self, selector: #selector(transferAppDidBecomeActive), name: UIApplication.didBecomeActiveNotification, object: nil)
        } else {
            center.addObserver(self, selector: #selector(presenceChanged), name: .LibraryPresenceDidChange, object: nil)
        }
        PHPhotoLibrary.shared().register(self)
    }

    private func stopRuntimeObservation() {
        guard isRuntimeObservationActive else { return }
        isRuntimeObservationActive = false
        NotificationCenter.default.removeObserver(self)
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
    }

    @objc private func presenceChanged() {
        DispatchQueue.main.async { [weak self] in
            self?.reloadRespectingActionGate(trigger: "presence")
        }
    }

    @objc private func transferAvailabilityChanged() {
        DispatchQueue.main.async { [weak self] in
            guard let self, self.isContentActive, self.selectionAction != nil else { return }
            self.recomputeBatchBar()
        }
    }

    @objc private func transferAppDidBecomeActive() {
        guard isContentActive else { return }
        let status = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard status != lastTransferPhotoAuthorizationStatus else { return }
        lastTransferPhotoAuthorizationStatus = status
        reloadRespectingActionGate(trigger: "photoAuthorization")
    }

    private func scheduleLibraryChangeReload() {
        guard isContentActive else { return }
        // Remote mode reloads too: membership is PhotoKit-independent, but the device handles bound at load
        // are not — a Photos edit must reproject the record (stale handle drops → `.remoteOnly`).
        thumbnailReloadTracker.requestReload()
        libraryChangeReload?.cancel()
        let work = DispatchWorkItem { [weak self] in self?.reloadRespectingActionGate(trigger: "photoLibrary") }
        libraryChangeReload = work
        // Coalesce PhotoKit bursts (iCloud sync fires per batch) into one reload.
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5, execute: work)
    }

    @objc private func thumbnailStored(_ notification: Notification) {
        guard isContentActive else { return }
        guard let fingerprint = notification.userInfo?[MediaThumbnailCache.storedFingerprintUserInfoKey] as? Data,
              let image = notification.userInfo?[MediaThumbnailCache.storedImageUserInfoKey] as? UIImage else { return }
        DispatchQueue.main.async { [weak self] in
            self?.applyStoredThumbnail(image, fingerprint: fingerprint)
        }
    }

    private func applyStoredThumbnail(_ image: UIImage, fingerprint: Data) {
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let item = item(at: indexPath),
                  item.fingerprint == fingerprint,
                  let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { continue }
            cell.applyStoredThumbnail(image, for: item)
        }
    }

    private func refreshVisibleThumbnails(forceReload: Bool = false) {
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let item = item(at: indexPath),
                  let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { continue }
            if forceReload {
                cell.reloadThumbnail(item: item, source: source)
            } else {
                cell.beginLoading(item: item, source: source)
            }
        }
    }

    private func reloadRespectingActionGate(trigger: String) {
        guard isContentActive else { return }
        if defersSnapshotReload {
            deferredReloadTrigger = trigger
            return
        }
        load(trigger: trigger)
    }

    private func flushDeferredReloadIfNeeded() {
        guard isContentActive else { return }
        guard !defersSnapshotReload, let trigger = deferredReloadTrigger else { return }
        deferredReloadTrigger = nil
        load(trigger: trigger)
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        endSelectionDrag()
        // Only when the browsing session actually ends — not when we merely present the full-screen viewer.
        guard isBeingDismissed || isMovingFromParent || navigationController?.isBeingDismissed == true else { return }
        if selectionAction != nil {
            transferFileSelection.cancelPendingImport()
        }
        selectionActionTask?.cancel()
        if let activity = selectionActionActivity {
            Task { await activity.pauseGate.setPaused(false) }
        }
        Task { await MediaThumbnailCache.enforceLimit() }
    }

    private func configureModeSwitcher() {
        guard selectionAction == nil else { return }
        guard specs.count > 1 else { return }
        let control = UISegmentedControl(items: specs.map { Self.title(for: $0.mode) })
        control.addTarget(self, action: #selector(modeChanged(_:)), for: .valueChanged)
        navigationItem.titleView = control
        segmentedControl = control
        refreshSegmentAvailability()
        control.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == currentMode }) ?? 0
    }

    @objc private func sessionChanged() {
        refreshSegmentAvailability()
        let newToken = sessionToken()
        guard newToken != sourceToken else { return }   // same profile/session → nothing went stale
        sourceToken = newToken
        if isMediaDrop, currentMode == .local {
            recomputeBatchBar()
            return
        }
        exitSelection()
        // Local content is session-independent, but its presence badges (.both) are computed against the
        // remote snapshot — recompute them for the new session without rebuilding the source.
        if currentMode == .local {
            load(trigger: "session")
            return
        }
        // Remote/merged: the source (and any presented viewer built on it) now points at the wrong / gone
        // node. Close the viewer, then rebuild the current mode with the new session — or fall back to local.
        if presentedViewController != nil { dismiss(animated: false) }
        let target = specs.first(where: { $0.mode == currentMode && $0.isAvailable() })
            ?? specs.first(where: { $0.mode == .local })
        if let target { switchTo(spec: target, trigger: "session") }
    }

    private func refreshSegmentAvailability() {
        guard let control = segmentedControl else { return }
        for (index, spec) in specs.enumerated() {
            control.setEnabled(spec.isAvailable(), forSegmentAt: index)
        }
    }

    @objc private func modeChanged(_ control: UISegmentedControl) {
        guard specs.indices.contains(control.selectedSegmentIndex) else { return }
        // Don't switch source out from under a running batch (its HUD/progress would float over the new mode).
        guard !isAnyActionRunning else {
            control.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == currentMode }) ?? 0
            return
        }
        let spec = specs[control.selectedSegmentIndex]
        guard spec.mode != currentMode, spec.isAvailable() else {
            control.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == currentMode }) ?? 0
            return
        }
        switchTo(spec: spec, trigger: "modeSwitch")
    }

    private func switchTo(spec: ModeSpec, trigger: String) {
        exitSelection()
        currentMode = spec.mode
        segmentedControl?.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == spec.mode }) ?? 0
        pendingScrollMonth = nil
        loadTask?.cancel()
        sourceLease = MediaBrowserSourceLease(spec.makeSource())
        sourceToken = sessionToken()
        browserSession.reset()
        collectionView.backgroundView = nil
        applySnapshot()
        load(trigger: trigger)
    }

    static func title(for mode: MediaBrowserMode) -> String {
        switch mode {
        case .local: return String(localized: "mediaBrowser.mode.local")
        case .remote: return String(localized: "mediaBrowser.mode.remote")
        case .merged: return String(localized: "mediaBrowser.mode.merged")
        }
    }

    private func configureUI() {
        collectionView.backgroundColor = .appBackground
        collectionView.alwaysBounceVertical = true
        collectionView.contentInsetAdjustmentBehavior = .always
        collectionView.allowsMultipleSelection = true
        // Avoid spawning thumbnail tasks for far off-screen cells during fast scrolling; cells that do
        // scroll past are recycled and cancel their in-flight request (renderLocalThumbnail is cancellable).
        collectionView.isPrefetchingEnabled = false
        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.translatesAutoresizingMaskIntoConstraints = false
        let topAnchor = selectionAction == nil ? view.topAnchor : transferTopBar.bottomAnchor
        let bottomAnchor = selectionAction == nil ? view.bottomAnchor : batchBarContainer.topAnchor
        NSLayoutConstraint.activate([
            collectionView.topAnchor.constraint(equalTo: topAnchor),
            collectionView.bottomAnchor.constraint(equalTo: bottomAnchor),
            collectionView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            collectionView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
    }

    private func configureTransferFileSelection() {
        guard selectionAction != nil else { return }
        transferFileSelection.onChange = { [weak self] in self?.recomputeBatchBar() }
        transferFileSelection.onError = { [weak self] error in
            self?.presentTransferFileImportError(error)
        }
    }

    private func configureDataSource() {
        collectionView.register(
            UICollectionViewCell.self,
            forCellWithReuseIdentifier: Self.fallbackCellReuseID
        )
        let cellRegistration = UICollectionView.CellRegistration<MediaBrowserGridCell, MediaBrowserItem> { [weak self] cell, _, item in
            guard let self else { return }
            cell.configure(
                with: item,
                remoteImage: self.remoteStorageImage(),
                showsPresenceBadge: !self.isMediaDrop
            )
            cell.setSelecting(self.isSelecting, selected: self.selectedItemIDs.contains(item.id))
            cell.onSelectionButtonTapped = { [weak self] in
                self?.toggleSelectionFromButton(for: item.id)
            }
            cell.onSelectionButtonDrag = { [weak self] gesture in
                self?.handleSelectionButtonDrag(gesture, startingAt: item.id)
            }
        }
        let dataSource = DataSource(collectionView: collectionView) { [weak self] collectionView, indexPath, itemID in
            guard let self,
                  let item = self.item(at: indexPath),
                  item.id == itemID else {
                return collectionView.dequeueReusableCell(
                    withReuseIdentifier: Self.fallbackCellReuseID,
                    for: indexPath
                )
            }
            return collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: item)
        }
        let headerRegistration = UICollectionView.SupplementaryRegistration<MediaBrowserHeaderView>(
            elementKind: Self.headerKind
        ) { [weak self] header, _, indexPath in
            guard let self, indexPath.section < self.months.count else { return }
            header.configure(title: self.months[indexPath.section].displayText)
        }
        dataSource.supplementaryViewProvider = { collectionView, _, indexPath in
            collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
        self.dataSource = dataSource
    }

    private func load(trigger: String, staleAttempt: Int = 0) {
        guard isContentActive else { return }
        if !defersSnapshotReload {
            deferredReloadTrigger = nil
        }
        loadTask?.cancel()
        loadGeneration += 1
        let generation = loadGeneration
        let thumbnailReloadGeneration = thumbnailReloadTracker.requestedGeneration
        let source = source
        let trace = MediaBrowserLoadTrace.makeContext(mode: source.mode)
        MediaBrowserLoadTrace.emit(
            "start",
            context: trace,
            details: "generation=\(generation) trigger=\(trigger)"
        )
        if months.isEmpty {
            collectionView.backgroundView = makeLoadingState()
        }
        loadTask = Task { [weak self] in
            await MediaBrowserLoadTrace.$context.withValue(trace) {
                let sectionsStartedAt = CFAbsoluteTimeGetCurrent()
                let result = await source.load()
                let content: MediaBrowserContent
                switch result {
                case .loaded(let loaded):
                    content = loaded
                case .stale:
                    MediaBrowserLoadTrace.emit(
                        "stale",
                        details: "after=load attempt=\(staleAttempt)"
                    )
                    guard staleAttempt < 5 else { return }
                    let retryDelay = min(0.8, 0.05 * pow(2.0, Double(staleAttempt)))
                    await MainActor.run {
                        guard let self,
                              !Task.isCancelled,
                              self.loadGeneration == generation else { return }
                        DispatchQueue.main.asyncAfter(deadline: .now() + retryDelay) { [weak self] in
                            guard let self, self.loadGeneration == generation else { return }
                            if self.defersSnapshotReload {
                                self.reloadRespectingActionGate(trigger: "staleDeferred")
                            } else {
                                self.load(
                                    trigger: "staleRetry",
                                    staleAttempt: staleAttempt + 1
                                )
                            }
                        }
                    }
                    return
                case .cancelled:
                    MediaBrowserLoadTrace.emit("cancelled", details: "after=load")
                    return
                }
                let snapshot = await withCancellableDetachedValue(priority: .userInitiated) {
                    MediaBrowserSnapshot(sections: content.sections)
                }
                let itemCount = snapshot.itemCount
                MediaBrowserLoadTrace.emit(
                    "sections",
                    startedAt: sectionsStartedAt,
                    details: "sections=\(content.sections.count) items=\(itemCount)"
                )
                guard !Task.isCancelled else {
                    MediaBrowserLoadTrace.emit("cancelled", details: "after=sections")
                    return
                }

                let mainStartedAt = CFAbsoluteTimeGetCurrent()
                await MainActor.run {
                    // Drop a completion from a superseded source (rapid tab switch / session change) so it can't
                    // overwrite the current snapshot — !Task.isCancelled alone can race a past-await continuation.
                    guard let self, !Task.isCancelled, self.loadGeneration == generation else {
                        MediaBrowserLoadTrace.emit(
                            "dropped",
                            context: trace,
                            startedAt: mainStartedAt,
                            details: "generation=\(generation)"
                        )
                        return
                    }
                    guard !self.defersSnapshotReload else {
                        self.reloadRespectingActionGate(trigger: "loadDeferred")
                        return
                    }
                    let changedItemIDs = snapshot.changedItemIDs(comparedTo: self.browserSnapshot)
                    self.browserSession.replace(with: snapshot)
                    self.applySnapshot(
                        reconfiguring: changedItemIDs,
                        thumbnailReloadGeneration: thumbnailReloadGeneration,
                        loadGeneration: generation,
                        trace: trace
                    )
                    self.collectionView.backgroundView = self.months.isEmpty ? self.makeEmptyState() : nil
                    self.scrollToPendingMonthIfNeeded()
                    self.updateSelectBarButton()
                    if self.isMediaDrop, !self.isSelecting {
                        self.enterSelection()
                    }
                    // Content may have changed under an active selection (a background refresh / a just-finished batch):
                    // leave selection if the grid emptied; otherwise drop stale ids and re-derive which actions still apply.
                    if self.isSelecting {
                        if self.months.isEmpty, self.selectionAction == nil {
                            self.exitSelection()
                        } else {
                            self.selectedItemIDs.formIntersection(Set(snapshot.itemIDs))
                            self.recomputeBatchBar()
                            self.refreshVisibleSelectionOverlays()
                        }
                    }
                    // The viewer keeps its page order but resolves visible pages from this new session revision.
                    (self.presentedViewController as? MediaBrowserViewerViewController)?.reconcileItems()
                    MediaBrowserLoadTrace.emit(
                        "main",
                        context: trace,
                        startedAt: mainStartedAt,
                        details: "generation=\(generation)"
                    )
                }
            }
        }
    }

    // Consumed once, on first load: jumps to the month the browser was opened at.
    private func scrollToPendingMonthIfNeeded() {
        guard let month = pendingScrollMonth, let section = months.firstIndex(of: month) else { return }
        pendingScrollMonth = nil
        collectionView.layoutIfNeeded()
        collectionView.scrollToItem(at: IndexPath(item: 0, section: section), at: .top, animated: false)
    }

    private func applySnapshot(
        reconfiguring changedItemIDs: Set<MediaBrowserItemID> = [],
        thumbnailReloadGeneration: UInt64? = nil,
        loadGeneration: Int? = nil,
        trace: MediaBrowserLoadTrace.Context? = nil
    ) {
        let buildStartedAt = CFAbsoluteTimeGetCurrent()
        var snapshot = Snapshot()
        snapshot.appendSections(months)
        let visibleItemIDs = collectionView.indexPathsForVisibleItems.compactMap {
            dataSource?.itemIdentifier(for: $0)
        }
        for sectionIndex in months.indices {
            snapshot.appendItems(
                browserSnapshot.itemIDs(inSection: sectionIndex),
                toSection: months[sectionIndex]
            )
        }
        let reconfiguredItemIDs = visibleItemIDs.filter {
            changedItemIDs.contains($0) && snapshot.indexOfItem($0) != nil
        }
        if !reconfiguredItemIDs.isEmpty { snapshot.reconfigureItems(reconfiguredItemIDs) }
        let itemCount = snapshot.numberOfItems
        MediaBrowserLoadTrace.emit(
            "snapshotBuild",
            context: trace,
            startedAt: buildStartedAt,
            details: "sections=\(snapshot.numberOfSections) items=\(itemCount) reconfigured=\(reconfiguredItemIDs.count)"
        )
        let applyStartedAt = CFAbsoluteTimeGetCurrent()
        dataSource?.apply(snapshot, animatingDifferences: false) {
            Task { @MainActor [weak self] in
                MediaBrowserLoadTrace.emit(
                    "snapshotApplied",
                    context: trace,
                    startedAt: applyStartedAt,
                    details: "items=\(itemCount)"
                )
                guard let self,
                      let thumbnailReloadGeneration,
                      let loadGeneration,
                      self.loadGeneration == loadGeneration,
                      self.thumbnailReloadTracker.shouldApply(thumbnailReloadGeneration) else { return }
                self.thumbnailReloadTracker.markApplied(thumbnailReloadGeneration)
                self.refreshVisibleThumbnails(forceReload: true)
            }
        }
    }

    private func item(at indexPath: IndexPath) -> MediaBrowserItem? {
        browserSnapshot.item(section: indexPath.section, item: indexPath.item)
    }

    // MARK: - Multi-select

    private func configureNavigationTitle() {
        guard selectionAction == nil else { return }
        title = navTitle
    }

    private func configureTransferTopBar() {
        guard selectionAction != nil else { return }
        let transferLocalHeader = UIView()
        let transferFileHeader = UIView()
        let transferLocalHeaderLabel = UILabel()
        let transferFileHeaderLabel = UILabel()
        let headerBackgroundColor = UIColor.materialSurface(
            light: .Material.Green._100,
            darkTint: .Material.Green._200,
            darkAlpha: 0.16
        )
        view.addSubview(transferTopBar)
        transferTopBar.addSubview(transferLocalHeader)
        transferTopBar.addSubview(transferFileHeader)
        transferLocalHeader.backgroundColor = headerBackgroundColor
        transferFileHeader.backgroundColor = headerBackgroundColor

        let headerTextColor = UIColor.materialOnContainer(
            light: .Material.Green._900,
            dark: .Material.Green._100
        )
        configureTransferHeaderLabel(
            transferLocalHeaderLabel,
            text: String(localized: "transfer.header.localLibrary"),
            color: headerTextColor
        )
        configureTransferHeaderLabel(
            transferFileHeaderLabel,
            text: String(localized: "transfer.source.files"),
            color: headerTextColor
        )
        configureTransferHeaderAction(
            transferOptionsButton,
            title: String(localized: "transfer.header.options"),
            symbolName: "line.3.horizontal.decrease",
            color: headerTextColor
        )
        transferOptionsButton.showsMenuAsPrimaryAction = true
        configureTransferHeaderAction(
            transferChooseFilesButton,
            title: String(localized: "transfer.files.select"),
            symbolName: "folder",
            color: headerTextColor
        )
        transferChooseFilesButton.addTarget(
            self,
            action: #selector(openTransferFilePicker),
            for: .touchUpInside
        )

        let localContent = makeTransferHeaderContent(
            titleLabel: transferLocalHeaderLabel,
            actionButton: transferOptionsButton
        )
        let fileContent = makeTransferHeaderContent(
            titleLabel: transferFileHeaderLabel,
            actionButton: transferChooseFilesButton
        )
        transferLocalHeader.addSubview(localContent)
        transferFileHeader.addSubview(fileContent)
        transferTopBar.translatesAutoresizingMaskIntoConstraints = false
        transferLocalHeader.translatesAutoresizingMaskIntoConstraints = false
        transferFileHeader.translatesAutoresizingMaskIntoConstraints = false
        localContent.translatesAutoresizingMaskIntoConstraints = false
        fileContent.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            transferTopBar.topAnchor.constraint(equalTo: view.topAnchor),
            transferTopBar.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            transferTopBar.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            transferTopBar.bottomAnchor.constraint(
                equalTo: view.safeAreaLayoutGuide.topAnchor,
                constant: HomeHeaderLayout.areaHeight
            ),
            transferLocalHeader.topAnchor.constraint(equalTo: transferTopBar.topAnchor),
            transferLocalHeader.leadingAnchor.constraint(equalTo: transferTopBar.leadingAnchor),
            transferLocalHeader.trailingAnchor.constraint(equalTo: transferTopBar.centerXAnchor, constant: -1),
            transferLocalHeader.bottomAnchor.constraint(equalTo: transferTopBar.bottomAnchor),
            transferFileHeader.topAnchor.constraint(equalTo: transferTopBar.topAnchor),
            transferFileHeader.leadingAnchor.constraint(equalTo: transferTopBar.centerXAnchor, constant: 1),
            transferFileHeader.trailingAnchor.constraint(equalTo: transferTopBar.trailingAnchor),
            transferFileHeader.bottomAnchor.constraint(equalTo: transferTopBar.bottomAnchor),
            localContent.centerXAnchor.constraint(equalTo: transferLocalHeader.centerXAnchor),
            localContent.centerYAnchor.constraint(
                equalTo: view.safeAreaLayoutGuide.topAnchor,
                constant: HomeHeaderLayout.areaHeight / 2
            ),
            localContent.leadingAnchor.constraint(greaterThanOrEqualTo: transferLocalHeader.leadingAnchor, constant: 12),
            localContent.trailingAnchor.constraint(lessThanOrEqualTo: transferLocalHeader.trailingAnchor, constant: -12),
            fileContent.centerXAnchor.constraint(equalTo: transferFileHeader.centerXAnchor),
            fileContent.centerYAnchor.constraint(
                equalTo: view.safeAreaLayoutGuide.topAnchor,
                constant: HomeHeaderLayout.areaHeight / 2
            ),
            fileContent.leadingAnchor.constraint(greaterThanOrEqualTo: transferFileHeader.leadingAnchor, constant: 12),
            fileContent.trailingAnchor.constraint(lessThanOrEqualTo: transferFileHeader.trailingAnchor, constant: -12),
        ])
        updateTransferOptionsButton()
    }

    private func configureTransferHeaderLabel(_ label: UILabel, text: String, color: UIColor) {
        label.text = text
        label.font = .systemFont(ofSize: 15, weight: .semibold)
        label.textColor = color
        label.textAlignment = .center
        label.numberOfLines = 1
        label.lineBreakMode = .byTruncatingTail
    }

    private func configureTransferHeaderAction(
        _ button: UIButton,
        title: String,
        symbolName: String,
        color: UIColor
    ) {
        var configuration = UIButton.Configuration.plain()
        configuration.title = title
        configuration.image = UIImage(
            systemName: symbolName,
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 11)
        )
        configuration.imagePlacement = .trailing
        configuration.imagePadding = 4
        configuration.contentInsets = .zero
        configuration.baseForegroundColor = color
        configuration.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var outgoing = incoming
            outgoing.font = .systemFont(ofSize: 12, weight: .regular)
            return outgoing
        }
        button.configuration = configuration
        button.setContentHuggingPriority(.required, for: .horizontal)
    }

    private func makeTransferHeaderContent(
        titleLabel: UILabel,
        actionButton: UIButton
    ) -> UIStackView {
        let stack = UIStackView(arrangedSubviews: [titleLabel, actionButton])
        stack.axis = .vertical
        stack.alignment = .center
        stack.spacing = 8
        return stack
    }

    private func transferTopBarButtonConfiguration() -> UIButton.Configuration {
        var configuration = UIButton.Configuration.plain()
        configuration.contentInsets = .zero
        return configuration
    }

    private func updateTransferSelectionSummary(
        mediaItems: [MediaBrowserItem],
        files: [InboxTransferFile]
    ) {
        let photos = mediaItems.filter { !$0.isVideo }
        let videos = mediaItems.filter(\.isVideo)
        requestTransferPhotoSelectionDetails(for: mediaItems)

        updateTransferSelectionSummaryButton(
            transferPhotoSummaryButton,
            count: photos.count,
            fileSize: totalTransferSize(for: photos)
        )
        updateTransferSelectionSummaryButton(
            transferVideoSummaryButton,
            count: videos.count,
            fileSize: totalTransferSize(for: videos)
        )
        updateTransferSelectionSummaryButton(
            transferFileSummaryButton,
            count: files.count,
            fileSize: totalKnownSize(files.map(\.fileSize))
        )
        transferPhotoSummaryButton.isHidden = photos.isEmpty
        transferVideoSummaryButton.isHidden = videos.isEmpty
        transferFileSummaryButton.isHidden = files.isEmpty
        updateSelectionActivityCategoryButton(
            selectionActivityPhotoButton,
            count: photos.count,
            symbolName: "photo"
        )
        updateSelectionActivityCategoryButton(
            selectionActivityVideoButton,
            count: videos.count,
            symbolName: "video"
        )
        updateSelectionActivityCategoryButton(
            selectionActivityFileButton,
            count: files.count,
            symbolName: "doc"
        )
    }

    private func requestTransferPhotoSelectionDetails(for items: [MediaBrowserItem]) {
        let localIdentifiers = items.compactMap(\.localIdentifier).sorted()
        let key = TransferPhotoSummaryRequestKey(
            localIdentifiers: localIdentifiers,
            options: inboxTransferOptions
        )
        guard key != transferPhotoSummaryRequestKey else { return }

        let previousKey = transferPhotoSummaryRequestKey
        transferPhotoSummaryRequestKey = key
        transferPhotoSummaryTask?.cancel()
        if previousKey?.options == key.options {
            transferPhotoSelectionDetails = transferPhotoSelectionDetails.filter {
                localIdentifiers.contains($0.key)
            }
        } else {
            transferPhotoSelectionDetails.removeAll()
        }
        guard !localIdentifiers.isEmpty else {
            transferPhotoSummaryTask = nil
            return
        }

        transferPhotoSummaryTask = Task { @MainActor [weak self] in
            let details = await withCancellableDetachedValue(priority: .userInitiated) {
                InboxTransferService.photoSelectionDetails(
                    localIdentifiers: Set(localIdentifiers),
                    options: key.options
                )
            }
            guard let self,
                  !Task.isCancelled,
                  self.transferPhotoSummaryRequestKey == key else { return }
            self.transferPhotoSummaryTask = nil
            self.transferPhotoSelectionDetails = Dictionary(
                uniqueKeysWithValues: details.map { ($0.localIdentifier, $0) }
            )
            self.recomputeBatchBar()
        }
    }

    private func totalTransferSize(for items: [MediaBrowserItem]) -> Int64? {
        totalKnownSize(items.map { item in
            item.localIdentifier.flatMap { transferPhotoSelectionDetails[$0]?.fileSize }
        })
    }

    private func totalKnownSize(_ values: [Int64?]) -> Int64? {
        guard !values.isEmpty, values.allSatisfy({ $0 != nil }) else { return nil }
        var total: Int64 = 0
        for value in values {
            guard let value else { return nil }
            let (sum, overflowed) = total.addingReportingOverflow(value)
            guard !overflowed else { return nil }
            total = sum
        }
        return total
    }

    private func transferMediaSelectionPopoverItems(
        for group: TransferSelectionGroup,
        items: [MediaBrowserItem]
    ) -> [MediaDropSelectionPopoverItem] {
        let symbolName = group == .videos ? "video" : "photo"
        return items.map { item in
            let detail = item.localIdentifier.flatMap { transferPhotoSelectionDetails[$0] }
            let title = detail?.preferredName ?? Date(
                timeIntervalSince1970: TimeInterval(item.creationDateMs) / 1_000
            ).formatted(date: .abbreviated, time: .shortened)
            return MediaDropSelectionPopoverItem(
                title: title,
                subtitle: detail?.fileSize.map(Self.formattedFileSize),
                image: UIImage(systemName: symbolName),
                thumbnailProvider: { [source] in
                    await source.thumbnail(for: item)
                },
                onOpen: { [weak self] in
                    guard let indexPath = self?.dataSource?.indexPath(for: item.id) else { return }
                    self?.presentViewer(at: indexPath)
                }
            ) { [weak self] in
                self?.setSelected(false, for: item.id)
            }
        }
    }

    private func transferFileSelectionPopoverItems(
        files: [InboxTransferFile]
    ) -> [MediaDropSelectionPopoverItem] {
        files.map { file in
            MediaDropSelectionPopoverItem(
                title: file.preferredName,
                subtitle: file.fileSize.map(Self.formattedFileSize),
                image: MediaDropFileIcon.image(for: file),
                thumbnailProvider: nil,
                onOpen: nil
            ) { [weak self] in
                self?.transferFileSelection.remove(file)
            }
        }
    }

    private static func formattedFileSize(_ fileSize: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: fileSize, countStyle: .file)
    }

    @objc
    private func showTransferPhotoSelectionPopover(_ sender: UIButton) {
        showTransferMediaSelectionPopover(for: .photos, sourceView: sender)
    }

    @objc
    private func showTransferVideoSelectionPopover(_ sender: UIButton) {
        showTransferMediaSelectionPopover(for: .videos, sourceView: sender)
    }

    @objc
    private func showTransferFileSelectionPopover(_ sender: UIButton) {
        let files = transferFileSelection.files
        showTransferSelectionPopover(
            title: String(localized: "transfer.source.files"),
            items: transferFileSelectionPopoverItems(files: files),
            sourceView: sender
        ) { [weak self] in
            self?.transferFileSelection.removeAll()
        }
    }

    private func showTransferMediaSelectionPopover(
        for group: TransferSelectionGroup,
        sourceView: UIView
    ) {
        let items = selectedMediaItems().filter {
            group == .videos ? $0.isVideo : !$0.isVideo
        }
        let title = group == .videos
            ? String(localized: "transfer.source.videos")
            : String(localized: "transfer.source.photos")
        showTransferSelectionPopover(
            title: title,
            items: transferMediaSelectionPopoverItems(for: group, items: items),
            sourceView: sourceView
        ) { [weak self] in
            guard let self else { return }
            self.applySelectedItemIDs(
                self.selectedItemIDs.subtracting(Set(items.map(\.id)))
            )
        }
    }

    private func showTransferSelectionPopover(
        title: String,
        items: [MediaDropSelectionPopoverItem],
        sourceView: UIView,
        onDeselectAll: @escaping @MainActor () -> Void
    ) {
        guard !items.isEmpty, presentedViewController == nil else { return }
        let controller = MediaDropSelectionPopoverViewController(
            title: title,
            items: items,
            onDeselectAll: onDeselectAll
        )
        let navigationController = UINavigationController(rootViewController: controller)
        navigationController.modalPresentationStyle = .popover
        navigationController.preferredContentSize = CGSize(
            width: min(320, max(280, view.bounds.width - 48)),
            height: controller.preferredHeight
        )
        if let popover = navigationController.popoverPresentationController {
            popover.sourceView = sourceView
            popover.sourceRect = sourceView.bounds
            popover.permittedArrowDirections = [.down, .up]
            popover.delegate = controller
            popover.backgroundColor = .appPaper
        }
        present(navigationController, animated: true)
    }

    @objc
    private func openTransferFilePicker() {
        guard !isAnyActionRunning, !transferFileSelection.isWorking else { return }
        let picker = UIDocumentPickerViewController(
            forOpeningContentTypes: [.content, .data],
            asCopy: true
        )
        picker.delegate = self
        picker.allowsMultipleSelection = true
        picker.shouldShowFileExtensions = true
        present(picker, animated: ConsideringUser.animated)
    }

    private func presentTransferFileImportError(_ error: Error) {
        if let picker = presentedViewController as? UIDocumentPickerViewController {
            picker.dismiss(animated: ConsideringUser.animated) { [weak self] in
                self?.presentTransferFileImportError(error)
            }
            return
        }
        guard presentedViewController == nil else { return }
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: error.localizedDescription,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        present(alert, animated: ConsideringUser.animated)
    }

    func refreshTransferAccessPresentation() {
        guard isContentActive, selectionAction != nil else { return }
        recomputeBatchBar()
    }

    private func makeTransferOptionsMenu() -> UIMenu {
        let defaults = InboxTransferOptions.storedValue
        let reset = UIAction(
            title: String(localized: "transfer.filter.resetDefaults"),
            image: UIImage(systemName: "arrow.counterclockwise"),
            attributes: inboxTransferOptions == defaults ? .disabled : []
        ) { [weak self] _ in
            guard let self else { return }
            self.inboxTransferOptions = InboxTransferOptions.storedValue
            self.updateTransferOptionsButton()
            self.recomputeBatchBar()
        }
        return UIMenu(children: [
            makeTransferOptionMenu(
                title: String(localized: "transfer.filter.includeLiveVideo"),
                image: UIImage(systemName: "livephoto"),
                option: .includeLivePhotoVideo
            ),
            makeTransferOptionMenu(
                title: String(localized: "transfer.filter.originalPhoto"),
                image: UIImage(systemName: "photo"),
                option: .useOriginalEditedPhoto
            ),
            makeTransferOptionMenu(
                title: String(localized: "transfer.filter.originalVideo"),
                image: UIImage(systemName: "video"),
                option: .useOriginalEditedVideo
            ),
            makeTransferOptionMenu(
                title: String(localized: "transfer.filter.removeLocation"),
                image: UIImage(systemName: "location.slash"),
                option: .removeLocationMetadata
            ),
            UIMenu(title: "", options: .displayInline, children: [reset]),
        ])
    }

    private func makeTransferOptionMenu(
        title: String,
        image: UIImage?,
        option: InboxTransferOptions.Option
    ) -> UIMenu {
        let isEnabled = inboxTransferOptions.contains(option)
        let subtitle = isEnabled
            ? String(localized: "settings.common.enable")
            : String(localized: "transfer.settings.option.off")
        let enable = UIAction(
            title: String(localized: "settings.common.enable"),
            state: isEnabled ? .on : .off
        ) { [weak self] _ in
            self?.setTransferOption(option, isEnabled: true)
        }
        let disable = UIAction(
            title: String(localized: "transfer.settings.option.off"),
            state: isEnabled ? .off : .on
        ) { [weak self] _ in
            self?.setTransferOption(option, isEnabled: false)
        }
        return UIMenu(
            title: title,
            subtitle: subtitle,
            image: image,
            children: [enable, disable]
        )
    }

    private func setTransferOption(_ option: InboxTransferOptions.Option, isEnabled: Bool) {
        guard inboxTransferOptions.contains(option) != isEnabled else { return }
        inboxTransferOptions = inboxTransferOptions.updating(option, isEnabled: isEnabled)
        updateTransferOptionsButton()
        recomputeBatchBar()
    }

    private func updateTransferOptionsButton() {
        guard selectionAction != nil else { return }
        let usesDefaults = inboxTransferOptions == InboxTransferOptions.storedValue
        var configuration = transferOptionsButton.configuration
        configuration?.baseForegroundColor = usesDefaults
            ? .materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100)
            : .appTint
        transferOptionsButton.configuration = configuration
        transferOptionsButton.menu = makeTransferOptionsMenu()
        transferOptionsButton.accessibilityLabel = String(localized: "transfer.header.options")
        transferOptionsButton.accessibilityValue = inboxTransferOptions.getName()
        transferChooseFilesButton.accessibilityLabel = String(localized: "transfer.files.select")
    }

    private func configureBatchBar() {
        batchBarContainer.isHidden = true
        batchBarContainer.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(batchBarContainer)
        if let selectionAction {
            let selectionPanelSurface = UIView()
            let transferSelectionSummaryStack = UIStackView()
            batchBarContainer.isHidden = false
            let iconConfiguration = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
            var configuration = UIButton.Configuration.filled()
            configuration.image = UIImage(systemName: selectionAction.symbolName, withConfiguration: iconConfiguration)
            configuration.cornerStyle = .capsule
            configuration.baseBackgroundColor = .systemGray3
            configuration.baseForegroundColor = .label
            configuration.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
            selectionActionButton.configuration = configuration
            selectionActionButton.showsMenuAsPrimaryAction = true
            selectionActionButton.accessibilityLabel = selectionAction.buttonTitle
            selectionActionButton.translatesAutoresizingMaskIntoConstraints = false

            configureTransferSelectionSummaryButton(
                transferPhotoSummaryButton,
                symbolName: "photo",
                accessibilityLabel: String(localized: "transfer.source.photos")
            )
            transferPhotoSummaryButton.addTarget(
                self,
                action: #selector(showTransferPhotoSelectionPopover(_:)),
                for: .touchUpInside
            )
            configureTransferSelectionSummaryButton(
                transferVideoSummaryButton,
                symbolName: "video",
                accessibilityLabel: String(localized: "transfer.source.videos")
            )
            transferVideoSummaryButton.addTarget(
                self,
                action: #selector(showTransferVideoSelectionPopover(_:)),
                for: .touchUpInside
            )
            configureTransferSelectionSummaryButton(
                transferFileSummaryButton,
                symbolName: "doc",
                accessibilityLabel: String(localized: "transfer.source.files")
            )
            transferFileSummaryButton.addTarget(
                self,
                action: #selector(showTransferFileSelectionPopover(_:)),
                for: .touchUpInside
            )
            transferSelectionSummaryStack.axis = .horizontal
            transferSelectionSummaryStack.alignment = .center
            transferSelectionSummaryStack.spacing = 10
            transferSelectionSummaryStack.addArrangedSubview(transferPhotoSummaryButton)
            transferSelectionSummaryStack.addArrangedSubview(transferVideoSummaryButton)
            transferSelectionSummaryStack.addArrangedSubview(transferFileSummaryButton)
            transferSelectionSummaryStack.translatesAutoresizingMaskIntoConstraints = false
            transferSelectionSummaryScrollView.showsHorizontalScrollIndicator = false
            transferSelectionSummaryScrollView.alwaysBounceHorizontal = true
            transferSelectionSummaryScrollView.delaysContentTouches = false
            transferSelectionSummaryScrollView.addSubview(transferSelectionSummaryStack)
            transferSelectionSummaryScrollView.translatesAutoresizingMaskIntoConstraints = false

            selectionPanelSurface.backgroundColor = .appPaper
            selectionPanelSurface.translatesAutoresizingMaskIntoConstraints = false
            batchBarContainer.addSubview(selectionPanelSurface)
            selectionPanelSurface.addSubview(transferSelectionSummaryScrollView)
            selectionPanelSurface.addSubview(selectionActionButton)

            selectionActivityPanel.backgroundColor = .clear
            selectionActivityPanel.isHidden = true
            selectionActivityPanel.translatesAutoresizingMaskIntoConstraints = false
            selectionPanelSurface.addSubview(selectionActivityPanel)

            let separator = UIView()
            separator.backgroundColor = .separator
            separator.translatesAutoresizingMaskIntoConstraints = false
            selectionPanelSurface.addSubview(separator)

            configureSelectionActivityStatusButton()
            configureSelectionActivityCategoryButton(selectionActivityPhotoButton, symbolName: "photo")
            configureSelectionActivityCategoryButton(selectionActivityVideoButton, symbolName: "video")
            configureSelectionActivityCategoryButton(selectionActivityFileButton, symbolName: "doc")
            configureSelectionActivityControlButtons()

            let categorySpacer = UIView()
            categorySpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
            let activityCategoryRow = UIStackView(arrangedSubviews: [
                selectionActivityPhotoButton,
                selectionActivityVideoButton,
                selectionActivityFileButton,
                categorySpacer,
            ])
            activityCategoryRow.axis = .horizontal
            activityCategoryRow.alignment = .fill
            activityCategoryRow.spacing = 10

            let activityInfoStack = UIStackView(arrangedSubviews: [
                activityCategoryRow,
                selectionActivityStatusButton,
            ])
            activityInfoStack.axis = .vertical
            activityInfoStack.alignment = .fill
            activityInfoStack.spacing = 6
            activityInfoStack.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

            let activityStack = UIStackView(arrangedSubviews: [
                activityInfoStack,
                selectionActivityStopButton,
                selectionActivityPauseButton,
            ])
            activityStack.axis = .horizontal
            activityStack.alignment = .center
            activityStack.spacing = 10
            activityStack.setCustomSpacing(12, after: selectionActivityStopButton)
            activityStack.translatesAutoresizingMaskIntoConstraints = false
            selectionActivityPanel.addSubview(activityStack)
            NSLayoutConstraint.activate([
                batchBarContainer.leadingAnchor.constraint(equalTo: view.leadingAnchor),
                batchBarContainer.trailingAnchor.constraint(equalTo: view.trailingAnchor),
                selectionPanelSurface.leadingAnchor.constraint(equalTo: batchBarContainer.leadingAnchor),
                selectionPanelSurface.trailingAnchor.constraint(equalTo: batchBarContainer.trailingAnchor),
                selectionPanelSurface.topAnchor.constraint(equalTo: batchBarContainer.topAnchor),
                selectionPanelSurface.bottomAnchor.constraint(equalTo: batchBarContainer.bottomAnchor),
                separator.topAnchor.constraint(equalTo: selectionPanelSurface.topAnchor),
                separator.leadingAnchor.constraint(equalTo: selectionPanelSurface.leadingAnchor),
                separator.trailingAnchor.constraint(equalTo: selectionPanelSurface.trailingAnchor),
                separator.heightAnchor.constraint(equalToConstant: 0.5),
                transferSelectionSummaryScrollView.leadingAnchor.constraint(equalTo: batchBarContainer.leadingAnchor, constant: 18),
                transferSelectionSummaryScrollView.topAnchor.constraint(equalTo: batchBarContainer.topAnchor, constant: 12),
                transferSelectionSummaryScrollView.bottomAnchor.constraint(
                    equalTo: batchBarContainer.safeAreaLayoutGuide.bottomAnchor,
                    constant: -12
                ),
                transferSelectionSummaryScrollView.heightAnchor.constraint(equalToConstant: 52),
                transferSelectionSummaryScrollView.trailingAnchor.constraint(
                    equalTo: selectionActionButton.leadingAnchor,
                    constant: -10
                ),
                transferSelectionSummaryStack.leadingAnchor.constraint(equalTo: transferSelectionSummaryScrollView.contentLayoutGuide.leadingAnchor),
                transferSelectionSummaryStack.trailingAnchor.constraint(equalTo: transferSelectionSummaryScrollView.contentLayoutGuide.trailingAnchor),
                transferSelectionSummaryStack.topAnchor.constraint(equalTo: transferSelectionSummaryScrollView.contentLayoutGuide.topAnchor),
                transferSelectionSummaryStack.bottomAnchor.constraint(equalTo: transferSelectionSummaryScrollView.contentLayoutGuide.bottomAnchor),
                transferSelectionSummaryStack.heightAnchor.constraint(equalTo: transferSelectionSummaryScrollView.frameLayoutGuide.heightAnchor),
                selectionActionButton.trailingAnchor.constraint(equalTo: batchBarContainer.trailingAnchor, constant: -12),
                selectionActionButton.centerYAnchor.constraint(equalTo: transferSelectionSummaryScrollView.centerYAnchor),
                selectionActionButton.widthAnchor.constraint(equalToConstant: 72),
                selectionActionButton.heightAnchor.constraint(equalToConstant: 36),

                selectionActivityPanel.leadingAnchor.constraint(equalTo: batchBarContainer.leadingAnchor),
                selectionActivityPanel.trailingAnchor.constraint(equalTo: batchBarContainer.trailingAnchor),
                selectionActivityPanel.topAnchor.constraint(equalTo: batchBarContainer.topAnchor),
                selectionActivityPanel.bottomAnchor.constraint(
                    equalTo: batchBarContainer.safeAreaLayoutGuide.bottomAnchor
                ),
                selectionActivityPanel.heightAnchor.constraint(greaterThanOrEqualToConstant: 58),

                activityStack.leadingAnchor.constraint(equalTo: selectionActivityPanel.leadingAnchor, constant: 18),
                activityStack.trailingAnchor.constraint(equalTo: selectionActivityPanel.trailingAnchor, constant: -12),
                activityStack.topAnchor.constraint(equalTo: selectionActivityPanel.topAnchor, constant: 12),
                activityStack.bottomAnchor.constraint(equalTo: selectionActivityPanel.bottomAnchor, constant: -12),
                activityInfoStack.heightAnchor.constraint(equalToConstant: 52),
                selectionActivityPauseButton.widthAnchor.constraint(equalToConstant: 72),
                selectionActivityPauseButton.heightAnchor.constraint(equalToConstant: 36),
                selectionActivityStopButton.widthAnchor.constraint(equalToConstant: 72),
                selectionActivityStopButton.heightAnchor.constraint(equalToConstant: 36),
            ])
            transferPanelShownConstraint = batchBarContainer.bottomAnchor.constraint(equalTo: view.bottomAnchor)
            transferPanelHiddenConstraint = batchBarContainer.topAnchor.constraint(equalTo: view.bottomAnchor)
            transferPanelHiddenConstraint?.isActive = true
            return
        }

        let blur = UIVisualEffectView(effect: UIBlurEffect(style: .systemThinMaterial))
        blur.translatesAutoresizingMaskIntoConstraints = false
        batchBarContainer.addSubview(blur)
        batchBar.translatesAutoresizingMaskIntoConstraints = false
        batchBarContainer.addSubview(batchBar)
        NSLayoutConstraint.activate([
            batchBarContainer.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            batchBarContainer.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            batchBarContainer.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            blur.topAnchor.constraint(equalTo: batchBarContainer.topAnchor),
            blur.bottomAnchor.constraint(equalTo: batchBarContainer.bottomAnchor),
            blur.leadingAnchor.constraint(equalTo: batchBarContainer.leadingAnchor),
            blur.trailingAnchor.constraint(equalTo: batchBarContainer.trailingAnchor),
            batchBar.leadingAnchor.constraint(equalTo: batchBarContainer.leadingAnchor),
            batchBar.trailingAnchor.constraint(equalTo: batchBarContainer.trailingAnchor),
            batchBar.topAnchor.constraint(equalTo: batchBarContainer.topAnchor, constant: 6),
            batchBar.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -6),
            batchBar.heightAnchor.constraint(equalToConstant: Self.batchBarHeight),
        ])
    }

    private func configureTransferSelectionSummaryButton(
        _ button: UIButton,
        symbolName: String,
        accessibilityLabel: String
    ) {
        var configuration = UIButton.Configuration.plain()
        configuration.image = UIImage(
            systemName: symbolName,
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 15, weight: .semibold)
        )
        configuration.imagePadding = 6
        configuration.title = "0"
        configuration.subtitle = "-"
        configuration.titleAlignment = .leading
        configuration.contentInsets = .init(top: 6, leading: 0, bottom: 6, trailing: 0)
        configuration.baseForegroundColor = .appTint
        configuration.subtitleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .preferredFont(forTextStyle: .caption1)
            return attributes
        }
        button.configuration = configuration
        button.contentHorizontalAlignment = .leading
        button.setContentHuggingPriority(.required, for: .horizontal)
        button.setContentCompressionResistancePriority(.required, for: .horizontal)
        button.accessibilityLabel = accessibilityLabel
    }

    private func updateTransferSelectionSummaryButton(
        _ button: UIButton,
        count: Int,
        fileSize: Int64?
    ) {
        guard var configuration = button.configuration else { return }
        configuration.title = String(count)
        configuration.subtitle = fileSize.map {
            ByteCountFormatter.string(fromByteCount: $0, countStyle: .file)
        } ?? "-"
        button.configuration = configuration
        button.isEnabled = count > 0
        button.accessibilityValue = ListFormatter.localizedString(byJoining: [
            String(count),
            configuration.subtitle ?? "-",
        ])
    }

    private func setBatchBarVisible(_ visible: Bool, animated: Bool) {
        guard selectionAction != nil else {
            batchBarContainer.isHidden = !visible
            let inset = visible ? Self.batchBarHeight + 12 : 0
            collectionView.contentInset.bottom = inset
            collectionView.verticalScrollIndicatorInsets.bottom = inset
            return
        }
        guard visible != isTransferPanelVisible else { return }
        view.layoutIfNeeded()
        isTransferPanelVisible = visible
        if visible {
            transferPanelHiddenConstraint?.isActive = false
            transferPanelShownConstraint?.isActive = true
        } else {
            transferPanelShownConstraint?.isActive = false
            transferPanelHiddenConstraint?.isActive = true
        }
        onTransferPanelVisibilityChanged?(visible)

        let animations = { [weak self] in
            guard let self else { return }
            self.parent?.view.layoutIfNeeded()
            self.view.layoutIfNeeded()
        }
        if animated {
            UIView.animate(
                withDuration: visible ? 0.3 : 0.25,
                delay: 0,
                options: visible ? .curveEaseOut : .curveEaseIn,
                animations: animations
            )
        } else {
            animations()
        }
    }

    private func updateSelectionActionButtonAppearance(isAvailable: Bool) {
        guard var configuration = selectionActionButton.configuration else { return }
        configuration.baseBackgroundColor = isAvailable ? .appTint : .systemGray3
        configuration.baseForegroundColor = isAvailable
            ? .materialOnPrimary(dark: .Material.Green._800)
            : .label
        selectionActionButton.configuration = configuration
    }

    private func configureSelectionActivityStatusButton() {
        var configuration = UIButton.Configuration.plain()
        configuration.title = String(localized: "panel.log")
        configuration.image = UIImage(
            systemName: "chevron.right",
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        )
        configuration.imagePlacement = .trailing
        configuration.imagePadding = 4
        configuration.titleAlignment = .leading
        configuration.baseForegroundColor = .materialOnSurfaceVariant(
            light: .Material.BlueGrey._700,
            dark: .Material.BlueGrey._200
        )
        configuration.titleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .systemFont(ofSize: 15, weight: .semibold)
            return attributes
        }
        configuration.contentInsets = .zero
        configuration.cornerStyle = .fixed
        configuration.background = .clear()
        selectionActivityStatusButton.configuration = configuration
        selectionActivityStatusButton.contentHorizontalAlignment = .leading
        selectionActivityStatusButton.isUserInteractionEnabled = false
        selectionActivityStatusButton.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
    }

    private func configureSelectionActivityCategoryButton(
        _ button: UIButton,
        symbolName: String
    ) {
        var configuration = UIButton.Configuration.plain()
        configuration.image = UIImage(
            systemName: symbolName,
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        )
        configuration.imagePadding = 3
        configuration.titleAlignment = .leading
        configuration.contentInsets = .init(top: 2, leading: 0, bottom: 2, trailing: 2)
        configuration.titleTextAttributesTransformer = .init {
            var attributes = $0
            attributes.font = .monospacedDigitSystemFont(ofSize: 13, weight: .semibold)
            return attributes
        }
        configuration.baseForegroundColor = .appTint
        button.configuration = configuration
        button.contentHorizontalAlignment = .leading
        button.isUserInteractionEnabled = false
        button.setContentHuggingPriority(.required, for: .horizontal)
    }

    private func updateSelectionActivityCategoryButton(
        _ button: UIButton,
        count: Int,
        symbolName: String
    ) {
        button.isHidden = count == 0
        guard count > 0, var configuration = button.configuration else { return }
        configuration.image = UIImage(
            systemName: symbolName,
            withConfiguration: UIImage.SymbolConfiguration(pointSize: 10, weight: .semibold)
        )
        configuration.title = String(count)
        button.configuration = configuration
    }

    private func configureSelectionActivityControlButtons() {
        selectionActivityPauseButton.addTarget(
            self,
            action: #selector(toggleSelectionActionPause),
            for: .touchUpInside
        )
        selectionActivityStopButton.addTarget(
            self,
            action: #selector(stopSelectionAction),
            for: .touchUpInside
        )
        updateSelectionActivityPanel()
    }

    private func selectionActivityControlConfiguration(
        symbolName: String?,
        showsActivityIndicator: Bool = false,
        light: UIColor,
        dark: UIColor,
        onDark: UIColor
    ) -> UIButton.Configuration {
        var configuration = UIButton.Configuration.filled()
        if let symbolName {
            configuration.image = UIImage(
                systemName: symbolName,
                withConfiguration: UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
            )
        }
        configuration.showsActivityIndicator = showsActivityIndicator
        configuration.cornerStyle = .capsule
        configuration.baseBackgroundColor = .materialPrimary(light: light, dark: dark)
        configuration.baseForegroundColor = .materialOnPrimary(dark: onDark)
        configuration.contentInsets = .init(top: 8, leading: 14, bottom: 8, trailing: 14)
        return configuration
    }

    private func updateSelectionActivityPanel() {
        let preparingStatus = String(localized: "transfer.progress.preparing")
        var statusText = preparingStatus
        var pauseSymbol: String? = "pause.fill"
        var pauseShowsSpinner = false
        var pauseIsEnabled = false
        var pauseUsesResumePalette = false
        var pauseUsesFailurePalette = false
        var pauseAccessibilityLabel = String(localized: "transfer.control.pause")
        var stopShowsSpinner = false
        var stopIsEnabled = false
        switch selectionActionUIState {
        case .idle:
            pauseSymbol = nil
            pauseShowsSpinner = true
            pauseUsesResumePalette = true
        case .running(let status, let pause):
            switch pause {
            case .active:
                statusText = status
                if status == preparingStatus {
                    pauseSymbol = nil
                    pauseShowsSpinner = true
                    pauseUsesResumePalette = true
                } else {
                    pauseIsEnabled = true
                }
            case .pausing:
                statusText = String(localized: "transfer.progress.pausing")
                pauseSymbol = nil
                pauseShowsSpinner = true
            case .paused:
                statusText = String(localized: "transfer.progress.paused")
                pauseSymbol = "play.fill"
                pauseIsEnabled = true
                pauseUsesResumePalette = true
                pauseAccessibilityLabel = String(localized: "transfer.control.resume")
            }
            stopIsEnabled = true
        case .stopping:
            statusText = String(localized: "transfer.progress.stopping")
            stopShowsSpinner = true
        case .terminal(let terminalStatus, let kind):
            pauseIsEnabled = true
            switch kind {
            case .completed:
                statusText = String(localized: "home.execution.completed")
                pauseSymbol = "checkmark"
                pauseUsesResumePalette = true
                pauseAccessibilityLabel = String(localized: "home.execution.completed")
            case .failed:
                statusText = terminalStatus
                pauseSymbol = "xmark"
                pauseUsesFailurePalette = true
                pauseAccessibilityLabel = String(localized: "common.close")
            }
        }
        var statusConfiguration = selectionActivityStatusButton.configuration ?? .plain()
        let trimmedStatus = statusText.trimmingCharacters(in: .whitespacesAndNewlines)
        statusConfiguration.title = trimmedStatus.isEmpty ? String(localized: "panel.log") : trimmedStatus
        selectionActivityStatusButton.configuration = statusConfiguration
        let pauseLight: UIColor = pauseUsesFailurePalette
            ? .Material.BlueGrey._600
            : (pauseUsesResumePalette ? .Material.Green._600 : .Material.Orange._600)
        let pauseDark: UIColor = pauseUsesFailurePalette
            ? .Material.BlueGrey._200
            : (pauseUsesResumePalette ? .Material.Green._200 : .Material.Orange._200)
        let pauseOnDark: UIColor = pauseUsesFailurePalette
            ? .Material.BlueGrey._800
            : (pauseUsesResumePalette ? .Material.Green._800 : .Material.Orange._800)
        selectionActivityPauseButton.configuration = selectionActivityControlConfiguration(
            symbolName: pauseSymbol,
            showsActivityIndicator: pauseShowsSpinner,
            light: pauseLight,
            dark: pauseDark,
            onDark: pauseOnDark
        )
        selectionActivityPauseButton.accessibilityLabel = pauseAccessibilityLabel
        selectionActivityPauseButton.isEnabled = pauseIsEnabled
        selectionActivityStopButton.configuration = selectionActivityControlConfiguration(
            symbolName: stopShowsSpinner ? nil : "stop.fill",
            showsActivityIndicator: stopShowsSpinner,
            light: .Material.Red._600,
            dark: .Material.Red._200,
            onDark: .Material.Red._800
        )
        selectionActivityStopButton.accessibilityLabel = String(localized: "common.stop")
        selectionActivityStopButton.isEnabled = stopIsEnabled
    }

    private func setSelectionActivityPanelVisible(_ visible: Bool) {
        guard showsSelectionActivityPanel != visible else {
            if selectionActivityTransitionAnimator == nil {
                transferSelectionSummaryScrollView.isHidden = visible
                selectionActionButton.isHidden = visible
                selectionActivityPanel.isHidden = !visible
            }
            return
        }
        showsSelectionActivityPanel = visible
        selectionActivityTransitionAnimator?.stopAnimation(true)
        selectionActivityTransitionAnimator = nil

        guard ConsideringUser.animated, view.window != nil else {
            transferSelectionSummaryScrollView.isHidden = visible
            transferSelectionSummaryScrollView.alpha = 1
            selectionActionButton.isHidden = visible
            selectionActionButton.alpha = 1
            selectionActivityPanel.isHidden = !visible
            selectionActivityPanel.alpha = 1
            return
        }

        transferSelectionSummaryScrollView.isHidden = false
        selectionActionButton.isHidden = false
        selectionActivityPanel.isHidden = false
        transferSelectionSummaryScrollView.alpha = visible ? 1 : 0
        selectionActionButton.alpha = visible ? 1 : 0
        selectionActivityPanel.alpha = visible ? 0 : 1

        let animator = UIViewPropertyAnimator(duration: 0.25, curve: .easeInOut) { [weak self] in
            guard let self else { return }
            self.transferSelectionSummaryScrollView.alpha = visible ? 0 : 1
            self.selectionActionButton.alpha = visible ? 0 : 1
            self.selectionActivityPanel.alpha = visible ? 1 : 0
        }
        animator.addCompletion { [weak self] position in
            guard let self, position == .end, self.showsSelectionActivityPanel == visible else { return }
            self.transferSelectionSummaryScrollView.isHidden = visible
            self.transferSelectionSummaryScrollView.alpha = 1
            self.selectionActionButton.isHidden = visible
            self.selectionActionButton.alpha = 1
            self.selectionActivityPanel.isHidden = !visible
            self.selectionActivityPanel.alpha = 1
            self.selectionActivityTransitionAnimator = nil
        }
        selectionActivityTransitionAnimator = animator
        animator.startAnimation()
    }

    @objc
    private func toggleSelectionActionPause() {
        if case .terminal(_, let kind) = selectionActionUIState {
            selectionActionUIState = .idle
            if kind == .completed {
                selectedItemIDs.removeAll()
                transferFileSelection.resetAfterSuccessfulTransfer()
                refreshVisibleSelectionOverlays()
            }
            recomputeBatchBar()
            flushDeferredReloadIfNeeded()
            return
        }
        guard let activity = selectionActionActivity,
              case .running(let status, let phase) = selectionActionUIState else { return }
        let shouldPause = phase == .active
        selectionActionUIState = .running(
            status: status,
            pause: shouldPause ? .pausing : .active
        )
        updateSelectionActivityPanel()
        Task { await activity.pauseGate.setPaused(shouldPause) }
    }

    @objc
    private func stopSelectionAction() {
        guard case .running(let status, _) = selectionActionUIState else { return }
        selectionActionUIState = .stopping(status: status)
        updateSelectionActivityPanel()
        if let activity = selectionActionActivity {
            Task { await activity.pauseGate.setPaused(false) }
        }
        selectionActionTask?.cancel()
    }

    private func updateSelectBarButton() {
        if selectionAction != nil {
            updateTransferOptionsButton()
            return
        }
        if isSelecting {
            navigationItem.rightBarButtonItem = UIBarButtonItem(
                barButtonSystemItem: .cancel,
                target: self,
                action: #selector(exitSelection)
            )
        } else if !months.isEmpty {
            navigationItem.rightBarButtonItem = UIBarButtonItem(title: String(localized: "mediaBrowser.select"), style: .plain, target: self, action: #selector(enterSelection))
        } else {
            navigationItem.rightBarButtonItem = nil
        }
    }

    @objc private func enterSelection() {
        guard !isSelecting, !months.isEmpty || selectionAction != nil else { return }
        isSelecting = true
        selectedItemIDs.removeAll()
        setBatchBarVisible(selectionAction == nil, animated: false)
        updateSelectBarButton()
        recomputeBatchBar()
        refreshVisibleSelectionOverlays()
    }

    @objc private func exitSelection() {
        guard isSelecting else { return }
        isSelecting = false
        isNativeMultipleSelectionInteraction = false
        endSelectionDrag()
        selectedItemIDs.removeAll()
        collectionView.indexPathsForSelectedItems?.forEach {
            collectionView.deselectItem(at: $0, animated: false)
        }
        setBatchBarVisible(false, animated: false)
        updateSelectBarButton()
        refreshVisibleSelectionOverlays()
        if selectionAction != nil { recomputeBatchBar() }
    }

    private func refreshVisibleSelectionOverlays() {
        synchronizeCollectionSelection()
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell,
                  let item = item(at: indexPath) else { continue }
            cell.setSelecting(isSelecting, selected: selectedItemIDs.contains(item.id))
        }
    }

    private func synchronizeCollectionSelection() {
        for indexPath in collectionView.indexPathsForSelectedItems ?? [] {
            guard isSelecting,
                  let item = item(at: indexPath),
                  selectedItemIDs.contains(item.id) else {
                collectionView.deselectItem(at: indexPath, animated: false)
                continue
            }
        }
        guard isSelecting else { return }
        for itemID in selectedItemIDs {
            guard let indexPath = dataSource?.indexPath(for: itemID) else { continue }
            collectionView.selectItem(at: indexPath, animated: false, scrollPosition: [])
        }
    }

    private func toggleSelectionFromButton(for itemID: MediaBrowserItemID) {
        guard !isAnyActionRunning else { return }
        if !isSelecting { enterSelection() }
        guard isSelecting else { return }
        setSelected(!selectedItemIDs.contains(itemID), for: itemID)
    }

    private func handleSelectionButtonDrag(
        _ gesture: UILongPressGestureRecognizer,
        startingAt itemID: MediaBrowserItemID
    ) {
        switch gesture.state {
        case .began:
            guard !isAnyActionRunning else { return }
            if !isSelecting { enterSelection() }
            guard isSelecting, let anchorIndex = browserSnapshot.index(of: itemID) else { return }
            selectionDragSelects = !selectedItemIDs.contains(itemID)
            selectionDragAnchorIndex = anchorIndex
            selectionDragBaselineIDs = selectedItemIDs
            activeSelectionDragGesture = gesture
            beginSelectionDragAutoscroll()
            updateSelectionDrag(through: itemID)
        case .changed:
            updateSelectionDrag(at: gesture.location(in: collectionView))
        case .ended, .cancelled, .failed:
            endSelectionDrag()
        default:
            break
        }
    }

    private func beginSelectionDragAutoscroll() {
        selectionDragDisplayLink?.invalidate()
        selectionDragDisplayLinkTimestamp = nil
        let displayLink = CADisplayLink(target: self, selector: #selector(selectionDragDisplayLinkFired(_:)))
        displayLink.add(to: .main, forMode: .common)
        selectionDragDisplayLink = displayLink
    }

    private func endSelectionDrag() {
        selectionDragDisplayLink?.invalidate()
        selectionDragDisplayLink = nil
        selectionDragDisplayLinkTimestamp = nil
        activeSelectionDragGesture = nil
        selectionDragSelects = nil
        selectionDragAnchorIndex = nil
        selectionDragBaselineIDs.removeAll()
        flushDeferredReloadIfNeeded()
    }

    @objc
    private func selectionDragDisplayLinkFired(_ displayLink: CADisplayLink) {
        guard let gesture = activeSelectionDragGesture,
              gesture.state == .began || gesture.state == .changed else {
            endSelectionDrag()
            return
        }
        defer { selectionDragDisplayLinkTimestamp = displayLink.timestamp }
        guard let previousTimestamp = selectionDragDisplayLinkTimestamp else { return }
        let point = gesture.location(in: collectionView)
        let viewportY = point.y - collectionView.bounds.minY
        let edgeZone: CGFloat = 64
        let maximumSpeed: CGFloat = 720
        let speed: CGFloat
        if viewportY < edgeZone {
            speed = -maximumSpeed * min(1, (edgeZone - viewportY) / edgeZone)
        } else if viewportY > collectionView.bounds.height - edgeZone {
            speed = maximumSpeed * min(
                1,
                (viewportY - (collectionView.bounds.height - edgeZone)) / edgeZone
            )
        } else {
            speed = 0
        }
        guard speed != 0 else { return }

        let elapsed = min(displayLink.timestamp - previousTimestamp, 1.0 / 20.0)
        let minimumY = -collectionView.adjustedContentInset.top
        let maximumY = max(
            minimumY,
            collectionView.contentSize.height
                - collectionView.bounds.height
                + collectionView.adjustedContentInset.bottom
        )
        let proposedY = collectionView.contentOffset.y + speed * elapsed
        let targetY = min(maximumY, max(minimumY, proposedY))
        guard targetY != collectionView.contentOffset.y else { return }
        collectionView.contentOffset.y = targetY
        collectionView.layoutIfNeeded()
        updateSelectionDrag(at: gesture.location(in: collectionView))
    }

    private func updateSelectionDrag(at point: CGPoint) {
        guard selectionDragSelects != nil else { return }
        if let indexPath = collectionView.indexPathForItem(at: point),
           let item = item(at: indexPath) {
            updateSelectionDrag(through: item.id)
            return
        }
        let candidates = collectionView.collectionViewLayout
            .layoutAttributesForElements(in: collectionView.bounds)?
            .filter { $0.representedElementCategory == .cell } ?? []
        guard let nearest = candidates.min(by: {
            squaredDistance(from: $0.center, to: point) < squaredDistance(from: $1.center, to: point)
        }), let item = item(at: nearest.indexPath) else { return }
        updateSelectionDrag(through: item.id)
    }

    private func squaredDistance(from lhs: CGPoint, to rhs: CGPoint) -> CGFloat {
        let dx = lhs.x - rhs.x
        let dy = lhs.y - rhs.y
        return dx * dx + dy * dy
    }

    private func updateSelectionDrag(through itemID: MediaBrowserItemID) {
        guard let selects = selectionDragSelects,
              let anchorIndex = selectionDragAnchorIndex,
              let currentIndex = browserSnapshot.index(of: itemID) else { return }
        let bounds = min(anchorIndex, currentIndex) ... max(anchorIndex, currentIndex)
        let rangeIDs = Set(bounds.compactMap { browserSnapshot.item(at: $0)?.id })
        var updatedIDs = selectionDragBaselineIDs
        if selects {
            updatedIDs.formUnion(rangeIDs)
        } else {
            updatedIDs.subtract(rangeIDs)
        }
        applySelectedItemIDs(updatedIDs)
    }

    private func applySelectedItemIDs(_ itemIDs: Set<MediaBrowserItemID>) {
        let changedIDs = selectedItemIDs.symmetricDifference(itemIDs)
        guard !changedIDs.isEmpty else { return }
        selectedItemIDs = itemIDs
        for itemID in changedIDs {
            guard let indexPath = dataSource?.indexPath(for: itemID) else { continue }
            let selected = itemIDs.contains(itemID)
            if selected {
                collectionView.selectItem(at: indexPath, animated: false, scrollPosition: [])
            } else {
                collectionView.deselectItem(at: indexPath, animated: false)
            }
            (collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell)?
                .setSelecting(true, selected: selected)
        }
        recomputeBatchBar()
    }

    private func setSelected(_ selected: Bool, for itemID: MediaBrowserItemID) {
        guard isSelecting, selectedItemIDs.contains(itemID) != selected else { return }
        var updatedIDs = selectedItemIDs
        if selected {
            updatedIDs.insert(itemID)
        } else {
            updatedIDs.remove(itemID)
        }
        applySelectedItemIDs(updatedIDs)
    }

    private func selectedMediaItems() -> [MediaBrowserItem] {
        guard !selectedItemIDs.isEmpty else { return [] }
        return selectedItemIDs
            .compactMap { itemID -> (Int, MediaBrowserItem)? in
                guard let index = browserSnapshot.index(of: itemID),
                      let item = browserSnapshot.item(at: index) else { return nil }
                return (index, item)
            }
            .sorted { $0.0 < $1.0 }
            .map(\.1)
    }

    private func selectedTransferItems() -> [InboxTransferItem] {
        let photos = selectedMediaItems().compactMap { item -> InboxTransferItem? in
            guard let localIdentifier = item.localIdentifier else { return nil }
            return .photoAsset(localIdentifier: localIdentifier)
        }
        return photos + transferFileSelection.files.map(InboxTransferItem.file)
    }

    private func recomputeBatchBar() {
        let selectedItems = selectedMediaItems()
        if let selectionAction {
            let transferItems = selectedTransferItems()
            let accessPolicy = selectionAction.accessPolicy()
            let exceedsTransferLimit = !accessPolicy.allows(itemCount: transferItems.count)
            if exceedsTransferLimit, !wasTransferLimitExceeded {
                HUD.flash(
                    String(localized: "transfer.limit.selectionToast"),
                    symbol: "exclamationmark.circle.fill",
                    on: self
                )
            }
            wasTransferLimitExceeded = exceedsTransferLimit
            updateTransferSelectionSummary(
                mediaItems: selectedItems,
                files: transferFileSelection.files
            )
            let canChooseDestination = !transferItems.isEmpty
                && !exceedsTransferLimit
                && selectionAction.canChooseDestination()
                && !selectionActionUIState.isPerforming
                && !transferFileSelection.isWorking
            let showsActivityPanel = selectionActionUIState.showsPanel
            setBatchBarVisible(!transferItems.isEmpty || showsActivityPanel, animated: true)
            selectionActionButton.isEnabled = true
            selectionActionButton.isUserInteractionEnabled = canChooseDestination
            updateSelectionActionButtonAppearance(isAvailable: canChooseDestination)
            selectionActionButton.accessibilityTraits = canChooseDestination
                ? .button
                : [.button, .notEnabled]
            setSelectionActivityPanelVisible(showsActivityPanel)
            let showsTerminalPrimary: Bool
            if case .terminal = selectionActionUIState {
                showsTerminalPrimary = true
            } else {
                showsTerminalPrimary = false
            }
            selectionActivityPauseButton.isHidden = !selectionActionUIState.isPerforming
                && !showsTerminalPrimary
            selectionActivityStopButton.isHidden = !selectionActionUIState.isPerforming
            let allowsModeChanges = !selectionActionUIState.showsPanel
                && !transferFileSelection.isWorking
            transferOptionsButton.isEnabled = allowsModeChanges
            transferChooseFilesButton.isEnabled = allowsModeChanges
            onTransferModeSwitchAvailabilityChanged?(allowsModeChanges)
            updateSelectionActivityPanel()
            selectionActionButton.menu = selectionAction.makeMenu { [weak self] destination in
                self?.handleSelectionAction(destination)
            }
            return
        }

        let result = BatchActionResolver.resolve(selectedItems)
        // Upload/download need a live remote session AND an authoritative presence for the active profile: hide
        // them offline (no destination → "not connected"), and during a profile switch's reload window, when
        // items transiently read `.localOnly`, so the bar can't advertise a remote action for content whose
        // backup state isn't yet known for the target profile.
        let remoteReady = actionRunner.isRemoteReachable && presenceIndex.isRemotePresenceAuthoritative
        var entries: [MediaActionBar.Entry] = []
        if result.showsUpload && remoteReady {
            entries.append(MediaActionBar.Entry(id: BatchAction.upload, symbolName: MediaBrowserActionKind.upload.symbolName, title: MediaBrowserActionKind.upload.title))
        }
        if result.showsDownload && remoteReady {
            entries.append(MediaActionBar.Entry(id: BatchAction.download, symbolName: MediaBrowserActionKind.download.symbolName, title: MediaBrowserActionKind.download.title))
        }
        if result.showsDelete {
            entries.append(MediaActionBar.Entry(id: BatchAction.delete, symbolName: "trash", title: String(localized: "mediaBrowser.action.delete"), isDestructive: true))
        }
        batchBar.configure(entries: entries) { [weak self] id in self?.handleBatchTap(id) }
    }

    private func handleBatchTap(_ id: AnyHashable) {
        guard let action = id as? BatchAction else { return }
        let items = selectedMediaItems()
        guard !items.isEmpty else { return }
        actionRunner.runBatch(action, items: items, from: self) { [weak self] in
            guard let self else { return }
            self.exitSelection()
            self.load(trigger: "batch")
        }
    }

    private func handleSelectionAction(_ destination: InboxTransferDestination) {
        guard let selectionAction else { return }
        let items = selectedTransferItems()
        guard !items.isEmpty,
              selectionAction.accessPolicy().allows(itemCount: items.count) else { return }
        runExternalSelectionAction { [weak self] activity in
            guard let self else { return false }
            return await selectionAction.perform(
                destination,
                items,
                self,
                inboxTransferOptions,
                activity
            )
        }
    }

    @discardableResult
    func runExternalSelectionAction(
        _ operation: @escaping @MainActor (InboxTransferActivity) async -> Bool
    ) -> Bool {
        guard !isAnyActionRunning else { return false }
        if let selectionAction,
           !selectionAction.accessPolicy().allows(itemCount: selectedTransferItems().count) {
            return false
        }
        let preparingStatus = String(localized: "transfer.progress.preparing")
        let activity = InboxTransferActivity(
            onUpdate: { [weak self] status in
                guard let self else { return }
                switch self.selectionActionUIState {
                case .running(_, let pause):
                    self.selectionActionUIState = .running(status: status, pause: pause)
                case .stopping:
                    self.selectionActionUIState = .stopping(status: status)
                case .idle, .terminal:
                    return
                }
                self.updateSelectionActivityPanel()
            },
            onPauseStateChange: { [weak self] paused in
                guard let self,
                      paused,
                      case .running(let status, let phase) = self.selectionActionUIState,
                      phase != .active else { return }
                self.selectionActionUIState = .running(status: status, pause: .paused)
                self.updateSelectionActivityPanel()
            }
        )
        selectionActionActivity = activity
        selectionActionUIState = .running(status: preparingStatus, pause: .active)
        recomputeBatchBar()
        selectionActionTask = Task { @MainActor [weak self] in
            let completed = await operation(activity)
            guard let self else { return }
            let terminalFailureStatus = activity.terminalFailureStatus
            let lastStatus: String = switch self.selectionActionUIState {
            case .running(let status, _), .stopping(let status), .terminal(let status, _): status
            case .idle: preparingStatus
            }
            self.selectionActionTask = nil
            self.selectionActionActivity = nil
            if completed {
                self.selectionActionUIState = .terminal(status: lastStatus, kind: .completed)
            } else if let terminalFailureStatus {
                self.selectionActionUIState = .terminal(status: terminalFailureStatus, kind: .failed)
            } else {
                self.selectionActionUIState = .idle
            }
            self.recomputeBatchBar()
            self.flushDeferredReloadIfNeeded()
        }
        return true
    }

    private func makeLayout() -> UICollectionViewCompositionalLayout {
        UICollectionViewCompositionalLayout { _, environment in
            let metrics = Layout.metrics(for: environment.container.effectiveContentSize.width)
            let itemSize = NSCollectionLayoutSize(
                widthDimension: .absolute(metrics.itemWidth),
                heightDimension: .absolute(metrics.itemWidth)
            )
            let item = NSCollectionLayoutItem(layoutSize: itemSize)
            let groupSize = NSCollectionLayoutSize(
                widthDimension: .fractionalWidth(1.0),
                heightDimension: .absolute(metrics.itemWidth)
            )
            let group = NSCollectionLayoutGroup.horizontal(
                layoutSize: groupSize,
                repeatingSubitem: item,
                count: metrics.columnCount
            )
            group.interItemSpacing = .fixed(Layout.spacing)
            let section = NSCollectionLayoutSection(group: group)
            section.interGroupSpacing = Layout.spacing
            let headerSize = NSCollectionLayoutSize(
                widthDimension: .fractionalWidth(1.0),
                heightDimension: .absolute(Layout.headerHeight)
            )
            let header = NSCollectionLayoutBoundarySupplementaryItem(
                layoutSize: headerSize,
                elementKind: Self.headerKind,
                alignment: .top
            )
            header.pinToVisibleBounds = true
            section.boundarySupplementaryItems = [header]
            section.contentInsets = NSDirectionalEdgeInsets(top: 0, leading: 0, bottom: Layout.spacing * 4, trailing: 0)
            return section
        }
    }
}

extension MediaBrowserGridViewController: UIDocumentPickerDelegate {
    func documentPicker(
        _ controller: UIDocumentPickerViewController,
        didPickDocumentsAt urls: [URL]
    ) {
        guard !isAnyActionRunning, !transferFileSelection.isWorking else { return }
        transferFileSelection.importFiles(urls)
    }
}

extension MediaBrowserGridViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        guard let item = item(at: indexPath) else { return }
        if isNativeMultipleSelectionInteraction {
            setSelected(true, for: item.id)
            return
        }
        collectionView.deselectItem(at: indexPath, animated: true)
        presentViewer(at: indexPath)
    }

    func collectionView(_ collectionView: UICollectionView, didDeselectItemAt indexPath: IndexPath) {
        guard let item = item(at: indexPath) else { return }
        if isNativeMultipleSelectionInteraction {
            setSelected(false, for: item.id)
            return
        }
        if isSelecting, selectedItemIDs.contains(item.id) {
            collectionView.selectItem(at: indexPath, animated: false, scrollPosition: [])
        }
        presentViewer(at: indexPath)
    }

    private func presentViewer(at indexPath: IndexPath) {
        guard !isAnyActionRunning else { return }
        guard presentedViewController == nil else { return }
        guard months.indices.contains(indexPath.section) else { return }
        guard let item = item(at: indexPath) else { return }
        let viewer = MediaBrowserViewerViewController(
            source: source,
            session: browserSession,
            startItemID: item.id,
            runner: actionRunner,
            presenceIndex: presenceIndex,
            actionScope: isMediaDrop ? .transfer : nil,
            onContentChanged: { [weak self] in self?.load(trigger: "viewer") }
        )
        // Hero zoom transition: opens from the tapped thumbnail, drag-dismisses back into it. overFullScreen
        // keeps the grid rendered behind so the zoom-out reveals it.
        viewer.heroTransition.source = self
        viewer.heroTransition.presentItemID = item.id
        viewer.modalPresentationStyle = .overFullScreen
        present(viewer, animated: true)
    }

    func collectionView(
        _ collectionView: UICollectionView,
        shouldBeginMultipleSelectionInteractionAt indexPath: IndexPath
    ) -> Bool {
        !isAnyActionRunning && !months.isEmpty
    }

    func collectionView(
        _ collectionView: UICollectionView,
        didBeginMultipleSelectionInteractionAt indexPath: IndexPath
    ) {
        isNativeMultipleSelectionInteraction = true
        if !isSelecting { enterSelection() }
    }

    func collectionViewDidEndMultipleSelectionInteraction(_ collectionView: UICollectionView) {
        isNativeMultipleSelectionInteraction = false
        flushDeferredReloadIfNeeded()
    }

    // Load a thumbnail only once its cell actually enters the visible rect…
    func collectionView(_ collectionView: UICollectionView, willDisplay cell: UICollectionViewCell, forItemAt indexPath: IndexPath) {
        guard let cell = cell as? MediaBrowserGridCell, let item = item(at: indexPath) else { return }
        cell.beginLoading(item: item, source: source)
    }

    // …and cancel it the moment the cell leaves the screen.
    func collectionView(_ collectionView: UICollectionView, didEndDisplaying cell: UICollectionViewCell, forItemAt indexPath: IndexPath) {
        (cell as? MediaBrowserGridCell)?.cancelLoading()
    }
}

extension MediaBrowserGridViewController: PHPhotoLibraryChangeObserver {
    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in self?.scheduleLibraryChangeReload() }
    }
}

extension MediaBrowserGridViewController: HeroTransitionSource {
    private func indexPath(forItemID id: MediaBrowserItemID) -> IndexPath? {
        dataSource?.indexPath(for: id)
    }

    func heroSource(forItemID id: MediaBrowserItemID) -> (image: UIImage, frameInWindow: CGRect)? {
        guard let indexPath = indexPath(forItemID: id),
              let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell,
              let image = cell.heroImage else { return nil }
        return (image, cell.heroFrameInWindow())
    }

    func heroSourceFrame(forItemID id: MediaBrowserItemID) -> CGRect? {
        guard let indexPath = indexPath(forItemID: id),
              let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { return nil }
        // Frame only — the cell's real rendered thumbnail rect; does NOT require heroImage to be loaded.
        return cell.heroFrameInWindow()
    }

    func heroPrepareSource(forItemID id: MediaBrowserItemID, hidden: Bool) {
        guard let indexPath = indexPath(forItemID: id) else { return }
        (collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell)?.setHeroImageHidden(hidden)
    }

    func heroScrollToItem(id: MediaBrowserItemID) {
        guard let indexPath = indexPath(forItemID: id) else { return }
        collectionView.scrollToItem(at: indexPath, at: .centeredVertically, animated: false)
        collectionView.layoutIfNeeded()
    }
}

// A floating, content-sized month pill (not a full-width bar).
private final class MediaBrowserHeaderView: UICollectionReusableView {
    private let pill = UIVisualEffectView(effect: UIBlurEffect(style: .systemThinMaterial))
    private let label = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear

        pill.clipsToBounds = true
        pill.layer.cornerRadius = 8
        addSubview(pill)

        label.font = .preferredFont(forTextStyle: .headline)
        label.adjustsFontForContentSizeCategory = true
        label.textColor = .label
        pill.contentView.addSubview(label)

        pill.translatesAutoresizingMaskIntoConstraints = false
        label.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            pill.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 6),
            pill.centerYAnchor.constraint(equalTo: centerYAnchor),
            label.topAnchor.constraint(equalTo: pill.contentView.topAnchor, constant: 6),
            label.bottomAnchor.constraint(equalTo: pill.contentView.bottomAnchor, constant: -6),
            label.leadingAnchor.constraint(equalTo: pill.contentView.leadingAnchor, constant: 6),
            label.trailingAnchor.constraint(equalTo: pill.contentView.trailingAnchor, constant: -6),
        ])
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(title: String) {
        label.text = title
    }
}

@MainActor
private final class MediaBrowserGridCell: UICollectionViewCell {
    private let imageView = UIImageView()
    private let bottomGradientView = GradientView(
        colors: [UIColor.black.withAlphaComponent(0.0), UIColor.black.withAlphaComponent(0.52)],
        startPoint: CGPoint(x: 0.5, y: 0),
        endPoint: CGPoint(x: 0.5, y: 1),
        locations: [0, 1]
    )
    private let videoIconView = UIImageView()
    private let livePhotoIconView = UIImageView()
    private let presenceIconView = UIImageView()
    private let incompleteIconView = UIImageView()
    private let needsLoadIconView = UIImageView()
    private let placeholderIconView = UIImageView()
    private let selectionIconView = UIImageView()
    private let selectionButton = UIButton(type: .custom)
    private var loadTask: Task<Void, Never>?
    private var currentItemID: MediaBrowserItemID?
    private var loadedItemID: MediaBrowserItemID?
    private var loadGeneration: UInt64 = 0
    var onSelectionButtonTapped: (() -> Void)?
    var onSelectionButtonDrag: ((UILongPressGestureRecognizer) -> Void)?

    private static let photoPlaceholder = UIImage(systemName: "photo")
    private static let videoPlaceholder = UIImage(systemName: "video")
    private static let selectionCheckmark = UIImage(
        systemName: "checkmark",
        withConfiguration: UIImage.SymbolConfiguration(pointSize: 8, weight: .bold)
    )
    private static let selectionBackgroundColor = UIColor.materialAdaptive(
        light: .Material.Green._600,
        dark: .Material.Green._500
    )

    override init(frame: CGRect) {
        super.init(frame: frame)
        configureUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override var isHighlighted: Bool {
        didSet { contentView.alpha = isHighlighted ? 0.82 : 1.0 }
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        cancelLoading()
        currentItemID = nil
        loadedItemID = nil
        imageView.image = nil
        bottomGradientView.isHidden = true
        videoIconView.isHidden = true
        livePhotoIconView.isHidden = true
        presenceIconView.isHidden = true
        incompleteIconView.isHidden = true
        incompleteIconView.alpha = 1
        needsLoadIconView.isHidden = true
        placeholderIconView.isHidden = true
        selectionIconView.isHidden = false
        imageView.alpha = 1
        onSelectionButtonTapped = nil
        onSelectionButtonDrag = nil
    }

    func setSelecting(_ selecting: Bool, selected: Bool) {
        selectionIconView.isHidden = false
        selectionIconView.image = selected ? Self.selectionCheckmark : nil
        selectionIconView.tintColor = .white
        selectionIconView.backgroundColor = selected ? Self.selectionBackgroundColor : .clear
        selectionIconView.layer.cornerRadius = 9
        selectionIconView.layer.borderWidth = selected ? 0 : 1
        selectionIconView.layer.borderColor = UIColor.white.cgColor
        selectionButton.isSelected = selected
        selectionButton.accessibilityTraits = selected ? [.button, .selected] : .button
        imageView.alpha = (selecting && selected) ? 0.7 : 1.0
        incompleteIconView.alpha = 1
    }

    // Static content only — the thumbnail itself is loaded by beginLoading(…) when the cell is on screen.
    func configure(
        with item: MediaBrowserItem,
        remoteImage: UIImage?,
        showsPresenceBadge: Bool
    ) {
        if currentItemID != item.id {
            cancelLoading()
            currentItemID = item.id
            loadedItemID = nil
            imageView.image = nil

            // Small, centered placeholder while the thumbnail loads (not a stretched full-cell symbol).
            placeholderIconView.image = item.isVideo ? Self.videoPlaceholder : Self.photoPlaceholder
            placeholderIconView.isHidden = false
        }

        bottomGradientView.isHidden = !item.isVideo
        videoIconView.isHidden = !item.isVideo
        livePhotoIconView.isHidden = !item.isLivePhoto
        needsLoadIconView.isHidden = true

        presenceIconView.image = MediaPresenceStyle.image(for: item.presence, remoteImage: remoteImage)
        presenceIconView.isHidden = !showsPresenceBadge

        incompleteIconView.isHidden = !item.isIncomplete
    }

    // Load only while actually on screen (willDisplay); cancelled by didEndDisplaying. Skips if the
    // thumbnail is already loaded or a load is already running for this item.
    func beginLoading(item: MediaBrowserItem, source: MediaBrowserSource) {
        guard currentItemID == item.id, loadTask == nil, loadedItemID != item.id else { return }
        let id = item.id
        let isVideo = item.isVideo
        let generation = loadGeneration
        loadTask = Task { [weak self] in
            let image = await source.thumbnail(for: item)
            await MainActor.run {
                guard let self, !Task.isCancelled,
                      self.loadGeneration == generation,
                      self.currentItemID == id else { return }
                self.loadTask = nil
                if let image {
                    self.loadedItemID = id
                    self.setThumbnail(image)
                } else {
                    self.loadedItemID = nil
                    self.imageView.image = nil
                    self.placeholderIconView.isHidden = !isVideo
                    self.needsLoadIconView.isHidden = isVideo
                }
            }
        }
    }

    func reloadThumbnail(item: MediaBrowserItem, source: MediaBrowserSource) {
        guard currentItemID == item.id else { return }
        cancelLoading()
        loadedItemID = nil
        needsLoadIconView.isHidden = true
        if imageView.image == nil {
            placeholderIconView.isHidden = false
        }
        beginLoading(item: item, source: source)
    }

    func applyStoredThumbnail(_ image: UIImage, for item: MediaBrowserItem) {
        guard currentItemID == item.id else { return }
        cancelLoading()
        loadedItemID = item.id
        setThumbnail(image)
    }

    func cancelLoading() {
        loadGeneration &+= 1
        loadTask?.cancel()
        loadTask = nil
    }

    private func setThumbnail(_ image: UIImage) {
        placeholderIconView.isHidden = true
        needsLoadIconView.isHidden = true
        guard imageView.image == nil else {
            imageView.image = image
            return
        }
        UIView.transition(with: imageView, duration: 0.12, options: [.transitionCrossDissolve, .allowUserInteraction]) {
            self.imageView.image = image
        }
    }

    // MARK: - Hero transition
    var heroImage: UIImage? { imageView.image }
    func heroFrameInWindow() -> CGRect { imageView.convert(imageView.bounds, to: nil) }
    func setHeroImageHidden(_ hidden: Bool) { imageView.alpha = hidden ? 0 : 1 }

    private func configureUI() {
        contentView.backgroundColor = .secondarySystemGroupedBackground
        contentView.clipsToBounds = true

        imageView.backgroundColor = .secondarySystemGroupedBackground
        imageView.clipsToBounds = true
        imageView.contentMode = .scaleAspectFill

        placeholderIconView.tintColor = .tertiaryLabel
        placeholderIconView.contentMode = .scaleAspectFit

        videoIconView.image = UIImage(systemName: "play.circle.fill")
        livePhotoIconView.image = UIImage(systemName: "livephoto")
        for icon in [videoIconView, livePhotoIconView, presenceIconView, incompleteIconView] {
            icon.tintColor = .white
            icon.contentMode = .scaleAspectFit
            icon.layer.shadowColor = UIColor.black.cgColor
            icon.layer.shadowOpacity = 0.35
            icon.layer.shadowRadius = 2
            icon.layer.shadowOffset = CGSize(width: 0, height: 1)
        }
        // Incomplete remote record (only the resolvable subset can be downloaded) — flag it, don't hide it.
        incompleteIconView.image = UIImage(systemName: "exclamationmark.triangle.fill")
        incompleteIconView.tintColor = .systemYellow

        needsLoadIconView.image = UIImage(systemName: "arrow.down.circle")
        needsLoadIconView.tintColor = .secondaryLabel
        needsLoadIconView.contentMode = .scaleAspectFit

        selectionIconView.contentMode = .center
        selectionIconView.isHidden = false
        selectionIconView.layer.shadowColor = UIColor.black.cgColor
        selectionIconView.layer.shadowOpacity = 0.35
        selectionIconView.layer.shadowRadius = 2
        selectionIconView.layer.shadowOffset = CGSize(width: 0, height: 1)

        selectionButton.accessibilityLabel = String(localized: "mediaBrowser.select")
        selectionButton.addTarget(self, action: #selector(selectionButtonTapped), for: .touchUpInside)
        let selectionDragGesture = UILongPressGestureRecognizer(
            target: self,
            action: #selector(selectionButtonDragged(_:))
        )
        selectionDragGesture.minimumPressDuration = 0
        selectionDragGesture.allowableMovement = .greatestFiniteMagnitude
        selectionButton.addGestureRecognizer(selectionDragGesture)

        contentView.addSubview(imageView)
        contentView.addSubview(placeholderIconView)
        contentView.addSubview(bottomGradientView)
        contentView.addSubview(videoIconView)
        contentView.addSubview(livePhotoIconView)
        contentView.addSubview(presenceIconView)
        contentView.addSubview(incompleteIconView)
        contentView.addSubview(needsLoadIconView)
        contentView.addSubview(selectionIconView)
        contentView.addSubview(selectionButton)

        for v in [imageView, placeholderIconView, bottomGradientView, videoIconView, livePhotoIconView, presenceIconView, incompleteIconView, needsLoadIconView, selectionIconView, selectionButton] {
            v.translatesAutoresizingMaskIntoConstraints = false
        }
        NSLayoutConstraint.activate([
            imageView.topAnchor.constraint(equalTo: contentView.topAnchor),
            imageView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            imageView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            imageView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),

            bottomGradientView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            bottomGradientView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            bottomGradientView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            bottomGradientView.heightAnchor.constraint(equalTo: contentView.heightAnchor, multiplier: 0.42),

            videoIconView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 6),
            videoIconView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -6),
            videoIconView.widthAnchor.constraint(equalToConstant: 18),
            videoIconView.heightAnchor.constraint(equalToConstant: 18),

            incompleteIconView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -6),
            incompleteIconView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -6),
            incompleteIconView.widthAnchor.constraint(equalToConstant: 16),
            incompleteIconView.heightAnchor.constraint(equalToConstant: 16),

            livePhotoIconView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 6),
            livePhotoIconView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -6),
            livePhotoIconView.widthAnchor.constraint(equalToConstant: 16),
            livePhotoIconView.heightAnchor.constraint(equalToConstant: 16),

            presenceIconView.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 6),
            presenceIconView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 6),
            presenceIconView.widthAnchor.constraint(equalToConstant: 15),
            presenceIconView.heightAnchor.constraint(equalToConstant: 15),

            needsLoadIconView.centerXAnchor.constraint(equalTo: contentView.centerXAnchor),
            needsLoadIconView.centerYAnchor.constraint(equalTo: contentView.centerYAnchor),
            needsLoadIconView.widthAnchor.constraint(equalToConstant: 24),
            needsLoadIconView.heightAnchor.constraint(equalToConstant: 24),

            placeholderIconView.centerXAnchor.constraint(equalTo: contentView.centerXAnchor),
            placeholderIconView.centerYAnchor.constraint(equalTo: contentView.centerYAnchor),
            placeholderIconView.widthAnchor.constraint(equalToConstant: 28),
            placeholderIconView.heightAnchor.constraint(equalToConstant: 28),

            selectionIconView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -6),
            selectionIconView.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 6),
            selectionIconView.widthAnchor.constraint(equalToConstant: 18),
            selectionIconView.heightAnchor.constraint(equalToConstant: 18),

            selectionButton.topAnchor.constraint(equalTo: contentView.topAnchor),
            selectionButton.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            selectionButton.widthAnchor.constraint(equalToConstant: 30),
            selectionButton.heightAnchor.constraint(equalToConstant: 30),
        ])
    }

    @objc
    private func selectionButtonTapped() {
        onSelectionButtonTapped?()
    }

    @objc
    private func selectionButtonDragged(_ gesture: UILongPressGestureRecognizer) {
        onSelectionButtonDrag?(gesture)
    }
}
