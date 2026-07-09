import Photos
import UIKit

// Source-driven media grid (local / remote / merged). Month sections, date-descending. Cells show a
// thumbnail plus type (video/live) and presence badges. Tapping opens the full-screen paging viewer.
// Modes are switchable via a segmented control whose availability tracks the remote connection live.
final class MediaBrowserGridViewController: UIViewController {
    // One selectable mode. `isAvailable` is re-evaluated on connection changes to enable/disable its tab.
    struct ModeSpec {
        let mode: MediaBrowserMode
        let isAvailable: () -> Bool
        let makeSource: () -> MediaBrowserSource
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

    private typealias DataSource = UICollectionViewDiffableDataSource<LibraryMonthKey, MediaBrowserItem>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<LibraryMonthKey, MediaBrowserItem>
    private static let headerKind = "month-header"

    private let specs: [ModeSpec]
    private let navTitle: String
    private let remoteStorageSymbol: () -> String
    private let actionRunner: MediaBrowserActionRunner
    // The one presence authority for this browser session. Observed so badges/actions self-correct after a
    // presence rebuild, and invalidated+refreshed when a background task ends (its snapshot update is otherwise
    // invisible to an already-open browser).
    private let presenceIndex: LibraryPresenceIndex
    // Identifies the active remote session/profile (nil = disconnected). A change means a remote-backed
    // source is now stale (disconnect, or profile A→B while still connected).
    private let sessionToken: () -> AnyHashable?
    private var currentMode: MediaBrowserMode
    private var source: MediaBrowserSource
    private var sourceToken: AnyHashable?
    private var pendingScrollMonth: LibraryMonthKey?

    private var months: [LibraryMonthKey] = []
    private var itemsByMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
    private var dataSource: DataSource?
    private var loadTask: Task<Void, Never>?
    private var loadGeneration = 0
    private weak var segmentedControl: UISegmentedControl?

    private var isSelecting = false
    private var selectedItemIDs: Set<String> = []
    private let batchBarContainer = UIView()
    private let batchBar = MediaActionBar()
    private static let batchBarHeight: CGFloat = 58
    private var libraryChangeReload: DispatchWorkItem?

    private lazy var collectionView = UICollectionView(frame: .zero, collectionViewLayout: makeLayout())

    // Empty-state copy depends on the mode: "nothing backed up" only makes sense for remote.
    private func makeEmptyState() -> UIView {
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
        remoteStorageSymbol: @escaping () -> String,
        sessionToken: @escaping () -> AnyHashable?,
        actionRunner: MediaBrowserActionRunner,
        presenceIndex: LibraryPresenceIndex,
        title: String
    ) {
        self.specs = specs
        self.navTitle = title
        self.remoteStorageSymbol = remoteStorageSymbol
        self.sessionToken = sessionToken
        self.actionRunner = actionRunner
        self.presenceIndex = presenceIndex
        self.pendingScrollMonth = initialMonth
        let initialSpec = specs.first(where: { $0.mode == initialMode }) ?? specs[0]
        self.currentMode = initialSpec.mode
        self.source = initialSpec.makeSource()
        self.sourceToken = sessionToken()
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        NotificationCenter.default.removeObserver(self)
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
        let source = source
        Task { await source.shutdown() }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = navTitle
        configureModeSwitcher()
        configureUI()
        configureBatchBar()
        configureDataSource()
        updateSelectBarButton()
        NotificationCenter.default.addObserver(self, selector: #selector(sessionChanged), name: .AppSessionChanged, object: nil)
        // Presence is single-sourced: LibraryPresenceIndex owns which upstream events (snapshot sync, execution
        // end) can change it and posts .LibraryPresenceDidChange when it rebuilds. The grid just reloads on that
        // one event — no proxy subscriptions here.
        NotificationCenter.default.addObserver(self, selector: #selector(presenceChanged), name: .LibraryPresenceDidChange, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(thumbnailStored(_:)), name: .MediaBrowserThumbnailDidStore, object: nil)
        // Local/merged membership comes from a PhotoKit fetch that only re-runs on load(): without this, an
        // asset inserted/deleted outside the browser (iCloud sync, Photos edits) stays wrong until reopen.
        PHPhotoLibrary.shared().register(self)
        load()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        refreshVisibleThumbnails()
    }

    @objc private func presenceChanged() {
        DispatchQueue.main.async { [weak self] in
            self?.reloadRespectingActionGate()
        }
    }

    private func scheduleLibraryChangeReload() {
        // Remote mode reloads too: membership is PhotoKit-independent, but the device handles bound at load
        // are not — a Photos edit must reproject the record (stale handle drops → `.remoteOnly`).
        libraryChangeReload?.cancel()
        let work = DispatchWorkItem { [weak self] in self?.reloadRespectingActionGate() }
        libraryChangeReload = work
        // Coalesce PhotoKit bursts (iCloud sync fires per batch) into one reload.
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5, execute: work)
    }

    @objc private func thumbnailStored(_ notification: Notification) {
        guard let fingerprint = notification.userInfo?[MediaThumbnailCache.storedFingerprintUserInfoKey] as? Data,
              let image = notification.userInfo?[MediaThumbnailCache.storedImageUserInfoKey] as? UIImage else { return }
        DispatchQueue.main.async { [weak self] in
            self?.applyStoredThumbnail(image, fingerprint: fingerprint)
        }
    }

    private func applyStoredThumbnail(_ image: UIImage, fingerprint: Data) {
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let item = dataSource?.itemIdentifier(for: indexPath),
                  item.fingerprint == fingerprint,
                  let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { continue }
            cell.applyStoredThumbnail(image, for: item)
        }
    }

    private func refreshVisibleThumbnails() {
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let item = dataSource?.itemIdentifier(for: indexPath),
                  let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { continue }
            cell.beginLoading(item: item, source: source)
        }
    }

    // A signal landing while an action is gated defers instead of dropping: a pre-lease abort (cancelled
    // confirmation) never reloads on its own, so a dropped signal would leave the grid stale indefinitely.
    // Mid-batch churn still coalesces — pending retries collapse into one load once the runner idles.
    private func reloadRespectingActionGate() {
        guard actionRunner.isActionRunning else {
            load()
            return
        }
        libraryChangeReload?.cancel()
        let retry = DispatchWorkItem { [weak self] in self?.reloadRespectingActionGate() }
        libraryChangeReload = retry
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5, execute: retry)
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        // Only when the browsing session actually ends — not when we merely present the full-screen viewer.
        guard isBeingDismissed || isMovingFromParent else { return }
        Task { await MediaThumbnailCache.enforceLimit() }
    }

    private func configureModeSwitcher() {
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
        exitSelection()
        // Local content is session-independent, but its presence badges (.both) are computed against the
        // remote snapshot — recompute them for the new session without rebuilding the source.
        if currentMode == .local {
            load()
            return
        }
        // Remote/merged: the source (and any presented viewer built on it) now points at the wrong / gone
        // node. Close the viewer, then rebuild the current mode with the new session — or fall back to local.
        if presentedViewController != nil { dismiss(animated: false) }
        let target = specs.first(where: { $0.mode == currentMode && $0.isAvailable() })
            ?? specs.first(where: { $0.mode == .local })
        if let target { switchTo(spec: target) }
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
        guard !actionRunner.isActionRunning else {
            control.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == currentMode }) ?? 0
            return
        }
        let spec = specs[control.selectedSegmentIndex]
        guard spec.mode != currentMode, spec.isAvailable() else {
            control.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == currentMode }) ?? 0
            return
        }
        switchTo(spec: spec)
    }

    private func switchTo(spec: ModeSpec) {
        exitSelection()
        currentMode = spec.mode
        segmentedControl?.selectedSegmentIndex = specs.firstIndex(where: { $0.mode == spec.mode }) ?? 0
        pendingScrollMonth = nil
        let previous = source
        Task { await previous.shutdown() }
        source = spec.makeSource()
        sourceToken = sessionToken()
        months = []
        itemsByMonth = [:]
        collectionView.backgroundView = nil
        applySnapshot()
        load()
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
        // Avoid spawning thumbnail tasks for far off-screen cells during fast scrolling; cells that do
        // scroll past are recycled and cancel their in-flight request (renderLocalThumbnail is cancellable).
        collectionView.isPrefetchingEnabled = false
        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            collectionView.topAnchor.constraint(equalTo: view.topAnchor),
            collectionView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            collectionView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            collectionView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
    }

    private func configureDataSource() {
        let cellRegistration = UICollectionView.CellRegistration<MediaBrowserGridCell, MediaBrowserItem> { [weak self] cell, _, item in
            guard let self else { return }
            cell.configure(with: item, remoteSymbol: self.remoteStorageSymbol())
            cell.setSelecting(self.isSelecting, selected: self.selectedItemIDs.contains(item.id))
        }
        let dataSource = DataSource(collectionView: collectionView) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: item)
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

    private func load() {
        loadTask?.cancel()
        loadGeneration += 1
        let generation = loadGeneration
        let source = source
        if months.isEmpty {
            collectionView.backgroundView = makeLoadingState()
        }
        loadTask = Task { [weak self] in
            await source.prepare()
            let sections = await source.loadSections()
            // Drop a completion from a superseded source (rapid tab switch / session change) so it can't
            // overwrite the current snapshot — !Task.isCancelled alone can race a past-await continuation.
            guard let self, !Task.isCancelled, self.loadGeneration == generation else { return }
            self.months = sections.map(\.month)
            self.itemsByMonth = Dictionary(uniqueKeysWithValues: sections.map { ($0.month, $0.items) })
            self.applySnapshot()
            self.collectionView.backgroundView = self.months.isEmpty ? self.makeEmptyState() : nil
            self.scrollToPendingMonthIfNeeded()
            self.updateSelectBarButton()
            // Content may have changed under an active selection (a background refresh / a just-finished batch):
            // leave selection if the grid emptied; otherwise drop stale ids and re-derive which actions still apply.
            if self.isSelecting {
                if self.months.isEmpty {
                    self.exitSelection()
                } else {
                    self.selectedItemIDs.formIntersection(Set(self.flattenedItems().map(\.id)))
                    self.recomputeBatchBar()
                    self.refreshVisibleSelectionOverlays()
                }
            }
            // A presented viewer's items were captured at present time — push the fresh projection so its
            // badge/actions track the grid (a stale `.both` drops to `.remoteOnly`, Download reappears).
            (self.presentedViewController as? MediaBrowserViewerViewController)?.reconcileItems(with: self.flattenedItems())
        }
    }

    // Consumed once, on first load: jumps to the month the browser was opened at.
    private func scrollToPendingMonthIfNeeded() {
        guard let month = pendingScrollMonth, let section = months.firstIndex(of: month) else { return }
        pendingScrollMonth = nil
        collectionView.layoutIfNeeded()
        collectionView.scrollToItem(at: IndexPath(item: 0, section: section), at: .top, animated: false)
    }

    private func applySnapshot() {
        var snapshot = Snapshot()
        snapshot.appendSections(months)
        for month in months {
            snapshot.appendItems(itemsByMonth[month] ?? [], toSection: month)
        }
        dataSource?.apply(snapshot, animatingDifferences: false)
    }

    private func flattenedItems() -> [MediaBrowserItem] {
        months.flatMap { itemsByMonth[$0] ?? [] }
    }

    // MARK: - Multi-select

    private func configureBatchBar() {
        batchBarContainer.isHidden = true
        batchBarContainer.translatesAutoresizingMaskIntoConstraints = false
        let blur = UIVisualEffectView(effect: UIBlurEffect(style: .systemThinMaterial))
        blur.translatesAutoresizingMaskIntoConstraints = false
        batchBarContainer.addSubview(blur)
        batchBar.translatesAutoresizingMaskIntoConstraints = false
        batchBarContainer.addSubview(batchBar)
        view.addSubview(batchBarContainer)
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

    private func updateSelectBarButton() {
        if isSelecting {
            navigationItem.rightBarButtonItem = UIBarButtonItem(barButtonSystemItem: .cancel, target: self, action: #selector(exitSelection))
        } else if !months.isEmpty {
            navigationItem.rightBarButtonItem = UIBarButtonItem(title: String(localized: "mediaBrowser.select"), style: .plain, target: self, action: #selector(enterSelection))
        } else {
            navigationItem.rightBarButtonItem = nil
        }
    }

    @objc private func enterSelection() {
        guard !isSelecting, !months.isEmpty else { return }
        isSelecting = true
        selectedItemIDs.removeAll()
        batchBarContainer.isHidden = false
        let inset = Self.batchBarHeight + 12
        collectionView.contentInset.bottom = inset
        collectionView.verticalScrollIndicatorInsets.bottom = inset
        updateSelectBarButton()
        recomputeBatchBar()
        refreshVisibleSelectionOverlays()
    }

    @objc private func exitSelection() {
        guard isSelecting else { return }
        isSelecting = false
        selectedItemIDs.removeAll()
        batchBarContainer.isHidden = true
        collectionView.contentInset.bottom = 0
        collectionView.verticalScrollIndicatorInsets.bottom = 0
        updateSelectBarButton()
        refreshVisibleSelectionOverlays()
    }

    private func refreshVisibleSelectionOverlays() {
        for indexPath in collectionView.indexPathsForVisibleItems {
            guard let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell,
                  let item = dataSource?.itemIdentifier(for: indexPath) else { continue }
            cell.setSelecting(isSelecting, selected: selectedItemIDs.contains(item.id))
        }
    }

    private func selectedMediaItems() -> [MediaBrowserItem] {
        guard !selectedItemIDs.isEmpty else { return [] }
        let byID = Dictionary(flattenedItems().map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        return selectedItemIDs.compactMap { byID[$0] }
    }

    private func recomputeBatchBar() {
        let result = BatchActionResolver.resolve(selectedMediaItems())
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
            self.load()
        }
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

extension MediaBrowserGridViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        if isSelecting {
            guard let item = dataSource?.itemIdentifier(for: indexPath) else { return }
            if selectedItemIDs.contains(item.id) { selectedItemIDs.remove(item.id) } else { selectedItemIDs.insert(item.id) }
            if let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell {
                cell.setSelecting(true, selected: selectedItemIDs.contains(item.id))
            }
            recomputeBatchBar()
            return
        }
        guard presentedViewController == nil else { return }
        guard months.indices.contains(indexPath.section) else { return }
        // Flat index from section math (not a firstIndex(of:) scan) — correct even if two items were equal.
        let start = months[..<indexPath.section].reduce(0) { $0 + (itemsByMonth[$1]?.count ?? 0) } + indexPath.item
        let items = flattenedItems()
        guard items.indices.contains(start) else { return }
        let viewer = MediaBrowserViewerViewController(
            source: source,
            items: items,
            startIndex: start,
            runner: actionRunner,
            presenceIndex: presenceIndex,
            onContentChanged: { [weak self] in self?.load() }
        )
        // Hero zoom transition: opens from the tapped thumbnail, drag-dismisses back into it. overFullScreen
        // keeps the grid rendered behind so the zoom-out reveals it.
        viewer.heroTransition.source = self
        viewer.heroTransition.presentItemID = items[start].id
        viewer.modalPresentationStyle = .overFullScreen
        present(viewer, animated: true)
    }

    // Load a thumbnail only once its cell actually enters the visible rect…
    func collectionView(_ collectionView: UICollectionView, willDisplay cell: UICollectionViewCell, forItemAt indexPath: IndexPath) {
        guard let cell = cell as? MediaBrowserGridCell, let item = dataSource?.itemIdentifier(for: indexPath) else { return }
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
    private func indexPath(forItemID id: String) -> IndexPath? {
        guard let dataSource, let item = flattenedItems().first(where: { $0.id == id }) else { return nil }
        return dataSource.indexPath(for: item)
    }

    func heroSource(forItemID id: String) -> (image: UIImage, frameInWindow: CGRect)? {
        guard let indexPath = indexPath(forItemID: id),
              let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell,
              let image = cell.heroImage else { return nil }
        return (image, cell.heroFrameInWindow())
    }

    func heroSourceFrame(forItemID id: String) -> CGRect? {
        guard let indexPath = indexPath(forItemID: id),
              let cell = collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell else { return nil }
        // Frame only — the cell's real rendered thumbnail rect; does NOT require heroImage to be loaded.
        return cell.heroFrameInWindow()
    }

    func heroPrepareSource(forItemID id: String, hidden: Bool) {
        guard let indexPath = indexPath(forItemID: id) else { return }
        (collectionView.cellForItem(at: indexPath) as? MediaBrowserGridCell)?.setHeroImageHidden(hidden)
    }

    func heroScrollToItem(id: String) {
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
    private var loadTask: Task<Void, Never>?
    private var currentItemID: String?
    private var loadedItemID: String?

    private static let photoPlaceholder = UIImage(systemName: "photo")
    private static let videoPlaceholder = UIImage(systemName: "video")

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
        selectionIconView.isHidden = true
        imageView.alpha = 1
    }

    // Toggle the selection overlay without disturbing the thumbnail load (called on every dequeue and on tap).
    func setSelecting(_ selecting: Bool, selected: Bool) {
        selectionIconView.isHidden = !selecting
        if selecting {
            selectionIconView.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
            selectionIconView.tintColor = selected ? .appTint : .white
        }
        // Dim a selected tile and hide the incomplete badge it would sit on top of.
        imageView.alpha = (selecting && selected) ? 0.7 : 1.0
        incompleteIconView.alpha = selecting ? 0 : 1
    }

    // Static content only — the thumbnail itself is loaded by beginLoading(…) when the cell is on screen.
    func configure(with item: MediaBrowserItem, remoteSymbol: String) {
        cancelLoading()
        currentItemID = item.id
        loadedItemID = nil
        imageView.image = nil

        // Small, centered placeholder while the thumbnail loads (not a stretched full-cell symbol).
        placeholderIconView.image = item.isVideo ? Self.videoPlaceholder : Self.photoPlaceholder
        placeholderIconView.isHidden = false

        bottomGradientView.isHidden = !item.isVideo
        videoIconView.isHidden = !item.isVideo
        livePhotoIconView.isHidden = !item.isLivePhoto
        needsLoadIconView.isHidden = true

        presenceIconView.image = UIImage(systemName: MediaPresenceStyle.symbolName(for: item.presence, remoteSymbol: remoteSymbol))
        presenceIconView.isHidden = false

        incompleteIconView.isHidden = !item.isIncomplete
    }

    // Load only while actually on screen (willDisplay); cancelled by didEndDisplaying. Skips if the
    // thumbnail is already loaded or a load is already running for this item.
    func beginLoading(item: MediaBrowserItem, source: MediaBrowserSource) {
        guard currentItemID == item.id, loadTask == nil, loadedItemID != item.id else { return }
        let id = item.id
        let isVideo = item.isVideo
        loadTask = Task { [weak self] in
            let image = await source.thumbnail(for: item)
            guard let self, !Task.isCancelled, self.currentItemID == id else { return }
            self.loadTask = nil
            if let image {
                self.loadedItemID = id
                self.setThumbnail(image)
            } else if !isVideo {
                self.placeholderIconView.isHidden = true
                self.needsLoadIconView.isHidden = false
            }
        }
    }

    func applyStoredThumbnail(_ image: UIImage, for item: MediaBrowserItem) {
        guard currentItemID == item.id else { return }
        cancelLoading()
        loadedItemID = item.id
        setThumbnail(image)
    }

    func cancelLoading() {
        loadTask?.cancel()
        loadTask = nil
    }

    private func setThumbnail(_ image: UIImage) {
        placeholderIconView.isHidden = true
        needsLoadIconView.isHidden = true
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

        selectionIconView.contentMode = .scaleAspectFit
        selectionIconView.isHidden = true
        selectionIconView.layer.shadowColor = UIColor.black.cgColor
        selectionIconView.layer.shadowOpacity = 0.35
        selectionIconView.layer.shadowRadius = 2
        selectionIconView.layer.shadowOffset = CGSize(width: 0, height: 1)

        contentView.addSubview(imageView)
        contentView.addSubview(placeholderIconView)
        contentView.addSubview(bottomGradientView)
        contentView.addSubview(videoIconView)
        contentView.addSubview(livePhotoIconView)
        contentView.addSubview(presenceIconView)
        contentView.addSubview(incompleteIconView)
        contentView.addSubview(needsLoadIconView)
        contentView.addSubview(selectionIconView)

        for v in [imageView, placeholderIconView, bottomGradientView, videoIconView, livePhotoIconView, presenceIconView, incompleteIconView, needsLoadIconView, selectionIconView] {
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

            livePhotoIconView.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 6),
            livePhotoIconView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -6),
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
            selectionIconView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -6),
            selectionIconView.widthAnchor.constraint(equalToConstant: 22),
            selectionIconView.heightAnchor.constraint(equalToConstant: 22),
        ])
    }
}
