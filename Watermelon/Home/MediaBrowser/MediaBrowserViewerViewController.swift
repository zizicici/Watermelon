import UIKit

// Full-screen, swipeable media viewer. Horizontally-paging collection view (one page per item); each
// page renders a zoomable photo, a native Live Photo, or a video poster with a play button. Single tap
// toggles chrome (immersive full-screen ↔ controls). Only visible/adjacent pages are alive; offscreen
// pages tear down (release image, stop Live playback) via cell reuse.
final class MediaBrowserViewerViewController: UIViewController {
    private let source: MediaBrowserSource
    private var items: [MediaBrowserItem]
    private var currentIndex: Int
    private let remoteStorageSymbol: String
    private let runner: MediaBrowserActionRunner
    // Single presence authority. Observed so an already-open viewer's badge/actions self-correct when presence
    // changes underneath it (e.g. the currently-viewed local-only item gets backed up by a background run).
    private let presenceIndex: LibraryPresenceIndex
    private let onContentChanged: () -> Void

    private lazy var collectionView = UICollectionView(frame: .zero, collectionViewLayout: makeLayout())
    private let topBar = UIView()
    private let bottomBar = UIView()
    private let closeButton = UIButton(type: .system)
    private let moreButton = UIButton(type: .system)
    private let titleLabel = UILabel()
    private let presenceBadge = PresenceBadgeView()
    private var chromeHidden = false
    private var didInitialCenter = false

    init(source: MediaBrowserSource, items: [MediaBrowserItem], startIndex: Int, remoteStorageSymbol: String,
         runner: MediaBrowserActionRunner, presenceIndex: LibraryPresenceIndex, onContentChanged: @escaping () -> Void) {
        self.source = source
        self.items = items
        self.currentIndex = startIndex
        self.remoteStorageSymbol = remoteStorageSymbol
        self.runner = runner
        self.presenceIndex = presenceIndex
        self.onContentChanged = onContentChanged
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override var prefersStatusBarHidden: Bool { chromeHidden }
    override var prefersHomeIndicatorAutoHidden: Bool { chromeHidden }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        configureCollectionView()
        configureChrome()
        updateChrome(for: currentIndex)
        // A backup/maintenance starting or ending while viewing changes which actions are runnable — refresh
        // the chrome so the More menu can't keep offering a now-disallowed action (nor hide a re-allowed one).
        for name in [Notification.Name.ExecutionLifecycleDidChange, .RemoteMaintenanceDidChange] {
            NotificationCenter.default.addObserver(self, selector: #selector(taskLifecycleChanged), name: name, object: nil)
        }
        // Presence rebuilt underneath us (background backup, same-profile snapshot update) → re-derive the
        // open items' badges/actions from the shared index instead of the stale snapshot passed at open.
        NotificationCenter.default.addObserver(self, selector: #selector(presenceChanged), name: .LibraryPresenceDidChange, object: nil)
    }

    @objc private func taskLifecycleChanged() {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            self.updateChrome(for: self.currentIndex)
        }
    }

    @objc private func presenceChanged() {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            for i in self.items.indices {
                guard let fp = self.items[i].fingerprint else { continue }
                // Adopt a local handle the index now knows (downloaded elsewhere); never clear an existing one
                // (an external Photos deletion needs per-asset PHAsset validation, deliberately out of scope).
                if self.items[i].localIdentifier == nil { self.items[i].localIdentifier = self.presenceIndex.localIdentifier(for: fp) }
                // Presence tracks "backed up" = has real media on the remote (a partial-but-has-media record
                // still counts); only a config-only / phantom fingerprint isn't in backedUp, so its local twin reads localOnly.
                self.items[i].presence = .of(onDevice: self.items[i].localIdentifier != nil, onRemote: self.presenceIndex.isBackedUp(fp))
            }
            self.updateChrome(for: self.currentIndex)
        }
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        refreshActivePage()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        deactivateVisiblePages()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        // One-shot: center on the opened page at first real layout. Rotation is handled by
        // viewWillTransition; using a flag (not a contentOffset heuristic) avoids fighting the user's swipe.
        guard !didInitialCenter, currentIndex > 0, collectionView.bounds.width > 0 else { return }
        didInitialCenter = true
        collectionView.scrollToItem(at: IndexPath(item: currentIndex, section: 0), at: .centeredHorizontally, animated: false)
    }

    override func viewWillTransition(to size: CGSize, with coordinator: UIViewControllerTransitionCoordinator) {
        super.viewWillTransition(to: size, with: coordinator)
        // The preserved contentOffset is computed for the old width and would strand the pager between
        // pages after rotation; re-lay-out and snap back to the current page.
        let index = currentIndex
        coordinator.animate(alongsideTransition: { _ in
            self.didInitialCenter = true   // rotation owns centering now; don't let the one-shot re-fire
            self.collectionView.collectionViewLayout.invalidateLayout()
            guard self.items.indices.contains(index), self.collectionView.bounds.width > 0 else { return }
            self.collectionView.scrollToItem(at: IndexPath(item: index, section: 0), at: .centeredHorizontally, animated: false)
        })
    }

    private func configureCollectionView() {
        collectionView.isPagingEnabled = true
        // Deliver touches to embedded controls immediately so an inline video's transport scrubber
        // responds; horizontal drags elsewhere still page (and stop the video via willBeginDragging).
        collectionView.delaysContentTouches = false
        collectionView.showsHorizontalScrollIndicator = false
        collectionView.backgroundColor = .black
        collectionView.contentInsetAdjustmentBehavior = .never
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.register(MediaPageCell.self, forCellWithReuseIdentifier: MediaPageCell.reuseID)
        view.addSubview(collectionView)
        collectionView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            collectionView.topAnchor.constraint(equalTo: view.topAnchor),
            collectionView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            collectionView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            collectionView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
    }

    private func configureChrome() {
        for bar in [topBar, bottomBar] {
            bar.translatesAutoresizingMaskIntoConstraints = false
            view.addSubview(bar)
        }
        let topBlur = UIVisualEffectView(effect: UIBlurEffect(style: .systemUltraThinMaterialDark))
        let bottomBlur = UIVisualEffectView(effect: UIBlurEffect(style: .systemUltraThinMaterialDark))
        embed(topBlur, in: topBar)
        embed(bottomBlur, in: bottomBar)

        closeButton.setImage(UIImage(systemName: "xmark"), for: .normal)
        closeButton.tintColor = .white
        closeButton.addTarget(self, action: #selector(close), for: .touchUpInside)

        moreButton.setImage(UIImage(systemName: "ellipsis.circle"), for: .normal)
        moreButton.tintColor = .white
        moreButton.addTarget(self, action: #selector(showActions), for: .touchUpInside)

        titleLabel.font = .preferredFont(forTextStyle: .subheadline)
        titleLabel.textColor = .white
        titleLabel.textAlignment = .center

        for v in [closeButton, moreButton, titleLabel] {
            v.translatesAutoresizingMaskIntoConstraints = false
            topBar.addSubview(v)
        }
        presenceBadge.translatesAutoresizingMaskIntoConstraints = false
        bottomBar.addSubview(presenceBadge)

        NSLayoutConstraint.activate([
            topBar.topAnchor.constraint(equalTo: view.topAnchor),
            topBar.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            topBar.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            topBar.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 44),

            closeButton.leadingAnchor.constraint(equalTo: topBar.leadingAnchor, constant: 12),
            closeButton.bottomAnchor.constraint(equalTo: topBar.bottomAnchor, constant: -8),
            closeButton.widthAnchor.constraint(equalToConstant: 40),
            closeButton.heightAnchor.constraint(equalToConstant: 36),
            moreButton.trailingAnchor.constraint(equalTo: topBar.trailingAnchor, constant: -12),
            moreButton.centerYAnchor.constraint(equalTo: closeButton.centerYAnchor),
            moreButton.widthAnchor.constraint(equalToConstant: 40),
            moreButton.heightAnchor.constraint(equalToConstant: 36),
            titleLabel.centerXAnchor.constraint(equalTo: topBar.centerXAnchor),
            titleLabel.centerYAnchor.constraint(equalTo: closeButton.centerYAnchor),

            bottomBar.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            bottomBar.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            bottomBar.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            bottomBar.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -44),

            presenceBadge.centerXAnchor.constraint(equalTo: bottomBar.centerXAnchor),
            presenceBadge.topAnchor.constraint(equalTo: bottomBar.topAnchor, constant: 6),
        ])
    }

    private func embed(_ child: UIView, in parent: UIView) {
        parent.insertSubview(child, at: 0)
        child.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            child.topAnchor.constraint(equalTo: parent.topAnchor),
            child.bottomAnchor.constraint(equalTo: parent.bottomAnchor),
            child.leadingAnchor.constraint(equalTo: parent.leadingAnchor),
            child.trailingAnchor.constraint(equalTo: parent.trailingAnchor),
        ])
    }

    private func makeLayout() -> UICollectionViewLayout {
        let layout = UICollectionViewFlowLayout()
        layout.scrollDirection = .horizontal
        layout.minimumLineSpacing = 0
        layout.minimumInteritemSpacing = 0
        return layout
    }

    private func updateChrome(for index: Int) {
        guard items.indices.contains(index) else { return }
        let item = items[index]
        titleLabel.text = Self.dateFormatter.string(from: Date(timeIntervalSince1970: Double(item.creationDateMs) / 1000))
        presenceBadge.configure(presence: item.presence, remoteSymbol: remoteStorageSymbol, showsLabel: true)
        // Hide the menu for an item that's gone from BOTH device and remote (deleted elsewhere while the viewer
        // stayed open): RemoteMediaSource.actions is presence-blind, so without this it would still offer
        // Download / Delete-from-backup on a vanished asset.
        moreButton.isHidden = runnableActions(for: item).isEmpty || !isPresent(item)
    }

    private func runnableActions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] {
        source.actions(for: item).filter { runner.canRun($0) }
    }

    // Still on device (has a local handle) or still on the remote per the shared index. A fingerprint-less
    // item is a plain local asset → present.
    private func isPresent(_ item: MediaBrowserItem) -> Bool {
        guard let fp = item.fingerprint else { return true }
        return item.localIdentifier != nil || presenceIndex.isOnRemote(fp)
    }

    @objc private func showActions() {
        guard items.indices.contains(currentIndex) else { return }
        let item = items[currentIndex]
        let kinds = runnableActions(for: item)
        guard !kinds.isEmpty else { return }
        let sheet = UIAlertController(title: nil, message: nil, preferredStyle: .actionSheet)
        for kind in kinds {
            let actedID = item.id
            sheet.addAction(UIAlertAction(title: kind.title, style: kind.isDestructive ? .destructive : .default) { [weak self] _ in
                guard let self else { return }
                // Re-validate against CURRENT state before running: the sheet may have stayed open while
                // presence changed (the item got downloaded elsewhere → Download no longer applies, or was
                // deleted → gone). Running a stale action could e.g. re-import a duplicate. Act on the fresh
                // item and skip if this action no longer applies.
                guard let idx = self.items.firstIndex(where: { $0.id == actedID }) else { return }
                let current = self.items[idx]
                guard self.isPresent(current), self.source.actions(for: current).contains(kind) else {
                    // Defer so the action sheet finishes dismissing before we present (avoids a present-while-
                    // dismissing conflict).
                    DispatchQueue.main.async { [weak self] in
                        guard let self else { return }
                        let alert = UIAlertController(title: nil, message: String(localized: "mediaBrowser.action.error"), preferredStyle: .alert)
                        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
                        self.present(alert, animated: true)
                    }
                    return
                }
                // Reload the grid on any content change; `dismiss` also closes the viewer (delete/upload).
                // Capture the grid-reload closure by value so the grid still reloads if the viewer closed.
                self.runner.run(kind, item: current, source: self.source, from: self, onChanged: { [weak self, onContentChanged = self.onContentChanged] dismiss, downloadedLocalID in
                    onContentChanged()
                    // Download keeps the viewer open: flip the acted item to on-device so it no longer offers
                    // Download (which would re-import a duplicate). Match by id — the user may have swiped away.
                    if let self, let downloadedLocalID, let idx = self.items.firstIndex(where: { $0.id == actedID }) {
                        // Route through the single presence derivation (never hard-code .both): Download only
                        // ever acts on a remote-present item, so it's now on-device AND on-remote.
                        self.items[idx].presence = .of(onDevice: true, onRemote: self.items[idx].presence != .localOnly)
                        self.items[idx].localIdentifier = downloadedLocalID
                        if idx == self.currentIndex { self.updateChrome(for: idx) }
                    }
                    if dismiss { self?.dismiss(animated: true) }
                })
            })
        }
        sheet.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        if let pop = sheet.popoverPresentationController {
            pop.sourceView = moreButton
            pop.sourceRect = moreButton.bounds
        }
        present(sheet, animated: true)
    }

    private func setChromeHidden(_ hidden: Bool) {
        guard hidden != chromeHidden else { return }
        chromeHidden = hidden
        UIView.animate(withDuration: 0.22) {
            self.topBar.alpha = hidden ? 0 : 1
            self.bottomBar.alpha = hidden ? 0 : 1
            self.setNeedsStatusBarAppearanceUpdate()
        }
        setNeedsUpdateOfHomeIndicatorAutoHidden()
    }

    fileprivate func toggleChrome() {
        setChromeHidden(!chromeHidden)
    }

    fileprivate func setPagingEnabled(_ enabled: Bool) {
        collectionView.isScrollEnabled = enabled
    }

    // Exactly one page is "active" (the centered one) and may play its Live Photo; every other visible
    // page is deactivated (stops Live playback / inline video).
    private func refreshActivePage() {
        for case let cell as MediaPageCell in collectionView.visibleCells {
            cell.setActive(collectionView.indexPath(for: cell)?.item == currentIndex)
        }
    }

    private func deactivateVisiblePages() {
        for case let cell as MediaPageCell in collectionView.visibleCells {
            cell.setActive(false)
        }
    }

    @objc private func close() {
        dismiss(animated: true)
    }

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .short
        return f
    }()
}

extension MediaBrowserViewerViewController: UICollectionViewDataSource, UICollectionViewDelegateFlowLayout {
    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        items.count
    }

    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        let cell = collectionView.dequeueReusableCell(withReuseIdentifier: MediaPageCell.reuseID, for: indexPath) as! MediaPageCell
        cell.configure(with: items[indexPath.item], source: source)
        cell.hostViewController = self
        cell.onSingleTap = { [weak self] in self?.toggleChrome() }
        cell.onZoomChanged = { [weak self] zoomedIn in self?.setPagingEnabled(!zoomedIn) }
        return cell
    }

    func collectionView(_ collectionView: UICollectionView, layout: UICollectionViewLayout, sizeForItemAt indexPath: IndexPath) -> CGSize {
        collectionView.bounds.size
    }

    // A page only becomes active once it's the current index — during a scroll the incoming neighbour is
    // displayed but stays inactive (so it doesn't play Live/haptics while merely scrolling past).
    func collectionView(_ collectionView: UICollectionView, willDisplay cell: UICollectionViewCell, forItemAt indexPath: IndexPath) {
        (cell as? MediaPageCell)?.setActive(indexPath.item == currentIndex)
    }

    // NB: do NOT tear down in didEndDisplaying — with a paging collection view that call can arrive on a
    // cell instance already reused/reconfigured for another page, cancelling its fresh load (black page).
    // prepareForReuse handles teardown safely when the cell is actually recycled.

    func scrollViewWillBeginDragging(_ scrollView: UIScrollView) {
        // Leaving the current page: quiet everything (stop Live playback / inline video).
        deactivateVisiblePages()
    }

    func scrollViewDidEndDecelerating(_ scrollView: UIScrollView) {
        settleActivePage()
    }

    // A slow drag released at a page boundary ends without deceleration — settle here too, else the newly
    // centered page never becomes active (Live/video won't play, chrome shows the previous page's info).
    func scrollViewDidEndDragging(_ scrollView: UIScrollView, willDecelerate decelerate: Bool) {
        if !decelerate { settleActivePage() }
    }

    private func settleActivePage() {
        let width = collectionView.bounds.width
        guard width > 0 else { return }
        let index = Int(round(collectionView.contentOffset.x / width))
        if index != currentIndex, items.indices.contains(index) {
            currentIndex = index
            updateChrome(for: index)
        }
        refreshActivePage()
    }
}
