import UIKit

// Full-screen, swipeable media viewer. Horizontally-paging collection view (one page per item); each
// page renders a zoomable photo, a native Live Photo, or a video poster with a play button. Single tap
// toggles chrome (immersive full-screen ↔ controls). Only visible/adjacent pages are alive; offscreen
// pages tear down (release image, stop Live playback) via cell reuse.
final class MediaBrowserViewerViewController: UIViewController {
    private let source: MediaBrowserSource
    private var items: [MediaBrowserItem]
    private var currentIndex: Int
    private let runner: MediaBrowserActionRunner
    // Single presence authority. Observed so an already-open viewer's badge/actions self-correct when presence
    // changes underneath it (e.g. the currently-viewed local-only item gets backed up by a background run).
    private let presenceIndex: LibraryPresenceIndex
    private let onContentChanged: () -> Void

    private lazy var collectionView = UICollectionView(frame: .zero, collectionViewLayout: makeLayout())
    private let topBar = UIView()
    private let bottomBar = UIView()
    private let closeButton = UIButton(type: .system)
    private let titleLabel = UILabel()
    private let actionBar = MediaActionBar()
    private var actionBarHeightConstraint: NSLayoutConstraint!
    private static let actionBarHeight: CGFloat = 48
    private var chromeHidden = false
    private var didInitialCenter = false
    // The single active (playing) page, held by reference — not index. A page that scrolls fully off-screen at
    // a settle is gone from `collectionView.visibleCells`/`cellForItem`, so only a retained handle can quiet it.
    private weak var activeCell: MediaPageCell?
    let heroTransition = HeroTransition()
    // Interactive drag-to-dismiss (keeps the viewer full-screen; a sheet would not).
    private var isZoomed = false
    private let dismissPan = UIPanGestureRecognizer()
    private var chromeHiddenBeforeDismissDrag = false
    private var dragSnapshot: UIImageView?
    private var dragOrigBounds: CGRect = .zero
    private var dragOrigCenter: CGPoint = .zero

    // Synthetic bar tag: a single Delete button that opens a presence-aware menu instead of running one action.
    private enum ViewerBarAction { case delete }

    init(source: MediaBrowserSource, items: [MediaBrowserItem], startIndex: Int,
         runner: MediaBrowserActionRunner, presenceIndex: LibraryPresenceIndex, onContentChanged: @escaping () -> Void) {
        self.source = source
        self.items = items
        self.currentIndex = startIndex
        self.runner = runner
        self.presenceIndex = presenceIndex
        self.onContentChanged = onContentChanged
        super.init(nibName: nil, bundle: nil)
        heroTransition.destination = self
        transitioningDelegate = heroTransition
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
        configureDismissGesture()
        updateChrome(for: currentIndex)
        // A backup/maintenance starting or ending changes which actions are runnable — refresh the chrome so the
        // bar can't keep offering a now-disallowed action (nor hide a re-allowed one).
        for name in [Notification.Name.ExecutionLifecycleDidChange, .RemoteMaintenanceDidChange] {
            NotificationCenter.default.addObserver(self, selector: #selector(taskLifecycleChanged), name: name, object: nil)
        }
        // A connect/disconnect changes both reachability and the item's presence projection for the new profile.
        // Handled separately from the task/maintenance refresh so connect can wait for the reprojection (below).
        NotificationCenter.default.addObserver(self, selector: #selector(appSessionChanged), name: .AppSessionChanged, object: nil)
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

    @objc private func appSessionChanged() {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            // Disconnect: hide remote actions immediately (reachability is now false; the presence rebuild that
            // reprojects items is async). Connect / profile switch: do NOT flip to reachable chrome on the still-
            // stale item projection — a `.localOnly` item might already be backed up on the now-active profile.
            // Wait for the presence reprojection (grid reload → `.LibraryPresenceDidChange` → `presenceChanged`),
            // which re-derives each item against the new profile before re-enabling Upload/Download.
            guard !self.runner.isRemoteReachable else { return }
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
        // responds; horizontal drags elsewhere still page (the departed page's video stops at settle).
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

        titleLabel.textColor = .white
        titleLabel.textAlignment = .center
        titleLabel.numberOfLines = 2
        titleLabel.adjustsFontForContentSizeCategory = true

        for v in [closeButton, titleLabel] {
            v.translatesAutoresizingMaskIntoConstraints = false
            topBar.addSubview(v)
        }
        actionBar.translatesAutoresizingMaskIntoConstraints = false
        actionBar.foregroundColor = .white
        bottomBar.addSubview(actionBar)

        actionBarHeightConstraint = actionBar.heightAnchor.constraint(equalToConstant: Self.actionBarHeight)
        NSLayoutConstraint.activate([
            topBar.topAnchor.constraint(equalTo: view.topAnchor),
            topBar.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            topBar.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            topBar.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 52),

            closeButton.leadingAnchor.constraint(equalTo: topBar.leadingAnchor, constant: 12),
            closeButton.bottomAnchor.constraint(equalTo: topBar.bottomAnchor, constant: -8),
            closeButton.widthAnchor.constraint(equalToConstant: 40),
            closeButton.heightAnchor.constraint(equalToConstant: 36),
            titleLabel.centerXAnchor.constraint(equalTo: topBar.centerXAnchor),
            titleLabel.centerYAnchor.constraint(equalTo: closeButton.centerYAnchor),

            bottomBar.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            bottomBar.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            bottomBar.bottomAnchor.constraint(equalTo: view.bottomAnchor),

            actionBar.leadingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.leadingAnchor),
            actionBar.trailingAnchor.constraint(equalTo: view.safeAreaLayoutGuide.trailingAnchor),
            actionBar.topAnchor.constraint(equalTo: bottomBar.topAnchor, constant: 8),
            actionBar.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -8),
            actionBarHeightConstraint,
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
        titleLabel.attributedText = Self.titleText(for: Date(timeIntervalSince1970: Double(item.creationDateMs) / 1000))
        // Hide the action bar for an item that's gone from BOTH device and remote (deleted elsewhere while the
        // viewer stayed open): RemoteMediaSource.actions is presence-blind, so without this it would still offer
        // Download / Delete on a vanished asset.
        let entries = actionBarEntries(for: item)
        let showBar = !entries.isEmpty && isPresent(item)
        actionBar.isHidden = !showBar
        actionBarHeightConstraint.constant = showBar ? Self.actionBarHeight : 0
        if showBar {
            actionBar.configure(entries: entries) { [weak self] id in self?.handleBarTap(id) }
        }
    }

    // Ordered bar entries for the item: share / download / upload as-is, and the two deletes collapsed into one
    // Delete button that opens a presence-aware menu.
    private func actionBarEntries(for item: MediaBrowserItem) -> [MediaActionBar.Entry] {
        let actions = runnableActions(for: item)
        var entries: [MediaActionBar.Entry] = []
        for kind in [MediaBrowserActionKind.share, .download, .upload] where actions.contains(kind) {
            entries.append(MediaActionBar.Entry(id: kind, symbolName: kind.symbolName, title: kind.title))
        }
        if actions.contains(.deleteLocal) || actions.contains(.deleteRemote) {
            entries.append(MediaActionBar.Entry(id: ViewerBarAction.delete, symbolName: "trash", title: String(localized: "mediaBrowser.action.delete"), isDestructive: true))
        }
        return entries
    }

    private func handleBarTap(_ id: AnyHashable) {
        guard items.indices.contains(currentIndex) else { return }
        let item = items[currentIndex]
        if let kind = id as? MediaBrowserActionKind {
            runAction(kind)
        } else if let bar = id as? ViewerBarAction, bar == .delete {
            presentDeleteMenu(for: item)
        }
    }

    // Presence-derived actions gated by per-item capability: a presence flip alone (e.g. a background
    // backup marking a `.localOnly` item `.both`) can't backfill `remoteMonth`/handles, and offering an
    // action the runner can't execute ends in a silent skip or a generic error.
    private func runnableActions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] {
        source.actions(for: item).filter { kind in
            guard runner.canRun(kind) else { return false }
            switch kind {
            case .deleteRemote: return item.isRemoteDeletable
            case .deleteLocal: return item.isDeviceDeletable
            // Only while the remote still holds restorable media (mirrors the grid builder's containsRealMedia
            // rule) — a record degraded to config-only / phantom / all-missing under an open viewer stops
            // advertising a Download that would resolve to nothing. Delete Remote stays for cleanup.
            case .download: return runner.isRemoteReachable && (item.fingerprint.map { presenceIndex.isBackedUp($0) } ?? false)
            // Also require authoritative presence: during an A→B switch the shared snapshot is tagged B while the
            // session is still A (or B just activated but hasn't reprojected), so an item transiently reads
            // `.localOnly` though it may already be backed up — don't advertise Upload for it in that window.
            case .upload: return item.localIdentifier != nil && runner.isRemoteReachable && presenceIndex.isRemotePresenceAuthoritative
            case .share: return true
            }
        }
    }

    // Still on device (has a local handle) or still on the remote per the shared index. A fingerprint-less
    // item is a plain local asset → present.
    private func isPresent(_ item: MediaBrowserItem) -> Bool {
        guard let fp = item.fingerprint else { return true }
        if item.localIdentifier != nil || presenceIndex.isOnRemote(fp) { return true }
        // Set-absence proves "vanished" only when the committed presence build matches the live cache; mid-sync
        // it may just predate this item's month — keep the bar (each action re-verifies before acting).
        return !presenceIndex.isRemotePresenceCurrent
    }

    // Run one action on the CURRENT item, re-validating against fresh state first: the bar was built for this
    // item but presence may have shifted (downloaded elsewhere → Download no longer applies, or deleted → gone).
    private func runAction(_ kind: MediaBrowserActionKind) {
        guard items.indices.contains(currentIndex) else { return }
        let actedID = items[currentIndex].id
        guard let idx = items.firstIndex(where: { $0.id == actedID }) else { return }
        let current = items[idx]
        guard isPresent(current), source.actions(for: current).contains(kind) else {
            presentActionError()
            return
        }
        // Reload the grid on any content change; `dismiss` also closes the viewer (delete/upload).
        runner.run(kind, item: current, source: source, from: self, onChanged: { [weak self, onContentChanged = self.onContentChanged] dismiss, downloadedLocalID in
            onContentChanged()
            // Download keeps the viewer open: flip the acted item to on-device so it no longer offers Download
            // (which would re-import a duplicate). Match by id — the user may have swiped away.
            if let self, let downloadedLocalID, let idx = self.items.firstIndex(where: { $0.id == actedID }) {
                self.items[idx].presence = .of(onDevice: true, onRemote: self.items[idx].presence != .localOnly)
                self.items[idx].localIdentifier = downloadedLocalID
                if idx == self.currentIndex { self.updateChrome(for: idx) }
            }
            if dismiss { self?.dismiss(animated: true) }
        })
    }

    // Delete tapped: choose a target by presence. On both → a menu (delete local / remote / all); on a single
    // presence → straight to that action (its own confirm is the second confirmation).
    private func presentDeleteMenu(for item: MediaBrowserItem) {
        let actions = runnableActions(for: item)
        let canLocal = actions.contains(.deleteLocal)
        let canRemote = actions.contains(.deleteRemote)
        guard canLocal || canRemote else { return }
        guard canLocal && canRemote else {
            // Single target. Remote delete is confirmed by the runner; local delete otherwise has only the
            // system sheet, so add an app-level confirmation (a local-only asset may not be backed up).
            if canLocal {
                let sheet = UIAlertController(title: nil, message: nil, preferredStyle: .actionSheet)
                sheet.addAction(UIAlertAction(title: String(localized: "mediaBrowser.action.deleteLocal"), style: .destructive) { [weak self] _ in self?.runAction(.deleteLocal) })
                sheet.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
                presentDeleteSheet(sheet)
            } else {
                runAction(.deleteRemote)
            }
            return
        }
        let sheet = UIAlertController(title: nil, message: nil, preferredStyle: .actionSheet)
        sheet.addAction(UIAlertAction(title: String(localized: "mediaBrowser.action.deleteLocal"), style: .destructive) { [weak self] _ in self?.runAction(.deleteLocal) })
        sheet.addAction(UIAlertAction(title: String(localized: "mediaBrowser.action.deleteRemote"), style: .destructive) { [weak self] _ in self?.runAction(.deleteRemote) })
        sheet.addAction(UIAlertAction(title: String(localized: "mediaBrowser.action.deleteAll"), style: .destructive) { [weak self] _ in self?.runDeleteAll(item) })
        sheet.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        presentDeleteSheet(sheet)
    }

    private func presentDeleteSheet(_ sheet: UIAlertController) {
        if let pop = sheet.popoverPresentationController {
            let anchor = actionBar.buttonView(for: ViewerBarAction.delete) ?? actionBar
            pop.sourceView = anchor
            pop.sourceRect = anchor.bounds
        }
        present(sheet, animated: true)
    }

    private func runDeleteAll(_ item: MediaBrowserItem) {
        runner.runDeleteAll(item, from: self) { [weak self] hadFailures in
            self?.onContentChanged()
            // A failed backup delete presented an error on this viewer — keep it open so the error stays visible.
            if !hadFailures { self?.dismiss(animated: true) }
        }
    }

    private func presentActionError() {
        // Defer so an in-flight sheet dismissal finishes before we present (avoids a present-while-dismissing conflict).
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            let alert = UIAlertController(title: nil, message: String(localized: "mediaBrowser.action.error"), preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
            self.present(alert, animated: true)
        }
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

    // MARK: - Interactive drag-to-dismiss

    private func configureDismissGesture() {
        dismissPan.addTarget(self, action: #selector(handleDismissPan(_:)))
        dismissPan.delegate = self
        view.addGestureRecognizer(dismissPan)
    }

    @objc private func handleDismissPan(_ pan: UIPanGestureRecognizer) {
        let translation = pan.translation(in: view)
        switch pan.state {
        case .began:
            chromeHiddenBeforeDismissDrag = chromeHidden
            setBarsAlpha(0)               // fade chrome out during the drag without changing the toggle state
            collectionView.isScrollEnabled = false
            beginHeroDrag()
        case .changed:
            let progress = min(1, max(0, translation.y) / (view.bounds.height * 0.6))
            if let snap = dragSnapshot {
                let scale = 1 - progress * 0.4
                snap.bounds = CGRect(origin: .zero, size: CGSize(width: dragOrigBounds.width * scale, height: dragOrigBounds.height * scale))
                snap.center = CGPoint(x: dragOrigCenter.x + translation.x, y: dragOrigCenter.y + translation.y)
                collectionView.alpha = 1 - progress
            } else {
                collectionView.transform = CGAffineTransform(translationX: translation.x, y: max(0, translation.y))
                    .scaledBy(x: 1 - progress * 0.15, y: 1 - progress * 0.15)
            }
            view.backgroundColor = UIColor.black.withAlphaComponent(1 - progress)
        case .ended, .cancelled:
            let velocity = pan.velocity(in: view)
            if translation.y > 100 || velocity.y > 800 {
                finishHeroDismiss()
            } else {
                cancelHeroDrag()
            }
        default:
            break
        }
    }

    // Snapshot the current photo so it (not the whole screen) follows the finger and can zoom into its grid
    // thumbnail on release. A nil snapshot (live / not-yet-loaded) falls back to a plain whole-view drag.
    private func beginHeroDrag() {
        guard let dst = heroDestination() else { dragSnapshot = nil; return }
        let snap = UIImageView(image: dst.image)
        snap.contentMode = .scaleAspectFill
        snap.clipsToBounds = true
        snap.frame = view.convert(dst.frameInWindow, from: nil)
        view.addSubview(snap)
        dragSnapshot = snap
        dragOrigBounds = snap.bounds
        dragOrigCenter = snap.center
        heroPrepareDestination(hidden: true)
    }

    private func finishHeroDismiss() {
        guard let snap = dragSnapshot else {
            dismiss(animated: true)   // no snapshot → the dismiss animator's fallback fade runs
            return
        }
        let itemID = heroCurrentItemID
        heroTransition.source?.heroScrollToItem(id: itemID)
        let target = heroTransition.source?.heroSourceFrame(forItemID: itemID)
        UIView.animate(withDuration: 0.2, delay: 0, options: [.curveEaseInOut]) {
            if let target {
                snap.frame = self.view.convert(target, from: nil)
            } else {
                snap.center.y += self.view.bounds.height
                snap.alpha = 0
            }
            self.collectionView.alpha = 0
            self.view.backgroundColor = UIColor.black.withAlphaComponent(0)
        } completion: { _ in
            self.dismiss(animated: false)
        }
    }

    private func cancelHeroDrag() {
        // No .allowUserInteraction: a second drag during the spring must not re-enter beginHeroDrag and
        // overwrite dragSnapshot (the first completion would then remove the new snapshot).
        UIView.animate(withDuration: 0.24, delay: 0, usingSpringWithDamping: 0.85, initialSpringVelocity: 0, options: []) {
            if let snap = self.dragSnapshot {
                snap.bounds = self.dragOrigBounds
                snap.center = self.dragOrigCenter
            } else {
                self.collectionView.transform = .identity
            }
            self.collectionView.alpha = 1
            self.view.backgroundColor = .black
            self.setBarsAlpha(self.chromeHiddenBeforeDismissDrag ? 0 : 1)
        } completion: { _ in
            self.dragSnapshot?.removeFromSuperview()
            self.dragSnapshot = nil
            self.heroPrepareDestination(hidden: false)
            self.collectionView.isScrollEnabled = !self.isZoomed
        }
    }

    private func setBarsAlpha(_ alpha: CGFloat) {
        topBar.alpha = alpha
        bottomBar.alpha = alpha
    }

    private func currentPageCell() -> MediaPageCell? {
        collectionView.cellForItem(at: IndexPath(item: currentIndex, section: 0)) as? MediaPageCell
    }

    // Make `cell` the sole active page: quiet the previously-active page first — even if it has scrolled
    // off-screen and is no longer in `visibleCells` — then activate the new one. Idempotent when `cell` is
    // already active, so a rubber-band drag that returns to the same page keeps its video/Live playing.
    private func setActivePage(_ cell: MediaPageCell?) {
        if activeCell !== cell { activeCell?.setActive(false) }
        activeCell = cell
        cell?.setActive(true)
    }

    // Exactly one page is "active" (the centered one) and may play its Live Photo / inline video; every other
    // page is deactivated — including the page just left, which after a completed page change is off-screen and
    // reachable only through `activeCell`, not `visibleCells`.
    private func refreshActivePage() {
        let current = currentPageCell()
        for case let cell as MediaPageCell in collectionView.visibleCells where cell !== current {
            cell.setActive(false)
        }
        setActivePage(current)
    }

    private func deactivateVisiblePages() {
        activeCell?.setActive(false)
        activeCell = nil
        for case let cell as MediaPageCell in collectionView.visibleCells {
            cell.setActive(false)
        }
    }

    @objc private func close() {
        dismiss(animated: true)
    }

    // Two-line date/time like the system Photos viewer: relative day (Today / Yesterday / date) on top,
    // time below in a lighter style.
    private static let dayFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .none
        f.doesRelativeDateFormatting = true
        return f
    }()
    private static let timeFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .none
        f.timeStyle = .short
        return f
    }()

    private static func titleText(for date: Date) -> NSAttributedString {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineSpacing = 1
        let text = NSMutableAttributedString(string: dayFormatter.string(from: date), attributes: [
            .font: UIFont.preferredFont(forTextStyle: .footnote).withWeight(.semibold),
            .foregroundColor: UIColor.white,
        ])
        text.append(NSAttributedString(string: "\n" + timeFormatter.string(from: date), attributes: [
            .font: UIFont.preferredFont(forTextStyle: .caption1),
            .foregroundColor: UIColor.white.withAlphaComponent(0.85),
        ]))
        text.addAttribute(.paragraphStyle, value: paragraph, range: NSRange(location: 0, length: text.length))
        return text
    }
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
        cell.onZoomChanged = { [weak self] zoomedIn in
            self?.isZoomed = zoomedIn
            self?.setPagingEnabled(!zoomedIn)
        }
        return cell
    }

    func collectionView(_ collectionView: UICollectionView, layout: UICollectionViewLayout, sizeForItemAt indexPath: IndexPath) -> CGSize {
        collectionView.bounds.size
    }

    // A page only becomes active once it's the current index — during a scroll the incoming neighbour is
    // displayed but stays inactive (so it doesn't play Live/haptics while merely scrolling past). Routing the
    // active case through `setActivePage` keeps `activeCell` authoritative across recycle.
    func collectionView(_ collectionView: UICollectionView, willDisplay cell: UICollectionViewCell, forItemAt indexPath: IndexPath) {
        guard let cell = cell as? MediaPageCell else { return }
        if indexPath.item == currentIndex { setActivePage(cell) } else { cell.setActive(false) }
    }

    // NB: do NOT tear down in didEndDisplaying — with a paging collection view that call can arrive on a
    // cell instance already reused/reconfigured for another page, cancelling its fresh load (black page).
    // prepareForReuse handles teardown safely when the cell is actually recycled.
    //
    // No scrollViewWillBeginDragging teardown: quieting the centered page at drag-begin broke a rubber-band
    // drag (video torn down, never resumed). Deactivation happens at settle via `setActivePage`, which reaches
    // the departed page even once it is off-screen; a drag that returns to the same page leaves it playing.

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

extension MediaBrowserViewerViewController: UIGestureRecognizerDelegate {
    // Start the dismiss drag only on a downward, vertical-dominant gesture on a non-zoomed page — so
    // horizontal paging and zoom-panning keep working.
    func gestureRecognizerShouldBegin(_ gestureRecognizer: UIGestureRecognizer) -> Bool {
        guard gestureRecognizer == dismissPan else { return true }
        guard !isZoomed else { return false }
        let velocity = dismissPan.velocity(in: view)
        return velocity.y > 0 && abs(velocity.y) > abs(velocity.x)
    }

    // Recognize alongside the collection view's own pan; began disables its scrolling so only the drag moves.
    func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer, shouldRecognizeSimultaneouslyWith other: UIGestureRecognizer) -> Bool {
        gestureRecognizer == dismissPan
    }
}

extension MediaBrowserViewerViewController: HeroTransitionDestination {
    var heroCurrentItemID: String {
        items.indices.contains(currentIndex) ? items[currentIndex].id : ""
    }

    func heroDestination() -> (image: UIImage, frameInWindow: CGRect)? {
        currentPageCell()?.heroSnapshot()
    }

    func heroPrepareDestination(hidden: Bool) {
        currentPageCell()?.setHeroContentHidden(hidden)
    }
}
