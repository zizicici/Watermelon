import MarqueeLabel
import MoreKit
import SnapKit
import UIKit

enum ConnectionFailureAlertFactory {
    static func make(
        profile: ServerProfileRecord,
        error: Error,
        onEdit: @escaping () -> Void
    ) -> UIAlertController {
        let alert = UIAlertController(
            title: String(localized: "home.alert.connectionFailed"),
            message: profile.userFacingStorageErrorMessage(error),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.edit"), style: .default) { [weak alert] _ in
            Task { @MainActor [weak alert] in
                await PresentationDismissalSequencer.performAfterDismissal(
                    isPresented: {
                        guard let alert else { return false }
                        return alert.presentingViewController != nil || alert.viewIfLoaded?.window != nil
                    },
                    action: onEdit
                )
            }
        })
        return alert
    }
}

final class HomeViewController: UIViewController {
    private static let privacyPolicyURLString = "https://watermelonbackup.com/privacy.html"

    private let dependencies: DependencyContainer
    private let store: HomeScreenStore
    private var browserLinkSessionID: UUID?
    private var isReplacingBrowserLink = false
    private var pendingBrowserLinkPairing: BrowserLinkPairing?
    private var activeBrowserLinkClient: BrowserLinkClient?
    private var activeBrowserLinkSessionID: String?
    private var activeBrowserLinkRegistration: StorageClientFactory.BrowserLinkRegistrationToken?
    private var pendingBrowserLinkRegistrations: [String: StorageClientFactory.BrowserLinkRegistrationToken] = [:]
    private lazy var menuFactory = HomeMenuFactory(
        store: store,
        hooks: HomeMenuFactory.Hooks(
            refreshLocalLibraryMenu: { [weak self] in self?.refreshLocalLibraryMenu() },
            openLocalAlbumPicker: { [weak self] in self?.openLocalAlbumPicker() },
            openBrowserLinkScanner: { [weak self] in self?.openBrowserLinkScanner() },
            openNewStorageFlow: { [weak self] dest in self?.openNewStorageFlow(dest) },
            openManageProfiles: { [weak self] in self?.openManageProfiles() },
            openCurrentProfileSettings: { [weak self] in self?.openCurrentProfileSettings() },
            scrollToMonth: { [weak self] month in self?.scrollToMonth(month) },
            openLocalIndex: { [weak self] in self?.openLocalIndex() },
            openDuplicates: { [weak self] in self?.openDuplicates() }
        )
    )

    private enum Section: Hashable {
        case year(Int)
    }

    private struct Item: Hashable {
        enum Side { case local, remote }
        let side: Side
        let month: LibraryMonthKey
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<Section, Item>

    private let collectionView: UICollectionView = {
        let cv = UICollectionView(frame: .zero, collectionViewLayout: createLayout())
        cv.backgroundColor = .clear
        cv.automaticallyAdjustsScrollIndicatorInsets = false
        cv.verticalScrollIndicatorInsets = .zero
        return cv
    }()

    private var dataSource: DataSource!
    private var panelShownConstraint: Constraint?
    private var panelHiddenConstraint: Constraint?
    private var settingsFABBottomToSafeArea: Constraint?
    private var settingsFABBottomToActionPanel: Constraint?
    private let leftHeaderLabel: MarqueeLabel = {
        let label = MarqueeLabel(frame: .zero, rate: 30, fadeLength: 8)
        label.animationDelay = 2
        label.trailingBuffer = 40
        return label
    }()
    private let leftHeaderCountLabel = UILabel()
    private let leftHeaderSizeLabel = UILabel()
    private let leftToggle = UIButton(type: .system)
    private let leftHeaderMenuOverlay = UIButton(type: .system)
    private let leftHeaderButton = UIButton(type: .system)
    private let rightHeaderLabel: MarqueeLabel = {
        let label = MarqueeLabel(frame: .zero, rate: 30, fadeLength: 8)
        label.animationDelay = 2
        label.trailingBuffer = 40
        return label
    }()
    private let rightHeaderActivityIndicator = UIActivityIndicatorView(style: .medium)
    private let rightHeaderCountLabel = UILabel()
    private let rightHeaderSizeLabel = UILabel()
    private let rightHeaderMenuOverlay = UIButton(type: .system)
    private let rightHeaderButton = UIButton(type: .system)
    private let rightToggle = UIButton(type: .system)
    private let actionPanel = SelectionActionPanel()
    private let settingsFAB = UIButton(type: .system)
    private var collectionBottomToActionPanel: Constraint?

    private let localOverlay = UIView()
    private let localOverlayLabel = UILabel()
    private let localOverlaySpinner = UIActivityIndicatorView(style: .medium)
    private let localOverlayButton = UIButton(type: .system)
    private let remoteOverlay = UIView()
    private let remoteNodeOverlayController = RemoteNodeOverlayViewController()
    private var didBecomeActiveObserver: NSObjectProtocol?

    private let rightHeaderBg = UIView()
    private var isPanelShown = false
    private var hasLoadedHeaderSummary = false
    private var didRequestReviewForCurrentExecution = false
    private var pendingSFTPHostKeyPromptContinuation: CheckedContinuation<Bool, Never>?
    private weak var sftpHostKeyPromptAlert: UIAlertController?

    private static let headerAreaHeight: CGFloat = 96

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.store = HomeScreenStore(dependencies: dependencies)
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let activeBrowserLinkClient {
            Task { @MainActor in activeBrowserLinkClient.stop() }
        }
        if let activeBrowserLinkRegistration {
            dependencies.storageClientFactory.unregisterBrowserLink(token: activeBrowserLinkRegistration)
        }
        let pendingSessionIDs = Array(pendingBrowserLinkRegistrations.keys)
        if !pendingSessionIDs.isEmpty {
            Task { @MainActor [store] in
                for sessionID in pendingSessionIDs {
                    store.cancelBrowserLinkConnection(sessionID: sessionID)
                }
            }
        }
        for registration in pendingBrowserLinkRegistrations.values {
            dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
        }
        resolveSFTPHostKeyPrompt(false)
        if let didBecomeActiveObserver {
            NotificationCenter.default.removeObserver(didBecomeActiveObserver)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground

        buildUI()
        configureDataSource()
        bindStore()
        observeApplicationLifecycle()

        store.load()
    }

    // MARK: - Layout

    private static func createLayout() -> UICollectionViewCompositionalLayout {
        UICollectionViewCompositionalLayout { _, _ in
            let arrowBadgeSize = NSCollectionLayoutSize(widthDimension: .absolute(40), heightDimension: .absolute(38))
            let arrowBadge = NSCollectionLayoutSupplementaryItem(
                layoutSize: arrowBadgeSize,
                elementKind: directionArrowElementKind,
                containerAnchor: NSCollectionLayoutAnchor(edges: [.trailing], absoluteOffset: CGPoint(x: 21, y: 0))
            )
            arrowBadge.zIndex = 10

            let itemSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(0.5), heightDimension: .absolute(72))
            let leftItem = NSCollectionLayoutItem(layoutSize: itemSize, supplementaryItems: [arrowBadge])
            let rightItem = NSCollectionLayoutItem(layoutSize: itemSize)
            let groupSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1), heightDimension: .absolute(72))
            let group = NSCollectionLayoutGroup.horizontal(layoutSize: groupSize, subitems: [leftItem, rightItem])
            group.interItemSpacing = .fixed(2)
            let section = NSCollectionLayoutSection(group: group)

            let headerSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1), heightDimension: .absolute(60))
            let header = NSCollectionLayoutBoundarySupplementaryItem(
                layoutSize: headerSize,
                elementKind: UICollectionView.elementKindSectionHeader,
                alignment: .top
            )
            header.pinToVisibleBounds = true
            section.boundarySupplementaryItems = [header]
            return section
        }
    }

    // MARK: - UI Construction

    private func buildUI() {
        let leftHeaderBg = UIView()
        leftHeaderBg.backgroundColor = .materialSurface(light: .Material.Green._100, darkTint: .Material.Green._200, darkAlpha: 0.16)
        rightHeaderBg.backgroundColor = .materialSurface(light: .Material.Green._100, darkTint: .Material.Green._200, darkAlpha: 0.16)

        view.addSubview(leftHeaderBg)
        view.addSubview(rightHeaderBg)

        leftHeaderBg.snp.makeConstraints { make in
            make.top.leading.equalToSuperview()
            make.trailing.equalTo(view.snp.centerX).offset(-1)
            make.bottom.equalTo(view.safeAreaLayoutGuide.snp.top).offset(Self.headerAreaHeight)
        }
        rightHeaderBg.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview()
            make.leading.equalTo(view.snp.centerX).offset(1)
            make.bottom.equalTo(view.safeAreaLayoutGuide.snp.top).offset(Self.headerAreaHeight)
        }

        let headerTextColor = UIColor.materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100)
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 12)

        leftToggle.setImage(UIImage(systemName: "circle", withConfiguration: symbolConfig), for: .normal)
        leftToggle.tintColor = headerTextColor
        leftToggle.setContentHuggingPriority(.required, for: .horizontal)
        leftToggle.setContentCompressionResistancePriority(.required, for: .horizontal)
        leftToggle.addTarget(self, action: #selector(leftToggleTapped), for: .touchUpInside)

        configureLeftHeaderButton()
        leftHeaderLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        leftHeaderLabel.textColor = headerTextColor
        leftHeaderLabel.textAlignment = .center
        configureHeaderDetailLabel(leftHeaderCountLabel, color: headerTextColor)
        configureHeaderDetailLabel(leftHeaderSizeLabel, color: headerTextColor)

        let leftHeaderContentView = UIView()
        let leftHeaderTitleStack = UIStackView(arrangedSubviews: [leftHeaderLabel, leftHeaderButton])
        leftHeaderTitleStack.axis = .horizontal
        leftHeaderTitleStack.spacing = 4
        leftHeaderTitleStack.alignment = .center
        let leftHeaderTitleRow = UIStackView(arrangedSubviews: [leftToggle, leftHeaderTitleStack])
        leftHeaderTitleRow.axis = .horizontal
        leftHeaderTitleRow.spacing = 4
        leftHeaderTitleRow.alignment = .center
        leftHeaderBg.addSubview(leftHeaderContentView)
        leftHeaderContentView.addSubview(leftHeaderTitleRow)
        leftHeaderContentView.addSubview(leftHeaderCountLabel)
        leftHeaderContentView.addSubview(leftHeaderSizeLabel)

        leftHeaderContentView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide.snp.top).offset(10)
            make.bottom.equalTo(leftHeaderBg.snp.bottom).inset(10)
            make.leading.equalTo(view.safeAreaLayoutGuide.snp.leading).offset(12)
            make.trailing.equalTo(leftHeaderBg.snp.trailing).inset(12)
        }
        leftHeaderTitleRow.snp.makeConstraints { make in
            make.top.equalToSuperview()
            make.centerX.equalToSuperview()
            make.leading.greaterThanOrEqualToSuperview()
            make.trailing.lessThanOrEqualToSuperview()
        }
        leftHeaderCountLabel.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderTitleRow.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview()
        }
        leftHeaderSizeLabel.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderCountLabel.snp.bottom).offset(6)
            make.leading.trailing.equalToSuperview()
            make.bottom.equalToSuperview()
        }

        leftHeaderMenuOverlay.showsMenuAsPrimaryAction = true
        leftHeaderMenuOverlay.menu = menuFactory.buildLocalLibrary(isPad: traitCollection.userInterfaceIdiom == .pad)
        leftHeaderBg.addSubview(leftHeaderMenuOverlay)
        leftHeaderMenuOverlay.snp.makeConstraints { make in
            make.leading.equalTo(leftHeaderTitleStack)
            make.trailing.equalTo(leftHeaderTitleStack)
            make.top.bottom.equalTo(leftHeaderTitleStack)
        }

        rightToggle.setImage(UIImage(systemName: "circle", withConfiguration: symbolConfig), for: .normal)
        rightToggle.tintColor = headerTextColor
        rightToggle.setContentHuggingPriority(.required, for: .horizontal)
        rightToggle.setContentCompressionResistancePriority(.required, for: .horizontal)
        rightToggle.addTarget(self, action: #selector(rightToggleTapped), for: .touchUpInside)

        configureRightHeaderButton()

        configureHeaderDetailLabel(rightHeaderCountLabel, color: headerTextColor)
        configureHeaderDetailLabel(rightHeaderSizeLabel, color: headerTextColor)

        let rightHeaderTitleStack = UIStackView(arrangedSubviews: [rightHeaderActivityIndicator, rightHeaderLabel, rightHeaderButton])
        rightHeaderTitleStack.axis = .horizontal
        rightHeaderTitleStack.spacing = 4
        rightHeaderTitleStack.alignment = .center
        let rightHeaderTitleRow = UIStackView(arrangedSubviews: [rightToggle, rightHeaderTitleStack])
        rightHeaderTitleRow.axis = .horizontal
        rightHeaderTitleRow.spacing = 4
        rightHeaderTitleRow.alignment = .center

        let rightHeaderContentView = UIView()
        rightHeaderBg.addSubview(rightHeaderContentView)
        rightHeaderContentView.addSubview(rightHeaderTitleRow)
        rightHeaderContentView.addSubview(rightHeaderCountLabel)
        rightHeaderContentView.addSubview(rightHeaderSizeLabel)

        rightHeaderContentView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide.snp.top).offset(10)
            make.bottom.equalTo(rightHeaderBg.snp.bottom).inset(10)
            make.leading.equalTo(rightHeaderBg.snp.leading).inset(12)
            make.trailing.equalTo(view.safeAreaLayoutGuide.snp.trailing).inset(12)
        }
        rightHeaderTitleRow.snp.makeConstraints { make in
            make.top.equalToSuperview()
            make.centerX.equalToSuperview()
            make.leading.greaterThanOrEqualToSuperview()
            make.trailing.lessThanOrEqualToSuperview()
        }
        rightHeaderCountLabel.snp.makeConstraints { make in
            make.top.equalTo(rightHeaderTitleRow.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview()
        }
        rightHeaderSizeLabel.snp.makeConstraints { make in
            make.top.equalTo(rightHeaderCountLabel.snp.bottom).offset(6)
            make.leading.trailing.equalToSuperview()
            make.bottom.equalToSuperview()
        }

        rightHeaderMenuOverlay.showsMenuAsPrimaryAction = true
        rightHeaderMenuOverlay.menu = menuFactory.buildDestination()
        rightHeaderBg.addSubview(rightHeaderMenuOverlay)
        rightHeaderMenuOverlay.snp.makeConstraints { make in
            make.leading.equalTo(rightHeaderTitleStack)
            make.trailing.equalTo(rightHeaderTitleStack)
            make.top.bottom.equalTo(rightHeaderTitleStack)
        }

        HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: leftHeaderCountLabel, sizeLabel: leftHeaderSizeLabel)
        HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: rightHeaderCountLabel, sizeLabel: rightHeaderSizeLabel)

        actionPanel.onExecuteTapped = { [weak self] in self?.executeTapped() }
        actionPanel.onPauseTapped = { [weak self] in self?.store.pauseExecution() }
        actionPanel.onStopTapped = { [weak self] in self?.confirmStop() }
        actionPanel.onResumeTapped = { [weak self] in self?.store.resumeExecution() }
        actionPanel.onCompleteTapped = { [weak self] in self?.store.exitExecution() }
        actionPanel.onExecutionDetailsTapped = { [weak self] in self?.openExecutionLog() }
        view.addSubview(actionPanel)
        actionPanel.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview()
            self.panelShownConstraint = make.bottom.equalToSuperview().constraint
            self.panelHiddenConstraint = make.top.equalTo(view.snp.bottom).constraint
        }
        panelShownConstraint?.deactivate()

        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderBg.snp.bottom)
            make.leading.trailing.equalToSuperview()
            self.collectionBottomToActionPanel = make.bottom.equalTo(actionPanel.snp.top).constraint
        }

        // Local overlay
        configureOverlayContainer(localOverlay)
        configureOverlayLabel(localOverlayLabel)
        localOverlaySpinner.hidesWhenStopped = true
        configureOverlayButtonBase(localOverlayButton)
        localOverlayButton.addTarget(self, action: #selector(localOverlayButtonTapped), for: .touchUpInside)

        let localOverlayStack = makeOverlayStack(
            spinner: localOverlaySpinner,
            label: localOverlayLabel,
            button: localOverlayButton
        )
        localOverlay.addSubview(localOverlayStack)
        localOverlayStack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(16)
        }

        view.addSubview(localOverlay)
        localOverlay.snp.makeConstraints { make in
            make.leading.equalToSuperview()
            make.trailing.equalTo(view.snp.centerX).offset(-1)
            make.top.equalTo(leftHeaderBg.snp.bottom)
            make.bottom.equalTo(actionPanel.snp.top)
        }

        // Remote overlay
        configureOverlayContainer(remoteOverlay)
        embedRemoteNodeOverlay()
        view.addSubview(remoteOverlay)
        remoteOverlay.snp.makeConstraints { make in
            make.leading.equalTo(view.snp.centerX).offset(1)
            make.trailing.equalToSuperview()
            make.top.equalTo(rightHeaderBg.snp.bottom)
            make.bottom.equalTo(actionPanel.snp.top)
        }

        configureSettingsFAB()
        view.addSubview(settingsFAB)
        settingsFAB.snp.makeConstraints { make in
            make.width.height.equalTo(48)
            make.trailing.equalTo(view.safeAreaLayoutGuide.snp.trailing).inset(20)
            self.settingsFABBottomToSafeArea = make.bottom.equalTo(view.safeAreaLayoutGuide.snp.bottom).inset(20).constraint
            self.settingsFABBottomToActionPanel = make.bottom.equalTo(actionPanel.snp.top).offset(-16).constraint
        }
        settingsFABBottomToActionPanel?.deactivate()
        view.bringSubviewToFront(settingsFAB)
    }

    // MARK: - Data Source

    private func configureCell(_ cell: MonthCell, item: Item) {
        cell.onToggle = { [weak self] in self?.toggleSelection(for: item) }
        let summary = (item.side == .local
            ? store.rowLookup[item.month]?.local
            : store.rowLookup[item.month]?.remote)
            ?? HomeMonthSummary(month: item.month, assetCount: 0, photoCount: 0, videoCount: 0, backedUpCount: nil, totalSizeBytes: 0)

        let m = item.month.month

        if let exec = store.executionState, let plan = exec.monthPlans[item.month] {
            switch plan.phase {
            case .completed:
                cell.configureCompleted(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: .tertiaryLabel),
                    sizeText: summary.sizeText
                )
                return
            case .failed:
                cell.configureFailed(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: .tertiaryLabel),
                    sizeText: summary.sizeText
                )
                return
            case .partiallyFailed, .uploading, .downloading, .uploadPaused, .downloadPaused, .uploadDone:
                cell.configureRunning(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: HomeSeasonStyle.monthSecondaryTextColor(month: m)),
                    sizeText: summary.sizeText,
                    bgColor: HomeSeasonStyle.monthColor(month: m),
                    titleColor: HomeSeasonStyle.monthTextColor(month: m),
                    detailColor: HomeSeasonStyle.monthSecondaryTextColor(month: m),
                    showSpinner: plan.isActive
                )
                if plan.phase == .partiallyFailed { cell.showWarningIndicator() }
                if plan.phase == .uploadPaused || plan.phase == .downloadPaused { cell.showPauseIndicator() }
                return
            case .pending:
                break
            }
        }

        let isSelected = item.side == .local
            ? store.selection.localMonths.contains(item.month)
            : store.selection.remoteMonths.contains(item.month)
        let selectionEnabled = store.isSelectable && (item.side == .local || store.isRemoteSelectionAllowed)
        cell.configure(
            monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: HomeSeasonStyle.monthSecondaryTextColor(month: m)),
            sizeText: summary.sizeText,
            bgColor: HomeSeasonStyle.monthColor(month: m),
            titleColor: HomeSeasonStyle.monthTextColor(month: m),
            detailColor: HomeSeasonStyle.monthSecondaryTextColor(month: m),
            isSelected: isSelected,
            selectionEnabled: selectionEnabled
        )
    }

    private func configureDataSource() {
        let cellRegistration = UICollectionView.CellRegistration<MonthCell, Item> { [weak self] cell, _, item in
            self?.configureCell(cell, item: item)
        }

        dataSource = DataSource(collectionView: collectionView) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: item)
        }

        let headerRegistration = UICollectionView.SupplementaryRegistration<MergedSectionHeaderView>(
            elementKind: UICollectionView.elementKindSectionHeader
        ) { [weak self] supplementaryView, _, indexPath in
            guard let self, indexPath.section < self.store.sections.count else { return }
            let section = self.store.sections[indexPath.section]
            let leftState = self.store.selection.selectionState(forRows: section.rows, side: .local)
            let rightState = self.store.selection.selectionState(forRows: section.rows, side: .remote)
            let accentColor = self.leftHeaderLabel.textColor ?? .secondaryLabel
            supplementaryView.configure(section: section,
                                        sectionIndex: indexPath.section,
                                        leftState: leftState, rightState: rightState,
                                        leftSelectionEnabled: self.store.isSelectable,
                                        rightSelectionEnabled: self.store.isSelectable && self.store.isRemoteSelectionAllowed,
                                        selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
            supplementaryView.onLeftToggle = { [weak self] sectionIndex in
                guard self?.confirmSelectionReadiness() == true else { return }
                self?.store.toggleYear(sectionIndex: sectionIndex, side: .local)
            }
            supplementaryView.onRightToggle = { [weak self] sectionIndex in
                guard self?.confirmSelectionReadiness() == true else { return }
                guard self?.confirmRemoteSelectionAllowed() == true else { return }
                self?.store.toggleYear(sectionIndex: sectionIndex, side: .remote)
            }
            supplementaryView.onLeftOpen = { [weak self] sectionIndex in
                self?.openBrowserForYear(sectionIndex: sectionIndex, remote: false)
            }
            supplementaryView.onRightOpen = { [weak self] sectionIndex in
                self?.openBrowserForYear(sectionIndex: sectionIndex, remote: true)
            }
        }

        let arrowRegistration = UICollectionView.SupplementaryRegistration<DirectionArrowView>(
            elementKind: directionArrowElementKind
        ) { [weak self] arrowView, _, indexPath in
            guard let self else { return }
            let itemIndexPath = IndexPath(item: indexPath.item * 2, section: indexPath.section)
            guard let item = self.dataSource.itemIdentifier(for: itemIndexPath) else { return }
            arrowView.configure(
                intent: self.store.intent(for: item.month),
                percent: self.store.progressPercent(for: item.month)
            )
        }

        dataSource.supplementaryViewProvider = { collectionView, kind, indexPath in
            if kind == directionArrowElementKind {
                return collectionView.dequeueConfiguredReusableSupplementary(using: arrowRegistration, for: indexPath)
            }
            return collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
    }

    // MARK: - Store Binding

    private func bindStore() {
        store.onDisconnecting = { [weak self] in
            self?.cancelPendingBrowserLinkConnections()
            self?.endActiveBrowserLink()
        }
        store.onChange = { [weak self] kind in
            guard let self else { return }
            switch kind {
            case .data(let months):    self.renderDataChange(months)
            case .fileSizes(let months): self.renderFileSizeChange(months)
            case .selection:           self.renderSelectionChange()
            case .execution(let months): self.renderExecutionChange(changedMonths: months)
            case .connection:          self.renderConnectionChange()
            case .connectionProgress:  self.updateRemoteOverlay()
            case .structural:          self.renderStructuralChange()
            }
            self.updateSelectionInteraction()
        }

        store.onAlert = { [weak self] title, message in
            self?.showAlert(title: title, message: message)
        }

        store.onNeedsPasswordPrompt = { [weak self] profile, completion in
            self?.presentPasswordPrompt(for: profile, completion: completion)
        }

        store.onNeedsSFTPHostKeyTrust = { [weak self] _, decision, actual in
            guard let self else { return false }
            return await self.presentSFTPHostKeyPrompt(decision: decision, actual: actual)
        }

        store.onConnectFailed = { [weak self] profile, error in
            self?.presentConnectionFailedAlert(profile: profile, error: error)
        }
    }

    // MARK: - Render Methods

    private func renderDataChange(_ months: Set<LibraryMonthKey>) {
        hasLoadedHeaderSummary = true
        reconfigureMonths(months)
        updateTopHeaderSummaries()
        // A side-presence flip shifts the toggle's selectable denominator without changing the selection set,
        // so the global toggle must re-render to stay in step with the section headers reconfigureMonths refreshed.
        updateTopHeaderToggles()
    }

    private func renderFileSizeChange(_ months: Set<LibraryMonthKey>) {
        hasLoadedHeaderSummary = true
        reconfigureMonths(months)
        updateTopHeaderSummaries()
        updateTopHeaderToggles()
    }

    private func renderSelectionChange() {
        let allMonths = Set(store.sections.flatMap { $0.rows.map(\.month) })
        reconfigureMonths(allMonths)
        updateTopHeaderToggles()
        updateActionPanel()
        // Picks up descriptor edits (e.g., album rename) when scope identity didn't change.
        refreshLocalLibraryMenu()
    }

    private func renderExecutionChange(changedMonths: Set<LibraryMonthKey>) {
        if let exec = store.executionState {
            reconfigureMonths(changedMonths.isEmpty ? exec.executionMonths : changedMonths)
            updateTopHeaderSummaries()
            updateActionPanelFromExecution(exec)
            maybeRequestRatingPrompt(for: exec)
            refreshLocalLibraryMenu()
        } else {
            didRequestReviewForCurrentExecution = false
            renderStructuralChange()
        }
    }

    private func maybeRequestRatingPrompt(for exec: HomeExecutionState) {
        guard !didRequestReviewForCurrentExecution,
              exec.phase == .completed,
              exec.failedMonthInfos.isEmpty,
              let scene = view.window?.windowScene else { return }
        didRequestReviewForCurrentExecution = true
        RatingPromptService.requestReviewIfEligible(in: scene)
    }

    private func renderConnectionChange() {
        if case .disconnected = store.connectionState {
            endActiveBrowserLink()
        }
        updateRightHeaderButton()
        renderStructuralChange()
    }

    private func renderStructuralChange() {
        hasLoadedHeaderSummary = true
        applyFullSnapshot()
        refreshLocalLibraryMenu()
        refreshDestinationMenus()
        updateTopHeaderToggles()
        updateTopHeaderSummaries()
        updateActionPanel()
        updateLocalOverlay()
        updateRemoteOverlay()
    }

    // MARK: - Snapshot Helpers

    private func applyFullSnapshot() {
        var snapshot = NSDiffableDataSourceSnapshot<Section, Item>()
        for section in store.sections {
            snapshot.appendSections([.year(section.year)])
            var items: [Item] = []
            items.reserveCapacity(section.rows.count * 2)
            for row in section.rows {
                items.append(Item(side: .local, month: row.month))
                items.append(Item(side: .remote, month: row.month))
            }
            snapshot.appendItems(items)
        }

        let currentSnapshot = dataSource.snapshot()
        let existingItems = snapshot.itemIdentifiers.filter { currentSnapshot.indexOfItem($0) != nil }
        if !existingItems.isEmpty {
            snapshot.reconfigureItems(existingItems)
        }

        dataSource.apply(snapshot, animatingDifferences: false)
        refreshAllVisibleSupplementaries()
    }

    private func refreshAllVisibleSupplementaries() {
        let accentColor = leftHeaderLabel.textColor ?? .secondaryLabel
        for sectionIndex in 0 ..< store.sections.count {
            let ms = store.sections[sectionIndex]
            let headerIndexPath = IndexPath(item: 0, section: sectionIndex)
            if let header = collectionView.supplementaryView(
                forElementKind: UICollectionView.elementKindSectionHeader,
                at: headerIndexPath
            ) as? MergedSectionHeaderView {
                let leftState = store.selection.selectionState(forRows: ms.rows, side: .local)
                let rightState = store.selection.selectionState(forRows: ms.rows, side: .remote)
                header.configure(section: ms,
                                 sectionIndex: sectionIndex,
                                 leftState: leftState, rightState: rightState,
                                 leftSelectionEnabled: store.isSelectable,
                                 rightSelectionEnabled: store.isSelectable && store.isRemoteSelectionAllowed,
                                 selectedColor: accentColor, deselectedColor: .tertiaryLabel)
            }
            for (rowIndex, row) in ms.rows.enumerated() {
                let badgeIndexPath = IndexPath(item: rowIndex, section: sectionIndex)
                if let arrowView = collectionView.supplementaryView(
                    forElementKind: directionArrowElementKind, at: badgeIndexPath
                ) as? DirectionArrowView {
                    arrowView.configure(
                        intent: store.intent(for: row.month),
                        percent: store.progressPercent(for: row.month)
                    )
                }
            }
        }
    }

    private func reconfigureMonths(_ months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }

        var itemsToReconfigure: [Item] = []
        itemsToReconfigure.reserveCapacity(months.count * 2)
        for month in months {
            itemsToReconfigure.append(Item(side: .local, month: month))
            itemsToReconfigure.append(Item(side: .remote, month: month))
        }

        var snapshot = dataSource.snapshot()
        snapshot.reconfigureItems(itemsToReconfigure.filter { snapshot.indexOfItem($0) != nil })
        dataSource.apply(snapshot, animatingDifferences: false)

        let affectedSections = Set(months.compactMap { month in
            store.sections.firstIndex { $0.rows.contains { $0.month == month } }
        })
        let accentColor = leftHeaderLabel.textColor ?? .secondaryLabel
        for sectionIndex in affectedSections {
            let ms = store.sections[sectionIndex]
            let headerIndexPath = IndexPath(item: 0, section: sectionIndex)
            if let header = collectionView.supplementaryView(
                forElementKind: UICollectionView.elementKindSectionHeader,
                at: headerIndexPath
            ) as? MergedSectionHeaderView {
                let leftState = store.selection.selectionState(forRows: ms.rows, side: .local)
                let rightState = store.selection.selectionState(forRows: ms.rows, side: .remote)
                header.configure(section: ms,
                                 sectionIndex: sectionIndex,
                                 leftState: leftState, rightState: rightState,
                                 leftSelectionEnabled: store.isSelectable,
                                 rightSelectionEnabled: store.isSelectable && store.isRemoteSelectionAllowed,
                                 selectedColor: accentColor, deselectedColor: .tertiaryLabel)
            }
            for (rowIndex, row) in ms.rows.enumerated() where months.contains(row.month) {
                let badgeIndexPath = IndexPath(item: rowIndex, section: sectionIndex)
                if let arrowView = collectionView.supplementaryView(
                    forElementKind: directionArrowElementKind, at: badgeIndexPath
                ) as? DirectionArrowView {
                    arrowView.configure(
                        intent: store.intent(for: row.month),
                        percent: store.progressPercent(for: row.month)
                    )
                }
            }
        }
    }

    // MARK: - UI Updates

    private func updateTopHeaderToggles() {
        let allRows = store.sections.flatMap(\.rows)
        let headerColor = leftHeaderLabel.textColor ?? .secondaryLabel
        let config = UIImage.SymbolConfiguration(pointSize: 12)

        func iconName(for state: HomeSelectionState) -> String {
            switch state {
            case .all: return "checkmark.circle.fill"
            case .partial: return "minus.circle.fill"
            case .none: return "circle"
            }
        }

        let localReady = store.localPhotoAccessState.isAuthorized
        // Match the remote overlay/summaries: until the post-connect sync lands, `sections` still hold the
        // prior connection's rows, so the toggle must stay hidden rather than mutate selection from stale truth.
        let remoteReady = store.connectionState.isConnected && store.isRemoteReady

        leftToggle.isHidden = !localReady
        if localReady {
            leftToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(forRows: allRows, side: .local)), withConfiguration: config), for: .normal)
            leftToggle.tintColor = headerColor
        }

        rightToggle.isHidden = !remoteReady
        if remoteReady {
            rightToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(forRows: allRows, side: .remote)), withConfiguration: config), for: .normal)
            rightToggle.tintColor = store.isRemoteSelectionAllowed ? headerColor : .tertiaryLabel
            rightToggle.alpha = store.isRemoteSelectionAllowed ? 1.0 : 0.45
        }
    }

    private func updateTopHeaderSummaries() {
        guard hasLoadedHeaderSummary else {
            HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: leftHeaderCountLabel, sizeLabel: leftHeaderSizeLabel)
            HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: rightHeaderCountLabel, sizeLabel: rightHeaderSizeLabel)
            return
        }

        let headerColor = leftHeaderLabel.textColor ?? .secondaryLabel
        if store.localPhotoAccessState.isAuthorized,
           let summary = HomeHeaderSummaryFormatter.aggregate(rowLookup: store.rowLookup, side: .local, treatsEmptyAsZero: true) {
            HomeHeaderSummaryFormatter.apply(summary, countLabel: leftHeaderCountLabel, sizeLabel: leftHeaderSizeLabel, color: headerColor)
        } else {
            HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: leftHeaderCountLabel, sizeLabel: leftHeaderSizeLabel)
        }

        if store.connectionState.isConnected, store.isRemoteReady,
           let summary = HomeHeaderSummaryFormatter.aggregate(rowLookup: store.rowLookup, side: .remote, treatsEmptyAsZero: true) {
            HomeHeaderSummaryFormatter.apply(summary, countLabel: rightHeaderCountLabel, sizeLabel: rightHeaderSizeLabel, color: headerColor)
        } else {
            HomeHeaderSummaryFormatter.applyPlaceholder(countLabel: rightHeaderCountLabel, sizeLabel: rightHeaderSizeLabel)
        }
    }

    private func configureHeaderDetailLabel(_ label: UILabel, color: UIColor) {
        label.textAlignment = .center
        label.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        label.textColor = color
        label.numberOfLines = 1
        label.lineBreakMode = .byTruncatingTail
    }

    private func configureSettingsFAB() {
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 18, weight: .semibold)
        var configuration: UIButton.Configuration = {
            if #available(iOS 26.0, *) {
                return .glass()
            } else {
                return .borderedTinted()
            }
        }()
        configuration.image = UIImage(systemName: "ellipsis", withConfiguration: symbolConfig)
        configuration.cornerStyle = .capsule
        configuration.contentInsets = .zero
        settingsFAB.configuration = configuration
        if #unavailable(iOS 26.0) {
            settingsFAB.tintColor = .materialPrimary(
                light: .Material.Green._600,
                dark: .Material.Green._200
            )
        }
        settingsFAB.accessibilityLabel = String(localized: "controller.more.title")
        settingsFAB.addTarget(self, action: #selector(openSettings), for: .touchUpInside)
    }

    @objc
    private func openSettings() {
        let configuration = MoreViewControllerConfiguration(
            title: String(localized: "controller.more.title"),
            promotionConfig: PromotionCellConfiguration(
                title: String(localized: "store.promotion.title"),
                titleHighlight: "Pro",
                features: [
                    String(localized: "store.promotion.feature.1"),
                    String(localized: "store.promotion.feature.2"),
                    String(localized: "store.promotion.feature.3"),
                ].map { "- \($0)" },
                gradientColors: [.Material.Green._700, .Material.Green._600],
                titleHighlightColor: .Material.Amber._200,
                featureColor: .white.withAlphaComponent(0.85),
                buttonTextColor: .Material.Green._800,
                buttonTitle: String(localized: "store.promotion.buttonTitle")
            ),
            gratefulConfig: GratefulCellConfiguration(
                title: String(localized: "store.grateful.title"),
                titleHighlight: "Pro",
                content: String(localized: "store.grateful.content"),
                gradientColors: [.Material.LightGreen._700, .Material.LightGreen._600],
                titleHighlightColor: .Material.Amber._200,
                contentColor: .white.withAlphaComponent(0.85)
            ),
            email: "watermelon@zi.ci",
            appStoreId: "6762260596",
            privacyPolicyURL: Self.privacyPolicyURLString,
            specificationsConfig: SpecificationsConfiguration(
                summaryItems: [
                    .init(type: .name, value: SpecificationsViewController.getAppName() ?? ""),
                    .init(type: .version, value: SpecificationsViewController.getAppVersion() ?? ""),
                    .init(type: .manufacturer, value: "@App君"),
                    .init(type: .publisher, value: "ZIZICICI LIMITED"),
                    .init(type: .dateOfProduction, value: "2026/07/12"),
                    .init(type: .license, value: "粤ICP备2025448771号-6A"),
                ],
                thirdPartyLibraries: [
                    .init(name: "AMSMB2", version: "main", urlString: "https://github.com/zizicici/AMSMB2"),
                    .init(name: "Citadel", version: "fix/sftp-response-lock", urlString: "https://github.com/zizicici/Citadel"),
                    .init(name: "GRDB", version: "7.10.0", urlString: "https://github.com/groue/GRDB.swift"),
                    .init(name: "Kingfisher", version: "8.7.0", urlString: "https://github.com/onevcat/Kingfisher"),
                    .init(name: "WatermelonWebRTC", version: "144.7559.1", urlString: "https://github.com/zizicici/WatermelonWebRTC"),
                    .init(name: "MarqueeLabel", version: "4.5.3", urlString: "https://github.com/cbpowell/MarqueeLabel"),
                    .init(name: "SnapKit", version: "5.7.1", urlString: "https://github.com/SnapKit/SnapKit"),
                ]
            ),
            appShowcase: AppShowcaseConfiguration(
                apps: [.lemon, .moontake, .coconut, .festivals, .pigeon, .one, .offDay, .tagDay, .pin, .campfire, .doufu],
                displayCount: 3
            )
        )

        let dataSource = WatermelonMoreDataSource(
            dependencies: dependencies,
            onProfilesChanged: { [weak self] in
                self?.reloadProfiles()
            },
            isMonthGroupingTimeZoneChangeBlocked: { [weak self, dependencies] in
                dependencies.appRuntimeFlags.isExecuting
                    || dependencies.remoteMaintenanceController.isBusy
                    || (self?.store.isLocalIndexReloading ?? false)
            }
        )

        let moreViewController = MoreViewController(configuration: configuration, dataSource: dataSource)

        if let navigationController {
            navigationController.pushViewController(moreViewController, animated: ConsideringUser.pushAnimated)
            return
        }

        let container = UINavigationController(rootViewController: moreViewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func reloadProfiles() {
        store.reloadProfiles()
    }

    private func presentConnectionFailedAlert(profile: ServerProfileRecord, error: Error) {
        let alert = ConnectionFailureAlertFactory.make(profile: profile, error: error) { [weak self] in
            self?.openConnectionEditor(for: profile)
        }
        present(alert, animated: true)
    }

    private func openConnectionEditor(for failedProfile: ServerProfileRecord) {
        let profile = failedProfile.id.flatMap { profileID in
            try? dependencies.databaseManager.fetchServerProfile(id: profileID)
        } ?? failedProfile
        let presentsModally = navigationController == nil
        let editor = StorageProfileConnectionEditorFactory.make(
            dependencies: dependencies,
            profile: profile,
            shouldPopToRootOnSave: false,
            onExternalPersistedWhileInactive: { [weak self] savedProfile in
                guard let self else { return }
                ExternalStoragePersistedProfileRefresh.applyToActiveSession(
                    appSession: self.dependencies.appSession,
                    originalProfile: profile,
                    savedProfile: savedProfile
                )
                self.reloadProfiles()
            }
        ) { [weak self] savedProfile, password in
            guard let self else { return }
            self.handleConnectionEdited(
                originalProfile: profile,
                savedProfile: savedProfile,
                password: password
            )
            if presentsModally {
                self.dismiss(animated: ConsideringUser.animated)
            }
        }

        if let navigationController {
            navigationController.pushViewController(editor, animated: ConsideringUser.pushAnimated)
            return
        }

        editor.navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .close,
            target: self,
            action: #selector(dismissModalFlow)
        )
        let container = UINavigationController(rootViewController: editor)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func handleConnectionEdited(
        originalProfile: ServerProfileRecord,
        savedProfile: ServerProfileRecord,
        password: String
    ) {
        if dependencies.appSession.activeProfile?.id == originalProfile.id {
            if originalProfile.hasSameRemoteDestination(as: savedProfile) {
                dependencies.appSession.activate(profile: savedProfile, password: password)
            } else {
                dependencies.appSession.clear()
            }
        }
        reloadProfiles()
    }

    private func openCurrentProfileSettings() {
        guard let activeID = dependencies.appSession.activeProfile?.id,
              let refreshed = (try? dependencies.databaseManager.fetchServerProfiles())?
                  .first(where: { $0.id == activeID })
        else { return }
        let detail = StorageProfileDetailViewController(
            dependencies: dependencies,
            profile: refreshed,
            onProfilesChanged: { [weak self] in self?.reloadProfiles() },
            onConnectRequested: { [weak self] profile in
                self?.connectRequestedFromManageOrDetail(profile)
            }
        )

        if let navigationController {
            navigationController.pushViewController(detail, animated: ConsideringUser.pushAnimated)
            return
        }

        let container = UINavigationController(rootViewController: detail)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    // Tapping a year header (outside its checkbox) opens the browser at that year's newest month.
    private func openBrowserForYear(sectionIndex: Int, remote: Bool) {
        guard store.sections.indices.contains(sectionIndex),
              let firstMonth = store.sections[sectionIndex].rows.first?.month else { return }
        presentMediaBrowser(initialMonth: firstMonth, initialRemote: remote)
    }

    // The checkbox is the only selection affordance now; the rest of a month cell opens the browser.
    private func toggleSelection(for item: Item) {
        guard confirmSelectionReadiness() else { return }
        switch item.side {
        case .local:
            store.toggleMonth(item.month, side: .local)
        case .remote:
            guard confirmRemoteSelectionAllowed() else { return }
            store.toggleMonth(item.month, side: .remote)
        }
    }

    // Opens the unified browser. Tabs are ordered Local / All / Remote; Remote + All are only selectable
    // while a node is connected (availability re-evaluates live on session changes). `initialRemote`
    // requests the Remote tab when connected, otherwise it opens on Local.
    private func presentMediaBrowser(initialMonth: LibraryMonthKey?, initialRemote: Bool) {
        let browser = makeMediaBrowser(initialMonth: initialMonth, initialRemote: initialRemote)
        // Sheet with a grabber (like the More page): drag-to-dismiss + present/dismiss animation for free.
        let container = UINavigationController(rootViewController: browser)
        if let sheet = container.sheetPresentationController {
            sheet.detents = [.large()]
            sheet.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    // Builds a browser session. `album` scopes it to one user album (local-only, single mode); nil = the
    // full local / merged / remote browser.
    private func makeMediaBrowser(album: LocalAlbumDescriptor? = nil, initialMonth: LibraryMonthKey? = nil, initialRemote: Bool = false) -> MediaBrowserGridViewController {
        // Read-only viewing — safe to open anytime (including while connecting / during backup). Remote
        // and merged tabs gate themselves on the live connection state.
        let dependencies = dependencies

        let isConnected: () -> Bool = {
            dependencies.appSession.activeProfile != nil &&
                dependencies.appSession.activePassword != nil
        }
        let remoteStorageSymbol: () -> String = {
            guard let profile = dependencies.appSession.activeProfile else { return "externaldrive" }
            return profile.isBrowserLinkProfile ? "desktopcomputer" : profile.storageProfile.storageType.symbolName
        }
        // Profile id (nil when disconnected) — lets the browser detect a profile A→B change or a disconnect
        // and rebuild/close its stale remote source.
        let sessionToken: () -> AnyHashable? = {
            dependencies.appSession.activeProfile.map { AnyHashable($0.runtimeConnectionIdentity) }
        }
        // Full profile key of the connected remote (nil when disconnected) — lets LocalMediaSource gate
        // `.both` presence on the snapshot actually belonging to the current connection.
        let sessionProfileKey: () -> String? = {
            dependencies.appSession.activeProfile.map { RemoteIndexSyncService.remoteProfileKey($0) }
        }
        // One presence authority for the whole browser session, shared by every source + thumbnail service +
        // action runner. It self-corrects on a profile A→B switch (profileKey read live).
        let presenceIndex = LibraryPresenceIndex(
            hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: dependencies.backupCoordinator,
            profileKey: sessionProfileKey,
            localIndexChangePublisher: dependencies.localIndexChangePublisher
        )
        func makeLocalSource(query: PhotoLibraryQuery = .allAssets) -> LocalMediaSource {
            LocalMediaSource(
                photoLibraryService: dependencies.photoLibraryService,
                hashIndexRepository: dependencies.hashIndexRepository,
                presenceIndex: presenceIndex,
                query: query
            )
        }
        func makeRemoteService() -> RemoteThumbnailService? {
            guard let profile = dependencies.appSession.activeProfile,
                  let password = dependencies.appSession.activePassword else { return nil }
            return RemoteThumbnailService(
                storageClientFactory: dependencies.storageClientFactory,
                presenceIndex: presenceIndex,
                profile: profile,
                password: password
            )
        }

        // A scoped album browses local-only (an album is a device collection); the full browser offers all modes.
        let specs: [MediaBrowserGridViewController.ModeSpec]
        if let album {
            specs = [.init(mode: .local, isAvailable: { true }, makeSource: { makeLocalSource(query: .albums([album.localIdentifier])) })]
        } else {
            specs = [
                .init(mode: .local, isAvailable: { true }, makeSource: { makeLocalSource() }),
                .init(mode: .merged, isAvailable: isConnected, makeSource: {
                    guard let service = makeRemoteService() else { return makeLocalSource() }
                    return MergedMediaSource(
                        localSource: makeLocalSource(),
                        remoteSource: RemoteMediaSource(service: service, coordinator: dependencies.backupCoordinator)
                    )
                }),
                .init(mode: .remote, isAvailable: isConnected, makeSource: {
                    guard let service = makeRemoteService() else { return makeLocalSource() }
                    return RemoteMediaSource(service: service, coordinator: dependencies.backupCoordinator)
                }),
            ]
        }
        let initialMode: MediaBrowserMode = (album == nil && initialRemote && isConnected()) ? .remote : .local

        let actionRunner = MediaBrowserActionRunner(env: .init(
            appSession: dependencies.appSession,
            backupCoordinator: dependencies.backupCoordinator,
            restoreService: dependencies.restoreService,
            photoLibraryService: dependencies.photoLibraryService,
            hashIndexRepository: dependencies.hashIndexRepository,
            presenceIndex: presenceIndex,
            appRuntimeFlags: dependencies.appRuntimeFlags,
            // Upload/download/delete are disallowed while a backup/download/maintenance task is running.
            isTaskActive: { [weak self] in
                guard let self else { return false }
                return self.store.isExecutionActive || self.store.isMaintenanceBlocked
            },
            iCloudPhotoBackupMode: { ICloudPhotoBackupMode.getValue() },
            monthGroupingTimeZone: { [weak self] in
                self?.store.dataManager.monthGroupingTimeZoneForLocalIndex() ?? .frozenCurrent()
            }
        ))
        return MediaBrowserGridViewController(
            specs: specs,
            initialMode: initialMode,
            initialMonth: initialMonth,
            remoteStorageSymbol: remoteStorageSymbol,
            sessionToken: sessionToken,
            actionRunner: actionRunner,
            presenceIndex: presenceIndex,
            title: album?.title ?? String(localized: "home.menu.browseRemoteAlbum")
        )
    }

    private func openManageProfiles() {
        let viewController = ManageStorageProfilesViewController(
            dependencies: dependencies,
            onProfilesChanged: { [weak self] in self?.reloadProfiles() },
            onConnectRequested: { [weak self] profile in
                self?.connectRequestedFromManageOrDetail(profile)
            }
        )

        if let navigationController {
            navigationController.pushViewController(viewController, animated: ConsideringUser.pushAnimated)
            return
        }

        let container = UINavigationController(rootViewController: viewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    /// Dismiss before connecting — the password prompt is presented by `self` and
    /// would land in the wrong context if manage/detail still occupies the presenter.
    private func connectRequestedFromManageOrDetail(_ profile: ServerProfileRecord) {
        let connect: () -> Void = { [weak self] in self?.store.connectProfile(profile) }
        if presentedViewController != nil {
            dismiss(animated: ConsideringUser.animated, completion: connect)
        } else {
            connect()
        }
    }

    private func openLocalAlbumPicker() {
        guard store.canChangeLocalSource else { return }
        guard store.localPhotoAccessState.isAuthorized else {
            localOverlayButtonTapped()
            return
        }

        let viewController = LocalAlbumPickerViewController(
            photoLibraryService: dependencies.photoLibraryService,
            selectedAlbumIDs: store.localLibraryScope.selectedAlbumIdentifiers,
            makeAlbumBrowser: { [weak self] album in self?.makeMediaBrowser(album: album) }
        ) { [weak self] albums in
            self?.store.setLocalLibraryScope(
                .albums(Set(albums.map(\.localIdentifier))),
                descriptors: albums
            )
            self?.refreshLocalLibraryMenu()
        }

        let container = UINavigationController(rootViewController: viewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
            presentation.detents = [.medium(), .large()]
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func openLocalIndex() {
        guard store.canChangeLocalSource else { return }
        guard store.localPhotoAccessState.isAuthorized else {
            localOverlayButtonTapped()
            return
        }

        let viewController = LocalIndexViewController(
            coordinator: dependencies.localIndexBuildCoordinator,
            photoLibraryService: dependencies.photoLibraryService,
            hashIndexRepository: dependencies.hashIndexRepository
        )

        let container = UINavigationController(rootViewController: viewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
            presentation.detents = [.medium(), .large()]
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func openDuplicates() {
        guard store.canChangeLocalSource else { return }
        guard store.localPhotoAccessState.isAuthorized else {
            localOverlayButtonTapped()
            return
        }

        let viewController = DuplicatesViewController(
            coordinator: dependencies.localIndexBuildCoordinator,
            hashIndexRepository: dependencies.hashIndexRepository,
            photoLibraryService: dependencies.photoLibraryService,
            changePublisher: dependencies.localIndexChangePublisher
        )

        let container = UINavigationController(rootViewController: viewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
            presentation.detents = [.large()]
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func openExecutionLog() {
        guard store.executionState != nil else { return }

        let viewController = HomeExecutionLogViewController(
            coordinator: store.executionCoordinator
        )

        if let navigationController {
            navigationController.pushViewController(viewController, animated: ConsideringUser.pushAnimated)
            return
        }

        let container = UINavigationController(rootViewController: viewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
            presentation.detents = [.large()]
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func updateActionPanel() {
        if let exec = store.executionState {
            updateActionPanelFromExecution(exec)
            return
        }

        let counts = store.selection.counts()
        actionPanel.render(
            state: SelectionActionPanelViewStateBuilder.selection(
                backupCount: counts.backup,
                downloadCount: counts.download,
                complementCount: counts.complement,
                // A runtime-only execution (browser action / maintenance) or a local-index reload holds the run
                // even though executionState is nil here — disable Execute so it isn't a tappable no-op. Also
                // disabled during the async start-resolution window (isStartingExecution).
                canExecute: !store.isExecutionActive && !store.isLocalIndexReloading && !isStartingExecution
            ),
            menus: SelectionActionPanelMenus(
                backup: menuFactory.buildCategory(for: .backup),
                download: menuFactory.buildCategory(for: .download),
                complement: menuFactory.buildCategory(for: .complement)
            )
        )

        let shouldShow = !store.selection.isEmpty
        if shouldShow && !isPanelShown {
            setActionPanelVisible(true, animated: true)
        } else if !shouldShow && isPanelShown {
            setActionPanelVisible(false, animated: true)
        }
    }

    private func updateActionPanelFromExecution(_ exec: HomeExecutionState) {
        if !isPanelShown {
            setActionPanelVisible(true, animated: false)
        }

        actionPanel.render(
            state: SelectionActionPanelViewStateBuilder.execution(from: exec),
            menus: SelectionActionPanelMenus(
                backup: nil,
                download: nil,
                complement: nil
            )
        )
    }

    private func setActionPanelVisible(_ visible: Bool, animated: Bool) {
        isPanelShown = visible

        if visible {
            panelHiddenConstraint?.deactivate()
            panelShownConstraint?.activate()
            settingsFABBottomToSafeArea?.deactivate()
            settingsFABBottomToActionPanel?.activate()
        } else {
            panelShownConstraint?.deactivate()
            panelHiddenConstraint?.activate()
            settingsFABBottomToActionPanel?.deactivate()
            settingsFABBottomToSafeArea?.activate()
        }

        let animations = { self.view.layoutIfNeeded() }
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

    private func updateSelectionInteraction() {
        let canAttemptSelection = store.isSelectable
        // Tapping a cell opens the browser (selection is via the checkbox), so it must stay tappable even
        // when month selection is disabled — e.g. while connecting or disconnected.
        collectionView.allowsSelection = true
        leftToggle.isEnabled = canAttemptSelection
        rightToggle.isEnabled = canAttemptSelection && store.isRemoteReady
        leftHeaderMenuOverlay.isEnabled = store.canChangeLocalSource
        leftHeaderButton.isEnabled = store.canChangeLocalSource
        rightHeaderMenuOverlay.isEnabled = store.canInteractWithRemoteNode
        rightHeaderButton.isEnabled = store.canInteractWithRemoteNode
    }

    private func updateLocalOverlay() {
        if store.localPhotoAccessState != .authorized, store.isLocalIndexReloadUnderway {
            localOverlay.isHidden = false
            localOverlaySpinner.startAnimating()
            localOverlayLabel.text = String(localized: "home.overlay.scanningLibrary")
            localOverlayButton.isHidden = true
            return
        }
        localOverlayButton.isHidden = false
        switch store.localPhotoAccessState {
        case .authorized:
            localOverlay.isHidden = true
            localOverlaySpinner.stopAnimating()
        case .notDetermined:
            localOverlay.isHidden = false
            localOverlaySpinner.stopAnimating()
            localOverlayLabel.text = String(localized: "home.overlay.authRequired")
            updateLocalOverlayButton(title: String(localized: "home.overlay.allowAccess"))
        case .denied:
            localOverlay.isHidden = false
            localOverlaySpinner.stopAnimating()
            localOverlayLabel.text = String(localized: "home.overlay.noAuth")
            updateLocalOverlayButton(title: String(localized: "home.overlay.goToSettings"))
        }
    }

    private func updateRemoteOverlay() {
        let canInteractWithNodeOverlay = store.canInteractWithRemoteNode
        switch store.connectionState {
        case .connecting:
            remoteOverlay.isHidden = false
            let progress = store.remoteSyncProgress
            remoteNodeOverlayController.render(
                mode: .progress(
                    message: progress.map { remoteOverlayMessage(for: $0) } ?? String(localized: "home.overlay.scanningIndex"),
                    showsDisconnect: progress.map { !$0.isRepoUpgrade } ?? true
                ),
                profiles: store.savedProfiles,
                reachability: { [store] in store.reachability(for: $0) },
                isInteractionEnabled: canInteractWithNodeOverlay
            )
        case .disconnected:
            remoteOverlay.isHidden = false
            remoteNodeOverlayController.render(
                mode: store.savedProfiles.isEmpty ? .emptySetup : .profileSelection,
                profiles: store.savedProfiles,
                reachability: { [store] in store.reachability(for: $0) },
                isInteractionEnabled: canInteractWithNodeOverlay
            )
        case .connected where store.isRemoteReady:
            remoteOverlay.isHidden = true
            remoteNodeOverlayController.stopProgressAnimation()
        case .connected:
            // .connected flips true before the post-switch syncRemote replaces the prior remote's
            // rows; keep the column covered until that sync lands so the old data never shows.
            remoteOverlay.isHidden = false
            let progress = store.remoteSyncProgress
            remoteNodeOverlayController.render(
                mode: .progress(
                    message: progress.map { remoteOverlayMessage(for: $0) } ?? String(localized: "home.overlay.scanningIndex"),
                    showsDisconnect: progress.map { !$0.isRepoUpgrade } ?? true
                ),
                profiles: store.savedProfiles,
                reachability: { [store] in store.reachability(for: $0) },
                isInteractionEnabled: canInteractWithNodeOverlay
            )
        }
    }

    private func updateRightHeaderButton() {
        switch store.connectionState {
        case .connecting(let profile):
            rightHeaderActivityIndicator.startAnimating()
            updateRightHeaderTitle(for: profile)
        case .connected(let profile):
            rightHeaderActivityIndicator.stopAnimating()
            updateRightHeaderTitle(for: profile)
        case .disconnected:
            rightHeaderActivityIndicator.stopAnimating()
            rightHeaderLabel.text = String(localized: "home.header.remoteStorage")
        }
        refreshDestinationMenus()
    }

    private func updateRightHeaderTitle(for profile: ServerProfileRecord) {
        let color = rightHeaderLabel.textColor ?? .label
        let font = rightHeaderLabel.font ?? .systemFont(ofSize: 15, weight: .semibold)
        let iconHeight: CGFloat = 14
        let attachment = NSTextAttachment()
        let symbolName = profile.isBrowserLinkProfile ? "desktopcomputer" : profile.storageProfile.storageType.symbolName
        let image = UIImage(
            systemName: symbolName,
            withConfiguration: UIImage.SymbolConfiguration(pointSize: iconHeight, weight: .semibold)
        )?.withTintColor(color, renderingMode: .alwaysOriginal)
        attachment.image = image
        let imageSize = image?.size ?? CGSize(width: iconHeight, height: iconHeight)
        let iconWidth = imageSize.height > 0 ? imageSize.width * iconHeight / imageSize.height : iconHeight
        attachment.bounds = CGRect(x: 0, y: (font.capHeight - iconHeight) / 2, width: iconWidth, height: iconHeight)

        let title = NSMutableAttributedString(attachment: attachment)
        title.append(NSAttributedString(string: " \(profile.name)", attributes: [
            .font: font,
            .foregroundColor: color,
        ]))
        rightHeaderLabel.attributedText = title
    }

    private func updateLocalOverlayButton(title: String) {
        var configuration = localOverlayButton.configuration ?? UIButton.Configuration.plain()
        configuration.title = title
        configuration.image = nil
        configuration.imagePadding = 0
        localOverlayButton.configuration = configuration
    }

    private func configureOverlayContainer(_ overlay: UIView) {
        overlay.backgroundColor = .appBackground
        overlay.isHidden = true
    }

    private func embedRemoteNodeOverlay() {
        remoteNodeOverlayController.onProfileSelected = { [weak self] profile in
            self?.store.connectProfile(profile)
        }
        remoteNodeOverlayController.onCreateDestinationSelected = { [weak self] destination in
            self?.openNewStorageFlow(destination)
        }
        remoteNodeOverlayController.onDisconnect = { [weak self] in
            self?.store.disconnect()
        }
        addChild(remoteNodeOverlayController)
        remoteOverlay.addSubview(remoteNodeOverlayController.view)
        remoteNodeOverlayController.view.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
        remoteNodeOverlayController.didMove(toParent: self)
    }

    private func configureOverlayLabel(_ label: UILabel) {
        label.textAlignment = .center
        label.numberOfLines = 0
        label.font = .systemFont(ofSize: 15, weight: .medium)
        label.textColor = .secondaryLabel
    }

    private func configureOverlayButtonBase(_ button: UIButton) {
        var configuration = UIButton.Configuration.plain()
        configuration.imagePlacement = .trailing
        configuration.imagePadding = 4
        configuration.baseForegroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        button.configuration = configuration
    }

    private func makeOverlayStack(
        spinner: UIActivityIndicatorView,
        label: UILabel,
        button: UIButton
    ) -> UIStackView {
        let stack = UIStackView(arrangedSubviews: [spinner, label, button])
        stack.axis = .vertical
        stack.spacing = 12
        stack.alignment = .center
        return stack
    }

    private func observeApplicationLifecycle() {
        didBecomeActiveObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.store.refreshLocalPhotoAccessIfNeeded()
            }
        }
    }

    private func configureLeftHeaderButton() {
        let headerTextColor = UIColor.materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100)
        leftHeaderLabel.text = headerTitle(for: store.localLibraryScope)

        var config = UIButton.Configuration.plain()
        config.image = UIImage(systemName: "chevron.down", withConfiguration: UIImage.SymbolConfiguration(pointSize: 11))
        config.contentInsets = .init(top: 0, leading: 0, bottom: 0, trailing: 0)
        config.baseForegroundColor = headerTextColor
        leftHeaderButton.configuration = config
        leftHeaderButton.showsMenuAsPrimaryAction = true
        leftHeaderButton.setContentHuggingPriority(.required, for: .horizontal)
        leftHeaderButton.setContentCompressionResistancePriority(.required, for: .horizontal)
        refreshLocalLibraryMenu()
    }

    private func refreshLocalLibraryMenu() {
        leftHeaderLabel.text = headerTitle(for: store.localLibraryScope)
        let menu = menuFactory.buildLocalLibrary(isPad: traitCollection.userInterfaceIdiom == .pad)
        leftHeaderButton.menu = menu
        leftHeaderMenuOverlay.menu = menu
    }

    private func headerTitle(for scope: HomeLocalLibraryScope) -> String {
        switch scope {
        case .allPhotos:
            return String(localized: "home.localSource.allPhotos")
        case .albums(let ids):
            if ids.count == 1, let id = ids.first, let descriptor = store.albumDisplayCache[id] {
                return descriptor.title
            }
            return String.localizedStringWithFormat(
                String(localized: "home.localSource.albumCount"),
                ids.count
            )
        }
    }

    private func configureRightHeaderButton() {
        let headerTextColor = UIColor.materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100)
        rightHeaderLabel.text = String(localized: "home.header.remoteStorage")
        rightHeaderLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        rightHeaderLabel.textColor = headerTextColor
        rightHeaderLabel.textAlignment = .center
        rightHeaderActivityIndicator.color = headerTextColor
        rightHeaderActivityIndicator.hidesWhenStopped = true
        rightHeaderActivityIndicator.transform = CGAffineTransform(scaleX: 0.78, y: 0.78)
        rightHeaderActivityIndicator.stopAnimating()

        var config = UIButton.Configuration.plain()
        config.image = UIImage(systemName: "chevron.down", withConfiguration: UIImage.SymbolConfiguration(pointSize: 11))
        config.contentInsets = .init(top: 0, leading: 0, bottom: 0, trailing: 0)
        config.baseForegroundColor = headerTextColor
        rightHeaderButton.configuration = config
        rightHeaderButton.showsMenuAsPrimaryAction = true
        rightHeaderButton.setContentHuggingPriority(.required, for: .horizontal)
        rightHeaderButton.setContentCompressionResistancePriority(.required, for: .horizontal)
        refreshDestinationMenus()
    }

    private func refreshDestinationMenus() {
        let menu = menuFactory.buildDestination()
        rightHeaderButton.menu = menu
        rightHeaderMenuOverlay.menu = menu
    }

    // MARK: - User Actions

    @discardableResult
    func prepareForIncomingBrowserLink(_ url: URL) -> Bool {
        guard (try? BrowserLinkPairing.parse(url)) != nil else { return false }
        store.setBrowserLinkSessionActive(true)
        return true
    }

    func handleBrowserLinkURL(_ url: URL) {
        do {
            let pairing = try BrowserLinkPairing.parse(url)
            startBrowserLink(pairing)
        } catch {
            clearPreparedBrowserLinkGateIfNeeded()
            presentBrowserLinkAlert(
                title: String(localized: "link.connection.invalidTitle"),
                message: error.localizedDescription
            )
        }
    }

    private func presentBrowserLinkAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .cancel))
        (presentedViewController ?? self).present(alert, animated: true)
    }

    private func openBrowserLinkScanner() {
        if let message = browserLinkUnavailableMessage {
            clearPreparedBrowserLinkGateIfNeeded()
            presentBrowserLinkAlert(
                title: String(localized: "link.connection.unavailableTitle"),
                message: message
            )
            return
        }
        guard BrowserLinkTutorialViewController.CompletionGate.hasCompleted else {
            let sessionID = beginBrowserLinkSession()
            presentBrowserLinkTutorial(
                from: self,
                startsScannerOnCompletion: true,
                sessionID: sessionID
            )
            return
        }
        presentBrowserLinkScanner()
    }

    private func presentBrowserLinkScanner(sessionID: UUID? = nil) {
        let sessionID = sessionID ?? beginBrowserLinkSession()
        let scanner = BrowserLinkScannerViewController()
        let container = makeBrowserLinkContainer(rootViewController: scanner, sessionID: sessionID)
        scanner.onTutorial = { [weak self, weak scanner] in
            guard let self, let scanner else { return }
            self.presentBrowserLinkTutorial(from: scanner, startsScannerOnCompletion: false)
        }
        scanner.onPairing = { [weak self, weak container] pairing in
            guard let self, let container else { return }
            if let message = self.browserLinkUnavailableMessage {
                container.dismiss(animated: ConsideringUser.animated) { [weak self] in
                    self?.presentBrowserLinkAlert(
                        title: String(localized: "link.connection.unavailableTitle"),
                        message: message
                    )
                }
                return
            }
            container.isModalInPresentation = true
            container.pushViewController(
                makeBrowserLinkConnection(pairing: pairing, container: container),
                animated: ConsideringUser.pushAnimated
            )
        }
        if let presentation = container.sheetPresentationController {
            presentation.detents = [.large()]
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func presentBrowserLinkTutorial(
        from presenter: UIViewController,
        startsScannerOnCompletion: Bool,
        sessionID: UUID? = nil
    ) {
        let tutorial = BrowserLinkTutorialViewController(allowsDismissal: !startsScannerOnCompletion)
        let navigation = UINavigationController(rootViewController: tutorial)
        navigation.modalPresentationStyle = .pageSheet
        navigation.isModalInPresentation = startsScannerOnCompletion
        if let presentation = navigation.sheetPresentationController {
            presentation.detents = [.large()]
            presentation.prefersGrabberVisible = !startsScannerOnCompletion
        }
        if let scanner = presenter as? BrowserLinkScannerViewController {
            tutorial.onDismissed = { [weak scanner] in
                scanner?.resumeAfterTutorial()
            }
        }
        tutorial.onCompleted = { [weak self, weak navigation] in
            BrowserLinkTutorialViewController.CompletionGate.markCompleted()
            navigation?.dismiss(animated: ConsideringUser.animated) { [weak self] in
                guard startsScannerOnCompletion,
                      let self,
                      let sessionID,
                      self.browserLinkSessionID == sessionID else { return }
                self.presentBrowserLinkScanner(sessionID: sessionID)
            }
        }
        presenter.present(navigation, animated: ConsideringUser.animated)
    }

    private var browserLinkUnavailableMessage: String? {
        switch BrowserLinkStartPolicy.blockReason(
            isConnected: store.connectionState.isConnected,
            isConnecting: store.connectionState.isConnecting,
            canInteractWithRemoteNode: store.canInteractWithRemoteNode
        ) {
        case .existingConnection:
            return String(localized: "link.connection.disconnectFirst")
        case .busy:
            return String(localized: "link.connection.busy")
        case nil:
            return nil
        }
    }

    private func startBrowserLink(_ pairing: BrowserLinkPairing) {
        if let message = browserLinkUnavailableMessage {
            clearPreparedBrowserLinkGateIfNeeded()
            presentBrowserLinkAlert(
                title: String(localized: "link.connection.unavailableTitle"),
                message: message
            )
            return
        }
        if isReplacingBrowserLink {
            pendingBrowserLinkPairing = pairing
            return
        }
        if let presented = presentedViewController {
            guard let navigation = presented as? UINavigationController,
                  navigation.viewControllers.contains(where: {
                      $0 is BrowserLinkConnectionViewController || $0 is BrowserLinkScannerViewController
                  }) else {
                presentBrowserLinkAlert(
                    title: String(localized: "link.connection.unavailableTitle"),
                    message: String(localized: "link.connection.closeCurrentScreen")
                )
                clearPreparedBrowserLinkGateIfNeeded()
                return
            }
            cancelPendingBrowserLinkConnections()
            let sessionID = beginBrowserLinkSession()
            isReplacingBrowserLink = true
            navigation.viewControllers
                .compactMap { $0 as? BrowserLinkConnectionViewController }
                .forEach { $0.stopConnection() }
            navigation.dismiss(animated: false) { [weak self] in
                guard let self else { return }
                self.isReplacingBrowserLink = false
                guard self.browserLinkSessionID == sessionID else { return }
                let nextPairing = self.pendingBrowserLinkPairing ?? pairing
                self.pendingBrowserLinkPairing = nil
                self.presentBrowserLinkConnection(nextPairing, sessionID: sessionID)
            }
            return
        }
        presentBrowserLinkConnection(pairing, sessionID: beginBrowserLinkSession())
    }

    private func presentBrowserLinkConnection(_ pairing: BrowserLinkPairing, sessionID: UUID) {
        let connection = BrowserLinkConnectionViewController(
            pairing: pairing,
            transferRateLimitBytesPerSecond: browserLinkTransferRateLimitBytesPerSecond
        )
        let container = makeBrowserLinkContainer(rootViewController: connection, sessionID: sessionID)
        configureBrowserLinkConnection(connection, pairing: pairing, container: container)
        container.isModalInPresentation = true
        if let presentation = container.sheetPresentationController {
            presentation.detents = [.large()]
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func makeBrowserLinkConnection(
        pairing: BrowserLinkPairing,
        container: UINavigationController
    ) -> BrowserLinkConnectionViewController {
        let connection = BrowserLinkConnectionViewController(
            pairing: pairing,
            transferRateLimitBytesPerSecond: browserLinkTransferRateLimitBytesPerSecond
        )
        configureBrowserLinkConnection(connection, pairing: pairing, container: container)
        return connection
    }

    private var browserLinkTransferRateLimitBytesPerSecond: Int? {
        BrowserLinkTransferRatePolicy.maximumBytesPerSecond(
            rateLimitEnabled: BrowserLinkRateLimitSetting.getValue() == .enable
        )
    }

    private func configureBrowserLinkConnection(
        _ connection: BrowserLinkConnectionViewController,
        pairing: BrowserLinkPairing,
        container: UINavigationController
    ) {
        connection.onAuthenticated = { [weak self, weak connection, weak container] client in
            guard let self, let connection, let container else { return }
            let storageClient = BrowserLinkStorageClient(client: client)
            let profile = BrowserLinkStorageClient.makeProfile(
                pairing: pairing,
                folderName: client.remoteFolderName,
                browserNodeID: client.remoteBrowserNodeID,
                reclaimBrowserNodeIDs: client.reclaimBrowserNodeIDs
            )
            let registration = self.dependencies.storageClientFactory.registerBrowserLink(
                sessionID: pairing.sessionID,
                client: storageClient
            )
            self.pendingBrowserLinkRegistrations[pairing.sessionID] = registration
            self.store.setBrowserLinkTransportActive(true)
            self.store.connectBrowserLink(profile: profile) { [weak self, weak connection, weak container] result in
                guard let self else { return }
                switch result {
                case .success:
                    self.activeBrowserLinkClient = client
                    self.activeBrowserLinkSessionID = pairing.sessionID
                    self.activeBrowserLinkRegistration = registration
                    self.pendingBrowserLinkRegistrations.removeValue(forKey: pairing.sessionID)
                    client.onTerminalFailure = { [weak self, weak client] error in
                        guard let self,
                              let client,
                              self.activeBrowserLinkClient === client,
                              self.activeBrowserLinkSessionID == pairing.sessionID else { return }
                        let message = Self.browserLinkTerminalMessage(error)
                        self.store.browserLinkTransportDidClose(message: message)
                        self.endActiveBrowserLink()
                    }
                    guard client.isFileSystemReady else {
                        connection?.showConnectionFailure(BrowserLinkClientError.connectionClosed)
                        self.endActiveBrowserLink()
                        return
                    }
                    connection?.markHandedOff()
                    container?.dismiss(animated: ConsideringUser.animated)
                case .failure(let error):
                    self.dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
                    self.pendingBrowserLinkRegistrations.removeValue(forKey: pairing.sessionID)
                    self.store.setBrowserLinkTransportActive(false)
                    connection?.showConnectionFailure(error)
                }
            }
        }
    }

    private func endActiveBrowserLink() {
        let hadActiveTransport = activeBrowserLinkClient != nil || activeBrowserLinkRegistration != nil
        activeBrowserLinkClient?.stop()
        activeBrowserLinkClient = nil
        activeBrowserLinkSessionID = nil
        if let activeBrowserLinkRegistration {
            dependencies.storageClientFactory.unregisterBrowserLink(token: activeBrowserLinkRegistration)
            self.activeBrowserLinkRegistration = nil
        }
        if hadActiveTransport {
            store.connectionController.disconnectBrowserLink()
        }
        store.setBrowserLinkTransportActive(false)
    }

    private static func browserLinkTerminalMessage(_ error: Error) -> String {
        guard let error = error as? BrowserLinkClientError else { return error.localizedDescription }
        switch error {
        case .peerLeft:
            return String(localized: "link.error.desktopLeft")
        case .connectionClosed:
            return String(localized: "link.error.connection")
        case .localNetworkRequired:
            return String(localized: "link.connection.sameNetworkHint")
        case .invalidServerMessage, .invalidSignal, .unexpectedSignal:
            return String(localized: "link.error.invalidSignal")
        case .requestFailed:
            return String(localized: "link.error.connection")
        case .webRTCUnavailable, .authenticationFailed:
            return error.localizedDescription
        }
    }

    private func clearPreparedBrowserLinkGateIfNeeded() {
        guard browserLinkSessionID == nil else { return }
        store.setBrowserLinkSessionActive(false)
    }

    private func cancelPendingBrowserLinkConnections() {
        for (pendingSessionID, registration) in pendingBrowserLinkRegistrations {
            store.cancelBrowserLinkConnection(sessionID: pendingSessionID)
            dependencies.storageClientFactory.unregisterBrowserLink(token: registration)
        }
        pendingBrowserLinkRegistrations.removeAll()
    }

    private func beginBrowserLinkSession() -> UUID {
        let sessionID = UUID()
        browserLinkSessionID = sessionID
        store.setBrowserLinkSessionActive(true)
        store.connectionController.suppressAutoConnectForBrowserLink()
        return sessionID
    }

    private func makeBrowserLinkContainer(
        rootViewController: UIViewController,
        sessionID: UUID
    ) -> BrowserLinkSessionNavigationController {
        let container = BrowserLinkSessionNavigationController(rootViewController: rootViewController)
        container.onSessionEnded = { [weak self] in
            guard let self, self.browserLinkSessionID == sessionID else { return }
            self.browserLinkSessionID = nil
            self.cancelPendingBrowserLinkConnections()
            if self.activeBrowserLinkClient == nil {
                self.store.setBrowserLinkTransportActive(false)
            }
            self.store.setBrowserLinkSessionActive(false)
        }
        return container
    }

    private func openNewStorageFlow(_ destination: NewStorageDestination) {
        if !ProStatus.isPro && store.savedProfiles.count >= 1 {
            presentProUpgradeAlert()
            return
        }

        let onSaved: (ServerProfileRecord, String) -> Void = { [weak self] profile, _ in
            self?.handleStorageCreated(profile)
        }
        let onExternalPersistedWhileInactive: (ServerProfileRecord) -> Void = { [weak self] _ in
            self?.reloadProfiles()
        }

        if let navigationController {
            let rootViewController = makeNewStorageRootViewController(
                for: destination,
                shouldPopToRootOnSave: true,
                onExternalPersistedWhileInactive: onExternalPersistedWhileInactive,
                onSaved: onSaved
            )
            navigationController.pushViewController(rootViewController, animated: ConsideringUser.pushAnimated)
            return
        }

        let rootViewController = makeNewStorageRootViewController(
            for: destination,
            shouldPopToRootOnSave: false,
            onExternalPersistedWhileInactive: onExternalPersistedWhileInactive
        ) { [weak self] profile, _ in
            self?.dismiss(animated: ConsideringUser.animated) {
                self?.handleStorageCreated(profile)
            }
        }
        rootViewController.navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .close,
            target: self,
            action: #selector(dismissModalFlow)
        )
        let container = UINavigationController(rootViewController: rootViewController)
        if let presentation = container.sheetPresentationController {
            presentation.prefersGrabberVisible = true
        }
        present(container, animated: ConsideringUser.animated)
    }

    private func presentProUpgradeAlert() {
        let alert = UIAlertController(
            title: String(localized: "home.alert.upgradeTitle"),
            message: String(localized: "home.alert.upgradeMessage"),
            preferredStyle: .alert
        )
        if let price = Store.shared.membershipDisplayPrice() {
            alert.addAction(UIAlertAction(title: String(format: String(localized: "home.alert.upgradeAction"), price), style: .default) { [weak self] _ in
                Task { @MainActor [weak self] in
                    do {
                        _ = try await Store.shared.purchaseLifetimeMembership()
                    } catch {
                        self?.presentPurchaseError(error)
                    }
                }
            })
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        present(alert, animated: true)
    }

    private func presentPurchaseError(_ error: Error) {
        let alert = UIAlertController(
            title: nil,
            message: error.localizedDescription,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func makeNewStorageRootViewController(
        for destination: NewStorageDestination,
        shouldPopToRootOnSave: Bool,
        onExternalPersistedWhileInactive: @escaping (ServerProfileRecord) -> Void,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) -> UIViewController {
        switch destination {
        case .smb:
            return AddSMBServerLoginViewController(
                dependencies: dependencies,
                draft: SMBServerLoginDraft(
                    name: "",
                    host: "",
                    port: SMBEndpoint.defaultPort,
                    username: "",
                    domain: nil
                ),
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onSaved: onSaved
            )
        case .smbDiscovery:
            return SMBLocalDiscoveryViewController(
                dependencies: dependencies,
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onSaved: onSaved
            )
        case .webdav:
            return AddWebDAVStorageViewController(
                dependencies: dependencies,
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onSaved: onSaved
            )
        case .externalVolume:
            return AddExternalStorageViewController(
                dependencies: dependencies,
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onPersistedWhileInactive: onExternalPersistedWhileInactive,
                onSaved: onSaved
            )
        case .s3:
            return AddS3StorageViewController(
                dependencies: dependencies,
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onSaved: onSaved
            )
        case .sftp:
            return AddSFTPStorageViewController(
                dependencies: dependencies,
                shouldPopToRootOnSave: shouldPopToRootOnSave,
                onSaved: onSaved
            )
        }
    }

    private func handleStorageCreated(_ profile: ServerProfileRecord) {
        reloadProfiles()
        store.connectProfile(profile)
    }

    @objc
    private func dismissModalFlow() {
        dismiss(animated: ConsideringUser.animated)
    }

    @objc private func leftToggleTapped() {
        guard confirmSelectionReadiness() else { return }
        store.toggleAll(side: .local)
    }

    @objc private func rightToggleTapped() {
        guard confirmSelectionReadiness() else { return }
        guard confirmRemoteSelectionAllowed() else { return }
        store.toggleAll(side: .remote)
    }

    @objc private func localOverlayButtonTapped() {
        switch store.localPhotoAccessState {
        case .authorized:
            break
        case .notDetermined:
            store.requestLocalPhotoAccessIfNeeded()
        case .denied:
            guard let url = URL(string: UIApplication.openSettingsURLString),
                  UIApplication.shared.canOpenURL(url) else { return }
            UIApplication.shared.open(url)
        }
    }

    // Guards the async start window (incomplete pre-scan + prompt) between tapping Start and execution entering —
    // during it `isExecutionActive` is still false, so without this a rapid re-tap would spawn a second flow.
    private var isStartingExecution = false

    private func executeTapped() {
        guard !isStartingExecution else { return }
        let backup = store.selection.months(for: .backup)
        let download = store.selection.months(for: .download)
        let complement = store.selection.months(for: .complement)

        guard !backup.isEmpty || !download.isEmpty || !complement.isEmpty else { return }

        var lines: [String] = []
        // localizedStringWithFormat (not plain String(format:)) so these plural-variation keys resolve per locale.
        if !backup.isEmpty { lines.append(String.localizedStringWithFormat(String(localized: "home.confirm.backupMonths"), backup.count)) }
        if !download.isEmpty { lines.append(String.localizedStringWithFormat(String(localized: "home.confirm.downloadMonths"), download.count)) }
        if !complement.isEmpty { lines.append(String.localizedStringWithFormat(String(localized: "home.confirm.complementMonths"), complement.count)) }

        let alert = UIAlertController(title: String(localized: "home.alert.confirmExecute"), message: lines.joined(separator: "\n"), preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.start"), style: .default) { [weak self] _ in
            guard let self else { return }
            Task { await self.startExecutionResolvingIncomplete(backup: backup, download: download, complement: complement) }
        })
        present(alert, animated: true)
    }

    // A restore that would touch incomplete remote records asks once, upfront, how to handle them: create new
    // (differently-fingerprinted) assets from the resolvable subset, skip them, or abort the whole run.
    private func startExecutionResolvingIncomplete(backup: [LibraryMonthKey], download: [LibraryMonthKey], complement: [LibraryMonthKey]) async {
        isStartingExecution = true
        updateActionPanel()   // grey out Execute for the async window
        defer {
            isStartingExecution = false
            updateActionPanel()
        }
        let incompleteCount = (download.isEmpty && complement.isEmpty)
            ? 0
            : await store.incompleteDownloadItemCount(download: download, complement: complement)
        guard incompleteCount > 0 else {
            store.startExecution(backup: backup, download: download, complement: complement, incompletePolicy: .skip)
            return
        }
        switch await presentIncompleteDownloadPrompt(count: incompleteCount) {
        case .some(let policy):
            store.startExecution(backup: backup, download: download, complement: complement, incompletePolicy: policy)
        case .none:
            break   // aborted — start nothing
        }
    }

    // Returns the chosen policy, or nil to abort the whole run. A modal `.alert` can't dismiss without a tap,
    // so exactly one of the three actions always resumes the continuation.
    private func presentIncompleteDownloadPrompt(count: Int) async -> IncompleteDownloadPolicy? {
        await withCheckedContinuation { (continuation: CheckedContinuation<IncompleteDownloadPolicy?, Never>) in
            // localizedStringWithFormat (not plain String(format:)) so the plural `variations` resolve per locale.
            let message = String.localizedStringWithFormat(String(localized: "home.incompleteDownload.message"), count)
            let alert = UIAlertController(title: String(localized: "home.incompleteDownload.title"), message: message, preferredStyle: .alert)
            var resumed = false
            let resume: (IncompleteDownloadPolicy?) -> Void = { value in
                guard !resumed else { return }
                resumed = true
                continuation.resume(returning: value)
            }
            alert.addAction(UIAlertAction(title: String(localized: "home.incompleteDownload.createAll"), style: .default) { _ in resume(.createNewAsset) })
            alert.addAction(UIAlertAction(title: String(localized: "home.incompleteDownload.skip"), style: .default) { _ in resume(.skip) })
            alert.addAction(UIAlertAction(title: String(localized: "home.incompleteDownload.abort"), style: .cancel) { _ in resume(nil) })
            present(alert, animated: true)
        }
    }

    private func confirmSelectionReadiness() -> Bool {
        guard !store.isExecutionActive, !store.isLocalIndexReloading else { return false }

        if store.isMaintenanceBlocked {
            showAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return false
        }

        var messages: [String] = []
        switch store.localPhotoAccessState {
        case .authorized:
            break
        case .notDetermined:
            messages.append(String(localized: "home.overlay.authRequired"))
        case .denied:
            messages.append(String(localized: "home.overlay.noAuth"))
        }

        switch store.connectionState {
        case .connected:
            break
        case .connecting:
            if let progress = store.remoteSyncProgress {
                messages.append(remoteOverlayMessage(for: progress))
            } else {
                messages.append(String(localized: "home.overlay.scanningIndex"))
            }
        case .disconnected:
            messages.append(String(localized: "home.overlay.notConnected"))
        }

        guard !messages.isEmpty else { return true }
        showAlert(
            title: String(localized: "home.alert.selectionUnavailable"),
            message: messages.joined(separator: "\n")
        )
        return false
    }

    private func remoteOverlayMessage(for progress: RemoteSyncProgress) -> String {
        switch progress.kind {
        case .scanningRemoteIndex:
            return String(localized: "home.overlay.scanningIndex")
        case .repoUpgrade(let phase):
            return repoUpgradeOverlayMessage(phase: phase, progress: progress)
        case .remoteIndex:
            return String.localizedStringWithFormat(
                String(localized: "home.overlay.processingMonths"),
                progress.current,
                progress.total
            )
        }
    }

    private func repoUpgradeOverlayMessage(phase: RepoUpgradePhase, progress: RemoteSyncProgress) -> String {
        switch phase {
        case .finalizing:
            return String(localized: "home.overlay.finalizingRepo")
        case .copying:
            return countedOverlayMessage(progress, key: "home.overlay.upgradingRepoMonths", fallback: "home.overlay.upgradingRepo")
        case .validating:
            return countedOverlayMessage(progress, key: "home.overlay.validatingRepoMonths", fallback: "home.overlay.upgradingRepo")
        case .cleaning:
            return countedOverlayMessage(progress, key: "home.overlay.cleaningRepoMonths", fallback: "home.overlay.cleaningRepo")
        }
    }

    private func countedOverlayMessage(_ progress: RemoteSyncProgress, key: String.LocalizationValue, fallback: String.LocalizationValue) -> String {
        guard progress.total > 0 else { return String(localized: fallback) }
        return String.localizedStringWithFormat(String(localized: key), progress.current, progress.total)
    }

    private func confirmRemoteSelectionAllowed() -> Bool {
        guard store.isRemoteSelectionAllowed else {
            showAlert(
                title: String(localized: "home.alert.remoteSelectionUnavailable"),
                message: String(localized: "home.alert.remoteSelectionUnavailableMessage")
            )
            return false
        }
        return true
    }

    private func confirmStop() {
        let alert = UIAlertController(title: String(localized: "home.alert.confirmStop"), message: String(localized: "home.alert.confirmStopMessage"), preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.stop"), style: .destructive) { [weak self] _ in
            self?.store.stopExecution()
        })
        present(alert, animated: true)
    }

    private func presentPasswordPrompt(for profile: ServerProfileRecord, completion: @escaping (String) -> Void) {
        let title: String
        let placeholder: String
        switch profile.resolvedStorageType {
        case .s3:
            title = String(localized: "home.alert.s3SecretKeyPrompt")
            placeholder = String(localized: "auth.s3.placeholder.secretKey")
        default:
            title = String(localized: "home.alert.passwordPrompt")
            placeholder = String(localized: "home.alert.passwordPlaceholder")
        }

        let alert = UIAlertController(title: title, message: profile.name, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = placeholder
            textField.isSecureTextEntry = true
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.connect"), style: .default) { _ in
            guard let password = alert.textFields?.first?.text else { return }
            completion(password)
        })
        present(alert, animated: true)
    }

    private func presentSFTPHostKeyPrompt(
        decision: SFTPHostKeyPromptPolicy.Decision,
        actual: String
    ) async -> Bool {
        let title: String
        let message: String
        let confirmTitle: String
        let confirmStyle: UIAlertAction.Style
        switch decision {
        case .none:
            return true
        case .firstTrust:
            title = String(localized: "auth.sftp.hostKey.confirmTitle")
            message = String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.confirmBody"),
                actual
            )
            confirmTitle = String(localized: "auth.sftp.hostKey.confirmAction")
            confirmStyle = .default
        case .changedKey(let expected):
            title = String(localized: "auth.sftp.hostKey.changedTitle")
            message = String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.changedBody"),
                expected,
                actual
            )
            confirmTitle = String(localized: "auth.sftp.hostKey.changedAction")
            confirmStyle = .destructive
        }
        guard pendingSFTPHostKeyPromptContinuation == nil else { return false }
        guard !Task.isCancelled else { return false }
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard !Task.isCancelled else {
                    continuation.resume(returning: false)
                    return
                }
                pendingSFTPHostKeyPromptContinuation = continuation
                let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
                alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { [weak self, weak alert] _ in
                    self?.resolveSFTPHostKeyPromptAfterDismissal(false, alert: alert)
                })
                alert.addAction(UIAlertAction(title: confirmTitle, style: confirmStyle) { [weak self, weak alert] _ in
                    self?.resolveSFTPHostKeyPromptAfterDismissal(true, alert: alert)
                })
                sftpHostKeyPromptAlert = alert
                present(alert, animated: true)
            }
        } onCancel: {
            Task { @MainActor [weak self] in
                self?.sftpHostKeyPromptAlert?.dismiss(animated: true)
                self?.resolveSFTPHostKeyPrompt(false)
            }
        }
    }

    private func resolveSFTPHostKeyPromptAfterDismissal(_ accepted: Bool, alert: UIAlertController?) {
        Task { @MainActor [weak self, weak alert] in
            await PresentationDismissalSequencer.waitUntilDismissed {
                guard let alert else { return false }
                return alert.presentingViewController != nil || alert.viewIfLoaded?.window != nil
            }
            self?.resolveSFTPHostKeyPrompt(accepted)
        }
    }

    private func resolveSFTPHostKeyPrompt(_ accepted: Bool) {
        guard let continuation = pendingSFTPHostKeyPromptContinuation else { return }
        pendingSFTPHostKeyPromptContinuation = nil
        sftpHostKeyPromptAlert = nil
        continuation.resume(returning: accepted)
    }

    private func scrollToMonth(_ month: LibraryMonthKey) {
        for (sectionIndex, section) in store.sections.enumerated() {
            guard let rowIndex = section.rows.firstIndex(where: { $0.month == month }) else { continue }
            let indexPath = IndexPath(item: rowIndex * 2, section: sectionIndex)
            collectionView.scrollToItem(at: indexPath, at: .centeredVertically, animated: true)
            return
        }
    }

}

private extension RemoteSyncProgress {
    var isRepoUpgrade: Bool {
        if case .repoUpgrade = kind { return true }
        return false
    }
}

// MARK: - UICollectionViewDelegate

extension HomeViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        presentMediaBrowser(initialMonth: item.month, initialRemote: item.side == .remote)
    }
}
