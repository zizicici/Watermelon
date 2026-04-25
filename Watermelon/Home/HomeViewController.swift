import MarqueeLabel
import MoreKit
import SnapKit
import UIKit

final class HomeViewController: UIViewController {
    private static let privacyPolicyEnglishURL = "https://github.com/zizicici/PublicContent/blob/main/Watermelon/PrivacyPolicy.en.md"
    private static let privacyPolicyChineseURL = "https://github.com/zizicici/PublicContent/blob/main/Watermelon/PrivacyPolicy.zh-Hans.md"

    private static var privacyPolicyURLString: String {
        let preferredLanguage = Bundle.main.preferredLocalizations.first?.lowercased()
            ?? Locale.preferredLanguages.first?.lowercased()
            ?? ""
        if preferredLanguage.hasPrefix("zh") {
            return privacyPolicyChineseURL
        }
        return privacyPolicyEnglishURL
    }

    private let dependencies: DependencyContainer
    private let store: HomeScreenStore
    private lazy var menuFactory = HomeMenuFactory(
        store: store,
        hooks: HomeMenuFactory.Hooks(
            refreshLocalLibraryMenu: { [weak self] in self?.refreshLocalLibraryMenu() },
            openLocalAlbumPicker: { [weak self] in self?.openLocalAlbumPicker() },
            openNewStorageFlow: { [weak self] dest in self?.openNewStorageFlow(dest) },
            openManageProfiles: { [weak self] in self?.openManageProfiles() },
            scrollToMonth: { [weak self] month in self?.scrollToMonth(month) }
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
    private let remoteOverlayLabel = UILabel()
    private let remoteOverlaySpinner = UIActivityIndicatorView(style: .medium)
    private let remoteOverlayButton = UIButton(type: .system)
    private var didBecomeActiveObserver: NSObjectProtocol?

    private let rightHeaderBg = UIView()
    private var isPanelShown = false
    private var hasLoadedHeaderSummary = false
    private var didRequestReviewForCurrentExecution = false

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
        if let didBecomeActiveObserver {
            NotificationCenter.default.removeObserver(didBecomeActiveObserver)
        }
    }

    // MARK: - Lifecycle

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
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 14)

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

        let rightHeaderTitleStack = UIStackView(arrangedSubviews: [rightHeaderLabel, rightHeaderButton])
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
        configureOverlayLabel(remoteOverlayLabel)
        remoteOverlaySpinner.hidesWhenStopped = true

        configureOverlayButtonBase(remoteOverlayButton)
        var btnCfg = remoteOverlayButton.configuration ?? UIButton.Configuration.plain()
        btnCfg.image = UIImage(systemName: "chevron.down", withConfiguration: UIImage.SymbolConfiguration(pointSize: 11))
        btnCfg.imagePlacement = .trailing
        btnCfg.imagePadding = 4
        btnCfg.title = String(localized: "home.overlay.selectStorage")
        remoteOverlayButton.configuration = btnCfg
        remoteOverlayButton.showsMenuAsPrimaryAction = true
        remoteOverlayButton.menu = menuFactory.buildDestination()

        let overlayStack = makeOverlayStack(
            spinner: remoteOverlaySpinner,
            label: remoteOverlayLabel,
            button: remoteOverlayButton
        )
        remoteOverlay.addSubview(overlayStack)
        overlayStack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(16)
        }

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
        let selectionEnabled = item.side == .local || store.isRemoteSelectionAllowed
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
            let allMonths = Set(section.rows.map(\.month))
            let leftState = self.store.selection.selectionState(for: allMonths, side: .local)
            let rightState = self.store.selection.selectionState(for: allMonths, side: .remote)
            let accentColor = self.leftHeaderLabel.textColor ?? .secondaryLabel
            supplementaryView.configure(section: section,
                                        sectionIndex: indexPath.section,
                                        leftState: leftState, rightState: rightState,
                                        leftSelectionEnabled: true,
                                        rightSelectionEnabled: self.store.isRemoteSelectionAllowed,
                                        selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
            supplementaryView.onLeftTap = { [weak self] sectionIndex in
                guard self?.confirmSelectionReadiness() == true else { return }
                self?.store.toggleYear(sectionIndex: sectionIndex, side: .local)
            }
            supplementaryView.onRightTap = { [weak self] sectionIndex in
                guard self?.confirmSelectionReadiness() == true else { return }
                guard self?.confirmRemoteSelectionAllowed() == true else { return }
                self?.store.toggleYear(sectionIndex: sectionIndex, side: .remote)
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

        store.onConnectFailed = { [weak self] profile, error in
            self?.showAlert(title: String(localized: "home.alert.connectionFailed"), message: profile.userFacingStorageErrorMessage(error))
        }
    }

    // MARK: - Render Methods

    private func renderDataChange(_ months: Set<LibraryMonthKey>) {
        hasLoadedHeaderSummary = true
        reconfigureMonths(months)
        updateTopHeaderSummaries()
    }

    private func renderFileSizeChange(_ months: Set<LibraryMonthKey>) {
        hasLoadedHeaderSummary = true
        reconfigureMonths(months)
        updateTopHeaderSummaries()
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
                let allMonths = Set(ms.rows.map(\.month))
                let leftState = store.selection.selectionState(for: allMonths, side: .local)
                let rightState = store.selection.selectionState(for: allMonths, side: .remote)
                header.configure(section: ms,
                                 sectionIndex: sectionIndex,
                                 leftState: leftState, rightState: rightState,
                                 leftSelectionEnabled: true,
                                 rightSelectionEnabled: store.isRemoteSelectionAllowed,
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
                let allMonths = Set(ms.rows.map(\.month))
                let leftState = store.selection.selectionState(for: allMonths, side: .local)
                let rightState = store.selection.selectionState(for: allMonths, side: .remote)
                header.configure(section: ms,
                                 sectionIndex: sectionIndex,
                                 leftState: leftState, rightState: rightState,
                                 leftSelectionEnabled: true,
                                 rightSelectionEnabled: store.isRemoteSelectionAllowed,
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
        let allMonths = Set(store.sections.flatMap { $0.rows.map(\.month) })
        let headerColor = leftHeaderLabel.textColor ?? .secondaryLabel
        let config = UIImage.SymbolConfiguration(pointSize: 14)

        func iconName(for state: HomeSelectionState) -> String {
            switch state {
            case .all: return "checkmark.circle.fill"
            case .partial: return "minus.circle.fill"
            case .none: return "circle"
            }
        }

        let localReady = store.localPhotoAccessState.isAuthorized
        let remoteReady = store.connectionState.isConnected

        leftToggle.isHidden = !localReady
        if localReady {
            leftToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(for: allMonths, side: .local)), withConfiguration: config), for: .normal)
            leftToggle.tintColor = headerColor
        }

        rightToggle.isHidden = !remoteReady
        if remoteReady {
            rightToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(for: allMonths, side: .remote)), withConfiguration: config), for: .normal)
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

        if store.connectionState.isConnected,
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
                    .init(type: .dateOfProduction, value: "2026/04/24"),
                    .init(type: .license, value: "粤ICP备2025448771号-6A"),
                ],
                thirdPartyLibraries: [
                    .init(name: "AMSMB2", version: "4.0.3", urlString: "https://github.com/amosavian/AMSMB2"),
                    .init(name: "GRDB", version: "7.10.0", urlString: "https://github.com/groue/GRDB.swift"),
                    .init(name: "Kingfisher", version: "8.7.0", urlString: "https://github.com/onevcat/Kingfisher"),
                    .init(name: "MarqueeLabel", version: "4.5.3", urlString: "https://github.com/cbpowell/MarqueeLabel"),
                    .init(name: "PhotoBrowser", version: "4.0.2", urlString: "https://github.com/JiongXing/PhotoBrowser"),
                    .init(name: "SnapKit", version: "5.7.1", urlString: "https://github.com/SnapKit/SnapKit"),
                ]
            ),
            appShowcase: AppShowcaseConfiguration(
                apps: [.lemon, .moontake, .coconut, .festivals, .pigeon, .one, .offDay, .tagDay, .pin, .campfire, .doufu],
                displayCount: 3
            )
        )

        let dataSource = WatermelonMoreDataSource(dependencies: dependencies) { [weak self] in
            self?.reloadProfiles()
        }

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

    private func openManageProfiles() {
        let viewController = ManageStorageProfilesViewController(dependencies: dependencies) { [weak self] in
            self?.reloadProfiles()
        }

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

    private func openLocalAlbumPicker() {
        guard store.executionState == nil else { return }
        guard store.localPhotoAccessState.isAuthorized else {
            localOverlayButtonTapped()
            return
        }

        let viewController = LocalAlbumPickerViewController(
            photoLibraryService: dependencies.photoLibraryService,
            selectedAlbumIDs: store.localLibraryScope.selectedAlbumIdentifiers
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
                complementCount: counts.complement
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
        let canAttemptSelection = store.executionState == nil && !store.isReloadingScope
        collectionView.allowsSelection = canAttemptSelection
        leftToggle.isEnabled = canAttemptSelection && store.localPhotoAccessState.isAuthorized
        rightToggle.isEnabled = canAttemptSelection && store.connectionState.isConnected
        leftHeaderMenuOverlay.isEnabled = store.executionState == nil
        leftHeaderButton.isEnabled = store.executionState == nil
        rightHeaderMenuOverlay.isEnabled = store.executionState == nil
        rightHeaderButton.isEnabled = store.executionState == nil
    }

    private func updateLocalOverlay() {
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
        switch store.connectionState {
        case .connecting:
            remoteOverlay.isHidden = false
            remoteOverlaySpinner.startAnimating()
            if let progress = store.remoteSyncProgress {
                remoteOverlayLabel.text = String.localizedStringWithFormat(
                    String(localized: "home.overlay.processingMonths"),
                    progress.current,
                    progress.total
                )
            } else {
                remoteOverlayLabel.text = String(localized: "home.overlay.scanningIndex")
            }
            remoteOverlayButton.isHidden = true
        case .disconnected:
            remoteOverlay.isHidden = false
            remoteOverlaySpinner.stopAnimating()
            remoteOverlayLabel.text = String(localized: "home.overlay.notConnected")
            remoteOverlayButton.isHidden = false
        case .connected:
            remoteOverlay.isHidden = true
            remoteOverlaySpinner.stopAnimating()
        }
    }

    private func updateRightHeaderButton() {
        switch store.connectionState {
        case .connecting(let profile):
            rightHeaderLabel.text = profile.storageProfile.indicatorText + "..."
        case .connected(let profile):
            rightHeaderLabel.text = profile.storageProfile.indicatorText
        case .disconnected:
            rightHeaderLabel.text = String(localized: "home.header.remoteStorage")
        }
        refreshDestinationMenus()
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
        remoteOverlayButton.menu = menu
    }

    // MARK: - User Actions

    private func openNewStorageFlow(_ destination: NewStorageDestination) {
        if !ProStatus.isPro && store.savedProfiles.count >= 1 {
            presentProUpgradeAlert()
            return
        }

        let onSaved: (ServerProfileRecord, String) -> Void = { [weak self] profile, _ in
            self?.handleStorageCreated(profile)
        }

        if let navigationController {
            let rootViewController = makeNewStorageRootViewController(
                for: destination,
                shouldPopToRootOnSave: true,
                onSaved: onSaved
            )
            navigationController.pushViewController(rootViewController, animated: ConsideringUser.pushAnimated)
            return
        }

        let rootViewController = makeNewStorageRootViewController(
            for: destination,
            shouldPopToRootOnSave: false
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
                Task { [weak self] in
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
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) -> UIViewController {
        switch destination {
        case .smb:
            return AddSMBServerLoginViewController(
                dependencies: dependencies,
                draft: SMBServerLoginDraft(name: "", host: "", port: 445, username: "", domain: nil),
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

    private func executeTapped() {
        let backup = store.selection.months(for: .backup)
        let download = store.selection.months(for: .download)
        let complement = store.selection.months(for: .complement)

        guard !backup.isEmpty || !download.isEmpty || !complement.isEmpty else { return }

        var lines: [String] = []
        if !backup.isEmpty { lines.append(String(format: String(localized: "home.confirm.backupMonths"), backup.count)) }
        if !download.isEmpty { lines.append(String(format: String(localized: "home.confirm.downloadMonths"), download.count)) }
        if !complement.isEmpty { lines.append(String(format: String(localized: "home.confirm.complementMonths"), complement.count)) }

        let alert = UIAlertController(title: String(localized: "home.alert.confirmExecute"), message: lines.joined(separator: "\n"), preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.start"), style: .default) { [weak self] _ in
            self?.store.startExecution(backup: backup, download: download, complement: complement)
        })
        present(alert, animated: true)
    }

    private func confirmSelectionReadiness() -> Bool {
        guard store.executionState == nil else { return false }

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
                messages.append(
                    String.localizedStringWithFormat(
                        String(localized: "home.overlay.processingMonths"),
                        progress.current,
                        progress.total
                    )
                )
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
        let alert = UIAlertController(title: String(localized: "home.alert.passwordPrompt"), message: profile.name, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = String(localized: "home.alert.passwordPlaceholder")
            textField.isSecureTextEntry = true
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.connect"), style: .default) { _ in
            guard let password = alert.textFields?.first?.text, !password.isEmpty else { return }
            completion(password)
        })
        present(alert, animated: true)
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

// MARK: - UICollectionViewDelegate

extension HomeViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        guard confirmSelectionReadiness() else { return }
        switch item.side {
        case .local:  store.toggleMonth(item.month, side: .local)
        case .remote:
            guard confirmRemoteSelectionAllowed() else { return }
            store.toggleMonth(item.month, side: .remote)
        }
    }
}
