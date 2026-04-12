import MarqueeLabel
import SnapKit
import UIKit

final class NewHomeViewController: UIViewController {

    private let store: HomeScreenStore

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
    private let leftHeaderLabel: MarqueeLabel = {
        let label = MarqueeLabel(frame: .zero, rate: 30, fadeLength: 8)
        label.animationDelay = 2
        return label
    }()
    private let leftToggle = UIButton(type: .system)
    private let rightHeaderLabel: MarqueeLabel = {
        let label = MarqueeLabel(frame: .zero, rate: 30, fadeLength: 8)
        label.animationDelay = 2
        return label
    }()
    private let rightHeaderMenuOverlay = UIButton(type: .system)
    private let rightHeaderButton = UIButton(type: .system)
    private let rightToggle = UIButton(type: .system)
    private let actionPanel = SelectionActionPanel()
    private var collectionBottomToActionPanel: Constraint?

    private let remoteOverlay = UIView()
    private let remoteOverlayLabel = UILabel()
    private let remoteOverlaySpinner = UIActivityIndicatorView(style: .medium)
    private let remoteOverlayButton = UIButton(type: .system)

    private var rightHeaderBg: UIView!
    private var isPanelShown = false

    private static let headerAreaHeight: CGFloat = 44

    init(dependencies: DependencyContainer) {
        self.store = HomeScreenStore(dependencies: dependencies)
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    // MARK: - Lifecycle

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground

        buildUI()
        configureDataSource()
        bindStore()

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
        rightHeaderBg = UIView()
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

        leftHeaderLabel.text = "本地相册"
        leftHeaderLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        leftHeaderLabel.textColor = headerTextColor

        let leftHeaderStack = UIStackView(arrangedSubviews: [leftToggle, leftHeaderLabel])
        leftHeaderStack.axis = .horizontal
        leftHeaderStack.spacing = 4
        leftHeaderStack.alignment = .center
        leftHeaderBg.addSubview(leftHeaderStack)
        leftHeaderStack.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.bottom.equalToSuperview()
            make.height.equalTo(Self.headerAreaHeight)
            make.leading.greaterThanOrEqualToSuperview().inset(8)
            make.trailing.lessThanOrEqualToSuperview().inset(8)
        }

        rightToggle.setImage(UIImage(systemName: "circle", withConfiguration: symbolConfig), for: .normal)
        rightToggle.tintColor = headerTextColor
        rightToggle.setContentHuggingPriority(.required, for: .horizontal)
        rightToggle.setContentCompressionResistancePriority(.required, for: .horizontal)
        rightToggle.addTarget(self, action: #selector(rightToggleTapped), for: .touchUpInside)

        configureRightHeaderButton()

        let rightHeaderStack = UIStackView(arrangedSubviews: [rightToggle, rightHeaderLabel, rightHeaderButton])
        rightHeaderStack.axis = .horizontal
        rightHeaderStack.spacing = 4
        rightHeaderStack.alignment = .center
        rightHeaderBg.addSubview(rightHeaderStack)
        rightHeaderStack.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.bottom.equalToSuperview()
            make.height.equalTo(Self.headerAreaHeight)
            make.leading.greaterThanOrEqualToSuperview().inset(8)
            make.trailing.lessThanOrEqualToSuperview().inset(8)
        }

        rightHeaderMenuOverlay.showsMenuAsPrimaryAction = true
        rightHeaderMenuOverlay.menu = buildDestinationMenu()
        rightHeaderBg.addSubview(rightHeaderMenuOverlay)
        rightHeaderMenuOverlay.snp.makeConstraints { make in
            make.leading.equalTo(rightHeaderLabel)
            make.trailing.equalTo(rightHeaderButton)
            make.top.bottom.equalTo(rightHeaderStack)
        }

        actionPanel.onExecuteTapped = { [weak self] in self?.executeTapped() }
        actionPanel.onPauseTapped = { [weak self] in self?.store.pauseExecution() }
        actionPanel.onStopTapped = { [weak self] in self?.confirmStop() }
        actionPanel.onResumeTapped = { [weak self] in self?.store.resumeExecution() }
        actionPanel.onCompleteTapped = { [weak self] in self?.store.exitExecution() }
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

        // Remote overlay
        remoteOverlay.backgroundColor = .appBackground
        remoteOverlay.isHidden = true
        remoteOverlayLabel.textAlignment = .center
        remoteOverlayLabel.numberOfLines = 0
        remoteOverlayLabel.font = .systemFont(ofSize: 15, weight: .medium)
        remoteOverlayLabel.textColor = .secondaryLabel
        remoteOverlaySpinner.hidesWhenStopped = true

        var btnCfg = UIButton.Configuration.plain()
        btnCfg.image = UIImage(systemName: "chevron.down", withConfiguration: UIImage.SymbolConfiguration(pointSize: 11))
        btnCfg.imagePlacement = .trailing
        btnCfg.imagePadding = 4
        btnCfg.title = "选择存储"
        btnCfg.baseForegroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        remoteOverlayButton.configuration = btnCfg
        remoteOverlayButton.showsMenuAsPrimaryAction = true
        remoteOverlayButton.menu = buildDestinationMenu()

        let overlayStack = UIStackView(arrangedSubviews: [remoteOverlaySpinner, remoteOverlayLabel, remoteOverlayButton])
        overlayStack.axis = .vertical
        overlayStack.spacing = 12
        overlayStack.alignment = .center
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
        cell.configure(
            monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: HomeSeasonStyle.monthSecondaryTextColor(month: m)),
            sizeText: summary.sizeText,
            bgColor: HomeSeasonStyle.monthColor(month: m),
            titleColor: HomeSeasonStyle.monthTextColor(month: m),
            detailColor: HomeSeasonStyle.monthSecondaryTextColor(month: m),
            isSelected: isSelected
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
                                        leftState: leftState, rightState: rightState,
                                        selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
            supplementaryView.onLeftTap = { [weak self] in
                self?.store.toggleYear(sectionIndex: indexPath.section, side: .local)
            }
            supplementaryView.onRightTap = { [weak self] in
                self?.store.toggleYear(sectionIndex: indexPath.section, side: .remote)
            }
        }

        let arrowRegistration = UICollectionView.SupplementaryRegistration<DirectionArrowView>(
            elementKind: directionArrowElementKind
        ) { [weak self] arrowView, _, indexPath in
            guard let self else { return }
            let itemIndexPath = IndexPath(item: indexPath.item * 2, section: indexPath.section)
            guard let item = self.dataSource.itemIdentifier(for: itemIndexPath) else { return }
            arrowView.configure(
                direction: self.store.arrowDirection(for: item.month),
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
            case .selection:           self.renderSelectionChange()
            case .execution(let months): self.renderExecutionChange(changedMonths: months)
            case .connection:          self.renderConnectionChange()
            case .structural:          self.renderStructuralChange()
            }
        }

        store.onAlert = { [weak self] title, message in
            self?.showAlert(title: title, message: message)
        }

        store.onNeedsPasswordPrompt = { [weak self] profile, completion in
            self?.presentPasswordPrompt(for: profile, completion: completion)
        }

        store.onConnectFailed = { [weak self] profile, error in
            self?.showAlert(title: "连接失败", message: profile.userFacingStorageErrorMessage(error))
        }
    }

    // MARK: - Render Methods

    private func renderDataChange(_ months: Set<LibraryMonthKey>) {
        reconfigureMonths(months)
    }

    private func renderSelectionChange() {
        let allMonths = Set(store.sections.flatMap { $0.rows.map(\.month) })
        reconfigureMonths(allMonths)
        updateTopHeaderToggles()
        updateActionPanel()
    }

    private func renderExecutionChange(changedMonths: Set<LibraryMonthKey>) {
        if let exec = store.executionState {
            let isFirstTick = !hasEnteredExecution
            reconfigureMonths(changedMonths.isEmpty ? exec.executionMonths : changedMonths)
            updateActionPanelFromExecution(exec)
            if isFirstTick {
                updateSelectionInteraction()
            }
        } else {
            // Execution ended
            hasEnteredExecution = false
            actionPanel.resetToSelection()
            renderStructuralChange()
        }
    }

    private func renderConnectionChange() {
        updateRightHeaderButton()
        renderStructuralChange()
    }

    private func renderStructuralChange() {
        applyFullSnapshot()
        updateTopHeaderToggles()
        updateActionPanel()
        updateSelectionInteraction()
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
                header.configure(section: ms, leftState: leftState, rightState: rightState,
                                 selectedColor: accentColor, deselectedColor: .tertiaryLabel)
            }
            for (rowIndex, row) in ms.rows.enumerated() {
                let badgeIndexPath = IndexPath(item: rowIndex, section: sectionIndex)
                if let arrowView = collectionView.supplementaryView(
                    forElementKind: directionArrowElementKind, at: badgeIndexPath
                ) as? DirectionArrowView {
                    arrowView.configure(
                        direction: store.arrowDirection(for: row.month),
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
                header.configure(section: ms, leftState: leftState, rightState: rightState,
                                 selectedColor: accentColor, deselectedColor: .tertiaryLabel)
            }
            for (rowIndex, row) in ms.rows.enumerated() where months.contains(row.month) {
                let badgeIndexPath = IndexPath(item: rowIndex, section: sectionIndex)
                if let arrowView = collectionView.supplementaryView(
                    forElementKind: directionArrowElementKind, at: badgeIndexPath
                ) as? DirectionArrowView {
                    arrowView.configure(
                        direction: store.arrowDirection(for: row.month),
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

        leftToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(for: allMonths, side: .local)), withConfiguration: config), for: .normal)
        leftToggle.tintColor = headerColor
        rightToggle.setImage(UIImage(systemName: iconName(for: store.selection.selectionState(for: allMonths, side: .remote)), withConfiguration: config), for: .normal)
        rightToggle.tintColor = headerColor
    }

    private func updateActionPanel() {
        if let exec = store.executionState {
            updateActionPanelFromExecution(exec)
            return
        }

        let counts = store.selection.counts()
        actionPanel.configure(backupCount: counts.backup, downloadCount: counts.download, syncCount: counts.sync)
        actionPanel.backupCategoryButton.menu = buildCategoryMenu(for: .toRemote)
        actionPanel.downloadCategoryButton.menu = buildCategoryMenu(for: .toLocal)
        actionPanel.syncCategoryButton.menu = buildCategoryMenu(for: .sync)

        let shouldShow = !store.selection.isEmpty
        if shouldShow && !isPanelShown {
            isPanelShown = true
            panelHiddenConstraint?.deactivate()
            panelShownConstraint?.activate()
            UIView.animate(withDuration: 0.3, delay: 0, options: .curveEaseOut) { self.view.layoutIfNeeded() }
        } else if !shouldShow && isPanelShown {
            isPanelShown = false
            panelShownConstraint?.deactivate()
            panelHiddenConstraint?.activate()
            UIView.animate(withDuration: 0.25, delay: 0, options: .curveEaseIn) { self.view.layoutIfNeeded() }
        }
    }

    private var hasEnteredExecution = false

    private func updateActionPanelFromExecution(_ exec: HomeExecutionState) {
        let phases = exec.panelPhases()

        if !isPanelShown {
            isPanelShown = true
            panelHiddenConstraint?.deactivate()
            panelShownConstraint?.activate()
            view.layoutIfNeeded()
        }

        if !hasEnteredExecution {
            hasEnteredExecution = true
            actionPanel.enterExecution(
                backupTotal: exec.uploadMonths.count,
                downloadTotal: exec.downloadMonths.count,
                syncTotal: exec.syncMonths.count
            )
        }

        actionPanel.updateExecution(
            backupPhase: phases.backup,
            downloadPhase: phases.download,
            syncPhase: phases.sync,
            phase: exec.phase,
            controlState: exec.controlState
        )

        if let (menu, title) = buildFailureMenu(from: exec) {
            actionPanel.updateFailureSummary(menu: menu, title: title)
        } else {
            actionPanel.updateFailureSummary(menu: nil, title: nil)
        }
    }

    private func buildFailureMenu(from exec: HomeExecutionState) -> (UIMenu, String)? {
        let infos = exec.failedMonthInfos
        guard !infos.isEmpty else { return nil }

        var byYear: [Int: [MonthFailureInfo]] = [:]
        for info in infos {
            byYear[info.month.year, default: []].append(info)
        }

        let yearMenus = byYear.keys.sorted().map { year -> UIMenu in
            let actions = byYear[year]!.sorted { $0.month < $1.month }.map { info in
                UIAction(title: info.month.displayText, subtitle: info.message) { [weak self] _ in
                    self?.scrollToMonth(info.month)
                }
            }
            return UIMenu(title: "\(year)年", options: .displayInline, children: actions)
        }

        return (UIMenu(children: yearMenus), "\(infos.count) 项失败")
    }

    private func updateSelectionInteraction() {
        let selectable = store.isSelectable
        leftToggle.isEnabled = selectable
        rightToggle.isEnabled = selectable
        rightHeaderMenuOverlay.isEnabled = store.executionState == nil
        rightHeaderButton.isEnabled = store.executionState == nil
    }

    private func updateRemoteOverlay() {
        switch store.connectionState {
        case .connecting:
            remoteOverlay.isHidden = false
            remoteOverlaySpinner.startAnimating()
            remoteOverlayLabel.text = "连接中..."
            remoteOverlayButton.isHidden = true
        case .disconnected:
            remoteOverlay.isHidden = false
            remoteOverlaySpinner.stopAnimating()
            remoteOverlayLabel.text = "未连接远端存储"
            remoteOverlayButton.isHidden = false
            remoteOverlayButton.menu = buildDestinationMenu()
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
            rightHeaderLabel.text = "远端存储"
        }
        let menu = buildDestinationMenu()
        rightHeaderButton.menu = menu
        rightHeaderMenuOverlay.menu = menu
    }

    private func configureRightHeaderButton() {
        let headerTextColor = UIColor.materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100)
        rightHeaderLabel.text = "远端存储"
        rightHeaderLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        rightHeaderLabel.textColor = headerTextColor

        var config = UIButton.Configuration.plain()
        config.image = UIImage(systemName: "chevron.down", withConfiguration: UIImage.SymbolConfiguration(pointSize: 11))
        config.contentInsets = .init(top: 0, leading: 0, bottom: 0, trailing: 0)
        config.baseForegroundColor = headerTextColor
        rightHeaderButton.configuration = config
        rightHeaderButton.showsMenuAsPrimaryAction = true
        rightHeaderButton.setContentHuggingPriority(.required, for: .horizontal)
        rightHeaderButton.setContentCompressionResistancePriority(.required, for: .horizontal)
        let menu = buildDestinationMenu()
        rightHeaderButton.menu = menu
        rightHeaderMenuOverlay.menu = menu
    }

    // MARK: - Destination Menu

    private func buildDestinationMenu() -> UIMenu {
        let disconnected = !store.connectionState.isConnected

        let disconnectAction = UIAction(
            title: "未连接",
            state: disconnected ? .on : .off
        ) { [weak self] _ in
            self?.store.disconnect()
        }

        var profileActions: [UIAction] = []
        for profile in store.savedProfiles {
            let isActive = store.connectionState.activeProfile?.id == profile.id
            let action = UIAction(
                title: profile.name,
                subtitle: profile.storageProfile.displaySubtitle,
                state: isActive ? .on : .off
            ) { [weak self] _ in
                self?.store.connectProfile(profile)
            }
            profileActions.append(action)
        }

        let profileSection = UIMenu(title: "", options: .displayInline, children: profileActions)
        let disconnectSection = UIMenu(title: "", options: .displayInline, children: [disconnectAction])
        return UIMenu(children: [profileSection, disconnectSection])
    }

    // MARK: - User Actions

    @objc private func leftToggleTapped() {
        store.toggleAll(side: .local)
    }

    @objc private func rightToggleTapped() {
        store.toggleAll(side: .remote)
    }

    private func executeTapped() {
        let counts = store.selection.counts()
        guard counts.backup > 0 || counts.download > 0 || counts.sync > 0 else { return }

        var lines: [String] = []
        if counts.backup > 0 { lines.append("备份 \(counts.backup) 个月份") }
        if counts.download > 0 { lines.append("下载 \(counts.download) 个月份") }
        if counts.sync > 0 { lines.append("同步 \(counts.sync) 个月份") }

        let upload = store.selection.months(for: .toRemote)
        let download = store.selection.months(for: .toLocal)
        let sync = store.selection.months(for: .sync)

        let alert = UIAlertController(title: "确认执行", message: lines.joined(separator: "\n"), preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "开始", style: .default) { [weak self] _ in
            self?.store.startExecution(upload: upload, download: download, sync: sync)
        })
        present(alert, animated: true)
    }

    private func confirmStop() {
        let alert = UIAlertController(title: "确认停止", message: "停止后需要重新选择月份执行", preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "停止", style: .destructive) { [weak self] _ in
            self?.store.stopExecution()
        })
        present(alert, animated: true)
    }

    private func presentPasswordPrompt(for profile: ServerProfileRecord, completion: @escaping (String) -> Void) {
        let alert = UIAlertController(title: "输入密码", message: profile.name, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Password"
            textField.isSecureTextEntry = true
        }
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "连接", style: .default) { _ in
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

    private func buildCategoryMenu(for category: HomeArrowDirection) -> UIMenu {
        let months = store.selection.months(for: category)
        var byYear: [Int: [LibraryMonthKey]] = [:]
        for month in months { byYear[month.year, default: []].append(month) }

        let yearMenus = byYear.keys.sorted().map { year -> UIMenu in
            let actions = byYear[year]!.map { month -> UIAction in
                let row = store.rowLookup[month]
                let title = String(format: "%02d月", month.month)
                var parts: [String] = []
                if let lc = row?.local?.assetCount { parts.append("本地 \(lc) 张") }
                if let rc = row?.remote?.assetCount { parts.append("远端 \(rc) 张") }
                let subtitle = parts.isEmpty ? nil : parts.joined(separator: " · ")
                return UIAction(title: title, subtitle: subtitle) { [weak self] _ in
                    self?.scrollToMonth(month)
                }
            }
            return UIMenu(title: "\(year)年", options: .displayInline, children: actions)
        }
        return UIMenu(children: yearMenus)
    }
}

// MARK: - UICollectionViewDelegate

extension NewHomeViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        switch item.side {
        case .local:  store.toggleMonth(item.month, side: .local)
        case .remote: store.toggleMonth(item.month, side: .remote)
        }
    }
}
