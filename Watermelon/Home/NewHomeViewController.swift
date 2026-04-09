import MarqueeLabel
import SnapKit
import UIKit

final class NewHomeViewController: UIViewController {

    fileprivate struct MonthSummary {
        let month: LibraryMonthKey
        let assetCount: Int
        let photoCount: Int
        let videoCount: Int
        let backedUpCount: Int?
        let totalSizeBytes: Int64?

        var monthTitle: String {
            String(format: "%02d月", month.month)
        }

        func countAttributedText(color: UIColor) -> NSAttributedString {
            let font = UIFont.monospacedDigitSystemFont(ofSize: 13, weight: .regular)
            let symbolConfig = UIImage.SymbolConfiguration(pointSize: 10, weight: .bold)
            let result = NSMutableAttributedString()

            if let img = UIImage(systemName: "photo", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
                result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
            }
            result.append(NSAttributedString(string: " \(photoCount)  ", attributes: [.font: font, .foregroundColor: color]))

            if let img = UIImage(systemName: "video", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
                result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
            }
            result.append(NSAttributedString(string: " \(videoCount)", attributes: [.font: font, .foregroundColor: color]))

            return result
        }

        var sizeText: String? {
            guard let bytes = totalSizeBytes else { return nil }
            return ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
        }
    }

    fileprivate struct MonthRow: Equatable {
        let month: LibraryMonthKey
        var local: MonthSummary?
        var remote: MonthSummary?

        static func == (lhs: MonthRow, rhs: MonthRow) -> Bool { lhs.month == rhs.month }
    }

    fileprivate struct MergedYearSection {
        let year: Int
        let rows: [MonthRow]

        var title: String { "\(year)年" }

        var localPhotoCount: Int { rows.compactMap(\.local).reduce(0) { $0 + $1.photoCount } }
        var localVideoCount: Int { rows.compactMap(\.local).reduce(0) { $0 + $1.videoCount } }
        var remotePhotoCount: Int { rows.compactMap(\.remote).reduce(0) { $0 + $1.photoCount } }
        var remoteVideoCount: Int { rows.compactMap(\.remote).reduce(0) { $0 + $1.videoCount } }

        var localSizeBytes: Int64? {
            let sizes = rows.compactMap { $0.local?.totalSizeBytes }
            let locals = rows.compactMap(\.local)
            guard !locals.isEmpty, sizes.count == locals.count else { return nil }
            return sizes.reduce(0, +)
        }

        var remoteSizeBytes: Int64? {
            let sizes = rows.compactMap { $0.remote?.totalSizeBytes }
            let remotes = rows.compactMap(\.remote)
            guard !remotes.isEmpty, sizes.count == remotes.count else { return nil }
            return sizes.reduce(0, +)
        }
    }

    private let dependencies: DependencyContainer
    private let homeDataManager: HomeIncrementalDataManager
    private lazy var backupSessionController = BackupSessionController(dependencies: dependencies)

    private var savedProfiles: [ServerProfileRecord] = []
    private var activeProfileID: Int64?
    private var isConnecting = false
    private var didAttemptAutoConnect = false
    private(set) var selectedLocalMonths = Set<LibraryMonthKey>()
    private(set) var selectedRemoteMonths = Set<LibraryMonthKey>()

    // MARK: - Execution Mode State
    private var isExecutionMode = false
    private var executionMonths = Set<LibraryMonthKey>()
    private var snapshotLocalSelection = Set<LibraryMonthKey>()
    private var snapshotRemoteSelection = Set<LibraryMonthKey>()
    private var completedMonths = Set<LibraryMonthKey>()
    private var activeMonths = Set<LibraryMonthKey>()
    private var backupObserverID: UUID?
    private var lastObservedState: BackupSessionController.State?

    private var activeMonthProgress: (completed: Int, total: Int)?
    private var assetCountByMonth: [LibraryMonthKey: Int] = [:]
    private var processedCountByMonth: [LibraryMonthKey: Int] = [:]

    private var uploadMonths: [LibraryMonthKey] = []
    private var pendingDownloadMonths: [LibraryMonthKey] = []
    private var pendingSyncMonths: [LibraryMonthKey] = []
    private var isDownloadPhase = false
    private var downloadTask: Task<Void, Never>?

    private enum Section: Hashable {
        case year(Int)
    }

    fileprivate enum SelectionState {
        case none, partial, all
    }

    fileprivate enum ArrowDirection {
        case toRemote      // arrow.right
        case toLocal       // arrow.left
        case sync          // arrow.left.arrow.right
    }

    private struct Item: Hashable {
        enum Side { case local, remote }
        let side: Side
        let month: LibraryMonthKey
        var arrowDirection: ArrowDirection?

        func hash(into hasher: inout Hasher) {
            hasher.combine(side)
            hasher.combine(month)
        }

        static func == (lhs: Item, rhs: Item) -> Bool {
            lhs.side == rhs.side && lhs.month == rhs.month
        }
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
    private var mergedSections: [MergedYearSection] = []
    private var rowLookup: [LibraryMonthKey: MonthRow] = [:]
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
    private let executionPanel = ExecutionActionPanel()
    private var executionPanelShownConstraint: Constraint?
    private var executionPanelHiddenConstraint: Constraint?
    private var collectionBottomToActionPanel: Constraint?
    private var collectionBottomToExecutionPanel: Constraint?

    private var localSummaries: [MonthSummary] = []
    private var remoteSummaries: [MonthSummary] = []

    private var reloadTask: Task<Void, Never>?

    private static let headerAreaHeight: CGFloat = 44

    private var hasActiveConnection: Bool {
        guard let profile = dependencies.appSession.activeProfile else { return false }
        return resolvedSessionPassword(for: profile) != nil
    }

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.homeDataManager = HomeIncrementalDataManager(
            photoLibraryService: dependencies.photoLibraryService,
            contentHashIndexRepository: ContentHashIndexRepository(databaseManager: dependencies.databaseManager)
        )
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        reloadTask?.cancel()
    }

    // MARK: - Lifecycle

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground

        buildUI()
        configureDataSource()
        bindSession()
        bindDataManager()

        loadSavedProfiles()
        scheduleReloadAllData()
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
        let rightHeaderBg = UIView()
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

        actionPanel.onExecuteTapped = { [weak self] in self?.backupTapped() }
        view.addSubview(actionPanel)
        actionPanel.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview()
            self.panelShownConstraint = make.bottom.equalToSuperview().constraint
            self.panelHiddenConstraint = make.top.equalTo(view.snp.bottom).constraint
        }
        panelShownConstraint?.deactivate()

        executionPanel.onPauseTapped = { [weak self] in self?.executionPauseTapped() }
        executionPanel.onStopTapped = { [weak self] in self?.executionStopTapped() }
        executionPanel.onResumeTapped = { [weak self] in self?.executionResumeTapped() }
        view.addSubview(executionPanel)
        executionPanel.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview()
            self.executionPanelShownConstraint = make.bottom.equalToSuperview().constraint
            self.executionPanelHiddenConstraint = make.top.equalTo(view.snp.bottom).constraint
        }
        executionPanelShownConstraint?.deactivate()

        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderBg.snp.bottom)
            make.leading.trailing.equalToSuperview()
            self.collectionBottomToActionPanel = make.bottom.equalTo(actionPanel.snp.top).constraint
            self.collectionBottomToExecutionPanel = make.bottom.equalTo(executionPanel.snp.top).constraint
        }
        collectionBottomToExecutionPanel?.deactivate()
    }

    // MARK: - Data Source

    private func configureCell(_ cell: MonthCell, item: Item) {
        let summary: MonthSummary
        let isSelected: Bool?

        switch item.side {
        case .local:
            summary = rowLookup[item.month]?.local
                ?? MonthSummary(month: item.month, assetCount: 0, photoCount: 0, videoCount: 0, backedUpCount: nil, totalSizeBytes: 0)
            isSelected = selectedLocalMonths.contains(item.month)
        case .remote:
            summary = rowLookup[item.month]?.remote
                ?? MonthSummary(month: item.month, assetCount: 0, photoCount: 0, videoCount: 0, backedUpCount: nil, totalSizeBytes: 0)
            isSelected = selectedRemoteMonths.contains(item.month)
        }

        let m = item.month.month

        if isExecutionMode, executionMonths.contains(item.month) {
            if completedMonths.contains(item.month) {
                cell.configureCompleted(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: .tertiaryLabel),
                    sizeText: summary.sizeText
                )
                return
            } else if activeMonths.contains(item.month) {
                cell.configureRunning(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: Self.monthSecondaryTextColor(month: item.month.month)),
                    sizeText: summary.sizeText,
                    bgColor: Self.monthColor(month: m),
                    titleColor: Self.monthTextColor(month: m),
                    detailColor: Self.monthSecondaryTextColor(month: m)
                )
                return
            } else {
                let toggleOn = item.side == .local
                    ? snapshotLocalSelection.contains(item.month)
                    : snapshotRemoteSelection.contains(item.month)
                cell.configure(
                    monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: Self.monthSecondaryTextColor(month: item.month.month)),
                    sizeText: summary.sizeText,
                    bgColor: Self.monthColor(month: m),
                    titleColor: Self.monthTextColor(month: m),
                    detailColor: Self.monthSecondaryTextColor(month: m),
                    isSelected: toggleOn
                )
                return
            }
        }

        if isExecutionMode {
            let toggleOn = item.side == .local
                ? snapshotLocalSelection.contains(item.month)
                : snapshotRemoteSelection.contains(item.month)
            cell.configure(
                monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: Self.monthSecondaryTextColor(month: item.month.month)),
                sizeText: summary.sizeText,
                bgColor: Self.monthColor(month: m),
                titleColor: Self.monthTextColor(month: m),
                detailColor: Self.monthSecondaryTextColor(month: m),
                isSelected: toggleOn
            )
            return
        }

        cell.configure(
            monthTitle: summary.monthTitle, countText: summary.countAttributedText(color: Self.monthSecondaryTextColor(month: item.month.month)),
            sizeText: summary.sizeText,
            bgColor: Self.monthColor(month: m),
            titleColor: Self.monthTextColor(month: m),
            detailColor: Self.monthSecondaryTextColor(month: m),
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
            guard let self, indexPath.section < self.mergedSections.count else { return }
            let section = self.mergedSections[indexPath.section]
            let allMonths = Set(section.rows.map(\.month))
            let leftState = self.selectionState(for: allMonths, in: self.selectedLocalMonths)
            let rightState = self.selectionState(for: allMonths, in: self.selectedRemoteMonths)
            let accentColor = self.leftHeaderLabel.textColor ?? .secondaryLabel
            supplementaryView.configure(section: section,
                                        leftState: leftState, rightState: rightState,
                                        selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
            supplementaryView.onLeftTap = { [weak self] in
                guard self?.isExecutionMode != true else { return }
                self?.toggleYearSelection(section: indexPath.section, side: .local)
            }
            supplementaryView.onRightTap = { [weak self] in
                guard self?.isExecutionMode != true else { return }
                self?.toggleYearSelection(section: indexPath.section, side: .remote)
            }
        }

        let arrowRegistration = UICollectionView.SupplementaryRegistration<DirectionArrowView>(
            elementKind: directionArrowElementKind
        ) { [weak self] arrowView, _, indexPath in
            guard let self else { return }
            // Badge supplementary indexPaths are sequential (0,1,2...),
            // but items alternate local/remote in the snapshot.
            // Badge N corresponds to the local item at index N*2.
            let itemIndexPath = IndexPath(item: indexPath.item * 2, section: indexPath.section)
            guard let item = self.dataSource.itemIdentifier(for: itemIndexPath) else { return }
            arrowView.configure(direction: item.arrowDirection, percent: self.progressPercent(for: item.month))
        }

        dataSource.supplementaryViewProvider = { collectionView, kind, indexPath in
            if kind == directionArrowElementKind {
                return collectionView.dequeueConfiguredReusableSupplementary(using: arrowRegistration, for: indexPath)
            }
            return collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
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
        let disconnected = dependencies.appSession.activeProfile == nil

        let disconnectAction = UIAction(
            title: "未连接",
            state: disconnected ? .on : .off
        ) { [weak self] _ in
            self?.disconnectRemote()
        }

        var profileActions: [UIAction] = []
        for profile in savedProfiles {
            let isActive = dependencies.appSession.activeProfile?.id == profile.id
            let action = UIAction(
                title: profile.name,
                subtitle: profile.storageProfile.displaySubtitle,
                state: isActive ? .on : .off
            ) { [weak self] _ in
                self?.promptPasswordAndConnect(profile: profile)
            }
            profileActions.append(action)
        }

        let profileSection = UIMenu(title: "", options: .displayInline, children: profileActions)
        let disconnectSection = UIMenu(title: "", options: .displayInline, children: [disconnectAction])

        return UIMenu(children: [profileSection, disconnectSection])
    }

    private func updateRightHeaderButton() {
        if isConnecting {
            rightHeaderLabel.text = "连接中..."
        } else if let profile = dependencies.appSession.activeProfile {
            rightHeaderLabel.text = profile.storageProfile.indicatorText
        } else {
            rightHeaderLabel.text = "远端存储"
        }
        let menu = buildDestinationMenu()
        rightHeaderButton.menu = menu
        rightHeaderMenuOverlay.menu = menu
    }

    // MARK: - Bindings

    private func bindSession() {
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            self.loadSavedProfiles()
            self.updateRightHeaderButton()
            self.syncRemoteDataIfNeeded()
            self.rebuildAndApply()
        }
    }

    private func bindDataManager() {
        homeDataManager.onDataChanged = { [weak self] in
            self?.rebuildAndApply()
        }
        homeDataManager.onFileSizesUpdated = { [weak self] in
            self?.reloadLocalAndApply()
        }
    }

    // MARK: - Data Loading

    private func scheduleReloadAllData() {
        reloadTask?.cancel()
        reloadTask = Task { [weak self] in
            guard let self else { return }
            await self.homeDataManager.ensureLocalIndexLoaded()
            self.attemptAutoConnectIfNeeded()
            self.syncRemoteDataIfNeeded()
            self.rebuildAndApply()
        }
    }

    private func reloadLocalAndApply() {
        localSummaries = homeDataManager.localMonthSummaries().map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
        }
        applyMergedSnapshot()
    }

    private func reloadRemoteAndApply(overrideActiveConnection: Bool? = nil) {
        guard overrideActiveConnection ?? hasActiveConnection else {
            remoteSummaries = []
            applyMergedSnapshot()
            return
        }
        let summaries = dependencies.backupCoordinator.remoteMonthSummaries()
        remoteSummaries = summaries.map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
        }
        applyMergedSnapshot()
    }

    private func rebuildAndApply() {
        localSummaries = homeDataManager.localMonthSummaries().map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
        }
        if hasActiveConnection {
            let summaries = dependencies.backupCoordinator.remoteMonthSummaries()
            remoteSummaries = summaries.map {
                MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
            }
        } else {
            remoteSummaries = []
        }
        applyMergedSnapshot()
    }

    private func applyMergedSnapshot() {
        let localByMonth = Dictionary(uniqueKeysWithValues: localSummaries.map { ($0.month, $0) })
        let remoteByMonth = Dictionary(uniqueKeysWithValues: remoteSummaries.map { ($0.month, $0) })
        let allMonths = Set(localByMonth.keys).union(remoteByMonth.keys)

        var rowsByYear: [Int: [MonthRow]] = [:]
        for month in allMonths {
            let row = MonthRow(month: month, local: localByMonth[month], remote: remoteByMonth[month])
            rowsByYear[month.year, default: []].append(row)
        }

        mergedSections = rowsByYear
            .map { year, rows in
                MergedYearSection(year: year, rows: rows.sorted { $0.month > $1.month })
            }
            .sorted { $0.year > $1.year }

        rowLookup = [:]
        for section in mergedSections {
            for row in section.rows {
                rowLookup[row.month] = row
            }
        }

        var snapshot = NSDiffableDataSourceSnapshot<Section, Item>()
        for section in mergedSections {
            snapshot.appendSections([.year(section.year)])
            var items: [Item] = []
            items.reserveCapacity(section.rows.count * 2)
            for row in section.rows {
                items.append(Item(side: .local, month: row.month, arrowDirection: arrowDirection(for: row.month)))
                items.append(Item(side: .remote, month: row.month))
            }
            snapshot.appendItems(items)
        }
        dataSource.applySnapshotUsingReloadData(snapshot)
        reconfigureVisibleHeaders()
        updateActionPanel()
    }

    /// Update remoteSummaries and rowLookup from snapshot cache without rebuilding the collection view snapshot.
    /// Also refreshes local backedUpCount so arrow progress percentages stay current.
    private func refreshRemoteDataInPlace() {
        guard hasActiveConnection else { return }
        let summaries = dependencies.backupCoordinator.remoteMonthSummaries()
        remoteSummaries = summaries.map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
        }
        // Refresh local backedUpCount for progress calculation
        let localByMonth = Dictionary(uniqueKeysWithValues:
            homeDataManager.localMonthSummaries().map {
                ($0.month, MonthSummary(month: $0.month, assetCount: $0.assetCount, photoCount: $0.photoCount, videoCount: $0.videoCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes))
            }
        )
        let remoteByMonth = Dictionary(uniqueKeysWithValues: remoteSummaries.map { ($0.month, $0) })
        for (month, var row) in rowLookup {
            row.remote = remoteByMonth[month]
            if let localSummary = localByMonth[month] {
                row.local = localSummary
            }
            rowLookup[month] = row
        }
        // Also add new remote-only months to rowLookup
        for summary in remoteSummaries where rowLookup[summary.month] == nil {
            rowLookup[summary.month] = MonthRow(month: summary.month, local: nil, remote: summary)
        }
    }

    private func reconfigureVisibleCells() {
        for cell in collectionView.visibleCells {
            guard let monthCell = cell as? MonthCell,
                  let indexPath = collectionView.indexPath(for: cell),
                  let item = dataSource.itemIdentifier(for: indexPath) else { continue }
            configureCell(monthCell, item: item)
        }
    }

    private func reconfigureVisibleArrows() {
        for (sectionIndex, section) in mergedSections.enumerated() {
            for (rowIndex, row) in section.rows.enumerated() {
                let badgeIndexPath = IndexPath(item: rowIndex, section: sectionIndex)
                guard let arrowView = collectionView.supplementaryView(
                    forElementKind: directionArrowElementKind, at: badgeIndexPath
                ) as? DirectionArrowView else { continue }
                let direction = arrowDirection(for: row.month)
                let percent = progressPercent(for: row.month)
                arrowView.configure(direction: direction, percent: percent)
            }
        }
    }

    private func arrowDirection(for month: LibraryMonthKey) -> ArrowDirection? {
        let l = selectedLocalMonths.contains(month)
        let r = selectedRemoteMonths.contains(month)
        var result: ArrowDirection? = nil
        switch (l, r) {
        case (true, false):  result = .toRemote
        case (false, true):  result = .toLocal
        case (true, true):   result = .sync
        case (false, false): result = nil
        }

        return result
    }

    /// Returns the progress percentage (0–100) for the given month based on its arrow direction,
    /// or nil if no direction is set or data is unavailable.
    private func progressPercent(for month: LibraryMonthKey) -> Double? {
        // During execution, use real-time processedCountByMonth for active upload months
        // because backedUpCount (fingerprint-based) doesn't refresh in the lightweight update path.
        if isExecutionMode, executionMonths.contains(month) {
            if completedMonths.contains(month) {
                return 100.0
            }
            if let total = assetCountByMonth[month], total > 0,
               let processed = processedCountByMonth[month], processed > 0 {
                return Double(processed) / Double(total) * 100
            }
            // Not yet started processing — fall through to backedUpCount-based calculation
        }

        guard let row = rowLookup[month] else { return nil }
        let direction = arrowDirection(for: month)
        guard let direction else { return nil }
        guard let backedUp = row.local?.backedUpCount else { return nil }

        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0

        switch direction {
        case .toRemote:
            guard localCount > 0 else { return nil }
            return Double(backedUp) / Double(localCount) * 100
        case .toLocal:
            guard remoteCount > 0 else { return nil }
            return Double(backedUp) / Double(remoteCount) * 100
        case .sync:
            let union = localCount + remoteCount - backedUp
            guard union > 0 else { return nil }
            return Double(backedUp) / Double(union) * 100
        }
    }

    private func selectionState(for months: Set<LibraryMonthKey>, in selected: Set<LibraryMonthKey>) -> SelectionState {
        guard !months.isEmpty else { return .none }
        if months.isSubset(of: selected) { return .all }
        if !months.isDisjoint(with: selected) { return .partial }
        return .none
    }

    private func reconfigureVisibleHeaders() {
        let accentColor = leftHeaderLabel.textColor ?? .secondaryLabel
        for section in 0 ..< collectionView.numberOfSections {
            guard section < mergedSections.count else { continue }
            let indexPath = IndexPath(item: 0, section: section)
            guard let header = collectionView.supplementaryView(
                forElementKind: UICollectionView.elementKindSectionHeader,
                at: indexPath
            ) as? MergedSectionHeaderView else { continue }
            let ms = mergedSections[section]
            let allMonths = Set(ms.rows.map(\.month))
            let leftState = selectionState(for: allMonths, in: selectedLocalMonths)
            let rightState = selectionState(for: allMonths, in: selectedRemoteMonths)
            header.configure(section: ms, leftState: leftState, rightState: rightState,
                             selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
        }
    }

    // MARK: - Connection Management

    private func loadSavedProfiles() {
        savedProfiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        activeProfileID = try? dependencies.databaseManager.activeServerProfileID()
    }

    private func attemptAutoConnectIfNeeded() {
        guard !didAttemptAutoConnect else { return }
        didAttemptAutoConnect = true

        guard let activeID = activeProfileID,
              let activeProfile = savedProfiles.first(where: { $0.id == activeID }) else {
            return
        }

        if activeProfile.storageProfile.requiresPassword {
            guard let password = try? dependencies.keychainService.readPassword(account: activeProfile.credentialRef),
                  !password.isEmpty else {
                return
            }
            connect(profile: activeProfile, password: password, showFailureAlert: false)
        } else {
            connect(profile: activeProfile, password: "", showFailureAlert: false)
        }
    }

    private func disconnectRemote() {
        try? dependencies.databaseManager.setActiveServerProfileID(nil)
        dependencies.appSession.clear()
    }

    private func promptPasswordAndConnect(profile: ServerProfileRecord) {
        if isConnecting { return }
        if !profile.storageProfile.requiresPassword {
            connect(profile: profile, password: "")
            return
        }

        if let saved = try? dependencies.keychainService.readPassword(account: profile.credentialRef),
           !saved.isEmpty {
            connect(profile: profile, password: saved)
            return
        }

        let alert = UIAlertController(title: "输入密码", message: profile.name, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Password"
            textField.isSecureTextEntry = true
        }
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "连接", style: .default) { [weak self] _ in
            guard let self,
                  let password = alert.textFields?.first?.text,
                  !password.isEmpty else { return }
            try? self.dependencies.keychainService.save(password: password, account: profile.credentialRef)
            self.connect(profile: profile, password: password)
        })
        present(alert, animated: true)
    }

    private func connect(profile: ServerProfileRecord, password: String, showFailureAlert: Bool = true) {
        guard !isConnecting else { return }
        isConnecting = true
        updateRightHeaderButton()

        Task { [weak self] in
            guard let self else { return }
            do {
                _ = try await self.dependencies.backupCoordinator.reloadRemoteIndex(
                    profile: profile,
                    password: password,
                    onMonthSynced: { [weak self] in
                        Task { @MainActor [weak self] in
                            self?.reloadRemoteAndApply(overrideActiveConnection: true)
                        }
                    }
                )
                try self.dependencies.databaseManager.setActiveServerProfileID(profile.id)
                self.dependencies.appSession.activate(profile: profile, password: password)

                await MainActor.run {
                    self.isConnecting = false
                    self.loadSavedProfiles()
                    self.updateRightHeaderButton()
                    self.syncRemoteDataIfNeeded()
                    self.rebuildAndApply()
                }
            } catch {
                await MainActor.run {
                    self.isConnecting = false
                    self.updateRightHeaderButton()
                    if showFailureAlert {
                        self.showAlert(
                            title: "连接失败",
                            message: profile.userFacingStorageErrorMessage(error)
                        )
                    }
                }
            }
        }
    }

    private func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        if profile.storageProfile.requiresPassword {
            guard let password = dependencies.appSession.activePassword,
                  !password.isEmpty else {
                return nil
            }
            return password
        }
        return dependencies.appSession.activePassword ?? ""
    }

    // MARK: - Selection

    private enum Side { case local, remote }

    private func toggleMonthSelection(_ month: LibraryMonthKey, side: Side) {
        switch side {
        case .local:
            if selectedLocalMonths.contains(month) { selectedLocalMonths.remove(month) }
            else { selectedLocalMonths.insert(month) }
        case .remote:
            if selectedRemoteMonths.contains(month) { selectedRemoteMonths.remove(month) }
            else { selectedRemoteMonths.insert(month) }
        }
        refreshSelectionState()
    }

    private func toggleYearSelection(section: Int, side: Side) {
        guard section < mergedSections.count else { return }
        let allMonths = Set(mergedSections[section].rows.map(\.month))
        switch side {
        case .local:
            if allMonths.isSubset(of: selectedLocalMonths) { selectedLocalMonths.subtract(allMonths) }
            else { selectedLocalMonths.formUnion(allMonths) }
        case .remote:
            if allMonths.isSubset(of: selectedRemoteMonths) { selectedRemoteMonths.subtract(allMonths) }
            else { selectedRemoteMonths.formUnion(allMonths) }
        }
        refreshSelectionState()
    }

    private func toggleAllSelection(side: Side) {
        let allMonths = Set(mergedSections.flatMap { $0.rows.map(\.month) })
        switch side {
        case .local:
            if allMonths.isSubset(of: selectedLocalMonths) { selectedLocalMonths.removeAll() }
            else { selectedLocalMonths = allMonths }
        case .remote:
            if allMonths.isSubset(of: selectedRemoteMonths) { selectedRemoteMonths.removeAll() }
            else { selectedRemoteMonths = allMonths }
        }
        refreshSelectionState()
    }

    private func refreshSelectionState() {
        var snapshot = NSDiffableDataSourceSnapshot<Section, Item>()
        for section in mergedSections {
            snapshot.appendSections([.year(section.year)])
            var items: [Item] = []
            items.reserveCapacity(section.rows.count * 2)
            for row in section.rows {
                items.append(Item(side: .local, month: row.month, arrowDirection: arrowDirection(for: row.month)))
                items.append(Item(side: .remote, month: row.month))
            }
            snapshot.appendItems(items)
        }
        dataSource.applySnapshotUsingReloadData(snapshot)
        reconfigureVisibleHeaders()
        updateTopHeaderToggles()
        updateActionPanel()
    }

    @objc private func leftToggleTapped() {
        toggleAllSelection(side: .local)
    }

    @objc private func rightToggleTapped() {
        toggleAllSelection(side: .remote)
    }

    private func updateTopHeaderToggles() {
        let allMonths = Set(mergedSections.flatMap { $0.rows.map(\.month) })
        let headerColor = leftHeaderLabel.textColor ?? .secondaryLabel
        let config = UIImage.SymbolConfiguration(pointSize: 14)

        let leftState = selectionState(for: allMonths, in: selectedLocalMonths)
        let leftIcon: String
        switch leftState {
        case .all:     leftIcon = "checkmark.circle.fill"
        case .partial: leftIcon = "minus.circle.fill"
        case .none:    leftIcon = "circle"
        }
        leftToggle.setImage(UIImage(systemName: leftIcon, withConfiguration: config), for: .normal)
        leftToggle.tintColor = headerColor

        let rightState = selectionState(for: allMonths, in: selectedRemoteMonths)
        let rightIcon: String
        switch rightState {
        case .all:     rightIcon = "checkmark.circle.fill"
        case .partial: rightIcon = "minus.circle.fill"
        case .none:    rightIcon = "circle"
        }
        rightToggle.setImage(UIImage(systemName: rightIcon, withConfiguration: config), for: .normal)
        rightToggle.tintColor = headerColor
    }

    private func selectionCounts() -> (backup: Int, download: Int, sync: Int) {
        let allSelectedMonths = selectedLocalMonths.union(selectedRemoteMonths)
        var backup = 0, download = 0, sync = 0
        for month in allSelectedMonths {
            switch arrowDirection(for: month) {
            case .toRemote: backup += 1
            case .toLocal:  download += 1
            case .sync:     sync += 1
            case nil:       break
            }
        }
        return (backup, download, sync)
    }

    private var isPanelShown = false

    private func updateActionPanel() {
        let counts = selectionCounts()
        actionPanel.configure(backupCount: counts.backup, downloadCount: counts.download, syncCount: counts.sync)
        actionPanel.backupCategoryButton.menu = buildCategoryMenu(for: .toRemote)
        actionPanel.downloadCategoryButton.menu = buildCategoryMenu(for: .toLocal)
        actionPanel.syncCategoryButton.menu = buildCategoryMenu(for: .sync)

        let shouldShow = !selectedLocalMonths.isEmpty || !selectedRemoteMonths.isEmpty

        if shouldShow && !isPanelShown {
            isPanelShown = true
            panelHiddenConstraint?.deactivate()
            panelShownConstraint?.activate()
            UIView.animate(withDuration: 0.3, delay: 0, options: .curveEaseOut) {
                self.view.layoutIfNeeded()
            }
        } else if !shouldShow && isPanelShown {
            isPanelShown = false
            panelShownConstraint?.deactivate()
            panelHiddenConstraint?.activate()
            UIView.animate(withDuration: 0.25, delay: 0, options: .curveEaseIn) {
                self.view.layoutIfNeeded()
            }
        }
    }

    private func syncRemoteDataInBackground(overrideActiveConnection: Bool? = nil) async {
        let active = overrideActiveConnection ?? hasActiveConnection
        let revision = homeDataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        let backupCoordinator = dependencies.backupCoordinator
        let snapshotState = await Task.detached {
            backupCoordinator.currentRemoteSnapshotState(since: revision)
        }.value
        homeDataManager.syncRemoteSnapshot(
            state: snapshotState,
            hasActiveConnection: active
        )
    }

    @discardableResult
    private func syncRemoteDataIfNeeded(overrideActiveConnection: Bool? = nil) -> Bool {
        let active = overrideActiveConnection ?? hasActiveConnection
        let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(
            since: homeDataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        )
        return homeDataManager.syncRemoteSnapshot(
            state: snapshotState,
            hasActiveConnection: active
        )
    }

    // MARK: - Actions

    @objc private func backupTapped() {
        let counts = selectionCounts()
        guard counts.backup > 0 || counts.download > 0 || counts.sync > 0 else { return }

        var lines: [String] = []
        if counts.backup > 0 { lines.append("备份 \(counts.backup) 个月份") }
        if counts.download > 0 { lines.append("下载 \(counts.download) 个月份") }
        if counts.sync > 0 { lines.append("同步 \(counts.sync) 个月份") }
        let message = lines.joined(separator: "\n")

        let allSelectedMonths = selectedLocalMonths.union(selectedRemoteMonths)
        let upload = allSelectedMonths.filter { arrowDirection(for: $0) == .toRemote }.sorted()
        let download = allSelectedMonths.filter { arrowDirection(for: $0) == .toLocal }.sorted()
        let sync = allSelectedMonths.filter { arrowDirection(for: $0) == .sync }.sorted()

        let alert = UIAlertController(
            title: "确认执行",
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "开始", style: .default) { [weak self] _ in
            self?.enterExecutionMode(uploadMonths: upload, downloadMonths: download, syncMonths: sync)
        })
        present(alert, animated: true)
    }

    // MARK: - Execution Mode

    private func enterExecutionMode(
        uploadMonths: [LibraryMonthKey],
        downloadMonths: [LibraryMonthKey],
        syncMonths: [LibraryMonthKey]
    ) {
        isExecutionMode = true
        self.uploadMonths = uploadMonths
        self.pendingDownloadMonths = downloadMonths
        self.pendingSyncMonths = syncMonths
        isDownloadPhase = false
        downloadTask = nil

        let allMonths = Set(uploadMonths).union(downloadMonths).union(syncMonths)
        executionMonths = allMonths
        completedMonths.removeAll()
        activeMonths.removeAll()
        lastObservedState = nil

        // Snapshot current toggle states
        snapshotLocalSelection = selectedLocalMonths
        snapshotRemoteSelection = selectedRemoteMonths

        // Switch UI to execution mode
        switchToExecutionPanel()
        disableSelectionInteraction()
        applyMergedSnapshot()

        // Start upload phase if there are upload months
        let backupTargetMonths = uploadMonths + syncMonths
        if !backupTargetMonths.isEmpty {
            startUploadPhase(months: backupTargetMonths)
        } else {
            startDownloadPhase()
        }
    }

    private func startUploadPhase(months: [LibraryMonthKey]) {
        var allAssetIDs = Set<String>()
        for month in months {
            let ids = homeDataManager.localAssetIDs(for: month)
            allAssetIDs.formUnion(ids)
            assetCountByMonth[month] = ids.count
        }

        let selection = BackupScopeSelection(
            selectedAssetIDs: allAssetIDs,
            selectedAssetCount: allAssetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: allAssetIDs.count,
            totalEstimatedBytes: nil
        )
        backupSessionController.updateScopeSelection(selection)

        backupObserverID = backupSessionController.addObserver { [weak self] snapshot in
            self?.handleBackupSnapshot(snapshot)
        }

        backupSessionController.startBackup()
    }

    private func exitExecutionMode() {
        downloadTask?.cancel()
        downloadTask = nil
        isDownloadPhase = false
        isExecutionMode = false
        executionMonths.removeAll()
        snapshotLocalSelection.removeAll()
        snapshotRemoteSelection.removeAll()
        completedMonths.removeAll()
        activeMonths.removeAll()
        activeMonthProgress = nil
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        lastObservedState = nil
        uploadMonths.removeAll()
        pendingDownloadMonths.removeAll()
        pendingSyncMonths.removeAll()

        if let observerID = backupObserverID {
            backupSessionController.removeObserver(observerID)
            backupObserverID = nil
        }

        switchToSelectionPanel()
        enableSelectionInteraction()
        // Reload all data so backedUpCount reflects assets uploaded in this run
        scheduleReloadAllData()
    }

    private func switchToExecutionPanel() {
        isPanelShown = false
        panelShownConstraint?.deactivate()
        panelHiddenConstraint?.activate()

        collectionBottomToActionPanel?.deactivate()
        collectionBottomToExecutionPanel?.activate()

        executionPanelHiddenConstraint?.deactivate()
        executionPanelShownConstraint?.activate()

        UIView.animate(withDuration: 0.3, delay: 0, options: .curveEaseOut) {
            self.view.layoutIfNeeded()
        }
    }

    private func switchToSelectionPanel() {
        executionPanelShownConstraint?.deactivate()
        executionPanelHiddenConstraint?.activate()

        collectionBottomToExecutionPanel?.deactivate()
        collectionBottomToActionPanel?.activate()

        UIView.animate(withDuration: 0.25, delay: 0, options: .curveEaseIn) {
            self.view.layoutIfNeeded()
        }
    }

    private func disableSelectionInteraction() {
        leftToggle.isEnabled = false
        rightToggle.isEnabled = false
        rightHeaderMenuOverlay.isEnabled = false
        rightHeaderButton.isEnabled = false
    }

    private func enableSelectionInteraction() {
        leftToggle.isEnabled = true
        rightToggle.isEnabled = true
        rightHeaderMenuOverlay.isEnabled = true
        rightHeaderButton.isEnabled = true
    }

    private func executionPauseTapped() {
        if isDownloadPhase {
            downloadTask?.cancel()
            downloadTask = nil
            executionPanel.update(
                state: .paused,
                statusText: "已暂停",
                completedCount: completedMonths.count,
                totalCount: executionMonths.count
            )
        } else {
            backupSessionController.pauseBackup()
        }
    }

    private func executionResumeTapped() {
        if isDownloadPhase {
            startDownloadPhase()
        } else {
            backupSessionController.startBackup()
        }
    }

    private func executionStopTapped() {
        let alert = UIAlertController(title: "确认停止", message: "停止后需要重新选择月份执行", preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "停止", style: .destructive) { [weak self] _ in
            if self?.isDownloadPhase == true {
                self?.downloadTask?.cancel()
                self?.downloadTask = nil
                self?.exitExecutionMode()
            } else {
                self?.backupSessionController.stopBackup()
            }
        })
        present(alert, animated: true)
    }

    // MARK: - Download / Sync Phase

    private func startDownloadPhase() {
        guard !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty else {
            showExecutionCompletedAlert()
            return
        }

        isDownloadPhase = true

        guard let profile = dependencies.appSession.activeProfile,
              let password = resolvedSessionPassword(for: profile) else {
            showAlert(title: "错误", message: "未连接远端存储")
            exitExecutionMode()
            return
        }

        // Collect months to process: downloads first, then syncs
        let downloadMonths = pendingDownloadMonths.filter { !completedMonths.contains($0) }
        let syncMonths = pendingSyncMonths.filter { !completedMonths.contains($0) }

        downloadTask = Task { [weak self] in
            guard let self else { return }

            // Phase 2: Pure download months
            // Run scoped backup first to ensure local hash index is populated,
            // preventing duplicate downloads for assets that exist locally but lack hashes.
            for month in downloadMonths {
                if Task.isCancelled { return }
                await self.ensureHashIndexAndDownload(month: month, phase: "下载", profile: profile, password: password)
            }

            // Phase 3: Sync months (upload then download each month)
            for month in syncMonths {
                if Task.isCancelled { return }
                await self.ensureHashIndexAndDownload(month: month, phase: "同步", profile: profile, password: password)
            }

            // All done
            if !Task.isCancelled {
                await MainActor.run {
                    self.activeMonths.removeAll()
                    self.showExecutionCompletedAlert()
                }
            }
        }
    }

    /// Run a scoped backup for given asset IDs and wait for completion.
    /// Returns true if backup completed successfully, false otherwise.
    private func runScopedBackup(assetIDs: Set<String>) async -> Bool {
        await withCheckedContinuation { continuation in
            Task { @MainActor in
                let selection = BackupScopeSelection(
                    selectedAssetIDs: assetIDs,
                    selectedAssetCount: assetIDs.count,
                    selectedEstimatedBytes: nil,
                    totalAssetCount: assetIDs.count,
                    totalEstimatedBytes: nil
                )
                self.backupSessionController.updateScopeSelection(selection)

                // Remove existing observer if any
                if let id = self.backupObserverID {
                    self.backupSessionController.removeObserver(id)
                    self.backupObserverID = nil
                }

                var hasResumed = false
                let observerID = self.backupSessionController.addObserver { [weak self] snapshot in
                    guard let self, !hasResumed else { return }
                    // Track progress for upload phase visual feedback
                    if snapshot.total > 0 {
                        let completed = snapshot.succeeded + snapshot.failed + snapshot.skipped
                        self.activeMonthProgress = (completed: completed, total: snapshot.total)
                    }
                    self.reconfigureVisibleCells()

                    switch snapshot.state {
                    case .completed:
                        hasResumed = true
                        self.backupSessionController.removeObserver(observerID)
                        self.backupObserverID = nil
                        continuation.resume(returning: true)
                    case .failed, .stopped:
                        hasResumed = true
                        self.backupSessionController.removeObserver(observerID)
                        self.backupObserverID = nil
                        continuation.resume(returning: false)
                    default:
                        break
                    }
                }
                self.backupObserverID = observerID

                self.backupSessionController.startBackup()
            }
        }
    }

    private func ensureHashIndexAndDownload(
        month: LibraryMonthKey,
        phase: String,
        profile: ServerProfileRecord,
        password: String
    ) async {
        await MainActor.run {
            self.activeMonths = [month]
            self.activeMonthProgress = nil
            self.executionPanel.update(
                state: .running,
                statusText: "正在\(phase) \(self.completedMonths.count)/\(self.executionMonths.count) 月份",
                completedCount: self.completedMonths.count,
                totalCount: self.executionMonths.count
            )
            self.dataSource.applySnapshotUsingReloadData(self.dataSource.snapshot())
        }

        // Run scoped backup to ensure local hash index is populated.
        // Already-backed-up assets will be skipped quickly.
        let assetIDs = homeDataManager.localAssetIDs(for: month)
        if !assetIDs.isEmpty {
            let uploadCompleted = await runScopedBackup(assetIDs: assetIDs)
            if !uploadCompleted || Task.isCancelled { return }
        }

        // Refresh remote data after potential upload
        await MainActor.run { [self] in
            _ = self.syncRemoteDataIfNeeded()
        }

        // Now download remoteOnly items with accurate matching
        await processDownloadMonth(month, phase: phase, profile: profile, password: password)
    }

    private func processDownloadMonth(
        _ month: LibraryMonthKey,
        phase: String,
        profile: ServerProfileRecord,
        password: String
    ) async {
        let remoteItems = homeDataManager.remoteOnlyItems(for: month)
        if !remoteItems.isEmpty {
            await MainActor.run {
                self.activeMonthProgress = (completed: 0, total: remoteItems.count)
                self.reconfigureVisibleCells()
            }

            do {
                let results = try await dependencies.restoreService.restoreItems(
                    items: remoteItems.map(\.resources),
                    profile: profile,
                    password: password,
                    onItemCompleted: { [weak self] completed, total in
                        guard let self else { return }
                        self.activeMonthProgress = (completed: completed, total: total)
                        self.reconfigureVisibleCells()
                    }
                )

                if !results.isEmpty {
                    writeHashIndex(results: results, remoteItems: remoteItems)
                    homeDataManager.refreshLocalIndex(forAssetIDs: Set(results.map(\.asset.localIdentifier)))
                }
            } catch {
                if Task.isCancelled { return }
                await MainActor.run {
                    self.showAlert(title: "\(phase)失败", message: "\(String(format: "%04d年%02d月", month.year, month.month)): \(error.localizedDescription)")
                }
            }
        }

        if Task.isCancelled { return }
        await MainActor.run {
            self.activeMonthProgress = nil
            self.completedMonths.insert(month)
            self.dataSource.applySnapshotUsingReloadData(self.dataSource.snapshot())
        }
    }

    private func writeHashIndex(results: [RestoreService.IndexedRestoredAsset], remoteItems: [RemoteAlbumItem]) {
        let hashRepo = ContentHashIndexRepository(databaseManager: dependencies.databaseManager)

        for result in results {
            guard result.itemIndex < remoteItems.count else { continue }
            let remoteItem = remoteItems[result.itemIndex]

            var records: [LocalAssetResourceHashRecord] = []
            var totalSize: Int64 = 0
            for link in remoteItem.resourceLinks {
                if let resource = remoteItem.resources.first(where: { $0.contentHash == link.resourceHash }) {
                    records.append(LocalAssetResourceHashRecord(
                        role: link.role,
                        slot: link.slot,
                        contentHash: link.resourceHash,
                        fileSize: resource.fileSize
                    ))
                    totalSize += resource.fileSize
                }
            }

            try? hashRepo.upsertAssetHashSnapshot(
                assetLocalIdentifier: result.asset.localIdentifier,
                assetFingerprint: remoteItem.assetFingerprint,
                resources: records,
                totalFileSizeBytes: totalSize
            )
        }
    }

    private func showExecutionCompletedAlert() {
        let alert = UIAlertController(
            title: "执行完成",
            message: "已完成 \(completedMonths.count)/\(executionMonths.count) 月份",
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: "确定", style: .default) { [weak self] _ in
            self?.exitExecutionMode()
        })
        present(alert, animated: true)
    }

    private func handleBackupSnapshot(_ snapshot: BackupSessionController.Snapshot) {
        guard isExecutionMode else { return }

        // Track month transitions
        let newCompleted = snapshot.flushedMonths.intersection(executionMonths)
        let newActive = snapshot.startedMonths.intersection(executionMonths).subtracting(snapshot.flushedMonths)

        let hadMonthStateChange = !newCompleted.subtracting(completedMonths).isEmpty
            || activeMonths != newActive

        completedMonths.formUnion(newCompleted)
        activeMonths = newActive
        processedCountByMonth = snapshot.processedCountByMonth

        // Update execution panel
        let overallProgressText: String
        if snapshot.total > 0 {
            let pct = Int(Double(snapshot.succeeded + snapshot.failed + snapshot.skipped) / Double(snapshot.total) * 100)
            overallProgressText = "正在上传 \(completedMonths.count)/\(executionMonths.count) 月份 (\(pct)%)"
        } else {
            overallProgressText = "正在上传 \(completedMonths.count)/\(executionMonths.count) 月份"
        }
        executionPanel.update(
            state: snapshot.state,
            statusText: overallProgressText,
            completedCount: completedMonths.count,
            totalCount: executionMonths.count
        )

        // Month state change (started/flushed): full rebuild needed for cell state transitions
        // Progress-only update: lightweight reconfigure on existing cells (preserves indicator)
        if hadMonthStateChange {
            rebuildAndApply()
        } else {
            refreshRemoteDataInPlace()
            reconfigureVisibleCells()
            reconfigureVisibleArrows()
        }
        lastObservedState = snapshot.state

        // Handle terminal states
        switch snapshot.state {
        case .completed:
            completedMonths.formUnion(uploadMonths)
            activeMonths.removeAll()

            if let id = backupObserverID {
                backupSessionController.removeObserver(id)
                backupObserverID = nil
            }

            rebuildAndApply()

            if !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty {
                startDownloadPhase()
            } else {
                showExecutionCompletedAlert()
            }

        case .failed:
            rebuildAndApply()
            let alert = UIAlertController(title: "上传失败", message: snapshot.statusText, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "确定", style: .default) { [weak self] _ in
                self?.exitExecutionMode()
            })
            present(alert, animated: true)

        case .stopped:
            rebuildAndApply()
            exitExecutionMode()

        default:
            break
        }
    }

    private func scrollToMonth(_ month: LibraryMonthKey) {
        for (sectionIndex, section) in mergedSections.enumerated() {
            guard let rowIndex = section.rows.firstIndex(where: { $0.month == month }) else { continue }
            let itemIndex = rowIndex * 2
            let indexPath = IndexPath(item: itemIndex, section: sectionIndex)
            collectionView.scrollToItem(at: indexPath, at: .centeredVertically, animated: true)
            return
        }
    }

    private func buildCategoryMenu(for category: ArrowDirection) -> UIMenu {
        let allSelected = selectedLocalMonths.union(selectedRemoteMonths)
        let months = allSelected
            .filter { arrowDirection(for: $0) == category }
            .sorted(by: <)

        var byYear: [Int: [LibraryMonthKey]] = [:]
        for month in months {
            byYear[month.year, default: []].append(month)
        }

        let yearMenus = byYear.keys.sorted().map { year -> UIMenu in
            let actions = byYear[year]!.map { month -> UIAction in
                let row = rowLookup[month]
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

    // MARK: - Season Colors

    private struct SeasonStyle {
        let bg: UIColor
        let title: UIColor
        let detail: UIColor
    }

    private static let seasonStyles: [SeasonStyle] = [
        SeasonStyle(
            bg:     .materialSurface(light: .Material.Green._50, darkTint: .Material.Green._200),
            title:  .materialOnContainer(light: .Material.Green._900, dark: .Material.Green._100),
            detail: .materialOnSurfaceVariant(light: .Material.Green._700, dark: .Material.Green._200)
        ),
        SeasonStyle(
            bg:     .materialSurface(light: .Material.Blue._50, darkTint: .Material.Blue._200),
            title:  .materialOnContainer(light: .Material.Blue._900, dark: .Material.Blue._100),
            detail: .materialOnSurfaceVariant(light: .Material.Blue._700, dark: .Material.Blue._200)
        ),
        SeasonStyle(
            bg:     .materialSurface(light: .Material.Amber._50, darkTint: .Material.Amber._200),
            title:  .materialOnContainer(light: .Material.Amber._900, dark: .Material.Amber._100),
            detail: .materialOnSurfaceVariant(light: .Material.Amber._700, dark: .Material.Amber._200)
        ),
        SeasonStyle(
            bg:     .materialSurface(light: .Material.Red._50, darkTint: .Material.Red._200),
            title:  .materialOnContainer(light: .Material.Red._900, dark: .Material.Red._100),
            detail: .materialOnSurfaceVariant(light: .Material.Red._700, dark: .Material.Red._200)
        ),
    ]

    fileprivate static func seasonIndex(for month: Int) -> Int {
        switch month {
        case 1...3:  return 0
        case 4...6:  return 1
        case 7...9:  return 2
        case 10...12: return 3
        default:      return 0
        }
    }

    fileprivate static func monthColor(month: Int) -> UIColor { seasonStyles[seasonIndex(for: month)].bg }
    fileprivate static func monthTextColor(month: Int) -> UIColor { seasonStyles[seasonIndex(for: month)].title }
    fileprivate static func monthSecondaryTextColor(month: Int) -> UIColor { seasonStyles[seasonIndex(for: month)].detail }
}

// MARK: - UICollectionViewDelegate

extension NewHomeViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard !isExecutionMode else { return }
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        switch item.side {
        case .local:
            toggleMonthSelection(item.month, side: .local)
        case .remote:
            toggleMonthSelection(item.month, side: .remote)
        }
    }
}

// MARK: - Merged Section Header View

private final class MergedSectionHeaderView: UICollectionReusableView {
    private let leftHalf = HalfHeaderView()
    private let rightHalf = HalfHeaderView()
    private let divider = UIView()

    var onLeftTap: (() -> Void)?
    var onRightTap: (() -> Void)?

    override init(frame: CGRect) {
        super.init(frame: frame)
        divider.backgroundColor = .clear

        addSubview(leftHalf)
        addSubview(divider)
        addSubview(rightHalf)

        leftHalf.snp.makeConstraints { make in
            make.top.bottom.leading.equalToSuperview()
            make.trailing.equalTo(divider.snp.leading)
        }
        divider.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.top.bottom.equalToSuperview()
            make.width.equalTo(2)
        }
        rightHalf.snp.makeConstraints { make in
            make.top.bottom.trailing.equalToSuperview()
            make.leading.equalTo(divider.snp.trailing)
        }

        leftHalf.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(leftTapped)))
        rightHalf.addGestureRecognizer(UITapGestureRecognizer(target: self, action: #selector(rightTapped)))
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    @objc private func leftTapped() { onLeftTap?() }
    @objc private func rightTapped() { onRightTap?() }

    func configure(section: NewHomeViewController.MergedYearSection,
                   leftState: NewHomeViewController.SelectionState,
                   rightState: NewHomeViewController.SelectionState,
                   selectedColor: UIColor, deselectedColor: UIColor) {
        let headerFont = UIFont.monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        let headerColor = UIColor.tertiaryLabel
        leftHalf.configure(
            title: section.title,
            countText: Self.mediaCountAttributedString(photoCount: section.localPhotoCount, videoCount: section.localVideoCount, font: headerFont, color: headerColor),
            sizeText: section.localSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            selectionState: leftState,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
        rightHalf.configure(
            title: section.title,
            countText: Self.mediaCountAttributedString(photoCount: section.remotePhotoCount, videoCount: section.remoteVideoCount, font: headerFont, color: headerColor),
            sizeText: section.remoteSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            selectionState: rightState,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
    }

    private static func mediaCountAttributedString(photoCount: Int, videoCount: Int, font: UIFont, color: UIColor) -> NSAttributedString {
        let symbolConfig = UIImage.SymbolConfiguration(pointSize: 9, weight: .bold)
        let result = NSMutableAttributedString()
        if let img = UIImage(systemName: "photo", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(photoCount)  ", attributes: [.font: font, .foregroundColor: color]))
        if let img = UIImage(systemName: "video", withConfiguration: symbolConfig)?.withTintColor(color, renderingMode: .alwaysOriginal) {
            result.append(NSAttributedString(attachment: NSTextAttachment(image: img)))
        }
        result.append(NSAttributedString(string: " \(videoCount)", attributes: [.font: font, .foregroundColor: color]))
        return result
    }
}

// MARK: - Half Header View (reused for left/right)

private final class HalfHeaderView: UIView {
    private let checkmark = UIImageView()
    private let titleLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)

        checkmark.contentMode = .scaleAspectFit
        checkmark.tintColor = .tertiaryLabel
        checkmark.isHidden = true

        titleLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        titleLabel.textColor = .secondaryLabel

        countLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        countLabel.textColor = .tertiaryLabel

        sizeLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        sizeLabel.textColor = .tertiaryLabel

        addSubview(checkmark)
        addSubview(titleLabel)
        addSubview(sizeLabel)
        addSubview(countLabel)

        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        checkmark.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(18)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }
        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.bottom.equalTo(snp.centerY).offset(-3)
        }
        sizeLabel.snp.makeConstraints { make in
            make.leading.equalTo(titleLabel.snp.trailing).offset(6)
            make.centerY.equalTo(titleLabel)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
        countLabel.snp.makeConstraints { make in
            make.top.equalTo(snp.centerY).offset(3)
            make.leading.equalToSuperview().inset(16)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(title: String?, countText: NSAttributedString?, sizeText: String?,
                   selectionState: NewHomeViewController.SelectionState,
                   selectedColor: UIColor?, deselectedColor: UIColor?) {
        titleLabel.text = title
        countLabel.attributedText = countText
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil

        checkmark.isHidden = false
        switch selectionState {
        case .all:
            checkmark.image = UIImage(systemName: "checkmark.circle.fill")
            checkmark.tintColor = selectedColor ?? .secondaryLabel
        case .partial:
            checkmark.image = UIImage(systemName: "minus.circle.fill")
            checkmark.tintColor = selectedColor ?? .secondaryLabel
        case .none:
            checkmark.image = UIImage(systemName: "circle")
            checkmark.tintColor = deselectedColor ?? .tertiaryLabel
        }
        titleLabel.snp.updateConstraints { make in
            make.leading.equalToSuperview().inset(50)
        }
        countLabel.snp.updateConstraints { make in
            make.leading.equalToSuperview().inset(50)
        }
    }
}

// MARK: - Direction Arrow Badge

private let directionArrowElementKind = "direction-arrow"

private final class DirectionArrowView: UICollectionReusableView {
    private let imageView = UIImageView()
    private let percentLabel: UILabel = {
        let label = UILabel()
        label.font = .monospacedDigitSystemFont(ofSize: 9, weight: .light)
        label.textAlignment = .center
        label.adjustsFontSizeToFitWidth = true
        label.minimumScaleFactor = 0.7
        return label
    }()

    override init(frame: CGRect) {
        super.init(frame: frame)
        isUserInteractionEnabled = false
        backgroundColor = .clear
        imageView.contentMode = .scaleAspectFit
        addSubview(imageView)
        addSubview(percentLabel)
        imageView.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalTo(20)
        }
        percentLabel.snp.makeConstraints { make in
            make.top.equalTo(imageView.snp.bottom)
            make.leading.trailing.equalToSuperview()
            make.bottom.lessThanOrEqualToSuperview()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    override func prepareForReuse() {
        super.prepareForReuse()
        imageView.image = nil
        percentLabel.attributedText = nil
        isHidden = true
    }

    func configure(direction: NewHomeViewController.ArrowDirection?, percent: Double? = nil) {
        guard let direction else {
            imageView.image = nil
            percentLabel.attributedText = nil
            isHidden = true
            return
        }

        let symbolName: String
        let iconColor: UIColor
        switch direction {
        case .toRemote:
            symbolName = "arrow.right"
            iconColor = .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        case .toLocal:
            symbolName = "arrow.left"
            iconColor = .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        case .sync:
            symbolName = "arrow.left.arrow.right"
            iconColor = .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        }

        let config = UIImage.SymbolConfiguration(pointSize: 14, weight: .bold)
        imageView.image = UIImage(systemName: symbolName, withConfiguration: config)
        imageView.tintColor = iconColor

        if let percent {
            let text = String(format: "%.1f%%", percent)
            let attrStr = NSAttributedString(string: text, attributes: [
                .kern: -0.5,
                .font: percentLabel.font!,
                .foregroundColor: iconColor
            ])
            percentLabel.attributedText = attrStr
            percentLabel.isHidden = false
        } else {
            percentLabel.attributedText = nil
            percentLabel.isHidden = true
        }

        isHidden = false
    }
}

// MARK: - Month Cell

private final class MonthCell: UICollectionViewCell {
    private let colorView = UIView()
    private let checkmark = UIImageView()
    private let activityIndicator = UIActivityIndicatorView(style: .medium)
    private let monthLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private var leftStackLeading: Constraint?
    private var currentTitleColor: UIColor = .label
    private var currentDetailColor: UIColor = .secondaryLabel

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
        contentView.backgroundColor = .clear

        contentView.addSubview(colorView)
        colorView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        checkmark.contentMode = .scaleAspectFit
        checkmark.tintColor = .tertiaryLabel
        checkmark.isHidden = true
        colorView.addSubview(checkmark)
        checkmark.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(18)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }

        activityIndicator.hidesWhenStopped = true
        colorView.addSubview(activityIndicator)
        activityIndicator.snp.makeConstraints { make in
            make.center.equalTo(checkmark)
        }

        monthLabel.font = .systemFont(ofSize: 15, weight: .medium)
        countLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        sizeLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)

        colorView.addSubview(monthLabel)
        colorView.addSubview(sizeLabel)
        colorView.addSubview(countLabel)

        monthLabel.setContentHuggingPriority(.required, for: .horizontal)
        monthLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        monthLabel.snp.makeConstraints { make in
            self.leftStackLeading = make.leading.equalToSuperview().inset(16).constraint
            make.bottom.equalTo(colorView.snp.centerY).offset(-3)
        }
        sizeLabel.snp.makeConstraints { make in
            make.leading.equalTo(monthLabel.snp.trailing).offset(6)
            make.centerY.equalTo(monthLabel)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
        countLabel.snp.makeConstraints { make in
            make.leading.equalTo(monthLabel)
            make.top.equalTo(colorView.snp.centerY).offset(3)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(monthTitle: String, countText: NSAttributedString, sizeText: String?,
                   bgColor: UIColor, titleColor: UIColor, detailColor: UIColor, isSelected: Bool?) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        currentTitleColor = titleColor
        currentDetailColor = detailColor

        if let selected = isSelected {
            checkmark.isHidden = false
            checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
            checkmark.tintColor = selected ? titleColor : detailColor
            leftStackLeading?.update(inset: 50)
        } else {
            checkmark.isHidden = true
            leftStackLeading?.update(inset: 50)
        }
    }

    func configureEmpty(bgColor: UIColor) {
        monthLabel.text = nil
        monthLabel.isHidden = true
        countLabel.text = nil
        countLabel.isHidden = true
        sizeLabel.text = nil
        sizeLabel.isHidden = true
        checkmark.isHidden = true
        colorView.backgroundColor = bgColor
        leftStackLeading?.update(inset: 50)
    }

    func setSelected(_ selected: Bool) {
        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
        checkmark.tintColor = selected ? currentTitleColor : currentDetailColor
        leftStackLeading?.update(inset: 50)
    }

    func configureRunning(monthTitle: String, countText: NSAttributedString, sizeText: String?,
                          bgColor: UIColor, titleColor: UIColor, detailColor: UIColor) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        currentTitleColor = titleColor
        currentDetailColor = detailColor

        checkmark.isHidden = true
        activityIndicator.color = titleColor
        if !activityIndicator.isAnimating {
            activityIndicator.startAnimating()
        }
        leftStackLeading?.update(inset: 50)
    }

    func configureCompleted(monthTitle: String, countText: NSAttributedString, sizeText: String?) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.attributedText = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil

        let grayBg = UIColor.systemGray5
        let grayTitle = UIColor.secondaryLabel
        let grayDetail = UIColor.tertiaryLabel
        colorView.backgroundColor = grayBg
        monthLabel.textColor = grayTitle
        countLabel.textColor = grayDetail
        sizeLabel.textColor = grayDetail
        currentTitleColor = grayTitle
        currentDetailColor = grayDetail

        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: "checkmark.circle.fill")
        checkmark.tintColor = .systemGreen
        activityIndicator.stopAnimating()
        leftStackLeading?.update(inset: 50)
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        activityIndicator.stopAnimating()
    }
}

// MARK: - Selection Action Panel

private final class SelectionActionPanel: UIView {
    var onExecuteTapped: (() -> Void)?
    private let separator = UIView()
    private(set) var backupCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.right", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "备份"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Cyan._600, dark: .Material.Cyan._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private(set) var downloadCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.left", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "下载"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Orange._600, dark: .Material.Orange._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private(set) var syncCategoryButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 15, weight: .bold)
        var cfg = UIButton.Configuration.plain()
        cfg.image = UIImage(systemName: "arrow.left.arrow.right", withConfiguration: iconConfig)
        cfg.imagePadding = 6
        cfg.titleAlignment = .leading
        cfg.subtitle = "同步"
        cfg.subtitleTextAttributesTransformer = .init { var a = $0; a.font = .preferredFont(forTextStyle: .caption1); return a }
        cfg.baseForegroundColor = .materialPrimary(light: .Material.Purple._600, dark: .Material.Purple._200)
        let btn = UIButton(configuration: cfg)
        btn.showsMenuAsPrimaryAction = true
        return btn
    }()
    private let executeButton: UIButton = {
        var cfg = UIButton.Configuration.filled()
        cfg.title = "执行"
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        cfg.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        cfg.contentInsets = .init(top: 8, leading: 20, bottom: 8, trailing: 20)
        return UIButton(configuration: cfg)
    }()

    override init(frame: CGRect) {
        super.init(frame: frame)
        setupUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    private func setupUI() {
        backgroundColor = .appPaper

        separator.backgroundColor = .separator
        addSubview(separator)
        separator.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalTo(0.5)
        }

        executeButton.addTarget(self, action: #selector(executeTapped), for: .touchUpInside)

        let spacer = UIView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)

        let contentStack = UIStackView(arrangedSubviews: [backupCategoryButton, downloadCategoryButton, syncCategoryButton, spacer, executeButton])
        contentStack.axis = .horizontal
        contentStack.spacing = 4
        contentStack.alignment = .center

        let inset: CGFloat = 12
        addSubview(contentStack)
        contentStack.snp.makeConstraints { make in
            make.top.equalToSuperview().inset(inset)
            make.leading.trailing.equalToSuperview().inset(inset)
            make.bottom.equalTo(safeAreaLayoutGuide).inset(inset)
        }
    }

    @objc private func executeTapped() { onExecuteTapped?() }

    func configure(backupCount: Int, downloadCount: Int, syncCount: Int) {
        backupCategoryButton.isHidden = backupCount == 0
        backupCategoryButton.configuration?.title = "\(backupCount)"
        downloadCategoryButton.isHidden = downloadCount == 0
        downloadCategoryButton.configuration?.title = "\(downloadCount)"
        syncCategoryButton.isHidden = syncCount == 0
        syncCategoryButton.configuration?.title = "\(syncCount)"
    }
}

// MARK: - Execution Action Panel

private final class ExecutionActionPanel: UIView {
    var onPauseTapped: (() -> Void)?
    var onStopTapped: (() -> Void)?
    var onResumeTapped: (() -> Void)?

    private let separator = UIView()
    private let statusLabel: UILabel = {
        let label = UILabel()
        label.font = .systemFont(ofSize: 14, weight: .medium)
        label.textColor = .label
        return label
    }()
    private let pauseResumeButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 16, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "pause.fill", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .systemOrange
        cfg.baseForegroundColor = .white
        cfg.contentInsets = .init(top: 8, leading: 16, bottom: 8, trailing: 16)
        return UIButton(configuration: cfg)
    }()
    private let stopButton: UIButton = {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 16, weight: .bold)
        var cfg = UIButton.Configuration.filled()
        cfg.image = UIImage(systemName: "stop.fill", withConfiguration: iconConfig)
        cfg.cornerStyle = .capsule
        cfg.baseBackgroundColor = .systemRed
        cfg.baseForegroundColor = .white
        cfg.contentInsets = .init(top: 8, leading: 16, bottom: 8, trailing: 16)
        return UIButton(configuration: cfg)
    }()

    private var isPaused = false

    override init(frame: CGRect) {
        super.init(frame: frame)
        setupUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    private func setupUI() {
        backgroundColor = .appPaper

        separator.backgroundColor = .separator
        addSubview(separator)
        separator.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalTo(0.5)
        }

        pauseResumeButton.addTarget(self, action: #selector(pauseResumeTapped), for: .touchUpInside)
        stopButton.addTarget(self, action: #selector(stopTappedAction), for: .touchUpInside)

        let spacer = UIView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)

        let contentStack = UIStackView(arrangedSubviews: [statusLabel, spacer, pauseResumeButton, stopButton])
        contentStack.axis = .horizontal
        contentStack.spacing = 8
        contentStack.alignment = .center

        let inset: CGFloat = 12
        addSubview(contentStack)
        contentStack.snp.makeConstraints { make in
            make.top.equalToSuperview().inset(inset)
            make.leading.trailing.equalToSuperview().inset(inset)
            make.bottom.equalTo(safeAreaLayoutGuide).inset(inset)
        }
    }

    @objc private func pauseResumeTapped() {
        if isPaused {
            onResumeTapped?()
        } else {
            onPauseTapped?()
        }
    }

    @objc private func stopTappedAction() {
        onStopTapped?()
    }

    func update(state: BackupSessionController.State, statusText: String, completedCount: Int, totalCount: Int) {
        let iconConfig = UIImage.SymbolConfiguration(pointSize: 16, weight: .bold)

        switch state {
        case .running:
            isPaused = false
            statusLabel.text = statusText
            pauseResumeButton.configuration?.image = UIImage(systemName: "pause.fill", withConfiguration: iconConfig)
            pauseResumeButton.configuration?.baseBackgroundColor = .systemOrange
            pauseResumeButton.isEnabled = true
            stopButton.isEnabled = true
        case .paused:
            isPaused = true
            statusLabel.text = "已暂停 \(completedCount)/\(totalCount) 月份"
            pauseResumeButton.configuration?.image = UIImage(systemName: "play.fill", withConfiguration: iconConfig)
            pauseResumeButton.configuration?.baseBackgroundColor = .systemGreen
            pauseResumeButton.isEnabled = true
            stopButton.isEnabled = true
        case .completed:
            isPaused = false
            statusLabel.text = "执行完成"
            pauseResumeButton.isEnabled = false
            stopButton.isEnabled = false
        case .failed:
            isPaused = false
            statusLabel.text = "执行失败"
            pauseResumeButton.isEnabled = false
            stopButton.isEnabled = true
        case .stopped:
            isPaused = false
            statusLabel.text = "已停止"
            pauseResumeButton.isEnabled = false
            stopButton.isEnabled = false
        case .idle:
            isPaused = false
            statusLabel.text = "准备中..."
            pauseResumeButton.isEnabled = false
            stopButton.isEnabled = false
        }
    }
}
