import MarqueeLabel
import SnapKit
import UIKit

final class NewHomeViewController: UIViewController {

    fileprivate struct MonthSummary {
        let month: LibraryMonthKey
        let assetCount: Int
        let backedUpCount: Int?
        let totalSizeBytes: Int64?

        var monthTitle: String {
            String(format: "%02d月", month.month)
        }

        var countText: String {
            "\(assetCount) 张"
        }

        var sizeText: String? {
            guard let bytes = totalSizeBytes else { return nil }
            return ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
        }

        var backedUpPercent: Double? {
            guard let backed = backedUpCount, assetCount > 0 else { return nil }
            return Double(backed) / Double(assetCount)
        }

        var backedUpText: String? {
            guard let pct = backedUpPercent else { return nil }
            return "\(Int(pct * 100))%"
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

        var localAssetCount: Int { rows.compactMap(\.local).reduce(0) { $0 + $1.assetCount } }
        var remoteAssetCount: Int { rows.compactMap(\.remote).reduce(0) { $0 + $1.assetCount } }

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

        var backedUpPercent: Double? {
            let locals = rows.compactMap(\.local)
            let totalBacked = locals.compactMap(\.backedUpCount).reduce(0, +)
            let total = locals.reduce(0) { $0 + $1.assetCount }
            guard total > 0, locals.allSatisfy({ $0.backedUpCount != nil }) else { return nil }
            return Double(totalBacked) / Double(total)
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

    private enum Section: Hashable {
        case year(Int)
    }

    private enum Item: Hashable {
        case local(LibraryMonthKey)
        case remote(LibraryMonthKey)
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
    private let backupButton = UIButton(type: .system)

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
            let itemSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(0.5), heightDimension: .absolute(72))
            let item = NSCollectionLayoutItem(layoutSize: itemSize)
            let groupSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1), heightDimension: .absolute(72))
            let group = NSCollectionLayoutGroup.horizontal(layoutSize: groupSize, subitems: [item, item])
            group.interItemSpacing = .fixed(2)
            let section = NSCollectionLayoutSection(group: group)

            let headerSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1), heightDimension: .absolute(50))
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
        leftHeaderBg.backgroundColor = UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Green._200, alpha: 0.16) : .Material.Green._100 }
        let rightHeaderBg = UIView()
        rightHeaderBg.backgroundColor = UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Green._200, alpha: 0.16) : .Material.Green._100 }

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

        let headerTextColor = UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200 : .Material.Green._900 }

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

        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderBg.snp.bottom)
            make.leading.trailing.bottom.equalToSuperview()
        }

        var btnConfig = UIButton.Configuration.filled()
        btnConfig.title = "一键备份"
        btnConfig.image = UIImage(systemName: "arrow.up.circle.fill")
        btnConfig.imagePadding = 6
        btnConfig.cornerStyle = .capsule
        btnConfig.baseBackgroundColor = .systemBlue
        btnConfig.baseForegroundColor = .white
        backupButton.configuration = btnConfig
        backupButton.addTarget(self, action: #selector(backupTapped), for: .touchUpInside)
        backupButton.isHidden = true

        view.addSubview(backupButton)
        backupButton.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.bottom.equalTo(view.safeAreaLayoutGuide).offset(-16)
            make.height.equalTo(44)
        }
        backupButton.layer.shadowColor = UIColor.black.cgColor
        backupButton.layer.shadowOpacity = 0.15
        backupButton.layer.shadowOffset = CGSize(width: 0, height: 2)
        backupButton.layer.shadowRadius = 6
    }

    // MARK: - Data Source

    private func configureDataSource() {
        let cellRegistration = UICollectionView.CellRegistration<MonthCell, Item> { [weak self] cell, _, item in
            guard let self else { return }
            let month: LibraryMonthKey
            let summary: MonthSummary
            let isSelected: Bool?

            switch item {
            case .local(let key):
                month = key
                summary = self.rowLookup[key]?.local
                    ?? MonthSummary(month: key, assetCount: 0, backedUpCount: nil, totalSizeBytes: 0)
                isSelected = self.selectedLocalMonths.contains(key)
            case .remote(let key):
                month = key
                summary = self.rowLookup[key]?.remote
                    ?? MonthSummary(month: key, assetCount: 0, backedUpCount: nil, totalSizeBytes: 0)
                isSelected = self.selectedRemoteMonths.contains(key)
            }

            let m = month.month
            cell.configure(
                monthTitle: summary.monthTitle, countText: summary.countText,
                sizeText: summary.sizeText, backedUpText: summary.backedUpText,
                bgColor: Self.monthColor(month: m),
                titleColor: Self.monthTextColor(month: m),
                detailColor: Self.monthSecondaryTextColor(month: m),
                isSelected: isSelected
            )
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
            let leftAllSelected = !allMonths.isEmpty && allMonths.isSubset(of: self.selectedLocalMonths)
            let rightAllSelected = !allMonths.isEmpty && allMonths.isSubset(of: self.selectedRemoteMonths)
            let accentColor = self.leftHeaderLabel.textColor ?? .secondaryLabel
            supplementaryView.configure(section: section,
                                        leftSelected: leftAllSelected, rightSelected: rightAllSelected,
                                        selectedColor: accentColor, deselectedColor: UIColor.tertiaryLabel)
            supplementaryView.onLeftTap = { [weak self] in
                self?.toggleYearSelection(section: indexPath.section, side: .local)
            }
            supplementaryView.onRightTap = { [weak self] in
                self?.toggleYearSelection(section: indexPath.section, side: .remote)
            }
        }

        dataSource.supplementaryViewProvider = { collectionView, kind, indexPath in
            collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
    }

    private func configureRightHeaderButton() {
        let headerTextColor = UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200 : .Material.Green._900 }
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
            MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
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
            MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
        }
        applyMergedSnapshot()
    }

    private func rebuildAndApply() {
        localSummaries = homeDataManager.localMonthSummaries().map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
        }
        if hasActiveConnection {
            let summaries = dependencies.backupCoordinator.remoteMonthSummaries()
            remoteSummaries = summaries.map {
                MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
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
                items.append(.local(row.month))
                items.append(.remote(row.month))
            }
            snapshot.appendItems(items)
        }
        dataSource.applySnapshotUsingReloadData(snapshot)
        reconfigureVisibleHeaders()
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
            let leftAll = !allMonths.isEmpty && allMonths.isSubset(of: selectedLocalMonths)
            let rightAll = !allMonths.isEmpty && allMonths.isSubset(of: selectedRemoteMonths)
            header.configure(section: ms, leftSelected: leftAll, rightSelected: rightAll,
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
        for cell in collectionView.visibleCells {
            guard let monthCell = cell as? MonthCell,
                  let indexPath = collectionView.indexPath(for: cell),
                  let item = dataSource.itemIdentifier(for: indexPath) else { continue }
            switch item {
            case .local(let month):
                monthCell.setSelected(selectedLocalMonths.contains(month))
            case .remote(let month):
                monthCell.setSelected(selectedRemoteMonths.contains(month))
            }
        }
        reconfigureVisibleHeaders()
        updateTopHeaderToggles()
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
        let leftAll = !allMonths.isEmpty && allMonths.isSubset(of: selectedLocalMonths)
        leftToggle.setImage(UIImage(systemName: leftAll ? "checkmark.circle.fill" : "circle", withConfiguration: config), for: .normal)
        leftToggle.tintColor = headerColor
        let rightAll = !allMonths.isEmpty && allMonths.isSubset(of: selectedRemoteMonths)
        rightToggle.setImage(UIImage(systemName: rightAll ? "checkmark.circle.fill" : "circle", withConfiguration: config), for: .normal)
        rightToggle.tintColor = headerColor
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
        let backupVC = BackupViewController(
            sessionController: backupSessionController,
            dependencies: dependencies
        )
        let nav = UINavigationController(rootViewController: backupVC)
        nav.modalPresentationStyle = .pageSheet
        present(nav, animated: true)
    }

    // MARK: - Season Colors

    private struct SeasonStyle {
        let bg: UIColor
        let title: UIColor
        let detail: UIColor
    }

    private static let seasonStyles: [SeasonStyle] = [
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Green._200)  : .Material.Green._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200  : .Material.Green._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._400  : .Material.Green._700 }
        ),
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Blue._200)  : .Material.Blue._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Blue._200  : .Material.Blue._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Blue._400  : .Material.Blue._700 }
        ),
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Amber._200) : .Material.Amber._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Amber._200 : .Material.Amber._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Amber._400 : .Material.Amber._700 }
        ),
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Red._200)  : .Material.Red._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Red._200  : .Material.Red._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Red._400  : .Material.Red._700 }
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
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        switch item {
        case .local(let month):
            toggleMonthSelection(month, side: .local)
        case .remote(let month):
            toggleMonthSelection(month, side: .remote)
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
                   leftSelected: Bool, rightSelected: Bool,
                   selectedColor: UIColor, deselectedColor: UIColor) {
        leftHalf.configure(
            title: section.title,
            countText: "\(section.localAssetCount) 张",
            sizeText: section.localSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            backedUpText: section.backedUpPercent.map { "\(Int($0 * 100))%" },
            isSelected: leftSelected,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
        rightHalf.configure(
            title: section.remoteAssetCount > 0 ? section.title : nil,
            countText: section.remoteAssetCount > 0 ? "\(section.remoteAssetCount) 张" : nil,
            sizeText: section.remoteSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
            backedUpText: nil,
            isSelected: rightSelected,
            selectedColor: selectedColor,
            deselectedColor: deselectedColor
        )
    }
}

// MARK: - Half Header View (reused for left/right)

private final class HalfHeaderView: UIView {
    private let checkmark = UIImageView()
    private let titleLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private let backedUpLabel = UILabel()

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
        sizeLabel.textAlignment = .right

        backedUpLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        backedUpLabel.textColor = .tertiaryLabel
        backedUpLabel.textAlignment = .right

        addSubview(checkmark)
        addSubview(titleLabel)
        addSubview(countLabel)
        addSubview(sizeLabel)
        addSubview(backedUpLabel)

        checkmark.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(12)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }
        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.bottom.equalTo(snp.centerY).offset(-1)
        }
        countLabel.snp.makeConstraints { make in
            make.top.equalTo(snp.centerY).offset(1)
            make.leading.equalToSuperview().inset(16)
        }
        sizeLabel.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalTo(titleLabel)
        }
        backedUpLabel.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalTo(countLabel)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(title: String?, countText: String?, sizeText: String?, backedUpText: String?,
                   isSelected: Bool?, selectedColor: UIColor?, deselectedColor: UIColor?) {
        titleLabel.text = title
        countLabel.text = countText
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        backedUpLabel.text = backedUpText
        backedUpLabel.isHidden = backedUpText == nil

        if let selected = isSelected {
            checkmark.isHidden = false
            checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
            checkmark.tintColor = selected ? (selectedColor ?? .secondaryLabel) : (deselectedColor ?? .tertiaryLabel)
            titleLabel.snp.updateConstraints { make in
                make.leading.equalToSuperview().inset(42)
            }
            countLabel.snp.updateConstraints { make in
                make.leading.equalToSuperview().inset(42)
            }
        } else {
            checkmark.isHidden = true
            titleLabel.snp.updateConstraints { make in
                make.leading.equalToSuperview().inset(16)
            }
            countLabel.snp.updateConstraints { make in
                make.leading.equalToSuperview().inset(16)
            }
        }
    }
}

// MARK: - Month Cell

private final class MonthCell: UICollectionViewCell {
    private let colorView = UIView()
    private let checkmark = UIImageView()
    private let monthLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private let backedUpLabel = UILabel()
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
            make.leading.equalToSuperview().inset(12)
            make.centerY.equalToSuperview()
            make.size.equalTo(18)
        }

        monthLabel.font = .systemFont(ofSize: 15, weight: .medium)
        countLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        sizeLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        sizeLabel.textAlignment = .right
        backedUpLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .regular)
        backedUpLabel.textAlignment = .right

        let leftStack = UIStackView(arrangedSubviews: [monthLabel, countLabel])
        leftStack.axis = .vertical
        leftStack.spacing = 2
        leftStack.alignment = .leading

        let rightStack = UIStackView(arrangedSubviews: [sizeLabel, backedUpLabel])
        rightStack.axis = .vertical
        rightStack.spacing = 2
        rightStack.alignment = .trailing

        colorView.addSubview(leftStack)
        colorView.addSubview(rightStack)
        leftStack.snp.makeConstraints { make in
            self.leftStackLeading = make.leading.equalToSuperview().inset(16).constraint
            make.centerY.equalToSuperview()
        }
        rightStack.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
            make.leading.greaterThanOrEqualTo(leftStack.snp.trailing).offset(8)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError() }

    func configure(monthTitle: String, countText: String, sizeText: String?, backedUpText: String?,
                   bgColor: UIColor, titleColor: UIColor, detailColor: UIColor, isSelected: Bool?) {
        monthLabel.text = monthTitle
        monthLabel.isHidden = false
        countLabel.text = countText
        countLabel.isHidden = false
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        backedUpLabel.text = backedUpText
        backedUpLabel.isHidden = backedUpText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        backedUpLabel.textColor = detailColor
        currentTitleColor = titleColor
        currentDetailColor = detailColor

        if let selected = isSelected {
            checkmark.isHidden = false
            checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
            checkmark.tintColor = selected ? titleColor : detailColor
            leftStackLeading?.update(inset: 42)
        } else {
            checkmark.isHidden = true
            leftStackLeading?.update(inset: 16)
        }
    }

    func configureEmpty(bgColor: UIColor) {
        monthLabel.text = nil
        monthLabel.isHidden = true
        countLabel.text = nil
        countLabel.isHidden = true
        sizeLabel.text = nil
        sizeLabel.isHidden = true
        backedUpLabel.text = nil
        backedUpLabel.isHidden = true
        checkmark.isHidden = true
        colorView.backgroundColor = bgColor
        leftStackLeading?.update(inset: 16)
    }

    func setSelected(_ selected: Bool) {
        checkmark.isHidden = false
        checkmark.image = UIImage(systemName: selected ? "checkmark.circle.fill" : "circle")
        checkmark.tintColor = selected ? currentTitleColor : currentDetailColor
        leftStackLeading?.update(inset: 42)
    }
}
