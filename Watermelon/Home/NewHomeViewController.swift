import SnapKit
import UIKit

final class NewHomeViewController: UIViewController {

    private struct MonthSummary {
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

    private struct YearSection {
        let year: Int
        let months: [MonthSummary]

        var title: String {
            "\(year)年"
        }

        var totalAssetCount: Int {
            months.reduce(0) { $0 + $1.assetCount }
        }

        var totalSizeBytes: Int64? {
            let sizes = months.compactMap(\.totalSizeBytes)
            guard sizes.count == months.count else { return nil }
            return sizes.reduce(0, +)
        }

        var backedUpPercent: Double? {
            let totalBacked = months.compactMap(\.backedUpCount).reduce(0, +)
            let total = totalAssetCount
            guard total > 0, months.allSatisfy({ $0.backedUpCount != nil }) else { return nil }
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

    private enum Section: Hashable {
        case year(Int)

        var header: String? {
            switch self {
            case .year(let y): return "\(y)年"
            }
        }
    }

    private enum Item: Hashable {
        case month(LibraryMonthKey, assetCount: Int, backedUpCount: Int?, totalSizeBytes: Int64?)
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<Section, Item>

    private let leftCollectionView: UICollectionView = {
        let cv = UICollectionView(frame: .zero, collectionViewLayout: createLayout())
        cv.backgroundColor = .clear
        cv.automaticallyAdjustsScrollIndicatorInsets = false
        cv.verticalScrollIndicatorInsets = .zero
        return cv
    }()

    private let rightCollectionView: UICollectionView = {
        let cv = UICollectionView(frame: .zero, collectionViewLayout: createLayout())
        cv.backgroundColor = .clear
        cv.automaticallyAdjustsScrollIndicatorInsets = false
        cv.verticalScrollIndicatorInsets = .zero
        return cv
    }()

    private var leftDataSource: DataSource!
    private var rightDataSource: DataSource!
    private var leftYearSections: [YearSection] = []
    private var rightYearSections: [YearSection] = []
    private let leftHeaderLabel = UILabel()
    private let rightHeaderButton = UIButton(type: .system)
    private let backupButton = UIButton(type: .system)
    private let emptyRemoteLabel = UILabel()

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
        configureDataSources()
        bindSession()
        bindDataManager()

        loadSavedProfiles()
        scheduleReloadAllData()
    }

    // MARK: - Layout

    private static func createLayout() -> UICollectionViewCompositionalLayout {
        UICollectionViewCompositionalLayout { _, _ in
            let itemSize = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1), heightDimension: .absolute(72))
            let item = NSCollectionLayoutItem(layoutSize: itemSize)
            let group = NSCollectionLayoutGroup.vertical(layoutSize: itemSize, subitems: [item])
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
        // Header backgrounds that extend into the status bar area
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

        // Left header label — positioned inside the safe-area portion
        leftHeaderLabel.text = "本地相册"
        leftHeaderLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        leftHeaderLabel.textColor = UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200 : .Material.Green._900 }
        leftHeaderLabel.textAlignment = .center
        leftHeaderBg.addSubview(leftHeaderLabel)
        leftHeaderLabel.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalTo(Self.headerAreaHeight)
        }

        // Right header button — same positioning
        configureRightHeaderButton()
        rightHeaderBg.addSubview(rightHeaderButton)
        rightHeaderButton.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalTo(Self.headerAreaHeight)
        }

        // Collection views
        leftCollectionView.delegate = self
        rightCollectionView.delegate = self

        view.addSubview(leftCollectionView)
        view.addSubview(rightCollectionView)

        leftCollectionView.snp.makeConstraints { make in
            make.top.equalTo(leftHeaderBg.snp.bottom)
            make.leading.equalToSuperview()
            make.trailing.equalTo(view.snp.centerX).offset(-1)
            make.bottom.equalToSuperview()
        }
        rightCollectionView.snp.makeConstraints { make in
            make.top.equalTo(rightHeaderBg.snp.bottom)
            make.leading.equalTo(view.snp.centerX).offset(1)
            make.trailing.equalToSuperview()
            make.bottom.equalToSuperview()
        }

        // Empty state label for remote
        emptyRemoteLabel.text = "未连接远端存储"
        emptyRemoteLabel.font = .systemFont(ofSize: 14)
        emptyRemoteLabel.textColor = .tertiaryLabel
        emptyRemoteLabel.textAlignment = .center
        emptyRemoteLabel.numberOfLines = 0
        view.addSubview(emptyRemoteLabel)
        emptyRemoteLabel.snp.makeConstraints { make in
            make.centerY.equalToSuperview()
            make.leading.equalTo(view.snp.centerX).offset(16)
            make.trailing.equalToSuperview().inset(16)
        }

        // Floating backup button
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
            make.centerX.equalTo(leftCollectionView)
            make.bottom.equalTo(view.safeAreaLayoutGuide).offset(-16)
            make.height.equalTo(44)
        }
        backupButton.layer.shadowColor = UIColor.black.cgColor
        backupButton.layer.shadowOpacity = 0.15
        backupButton.layer.shadowOffset = CGSize(width: 0, height: 2)
        backupButton.layer.shadowRadius = 6
    }

    // MARK: - Data Sources

    private func configureDataSources() {
        let cellRegistration = UICollectionView.CellRegistration<MonthSummaryCell, Item> { cell, _, item in
            guard case .month(let month, let assetCount, let backedUpCount, let totalSizeBytes) = item else { return }
            let summary = MonthSummary(month: month, assetCount: assetCount, backedUpCount: backedUpCount, totalSizeBytes: totalSizeBytes)
            let m = month.month
            cell.configure(
                monthTitle: summary.monthTitle,
                countText: summary.countText,
                sizeText: summary.sizeText,
                backedUpText: summary.backedUpText,
                bgColor: Self.monthColor(month: m),
                titleColor: Self.monthTextColor(month: m),
                detailColor: Self.monthSecondaryTextColor(month: m)
            )
        }

        leftDataSource = DataSource(collectionView: leftCollectionView) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: item)
        }
        rightDataSource = DataSource(collectionView: rightCollectionView) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: item)
        }

        let headerRegistration = UICollectionView.SupplementaryRegistration<SectionHeaderView>(
            elementKind: UICollectionView.elementKindSectionHeader
        ) { [weak self] supplementaryView, _, indexPath in
            guard let self else { return }
            let isLeft = supplementaryView.isDescendant(of: self.leftCollectionView)
            let yearSections = isLeft ? self.leftYearSections : self.rightYearSections
            guard indexPath.section < yearSections.count else { return }
            let section = yearSections[indexPath.section]
            supplementaryView.configure(
                title: section.title,
                countText: "\(section.totalAssetCount) 张",
                sizeText: section.totalSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
                backedUpText: section.backedUpPercent.map { "\(Int($0 * 100))%" }
            )
        }

        let supplementaryProvider: DataSource.SupplementaryViewProvider = { collectionView, kind, indexPath in
            collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
        leftDataSource.supplementaryViewProvider = supplementaryProvider
        rightDataSource.supplementaryViewProvider = supplementaryProvider
    }

    private func configureRightHeaderButton() {
        var config = UIButton.Configuration.plain()
        config.title = "远端存储"
        config.image = UIImage(systemName: "chevron.down")
        config.imagePlacement = .trailing
        config.imagePadding = 4
        config.baseForegroundColor = UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200 : .Material.Green._900 }
        config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var outgoing = incoming
            outgoing.font = UIFont.systemFont(ofSize: 15, weight: .semibold)
            return outgoing
        }
        rightHeaderButton.configuration = config
        rightHeaderButton.showsMenuAsPrimaryAction = true
        rightHeaderButton.menu = buildDestinationMenu()
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
        var config = rightHeaderButton.configuration ?? .plain()
        if isConnecting {
            config.title = "连接中..."
        } else if let profile = dependencies.appSession.activeProfile {
            config.title = profile.storageProfile.indicatorText
        } else {
            config.title = "远端存储"
        }
        rightHeaderButton.configuration = config
        rightHeaderButton.menu = buildDestinationMenu()
    }

    // MARK: - Bindings

    private func bindSession() {
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            self.loadSavedProfiles()
            self.updateRightHeaderButton()
            self.syncRemoteDataIfNeeded()
            self.reloadAllSummaries()
        }
    }

    private func bindDataManager() {
        homeDataManager.onDataChanged = { [weak self] in
            self?.reloadAllSummaries()
        }
        homeDataManager.onFileSizesUpdated = { [weak self] in
            self?.reloadLocalSummaries()
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
            self.reloadAllSummaries()
        }
    }

    private func reloadAllSummaries() {
        reloadLocalSummaries()
        reloadRemoteSummaries()
    }

    private func reloadLocalSummaries() {
        let flat = homeDataManager.localMonthSummaries().map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
        }
        leftYearSections = Self.groupByYear(flat)
        leftDataSource.apply(Self.buildSnapshot(from: leftYearSections), animatingDifferences: false)
        reconfigureHeaders(leftCollectionView, yearSections: leftYearSections)
    }

    private func reloadRemoteSummaries(overrideActiveConnection: Bool? = nil) {
        guard overrideActiveConnection ?? hasActiveConnection else {
            rightYearSections = []
            rightDataSource.apply(NSDiffableDataSourceSnapshot<Section, Item>(), animatingDifferences: false)
            emptyRemoteLabel.isHidden = false
            return
        }

        let summaries = dependencies.backupCoordinator.remoteMonthSummaries()
        let flat = summaries.map {
            MonthSummary(month: $0.month, assetCount: $0.assetCount, backedUpCount: nil, totalSizeBytes: $0.totalSizeBytes)
        }

        rightYearSections = Self.groupByYear(flat)
        emptyRemoteLabel.isHidden = true
        rightDataSource.apply(Self.buildSnapshot(from: rightYearSections), animatingDifferences: false)
        reconfigureHeaders(rightCollectionView, yearSections: rightYearSections)
    }

    private func reconfigureHeaders(_ collectionView: UICollectionView, yearSections: [YearSection]) {
        for section in 0 ..< collectionView.numberOfSections {
            guard section < yearSections.count else { continue }
            let indexPath = IndexPath(item: 0, section: section)
            guard let header = collectionView.supplementaryView(
                forElementKind: UICollectionView.elementKindSectionHeader,
                at: indexPath
            ) as? SectionHeaderView else { continue }
            let ys = yearSections[section]
            header.configure(
                title: ys.title,
                countText: "\(ys.totalAssetCount) 张",
                sizeText: ys.totalSizeBytes.map { ByteCountFormatter.string(fromByteCount: $0, countStyle: .file) },
                backedUpText: ys.backedUpPercent.map { "\(Int($0 * 100))%" }
            )
        }
    }

    private static func buildSnapshot(from yearSections: [YearSection]) -> NSDiffableDataSourceSnapshot<Section, Item> {
        var snapshot = NSDiffableDataSourceSnapshot<Section, Item>()
        for section in yearSections {
            snapshot.appendSections([.year(section.year)])
            snapshot.appendItems(section.months.map {
                .month($0.month, assetCount: $0.assetCount, backedUpCount: $0.backedUpCount, totalSizeBytes: $0.totalSizeBytes)
            })
        }
        return snapshot
    }

    private static func groupByYear(_ summaries: [MonthSummary]) -> [YearSection] {
        Dictionary(grouping: summaries) { $0.month.year }
            .map { year, months in
                YearSection(year: year, months: months.sorted { $0.month > $1.month })
            }
            .sorted { $0.year > $1.year }
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
                            self?.reloadRemoteSummaries(overrideActiveConnection: true)
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
                    self.reloadRemoteSummaries()
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
    // Material Design 50/900 weight — barely tinted backgrounds, high-contrast text.
    private struct SeasonStyle {
        let bg: UIColor
        let title: UIColor
        let detail: UIColor
    }

    // Dark: #121212 surface + 8% tint overlay; text uses 200 (title) / 400 (detail)
    // Light: 50 bg; text uses 900 (title) / 700 (detail)
    private static let seasonStyles: [SeasonStyle] = [
        // Q1 春 — Green
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Green._200)  : .Material.Green._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._200  : .Material.Green._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Green._400  : .Material.Green._700 }
        ),
        // Q2 夏 — Blue
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Blue._200)  : .Material.Blue._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Blue._200  : .Material.Blue._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Blue._400  : .Material.Blue._700 }
        ),
        // Q3 秋 — Amber
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Amber._200) : .Material.Amber._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Amber._200 : .Material.Amber._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Amber._400 : .Material.Amber._700 }
        ),
        // Q4 冬 — Red
        SeasonStyle(
            bg:     UIColor { $0.userInterfaceStyle == .dark ? .materialDarkSurface(tint: .Material.Red._200)  : .Material.Red._50 },
            title:  UIColor { $0.userInterfaceStyle == .dark ? .Material.Red._200  : .Material.Red._900 },
            detail: UIColor { $0.userInterfaceStyle == .dark ? .Material.Red._400  : .Material.Red._700 }
        ),
    ]

    private static func seasonIndex(for month: Int) -> Int {
        switch month {
        case 1...3:  return 0
        case 4...6:  return 1
        case 7...9:  return 2
        case 10...12: return 3
        default:      return 0
        }
    }

    private static func monthColor(month: Int) -> UIColor {
        seasonStyles[seasonIndex(for: month)].bg
    }

    private static func monthTextColor(month: Int) -> UIColor {
        seasonStyles[seasonIndex(for: month)].title
    }

    private static func monthSecondaryTextColor(month: Int) -> UIColor {
        seasonStyles[seasonIndex(for: month)].detail
    }
}

// MARK: - UICollectionViewDelegate

extension NewHomeViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
    }
}

// MARK: - Section Header View

private final class SectionHeaderView: UICollectionReusableView {
    private let titleLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private let backedUpLabel = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)
//        backgroundColor = .systemBackground

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

        addSubview(titleLabel)
        addSubview(countLabel)
        addSubview(sizeLabel)
        addSubview(backedUpLabel)

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
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(title: String, countText: String, sizeText: String?, backedUpText: String?) {
        titleLabel.text = title
        countLabel.text = countText
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        backedUpLabel.text = backedUpText
        backedUpLabel.isHidden = backedUpText == nil
    }
}

// MARK: - Month Summary Cell

private final class MonthSummaryCell: UICollectionViewCell {
    private let colorView = UIView()
    private let monthLabel = UILabel()
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()
    private let backedUpLabel = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)
        backgroundColor = .clear
        contentView.backgroundColor = .clear

        contentView.addSubview(colorView)
        colorView.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.bottom.equalToSuperview().inset(1)
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
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }
        rightStack.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
            make.leading.greaterThanOrEqualTo(leftStack.snp.trailing).offset(8)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(monthTitle: String, countText: String, sizeText: String?, backedUpText: String?,
                   bgColor: UIColor, titleColor: UIColor, detailColor: UIColor) {
        monthLabel.text = monthTitle
        countLabel.text = countText
        sizeLabel.text = sizeText
        sizeLabel.isHidden = sizeText == nil
        backedUpLabel.text = backedUpText
        backedUpLabel.isHidden = backedUpText == nil
        colorView.backgroundColor = bgColor
        monthLabel.textColor = titleColor
        countLabel.textColor = detailColor
        sizeLabel.textColor = detailColor
        backedUpLabel.textColor = detailColor
    }
}
