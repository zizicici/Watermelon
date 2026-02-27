import GRDB
import Kingfisher
import Photos
import SnapKit
import UIKit

final class HomeViewController: UIViewController {
    private enum GridLayout {
        static let columns: CGFloat = 4
        static let itemSpacing: CGFloat = 4
        static let sectionTopInset: CGFloat = 8
        static let sectionBottomInset: CGFloat = 28
        static let sectionHeaderInset: CGFloat = itemSpacing + 6
        static let sectionHeaderHeight: CGFloat = 48
    }

    private enum SourceFilterMode {
        case all
        case localOnly
        case remoteOnly
        case both
    }

    private enum SortMode {
        case descending
        case ascending
    }

    private enum DisplayOption {
        case square
        case originalRatio
    }

    private enum SectionReloadMode {
        case full
        case nonAnimatedDiff
    }

    private enum MonthDebugExportScope {
        case all
        case unmatchedOnly

        var scopeText: String {
            switch self {
            case .all:
                return "all_local_and_remote"
            case .unmatchedOnly:
                return "only_local_and_only_remote"
            }
        }

        var triggerText: String {
            switch self {
            case .all:
                return "section_header_tap"
            case .unmatchedOnly:
                return "section_header_long_press"
            }
        }

        var fileTag: String {
            switch self {
            case .all:
                return "all"
            case .unmatchedOnly:
                return "unmatched"
            }
        }
    }

    private struct AlbumSection {
        let key: YearMonth
        let items: [HomeAlbumItem]

        var title: String {
            "\(key.year)年\(String(format: "%02d", key.month))月"
        }
    }

    private struct YearMonth: Hashable, Comparable {
        let year: Int
        let month: Int

        static func < (lhs: YearMonth, rhs: YearMonth) -> Bool {
            if lhs.year == rhs.year {
                return lhs.month < rhs.month
            }
            return lhs.year < rhs.year
        }
    }

    private let dependencies: DependencyContainer
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let calendar = Calendar(identifier: .gregorian)
    private let discoveryService = SMBDiscoveryService()

    private let collectionView: UICollectionView
    private lazy var connectionBarButtonItem = UIBarButtonItem(
        title: "加载中……",
        style: .prominent,
        target: nil,
        action: nil
    )

    private lazy var filterToolbarItem = UIBarButtonItem(
        title: "筛选",
        style: .plain,
        target: nil,
        action: nil
    )
    private lazy var backupToolbarItem = UIBarButtonItem(
        title: "备份",
        style: .prominent,
        target: self,
        action: #selector(openBackupStatusTapped)
    )

    private var sourceFilterMode: SourceFilterMode = .all
    private var sortMode: SortMode = .descending
    private var displayOption: DisplayOption = .square

    private var savedProfiles: [ServerProfileRecord] = []
    private var activeProfileID: Int64?
    private var discoveredServers: [DiscoveredSMBServer] = []
    private var didAttemptAutoConnect = false
    private var isConnecting = false

    private var localItems: [LocalAlbumItem] = []
    private var remoteItems: [RemoteAlbumItem] = []
    private var mergedItems: [HomeAlbumItem] = []
    private var sections: [AlbumSection] = []
    private var localAssetsByID: [String: PHAsset] = [:]
    private var localAssetIdentifierByHash: [Data: [String]] = [:]

    private let remoteImageCache = ImageCache(name: "home_remote_album_cache")
    private let remoteThumbnailService = RemoteThumbnailService()
    private var prefetchTasks: [IndexPath: DownloadTask] = [:]
    private var reloadTask: Task<Void, Never>?
    private var pendingRemoteSectionRefresh = false
    private var lastRunningSectionRefreshAt: Date = .distantPast

    private lazy var backupSessionController = BackupSessionController(dependencies: dependencies)
    private var backupSessionObserverID: UUID?
    private var lastObservedBackupState: BackupSessionController.State = .idle

    private var hasActiveConnection: Bool {
        dependencies.appSession.activeProfile != nil && dependencies.appSession.activePassword != nil
    }

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        self.contentHashIndexRepository = ContentHashIndexRepository(databaseManager: dependencies.databaseManager)

        let layout = UICollectionViewFlowLayout()
        layout.minimumInteritemSpacing = GridLayout.itemSpacing
        layout.minimumLineSpacing = GridLayout.itemSpacing
        layout.headerReferenceSize = CGSize(width: 100, height: GridLayout.sectionHeaderHeight)
        self.collectionView = UICollectionView(frame: .zero, collectionViewLayout: layout)

        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        reloadTask?.cancel()
        discoveryService.stop()
        for task in prefetchTasks.values {
            task.cancel()
        }
        let thumbnailService = remoteThumbnailService
        Task {
            await thumbnailService.invalidate()
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        navigationItem.largeTitleDisplayMode = .never
        title = ""

        remoteImageCache.memoryStorage.config.countLimit = 350
        remoteImageCache.memoryStorage.config.totalCostLimit = 80 * 1024 * 1024
        remoteImageCache.memoryStorage.config.expiration = .seconds(600)
        remoteImageCache.diskStorage.config.sizeLimit = 400 * 1024 * 1024
        remoteImageCache.diskStorage.config.expiration = .days(7)

        configureUI()
        configureNavigationItems()
        configureToolbarItems()
        bindSession()
        bindDiscovery()
        bindBackupSession()

        loadSavedProfiles()
        updateConnectionIndicator()
        updateConnectionMenu()
        updateFilterMenu()
        scheduleReloadAllData()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        navigationController?.setToolbarHidden(false, animated: false)
        configureToolbarItems()
        discoveryService.start()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        attemptAutoConnectIfNeeded()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        guard let flow = collectionView.collectionViewLayout as? UICollectionViewFlowLayout else { return }
        
        flow.minimumInteritemSpacing = GridLayout.itemSpacing
        flow.minimumLineSpacing = GridLayout.itemSpacing
        flow.sectionInset = UIEdgeInsets(
            top: GridLayout.sectionTopInset,
            left: GridLayout.itemSpacing,
            bottom: GridLayout.sectionBottomInset,
            right: GridLayout.itemSpacing
        )
        flow.headerReferenceSize = CGSize(width: collectionView.bounds.width, height: GridLayout.sectionHeaderHeight)
    }

    private func configureUI() {
        if let flow = collectionView.collectionViewLayout as? UICollectionViewFlowLayout {
            flow.sectionHeadersPinToVisibleBounds = true
        }
        collectionView.backgroundColor = .clear
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.prefetchDataSource = self
        collectionView.allowsMultipleSelection = false
        collectionView.register(AlbumGridCell.self, forCellWithReuseIdentifier: AlbumGridCell.reuseID)
        collectionView.register(
            AlbumSectionHeaderView.self,
            forSupplementaryViewOfKind: UICollectionView.elementKindSectionHeader,
            withReuseIdentifier: AlbumSectionHeaderView.reuseID
        )

        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
    }

    private func configureNavigationItems() {
        navigationItem.titleView = nil
        navigationItem.rightBarButtonItem = connectionBarButtonItem
    }

    private func configureToolbarItems() {
        let spacer = UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil)
        backupToolbarItem.tintColor = .systemGreen
        filterToolbarItem.tintColor = .systemGreen
        toolbarItems = [filterToolbarItem, spacer, backupToolbarItem]
    }

    private func bindSession() {
        dependencies.appSession.onSessionChanged = { [weak self] _ in
            guard let self else { return }
            Task { @MainActor [weak self] in
                guard let self else { return }
                self.loadSavedProfiles()
                self.updateConnectionIndicator()
                self.updateConnectionMenu()
                self.updateFilterMenu()
                self.scheduleReloadAllData()
            }
        }
    }

    private func bindDiscovery() {
        discoveryService.onUpdate = { [weak self] discovered in
            DispatchQueue.main.async {
                self?.discoveredServers = discovered
                self?.updateConnectionMenu()
            }
        }
    }

    private func bindBackupSession() {
        if let existing = backupSessionObserverID {
            backupSessionController.removeObserver(existing)
        }
        backupSessionObserverID = backupSessionController.addObserver { [weak self] snapshot in
            guard let self else { return }
            self.backupToolbarItem.title = snapshot.state == .running ? "备份中" : "备份"
            self.updateFilterMenu()

            if snapshot.state == .running, self.hasActiveConnection {
                self.scheduleRemoteSectionRefresh()
            }

            let previous = self.lastObservedBackupState
            self.lastObservedBackupState = snapshot.state
            if previous == .running && snapshot.state != .running {
                self.reloadRemoteIndexAfterBackupEnded()
            }
        }
    }

    private func reloadRemoteIndexAfterBackupEnded() {
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            scheduleReloadAllData()
            return
        }

        Task { [weak self] in
            guard let self else { return }
            _ = try? await self.dependencies.backupExecutor.reloadRemoteIndex(
                profile: profile,
                password: password
            )
            await MainActor.run {
                self.scheduleReloadAllData()
            }
        }
    }

    private func scheduleRemoteSectionRefresh() {
        guard hasActiveConnection else { return }
        let now = Date()
        guard now.timeIntervalSince(lastRunningSectionRefreshAt) >= 2.4 else { return }
        guard !pendingRemoteSectionRefresh else { return }
        pendingRemoteSectionRefresh = true
        lastRunningSectionRefreshAt = now

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) { [weak self] in
            guard let self else { return }
            Task { [weak self] in
                guard let self else { return }
                await MainActor.run {
                    self.pendingRemoteSectionRefresh = false
                }
                guard self.hasActiveConnection else { return }
                await self.refreshLocalHashMirrorIndex()
                await MainActor.run {
                    self.loadRemoteItems()
                    self.rebuildMergedItems()
                    self.applySections(reloadMode: .nonAnimatedDiff)
                }
            }
        }
    }

    private func loadSavedProfiles() {
        savedProfiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        activeProfileID = try? dependencies.databaseManager.activeServerProfileID()
    }

    private func attemptAutoConnectIfNeeded() {
        guard !didAttemptAutoConnect else { return }
        didAttemptAutoConnect = true

        guard let activeID = activeProfileID,
              let activeProfile = savedProfiles.first(where: { $0.id == activeID }),
              let password = try? dependencies.keychainService.readPassword(account: activeProfile.credentialRef),
              !password.isEmpty else {
            return
        }

        connect(profile: activeProfile, password: password, showFailureAlert: false)
    }

    private func updateConnectionIndicator() {
        if isConnecting {
            connectionBarButtonItem.title = "加载中……"
        } else if let profile = dependencies.appSession.activeProfile {
            connectionBarButtonItem.title = "\(profile.username)@\(profile.shareName)\(profile.basePath)"
        } else {
            connectionBarButtonItem.title = "单机模式"
        }
        connectionBarButtonItem.isEnabled = !isConnecting
        connectionBarButtonItem.tintColor = .systemGreen
    }

    private func updateConnectionMenu() {
        connectionBarButtonItem.menu = buildConnectionMenu()
    }

    private func buildConnectionMenu() -> UIMenu {
        let disconnected = dependencies.appSession.activeProfile == nil
        let disconnectAction = makeMenuAction(
            title: "单机模式",
            subtitle: "不连接外部存储",
            state: disconnected ? .on : .off
        ) { [weak self] _ in
            self?.disconnectRemote()
        }

        let savedServerActions: [UIMenuElement]
        if savedProfiles.isEmpty {
            savedServerActions = [
                UIAction(title: "暂无已保存 SMB 服务器", attributes: [.disabled]) { _ in }
            ]
        } else {
            savedServerActions = savedProfiles.map { profile in
                let title = "\(profile.username)@\(profile.name)"
                let subtitle = "SMB://\(profile.host)/\(profile.shareName)\(profile.basePath)"
                let state: UIMenuElement.State
                if let active = dependencies.appSession.activeProfile?.id, active == profile.id {
                    state = .on
                } else {
                    state = .off
                }
                return makeMenuAction(title: title, subtitle: subtitle, state: state) { [weak self] _ in
                    self?.promptPasswordAndConnect(profile: profile)
                }
            }
        }
        var currentChildren: [UIMenuElement] = [disconnectAction]
        currentChildren.append(contentsOf: savedServerActions)
        let currentMenu = UIMenu(
            title: "当前",
            options: .displayInline,
            children: currentChildren
        )

        let discoveredChildren: [UIMenuElement]
        if discoveredServers.isEmpty {
            discoveredChildren = [
                UIAction(title: "未发现 SMB 服务", attributes: [.disabled]) { _ in }
            ]
        } else {
            discoveredChildren = discoveredServers.map { server in
                UIAction(title: "\(server.serviceName) (\(server.host):\(server.port))") { [weak self] _ in
                    self?.openAddServerFlow(
                        draft: SMBServerLoginDraft(
                            name: server.serviceName,
                            host: server.host,
                            port: server.port > 0 ? server.port : 445,
                            username: "",
                            domain: nil
                        )
                    )
                }
            }
        }
        let discoveredSection = UIMenu(title: "局域网发现", options: .displayInline, children: discoveredChildren)
        let manualAdd = UIAction(title: "手动添加 SMB 服务器", image: UIImage(systemName: "plus")) { [weak self] _ in
            self?.openAddServerFlow(
                draft: SMBServerLoginDraft(
                    name: "",
                    host: "",
                    port: 445,
                    username: "",
                    domain: nil
                )
            )
        }
        let manualSection = UIMenu(title: "", options: .displayInline, children: [manualAdd])
        let addServerMenu = UIMenu(
            title: "添加SMB服务器",
            children: [discoveredSection, manualSection]
        )

        return UIMenu(children: [currentMenu, addServerMenu])
    }

    private func makeMenuAction(
        title: String,
        subtitle: String?,
        state: UIMenuElement.State = .off,
        handler: @escaping UIActionHandler
    ) -> UIAction {
        if #available(iOS 15.0, *) {
            return UIAction(
                title: title,
                subtitle: subtitle,
                image: nil,
                identifier: nil,
                discoverabilityTitle: nil,
                attributes: [],
                state: state,
                handler: handler
            )
        }

        if let subtitle, !subtitle.isEmpty {
            return UIAction(title: "\(title) · \(subtitle)", state: state, handler: handler)
        }
        return UIAction(title: title, state: state, handler: handler)
    }

    private func disconnectRemote() {
        if backupSessionController.state == .running {
            presentAlert(title: "备份进行中", message: "请先停止备份后再断开远端连接")
            return
        }
        try? dependencies.databaseManager.setActiveServerProfileID(nil)
        dependencies.appSession.clear()
    }

    private func promptPasswordAndConnect(profile: ServerProfileRecord) {
        if isConnecting { return }

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
        alert.addAction(UIAlertAction(title: "连接", style: .default, handler: { [weak self] _ in
            guard let self,
                  let password = alert.textFields?.first?.text,
                  !password.isEmpty else { return }
            try? self.dependencies.keychainService.save(password: password, account: profile.credentialRef)
            self.connect(profile: profile, password: password)
        }))
        present(alert, animated: true)
    }

    private func connect(profile: ServerProfileRecord, password: String, showFailureAlert: Bool = true) {
        guard !isConnecting else { return }
        isConnecting = true
        updateConnectionIndicator()
        updateConnectionMenu()

        Task { [weak self] in
            guard let self else { return }

            do {
                _ = try await self.dependencies.backupExecutor.reloadRemoteIndex(
                    profile: profile,
                    password: password
                )
                try self.dependencies.databaseManager.setActiveServerProfileID(profile.id)
                self.dependencies.appSession.activate(profile: profile, password: password)

                await MainActor.run {
                    self.isConnecting = false
                    self.loadSavedProfiles()
                    self.updateConnectionIndicator()
                    self.updateConnectionMenu()
                }
            } catch {
                await MainActor.run {
                    self.isConnecting = false
                    self.updateConnectionIndicator()
                    self.updateConnectionMenu()
                    if showFailureAlert {
                        self.presentAlert(title: "连接失败", message: error.localizedDescription)
                    }
                }
            }
        }
    }

    private func openAddServerFlow(draft: SMBServerLoginDraft) {
        let addVC = AddSMBServerLoginViewController(dependencies: dependencies, draft: draft) { [weak self] profile, password in
            guard let self else { return }
            self.loadSavedProfiles()
            self.updateConnectionMenu()
            self.connect(profile: profile, password: password)
        }
        navigationController?.pushViewController(addVC, animated: true)
    }

    @objc
    private func openBackupStatusTapped() {
        let statusVC = BackupStatusViewController(sessionController: backupSessionController)
        let nav = UINavigationController(rootViewController: statusVC)
        nav.modalPresentationStyle = .pageSheet
        present(nav, animated: true)
    }

    @objc
    private func reloadRemoteIndexTapped() {
        guard hasActiveConnection else {
            presentAlert(title: "未连接", message: "请先连接 SMB 服务器")
            return
        }
        guard backupSessionController.state != .running else {
            presentAlert(title: "备份进行中", message: "请先暂停或停止备份后再重建索引")
            return
        }
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            presentAlert(title: "未连接", message: "请先连接 SMB 服务器")
            return
        }

        Task { [weak self] in
            guard let self else { return }
            do {
                let snapshot = try await self.dependencies.backupExecutor.reloadRemoteIndex(
                    profile: profile,
                    password: password
                )
                await MainActor.run {
                    self.scheduleReloadAllData()
                    if snapshot.totalCount == 0 {
                        self.presentAlert(title: "重建完成", message: "远端索引为空")
                    } else {
                        self.presentAlert(title: "重建完成", message: "远端索引共 \(snapshot.totalCount) 项")
                    }
                }
            } catch {
                await MainActor.run {
                    self.presentAlert(title: "重建失败", message: error.localizedDescription)
                }
            }
        }
    }

    private func scheduleReloadAllData() {
        reloadTask?.cancel()
        reloadTask = Task { [weak self] in
            await self?.reloadAllData()
        }
    }

    private func reloadAllData() async {
        await loadLocalItems()
        await MainActor.run {
            self.loadRemoteItems()
            self.rebuildMergedItems()
            self.applySections()
        }
    }

    private func loadLocalItems() async {
        let status = dependencies.photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await dependencies.photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }

        guard authorized else {
            await MainActor.run {
                self.localItems = []
                self.localAssetsByID = [:]
                self.localAssetIdentifierByHash = [:]
            }
            return
        }

        let assets = dependencies.photoLibraryService.fetchAssets()
        let snapshot = dependencies.backupExecutor.currentRemoteSnapshot()
        let remoteAssetFingerprintSet = snapshot.assetFingerprintSet

        let localHashMapByAsset = (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
        let localFingerprintByAsset = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]

        let finalizedLocalAssetByHash = HomeAlbumMatching.makeHashToAssetIndex(localHashMapByAsset)

        let builtItems: [LocalAlbumItem] = assets.map { asset in
            let mediaKind: AlbumMediaKind
            if PhotoLibraryService.isLivePhoto(asset) {
                mediaKind = .livePhoto
            } else if asset.mediaType == .video {
                mediaKind = .video
            } else {
                mediaKind = .photo
            }

            let hashes = localHashMapByAsset[asset.localIdentifier] ?? []
            let isBackedUp: Bool
            if let fingerprint = localFingerprintByAsset[asset.localIdentifier] {
                isBackedUp = remoteAssetFingerprintSet.contains(fingerprint)
            } else {
                isBackedUp = false
            }

            return LocalAlbumItem(
                id: asset.localIdentifier,
                asset: asset,
                creationDate: asset.creationDate ?? Date(timeIntervalSince1970: 0),
                isBackedUp: isBackedUp,
                mediaKind: mediaKind,
                contentHashes: hashes
            )
        }

        let mapping = Dictionary(uniqueKeysWithValues: assets.map { ($0.localIdentifier, $0) })
        await MainActor.run {
            self.localItems = builtItems
            self.localAssetsByID = mapping
            self.localAssetIdentifierByHash = finalizedLocalAssetByHash
        }
    }

    private func loadRemoteItems() {
        guard hasActiveConnection else {
            remoteItems = []
            return
        }

        let snapshot = dependencies.backupExecutor.currentRemoteSnapshot()
        remoteItems = HomeAlbumMatching.buildRemoteItems(from: snapshot)
    }

    private func rebuildMergedItems() {
        mergedItems = HomeAlbumMatching.mergeItems(
            localItems: localItems,
            remoteItems: remoteItems,
            localAssetIdentifierByHash: localAssetIdentifierByHash,
            hasActiveConnection: hasActiveConnection
        )
    }

    private func applySections(reloadMode: SectionReloadMode = .full) {
        let filteredItems: [HomeAlbumItem]
        switch sourceFilterMode {
        case .all:
            filteredItems = mergedItems
        case .localOnly:
            filteredItems = mergedItems.filter { $0.sourceTag == .localOnly }
        case .remoteOnly:
            filteredItems = mergedItems.filter { $0.sourceTag == .remoteOnly }
        case .both:
            filteredItems = mergedItems.filter { $0.sourceTag == .both }
        }

        let grouped = Dictionary(grouping: filteredItems) { item -> YearMonth in
            let components = calendar.dateComponents([.year, .month], from: item.creationDate)
            return YearMonth(year: components.year ?? 1970, month: components.month ?? 1)
        }

        let sortedKeys = grouped.keys.sorted {
            switch sortMode {
            case .descending:
                return $0 > $1
            case .ascending:
                return $0 < $1
            }
        }

        let newSections = sortedKeys.map { key in
            let values = (grouped[key] ?? []).sorted {
                switch sortMode {
                case .descending:
                    return $0.creationDate > $1.creationDate
                case .ascending:
                    return $0.creationDate < $1.creationDate
                }
            }
            return AlbumSection(key: key, items: values)
        }

        let oldSections = sections
        sections = newSections
        updateFilterMenu()
        applyCollectionUpdates(oldSections: oldSections, newSections: newSections, reloadMode: reloadMode)
    }

    private func applyCollectionUpdates(
        oldSections: [AlbumSection],
        newSections: [AlbumSection],
        reloadMode: SectionReloadMode
    ) {
        switch reloadMode {
        case .full:
            collectionView.reloadData()

        case .nonAnimatedDiff:
            guard oldSections.count == newSections.count else {
                UIView.performWithoutAnimation {
                    collectionView.reloadData()
                }
                return
            }

            let keysUnchanged = zip(oldSections, newSections).allSatisfy { lhs, rhs in
                lhs.key == rhs.key
            }
            guard keysUnchanged else {
                UIView.performWithoutAnimation {
                    collectionView.reloadData()
                }
                return
            }

            var changedSections = IndexSet()
            for index in newSections.indices {
                if !Self.sameSectionItems(lhs: oldSections[index].items, rhs: newSections[index].items) {
                    changedSections.insert(index)
                }
            }

            guard !changedSections.isEmpty else { return }
            UIView.performWithoutAnimation {
                collectionView.performBatchUpdates {
                    collectionView.reloadSections(changedSections)
                }
            }
        }
    }

    private static func sameSectionItems(lhs: [HomeAlbumItem], rhs: [HomeAlbumItem]) -> Bool {
        guard lhs.count == rhs.count else { return false }
        for (left, right) in zip(lhs, rhs) {
            if left.id != right.id || left.sourceTag != right.sourceTag || left.mediaKind != right.mediaKind {
                return false
            }
        }
        return true
    }

    private func updateFilterMenu() {
        let sourceAll = UIAction(title: "全部", state: sourceFilterMode == .all ? .on : .off) { [weak self] _ in
            self?.sourceFilterMode = .all
            self?.applySections()
        }
        let sourceLocal = UIAction(title: "仅本地", state: sourceFilterMode == .localOnly ? .on : .off) { [weak self] _ in
            self?.sourceFilterMode = .localOnly
            self?.applySections()
        }
        let sourceRemote = UIAction(title: "仅远端", state: sourceFilterMode == .remoteOnly ? .on : .off) { [weak self] _ in
            self?.sourceFilterMode = .remoteOnly
            self?.applySections()
        }
        let sourceBoth = UIAction(title: "远端+本地", state: sourceFilterMode == .both ? .on : .off) { [weak self] _ in
            self?.sourceFilterMode = .both
            self?.applySections()
        }
        let sourceMenu = UIMenu(title: "来源", children: [sourceAll, sourceLocal, sourceRemote, sourceBoth])

        let sortDesc = UIAction(title: "倒序", state: sortMode == .descending ? .on : .off) { [weak self] _ in
            self?.sortMode = .descending
            self?.applySections()
        }
        let sortAsc = UIAction(title: "正序", state: sortMode == .ascending ? .on : .off) { [weak self] _ in
            self?.sortMode = .ascending
            self?.applySections()
        }
        let sortMenu = UIMenu(title: "排序", children: [sortAsc, sortDesc])

        let displaySquare = UIAction(title: "正方形照片风格", state: displayOption == .square ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.displayOption = .square
            self.collectionView.collectionViewLayout.invalidateLayout()
            self.collectionView.reloadData()
            self.updateFilterMenu()
        }
        let displayOriginal = UIAction(title: "原始比例网格", state: displayOption == .originalRatio ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.displayOption = .originalRatio
            self.collectionView.collectionViewLayout.invalidateLayout()
            self.collectionView.reloadData()
            self.updateFilterMenu()
        }
        let displayMenu = UIMenu(title: "显示选项", children: [displaySquare, displayOriginal])

        var menuChildren: [UIMenuElement] = [sourceMenu, sortMenu, displayMenu]
        if hasActiveConnection {
            let disabledWhileRunning = backupSessionController.state == .running
            let rebuild = UIAction(
                title: "重建远端索引",
                image: UIImage(systemName: "arrow.clockwise"),
                attributes: disabledWhileRunning ? [.disabled] : []
            ) { [weak self] _ in
                self?.reloadRemoteIndexTapped()
            }
            menuChildren.append(UIMenu(title: "", options: .displayInline, children: [rebuild]))
        }

        filterToolbarItem.menu = UIMenu(children: menuChildren)
    }

    private func gridItemWidth() -> CGFloat {
        let horizontalInsets = GridLayout.itemSpacing * 2
        let width = collectionView.bounds.width - horizontalInsets - (GridLayout.columns - 1) * GridLayout.itemSpacing
        return floor(width / GridLayout.columns)
    }

    private func aspectRatio(for item: HomeAlbumItem) -> CGFloat {
        if let local = item.localItem {
            let width = max(local.asset.pixelWidth, 1)
            let height = max(local.asset.pixelHeight, 1)
            return CGFloat(height) / CGFloat(width)
        }
        if let remote = item.remoteItem,
           let width = remote.pixelWidth,
           let height = remote.pixelHeight,
           width > 0,
           height > 0 {
            return CGFloat(height) / CGFloat(width)
        }
        return 1
    }

    private func badges(for item: HomeAlbumItem) -> [(String, UIColor)] {
        switch item.mediaKind {
        case .livePhoto:
            return [("LIVE", .systemTeal)]
        case .video:
            return [("VIDEO", .systemPurple)]
        case .photo:
            return [("PHOTO", .systemBlue)]
        }
    }

    private func topRightSourceBadge(for item: HomeAlbumItem) -> (String, UIColor)? {
        guard item.remoteItem != nil else { return nil }
        return ("远端", .systemOrange)
    }

    private func topLeftSourceBadges(for item: HomeAlbumItem) -> [(String, UIColor)] {
        guard item.localItem != nil else { return [] }
        return [("本地", .systemGreen)]
    }

    private func configureCell(_ cell: AlbumGridCell, item: HomeAlbumItem) {
        cell.representedID = item.id
        cell.titleLabel.text = Self.dayFormatter.string(from: item.creationDate)
        cell.setBadges(topLeftSourceBadges(for: item))
        cell.setTopRightBadge(topRightSourceBadge(for: item))
        cell.setBottomBadges(badges(for: item))
        cell.setUnbacked(false)

        switch item.sourceTag {
        case .localOnly:
            if let local = item.localItem {
                configureLocalThumbnail(cell, asset: local.asset, representedID: item.id)
            } else {
                applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
            }
        case .both:
            if let local = item.localItem {
                configureLocalThumbnail(cell, asset: local.asset, representedID: item.id)
            } else if let remote = item.remoteItem {
                configureRemoteThumbnail(cell, item: remote, representedID: item.id)
            } else {
                applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
            }
        case .remoteOnly:
            if let remote = item.remoteItem {
                configureRemoteThumbnail(cell, item: remote, representedID: item.id)
            } else {
                applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
            }
        }
    }

    private func configureLocalThumbnail(_ cell: AlbumGridCell, asset: PHAsset, representedID: String) {
        applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
        dependencies.photoLibraryService.requestThumbnail(for: asset, targetSize: CGSize(width: 400, height: 400)) { image in
            if cell.representedID == representedID {
                if let image {
                    cell.imageView.contentMode = .scaleAspectFill
                    cell.imageView.tintColor = nil
                    cell.imageView.image = image
                } else {
                    self.applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
                }
            }
        }
    }

    private func configureRemoteThumbnail(_ cell: AlbumGridCell, item: RemoteAlbumItem, representedID: String) {
        if item.mediaKind == .video {
            applyLoadingPlaceholder(to: cell.imageView, symbolName: "video")
            return
        }

        guard let source = makeRemoteKingfisherSource(for: item, traitCollection: cell.traitCollection) else {
            applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
            return
        }

        let placeholder = loadingPlaceholderImage(symbolName: "photo")
        applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
        cell.imageView.kf.indicatorType = .activity
        cell.imageView.kf.setImage(
            with: source,
            placeholder: placeholder,
            options: remoteKingfisherOptions(displayScale: max(cell.traitCollection.displayScale, 1))
        ) { [weak cell] result in
            guard let cell, cell.representedID == representedID else { return }
            switch result {
            case .success:
                cell.imageView.contentMode = .scaleAspectFill
                cell.imageView.tintColor = nil
            case .failure:
                cell.imageView.contentMode = .center
                cell.imageView.tintColor = .systemGray3
                cell.imageView.image = self.loadingPlaceholderImage(symbolName: "exclamationmark.triangle")
            }
        }
    }

    private func applyLoadingPlaceholder(to imageView: UIImageView, symbolName: String) {
        imageView.contentMode = .center
        imageView.tintColor = .systemGray3
        imageView.image = loadingPlaceholderImage(symbolName: symbolName)
    }

    private func loadingPlaceholderImage(symbolName: String) -> UIImage? {
        let config = UIImage.SymbolConfiguration(pointSize: 14, weight: .regular)
        return UIImage(systemName: symbolName, withConfiguration: config)?.withRenderingMode(.alwaysTemplate)
    }

    private func makeRemoteKingfisherSource(
        for item: RemoteAlbumItem,
        traitCollection: UITraitCollection
    ) -> Source? {
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            return nil
        }

        let scale = max(traitCollection.displayScale, 1)
        let maxPixelSize = max(gridItemWidth() * scale * 1.6, 220)
        let remoteAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: item.representative.remoteRelativePath
        )

        let provider = SMBRemoteImageDataProvider(
            profile: profile,
            password: password,
            remoteAbsolutePath: remoteAbsolutePath,
            maxPixelSize: maxPixelSize,
            thumbnailService: remoteThumbnailService
        )
        return .provider(provider)
    }

    private func remoteKingfisherOptions(displayScale: CGFloat) -> KingfisherOptionsInfo {
        [
            .targetCache(remoteImageCache),
            .memoryCacheExpiration(.seconds(600)),
            .diskCacheExpiration(.days(7)),
            .backgroundDecode,
            .scaleFactor(max(displayScale, 1)),
            .transition(.fade(0.12))
        ]
    }

    private func refreshLocalHashMirrorIndex() async {
        let mirrorMap: [Data: [String]]
        do {
            let hashMapByAsset = try contentHashIndexRepository.fetchHashMapByAsset()
            mirrorMap = HomeAlbumMatching.makeHashToAssetIndex(hashMapByAsset)
        } catch {
            mirrorMap = [:]
        }

        await MainActor.run {
            self.localAssetIdentifierByHash = mirrorMap
        }
    }

    private func exportMonthDebugReport(for key: YearMonth, scope: MonthDebugExportScope = .all) {
        let monthText = String(format: "%04d-%02d", key.year, key.month)

        let localMonthItems = localItems
            .filter { Self.yearMonth(for: $0.creationDate) == key }
            .sorted { $0.creationDate > $1.creationDate }

        let remoteSnapshotResources = dependencies.backupExecutor.currentRemoteSnapshot().resources
            .filter { $0.year == key.year && $0.month == key.month }
            .sorted { lhs, rhs in
                if lhs.creationDate != rhs.creationDate {
                    return lhs.creationDate > rhs.creationDate
                }
                return lhs.fileName < rhs.fileName
            }

        let remoteMonthItems = remoteItems
            .filter { item in
                item.resources.contains { $0.year == key.year && $0.month == key.month }
            }
            .sorted { $0.creationDate > $1.creationDate }

        let mergedMonthItems = mergedItems
            .filter { Self.yearMonth(for: $0.creationDate) == key }
            .sorted { $0.creationDate > $1.creationDate }

        let localOnlyItems = mergedMonthItems
            .filter { $0.sourceTag == .localOnly }
            .compactMap(\.localItem)
            .sorted { $0.creationDate > $1.creationDate }

        let remoteOnlyItems = mergedMonthItems
            .filter { $0.sourceTag == .remoteOnly }
            .compactMap(\.remoteItem)
            .sorted { $0.creationDate > $1.creationDate }

        let localAssetIDs: [String] = {
            switch scope {
            case .all:
                return localMonthItems.map(\.id)
            case .unmatchedOnly:
                return localOnlyItems.map(\.id)
            }
        }()
        let hashIndexRows = loadHashIndexRows(forAssetIDs: localAssetIDs)

        var seenRemoteResourceIDs = Set<String>()
        let remoteOnlyResources = remoteOnlyItems
            .flatMap(\.resources)
            .filter { seenRemoteResourceIDs.insert($0.id).inserted }
            .sorted { lhs, rhs in
                if lhs.creationDate != rhs.creationDate {
                    return lhs.creationDate > rhs.creationDate
                }
                return lhs.fileName < rhs.fileName
            }

        let mapRemoteResource: (RemoteManifestResource) -> [String: Any] = { resource in
            [
                "id": resource.id,
                "monthKey": resource.monthKey,
                "fileName": resource.fileName,
                "remoteRelativePath": resource.remoteRelativePath,
                "resourceType": resource.resourceType,
                "fileSize": resource.fileSize,
                "creationDate": Self.debugISO8601Formatter.string(from: resource.creationDate),
                "contentHashHex": resource.contentHashHex
            ]
        }

        let mapRemoteGroupedItem: (RemoteAlbumItem) -> [String: Any] = { [self] item in
            [
                "id": item.id,
                "creationDate": Self.debugISO8601Formatter.string(from: item.creationDate),
                "mediaKind": self.mediaKindText(item.mediaKind),
                "resourceCount": item.resources.count,
                "representativePath": item.representative.remoteRelativePath,
                "contentHashes": item.contentHashes.map(Self.hexString),
                "resources": item.resources
                    .sorted { lhs, rhs in
                        if lhs.creationDate != rhs.creationDate {
                            return lhs.creationDate > rhs.creationDate
                        }
                        return lhs.fileName < rhs.fileName
                    }
                    .map(mapRemoteResource)
            ]
        }

        let basePayload: [String: Any] = [
            "generatedAt": Self.debugISO8601Formatter.string(from: Date()),
            "trigger": scope.triggerText,
            "month": monthText,
            "scope": scope.scopeText,
            "connection": [
                "connected": hasActiveConnection,
                "profileName": dependencies.appSession.activeProfile?.name as Any,
                "host": dependencies.appSession.activeProfile?.host as Any,
                "shareName": dependencies.appSession.activeProfile?.shareName as Any,
                "basePath": dependencies.appSession.activeProfile?.basePath as Any,
                "username": dependencies.appSession.activeProfile?.username as Any
            ]
        ]

        var report = basePayload
        switch scope {
        case .all:
            let mergedPayload: [[String: Any]] = mergedMonthItems.map { [self] item in
                let sourceText: String
                switch item.sourceTag {
                case .localOnly:
                    sourceText = "localOnly"
                case .remoteOnly:
                    sourceText = "remoteOnly"
                case .both:
                    sourceText = "both"
                }

                return [
                    "id": item.id,
                    "creationDate": Self.debugISO8601Formatter.string(from: item.creationDate),
                    "sourceTag": sourceText,
                    "mediaKind": self.mediaKindText(item.mediaKind),
                    "localAssetIdentifier": item.localItem?.id as Any,
                    "remoteItemIdentifier": item.remoteItem?.id as Any
                ]
            }

            report["counts"] = [
                "localItems": localMonthItems.count,
                "remoteSnapshotResources": remoteSnapshotResources.count,
                "remoteGroupedItems": remoteMonthItems.count,
                "mergedItems": mergedMonthItems.count,
                "localHashIndexRows": hashIndexRows.count,
                "localOnlyItems": localOnlyItems.count,
                "remoteOnlyItems": remoteOnlyItems.count,
                "bothItems": max(mergedMonthItems.count - localOnlyItems.count - remoteOnlyItems.count, 0)
            ]
            report["localItems"] = localMonthItems.map { [self] item in
                [
                    "assetLocalIdentifier": item.id,
                    "creationDate": Self.debugISO8601Formatter.string(from: item.creationDate),
                    "mediaKind": self.mediaKindText(item.mediaKind),
                    "isBackedUp": item.isBackedUp,
                    "contentHashes": item.contentHashes.map(Self.hexString)
                ] as [String: Any]
            }
            report["localHashIndexRows"] = hashIndexRows
            report["remoteSnapshotResources"] = remoteSnapshotResources.map(mapRemoteResource)
            report["remoteGroupedItems"] = remoteMonthItems.map(mapRemoteGroupedItem)
            report["mergedItems"] = mergedPayload

        case .unmatchedOnly:
            report["counts"] = [
                "localOnlyItems": localOnlyItems.count,
                "remoteOnlyItems": remoteOnlyItems.count,
                "remoteOnlyResources": remoteOnlyResources.count,
                "localOnlyHashIndexRows": hashIndexRows.count,
                "excludedBothItems": max(mergedMonthItems.count - localOnlyItems.count - remoteOnlyItems.count, 0)
            ]
            report["localOnlyItems"] = localOnlyItems.map { [self] item in
                [
                    "assetLocalIdentifier": item.id,
                    "creationDate": Self.debugISO8601Formatter.string(from: item.creationDate),
                    "mediaKind": self.mediaKindText(item.mediaKind),
                    "isBackedUp": item.isBackedUp,
                    "contentHashes": item.contentHashes.map(Self.hexString)
                ] as [String: Any]
            }
            report["localOnlyHashIndexRows"] = hashIndexRows
            report["remoteOnlyItems"] = remoteOnlyItems.map(mapRemoteGroupedItem)
            report["remoteOnlyResources"] = remoteOnlyResources.map(mapRemoteResource)
        }

        do {
            let data = try JSONSerialization.data(withJSONObject: report, options: [.prettyPrinted, .sortedKeys])
            let filename = "month_debug_\(scope.fileTag)_\(key.year)_\(String(format: "%02d", key.month))_\(Self.debugFileNameFormatter.string(from: Date())).json"
            let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(filename)
            try data.write(to: fileURL, options: .atomic)
            presentShareSheet(for: fileURL)
        } catch {
            presentAlert(title: "导出失败", message: error.localizedDescription)
        }
    }

    private func loadHashIndexRows(forAssetIDs assetIDs: [String]) -> [[String: Any]] {
        guard !assetIDs.isEmpty else { return [] }
        do {
            return try dependencies.databaseManager.read { db in
                let placeholders = Array(repeating: "?", count: assetIDs.count).joined(separator: ",")
                let sql = """
                SELECT assetLocalIdentifier, role, slot, hex(contentHash) AS contentHashHex
                FROM local_asset_resources
                WHERE assetLocalIdentifier IN (\(placeholders))
                ORDER BY assetLocalIdentifier, role, slot
                """
                let rows = try Row.fetchAll(db, sql: sql, arguments: StatementArguments(assetIDs))
                return rows.map { row in
                    [
                        "assetLocalIdentifier": row["assetLocalIdentifier"] as String,
                        "role": row["role"] as Int64,
                        "slot": row["slot"] as Int64,
                        "contentHashHex": row["contentHashHex"] as String
                    ]
                }
            }
        } catch {
            return [["error": error.localizedDescription]]
        }
    }

    private func presentShareSheet(for fileURL: URL) {
        let activityVC = UIActivityViewController(activityItems: [fileURL], applicationActivities: nil)
        if let popover = activityVC.popoverPresentationController {
            popover.sourceView = view
            popover.sourceRect = CGRect(x: view.bounds.midX, y: view.bounds.midY, width: 1, height: 1)
            popover.permittedArrowDirections = []
        }
        present(activityVC, animated: true)
    }

    private func mediaKindText(_ kind: AlbumMediaKind) -> String {
        switch kind {
        case .photo:
            return "photo"
        case .video:
            return "video"
        case .livePhoto:
            return "livePhoto"
        }
    }

    private func openRemoteOnlyDetail(for item: HomeAlbumItem) {
        guard let remote = item.remoteItem else {
            presentAlert(title: "仅远端资源", message: "该项目当前仅存在远端，但缺少可展示的信息。")
            return
        }

        var lines: [String] = []

        lines.append("来源: 仅远端")
        lines.append("条目标识: \(remote.id)")
        lines.append("媒体类型: \(mediaKindText(remote.mediaKind))")
        lines.append("创建时间: \(Self.detailDateFormatter.string(from: remote.creationDate))")
        lines.append("月份: \(remote.representative.monthKey)")

        if let profile = dependencies.appSession.activeProfile {
            let normalizedPath: String
            if profile.basePath.isEmpty {
                normalizedPath = "/"
            } else if profile.basePath.hasPrefix("/") {
                normalizedPath = profile.basePath
            } else {
                normalizedPath = "/\(profile.basePath)"
            }
            lines.append("当前连接: \(profile.username)@\(profile.shareName)\(normalizedPath)")
        } else {
            lines.append("当前连接: 未连接")
        }

        lines.append("资源数量: \(remote.resources.count)")
        lines.append("")
        lines.append("资源明细")

        let sortedResources = remote.resources.sorted { lhs, rhs in
            if lhs.creationDate != rhs.creationDate {
                return lhs.creationDate > rhs.creationDate
            }
            return lhs.fileName < rhs.fileName
        }

        for (index, resource) in sortedResources.enumerated() {
            lines.append("")
            lines.append("[\(index + 1)] \(resource.fileName)")
            lines.append("路径: \(resource.remoteRelativePath)")
            lines.append("类型: \(PhotoLibraryService.resourceTypeName(from: resource.resourceType)) (\(resource.resourceType))")
            lines.append("大小: \(ByteCountFormatter.string(fromByteCount: resource.fileSize, countStyle: .file))")
            lines.append("创建时间: \(Self.detailDateFormatter.string(from: resource.creationDate))")
            lines.append("Hash: \(resource.contentHashHex)")
        }

        let detailVC = RemoteOnlyItemInfoViewController(infoText: lines.joined(separator: "\n"))
        navigationController?.pushViewController(detailVC, animated: true)
    }

    nonisolated private static func yearMonth(for date: Date) -> YearMonth {
        let components = Calendar(identifier: .gregorian).dateComponents([.year, .month], from: date)
        return YearMonth(year: components.year ?? 1970, month: components.month ?? 1)
    }

    nonisolated private static func hexString(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }

    private static let dayFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "MM-dd"
        return formatter
    }()

    private static let debugISO8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let debugFileNameFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter
    }()

    private static let detailDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()
}

extension HomeViewController: UICollectionViewDataSource, UICollectionViewDelegate, UICollectionViewDelegateFlowLayout, UICollectionViewDataSourcePrefetching {
    func numberOfSections(in collectionView: UICollectionView) -> Int {
        sections.count
    }

    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        sections[section].items.count
    }

    func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        sizeForItemAt indexPath: IndexPath
    ) -> CGSize {
        let width = gridItemWidth()
        guard width > 0 else { return .zero }
        let item = sections[indexPath.section].items[indexPath.item]

        switch displayOption {
        case .square:
            return CGSize(width: width, height: width)
        case .originalRatio:
            let ratio = max(aspectRatio(for: item), 0.2)
            return CGSize(width: width, height: width * ratio)
        }
    }

    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        guard let cell = collectionView.dequeueReusableCell(withReuseIdentifier: AlbumGridCell.reuseID, for: indexPath) as? AlbumGridCell else {
            return UICollectionViewCell()
        }

        let item = sections[indexPath.section].items[indexPath.item]
        configureCell(cell, item: item)
        return cell
    }

    func collectionView(
        _ collectionView: UICollectionView,
        viewForSupplementaryElementOfKind kind: String,
        at indexPath: IndexPath
    ) -> UICollectionReusableView {
        guard kind == UICollectionView.elementKindSectionHeader,
              let view = collectionView.dequeueReusableSupplementaryView(
                  ofKind: kind,
                  withReuseIdentifier: AlbumSectionHeaderView.reuseID,
                  for: indexPath
              ) as? AlbumSectionHeaderView else {
            return UICollectionReusableView()
        }

        view.backgroundColor = .clear
        view.setRoundedRectStyle(true)
        view.setHorizontalInset(GridLayout.sectionHeaderInset)
        view.setTitle(sections[indexPath.section].title)
        let sectionKey = sections[indexPath.section].key
        view.onTap = { [weak self] in
            self?.exportMonthDebugReport(for: sectionKey, scope: .all)
        }
        view.onLongPress = { [weak self] in
            self?.exportMonthDebugReport(for: sectionKey, scope: .unmatchedOnly)
        }
        return view
    }

    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        let item = sections[indexPath.section].items[indexPath.item]

        if let local = item.localItem {
            let isBackedUp: Bool
            switch item.sourceTag {
            case .both:
                isBackedUp = true
            case .localOnly:
                isBackedUp = local.isBackedUp
            case .remoteOnly:
                isBackedUp = false
            }

            let detail = LocalAssetDetailViewController(
                dependencies: dependencies,
                asset: local.asset,
                isBackedUp: isBackedUp
            )
            detail.title = "照片详情"
            navigationController?.pushViewController(detail, animated: true)
            return
        }

        openRemoteOnlyDetail(for: item)
    }

    func collectionView(_ collectionView: UICollectionView, prefetchItemsAt indexPaths: [IndexPath]) {
        guard hasActiveConnection else { return }

        for indexPath in indexPaths {
            guard prefetchTasks[indexPath] == nil else { continue }
            guard indexPath.section < sections.count,
                  indexPath.item < sections[indexPath.section].items.count else {
                continue
            }

            let albumItem = sections[indexPath.section].items[indexPath.item]
            guard albumItem.sourceTag == .remoteOnly,
                  albumItem.localItem == nil,
                  albumItem.mediaKind != .video,
                  let remoteItem = albumItem.remoteItem,
                  let source = makeRemoteKingfisherSource(for: remoteItem, traitCollection: collectionView.traitCollection) else {
                continue
            }

            let task = KingfisherManager.shared.retrieveImage(
                with: source,
                options: remoteKingfisherOptions(displayScale: max(collectionView.traitCollection.displayScale, 1)),
                completionHandler: { [weak self] _ in
                    Task { @MainActor [weak self] in
                        self?.prefetchTasks[indexPath] = nil
                    }
                }
            )
            prefetchTasks[indexPath] = task
        }
    }

    func collectionView(_ collectionView: UICollectionView, cancelPrefetchingForItemsAt indexPaths: [IndexPath]) {
        for indexPath in indexPaths {
            prefetchTasks[indexPath]?.cancel()
            prefetchTasks[indexPath] = nil
        }
    }
}

private final class RemoteOnlyItemInfoViewController: UIViewController {
    private let infoText: String
    private let textView = UITextView()

    init(infoText: String) {
        self.infoText = infoText
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "远端条目信息"
        navigationItem.largeTitleDisplayMode = .never

        textView.backgroundColor = .secondarySystemBackground
        textView.isEditable = false
        textView.alwaysBounceVertical = true
        textView.font = .monospacedSystemFont(ofSize: 13, weight: .regular)
        textView.textContainerInset = UIEdgeInsets(top: 14, left: 14, bottom: 14, right: 14)
        textView.layer.cornerRadius = 12
        textView.text = infoText

        view.addSubview(textView)
        textView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide).inset(12)
        }
    }
}
