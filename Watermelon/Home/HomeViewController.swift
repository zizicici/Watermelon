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

    private enum AlbumMediaKind {
        case photo
        case video
        case livePhoto
    }

    private enum ItemSourceTag {
        case localOnly
        case remoteOnly
        case both
    }

    private struct LocalAlbumItem {
        let id: String
        let asset: PHAsset
        let creationDate: Date
        let isBackedUp: Bool
        let mediaKind: AlbumMediaKind
        let contentHashes: [Data]
    }

    private struct RemoteAlbumItem {
        let id: String
        let creationDate: Date
        let resources: [RemoteManifestResource]
        let representative: RemoteManifestResource
        let mediaKind: AlbumMediaKind
        let pixelWidth: Int?
        let pixelHeight: Int?
        let contentHashes: [Data]
    }

    private struct HomeAlbumItem {
        let id: String
        let creationDate: Date
        let sourceTag: ItemSourceTag
        let mediaKind: AlbumMediaKind
        let localItem: LocalAlbumItem?
        let remoteItem: RemoteAlbumItem?
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
    private var localAssetIdentifierByHash: [Data: String] = [:]

    private let remoteImageCache = ImageCache(name: "home_remote_album_cache")
    private let remoteThumbnailService = RemoteThumbnailService()
    private var prefetchTasks: [IndexPath: DownloadTask] = [:]
    private var reloadTask: Task<Void, Never>?
    private var pendingRemoteSectionRefresh = false

    private lazy var backupSessionController = BackupSessionController(dependencies: dependencies)
    private var backupSessionObserverID: UUID?
    private var lastObservedBackupState: BackupSessionController.State = .idle

    private var hasActiveConnection: Bool {
        dependencies.appSession.activeProfile != nil && dependencies.appSession.activePassword != nil
    }

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies

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
                self.scheduleReloadAllData()
            }
        }
    }

    private func scheduleRemoteSectionRefresh() {
        guard hasActiveConnection else { return }
        guard !pendingRemoteSectionRefresh else { return }
        pendingRemoteSectionRefresh = true

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
                    self.applySections()
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
        let remoteHashSet = dependencies.backupExecutor.currentRemoteSnapshot().hashSet

        var localHashMapByAsset: [String: [Data]] = [:]
        if let map = try? dependencies.databaseManager.read({ db -> [String: [Data]] in
            let rows = try Row.fetchAll(db, sql: "SELECT assetLocalIdentifier, contentHash FROM content_hash_index")
            var result: [String: [Data]] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                let hash: Data = row["contentHash"]
                result[assetID, default: []].append(hash)
            }
            return result
        }) {
            localHashMapByAsset = map
        }

        var localAssetByHash: [Data: String] = [:]
        for (assetID, hashes) in localHashMapByAsset {
            for hash in hashes where localAssetByHash[hash] == nil {
                localAssetByHash[hash] = assetID
            }
        }

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
            let isBackedUp = !hashes.isEmpty && !remoteHashSet.isEmpty && hashes.allSatisfy { remoteHashSet.contains($0) }

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
            self.localAssetIdentifierByHash = localAssetByHash
        }
    }

    private func loadRemoteItems() {
        guard hasActiveConnection else {
            remoteItems = []
            return
        }

        let resources = dependencies.backupExecutor.currentRemoteSnapshot().resources
        guard !resources.isEmpty else {
            remoteItems = []
            return
        }

        let liveCandidateGroups = Dictionary(grouping: resources) { resource -> String in
            let stem = (resource.fileName as NSString).deletingPathExtension.lowercased()
            return "\(resource.monthKey)|\(resource.creationDateNs ?? -1)|\(stem)"
        }

        var groupedIDs = Set<String>()
        var result: [RemoteAlbumItem] = []

        for (groupKey, groupResources) in liveCandidateGroups {
            let hasPhoto = groupResources.contains { ResourceTypeCode.isPhotoLike($0.resourceType) }
            let hasPairedVideo = groupResources.contains { $0.resourceType == ResourceTypeCode.pairedVideo }
            guard hasPhoto, hasPairedVideo,
                  let representative = Self.chooseRepresentativeResource(groupResources) else {
                continue
            }

            groupedIDs.formUnion(groupResources.map(\.id))
            let hashes = Array(Set(groupResources.map(\.contentHash)))
            result.append(
                RemoteAlbumItem(
                    id: "live:\(groupKey)",
                    creationDate: groupResources.map(\.creationDate).min() ?? representative.creationDate,
                    resources: groupResources,
                    representative: representative,
                    mediaKind: .livePhoto,
                    pixelWidth: nil,
                    pixelHeight: nil,
                    contentHashes: hashes
                )
            )
        }

        for resource in resources where !groupedIDs.contains(resource.id) {
            result.append(
                RemoteAlbumItem(
                    id: resource.id,
                    creationDate: resource.creationDate,
                    resources: [resource],
                    representative: resource,
                    mediaKind: Self.detectMediaKind(from: [resource]),
                    pixelWidth: nil,
                    pixelHeight: nil,
                    contentHashes: [resource.contentHash]
                )
            )
        }

        remoteItems = result.sorted { $0.creationDate > $1.creationDate }
    }

    private func rebuildMergedItems() {
        let localByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, $0) })
        var consumedLocalIDs = Set<String>()
        var seenRemoteContentKeys = Set<String>()
        var result: [HomeAlbumItem] = []
        result.reserveCapacity(localItems.count + remoteItems.count)

        if hasActiveConnection {
            for remote in remoteItems {
                let dedupeKey = Self.contentKey(hashes: remote.contentHashes)
                if !dedupeKey.isEmpty, seenRemoteContentKeys.contains(dedupeKey) {
                    continue
                }
                if !dedupeKey.isEmpty {
                    seenRemoteContentKeys.insert(dedupeKey)
                }

                let candidateLocalIDs = remote.contentHashes.compactMap { localAssetIdentifierByHash[$0] }
                let localID = candidateLocalIDs.first {
                    !consumedLocalIDs.contains($0) && localByID[$0] != nil
                }

                if let localID, let local = localByID[localID] {
                    consumedLocalIDs.insert(localID)
                    result.append(
                        HomeAlbumItem(
                            id: "both:\(local.id)",
                            creationDate: local.creationDate,
                            sourceTag: .both,
                            mediaKind: Self.mergeMediaKind(local: local.mediaKind, remote: remote.mediaKind),
                            localItem: local,
                            remoteItem: remote
                        )
                    )
                } else {
                    result.append(
                        HomeAlbumItem(
                            id: "remote:\(remote.id)",
                            creationDate: remote.creationDate,
                            sourceTag: .remoteOnly,
                            mediaKind: remote.mediaKind,
                            localItem: nil,
                            remoteItem: remote
                        )
                    )
                }
            }
        }

        for local in localItems where !consumedLocalIDs.contains(local.id) {
            result.append(
                HomeAlbumItem(
                    id: "local:\(local.id)",
                    creationDate: local.creationDate,
                    sourceTag: .localOnly,
                    mediaKind: local.mediaKind,
                    localItem: local,
                    remoteItem: nil
                )
            )
        }

        mergedItems = result
    }

    private func applySections() {
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

        sections = sortedKeys.map { key in
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

        updateFilterMenu()
        collectionView.reloadData()
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
        let mirrorMap: [Data: String]
        do {
            mirrorMap = try dependencies.databaseManager.read { db in
                let rows = try Row.fetchAll(db, sql: "SELECT assetLocalIdentifier, contentHash FROM content_hash_index")
                var result: [Data: String] = [:]
                result.reserveCapacity(rows.count)
                for row in rows {
                    let assetID: String = row["assetLocalIdentifier"]
                    let hash: Data = row["contentHash"]
                    if result[hash] == nil {
                        result[hash] = assetID
                    }
                }
                return result
            }
        } catch {
            mirrorMap = [:]
        }

        await MainActor.run {
            self.localAssetIdentifierByHash = mirrorMap
        }
    }

    private static func chooseRepresentativeResource(_ resources: [RemoteManifestResource]) -> RemoteManifestResource? {
        let preferred = resources.first {
            ResourceTypeCode.isPhotoLike($0.resourceType)
        }
        return preferred ?? resources.first
    }

    private static func detectMediaKind(from resources: [RemoteManifestResource]) -> AlbumMediaKind {
        let hasPairedVideo = resources.contains { $0.resourceType == ResourceTypeCode.pairedVideo }
        let hasPhotoLike = resources.contains { ResourceTypeCode.isPhotoLike($0.resourceType) }
        if hasPairedVideo, hasPhotoLike {
            return .livePhoto
        }

        let hasVideo = resources.contains { ResourceTypeCode.isVideoLike($0.resourceType) }
        return hasVideo ? .video : .photo
    }

    private static func mergeMediaKind(local: AlbumMediaKind, remote: AlbumMediaKind) -> AlbumMediaKind {
        if local == .livePhoto || remote == .livePhoto {
            return .livePhoto
        }
        if local == .video || remote == .video {
            return .video
        }
        return .photo
    }

    private static func contentKey(hashes: [Data]) -> String {
        hashes
            .map { $0.map { String(format: "%02x", $0) }.joined() }
            .sorted()
            .joined(separator: "|")
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

        presentAlert(title: "仅远端资源", message: "该项目当前仅存在远端，暂不支持本地详情展示。")
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
