import GRDB
import Kingfisher
import Photos
import SnapKit
import UIKit

final class AlbumViewController: UIViewController {
    private enum GridLayout {
        static let columns: CGFloat = 4
        static let itemSpacing: CGFloat = 4
        static let sectionTopInset: CGFloat = 8
        static let sectionBottomInset: CGFloat = 28
        static let sectionHeaderInset: CGFloat = itemSpacing + 6
    }

    private enum SourceMode: Int {
        case local
        case remote

        var title: String {
            switch self {
            case .local: return "本地"
            case .remote: return "远端"
            }
        }
    }

    private enum LocalFilterMode {
        case all
        case unbackedOnly
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

    private struct LocalAlbumItem {
        let id: String
        let asset: PHAsset
        let creationDate: Date
        let isBackedUp: Bool
        let mediaKind: AlbumMediaKind
    }

    private struct RemoteAlbumItem {
        let id: String
        let creationDate: Date
        let resources: [RemoteManifestResource]
        let representative: RemoteManifestResource
        let mediaKind: AlbumMediaKind
        let pixelWidth: Int?
        let pixelHeight: Int?
        let localMirrorAssetLocalIdentifier: String?
    }

    private enum AlbumItem {
        case local(LocalAlbumItem)
        case remote(RemoteAlbumItem)

        var id: String {
            switch self {
            case .local(let item): return item.id
            case .remote(let item): return item.id
            }
        }

        var date: Date {
            switch self {
            case .local(let item): return item.creationDate
            case .remote(let item): return item.creationDate
            }
        }
    }

    private struct AlbumSection {
        let key: YearMonth
        let items: [AlbumItem]

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
    private let onOpenSettings: () -> Void
    private let calendar = Calendar(identifier: .gregorian)

    private let sourceControl = UISegmentedControl(items: [SourceMode.local.title, SourceMode.remote.title])
    private let backupButton = UIButton(type: .system)
    private lazy var filterBarButtonItem = UIBarButtonItem(
        image: UIImage(systemName: "line.3.horizontal.decrease.circle"),
        style: .plain,
        target: nil,
        action: nil
    )
    private lazy var reloadRemoteBarButtonItem = UIBarButtonItem(
        image: UIImage(systemName: "arrow.clockwise"),
        style: .plain,
        target: self,
        action: #selector(reloadRemoteIndexTapped)
    )
    private lazy var restoreBarButtonItem = UIBarButtonItem(title: "导回", style: .plain, target: self, action: #selector(restoreSelectedRemote))

    private let collectionView: UICollectionView

    private var sourceMode: SourceMode = .local
    private var localItems: [LocalAlbumItem] = []
    private var remoteItems: [RemoteAlbumItem] = []
    private var sections: [AlbumSection] = []
    private var localAssetsByID: [String: PHAsset] = [:]
    private var localAssetIdentifierByHash: [Data: String] = [:]

    private var localFilterMode: LocalFilterMode = .all
    private var sortMode: SortMode = .descending
    private var displayOption: DisplayOption = .square
    private var selectedRemoteAssetIDs = Set<String>()
    private lazy var backupSessionController = BackupSessionController(dependencies: dependencies)
    private var backupSessionObserverID: UUID?
    private var lastObservedBackupState: BackupSessionController.State = .idle

    private let remoteImageCache = ImageCache(name: "remote_album_cache")
    private let remoteThumbnailService = RemoteThumbnailService()
    private var prefetchTasks: [IndexPath: DownloadTask] = [:]
    private var pendingRemoteSectionRefresh = false

    init(dependencies: DependencyContainer, onOpenSettings: @escaping () -> Void) {
        self.dependencies = dependencies
        self.onOpenSettings = onOpenSettings

        let layout = UICollectionViewFlowLayout()
        layout.minimumInteritemSpacing = GridLayout.itemSpacing
        layout.minimumLineSpacing = GridLayout.itemSpacing
        layout.headerReferenceSize = CGSize(width: 100, height: 30)
        self.collectionView = UICollectionView(frame: .zero, collectionViewLayout: layout)

        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
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

        remoteImageCache.memoryStorage.config.countLimit = 350
        remoteImageCache.memoryStorage.config.totalCostLimit = 80 * 1024 * 1024
        remoteImageCache.memoryStorage.config.expiration = .seconds(600)
        remoteImageCache.diskStorage.config.sizeLimit = 400 * 1024 * 1024
        remoteImageCache.diskStorage.config.expiration = .days(7)

        configureUI()
        configureNavigationItems()
        bindBackupSession()

        Task { await reloadAllData() }
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
        flow.headerReferenceSize = CGSize(width: collectionView.bounds.width, height: 30)
        updateCollectionContentInsetForFloatingButton()
    }

    private func configureUI() {
        sourceControl.selectedSegmentIndex = 0
        sourceControl.addTarget(self, action: #selector(sourceChanged), for: .valueChanged)
        updateSourceControlTitles()

        var backupButtonConfig = UIButton.Configuration.filled()
        backupButtonConfig.title = "开始备份"
        backupButtonConfig.titleAlignment = .center
        backupButtonConfig.cornerStyle = .large
        backupButtonConfig.contentInsets = NSDirectionalEdgeInsets(top: 12, leading: 16, bottom: 12, trailing: 16)
        backupButtonConfig.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
            var attrs = incoming
            attrs.font = .systemFont(ofSize: 16, weight: .semibold)
            return attrs
        }
        backupButtonConfig.baseBackgroundColor = UIColor.systemBlue
        backupButtonConfig.baseForegroundColor = UIColor.white
        backupButton.configuration = backupButtonConfig
        backupButton.addTarget(self, action: #selector(openBackupStatusTapped), for: .touchUpInside)

        collectionView.backgroundColor = .clear
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.prefetchDataSource = self
        collectionView.allowsMultipleSelection = true
        collectionView.register(AlbumGridCell.self, forCellWithReuseIdentifier: AlbumGridCell.reuseID)
        collectionView.register(AlbumSectionHeaderView.self, forSupplementaryViewOfKind: UICollectionView.elementKindSectionHeader, withReuseIdentifier: AlbumSectionHeaderView.reuseID)

        let stack = UIStackView(arrangedSubviews: [sourceControl])
        stack.axis = .vertical
        stack.spacing = 8

        view.addSubview(stack)
        view.addSubview(collectionView)
        view.addSubview(backupButton)

        stack.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        collectionView.snp.makeConstraints { make in
            make.top.equalTo(stack.snp.bottom).offset(10)
            make.leading.trailing.equalToSuperview()
            make.bottom.equalToSuperview()
        }

        backupButton.snp.makeConstraints { make in
            make.centerX.equalToSuperview()
            make.leading.greaterThanOrEqualToSuperview().inset(12)
            make.trailing.lessThanOrEqualToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(10)
            make.height.greaterThanOrEqualTo(52)
        }
    }

    private func configureNavigationItems() {
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            image: UIImage(systemName: "gearshape"),
            style: .plain,
            target: self,
            action: #selector(openSettingsTapped)
        )
        updateFilterMenu()
        updateNavigationItems()
    }

    private func updateNavigationItems() {
        switch sourceMode {
        case .local:
            navigationItem.rightBarButtonItems = nil
            navigationItem.rightBarButtonItem = filterBarButtonItem
        case .remote:
            navigationItem.rightBarButtonItem = nil
            navigationItem.rightBarButtonItems = [restoreBarButtonItem, reloadRemoteBarButtonItem]
        }
    }

    @objc
    private func sourceChanged() {
        sourceMode = SourceMode(rawValue: sourceControl.selectedSegmentIndex) ?? .local
        selectedRemoteAssetIDs.removeAll()
        if sourceMode == .local {
            for task in prefetchTasks.values {
                task.cancel()
            }
            prefetchTasks.removeAll()
            collectionView.indexPathsForSelectedItems?.forEach { collectionView.deselectItem(at: $0, animated: false) }
            Task { await remoteThumbnailService.invalidate() }
            collectionView.reloadData()
            applySections()
            updateNavigationItems()
            return
        }

        Task { [weak self] in
            guard let self else { return }
            await self.refreshLocalHashMirrorIndex()
            await MainActor.run {
                self.loadRemoteItems()
                self.collectionView.reloadData()
                self.applySections()
                self.updateNavigationItems()
            }
        }
    }

    @objc
    private func openSettingsTapped() {
        onOpenSettings()
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
        guard sourceMode == .remote else { return }
        guard backupSessionController.state != .running else {
            presentAlert(title: "备份进行中", message: "请先暂停或停止备份后再刷新远端索引")
            return
        }
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            presentAlert(title: "未登录", message: "请先登录 SMB")
            return
        }

        reloadRemoteBarButtonItem.isEnabled = false
        restoreBarButtonItem.isEnabled = false

        Task { [weak self] in
            guard let self else { return }

            do {
                let snapshot = try await self.dependencies.backupExecutor.reloadRemoteIndex(
                    profile: profile,
                    password: password
                )
                await self.reloadAllData()

                await MainActor.run {
                    self.selectedRemoteAssetIDs.removeAll()
                    self.collectionView.indexPathsForSelectedItems?.forEach {
                        self.collectionView.deselectItem(at: $0, animated: false)
                    }
                    self.reloadRemoteBarButtonItem.isEnabled = true
                    self.restoreBarButtonItem.isEnabled = true
                    self.applySections()

                    if snapshot.totalCount == 0 {
                        self.presentAlert(title: "已刷新", message: "远端索引为空")
                    } else {
                        self.presentAlert(title: "已刷新", message: "远端索引共 \(snapshot.totalCount) 项")
                    }
                }
            } catch {
                await MainActor.run {
                    self.reloadRemoteBarButtonItem.isEnabled = true
                    self.restoreBarButtonItem.isEnabled = true
                    self.presentAlert(title: "刷新失败", message: error.localizedDescription)
                }
            }
        }
    }

    @objc
    private func restoreSelectedRemote() {
        guard sourceMode == .remote else { return }
        guard !selectedRemoteAssetIDs.isEmpty else {
            presentAlert(title: "未选择", message: "请先选择远端资源")
            return
        }
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            presentAlert(title: "未登录", message: "请先登录 SMB")
            return
        }

        let selectedResources = remoteItems
            .filter { selectedRemoteAssetIDs.contains($0.id) }
            .flatMap { $0.resources }

        Task { [weak self] in
            guard let self else { return }
            do {
                try await self.dependencies.restoreService.restore(
                    resources: selectedResources,
                    profile: profile,
                    password: password,
                    onLog: { _ in }
                )
                await MainActor.run {
                    self.presentAlert(title: "导回完成", message: "已导回 \(self.selectedRemoteAssetIDs.count) 项")
                }
            } catch {
                await MainActor.run {
                    self.presentAlert(title: "导回失败", message: error.localizedDescription)
                }
            }
        }
    }

    private func reloadAllData() async {
        await loadLocalItems()
        loadRemoteItems()
        await MainActor.run {
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
                self.applySections()
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

            return LocalAlbumItem(
                id: asset.localIdentifier,
                asset: asset,
                creationDate: asset.creationDate ?? Date(timeIntervalSince1970: 0),
                isBackedUp: {
                    let hashes = localHashMapByAsset[asset.localIdentifier] ?? []
                    guard !hashes.isEmpty else { return false }
                    guard !remoteHashSet.isEmpty else { return false }
                    return hashes.allSatisfy { remoteHashSet.contains($0) }
                }(),
                mediaKind: mediaKind
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
            result.append(
                RemoteAlbumItem(
                    id: "live:\(groupKey)",
                    creationDate: groupResources.map(\.creationDate).min() ?? representative.creationDate,
                    resources: groupResources,
                    representative: representative,
                    mediaKind: .livePhoto,
                    pixelWidth: nil,
                    pixelHeight: nil,
                    localMirrorAssetLocalIdentifier: resolveLocalMirrorAssetID(from: groupResources)
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
                    localMirrorAssetLocalIdentifier: resolveLocalMirrorAssetID(from: [resource])
                )
            )
        }

        remoteItems = result.sorted { $0.creationDate > $1.creationDate }
    }

    private func applySections() {
        let sourceItems: [AlbumItem]
        switch sourceMode {
        case .local:
            let filtered: [LocalAlbumItem]
            switch localFilterMode {
            case .all:
                filtered = localItems
            case .unbackedOnly:
                filtered = localItems.filter { !$0.isBackedUp }
            }
            sourceItems = filtered.map { .local($0) }
        case .remote:
            sourceItems = remoteItems.map { .remote($0) }
        }
        updateSourceControlTitles()

        let grouped = Dictionary(grouping: sourceItems) { item -> YearMonth in
            let components = calendar.dateComponents([.year, .month], from: item.date)
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
                    return $0.date > $1.date
                case .ascending:
                    return $0.date < $1.date
                }
            }
            return AlbumSection(key: key, items: values)
        }

        updateFilterMenu()
        collectionView.reloadData()
    }

    private func updateSourceControlTitles() {
        sourceControl.setTitle("本地 - \(localItems.count)项", forSegmentAt: SourceMode.local.rawValue)
        sourceControl.setTitle("远端 - \(remoteItems.count)项", forSegmentAt: SourceMode.remote.rawValue)
    }

    private func bindBackupSession() {
        if let existing = backupSessionObserverID {
            backupSessionController.removeObserver(existing)
        }
        backupSessionObserverID = backupSessionController.addObserver { [weak self] snapshot in
            guard let self else { return }
            self.updateBackupButton(snapshot: snapshot)

            if snapshot.state == .running, self.sourceMode == .remote {
                self.scheduleRemoteSectionRefresh()
            }

            let previous = self.lastObservedBackupState
            self.lastObservedBackupState = snapshot.state
            if previous == .running && snapshot.state != .running {
                Task { await self.reloadAllData() }
            }
        }
    }

    private func updateBackupButton(snapshot: BackupSessionController.Snapshot) {
        var config = backupButton.configuration ?? .filled()
        config.title = snapshot.primaryActionTitle
        config.subtitle = nil
        config.baseBackgroundColor = snapshot.state.buttonColor
        config.baseForegroundColor = .white
        backupButton.configuration = config

        let canOperateRemote = snapshot.state != .running
        reloadRemoteBarButtonItem.isEnabled = canOperateRemote
        restoreBarButtonItem.isEnabled = canOperateRemote
    }

    private func scheduleRemoteSectionRefresh() {
        guard sourceMode == .remote else { return }
        guard !pendingRemoteSectionRefresh else { return }
        pendingRemoteSectionRefresh = true

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) { [weak self] in
            guard let self else { return }
            Task { [weak self] in
                guard let self else { return }
                await MainActor.run {
                    self.pendingRemoteSectionRefresh = false
                }
                guard self.sourceMode == .remote else { return }
                await self.refreshLocalHashMirrorIndex()
                await MainActor.run {
                    self.loadRemoteItems()
                    self.applySections()
                }
            }
        }
    }

    private func updateCollectionContentInsetForFloatingButton() {
        let bottomInset = backupButton.bounds.height + 24
        collectionView.contentInset.bottom = bottomInset
        collectionView.verticalScrollIndicatorInsets.bottom = bottomInset
    }

    private func updateFilterMenu() {
        let filterAll = UIAction(title: "全部", state: localFilterMode == .all ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.localFilterMode = .all
            self.applySections()
        }
        let filterUnbacked = UIAction(title: "仅未备份", state: localFilterMode == .unbackedOnly ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.localFilterMode = .unbackedOnly
            self.applySections()
        }
        let filterMenu = UIMenu(title: "筛选", children: [filterAll, filterUnbacked])

        let sortDesc = UIAction(title: "倒序", state: sortMode == .descending ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.sortMode = .descending
            self.applySections()
        }
        let sortAsc = UIAction(title: "正序", state: sortMode == .ascending ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.sortMode = .ascending
            self.applySections()
        }
        let sortMenu = UIMenu(title: "排序", children: [sortAsc, sortDesc])

        let displaySquare = UIAction(title: "正方形照片风格", state: displayOption == .square ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.displayOption = .square
            self.updateFilterMenu()
            self.collectionView.collectionViewLayout.invalidateLayout()
            self.collectionView.reloadData()
        }
        let displayOriginal = UIAction(title: "原始比例网格", state: displayOption == .originalRatio ? .on : .off) { [weak self] _ in
            guard let self else { return }
            self.displayOption = .originalRatio
            self.updateFilterMenu()
            self.collectionView.collectionViewLayout.invalidateLayout()
            self.collectionView.reloadData()
        }
        let displayMenu = UIMenu(title: "显示选项", children: [displaySquare, displayOriginal])

        filterBarButtonItem.menu = UIMenu(title: "", children: [filterMenu, sortMenu, displayMenu])
    }

    private func gridItemWidth() -> CGFloat {
        let horizontalInsets = GridLayout.itemSpacing * 2
        let width = collectionView.bounds.width - horizontalInsets - (GridLayout.columns - 1) * GridLayout.itemSpacing
        return floor(width / GridLayout.columns)
    }

    private func aspectRatio(for item: AlbumItem) -> CGFloat {
        switch item {
        case .local(let local):
            let width = max(local.asset.pixelWidth, 1)
            let height = max(local.asset.pixelHeight, 1)
            return CGFloat(height) / CGFloat(width)
        case .remote(let remote):
            guard let width = remote.pixelWidth, let height = remote.pixelHeight, width > 0, height > 0 else {
                return 1
            }
            return CGFloat(height) / CGFloat(width)
        }
    }

    private func badges(for item: AlbumItem) -> [(String, UIColor)] {
        switch item {
        case .local(let local):
            var result: [(String, UIColor)] = []
            switch local.mediaKind {
            case .livePhoto:
                result.append(("LIVE", .systemTeal))
            case .video:
                result.append(("VIDEO", .systemPurple))
            case .photo:
                result.append(("PHOTO", .systemBlue))
            }
            return result
        case .remote(let remote):
            switch remote.mediaKind {
            case .livePhoto:
                return [("LIVE", .systemTeal)]
            case .video:
                return [("VIDEO", .systemPurple)]
            case .photo:
                return [("PHOTO", .systemBlue)]
            }
        }
    }

    private func configureLocalCell(_ cell: AlbumGridCell, item: LocalAlbumItem) {
        cell.representedID = item.id
        cell.titleLabel.text = Self.dayFormatter.string(from: item.creationDate)
        cell.setBadges(badges(for: .local(item)))
        cell.setUnbacked(!item.isBackedUp)
        cell.imageView.image = UIImage(systemName: "photo")

        dependencies.photoLibraryService.requestThumbnail(for: item.asset, targetSize: CGSize(width: 400, height: 400)) { image in
            if cell.representedID == item.id {
                cell.imageView.image = image
            }
        }
    }

    private func configureRemoteCell(_ cell: AlbumGridCell, item: RemoteAlbumItem) {
        cell.representedID = item.id
        cell.titleLabel.text = Self.dayFormatter.string(from: item.creationDate)
        cell.setBadges(badges(for: .remote(item)))
        cell.setUnbacked(false)

        if selectedRemoteAssetIDs.contains(item.id) {
            cell.layer.borderWidth = 2
            cell.layer.borderColor = UIColor.systemBlue.cgColor
        } else {
            cell.layer.borderWidth = 0
            cell.layer.borderColor = UIColor.clear.cgColor
        }

        if let localAssetID = item.localMirrorAssetLocalIdentifier,
           let local = localAssetsByID[localAssetID] {
            dependencies.photoLibraryService.requestThumbnail(for: local, targetSize: CGSize(width: 400, height: 400)) { image in
                if cell.representedID == item.id {
                    cell.imageView.image = image
                }
            }
            return
        }

        if item.mediaKind == .video {
            cell.imageView.image = UIImage(systemName: "video")
            return
        }

        guard let source = makeRemoteKingfisherSource(for: item, traitCollection: cell.traitCollection) else {
            cell.imageView.image = UIImage(systemName: "photo")
            return
        }

        cell.imageView.kf.indicatorType = .activity
        cell.imageView.kf.setImage(
            with: source,
            placeholder: UIImage(systemName: "photo"),
            options: remoteKingfisherOptions(displayScale: max(cell.traitCollection.displayScale, 1))
        ) { [weak cell] result in
            if case .failure = result, cell?.representedID == item.id {
                cell?.imageView.image = UIImage(systemName: "exclamationmark.triangle")
            }
        }
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
        return [
            .targetCache(remoteImageCache),
            .memoryCacheExpiration(.seconds(600)),
            .diskCacheExpiration(.days(7)),
            .backgroundDecode,
            .scaleFactor(max(displayScale, 1)),
            .transition(.fade(0.12))
        ]
    }

    private func resolveLocalMirrorAssetID(from resources: [RemoteManifestResource]) -> String? {
        for resource in resources where ResourceTypeCode.isPhotoLike(resource.resourceType) {
            if let assetID = localAssetIdentifierByHash[resource.contentHash] {
                return assetID
            }
        }
        for resource in resources {
            if let assetID = localAssetIdentifierByHash[resource.contentHash] {
                return assetID
            }
        }
        return nil
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

extension AlbumViewController: UICollectionViewDataSource, UICollectionViewDelegate, UICollectionViewDelegateFlowLayout, UICollectionViewDataSourcePrefetching {
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
        switch item {
        case .local(let local):
            configureLocalCell(cell, item: local)
        case .remote(let remote):
            configureRemoteCell(cell, item: remote)
        }

        return cell
    }

    func collectionView(_ collectionView: UICollectionView, viewForSupplementaryElementOfKind kind: String, at indexPath: IndexPath) -> UICollectionReusableView {
        guard kind == UICollectionView.elementKindSectionHeader,
              let view = collectionView.dequeueReusableSupplementaryView(ofKind: kind, withReuseIdentifier: AlbumSectionHeaderView.reuseID, for: indexPath) as? AlbumSectionHeaderView else {
            return UICollectionReusableView()
        }

        view.setHorizontalInset(GridLayout.sectionHeaderInset)
        view.titleLabel.text = sections[indexPath.section].title
        return view
    }

    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        let item = sections[indexPath.section].items[indexPath.item]

        switch item {
        case .local(let local):
            collectionView.deselectItem(at: indexPath, animated: true)
            let detail = LocalAssetDetailViewController(dependencies: dependencies, asset: local.asset, isBackedUp: local.isBackedUp)
            detail.title = "照片详情"
            navigationController?.pushViewController(detail, animated: true)
        case .remote(let remote):
            selectedRemoteAssetIDs.insert(remote.id)
            collectionView.reloadItems(at: [indexPath])
        }
    }

    func collectionView(_ collectionView: UICollectionView, didDeselectItemAt indexPath: IndexPath) {
        let item = sections[indexPath.section].items[indexPath.item]
        guard case .remote(let remote) = item else { return }
        selectedRemoteAssetIDs.remove(remote.id)
        collectionView.reloadItems(at: [indexPath])
    }

    func collectionView(_ collectionView: UICollectionView, prefetchItemsAt indexPaths: [IndexPath]) {
        guard sourceMode == .remote else { return }

        for indexPath in indexPaths {
            guard prefetchTasks[indexPath] == nil else { continue }
            guard indexPath.section < sections.count,
                  indexPath.item < sections[indexPath.section].items.count else {
                continue
            }
            guard case .remote(let item) = sections[indexPath.section].items[indexPath.item] else {
                continue
            }
            if item.mediaKind == .video { continue }
            guard localAssetsByID[item.id] == nil else { continue }
            guard let source = makeRemoteKingfisherSource(for: item, traitCollection: collectionView.traitCollection) else {
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
        guard sourceMode == .remote else { return }
        for indexPath in indexPaths {
            prefetchTasks[indexPath]?.cancel()
            prefetchTasks[indexPath] = nil
        }
    }
}
