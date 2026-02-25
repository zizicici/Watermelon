import GRDB
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
        let resources: [BackupResourceRecord]
        let representative: BackupResourceRecord
        let mediaKind: AlbumMediaKind
        let pixelWidth: Int?
        let pixelHeight: Int?
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

    private var localFilterMode: LocalFilterMode = .all
    private var sortMode: SortMode = .descending
    private var displayOption: DisplayOption = .square
    private var selectedRemoteAssetIDs = Set<String>()
    private lazy var backupSessionController = BackupSessionController(dependencies: dependencies)
    private var backupSessionObserverID: UUID?
    private var lastObservedBackupState: BackupSessionController.State = .idle

    private let remoteImageCache = NSCache<NSString, UIImage>()

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

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        navigationItem.largeTitleDisplayMode = .never

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
            collectionView.indexPathsForSelectedItems?.forEach { collectionView.deselectItem(at: $0, animated: false) }
        }
        collectionView.reloadData()
        applySections()
        updateNavigationItems()
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
                let client = try AMSMB2Client(config: SMBServerConfig(
                    host: profile.host,
                    port: profile.port,
                    shareName: profile.shareName,
                    basePath: profile.basePath,
                    username: profile.username,
                    password: password,
                    domain: profile.domain
                ))
                try await client.connect()
                defer { Task { await client.disconnect() } }

                let refreshResult = try await self.dependencies.manifestSyncService.refreshFromRemote(
                    client: client,
                    basePath: profile.basePath,
                    clearLocalWhenMissing: true
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

                    switch refreshResult {
                    case .pulled:
                        self.presentAlert(title: "已刷新", message: "已从远端重新同步索引")
                    case .remoteMissingClearedLocal:
                        self.presentAlert(title: "远端索引缺失", message: "未找到 manifest（可能尚未初始化或已被删除），已清空本地缓存索引")
                    case .remoteMissingKeptLocal:
                        self.presentAlert(title: "远端索引缺失", message: "未找到 manifest，已保留本地缓存索引。若要重建可先执行一次备份。")
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
                self.applySections()
            }
            return
        }

        let assets = dependencies.photoLibraryService.fetchAssets()
        var backedUpSet = Set<String>()
        if let set = try? dependencies.databaseManager.read({ db in
            Set(try String.fetchAll(db, sql: "SELECT DISTINCT assetLocalIdentifier FROM resources"))
        }) {
            backedUpSet = set
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
                isBackedUp: backedUpSet.contains(asset.localIdentifier),
                mediaKind: mediaKind
            )
        }

        let mapping = Dictionary(uniqueKeysWithValues: assets.map { ($0.localIdentifier, $0) })
        await MainActor.run {
            self.localItems = builtItems
            self.localAssetsByID = mapping
        }
    }

    private func loadRemoteItems() {
        guard let result = try? dependencies.databaseManager.read({ db -> [RemoteAlbumItem] in
            let assets = try BackupAssetRecord.fetchAll(db)
            let resources = try BackupResourceRecord.fetchAll(db)

            let assetMap = Dictionary(uniqueKeysWithValues: assets.map { ($0.localIdentifier, $0) })
            let grouped = Dictionary(grouping: resources, by: { $0.assetLocalIdentifier })

            return grouped.compactMap { key, values in
                guard let representative = Self.chooseRepresentativeResource(values) else { return nil }
                let creationDate = assetMap[key]?.creationDate ?? representative.backedUpAt
                let mediaKind = Self.detectMediaKind(from: values, asset: assetMap[key])
                return RemoteAlbumItem(
                    id: key,
                    creationDate: creationDate,
                    resources: values,
                    representative: representative,
                    mediaKind: mediaKind,
                    pixelWidth: assetMap[key]?.pixelWidth,
                    pixelHeight: assetMap[key]?.pixelHeight
                )
            }
        }) else {
            remoteItems = []
            return
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

            let previous = self.lastObservedBackupState
            self.lastObservedBackupState = snapshot.state
            if previous == .running && snapshot.state != .running {
                Task { await self.reloadAllData() }
            }
        }
    }

    private func updateBackupButton(snapshot: BackupSessionController.Snapshot) {
        var config = backupButton.configuration ?? .filled()
        config.title = snapshot.state.buttonTitle
        config.subtitle = nil
        config.baseBackgroundColor = snapshot.state.buttonColor
        config.baseForegroundColor = .white
        backupButton.configuration = config
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
        }

        if let local = localAssetsByID[item.id] {
            dependencies.photoLibraryService.requestThumbnail(for: local, targetSize: CGSize(width: 400, height: 400)) { image in
                if cell.representedID == item.id {
                    cell.imageView.image = image
                }
            }
            return
        }

        let cacheKey = item.representative.remoteRelativePath as NSString
        if let cached = remoteImageCache.object(forKey: cacheKey) {
            cell.imageView.image = cached
            return
        }

        cell.imageView.image = UIImage(systemName: "photo")
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            return
        }

        Task {
            do {
                let client = try AMSMB2Client(config: SMBServerConfig(
                    host: profile.host,
                    port: profile.port,
                    shareName: profile.shareName,
                    basePath: profile.basePath,
                    username: profile.username,
                    password: password,
                    domain: profile.domain
                ))
                try await client.connect()
                defer { Task { await client.disconnect() } }

                let temp = FileManager.default.temporaryDirectory.appendingPathComponent("remote_preview_\(UUID().uuidString)_\(item.representative.originalFilename)")
                let remotePath = RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: item.representative.remoteRelativePath
                )
                try await client.download(remotePath: remotePath, localURL: temp)
                let image = UIImage(contentsOfFile: temp.path)
                try? FileManager.default.removeItem(at: temp)

                guard let image else { return }
                remoteImageCache.setObject(image, forKey: cacheKey)
                await MainActor.run {
                    if cell.representedID == item.id {
                        cell.imageView.image = image
                    }
                }
            } catch {
                await MainActor.run {
                    if cell.representedID == item.id {
                        cell.imageView.image = UIImage(systemName: "exclamationmark.triangle")
                    }
                }
            }
        }
    }

    private static func chooseRepresentativeResource(_ resources: [BackupResourceRecord]) -> BackupResourceRecord? {
        let preferred = resources.first {
            $0.resourceType == "photo" || $0.resourceType == "fullSizePhoto"
        }
        return preferred ?? resources.first
    }

    private static func detectMediaKind(from resources: [BackupResourceRecord], asset: BackupAssetRecord?) -> AlbumMediaKind {
        if asset?.isLivePhoto == true {
            return .livePhoto
        }

        let hasPairedVideo = resources.contains { $0.resourceType == "pairedVideo" }
        let hasPhoto = resources.contains { $0.resourceType.contains("photo") }
        if hasPairedVideo && hasPhoto {
            return .livePhoto
        }

        let hasVideo = resources.contains {
            $0.resourceType.contains("video") || $0.originalFilename.lowercased().hasSuffix(".mov") || $0.originalFilename.lowercased().hasSuffix(".mp4")
        }
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

extension AlbumViewController: UICollectionViewDataSource, UICollectionViewDelegate, UICollectionViewDelegateFlowLayout {
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
}
