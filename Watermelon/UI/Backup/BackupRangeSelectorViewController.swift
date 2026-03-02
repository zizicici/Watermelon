import Photos
import SnapKit
import UIKit

@MainActor
final class BackupRangeSelectorViewController: UIViewController {
    private struct MonthKey: Hashable, Comparable {
        let year: Int
        let month: Int

        var text: String {
            "\(year)年\(String(format: "%02d", month))月"
        }

        static func < (lhs: MonthKey, rhs: MonthKey) -> Bool {
            if lhs.year == rhs.year {
                return lhs.month < rhs.month
            }
            return lhs.year < rhs.year
        }
    }

    private struct AssetNode {
        let asset: PHAsset
        let assetID: String
        var bytes: Int64?
        let creationDate: Date?
        let mediaKind: ScopeMediaKind
    }

    private enum ScopeMediaKind {
        case photo
        case video
        case livePhoto

        var badgeText: String {
            switch self {
            case .photo: return "PHOTO"
            case .video: return "VIDEO"
            case .livePhoto: return "LIVE"
            }
        }
    }

    private struct MonthNode {
        let key: MonthKey
        var assets: [AssetNode]
        var expanded: Bool
    }

    private struct LoadPayload {
        let buckets: [MonthKey: [AssetNode]]
        let allAssetIDs: [String]
        let bytesByID: [String: Int64]
        let totalBytes: Int64?
    }

    private let dependencies: DependencyContainer
    private let initialSelection: BackupScopeSelection
    private let readOnly: Bool
    private let onApply: (BackupScopeSelection) -> Void

    private let collectionView: UICollectionView
    private let imageManager = PHCachingImageManager()
    private let thumbnailCache = NSCache<NSString, UIImage>()

    private var months: [MonthNode] = []
    private var allAssetIDSet: Set<String> = []
    private var assetBytesByID: [String: Int64] = [:]
    private var pendingSizeAssetIDs: Set<String> = []
    private var selectedAssetIDs: Set<String> = []
    private var totalAssetCount = 0
    private var totalBytes: Int64?
    private var thumbnailRequestIDs: [String: PHImageRequestID] = [:]
    private var loadingTask: Task<Void, Never>?

    private static let headerKind = UICollectionView.elementKindSectionHeader

    init(
        dependencies: DependencyContainer,
        initialSelection: BackupScopeSelection,
        readOnly: Bool,
        onApply: @escaping (BackupScopeSelection) -> Void
    ) {
        self.dependencies = dependencies
        self.initialSelection = initialSelection
        self.readOnly = readOnly
        self.onApply = onApply

        let layout = UICollectionViewFlowLayout()
        layout.minimumLineSpacing = 4
        layout.minimumInteritemSpacing = 4
        layout.sectionInset = UIEdgeInsets(top: 8, left: 4, bottom: 8, right: 4)
        layout.headerReferenceSize = CGSize(width: 1, height: 48)
        collectionView = UICollectionView(frame: .zero, collectionViewLayout: layout)

        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadingTask?.cancel()
        for requestID in thumbnailRequestIDs.values {
            imageManager.cancelImageRequest(requestID)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "选择备份范围"

        configureNavigationBar()
        configureCollectionView()
        configureToolbar()
        updateTitleWithSelectedCapacity()

        loadingTask = Task { [weak self] in
            await self?.loadData()
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if isMovingFromParent || isBeingDismissed {
            navigationController?.setToolbarHidden(true, animated: false)
        }
        if isMovingFromParent || isBeingDismissed {
            for requestID in thumbnailRequestIDs.values {
                imageManager.cancelImageRequest(requestID)
            }
            thumbnailRequestIDs.removeAll()
        }
    }

    private func configureNavigationBar() {
        let doneStyle: UIBarButtonItem.Style
        if #available(iOS 26.0, *) {
            doneStyle = .prominent
        } else {
            doneStyle = .done
        }
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            title: "取消",
            style: .plain,
            target: self,
            action: #selector(cancelTapped)
        )
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            title: readOnly ? "完成" : "应用",
            style: doneStyle,
            target: self,
            action: #selector(doneTapped)
        )
    }

    private func configureCollectionView() {
        collectionView.backgroundColor = .clear
        collectionView.alwaysBounceVertical = true
        collectionView.register(AlbumGridCell.self, forCellWithReuseIdentifier: AlbumGridCell.reuseID)
        collectionView.register(
            BackupRangeMonthHeaderView.self,
            forSupplementaryViewOfKind: Self.headerKind,
            withReuseIdentifier: BackupRangeMonthHeaderView.reuseID
        )
        collectionView.dataSource = self
        collectionView.delegate = self
        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
    }

    private func configureToolbar() {
        guard !readOnly else {
            navigationController?.setToolbarHidden(true, animated: false)
            toolbarItems = nil
            return
        }
        navigationController?.setToolbarHidden(false, animated: false)
        let selectAll = UIBarButtonItem(
            title: "全选",
            style: .plain,
            target: self,
            action: #selector(selectAllTapped)
        )
        let selectNone = UIBarButtonItem(
            title: "全不选",
            style: .plain,
            target: self,
            action: #selector(selectNoneTapped)
        )
        toolbarItems = [
            selectAll,
            UIBarButtonItem.flexibleSpace(),
            selectNone
        ]
    }

    @objc
    private func cancelTapped() {
        dismiss(animated: true)
    }

    @objc
    private func doneTapped() {
        let selectedBytes = selectedBytesIfAvailable()
        let selection: BackupScopeSelection
        if selectedAssetIDs.count == totalAssetCount {
            selection = BackupScopeSelection(
                selectedAssetIDs: nil,
                selectedAssetCount: totalAssetCount,
                selectedEstimatedBytes: totalBytes,
                totalAssetCount: totalAssetCount,
                totalEstimatedBytes: totalBytes
            )
        } else {
            selection = BackupScopeSelection(
                selectedAssetIDs: selectedAssetIDs,
                selectedAssetCount: selectedAssetIDs.count,
                selectedEstimatedBytes: selectedBytes,
                totalAssetCount: totalAssetCount,
                totalEstimatedBytes: totalBytes
            )
        }
        onApply(selection)
        dismiss(animated: true)
    }

    @objc
    private func selectAllTapped() {
        guard !readOnly else { return }
        applyBatchSelection(selectAll: true)
    }

    @objc
    private func selectNoneTapped() {
        guard !readOnly else { return }
        applyBatchSelection(selectAll: false)
    }

    private func applyBatchSelection(selectAll: Bool) {
        let isCurrentlyAll = selectedAssetIDs.count == totalAssetCount
        let isCurrentlyNone = selectedAssetIDs.isEmpty
        let isEasyFlip = (isCurrentlyAll && !selectAll) || (isCurrentlyNone && selectAll)

        let apply: () -> Void = { [weak self] in
            guard let self else { return }
            if selectAll {
                self.selectedAssetIDs = Set(self.months.flatMap { $0.assets.map(\.assetID) })
            } else {
                self.selectedAssetIDs.removeAll()
            }
            self.updateTitleWithSelectedCapacity()
            self.collectionView.reloadData()
        }

        if isEasyFlip {
            apply()
            return
        }

        let actionTitle = selectAll ? "全选" : "全不选"
        let alert = UIAlertController(
            title: "确认\(actionTitle)",
            message: "将修改当前选择范围，是否继续？",
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "确认", style: .destructive, handler: { _ in
            apply()
        }))
        present(alert, animated: true)
    }

    private func loadData() async {
        let authStatus = dependencies.photoLibraryService.authorizationStatus()
        let authorized: Bool
        if authStatus == .authorized || authStatus == .limited {
            authorized = true
        } else {
            let requested = await dependencies.photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }
        guard authorized else {
            presentAlert(title: "无访问权限", message: "请在系统设置中允许访问相册。")
            return
        }

        let payload: LoadPayload
        do {
            payload = try await Self.buildLoadPayload(
                photoLibraryService: dependencies.photoLibraryService,
                hashIndexRepository: dependencies.hashIndexRepository
            )
        } catch is CancellationError {
            return
        } catch {
            presentAlert(title: "加载失败", message: "读取备份范围失败：\(error.localizedDescription)")
            return
        }
        if Task.isCancelled { return }

        applyBuckets(
            buckets: payload.buckets,
            allAssetIDs: payload.allAssetIDs,
            bytesByID: payload.bytesByID,
            totalBytes: payload.totalBytes
        )
    }

    private func applyBuckets(
        buckets: [MonthKey: [AssetNode]],
        allAssetIDs: [String],
        bytesByID: [String: Int64],
        totalBytes: Int64?
    ) {
        pendingSizeAssetIDs.removeAll()
        let orderedKeys = buckets.keys.sorted(by: >)
        months = orderedKeys.map { key in
            MonthNode(
                key: key,
                assets: buckets[key] ?? [],
                expanded: false
            )
        }
        totalAssetCount = allAssetIDs.count
        self.totalBytes = totalBytes
        allAssetIDSet = Set(allAssetIDs)
        assetBytesByID = bytesByID

        if let scoped = initialSelection.selectedAssetIDs {
            selectedAssetIDs = scoped.intersection(allAssetIDSet)
        } else {
            selectedAssetIDs = allAssetIDSet
        }

        updateTitleWithSelectedCapacity()
        collectionView.reloadData()
    }

    private func updateTitleWithSelectedCapacity() {
        if let selectedBytes = selectedBytesIfAvailable() {
            title = "已选 \(Self.byteCountFormatter.string(fromByteCount: selectedBytes))"
        } else {
            title = "已选 \(selectedAssetIDs.count) 张"
        }
    }

    private func selectedBytesIfAvailable() -> Int64? {
        guard !selectedAssetIDs.isEmpty else { return 0 }
        var total: Int64 = 0
        for assetID in selectedAssetIDs {
            guard let bytes = assetBytesByID[assetID] else {
                return nil
            }
            total += bytes
        }
        return total
    }

    private func monthSelectionState(_ month: MonthNode) -> BackupRangeMonthHeaderView.SelectionState {
        let selectedCount = month.assets.reduce(into: 0) { result, node in
            if selectedAssetIDs.contains(node.assetID) {
                result += 1
            }
        }
        if selectedCount == 0 { return .none }
        if selectedCount == month.assets.count { return .all }
        return .partial
    }

    private func monthSummary(for month: MonthNode) -> BackupRangeMonthHeaderView.Summary {
        let totalCount = month.assets.count
        let monthBytes: Int64? = {
            var total: Int64 = 0
            for node in month.assets {
                guard let bytes = node.bytes else { return nil }
                total += bytes
            }
            return total
        }()
        let selectedCount = month.assets.reduce(into: 0) { result, node in
            if selectedAssetIDs.contains(node.assetID) {
                result += 1
            }
        }
        let selectedBytes: Int64? = {
            var total: Int64 = 0
            for node in month.assets where selectedAssetIDs.contains(node.assetID) {
                guard let bytes = node.bytes else { return nil }
                total += bytes
            }
            return total
        }()

        let countText = "\(selectedCount)/\(totalCount)"
        let bytesText: String = {
            guard let selectedBytes, let monthBytes else { return "--/--" }
            return "\(Self.byteCountFormatter.string(fromByteCount: selectedBytes))/\(Self.byteCountFormatter.string(fromByteCount: monthBytes))"
        }()
        return BackupRangeMonthHeaderView.Summary(
            title: month.key.text,
            countText: countText,
            sizeText: bytesText
        )
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }

    private static let byteCountFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    private static let monthDayFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "MM-dd"
        return formatter
    }()

    private nonisolated static func mediaKind(for asset: PHAsset) -> ScopeMediaKind {
        if PhotoLibraryService.isLivePhoto(asset) {
            return .livePhoto
        }
        if asset.mediaType == .video {
            return .video
        }
        return .photo
    }

    private nonisolated static func buildLoadPayload(
        photoLibraryService: PhotoLibraryServiceProtocol,
        hashIndexRepository: ContentHashIndexRepositoryProtocol
    ) async throws -> LoadPayload {
        let workerTask = Task.detached(priority: .userInitiated) { () throws -> LoadPayload in
            let assetsResult = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
            let cachedSizesByAssetID: [String: Int64] = {
                guard let caches = try? hashIndexRepository.fetchAssetHashCaches() else { return [:] }
                var result: [String: Int64] = [:]
                result.reserveCapacity(caches.count)
                for (assetID, cache) in caches {
                    result[assetID] = cache.totalFileSizeBytes
                }
                return result
            }()

            var buckets: [MonthKey: [AssetNode]] = [:]
            var allAssetIDs: [String] = []
            var bytesByID: [String: Int64] = [:]
            bytesByID.reserveCapacity(cachedSizesByAssetID.count)

            for index in 0 ..< assetsResult.count {
                if index % 200 == 0 {
                    try Task.checkCancellation()
                }
                let asset = assetsResult.object(at: index)
                let date = asset.creationDate ?? Date(timeIntervalSince1970: 0)
                let components = Calendar.current.dateComponents([.year, .month], from: date)
                let monthKey = MonthKey(year: components.year ?? 1970, month: components.month ?? 1)
                allAssetIDs.append(asset.localIdentifier)
                let mediaKind = Self.mediaKind(for: asset)
                let bytes = cachedSizesByAssetID[asset.localIdentifier]
                if let bytes {
                    bytesByID[asset.localIdentifier] = bytes
                }

                let node = AssetNode(
                    asset: asset,
                    assetID: asset.localIdentifier,
                    bytes: bytes,
                    creationDate: asset.creationDate,
                    mediaKind: mediaKind
                )
                buckets[monthKey, default: []].append(node)
            }

            try Task.checkCancellation()
            let totalBytes: Int64? = {
                guard allAssetIDs.count == bytesByID.count else { return nil }
                return allAssetIDs.reduce(Int64(0)) { partial, assetID in
                    partial + max(bytesByID[assetID] ?? 0, 0)
                }
            }()

            return LoadPayload(
                buckets: buckets,
                allAssetIDs: allAssetIDs,
                bytesByID: bytesByID,
                totalBytes: totalBytes
            )
        }

        return try await withTaskCancellationHandler {
            try await workerTask.value
        } onCancel: {
            workerTask.cancel()
        }
    }
}

extension BackupRangeSelectorViewController: UICollectionViewDataSource, UICollectionViewDelegateFlowLayout {
    func numberOfSections(in collectionView: UICollectionView) -> Int {
        months.count
    }

    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        guard section < months.count else { return 0 }
        return months[section].expanded ? months[section].assets.count : 0
    }

    func collectionView(
        _ collectionView: UICollectionView,
        cellForItemAt indexPath: IndexPath
    ) -> UICollectionViewCell {
        let cell = collectionView.dequeueReusableCell(withReuseIdentifier: AlbumGridCell.reuseID, for: indexPath)
        guard let cell = cell as? AlbumGridCell,
              indexPath.section < months.count,
              indexPath.item < months[indexPath.section].assets.count else {
            return cell
        }

        let item = months[indexPath.section].assets[indexPath.item]
        let selected = selectedAssetIDs.contains(item.assetID)
        cell.representedID = item.assetID
        cell.titleLabel.text = item.creationDate.map { Self.monthDayFormatter.string(from: $0) } ?? "--"
        let sizeText = item.bytes.map { Self.byteCountFormatter.string(fromByteCount: $0) } ?? "..."
        cell.setBadges([(sizeText, UIColor.black.withAlphaComponent(0.45))])
        cell.setTopRightBadge(nil)
        cell.setBottomBadges([(item.mediaKind.badgeText, .clear)])
        cell.setSelectionMarked(selected, editable: !readOnly)
        resolveAssetSizeIfNeeded(for: item)

        if let image = thumbnailCache.object(forKey: item.assetID as NSString) {
            cell.imageView.contentMode = .scaleAspectFill
            cell.imageView.tintColor = nil
            cell.imageView.image = image
        } else {
            applyLoadingPlaceholder(to: cell.imageView, symbolName: "photo")
            requestThumbnail(for: item)
        }
        return cell
    }

    func collectionView(
        _ collectionView: UICollectionView,
        didSelectItemAt indexPath: IndexPath
    ) {
        guard !readOnly else { return }
        guard indexPath.section < months.count,
              indexPath.item < months[indexPath.section].assets.count else {
            return
        }

        let item = months[indexPath.section].assets[indexPath.item]
        if selectedAssetIDs.contains(item.assetID) {
            selectedAssetIDs.remove(item.assetID)
        } else {
            selectedAssetIDs.insert(item.assetID)
        }
        updateTitleWithSelectedCapacity()
        collectionView.reloadSections(IndexSet(integer: indexPath.section))
    }

    func collectionView(
        _ collectionView: UICollectionView,
        viewForSupplementaryElementOfKind kind: String,
        at indexPath: IndexPath
    ) -> UICollectionReusableView {
        let view = collectionView.dequeueReusableSupplementaryView(
            ofKind: kind,
            withReuseIdentifier: BackupRangeMonthHeaderView.reuseID,
            for: indexPath
        )
        guard let header = view as? BackupRangeMonthHeaderView,
              indexPath.section < months.count else {
            return view
        }

        let month = months[indexPath.section]
        header.apply(
            summary: monthSummary(for: month),
            state: monthSelectionState(month),
            expanded: month.expanded,
            editable: !readOnly
        )
        header.onToggleSelect = { [weak self] in
            guard let self else { return }
            guard !self.readOnly else { return }
            let section = indexPath.section
            guard section < self.months.count else { return }

            let month = self.months[section]
            let state = self.monthSelectionState(month)
            switch state {
            case .none, .partial:
                month.assets.forEach { self.selectedAssetIDs.insert($0.assetID) }
            case .all:
                month.assets.forEach { self.selectedAssetIDs.remove($0.assetID) }
            }
            self.updateTitleWithSelectedCapacity()
            self.collectionView.reloadSections(IndexSet(integer: section))
        }
        header.onToggleExpanded = { [weak self] in
            guard let self else { return }
            let section = indexPath.section
            guard section < self.months.count else { return }
            self.months[section].expanded.toggle()
            if self.months[section].expanded {
                for asset in self.months[section].assets.prefix(48) {
                    if self.thumbnailCache.object(forKey: asset.assetID as NSString) == nil {
                        self.requestThumbnail(for: asset)
                    }
                }
            }
            self.collectionView.performBatchUpdates({
                self.collectionView.reloadSections(IndexSet(integer: section))
            })
        }
        return header
    }

    func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        sizeForItemAt indexPath: IndexPath
    ) -> CGSize {
        let contentWidth = collectionView.bounds.width - 8
        let spacing: CGFloat = 4
        let columns: CGFloat = 4
        let itemWidth = floor((contentWidth - (columns - 1) * spacing) / columns)
        return CGSize(width: itemWidth, height: itemWidth)
    }

    func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        referenceSizeForHeaderInSection section: Int
    ) -> CGSize {
        CGSize(width: collectionView.bounds.width, height: 48)
    }
}

private final class BackupRangeMonthHeaderView: UICollectionReusableView {
    enum SelectionState {
        case none
        case partial
        case all
    }

    struct Summary {
        let title: String
        let countText: String
        let sizeText: String
    }

    static let reuseID = "range_month_header"

    private let monthButton = UIButton(type: .system)
    private let selectButton = UIButton(type: .system)
    private let countLabel = UILabel()
    private let sizeLabel = UILabel()

    var onToggleSelect: (() -> Void)?
    var onToggleExpanded: (() -> Void)?

    override init(frame: CGRect) {
        super.init(frame: frame)
        buildUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func apply(
        summary: Summary,
        state: SelectionState,
        expanded: Bool,
        editable: Bool
    ) {
        setMonthButtonTitle(summary.title)
        countLabel.text = summary.countText
        sizeLabel.text = summary.sizeText

        updateSelectButtonImage(for: state)
        selectButton.isEnabled = editable
        selectButton.alpha = editable ? 1 : 0.45

        let symbolName = expanded
            ? "chevron.down"
            : "chevron.up"
        setMonthButtonImage(symbolName)
    }

    private func buildUI() {
        configureMonthButton()
        configureSelectButton()
        monthButton.addTarget(self, action: #selector(toggleMonthTapped), for: .touchUpInside)

        countLabel.font = .systemFont(ofSize: 12, weight: .semibold)
        sizeLabel.font = .systemFont(ofSize: 11, weight: .regular)
        countLabel.textColor = .label
        sizeLabel.textColor = .secondaryLabel
        countLabel.textAlignment = .right
        sizeLabel.textAlignment = .right

        selectButton.addTarget(self, action: #selector(toggleSelectTapped), for: .touchUpInside)

        addSubview(monthButton)
        addSubview(countLabel)
        addSubview(sizeLabel)
        addSubview(selectButton)

        monthButton.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(10)
            make.centerY.equalToSuperview()
            make.height.equalTo(40)
            make.width.greaterThanOrEqualTo(118)
        }

        selectButton.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(12)
            make.centerY.equalToSuperview()
            make.size.equalTo(CGSize(width: 32, height: 32))
        }

        countLabel.snp.makeConstraints { make in
            make.trailing.equalTo(selectButton.snp.leading).offset(-8)
            make.leading.greaterThanOrEqualTo(monthButton.snp.trailing).offset(10)
            make.bottom.equalTo(self.snp.centerY).offset(-1)
        }

        sizeLabel.snp.makeConstraints { make in
            make.trailing.equalTo(countLabel)
            make.leading.greaterThanOrEqualTo(monthButton.snp.trailing).offset(10)
            make.top.equalTo(self.snp.centerY).offset(1)
        }
    }

    private func configureMonthButton() {
        if #available(iOS 15.0, *) {
            var config = UIButton.Configuration.filled()
            config.baseBackgroundColor = .secondarySystemBackground
            config.baseForegroundColor = .label
            config.cornerStyle = .large
            config.contentInsets = NSDirectionalEdgeInsets(top: 8, leading: 14, bottom: 8, trailing: 10)
            config.imagePadding = 6
            config.imagePlacement = .trailing
            config.preferredSymbolConfigurationForImage = UIImage.SymbolConfiguration(pointSize: 11, weight: .semibold)
            config.titleTextAttributesTransformer = UIConfigurationTextAttributesTransformer { incoming in
                var attrs = incoming
                attrs.font = .systemFont(ofSize: 16, weight: .semibold)
                return attrs
            }
            monthButton.configuration = config
        } else {
            monthButton.backgroundColor = .secondarySystemBackground
            monthButton.setTitleColor(.label, for: .normal)
            monthButton.titleLabel?.font = .systemFont(ofSize: 16, weight: .semibold)
            monthButton.layer.cornerRadius = 20
            monthButton.contentEdgeInsets = UIEdgeInsets(top: 8, left: 14, bottom: 8, right: 10)
            monthButton.semanticContentAttribute = .forceRightToLeft
            monthButton.imageEdgeInsets = UIEdgeInsets(top: 0, left: 8, bottom: 0, right: -8)
            monthButton.setPreferredSymbolConfiguration(
                UIImage.SymbolConfiguration(pointSize: 11, weight: .semibold),
                forImageIn: .normal
            )
        }
        monthButton.clipsToBounds = true
    }

    private func configureSelectButton() {
        if #available(iOS 15.0, *) {
            var config = UIButton.Configuration.plain()
            config.contentInsets = .zero
            config.preferredSymbolConfigurationForImage = UIImage.SymbolConfiguration(pointSize: 20, weight: .semibold)
            selectButton.configuration = config
        } else {
            selectButton.setPreferredSymbolConfiguration(
                UIImage.SymbolConfiguration(pointSize: 20, weight: .semibold),
                forImageIn: .normal
            )
            selectButton.tintColor = .systemGreen
        }
    }

    private func setMonthButtonTitle(_ title: String) {
        if #available(iOS 15.0, *) {
            var config = monthButton.configuration ?? .filled()
            config.title = title
            monthButton.configuration = config
        } else {
            monthButton.setTitle(title, for: .normal)
        }
    }

    private func setMonthButtonImage(_ symbolName: String) {
        let image = UIImage(systemName: symbolName)
        if #available(iOS 15.0, *) {
            var config = monthButton.configuration ?? .filled()
            config.image = image
            monthButton.configuration = config
        } else {
            monthButton.setImage(image, for: .normal)
        }
    }

    private func updateSelectButtonImage(for state: SelectionState) {
        let image: UIImage?
        switch state {
        case .none:
            image = UIImage(systemName: "circle")?
                .withTintColor(.systemGreen, renderingMode: .alwaysOriginal)
        case .partial:
            image = filledSelectionImage(symbolName: "minus.circle.fill")
        case .all:
            image = filledSelectionImage(symbolName: "checkmark.circle.fill")
        }

        if #available(iOS 15.0, *) {
            var config = selectButton.configuration ?? .plain()
            config.image = image
            config.baseForegroundColor = nil
            selectButton.configuration = config
        } else {
            selectButton.setImage(image, for: .normal)
            selectButton.tintColor = nil
        }
    }

    private func filledSelectionImage(symbolName: String) -> UIImage? {
        return UIImage(systemName: symbolName)?
            .withTintColor(.systemGreen, renderingMode: .alwaysOriginal)
    }

    @objc
    private func toggleSelectTapped() {
        onToggleSelect?()
    }

    @objc
    private func toggleMonthTapped() {
        onToggleExpanded?()
    }
}

private extension BackupRangeSelectorViewController {
    private func resolveAssetSizeIfNeeded(for item: AssetNode) {
        if assetBytesByID[item.assetID] != nil { return }
        if pendingSizeAssetIDs.contains(item.assetID) { return }

        let assetID = item.assetID
        let asset = item.asset
        pendingSizeAssetIDs.insert(assetID)

        DispatchQueue.global(qos: .utility).async { [weak self] in
            var totalSize: Int64 = 0
            let resources = PHAssetResource.assetResources(for: asset)
            for resource in resources {
                totalSize += max(PhotoLibraryService.resourceFileSize(resource), 0)
            }

            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.pendingSizeAssetIDs.remove(assetID)
                self.assetBytesByID[assetID] = totalSize

                var changedSection: Int?
                var changedItemIndex: Int?
                for sectionIndex in self.months.indices {
                    if let itemIndex = self.months[sectionIndex].assets.firstIndex(where: { $0.assetID == assetID }) {
                        self.months[sectionIndex].assets[itemIndex].bytes = totalSize
                        changedSection = sectionIndex
                        changedItemIndex = itemIndex
                        break
                    }
                }

                guard let changedSection else { return }
                self.updateTitleWithSelectedCapacity()
                let headerIndexPath = IndexPath(item: 0, section: changedSection)
                if let header = self.collectionView.supplementaryView(
                    forElementKind: Self.headerKind,
                    at: headerIndexPath
                ) as? BackupRangeMonthHeaderView {
                    let month = self.months[changedSection]
                    header.apply(
                        summary: self.monthSummary(for: month),
                        state: self.monthSelectionState(month),
                        expanded: month.expanded,
                        editable: !self.readOnly
                    )
                }
                if let changedItemIndex {
                    let changedIndexPath = IndexPath(item: changedItemIndex, section: changedSection)
                    if self.collectionView.indexPathsForVisibleItems.contains(changedIndexPath) {
                        self.collectionView.reloadItems(at: [changedIndexPath])
                    }
                }
            }
        }
    }

    private func requestThumbnail(for item: AssetNode) {
        let key = item.assetID as NSString
        if thumbnailRequestIDs[item.assetID] != nil { return }

        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        let requestID = imageManager.requestImage(
            for: item.asset,
            targetSize: CGSize(width: 400, height: 400),
            contentMode: .aspectFill,
            options: options
        ) { [weak self] image, info in
            guard let self else { return }
            if (info?[PHImageCancelledKey] as? Bool) == true {
                self.thumbnailRequestIDs[item.assetID] = nil
                return
            }
            if (info?[PHImageResultIsDegradedKey] as? Bool) == true {
                return
            }
            self.thumbnailRequestIDs[item.assetID] = nil
            guard let image else { return }
            self.thumbnailCache.setObject(image, forKey: key)
            for case let visibleCell as AlbumGridCell in self.collectionView.visibleCells
            where visibleCell.representedID == item.assetID {
                visibleCell.imageView.contentMode = .scaleAspectFill
                visibleCell.imageView.tintColor = nil
                visibleCell.imageView.image = image
            }
        }
        thumbnailRequestIDs[item.assetID] = requestID
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
}
