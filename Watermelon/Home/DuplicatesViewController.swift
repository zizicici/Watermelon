import Photos
import SnapKit
import UIKit

@MainActor
final class DuplicatesViewController: UIViewController {
    fileprivate struct DuplicateEntry {
        let assetLocalIdentifier: String
        let creationDate: Date?
        let mediaType: PHAssetMediaType
    }

    fileprivate enum SectionID: Hashable {
        case indexGate
        case group(fingerprint: Data)
    }

    fileprivate enum ItemID: Hashable {
        case gateEntry
        case header(fingerprint: Data)
        case entry(assetLocalIdentifier: String)
    }

    fileprivate struct EntryLocator {
        let fingerprint: Data
        let indexInGroup: Int
    }

    private struct DuplicatesData {
        let scopeTotal: Int
        let scopeIndexed: Int
        let groups: [(fingerprint: Data, entries: [DuplicateEntry])]
    }

    private final class DiffableDataSource: UITableViewDiffableDataSource<SectionID, ItemID> {
        var footerTitle: ((SectionID) -> String?)?

        override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
            guard let id = sectionIdentifier(for: section) else { return nil }
            return footerTitle?(id)
        }
    }

    private let coordinator: LocalIndexBuildCoordinator
    private let hashIndexRepository: ContentHashIndexRepository
    private let photoLibraryService: PhotoLibraryService
    private let changePublisher: LocalIndexChangePublisher

    private let summaryContainer = UIView()
    private let summaryLabel = UILabel()
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let loadingIndicator = UIActivityIndicatorView(style: .large)
    private let emptyStateView: UIView

    private var groupOrder: [Data] = []
    private var entriesByGroup: [Data: [DuplicateEntry]] = [:]
    private var keepIndexByGroup: [Data: Int] = [:]
    private var skippedGroups: Set<Data> = []
    private var locatorByEntry: [String: EntryLocator] = [:]
    private var scopeTotal = 0
    private var scopeIndexed = 0

    private var dataSource: DiffableDataSource!
    private var loadTask: Task<Void, Never>?
    private var executeTask: Task<Void, Never>?
    private var coordinatorObserverID: UUID?
    private var lastObservedRunning = false

    private var executeBarButton: UIBarButtonItem!

    private var isIndexGateVisible: Bool {
        coordinator.isRunning || scopeTotal > scopeIndexed
    }

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .short
        return f
    }()

    init(
        coordinator: LocalIndexBuildCoordinator,
        hashIndexRepository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService,
        changePublisher: LocalIndexChangePublisher
    ) {
        self.coordinator = coordinator
        self.hashIndexRepository = hashIndexRepository
        self.photoLibraryService = photoLibraryService
        self.changePublisher = changePublisher
        self.emptyStateView = makeAlbumEmptyStateView(
            title: String(localized: "home.duplicates.emptyTitle"),
            message: String(localized: "home.duplicates.emptyMessage")
        )
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        executeTask?.cancel()
        let coordinator = coordinator
        let observerID = coordinatorObserverID
        Task { @MainActor in
            if let observerID { coordinator.removeObserver(observerID) }
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "home.duplicates.title")

        if navigationController?.viewControllers.first === self {
            navigationItem.leftBarButtonItem = UIBarButtonItem(
                systemItem: .close,
                primaryAction: UIAction { [weak self] _ in
                    self?.dismiss(animated: ConsideringUser.animated)
                }
            )
        }
        executeBarButton = UIBarButtonItem(
            title: String(localized: "home.duplicates.execute"),
            primaryAction: UIAction { [weak self] _ in
                self?.executeTapped()
            }
        )
        executeBarButton.isEnabled = false
        navigationItem.rightBarButtonItem = executeBarButton

        configureUI()
        configureDataSource()

        lastObservedRunning = coordinator.isRunning
        coordinatorObserverID = coordinator.addObserver { [weak self] in
            self?.handleCoordinatorChange()
        }

        loadDuplicates()
    }

    private func handleCoordinatorChange() {
        let nowRunning = coordinator.isRunning
        let didFinish = lastObservedRunning && !nowRunning
        let didStart = !lastObservedRunning && nowRunning
        lastObservedRunning = nowRunning
        if didFinish {
            loadDuplicates()
        } else if didStart {
            applySnapshot(animatingDifferences: false)
            updateSummary()
        }
    }

    private func configureUI() {
        summaryLabel.font = .preferredFont(forTextStyle: .footnote)
        summaryLabel.adjustsFontForContentSizeCategory = true
        summaryLabel.textColor = .secondaryLabel
        summaryLabel.numberOfLines = 0
        summaryLabel.textAlignment = .center
        summaryLabel.text = String(localized: "home.duplicates.loading")

        summaryContainer.addSubview(summaryLabel)
        summaryLabel.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(UIEdgeInsets(top: 12, left: 20, bottom: 12, right: 20))
        }

        tableView.backgroundColor = .appBackground
        tableView.delegate = self
        tableView.register(DuplicateGateCell.self, forCellReuseIdentifier: DuplicateGateCell.reuseIdentifier)
        tableView.register(DuplicateGroupHeaderCell.self, forCellReuseIdentifier: DuplicateGroupHeaderCell.reuseIdentifier)
        tableView.register(DuplicateEntryCell.self, forCellReuseIdentifier: DuplicateEntryCell.reuseIdentifier)

        view.addSubview(summaryContainer)
        view.addSubview(tableView)
        view.addSubview(loadingIndicator)
        view.addSubview(emptyStateView)

        emptyStateView.isHidden = true

        summaryContainer.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide.snp.top)
            make.leading.trailing.equalToSuperview()
        }
        tableView.snp.makeConstraints { make in
            make.top.equalTo(summaryContainer.snp.bottom)
            make.leading.trailing.bottom.equalToSuperview()
        }
        loadingIndicator.snp.makeConstraints { make in
            make.center.equalTo(tableView)
        }
        emptyStateView.snp.makeConstraints { make in
            make.center.equalTo(tableView)
            make.leading.greaterThanOrEqualToSuperview().offset(24)
            make.trailing.lessThanOrEqualToSuperview().offset(-24)
        }
    }

    private func configureDataSource() {
        let ds = DiffableDataSource(tableView: tableView) { [weak self] tableView, indexPath, item in
            guard let self else { return UITableViewCell() }
            switch item {
            case .gateEntry:
                return self.dequeueGateCell(tableView: tableView, indexPath: indexPath)
            case .header(let fingerprint):
                return self.dequeueHeaderCell(tableView: tableView, indexPath: indexPath, fingerprint: fingerprint)
            case .entry(let assetID):
                return self.dequeueEntryCell(tableView: tableView, indexPath: indexPath, assetID: assetID)
            }
        }
        ds.footerTitle = { section in
            switch section {
            case .indexGate:
                return String(localized: "home.duplicates.gateExplanation")
            case .group:
                return nil
            }
        }
        dataSource = ds
    }

    private func dequeueGateCell(tableView: UITableView, indexPath: IndexPath) -> UITableViewCell {
        tableView.dequeueReusableCell(
            withIdentifier: DuplicateGateCell.reuseIdentifier,
            for: indexPath
        )
    }

    private func dequeueHeaderCell(tableView: UITableView, indexPath: IndexPath, fingerprint: Data) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(
            withIdentifier: DuplicateGroupHeaderCell.reuseIdentifier,
            for: indexPath
        ) as! DuplicateGroupHeaderCell
        let isProcessed = !skippedGroups.contains(fingerprint)
        cell.configure(isProcessed: isProcessed) { [weak self] isOn in
            self?.setGroupSkip(fingerprint, skipped: !isOn)
        }
        return cell
    }

    private func dequeueEntryCell(tableView: UITableView, indexPath: IndexPath, assetID: String) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(
            withIdentifier: DuplicateEntryCell.reuseIdentifier,
            for: indexPath
        ) as! DuplicateEntryCell
        guard let locator = locatorByEntry[assetID],
              let entries = entriesByGroup[locator.fingerprint],
              locator.indexInGroup < entries.count
        else { return cell }
        let entry = entries[locator.indexInGroup]
        let isSkipped = skippedGroups.contains(locator.fingerprint)
        let isKeep = !isSkipped && keepIndexByGroup[locator.fingerprint] == locator.indexInGroup
        cell.configure(
            assetLocalIdentifier: entry.assetLocalIdentifier,
            creationDate: entry.creationDate,
            isVideo: entry.mediaType == .video,
            isKeep: isKeep,
            isSkipped: isSkipped,
            dateFormatter: Self.dateFormatter
        )
        return cell
    }

    private func loadDuplicates() {
        loadTask?.cancel()
        loadingIndicator.startAnimating()
        emptyStateView.isHidden = true
        tableView.isHidden = true
        executeBarButton.isEnabled = false
        summaryLabel.text = String(localized: "home.duplicates.loading")

        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        loadTask = Task { [weak self] in
            let data = await Self.computeData(
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            guard !Task.isCancelled else { return }
            self?.applyData(data)
        }
    }

    private func applyData(_ data: DuplicatesData) {
        scopeTotal = data.scopeTotal
        scopeIndexed = data.scopeIndexed

        groupOrder = data.groups.map(\.fingerprint)
        var entriesByGroup: [Data: [DuplicateEntry]] = [:]
        var locatorByEntry: [String: EntryLocator] = [:]
        var keepIndexByGroup: [Data: Int] = [:]
        for (fingerprint, entries) in data.groups {
            entriesByGroup[fingerprint] = entries
            keepIndexByGroup[fingerprint] = 0
            for (index, entry) in entries.enumerated() {
                locatorByEntry[entry.assetLocalIdentifier] = EntryLocator(
                    fingerprint: fingerprint,
                    indexInGroup: index
                )
            }
        }
        self.entriesByGroup = entriesByGroup
        self.locatorByEntry = locatorByEntry
        self.keepIndexByGroup = keepIndexByGroup
        self.skippedGroups = []

        loadingIndicator.stopAnimating()
        let hasContent = !groupOrder.isEmpty || isIndexGateVisible
        tableView.isHidden = !hasContent
        emptyStateView.isHidden = hasContent

        applySnapshot(animatingDifferences: false)
        updateSummary()
    }

    private func applySnapshot(animatingDifferences: Bool) {
        var snapshot = NSDiffableDataSourceSnapshot<SectionID, ItemID>()
        if isIndexGateVisible {
            snapshot.appendSections([.indexGate])
            snapshot.appendItems([.gateEntry], toSection: .indexGate)
        }
        // Build is mutating the index; hide groups so a delete can't act on a partial snapshot.
        if !coordinator.isRunning {
            for fingerprint in groupOrder {
                guard let entries = entriesByGroup[fingerprint] else { continue }
                let section: SectionID = .group(fingerprint: fingerprint)
                snapshot.appendSections([section])
                snapshot.appendItems([.header(fingerprint: fingerprint)], toSection: section)
                snapshot.appendItems(
                    entries.map { ItemID.entry(assetLocalIdentifier: $0.assetLocalIdentifier) },
                    toSection: section
                )
            }
        }
        dataSource.apply(snapshot, animatingDifferences: animatingDifferences)
    }

    private func updateSummary() {
        if coordinator.isRunning {
            summaryLabel.text = String(localized: "home.duplicates.summaryBuilding")
            executeBarButton.isEnabled = false
            return
        }
        if groupOrder.isEmpty {
            summaryLabel.text = String(localized: "home.duplicates.summaryEmpty")
            executeBarButton.isEnabled = false
            return
        }
        let deletionCount = computeDeletionCount()
        summaryLabel.text = String.localizedStringWithFormat(
            String(localized: "home.duplicates.summary"),
            deletionCount
        )
        executeBarButton.isEnabled = deletionCount > 0
    }

    private func computeDeletionCount() -> Int {
        var total = 0
        for fingerprint in groupOrder {
            guard !skippedGroups.contains(fingerprint),
                  let entries = entriesByGroup[fingerprint]
            else { continue }
            total += max(0, entries.count - 1)
        }
        return total
    }

    private struct KeepDeletePair: Sendable {
        let keep: String
        let delete: String
    }

    private func collectKeepDeletePairs() -> [KeepDeletePair] {
        var pairs: [KeepDeletePair] = []
        for fingerprint in groupOrder {
            guard !skippedGroups.contains(fingerprint),
                  let entries = entriesByGroup[fingerprint]
            else { continue }
            let keepIndex = keepIndexByGroup[fingerprint] ?? 0
            guard keepIndex < entries.count else { continue }
            let keepID = entries[keepIndex].assetLocalIdentifier
            for (index, entry) in entries.enumerated() where index != keepIndex {
                pairs.append(KeepDeletePair(keep: keepID, delete: entry.assetLocalIdentifier))
            }
        }
        return pairs
    }

    private func setGroupSkip(_ fingerprint: Data, skipped: Bool) {
        if skipped {
            guard !skippedGroups.contains(fingerprint) else { return }
            skippedGroups.insert(fingerprint)
        } else {
            guard skippedGroups.contains(fingerprint) else { return }
            skippedGroups.remove(fingerprint)
        }
        reconfigureEntries(in: fingerprint)
        updateSummary()
    }

    private func reconfigureEntries(in fingerprint: Data) {
        var snapshot = dataSource.snapshot()
        let section: SectionID = .group(fingerprint: fingerprint)
        guard snapshot.sectionIdentifiers.contains(section) else { return }
        let entryItems = snapshot.itemIdentifiers(inSection: section).filter { item in
            if case .entry = item { return true } else { return false }
        }
        guard !entryItems.isEmpty else { return }
        snapshot.reconfigureItems(entryItems)
        dataSource.apply(snapshot, animatingDifferences: false)
    }

    private func setKeepEntry(assetID: String) {
        guard let locator = locatorByEntry[assetID] else { return }
        if skippedGroups.contains(locator.fingerprint) { return }
        if keepIndexByGroup[locator.fingerprint] == locator.indexInGroup { return }
        keepIndexByGroup[locator.fingerprint] = locator.indexInGroup
        reconfigureEntries(in: locator.fingerprint)
    }

    private func openLocalIndex() {
        let viewController = LocalIndexViewController(
            coordinator: coordinator,
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository
        )
        navigationController?.pushViewController(viewController, animated: ConsideringUser.pushAnimated)
    }

    private func executeTapped() {
        let pairs = collectKeepDeletePairs()
        guard !pairs.isEmpty else { return }

        let alert = UIAlertController(
            title: String(localized: "home.duplicates.confirmTitle"),
            message: String.localizedStringWithFormat(
                String(localized: "home.duplicates.confirmMessage"),
                pairs.count
            ),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "home.duplicates.execute"),
            style: .destructive
        ) { [weak self] _ in
            self?.performDeletion(pairs: pairs)
        })
        present(alert, animated: ConsideringUser.animated)
    }

    private func performDeletion(pairs: [KeepDeletePair]) {
        executeBarButton.isEnabled = false
        loadingIndicator.startAnimating()
        tableView.isHidden = true
        emptyStateView.isHidden = true
        summaryLabel.text = String(localized: "home.duplicates.loading")

        let photoLibraryService = photoLibraryService
        let repository = hashIndexRepository
        let publisher = changePublisher
        executeTask = Task { [weak self] in
            let stillDuplicate = await Self.revalidate(
                pairs: pairs,
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            guard stillDuplicate else {
                self?.handleStaleSnapshot()
                return
            }

            let assetIDsToDelete = pairs.map(\.delete)
            let success = await Self.deleteAssets(
                photoLibraryService: photoLibraryService,
                assetLocalIdentifiers: assetIDsToDelete
            )
            if success {
                await Self.removeIndexEntries(
                    repository: repository,
                    assetIDs: assetIDsToDelete
                )
                publisher.publish(.touched(assetIDs: Set(assetIDsToDelete)))
            }
            self?.handleDeletionResult(success: success)
        }
    }

    private func handleStaleSnapshot() {
        loadingIndicator.stopAnimating()
        let alert = UIAlertController(
            title: String(localized: "home.duplicates.staleSnapshotTitle"),
            message: String(localized: "home.duplicates.staleSnapshotMessage"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: ConsideringUser.animated)
        loadDuplicates()
    }

    private func handleDeletionResult(success: Bool) {
        loadingIndicator.stopAnimating()
        if success {
            loadDuplicates()
        } else {
            tableView.isHidden = groupOrder.isEmpty
            emptyStateView.isHidden = !groupOrder.isEmpty
            updateSummary()
            let alert = UIAlertController(
                title: String(localized: "common.error"),
                message: String(localized: "home.duplicates.deleteFailed"),
                preferredStyle: .alert
            )
            alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
            present(alert, animated: ConsideringUser.animated)
        }
    }

    private nonisolated static func computeData(
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> DuplicatesData {
        await withCancellableDetachedValue(priority: .userInitiated) {
            let allIDs = photoLibraryService.collectAssetIDs(query: .allAssets)
            let valid = (try? repository.fetchValidIndexedRows(assetIDs: allIDs)) ?? [:]

            let phAssets = photoLibraryService.fetchAssets(localIdentifiers: Set(valid.keys))
            var assetsByID: [String: PHAsset] = [:]
            var validFingerprints: [String: Data] = [:]
            for asset in phAssets {
                guard let row = valid[asset.localIdentifier] else { continue }
                if let modificationDate = asset.modificationDate,
                   modificationDate > row.updatedAt {
                    continue
                }
                assetsByID[asset.localIdentifier] = asset
                validFingerprints[asset.localIdentifier] = row.assetFingerprint
            }

            var assetIDsByFingerprint: [Data: [String]] = [:]
            for (assetID, fingerprint) in validFingerprints {
                assetIDsByFingerprint[fingerprint, default: []].append(assetID)
            }
            let duplicateIDArrays = assetIDsByFingerprint.filter { $0.value.count > 1 }

            var groups: [(fingerprint: Data, entries: [DuplicateEntry])] = []
            for (fingerprint, assetIDs) in duplicateIDArrays {
                var entries: [DuplicateEntry] = []
                for assetID in assetIDs {
                    guard let phAsset = assetsByID[assetID] else { continue }
                    entries.append(DuplicateEntry(
                        assetLocalIdentifier: assetID,
                        creationDate: phAsset.creationDate,
                        mediaType: phAsset.mediaType
                    ))
                }
                guard entries.count > 1 else { continue }
                entries.sort { lhs, rhs in
                    let lhsDate = lhs.creationDate ?? .distantFuture
                    let rhsDate = rhs.creationDate ?? .distantFuture
                    if lhsDate != rhsDate { return lhsDate < rhsDate }
                    return lhs.assetLocalIdentifier < rhs.assetLocalIdentifier
                }
                groups.append((fingerprint: fingerprint, entries: entries))
            }
            groups.sort { lhs, rhs in
                lhs.fingerprint.lexicographicallyPrecedes(rhs.fingerprint)
            }

            return DuplicatesData(
                scopeTotal: allIDs.count,
                scopeIndexed: validFingerprints.count,
                groups: groups
            )
        }
    }

    private nonisolated static func revalidate(
        pairs: [KeepDeletePair],
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> Bool {
        await withCancellableDetachedValue(priority: .userInitiated) {
            let allIDs = Set(pairs.flatMap { [$0.keep, $0.delete] })
            guard let valid = try? repository.fetchValidIndexedRows(assetIDs: allIDs) else {
                return false
            }
            let phAssets = photoLibraryService.fetchAssets(localIdentifiers: allIDs)
            var phAssetByID: [String: PHAsset] = [:]
            for asset in phAssets {
                phAssetByID[asset.localIdentifier] = asset
            }
            for pair in pairs {
                guard let keepRow = valid[pair.keep],
                      let deleteRow = valid[pair.delete],
                      let keepAsset = phAssetByID[pair.keep],
                      let deleteAsset = phAssetByID[pair.delete]
                else { return false }
                if let mtime = keepAsset.modificationDate, mtime > keepRow.updatedAt { return false }
                if let mtime = deleteAsset.modificationDate, mtime > deleteRow.updatedAt { return false }
                if keepRow.assetFingerprint != deleteRow.assetFingerprint { return false }
            }
            return true
        }
    }

    private nonisolated static func deleteAssets(
        photoLibraryService: PhotoLibraryService,
        assetLocalIdentifiers: [String]
    ) async -> Bool {
        let assets = photoLibraryService.fetchAssets(localIdentifiers: Set(assetLocalIdentifiers))
        guard !assets.isEmpty else { return true }
        return await withCheckedContinuation { continuation in
            PHPhotoLibrary.shared().performChanges {
                PHAssetChangeRequest.deleteAssets(assets as NSFastEnumeration)
            } completionHandler: { success, _ in
                continuation.resume(returning: success)
            }
        }
    }

    private nonisolated static func removeIndexEntries(
        repository: ContentHashIndexRepository,
        assetIDs: [String]
    ) async {
        await withCancellableDetachedValue(priority: .userInitiated) {
            try? repository.deleteIndexEntries(assetIDs: assetIDs)
        }
    }
}

extension DuplicatesViewController: UITableViewDelegate {
    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: ConsideringUser.animated)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        switch item {
        case .gateEntry:
            openLocalIndex()
        case .header:
            break
        case .entry(let assetID):
            setKeepEntry(assetID: assetID)
        }
    }
}

private final class DuplicateGateCell: UITableViewCell {
    static let reuseIdentifier = "DuplicateGateCell"

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: .default, reuseIdentifier: reuseIdentifier)
        accessoryType = .disclosureIndicator
        var content = UIListContentConfiguration.cell()
        content.text = String(localized: "home.duplicates.gateTitle")
        content.textProperties.color = .tintColor
        content.image = UIImage(systemName: "square.stack.3d.up")
        content.imageProperties.tintColor = .tintColor
        contentConfiguration = content
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}

private final class DuplicateGroupHeaderCell: UITableViewCell {
    static let reuseIdentifier = "DuplicateGroupHeaderCell"

    private let titleLabel = UILabel()
    private let toggleSwitch = UISwitch()
    private var onToggle: ((Bool) -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        selectionStyle = .none

        titleLabel.text = String(localized: "home.duplicates.processGroup")
        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.adjustsFontForContentSizeCategory = true
        titleLabel.textColor = .label

        toggleSwitch.addTarget(self, action: #selector(switchValueChanged), for: .valueChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(toggleSwitch)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }
        toggleSwitch.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.top.equalToSuperview().inset(10)
            make.bottom.equalToSuperview().inset(10)
            make.centerY.equalToSuperview()
            make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(8)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onToggle = nil
    }

    func configure(isProcessed: Bool, onToggle: @escaping (Bool) -> Void) {
        toggleSwitch.setOn(isProcessed, animated: false)
        self.onToggle = onToggle
    }

    @objc private func switchValueChanged() {
        onToggle?(toggleSwitch.isOn)
    }
}

private final class DuplicateEntryCell: UITableViewCell {
    static let reuseIdentifier = "DuplicateEntryCell"
    private static let thumbnailPixelSide = 56

    private let thumbnailView = UIImageView()
    private let videoBadge = UIImageView()
    private let dateLabel = UILabel()
    private let statusLabel = UILabel()
    private var thumbnailRequest: PHAssetThumbnailRequest?
    private var loadedAssetID: String?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)

        thumbnailView.contentMode = .scaleAspectFill
        thumbnailView.clipsToBounds = true
        thumbnailView.layer.cornerRadius = 6
        thumbnailView.backgroundColor = .secondarySystemBackground

        videoBadge.image = UIImage(systemName: "video.fill")
        videoBadge.tintColor = .white
        videoBadge.contentMode = .scaleAspectFit
        videoBadge.isHidden = true
        videoBadge.layer.shadowColor = UIColor.black.cgColor
        videoBadge.layer.shadowOpacity = 0.5
        videoBadge.layer.shadowRadius = 1
        videoBadge.layer.shadowOffset = CGSize(width: 0, height: 0.5)

        dateLabel.font = .preferredFont(forTextStyle: .body)
        dateLabel.adjustsFontForContentSizeCategory = true
        dateLabel.textColor = .label
        dateLabel.numberOfLines = 1

        statusLabel.font = .preferredFont(forTextStyle: .footnote).withWeight(.semibold)
        statusLabel.adjustsFontForContentSizeCategory = true
        statusLabel.textAlignment = .right
        statusLabel.setContentHuggingPriority(.required, for: .horizontal)
        statusLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        contentView.addSubview(thumbnailView)
        contentView.addSubview(videoBadge)
        contentView.addSubview(dateLabel)
        contentView.addSubview(statusLabel)

        thumbnailView.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.top.equalToSuperview().inset(10)
            make.bottom.equalToSuperview().inset(10)
            make.width.height.equalTo(Self.thumbnailPixelSide)
        }
        videoBadge.snp.makeConstraints { make in
            make.leading.equalTo(thumbnailView).offset(4)
            make.bottom.equalTo(thumbnailView).inset(4)
            make.width.height.equalTo(14)
        }
        statusLabel.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalTo(thumbnailView)
        }
        dateLabel.snp.makeConstraints { make in
            make.leading.equalTo(thumbnailView.snp.trailing).offset(12)
            make.centerY.equalTo(thumbnailView)
            make.trailing.lessThanOrEqualTo(statusLabel.snp.leading).offset(-8)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        thumbnailRequest?.cancel()
        thumbnailRequest = nil
        thumbnailView.image = nil
        videoBadge.isHidden = true
        loadedAssetID = nil
        contentView.alpha = 1.0
    }

    func configure(
        assetLocalIdentifier: String,
        creationDate: Date?,
        isVideo: Bool,
        isKeep: Bool,
        isSkipped: Bool,
        dateFormatter: DateFormatter
    ) {
        if let creationDate {
            dateLabel.text = dateFormatter.string(from: creationDate)
        } else {
            dateLabel.text = String(localized: "home.duplicates.unknownDate")
        }
        videoBadge.isHidden = !isVideo

        if isSkipped {
            statusLabel.text = nil
            statusLabel.isHidden = true
        } else {
            statusLabel.isHidden = false
            if isKeep {
                statusLabel.text = String(localized: "home.duplicates.statusKeep")
                statusLabel.textColor = .systemGreen
            } else {
                statusLabel.text = String(localized: "home.duplicates.statusDelete")
                statusLabel.textColor = .systemRed
            }
        }
        contentView.alpha = isSkipped ? 0.4 : 1.0
        accessoryType = .none

        if loadedAssetID != assetLocalIdentifier {
            thumbnailRequest?.cancel()
            thumbnailView.image = nil
            loadedAssetID = assetLocalIdentifier
            let scale = window?.screen.scale ?? UIScreen.main.scale
            thumbnailRequest = PHAssetThumbnailLoader.setImage(
                assetLocalIdentifier: assetLocalIdentifier,
                pixelSide: Self.thumbnailPixelSide * Int(scale),
                on: thumbnailView,
                fadeDuration: 0.15
            )
        }
    }
}
