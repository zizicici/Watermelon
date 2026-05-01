import Photos
import SnapKit
import UIKit

@MainActor
final class LocalIndexViewController: UIViewController {
    private enum SectionID: Hashable, CaseIterable {
        case status
        case incremental
        case rebuild
    }

    private enum ItemID: Hashable {
        case statusIndexed
        case statusTotalSize
        case statusLastUpdated
        case actionIncremental
        case actionRebuild
    }

    private struct ScopeSnapshot {
        let total: Int
        let indexed: Int
        let totalSizeBytes: Int64
        let lastUpdatedAt: Date?
    }

    private final class DiffableDataSource: UITableViewDiffableDataSource<SectionID, ItemID> {
        var headerTitle: ((SectionID) -> String?)?
        var footerTitle: ((SectionID) -> String?)?

        override func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
            guard let id = sectionIdentifier(for: section) else { return nil }
            return headerTitle?(id)
        }

        override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
            guard let id = sectionIdentifier(for: section) else { return nil }
            return footerTitle?(id)
        }
    }

    private let coordinator: LocalIndexBuildCoordinator
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private var dataSource: DiffableDataSource!

    private var hasLoadedStats = false
    private var indexedCount = 0
    private var totalCount = 0
    private var totalSizeBytes: Int64 = 0
    private var lastUpdatedAt: Date?

    private var loadTask: Task<Void, Never>?
    private var coordinatorObserverID: UUID?
    private var lastObservedRunning = false

    private lazy var closeBarButtonItem = UIBarButtonItem(
        systemItem: .close,
        primaryAction: UIAction { [weak self] _ in
            self?.dismiss(animated: ConsideringUser.animated)
        }
    )
    private lazy var stopBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.stop"),
        primaryAction: UIAction { [weak self] _ in
            self?.coordinator.cancel()
        }
    )

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .short
        return f
    }()

    private static let byteFormatter: ByteCountFormatter = {
        let f = ByteCountFormatter()
        f.allowedUnits = [.useMB, .useGB, .useKB]
        f.countStyle = .file
        return f
    }()

    init(
        coordinator: LocalIndexBuildCoordinator,
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository
    ) {
        self.coordinator = coordinator
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        let coordinator = coordinator
        let observerID = coordinatorObserverID
        Task { @MainActor in
            if let observerID { coordinator.removeObserver(observerID) }
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "home.localIndex.title")
        if navigationController?.viewControllers.first === self {
            navigationItem.leftBarButtonItem = closeBarButtonItem
        }

        configureTableView()
        applyInitialSnapshot()

        lastObservedRunning = coordinator.isRunning
        coordinatorObserverID = coordinator.addObserver { [weak self] in
            self?.applyCoordinatorState()
        }
        applyCoordinatorState()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        if !coordinator.isRunning {
            reloadStats()
        }
    }

    private func configureTableView() {
        tableView.backgroundColor = .appBackground
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "value")
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "action")

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        let dataSource = DiffableDataSource(tableView: tableView) { [weak self] tableView, indexPath, item in
            self?.makeCell(for: tableView, at: indexPath, item: item) ?? UITableViewCell()
        }
        dataSource.headerTitle = { section in
            switch section {
            case .status:
                return String(localized: "home.localIndex.statsHeader")
            case .incremental, .rebuild:
                return nil
            }
        }
        dataSource.footerTitle = { section in
            switch section {
            case .status:
                return nil
            case .incremental:
                return String(localized: "home.localIndex.incrementalDetail")
            case .rebuild:
                return String(localized: "home.localIndex.rebuildDetail")
            }
        }
        self.dataSource = dataSource
    }

    private func applyInitialSnapshot() {
        var snapshot = NSDiffableDataSourceSnapshot<SectionID, ItemID>()
        snapshot.appendSections([.status, .incremental, .rebuild])
        snapshot.appendItems([.statusIndexed, .statusTotalSize, .statusLastUpdated], toSection: .status)
        snapshot.appendItems([.actionIncremental], toSection: .incremental)
        snapshot.appendItems([.actionRebuild], toSection: .rebuild)
        dataSource.apply(snapshot, animatingDifferences: false)
    }

    private func reconfigure(_ items: [ItemID]) {
        var snapshot = dataSource.snapshot()
        let valid = items.filter { snapshot.itemIdentifiers.contains($0) }
        guard !valid.isEmpty else { return }
        snapshot.reconfigureItems(valid)
        dataSource.apply(snapshot, animatingDifferences: false)
    }

    private func makeCell(for tableView: UITableView, at indexPath: IndexPath, item: ItemID) -> UITableViewCell {
        switch item {
        case .statusIndexed, .statusTotalSize, .statusLastUpdated:
            return statusCell(for: tableView, at: indexPath, item: item)
        case .actionIncremental, .actionRebuild:
            return actionCell(for: tableView, at: indexPath, item: item)
        }
    }

    private func statusCell(for tableView: UITableView, at indexPath: IndexPath, item: ItemID) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "value", for: indexPath)
        cell.selectionStyle = .none
        cell.accessoryType = .none

        var content = UIListContentConfiguration.valueCell()
        content.textProperties.color = .label
        content.secondaryTextProperties.color = .secondaryLabel
        switch item {
        case .statusIndexed:
            content.text = String(localized: "home.localIndex.indexedCount")
            content.secondaryText = formatIndexedValue()
        case .statusTotalSize:
            content.text = String(localized: "home.localIndex.totalSize")
            content.secondaryText = formatSizeValue()
        case .statusLastUpdated:
            content.text = String(localized: "home.localIndex.lastUpdated")
            content.secondaryText = formatLastUpdatedValue()
        default:
            break
        }
        cell.contentConfiguration = content
        return cell
    }

    private func actionCell(for tableView: UITableView, at indexPath: IndexPath, item: ItemID) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "action", for: indexPath)
        cell.accessoryType = .none

        var content = UIListContentConfiguration.cell()
        switch item {
        case .actionIncremental:
            content.text = String(localized: "home.localIndex.incrementalTitle")
        case .actionRebuild:
            content.text = String(localized: "home.localIndex.rebuildTitle")
        default:
            break
        }
        content.textProperties.alignment = .center
        content.textProperties.color = .tintColor
        cell.contentConfiguration = content

        let isRunning = coordinator.isRunning
        cell.selectionStyle = isRunning ? .none : .default
        cell.contentView.alpha = isRunning ? 0.4 : 1.0
        return cell
    }

    private func formatIndexedValue() -> String {
        guard hasLoadedStats || coordinator.isRunning else {
            return String(localized: "home.localIndex.never")
        }
        return String.localizedStringWithFormat(
            String(localized: "home.localIndex.indexedCountValue"),
            indexedCount,
            totalCount
        )
    }

    private func formatSizeValue() -> String {
        guard hasLoadedStats, totalSizeBytes > 0 else {
            return String(localized: "home.localIndex.never")
        }
        return Self.byteFormatter.string(fromByteCount: totalSizeBytes)
    }

    private func formatLastUpdatedValue() -> String {
        guard let lastUpdatedAt else {
            return String(localized: "home.localIndex.never")
        }
        return Self.dateFormatter.string(from: lastUpdatedAt)
    }

    private func applyCoordinatorState() {
        let nowRunning = coordinator.isRunning
        let didFinish = lastObservedRunning && !nowRunning
        let isEdge = nowRunning != lastObservedRunning
        lastObservedRunning = nowRunning

        navigationItem.rightBarButtonItem = nowRunning ? stopBarButtonItem : nil

        if let runState = coordinator.state {
            indexedCount = runState.displayedIndexed
            totalCount = runState.totalCount
            if isEdge {
                reconfigure([.statusIndexed, .actionIncremental, .actionRebuild])
            } else {
                reconfigure([.statusIndexed])
            }
        } else {
            if isEdge {
                reconfigure([.actionIncremental, .actionRebuild])
            }
            if isViewLoaded {
                reloadStats()
            }
            if didFinish, let error = coordinator.lastError {
                presentError(error)
            }
        }
    }

    private func presentError(_ error: Error) {
        let alert = UIAlertController(
            title: String(localized: "home.localIndex.error.title"),
            message: error.localizedDescription,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: ConsideringUser.animated)
    }

    private func reloadStats() {
        loadTask?.cancel()
        let repository = hashIndexRepository
        let photoLibraryService = photoLibraryService
        loadTask = Task { [weak self] in
            let snapshot = await Self.loadGlobalSnapshot(
                repository: repository,
                photoLibraryService: photoLibraryService
            )
            guard !Task.isCancelled else { return }
            self?.applyLoaded(snapshot: snapshot)
        }
    }

    private func applyLoaded(snapshot: ScopeSnapshot) {
        indexedCount = snapshot.indexed
        totalCount = snapshot.total
        totalSizeBytes = snapshot.totalSizeBytes
        lastUpdatedAt = snapshot.lastUpdatedAt
        hasLoadedStats = true
        reconfigure([.statusIndexed, .statusTotalSize, .statusLastUpdated])
    }

    private nonisolated static func loadGlobalSnapshot(
        repository: ContentHashIndexRepository,
        photoLibraryService: PhotoLibraryService
    ) async -> ScopeSnapshot {
        await withCancellableDetachedValue(priority: .userInitiated) {
            let allIDs = photoLibraryService.collectAssetIDs(query: .allAssets)
            let validRaw = (try? repository.fetchValidIndexedRows(assetIDs: allIDs)) ?? [:]

            let phAssets = photoLibraryService.fetchAssets(localIdentifiers: Set(validRaw.keys))
            var totalSize: Int64 = 0
            var indexedCount = 0
            var newest: Date?
            for asset in phAssets {
                guard let row = validRaw[asset.localIdentifier] else { continue }
                if let mtime = asset.modificationDate, mtime > row.updatedAt {
                    continue
                }
                totalSize += row.totalFileSizeBytes
                indexedCount += 1
                if let n = newest {
                    newest = max(n, row.updatedAt)
                } else {
                    newest = row.updatedAt
                }
            }
            return ScopeSnapshot(
                total: allIDs.count,
                indexed: indexedCount,
                totalSizeBytes: totalSize,
                lastUpdatedAt: newest
            )
        }
    }

    private func handleIncrementalTap() {
        guard !coordinator.isRunning, hasLoadedStats else { return }
        coordinator.start(mode: .incremental, initialIndexed: indexedCount)
    }

    private func handleRebuildTap() {
        guard !coordinator.isRunning else { return }
        let alert = UIAlertController(
            title: String(localized: "home.localIndex.confirmRebuildTitle"),
            message: String(localized: "home.localIndex.confirmRebuildMessage"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "home.localIndex.confirmRebuildAction"),
            style: .destructive
        ) { [weak self] _ in
            guard let self else { return }
            self.coordinator.start(mode: .rebuild, initialIndexed: self.indexedCount)
        })
        present(alert, animated: ConsideringUser.animated)
    }
}

extension LocalIndexViewController: UITableViewDelegate {
    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: ConsideringUser.animated)
        guard let item = dataSource.itemIdentifier(for: indexPath) else { return }
        switch item {
        case .actionIncremental:
            handleIncrementalTap()
        case .actionRebuild:
            handleRebuildTap()
        case .statusIndexed, .statusTotalSize, .statusLastUpdated:
            break
        }
    }
}
