import Foundation
import MoreKit
import UIKit

final class StorageUsageViewController: UIViewController {
    private typealias Category = AppCacheManager.Category
    private let category: Category?
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private var usage: [Category: AppCacheManager.Usage] = [:]
    private var appUsage: AppCacheManager.Usage?
    private var dataUsage: AppCacheManager.Usage?
    private var dataEntries: [AppCacheManager.DataEntry] = []
    private var refreshID = UUID()
    private var refreshTask: Task<Void, Never>?
    private var isClearing = false

    init(category: AppCacheManager.Category? = nil) {
        self.category = category
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit { refreshTask?.cancel() }

    private enum Row {
        case size, clearableSize, limit, clear
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = category.map { categoryTitle($0) } ?? String(localized: "more.item.storageUsage")
        view.backgroundColor = .appBackground
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "Cell")
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "DataDetailCell")
        tableView.refreshControl = UIRefreshControl()
        tableView.refreshControl?.addTarget(self, action: #selector(refreshSizes), for: .valueChanged)
        view.addSubview(tableView)
        tableView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.topAnchor),
            tableView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        refreshSizes()
    }

    @objc private func refreshSizes() {
        guard !isClearing else { return }
        let id = UUID()
        refreshTask?.cancel()
        refreshID = id
        usage = [:]
        appUsage = nil
        dataUsage = nil
        dataEntries = []
        tableView.reloadData()
        if let category, !category.allowsClearing {
            refreshTask = Task { [weak self] in
                let result = await withCancellableDetachedValue(priority: .utility) {
                    category == .localData ? AppCacheManager.shared.localDataDetails() : AppCacheManager.shared.otherDataDetails()
                }
                guard !Task.isCancelled, let self, self.refreshID == id else { return }
                self.usage[category] = result.total
                self.dataEntries = result.entries
                self.tableView.refreshControl?.endRefreshing()
                self.tableView.reloadData()
            }
        } else if let category {
            refreshTask = Task { [weak self] in
                let result = await withCancellableDetachedValue(priority: .utility) { AppCacheManager.shared.usage(for: category) }
                guard !Task.isCancelled, let self, self.refreshID == id else { return }
                self.usage[category] = result
                self.tableView.refreshControl?.endRefreshing()
                self.tableView.reloadData()
            }
        } else {
            refreshTask = Task { [weak self] in
                let result = await withCancellableDetachedValue(priority: .utility) { AppCacheManager.shared.snapshot() }
                guard !Task.isCancelled, let self, self.refreshID == id else { return }
                self.usage = result.categories
                self.appUsage = result.app
                self.dataUsage = result.data
                self.tableView.refreshControl?.endRefreshing()
                self.tableView.reloadData()
            }
        }
    }

    private func rows(for category: Category) -> [Row] {
        if category == .thumbnails || category == .originals {
            return [.size, .limit, .clear]
        }
        return category.allowsClearing ? [.size, .clearableSize, .clear] : [.size]
    }

    private func clear(_ category: Category) {
        guard !isClearing, let current = usage[category], current.clearableFiles > 0 else { return }
        isClearing = true
        refreshTask?.cancel()
        refreshID = UUID()
        let progress = UIAlertController(
            title: String(localized: "thumbnailCache.clearing"), message: nil, preferredStyle: .alert
        )
        present(progress, animated: true)
        Task { [weak self] in
            let success = await AppCacheManager.shared.clear(category)
            guard let self else { return }
            progress.dismiss(animated: true) {
                self.isClearing = false
                self.refreshSizes()
                if !success {
                    let alert = UIAlertController(
                        title: String(localized: "common.error"),
                        message: String(localized: "cacheManagement.clearFailed"),
                        preferredStyle: .alert
                    )
                    alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
                    self.present(alert, animated: true)
                }
            }
        }
    }

    private func openLimitPicker(for category: Category) {
        let picker: CacheLimitPickerViewController
        if category == .thumbnails {
            let options = ThumbnailCacheSizeLimit.getOptions()
            picker = CacheLimitPickerViewController(
                navTitle: ThumbnailCacheSizeLimit.getTitle(),
                footerText: ThumbnailCacheSizeLimit.getFooter(),
                titles: options.map { $0.getName() },
                selectedIndex: { options.firstIndex(of: ThumbnailCacheSizeLimit.getValue()) ?? 0 },
                onSelect: { [weak self] index in
                    ThumbnailCacheSizeLimit.setValue(options[index])
                    Task {
                        await MediaThumbnailCache.applySizeLimit(options[index].maxBytes)
                        self?.refreshSizes()
                    }
                }
            )
        } else {
            let options = OriginalPhotoCacheSizeLimit.getOptions()
            picker = CacheLimitPickerViewController(
                navTitle: OriginalPhotoCacheSizeLimit.getTitle(),
                footerText: OriginalPhotoCacheSizeLimit.getFooter(),
                titles: options.map { $0.getName() },
                selectedIndex: { options.firstIndex(of: OriginalPhotoCacheSizeLimit.getValue()) ?? 0 },
                onSelect: { [weak self] index in
                    OriginalPhotoCacheSizeLimit.setValue(options[index])
                    Task {
                        if let cap = options[index].maxBytes {
                            await withCancellableDetachedValue(priority: .utility) {
                                OriginalPhotoCache.shared.enforceCap(maxBytes: cap, preservingActiveFiles: true)
                            }
                        } else {
                            _ = await AppCacheManager.shared.clear(.originals)
                        }
                        self?.refreshSizes()
                    }
                }
            )
        }
        navigationController?.pushViewController(picker, animated: true)
    }
}

extension StorageUsageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int { category == nil ? 2 : 1 }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        if let category, !category.allowsClearing { return 1 + dataEntries.count }
        if let category { return rows(for: category).count }
        return section == 0 ? 1 : Category.allCases.count + 1
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard category == nil, section == 1 else { return nil }
        return String(localized: "storageUsage.dataSize")
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let category else {
            return section == 1 ? String(localized: "storageUsage.overview.footer") : nil
        }
        switch category {
        case .thumbnails: return String(localized: "cacheManagement.thumbnails.footer")
        case .originals: return String(localized: "originalCache.footer")
        case .temporaryFiles: return String(localized: "cacheManagement.temporary.footer")
        case .stagedFiles: return String(localized: "cacheManagement.staging.footer")
        case .executionLogs: return String(localized: "cacheManagement.logs.footer")
        case .localData: return String(localized: "storageUsage.localData.footer")
        case .otherData: return String(localized: "storageUsage.otherData.footer")
        }
    }

    private func categoryTitle(_ category: Category) -> String {
        switch category {
        case .thumbnails: return String(localized: "imageBrowserCache.thumbnail.header")
        case .originals: return String(localized: "originalCache.header")
        case .temporaryFiles: return String(localized: "cacheManagement.temporary.header")
        case .stagedFiles: return String(localized: "cacheManagement.staging.header")
        case .executionLogs: return String(localized: "cacheManagement.logs.header")
        case .localData: return String(localized: "storageUsage.localData.header")
        case .otherData: return String(localized: "storageUsage.otherData.header")
        }
    }

    private func overviewCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
        var content = UIListContentConfiguration.valueCell()
        cell.accessoryView = nil
        cell.accessoryType = .none
        cell.selectionStyle = .none
        cell.indentationLevel = 0
        if indexPath.section == 0 {
            content.text = String(localized: "storageUsage.appSize")
            content.secondaryText = sizeText(appUsage)
        } else if indexPath.row == 0 {
            content.text = String(localized: "storageUsage.total")
            content.secondaryText = sizeText(dataUsage)
            content.textProperties.font = .preferredFont(forTextStyle: .headline)
            content.secondaryTextProperties.font = .preferredFont(forTextStyle: .headline)
        } else {
            let category = Category.allCases[indexPath.row - 1]
            content.text = categoryTitle(category)
            content.secondaryText = sizeText(usage[category])
            cell.indentationWidth = 12
            cell.indentationLevel = 1
            cell.accessoryType = .disclosureIndicator
            cell.selectionStyle = .default
        }
        cell.contentConfiguration = content
        return cell
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let category else { return overviewCell(in: tableView, at: indexPath) }
        if !category.allowsClearing, indexPath.row > 0 {
            return dataDetailCell(in: tableView, at: indexPath)
        }
        let row = rows(for: category)[indexPath.row]
        let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
        cell.accessoryType = .none
        cell.accessoryView = nil
        cell.selectionStyle = .none
        var content = UIListContentConfiguration.valueCell()
        switch row {
        case .size, .clearableSize:
            content.text = row == .size
                ? String(localized: "storageUsage.usedSize")
                : String(localized: "cacheManagement.clearableSize")
            if let current = usage[category] {
                content.secondaryText = current.hasErrors
                    ? String(localized: "cacheManagement.scanFailed")
                    : ByteCountFormatter.string(
                        fromByteCount: row == .size ? current.bytes : current.clearableBytes, countStyle: .file
                    )
            } else {
                content.secondaryText = "…"
            }
        case .limit:
            content.text = String(localized: "originalCache.limit.label")
            content.secondaryText = category == .thumbnails
                ? ThumbnailCacheSizeLimit.getValue().getName()
                : OriginalPhotoCacheSizeLimit.getValue().getName()
            cell.accessoryType = .disclosureIndicator
            cell.selectionStyle = .default
        case .clear:
            let enabled = !isClearing && (usage[category]?.clearableFiles ?? 0) > 0
            content = cell.defaultContentConfiguration()
            content.text = String(localized: "storageUsage.clearAvailable")
            content.textProperties.color = enabled ? .systemRed : .tertiaryLabel
            content.textProperties.alignment = .center
            cell.selectionStyle = enabled ? .default : .none
        }
        cell.contentConfiguration = content
        return cell
    }

    private func dataDetailCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        let entry = dataEntries[indexPath.row - 1]
        let cell = tableView.dequeueReusableCell(withIdentifier: "DataDetailCell", for: indexPath)
        cell.accessoryType = .none
        cell.selectionStyle = .none
        var content = UIListContentConfiguration.subtitleCell()
        switch entry.kind {
        case .remoteIndex: content.text = String(localized: "storageUsage.remoteIndex")
        case .caches where entry.path == "Library/Caches/\(Bundle.main.bundleIdentifier ?? "")":
            content.text = String(localized: "storageUsage.networkCache")
        case .caches: content.text = String(localized: "storageUsage.otherCaches")
        case .applicationSupport: content.text = String(localized: "storageUsage.supportData")
        case .temporary: content.text = String(localized: "storageUsage.otherTemporary")
        case .documents: content.text = String(localized: "storageUsage.documents")
        case .library: content.text = String(localized: "storageUsage.libraryData")
        case .unclassified: content.text = String(localized: "storageUsage.otherData.header")
        case .database: content.text = String(localized: "storageUsage.database")
        case .databaseLog: content.text = String(localized: "storageUsage.databaseLog")
        case .databaseMemory: content.text = String(localized: "storageUsage.databaseMemory")
        case .databaseJournal: content.text = String(localized: "storageUsage.databaseJournal")
        case .preferences: content.text = String(localized: "storageUsage.preferences")
        case .localFiles: content.text = String(localized: "storageUsage.localFiles")
        }
        let title = content.text ?? ""
        content.secondaryText = entry.path
        content.secondaryTextProperties.font = .preferredFont(forTextStyle: .footnote)
        content.secondaryTextProperties.color = .secondaryLabel
        content.secondaryTextProperties.numberOfLines = 0
        cell.contentConfiguration = content
        let size = UILabel()
        size.font = .preferredFont(forTextStyle: .subheadline)
        size.adjustsFontForContentSizeCategory = true
        size.textColor = .secondaryLabel
        size.text = sizeText(entry.usage)
        size.sizeToFit()
        size.isAccessibilityElement = false
        cell.accessoryView = size
        cell.isAccessibilityElement = false
        cell.contentView.isAccessibilityElement = true
        cell.contentView.accessibilityTraits = .staticText
        cell.contentView.accessibilityLabel = [title, entry.path, sizeText(entry.usage)].joined(separator: ", ")
        return cell
    }

    private func sizeText(_ usage: AppCacheManager.Usage?) -> String {
        guard let usage else { return "…" }
        return usage.hasErrors ? String(localized: "cacheManagement.scanFailed")
            : ByteCountFormatter.string(fromByteCount: usage.bytes, countStyle: .file)
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard !isClearing else { return }
        guard let category else {
            guard indexPath.section == 1, indexPath.row > 0 else { return }
            let detail = StorageUsageViewController(category: Category.allCases[indexPath.row - 1])
            navigationController?.pushViewController(detail, animated: true)
            return
        }
        guard category.allowsClearing else { return }
        switch rows(for: category)[indexPath.row] {
        case .limit: openLimitPicker(for: category)
        case .clear: clear(category)
        case .size, .clearableSize: break
        }
    }
}

private final class CacheLimitPickerViewController: UITableViewController {
    private let navTitle: String
    private let footerText: String?
    private let titles: [String]
    private let selectedIndex: () -> Int
    private let onSelect: (Int) -> Void

    init(
        navTitle: String,
        footerText: String?,
        titles: [String],
        selectedIndex: @escaping () -> Int,
        onSelect: @escaping (Int) -> Void
    ) {
        self.navTitle = navTitle
        self.footerText = footerText
        self.titles = titles
        self.selectedIndex = selectedIndex
        self.onSelect = onSelect
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = navTitle
        view.backgroundColor = .appBackground
        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "Cell")
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        titles.count
    }

    override func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        footerText
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
        var content = cell.defaultContentConfiguration()
        content.text = titles[indexPath.row]
        cell.contentConfiguration = content
        cell.tintColor = .appTint
        cell.accessoryType = indexPath.row == selectedIndex() ? .checkmark : .none
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        onSelect(indexPath.row)
        tableView.reloadData()
    }
}
