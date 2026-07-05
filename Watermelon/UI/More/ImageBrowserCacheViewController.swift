import Foundation
import MoreKit
import UIKit

// On-device caches used by the image browser (L1). Two independently-managed caches, each with a
// settable size cap:
//   • Thumbnail cache (Kingfisher, shared across nodes) — see MediaThumbnailCache.
//   • Original cache (OriginalPhotoCache) — photos and small (≤ threshold) videos downloaded while
//     browsing, so re-viewing doesn't re-download. Distinct from a node's remote sidecar (L2).
final class ImageBrowserCacheViewController: UIViewController {
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private var thumbSizeText: String?
    private var originalSizeText: String?

    private enum Row {
        case thumbSize
        case thumbLimit
        case thumbClear
        case originalSize
        case originalLimit
        case originalClear
    }

    private struct SectionModel {
        let header: String?
        let footer: String?
        let rows: [Row]
    }

    private var sections: [SectionModel] = []

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "more.item.imageBrowserCache")
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "Cell")
        view.addSubview(tableView)
        tableView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.topAnchor),
            tableView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
        rebuildSections()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        refreshSizes()
    }

    private func rebuildSections() {
        sections = [
            SectionModel(
                header: String(localized: "imageBrowserCache.thumbnail.header"),
                footer: String(localized: "thumbnailCache.footer"),
                rows: [.thumbSize, .thumbLimit, .thumbClear]
            ),
            SectionModel(
                header: String(localized: "originalCache.header"),
                footer: String(localized: "originalCache.footer"),
                rows: [.originalSize, .originalLimit, .originalClear]
            ),
        ]
    }

    private func refreshSizes() {
        thumbSizeText = nil
        originalSizeText = nil
        tableView.reloadData()
        Task { [weak self] in
            let bytes = await MediaThumbnailCache.diskSizeBytes()
            guard let self else { return }
            self.thumbSizeText = ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
            self.tableView.reloadData()
        }
        Task { [weak self] in
            let bytes = await withCancellableDetachedValue { OriginalPhotoCache.shared.diskSizeBytes() }
            guard let self else { return }
            self.originalSizeText = ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
            self.tableView.reloadData()
        }
    }

    private func clearThumbnailCache() {
        presentClearing { await MediaThumbnailCache.clear() }
    }

    private func clearOriginalCache() {
        presentClearing { await withCancellableDetachedValue { OriginalPhotoCache.shared.clear() } }
    }

    private func presentClearing(_ work: @escaping () async -> Void) {
        let progress = UIAlertController(
            title: String(localized: "thumbnailCache.clearing"),
            message: nil,
            preferredStyle: .alert
        )
        present(progress, animated: true)
        Task { [weak self] in
            await work()
            guard let self else { return }
            progress.dismiss(animated: true) { self.refreshSizes() }
        }
    }

    private func openThumbnailLimitPicker() {
        let options = ThumbnailCacheSizeLimit.getOptions()
        let picker = CacheLimitPickerViewController(
            navTitle: ThumbnailCacheSizeLimit.getTitle(),
            footerText: ThumbnailCacheSizeLimit.getFooter(),
            titles: options.map { $0.getName() },
            selectedIndex: { options.firstIndex(of: ThumbnailCacheSizeLimit.getValue()) ?? 0 },
            onSelect: { index in
                ThumbnailCacheSizeLimit.setValue(options[index])
                let bytes = options[index].maxBytes
                Task { await MediaThumbnailCache.applySizeLimit(bytes) }
            }
        )
        navigationController?.pushViewController(picker, animated: true)
    }

    private func openOriginalLimitPicker() {
        let options = OriginalPhotoCacheSizeLimit.getOptions()
        let picker = CacheLimitPickerViewController(
            navTitle: OriginalPhotoCacheSizeLimit.getTitle(),
            footerText: OriginalPhotoCacheSizeLimit.getFooter(),
            titles: options.map { $0.getName() },
            selectedIndex: { options.firstIndex(of: OriginalPhotoCacheSizeLimit.getValue()) ?? 0 },
            onSelect: { index in
                OriginalPhotoCacheSizeLimit.setValue(options[index])
                guard let cap = options[index].maxBytes else {
                    // Off = fully disabled: purge any originals cached under a previous cap.
                    Task { await withCancellableDetachedValue { OriginalPhotoCache.shared.clear() } }
                    return
                }
                // Lowering the cap should reclaim space immediately, not lazily on the next download.
                Task { await withCancellableDetachedValue { OriginalPhotoCache.shared.enforceCap(maxBytes: cap) } }
            }
        )
        navigationController?.pushViewController(picker, animated: true)
    }
}

extension ImageBrowserCacheViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        sections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        sections[section].rows.count
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        sections[section].header
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        sections[section].footer
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
        cell.accessoryType = .none
        cell.accessoryView = nil
        switch sections[indexPath.section].rows[indexPath.row] {
        case .thumbSize:
            configureSizeCell(cell, value: thumbSizeText)
        case .originalSize:
            configureSizeCell(cell, value: originalSizeText)
        case .thumbLimit:
            configureLimitCell(cell, value: ThumbnailCacheSizeLimit.getValue().getName())
        case .originalLimit:
            configureLimitCell(cell, value: OriginalPhotoCacheSizeLimit.getValue().getName())
        case .thumbClear, .originalClear:
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "thumbnailCache.clear")
            content.textProperties.color = .systemRed
            content.textProperties.alignment = .center
            cell.contentConfiguration = content
            cell.selectionStyle = .default
        }
        return cell
    }

    private func configureSizeCell(_ cell: UITableViewCell, value: String?) {
        var content = UIListContentConfiguration.valueCell()
        content.text = String(localized: "thumbnailCache.size.label")
        content.secondaryText = value ?? "…"
        cell.contentConfiguration = content
        cell.selectionStyle = .none
    }

    private func configureLimitCell(_ cell: UITableViewCell, value: String) {
        var content = UIListContentConfiguration.valueCell()
        content.text = String(localized: "originalCache.limit.label")
        content.secondaryText = value
        cell.contentConfiguration = content
        cell.tintColor = .appTint
        cell.accessoryType = .disclosureIndicator
        cell.selectionStyle = .default
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        switch sections[indexPath.section].rows[indexPath.row] {
        case .thumbSize, .originalSize:
            break
        case .thumbLimit:
            openThumbnailLimitPicker()
        case .originalLimit:
            openOriginalLimitPicker()
        case .thumbClear:
            clearThumbnailCache()
        case .originalClear:
            clearOriginalCache()
        }
    }
}

// Checkmark picker for a settable cache cap, driven by closures so it serves both cache limits.
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
