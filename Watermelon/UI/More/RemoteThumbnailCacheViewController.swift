import Foundation
import UIKit

// Global (cross-node) management for the on-device thumbnail cache (L1). Distinct from a node's
// "Delete Remote Thumbnails" (L2) — this only frees local disk.
final class RemoteThumbnailCacheViewController: UIViewController {
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private var sizeText: String?

    private enum Section: Int, CaseIterable {
        case size
        case clear
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "more.item.thumbnailCache")
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
        refreshSize()
    }

    private func refreshSize() {
        sizeText = nil
        tableView.reloadData()
        Task { [weak self] in
            let bytes = await RemoteThumbnailCache.diskSizeBytes()
            guard let self else { return }
            self.sizeText = ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
            self.tableView.reloadData()
        }
    }

    private func clearCache() {
        let progress = UIAlertController(
            title: String(localized: "thumbnailCache.clearing"),
            message: nil,
            preferredStyle: .alert
        )
        present(progress, animated: true)
        Task { [weak self] in
            await RemoteThumbnailCache.clear()
            guard let self else { return }
            progress.dismiss(animated: true) {
                self.refreshSize()
            }
        }
    }
}

extension RemoteThumbnailCacheViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        1
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        switch Section(rawValue: section)! {
        case .size: return String(localized: "thumbnailCache.footer")
        case .clear: return nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
        switch Section(rawValue: indexPath.section)! {
        case .size:
            var content = UIListContentConfiguration.valueCell()
            content.text = String(localized: "thumbnailCache.size.label")
            content.secondaryText = sizeText ?? "…"
            cell.contentConfiguration = content
            cell.selectionStyle = .none
        case .clear:
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "thumbnailCache.clear")
            content.textProperties.color = .systemRed
            content.textProperties.alignment = .center
            cell.contentConfiguration = content
            cell.selectionStyle = .default
        }
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard case .clear = Section(rawValue: indexPath.section)! else { return }
        clearCache()
    }
}
