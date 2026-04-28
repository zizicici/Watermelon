import Foundation
import UIKit

/// Frozen at push time — does not subscribe to cache notifications.
final class RemoteIncompleteAssetsViewController: UITableViewController {
    private struct Section {
        let month: LibraryMonthKey
        let entries: [IncompleteAssetEntry]
    }

    private let sections: [Section]

    init(entries: [IncompleteAssetEntry]) {
        var grouped: [LibraryMonthKey: [IncompleteAssetEntry]] = [:]
        for entry in entries {
            grouped[entry.month, default: []].append(entry)
        }
        self.sections = grouped.keys.sorted(by: >).map { month in
            let monthEntries = (grouped[month] ?? []).sorted { lhs, rhs in
                let lDate = lhs.creationDate ?? .distantPast
                let rDate = rhs.creationDate ?? .distantPast
                if lDate != rDate { return lDate < rDate }
                return lhs.id.lexicographicallyPrecedes(rhs.id)
            }
            return Section(month: month, entries: monthEntries)
        }
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "storage.detail.incompleteAssets.title")
        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "incomplete")
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        sections.count
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        sections[section].entries.count
    }

    override func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        sections[section].month.displayText
    }

    override func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "incomplete", for: indexPath)
        let entry = sections[indexPath.section].entries[indexPath.row]
        var content = cell.defaultContentConfiguration()
        content.text = entry.representativeFileName ?? String(localized: "storage.detail.incompleteAssets.unknownFileName")
        let missingText = String.localizedStringWithFormat(
            String(localized: "storage.detail.incompleteAssets.missingCount"),
            entry.missingResourceCount,
            entry.totalResourceCount
        )
        let dateText = entry.creationDate.map { $0.formatted(date: .abbreviated, time: .shortened) } ?? "-"
        let fpHex = String(entry.id.hexString.prefix(16))
        content.secondaryText = "\(missingText) · \(dateText) · \(fpHex)"
        cell.contentConfiguration = content
        cell.accessoryType = .disclosureIndicator
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let entry = sections[indexPath.section].entries[indexPath.row]
        let alert = UIAlertController(
            title: String(localized: "storage.detail.incompleteAssets.detailTitle"),
            message: makeDetailMessage(for: entry),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func makeDetailMessage(for entry: IncompleteAssetEntry) -> String {
        var lines: [String] = []
        lines.append(String.localizedStringWithFormat(
            String(localized: "storage.detail.incompleteAssets.fingerprintLine"),
            entry.id.hexString
        ))
        if let name = entry.representativeFileName {
            lines.append(String.localizedStringWithFormat(
                String(localized: "storage.detail.incompleteAssets.fileLine"),
                name
            ))
        }
        if let date = entry.creationDate {
            lines.append(String.localizedStringWithFormat(
                String(localized: "storage.detail.incompleteAssets.createdLine"),
                date.formatted(date: .abbreviated, time: .shortened)
            ))
        }
        if entry.missingResourceHashes.isEmpty {
            lines.append(String(localized: "storage.detail.incompleteAssets.noMissingHashes"))
        } else {
            lines.append(String(localized: "storage.detail.incompleteAssets.missingHashesHeader"))
            for hash in entry.missingResourceHashes {
                lines.append("  \(String(hash.hexString.prefix(32)))")
            }
        }
        return lines.joined(separator: "\n")
    }

}
