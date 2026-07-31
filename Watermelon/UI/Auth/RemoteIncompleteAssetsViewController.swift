import Foundation
import UIKit

final class RemoteIncompleteAssetsViewController: UITableViewController {
    private let sections: [IncompleteAssetSection]

    init(entries: [IncompleteAssetEntry]) {
        self.sections = IncompleteAssetPresentation.sections(
            from: entries
        )
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
        content.text = IncompleteAssetPresentation.fileName(
            for: entry
        )
        content.secondaryText = IncompleteAssetPresentation.summary(
            for: entry
        )
        cell.contentConfiguration = content
        cell.accessoryType = .disclosureIndicator
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let entry = sections[indexPath.section].entries[indexPath.row]
        let alert = UIAlertController(
            title: String(localized: "storage.detail.incompleteAssets.detailTitle"),
            message: IncompleteAssetPresentation.detailText(
                for: entry
            ),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

}
