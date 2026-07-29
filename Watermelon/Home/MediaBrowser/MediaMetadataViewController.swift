import UIKit

@MainActor
final class MediaMetadataViewController: UITableViewController {
    private let item: MediaBrowserItem
    private let source: MediaBrowserSource
    private var document: MediaMetadataDocument?
    private var loadTask: Task<Void, Never>?

    init(item: MediaBrowserItem, source: MediaBrowserSource) {
        self.item = item
        self.source = source
        super.init(style: .insetGrouped)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        overrideUserInterfaceStyle = .dark
        view.backgroundColor = .mediaBrowserBackdrop
        view.tintColor = .mediaBrowserAccent
        tableView.backgroundColor = .mediaBrowserBackdrop
        tableView.separatorColor = .mediaBrowserDivider
        title = String(localized: "mediaBrowser.info.title")
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            title: String(localized: "common.close"),
            style: .done,
            target: self,
            action: #selector(close)
        )
        tableView.cellLayoutMarginsFollowReadableWidth = true
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 52
        showLoading()
        let source = source
        let item = item
        loadTask = Task { [weak self, source, item] in
            let document = await source.metadata(for: item)
            guard let self, !Task.isCancelled else { return }
            self.document = document
            self.tableView.reloadData()
            if document == nil {
                self.showUnavailable()
            } else {
                self.contentUnavailableConfiguration = nil
            }
        }
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if isBeingDismissed || navigationController?.isBeingDismissed == true {
            cancelLoad()
        }
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        document?.sections.count ?? 0
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        document?.sections[section].rows.count ?? 0
    }

    override func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        document?.sections[section].title
    }

    override func tableView(
        _ tableView: UITableView,
        cellForRowAt indexPath: IndexPath
    ) -> UITableViewCell {
        let identifier = "MetadataValue"
        let cell = tableView.dequeueReusableCell(withIdentifier: identifier)
            ?? UITableViewCell(style: .default, reuseIdentifier: identifier)
        guard let row = document?.sections[indexPath.section].rows[indexPath.row] else {
            return cell
        }
        var content = UIListContentConfiguration.valueCell()
        content.text = row.label
        content.secondaryText = row.value
        content.textProperties.numberOfLines = 0
        content.secondaryTextProperties.numberOfLines = 0
        content.textProperties.color = .mediaBrowserAccent
        content.secondaryTextProperties.color = .mediaBrowserOnSurface
        cell.contentConfiguration = content
        cell.backgroundColor = .mediaBrowserSurface
        cell.selectionStyle = .none
        return cell
    }

    override func tableView(
        _ tableView: UITableView,
        contextMenuConfigurationForRowAt indexPath: IndexPath,
        point: CGPoint
    ) -> UIContextMenuConfiguration? {
        guard let row = document?.sections[indexPath.section].rows[indexPath.row] else { return nil }
        return UIContextMenuConfiguration(identifier: nil, previewProvider: nil) { _ in
            UIMenu(children: [
                UIAction(
                    title: String(localized: "common.copy"),
                    image: UIImage(systemName: "doc.on.doc")
                ) { _ in
                    UIPasteboard.general.string = row.value
                },
            ])
        }
    }

    private func showLoading() {
        var configuration = UIContentUnavailableConfiguration.loading()
        configuration.text = String(localized: "mediaBrowser.info.loading")
        contentUnavailableConfiguration = configuration
    }

    private func showUnavailable() {
        var configuration = UIContentUnavailableConfiguration.empty()
        configuration.image = UIImage(systemName: "info.circle")
        configuration.text = String(localized: "mediaBrowser.info.unavailable.title")
        configuration.secondaryText = String(localized: "mediaBrowser.info.unavailable.message")
        contentUnavailableConfiguration = configuration
    }

    @objc private func close() {
        cancelLoad()
        dismiss(animated: true)
    }

    private func cancelLoad() {
        loadTask?.cancel()
        loadTask = nil
    }
}
