import UIKit

@MainActor
struct MediaDropSelectionPopoverItem {
    let id = UUID()
    let title: String
    let subtitle: String?
    let image: UIImage?
    let thumbnailProvider: (() async -> UIImage?)?
    let onOpen: (() -> Void)?
    let onRemove: () -> Void
}

@MainActor
final class MediaDropSelectionPopoverViewController: UITableViewController {
    private enum Layout {
        static let navigationBarHeight: CGFloat = 44
        static let estimatedItemRowHeight: CGFloat = 68
        static let groupedInsetsHeight: CGFloat = 32
        static let maximumVisibleRows = 6
    }

    private var items: [MediaDropSelectionPopoverItem]
    private let onDeselectAll: () -> Void
    private var thumbnails: [UUID: UIImage] = [:]
    private var thumbnailTasks: [UUID: Task<Void, Never>] = [:]

    var preferredHeight: CGFloat {
        Layout.navigationBarHeight
            + CGFloat(min(max(items.count, 1), Layout.maximumVisibleRows)) * Layout.estimatedItemRowHeight
            + Layout.groupedInsetsHeight
    }

    init(
        title: String,
        items: [MediaDropSelectionPopoverItem],
        onDeselectAll: @escaping () -> Void
    ) {
        self.items = items
        self.onDeselectAll = onDeselectAll
        super.init(style: .insetGrouped)
        navigationItem.title = title
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            title: String(localized: "transfer.selection.clear"),
            style: .plain,
            target: self,
            action: #selector(deselectAll)
        )
        navigationItem.rightBarButtonItem?.tintColor = .systemRed
    }

    deinit {
        thumbnailTasks.values.forEach { $0.cancel() }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemGroupedBackground
        tableView.backgroundColor = .systemGroupedBackground
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = Layout.estimatedItemRowHeight
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "selection")
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        let maximumTableHeight = CGFloat(Layout.maximumVisibleRows) * Layout.estimatedItemRowHeight
            + Layout.groupedInsetsHeight
        let height = Layout.navigationBarHeight + min(tableView.contentSize.height, maximumTableHeight)
        guard abs((navigationController?.preferredContentSize.height ?? 0) - height) > 1 else { return }
        navigationController?.preferredContentSize.height = height
    }

    override func numberOfSections(in tableView: UITableView) -> Int {
        1
    }

    override func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        items.count
    }

    override func tableView(
        _ tableView: UITableView,
        cellForRowAt indexPath: IndexPath
    ) -> UITableViewCell {
        let item = items[indexPath.row]
        let cell = tableView.dequeueReusableCell(withIdentifier: "selection", for: indexPath)
        configureSelectionCell(cell, item: item)
        requestThumbnailIfNeeded(for: item)
        return cell
    }

    override func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let onOpen = items[indexPath.row].onOpen else { return }
        dismiss(animated: true, completion: onOpen)
    }

    override func tableView(
        _ tableView: UITableView,
        trailingSwipeActionsConfigurationForRowAt indexPath: IndexPath
    ) -> UISwipeActionsConfiguration? {
        let itemID = items[indexPath.row].id
        let deselect = UIContextualAction(
            style: .destructive,
            title: String(localized: "transfer.selection.deselect")
        ) { [weak self] _, _, completion in
            self?.removeItem(id: itemID)
            completion(true)
        }
        deselect.image = UIImage(systemName: "minus.circle")
        let configuration = UISwipeActionsConfiguration(actions: [deselect])
        configuration.performsFirstActionWithFullSwipe = true
        return configuration
    }

    private func configureSelectionCell(
        _ cell: UITableViewCell,
        item: MediaDropSelectionPopoverItem
    ) {
        var content = UIListContentConfiguration.subtitleCell()
        content.text = item.title
        content.secondaryText = item.subtitle ?? "-"
        content.image = thumbnails[item.id] ?? item.image
        content.imageProperties.maximumSize = CGSize(width: 44, height: 44)
        content.imageProperties.reservedLayoutSize = CGSize(width: 44, height: 44)
        content.imageProperties.cornerRadius = 6
        if thumbnails[item.id] == nil {
            content.imageProperties.tintColor = .appTint
        }
        content.textProperties.lineBreakMode = .byTruncatingMiddle
        content.textProperties.numberOfLines = 2
        content.secondaryTextProperties.numberOfLines = 1
        cell.contentConfiguration = content
        cell.accessoryType = item.onOpen == nil ? .none : .disclosureIndicator
        cell.backgroundColor = .secondarySystemGroupedBackground
    }

    private func requestThumbnailIfNeeded(for item: MediaDropSelectionPopoverItem) {
        guard thumbnails[item.id] == nil,
              thumbnailTasks[item.id] == nil,
              let provider = item.thumbnailProvider else { return }
        thumbnailTasks[item.id] = Task { @MainActor [weak self] in
            let image = await provider()
            guard let self, !Task.isCancelled else { return }
            self.thumbnailTasks[item.id] = nil
            guard let image,
                  let row = self.items.firstIndex(where: { $0.id == item.id }) else { return }
            self.thumbnails[item.id] = image
            guard let cell = self.tableView.cellForRow(
                at: IndexPath(row: row, section: 0)
            ) else { return }
            self.configureSelectionCell(cell, item: item)
        }
    }

    private func removeItem(id: UUID) {
        guard let index = items.firstIndex(where: { $0.id == id }) else { return }
        let item = items.remove(at: index)
        thumbnailTasks.removeValue(forKey: id)?.cancel()
        thumbnails.removeValue(forKey: id)
        guard !items.isEmpty else {
            dismiss(animated: true, completion: item.onRemove)
            return
        }
        tableView.deleteRows(at: [IndexPath(row: index, section: 0)], with: .automatic)
        navigationController?.preferredContentSize.height = preferredHeight
        item.onRemove()
    }

    @objc
    private func deselectAll() {
        guard !items.isEmpty else { return }
        dismiss(animated: true, completion: onDeselectAll)
    }
}

extension MediaDropSelectionPopoverViewController: UIPopoverPresentationControllerDelegate {
    func adaptivePresentationStyle(
        for controller: UIPresentationController
    ) -> UIModalPresentationStyle {
        .none
    }
}
