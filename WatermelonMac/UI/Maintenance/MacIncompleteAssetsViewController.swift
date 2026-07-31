import AppKit

@MainActor
final class MacIncompleteAssetsViewController: NSViewController {
    private let entries: [IncompleteAssetEntry]
    private let tableView = NSTableView()
    private let detailsButton = NSButton()

    init(entries: [IncompleteAssetEntry]) {
        self.entries = IncompleteAssetPresentation.sections(
            from: entries
        ).flatMap(\.entries)
        super.init(nibName: nil, bundle: nil)
        preferredContentSize = NSSize(width: 780, height: 480)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        view = NSView()

        configureTable()

        let scrollView = NSScrollView()
        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .bezelBorder

        detailsButton.title = String(
            localized: "mediaBrowser.info.title",
            defaultValue: "Info"
        )
        detailsButton.bezelStyle = .rounded
        detailsButton.target = self
        detailsButton.action = #selector(showSelectedDetails(_:))
        detailsButton.isEnabled = false

        let closeButton = NSButton(
            title: String(
                localized: "common.close",
                defaultValue: "Close"
            ),
            target: self,
            action: #selector(close(_:))
        )
        closeButton.bezelStyle = .rounded
        closeButton.keyEquivalent = "\r"

        let actions = NSStackView(
            views: [NSView(), detailsButton, closeButton]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 8

        let stack = NSStackView(views: [scrollView, actions])
        stack.orientation = .vertical
        stack.alignment = .width
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 20
            ),
            stack.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 20
            ),
            stack.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -20
            ),
            stack.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -16
            ),
            scrollView.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 360
            ),
            actions.widthAnchor.constraint(
                equalTo: stack.widthAnchor
            )
        ])
    }

    private func configureTable() {
        let columns: [(String, String, CGFloat)] = [
            (
                "month",
                String(
                    localized: "mac.library.month",
                    defaultValue: "Month"
                ),
                100
            ),
            (
                "file",
                String(
                    localized: "mediaMetadata.section.file",
                    defaultValue: "File"
                ),
                240
            ),
            (
                "info",
                String(
                    localized: "mediaBrowser.info.title",
                    defaultValue: "Info"
                ),
                390
            )
        ]
        for (identifier, title, width) in columns {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier(identifier)
            )
            column.title = title
            column.width = width
            column.minWidth = identifier == "info" ? 250 : width
            tableView.addTableColumn(column)
        }
        tableView.headerView = NSTableHeaderView()
        tableView.rowHeight = 30
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.allowsMultipleSelection = false
        tableView.dataSource = self
        tableView.delegate = self
        tableView.target = self
        tableView.doubleAction = #selector(showSelectedDetails(_:))
    }

    @objc private func showSelectedDetails(_ sender: Any?) {
        guard entries.indices.contains(tableView.selectedRow) else {
            return
        }
        let entry = entries[tableView.selectedRow]
        let alert = NSAlert()
        alert.messageText = String(
            localized:
                "storage.detail.incompleteAssets.detailTitle"
        )
        alert.informativeText =
            IncompleteAssetPresentation.detailText(for: entry)
        alert.addButton(
            withTitle: String(
                localized: "common.ok",
                defaultValue: "OK"
            )
        )
        if let window = view.window {
            alert.beginSheetModal(for: window)
        } else {
            alert.runModal()
        }
    }

    @objc private func close(_ sender: Any?) {
        dismiss(nil)
    }
}

extension MacIncompleteAssetsViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        entries.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard entries.indices.contains(row), let tableColumn else {
            return nil
        }
        let identifier = NSUserInterfaceItemIdentifier(
            "incomplete.\(tableColumn.identifier.rawValue)"
        )
        let cell = tableView.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? NSTableCellView ?? NSTableCellView()
        cell.identifier = identifier

        let field: NSTextField
        if let existing = cell.textField {
            field = existing
        } else {
            field = NSTextField(labelWithString: "")
            field.translatesAutoresizingMaskIntoConstraints = false
            field.lineBreakMode = .byTruncatingMiddle
            cell.addSubview(field)
            cell.textField = field
            NSLayoutConstraint.activate([
                field.leadingAnchor.constraint(
                    equalTo: cell.leadingAnchor,
                    constant: 4
                ),
                field.trailingAnchor.constraint(
                    equalTo: cell.trailingAnchor,
                    constant: -4
                ),
                field.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                )
            ])
        }

        let entry = entries[row]
        switch tableColumn.identifier.rawValue {
        case "month":
            field.stringValue = entry.month.displayText
        case "file":
            field.stringValue =
                IncompleteAssetPresentation.fileName(for: entry)
        case "info":
            field.stringValue =
                IncompleteAssetPresentation.summary(for: entry)
            field.textColor = .secondaryLabelColor
        default:
            field.stringValue = ""
        }
        return cell
    }

    func tableViewSelectionDidChange(
        _ notification: Notification
    ) {
        detailsButton.isEnabled =
            entries.indices.contains(tableView.selectedRow)
    }
}
