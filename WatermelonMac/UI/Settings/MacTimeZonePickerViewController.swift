import AppKit

@MainActor
final class MacTimeZonePickerViewController: NSViewController {
    var onSelect: ((TimeZone) -> Void)?

    private struct Choice {
        let identifier: String
        let city: String
        let region: String
        let offset: String
        let offsetSeconds: Int
        let searchText: String
    }

    private let searchField = NSSearchField()
    private let tableView = NSTableView()
    private let chooseButton = NSButton()
    private var allChoices: [Choice] = []
    private var visibleChoices: [Choice] = []

    override func loadView() {
        view = NSView(
            frame: NSRect(x: 0, y: 0, width: 660, height: 540)
        )
        preferredContentSize = view.frame.size

        let titleLabel = NSTextField(
            labelWithString: String(
                localized: "settings.monthGroupingTimeZone.chooseTitle",
                defaultValue: "Choose Time Zone"
            )
        )
        titleLabel.font = .systemFont(ofSize: 22, weight: .bold)

        searchField.placeholderString = String(
            localized: "settings.monthGroupingTimeZone.search",
            defaultValue: "Search city or time zone"
        )
        searchField.target = self
        searchField.action = #selector(search(_:))
        searchField.sendsSearchStringImmediately = true

        configureTable()
        let scroll = NSScrollView()
        scroll.documentView = tableView
        scroll.hasVerticalScroller = true
        scroll.autohidesScrollers = true
        scroll.borderType = .bezelBorder

        let cancelButton = NSButton(
            title: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            ),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded

        chooseButton.title = String(
            localized: "common.choose",
            defaultValue: "Choose"
        )
        chooseButton.bezelStyle = .rounded
        chooseButton.keyEquivalent = "\r"
        chooseButton.target = self
        chooseButton.action = #selector(choose(_:))
        chooseButton.isEnabled = false

        let actions = NSStackView(
            views: [NSView(), cancelButton, chooseButton]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 10

        let root = NSStackView(
            views: [
                titleLabel,
                searchField,
                scroll,
                actions
            ]
        )
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 12
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(equalTo: view.topAnchor, constant: 24),
            root.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 24
            ),
            root.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -24
            ),
            root.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -20
            ),
            searchField.widthAnchor.constraint(
                equalTo: root.widthAnchor
            ),
            scroll.widthAnchor.constraint(equalTo: root.widthAnchor),
            actions.widthAnchor.constraint(equalTo: root.widthAnchor),
            scroll.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 350
            )
        ])
        loadChoices()
    }

    private func configureTable() {
        let definitions: [(String, String, CGFloat)] = [
            (
                "city",
                String(localized: "mac.settings.timeZoneCity"),
                190
            ),
            (
                "region",
                String(
                    localized:
                        "settings.monthGroupingTimeZone.section.timeZone"
                ),
                250
            ),
            (
                "offset",
                String(
                    localized:
                        "mac.settings.timeZoneCurrentOffset"
                ),
                110
            )
        ]
        for definition in definitions {
            let column = NSTableColumn(
                identifier: NSUserInterfaceItemIdentifier(definition.0)
            )
            column.title = definition.1
            column.width = definition.2
            tableView.addTableColumn(column)
        }
        tableView.headerView = NSTableHeaderView()
        tableView.rowHeight = 28
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.dataSource = self
        tableView.delegate = self
        tableView.target = self
        tableView.doubleAction = #selector(choose(_:))
    }

    private func loadChoices() {
        let now = Date()
        let identifiers =
            MonthGroupingTimeZoneCatalog.selectableIdentifiers(
                adding: [
                    TimeZone.current.identifier,
                    MonthGroupingTimeZonePreference.current.identifier
                ]
            )
        allChoices = identifiers.compactMap { identifier in
            guard let timeZone = TimeZone(identifier: identifier) else {
                return nil
            }
            let parts = identifier.split(separator: "/").map(String.init)
            let city = (parts.last ?? identifier)
                .replacingOccurrences(of: "_", with: " ")
            let region = parts.dropLast().joined(separator: " / ")
            let offset = Self.offsetText(
                seconds: timeZone.secondsFromGMT(for: now)
            )
            return Choice(
                identifier: identifier,
                city: city,
                region: region.isEmpty ? identifier : region,
                offset: offset,
                offsetSeconds: timeZone.secondsFromGMT(for: now),
                searchText: [
                    identifier,
                    city,
                    region,
                    offset,
                    timeZone.abbreviation(for: now) ?? ""
                ]
                .joined(separator: " ")
                .folding(
                    options: [
                        .caseInsensitive,
                        .diacriticInsensitive,
                        .widthInsensitive
                    ],
                    locale: .current
                )
            )
        }
        .sorted {
            if $0.offsetSeconds != $1.offsetSeconds {
                return $0.offsetSeconds < $1.offsetSeconds
            }
            return $0.identifier.localizedCaseInsensitiveCompare(
                $1.identifier
            ) == .orderedAscending
        }
        visibleChoices = allChoices
        tableView.reloadData()

        if let currentIdentifier =
            MonthGroupingTimeZonePreference.current.identifier,
           let row = visibleChoices.firstIndex(
               where: { $0.identifier == currentIdentifier }
           ) {
            tableView.selectRowIndexes(
                IndexSet(integer: row),
                byExtendingSelection: false
            )
            tableView.scrollRowToVisible(row)
        }
    }

    private static func offsetText(seconds: Int) -> String {
        let sign = seconds < 0 ? "-" : "+"
        let value = abs(seconds)
        return String(
            format: "GMT%@%02d:%02d",
            sign,
            value / 3_600,
            value % 3_600 / 60
        )
    }

    @objc private func search(_ sender: NSSearchField) {
        let query = sender.stringValue.trimmingCharacters(
            in: .whitespacesAndNewlines
        )
        .folding(
            options: [
                .caseInsensitive,
                .diacriticInsensitive,
                .widthInsensitive
            ],
            locale: .current
        )
        visibleChoices = query.isEmpty
            ? allChoices
            : allChoices.filter { $0.searchText.contains(query) }
        tableView.reloadData()
        tableView.deselectAll(nil)
        chooseButton.isEnabled = false
    }

    @objc private func choose(_ sender: Any?) {
        let row = tableView.selectedRow
        guard visibleChoices.indices.contains(row),
              let timeZone = TimeZone(
                  identifier: visibleChoices[row].identifier
              ) else {
            return
        }
        onSelect?(timeZone)
        dismiss(nil)
    }

    @objc private func cancel(_ sender: Any?) {
        dismiss(nil)
    }
}

extension MacTimeZonePickerViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        visibleChoices.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard visibleChoices.indices.contains(row), let tableColumn else {
            return nil
        }
        let choice = visibleChoices[row]
        let identifier = NSUserInterfaceItemIdentifier(
            "timezone.\(tableColumn.identifier.rawValue)"
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
            cell.textField = field
            cell.addSubview(field)
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
        switch tableColumn.identifier.rawValue {
        case "city":
            field.stringValue = choice.city
            field.font = .systemFont(ofSize: 12, weight: .medium)
            field.textColor = .labelColor
        case "region":
            field.stringValue = choice.identifier
            field.font = .systemFont(ofSize: 12)
            field.textColor = .secondaryLabelColor
        default:
            field.stringValue = choice.offset
            field.font = .monospacedDigitSystemFont(
                ofSize: 11,
                weight: .regular
            )
            field.textColor = .secondaryLabelColor
        }
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        chooseButton.isEnabled = visibleChoices.indices.contains(
            tableView.selectedRow
        )
    }
}
