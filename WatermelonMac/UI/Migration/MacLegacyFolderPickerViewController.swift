import AppKit

@MainActor
final class MacLegacyFolderPickerViewController: NSViewController {
    private let viewModel: LegacyFolderPickerViewModel
    private let onPick: (String) -> Void
    private let pathLabel = NSTextField(labelWithString: "")
    private let upButton = NSButton()
    private let refreshButton = NSButton()
    private let tableView = NSTableView()
    private let scrollView = NSScrollView()
    private let statusStack = NSStackView()
    private let statusSpinner = NSProgressIndicator()
    private let statusLabel = NSTextField(wrappingLabelWithString: "")
    private let retryButton = NSButton()
    private let useButton = NSButton()

    init(
        client: any RemoteStorageClientProtocol,
        initialPath: String,
        onPick: @escaping (String) -> Void
    ) {
        viewModel = LegacyFolderPickerViewModel(
            client: client,
            initialPath: initialPath
        )
        self.onPick = onPick
        super.init(nibName: nil, bundle: nil)
        preferredContentSize = NSSize(width: 560, height: 480)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        view = NSView()

        let titleLabel = NSTextField(
            labelWithString: String(
                localized: "legacy.folder.picker.title"
            )
        )
        titleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        upButton.title = ""
        upButton.image = NSImage(
            systemSymbolName: "arrow.up",
            accessibilityDescription: nil
        )
        upButton.bezelStyle = .rounded
        upButton.target = self
        upButton.action = #selector(navigateUp(_:))

        pathLabel.font = .monospacedSystemFont(
            ofSize: 13,
            weight: .regular
        )
        pathLabel.lineBreakMode = .byTruncatingMiddle
        pathLabel.setContentCompressionResistancePriority(
            .defaultLow,
            for: .horizontal
        )

        refreshButton.title = ""
        refreshButton.image = NSImage(
            systemSymbolName: "arrow.clockwise",
            accessibilityDescription: nil
        )
        refreshButton.bezelStyle = .rounded
        refreshButton.target = self
        refreshButton.action = #selector(refresh(_:))

        let pathRow = NSStackView(
            views: [upButton, pathLabel, refreshButton]
        )
        pathRow.orientation = .horizontal
        pathRow.alignment = .centerY
        pathRow.spacing = 8

        configureTable()
        scrollView.documentView = tableView
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .bezelBorder

        statusSpinner.style = .spinning
        statusSpinner.controlSize = .small
        statusLabel.alignment = .center
        statusLabel.textColor = .secondaryLabelColor
        statusLabel.maximumNumberOfLines = 4

        retryButton.title = String(localized: "common.retry")
        retryButton.bezelStyle = .rounded
        retryButton.target = self
        retryButton.action = #selector(refresh(_:))

        statusStack.orientation = .vertical
        statusStack.alignment = .centerX
        statusStack.spacing = 10
        statusStack.addArrangedSubview(statusSpinner)
        statusStack.addArrangedSubview(statusLabel)
        statusStack.addArrangedSubview(retryButton)
        statusStack.translatesAutoresizingMaskIntoConstraints = false

        let browserContainer = NSView()
        browserContainer.addSubview(scrollView)
        browserContainer.addSubview(statusStack)
        scrollView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            scrollView.topAnchor.constraint(
                equalTo: browserContainer.topAnchor
            ),
            scrollView.leadingAnchor.constraint(
                equalTo: browserContainer.leadingAnchor
            ),
            scrollView.trailingAnchor.constraint(
                equalTo: browserContainer.trailingAnchor
            ),
            scrollView.bottomAnchor.constraint(
                equalTo: browserContainer.bottomAnchor
            ),
            statusStack.centerXAnchor.constraint(
                equalTo: browserContainer.centerXAnchor
            ),
            statusStack.centerYAnchor.constraint(
                equalTo: browserContainer.centerYAnchor
            ),
            statusStack.leadingAnchor.constraint(
                greaterThanOrEqualTo: browserContainer.leadingAnchor,
                constant: 30
            ),
            statusStack.trailingAnchor.constraint(
                lessThanOrEqualTo: browserContainer.trailingAnchor,
                constant: -30
            ),
        ])

        let cancelButton = NSButton(
            title: String(localized: "common.cancel"),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded
        cancelButton.keyEquivalent = "\u{1b}"

        useButton.title = String(
            localized: "legacy.folder.picker.useThis"
        )
        useButton.bezelStyle = .rounded
        useButton.bezelColor = .wmMaterialPrimary
        useButton.keyEquivalent = "\r"
        useButton.target = self
        useButton.action = #selector(useCurrentFolder(_:))

        let actions = NSStackView(
            views: [NSView(), cancelButton, useButton]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 8

        let content = NSStackView(
            views: [titleLabel, pathRow, browserContainer, actions]
        )
        content.orientation = .vertical
        content.alignment = .width
        content.spacing = 12
        content.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(content)

        NSLayoutConstraint.activate([
            content.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 20
            ),
            content.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 20
            ),
            content.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -20
            ),
            content.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -16
            ),
            browserContainer.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 330
            ),
            actions.widthAnchor.constraint(equalTo: content.widthAnchor),
        ])

        viewModel.onChange = { [weak self] in
            self?.render()
        }
        render()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        if viewModel.state == .idle {
            viewModel.load()
        }
    }

    private func configureTable() {
        let column = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("folder")
        )
        column.title = ""
        tableView.addTableColumn(column)
        tableView.headerView = nil
        tableView.rowHeight = 34
        tableView.allowsMultipleSelection = false
        tableView.dataSource = self
        tableView.delegate = self
        tableView.target = self
        tableView.action = #selector(openSelectedFolder(_:))
    }

    private func render() {
        pathLabel.stringValue = viewModel.currentPath
        upButton.isEnabled = viewModel.canGoUp

        switch viewModel.state {
        case .idle, .loading:
            scrollView.isHidden = true
            statusStack.isHidden = false
            statusSpinner.isHidden = false
            statusSpinner.startAnimation(nil)
            statusLabel.stringValue = String(
                localized: "smb.path.loading"
            )
            statusLabel.textColor = .secondaryLabelColor
            retryButton.isHidden = true
            useButton.isEnabled = false
        case .failed(let message):
            scrollView.isHidden = true
            statusStack.isHidden = false
            statusSpinner.stopAnimation(nil)
            statusSpinner.isHidden = true
            statusLabel.stringValue = message
            statusLabel.textColor = .wmMaterialError
            retryButton.isHidden = false
            useButton.isEnabled = false
        case .loaded:
            statusSpinner.stopAnimation(nil)
            retryButton.isHidden = true
            useButton.isEnabled = true
            if viewModel.entries.isEmpty {
                scrollView.isHidden = true
                statusStack.isHidden = false
                statusSpinner.isHidden = true
                statusLabel.stringValue = String(
                    localized: "smb.path.emptyDir"
                )
                statusLabel.textColor = .secondaryLabelColor
            } else {
                scrollView.isHidden = false
                statusStack.isHidden = true
                tableView.reloadData()
            }
        }
    }

    @objc private func navigateUp(_ sender: Any?) {
        viewModel.navigateUp()
    }

    @objc private func refresh(_ sender: Any?) {
        viewModel.load()
    }

    @objc private func openSelectedFolder(_ sender: Any?) {
        guard viewModel.entries.indices.contains(tableView.clickedRow) else {
            return
        }
        viewModel.navigate(
            to: viewModel.entries[tableView.clickedRow].path
        )
    }

    @objc private func cancel(_ sender: Any?) {
        dismiss(self)
    }

    @objc private func useCurrentFolder(_ sender: Any?) {
        let path = viewModel.currentPath
        dismiss(self)
        onPick(path)
    }
}

extension MacLegacyFolderPickerViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        viewModel.entries.count
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard viewModel.entries.indices.contains(row) else {
            return nil
        }
        let identifier = NSUserInterfaceItemIdentifier(
            "legacyFolderCell"
        )
        let cell = tableView.makeView(
            withIdentifier: identifier,
            owner: self
        ) as? NSTableCellView ?? NSTableCellView()
        cell.identifier = identifier

        let symbol: NSImageView
        let label: NSTextField
        let chevron: NSImageView
        if let existingSymbol = cell.imageView,
           let existingLabel = cell.textField,
           let existingChevron = cell.subviews.last as? NSImageView {
            symbol = existingSymbol
            label = existingLabel
            chevron = existingChevron
        } else {
            symbol = NSImageView()
            symbol.image = NSImage(
                systemSymbolName: "folder",
                accessibilityDescription: nil
            )
            symbol.contentTintColor = .secondaryLabelColor
            symbol.translatesAutoresizingMaskIntoConstraints = false
            cell.imageView = symbol
            cell.addSubview(symbol)

            label = NSTextField(labelWithString: "")
            label.lineBreakMode = .byTruncatingMiddle
            label.translatesAutoresizingMaskIntoConstraints = false
            cell.textField = label
            cell.addSubview(label)

            chevron = NSImageView()
            chevron.image = NSImage(
                systemSymbolName: "chevron.right",
                accessibilityDescription: nil
            )
            chevron.contentTintColor = .tertiaryLabelColor
            chevron.translatesAutoresizingMaskIntoConstraints = false
            cell.addSubview(chevron)

            NSLayoutConstraint.activate([
                symbol.leadingAnchor.constraint(
                    equalTo: cell.leadingAnchor,
                    constant: 8
                ),
                symbol.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                ),
                symbol.widthAnchor.constraint(equalToConstant: 18),
                label.leadingAnchor.constraint(
                    equalTo: symbol.trailingAnchor,
                    constant: 8
                ),
                label.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                ),
                chevron.leadingAnchor.constraint(
                    equalTo: label.trailingAnchor,
                    constant: 8
                ),
                chevron.trailingAnchor.constraint(
                    equalTo: cell.trailingAnchor,
                    constant: -8
                ),
                chevron.centerYAnchor.constraint(
                    equalTo: cell.centerYAnchor
                ),
                chevron.widthAnchor.constraint(equalToConstant: 10),
            ])
        }

        label.stringValue = viewModel.entries[row].name
        return cell
    }
}
