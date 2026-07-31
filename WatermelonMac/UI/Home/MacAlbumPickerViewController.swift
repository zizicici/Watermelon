import AppKit
@preconcurrency import Photos

@MainActor
final class MacAlbumPickerViewController: NSViewController {
    var onApply: (([LocalAlbumDescriptor]) -> Void)?
    var onCancel: (() -> Void)?
    var onOpenAlbum: ((LocalAlbumDescriptor) -> Void)?

    private let photoLibraryService: PhotoLibraryService
    private let tableView = NSTableView()
    private let stateLabel = NSTextField(labelWithString: "")
    private let progressIndicator = NSProgressIndicator()
    private let applyButton = NSButton()
    private var albums: [LocalAlbumDescriptor] = []
    private var selectedAlbumIDs: Set<String>
    private var loadTask: Task<Void, Never>?
    private let imageManager = PHCachingImageManager()

    init(
        photoLibraryService: PhotoLibraryService,
        selectedAlbumIDs: Set<String>
    ) {
        self.photoLibraryService = photoLibraryService
        self.selectedAlbumIDs = selectedAlbumIDs
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override func loadView() {
        view = NSView(
            frame: NSRect(x: 0, y: 0, width: 640, height: 540)
        )
        preferredContentSize = view.frame.size

        configureTable()
        let scroll = NSScrollView()
        scroll.documentView = tableView
        scroll.hasVerticalScroller = true
        scroll.autohidesScrollers = true
        scroll.borderType = .bezelBorder

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .small
        progressIndicator.isDisplayedWhenStopped = false

        stateLabel.textColor = .secondaryLabelColor
        stateLabel.alignment = .center
        stateLabel.maximumNumberOfLines = 0

        let stateStack = NSStackView(
            views: [progressIndicator, stateLabel]
        )
        stateStack.orientation = .vertical
        stateStack.alignment = .centerX
        stateStack.spacing = 10

        let cancelButton = NSButton(
            title: String(
                localized: "common.cancel",
                defaultValue: "Cancel"
            ),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded

        applyButton.title = String(
            localized: "common.done",
            defaultValue: "Done"
        )
        applyButton.bezelStyle = .rounded
        applyButton.keyEquivalent = "\r"
        applyButton.target = self
        applyButton.action = #selector(apply(_:))

        let actions = NSStackView(
            views: [
                NSView(),
                cancelButton,
                applyButton
            ]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 10

        let root = NSStackView(
            views: [
                scroll,
                actions
            ]
        )
        root.orientation = .vertical
        root.alignment = .leading
        root.spacing = 12
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        stateStack.translatesAutoresizingMaskIntoConstraints = false
        scroll.contentView.addSubview(stateStack)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 24
            ),
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
            scroll.widthAnchor.constraint(equalTo: root.widthAnchor),
            scroll.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 350
            ),
            actions.widthAnchor.constraint(equalTo: root.widthAnchor),
            stateStack.centerXAnchor.constraint(
                equalTo: scroll.contentView.centerXAnchor
            ),
            stateStack.centerYAnchor.constraint(
                equalTo: scroll.contentView.centerYAnchor
            ),
            stateStack.widthAnchor.constraint(
                lessThanOrEqualTo: scroll.contentView.widthAnchor,
                constant: -40
            )
        ])
        updateApplyButton()
        loadAlbums()
    }

    private func configureTable() {
        let selected = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("selected")
        )
        selected.title = ""
        selected.width = 42
        selected.minWidth = 42
        selected.maxWidth = 42
        tableView.addTableColumn(selected)

        let album = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("album")
        )
        album.title = ""
        album.width = 420
        tableView.addTableColumn(album)

        let count = NSTableColumn(
            identifier: NSUserInterfaceItemIdentifier("count")
        )
        count.title = ""
        count.width = 90
        count.minWidth = 70
        count.maxWidth = 110
        tableView.addTableColumn(count)

        tableView.headerView = nil
        tableView.rowHeight = 50
        tableView.intercellSpacing = NSSize(width: 6, height: 2)
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.dataSource = self
        tableView.delegate = self
    }

    private func loadAlbums() {
        progressIndicator.startAnimation(nil)
        stateLabel.stringValue = ""
        stateLabel.isHidden = true
        tableView.isHidden = true
        let service = photoLibraryService
        loadTask = Task { [weak self] in
            #if DEBUG
            let loaded: [LocalAlbumDescriptor]
            if ProcessInfo.processInfo.arguments.contains(
                "--demo-album-picker"
            ) {
                loaded = Self.demoAlbums
            } else {
                loaded = await Task.detached(priority: .userInitiated) {
                    service.fetchUserAlbums {
                        Task.isCancelled
                    }
                }.value
            }
            #else
            let loaded = await Task.detached(priority: .userInitiated) {
                service.fetchUserAlbums {
                    Task.isCancelled
                }
            }.value
            #endif
            guard let self, !Task.isCancelled else { return }
            albums = loaded
            selectedAlbumIDs.formIntersection(
                Set(loaded.map(\.localIdentifier))
            )
            progressIndicator.stopAnimation(nil)
            stateLabel.stringValue = String(
                localized: "home.localAlbums.emptyMessage",
                defaultValue: "Create an album in Photos, then try again."
            )
            stateLabel.isHidden = !albums.isEmpty
            tableView.isHidden = albums.isEmpty
            updateApplyButton()
        }
    }

    private func updateApplyButton() {
        applyButton.isEnabled = !selectedAlbumIDs.isEmpty
        applyButton.title = String(
            localized: "common.done",
            defaultValue: "Done"
        )
    }

    private func toggle(_ albumID: String, enabled: Bool) {
        if enabled {
            selectedAlbumIDs.insert(albumID)
        } else {
            selectedAlbumIDs.remove(albumID)
        }
        updateApplyButton()
    }

    @objc private func cancel(_ sender: Any?) {
        onCancel?()
    }

    @objc private func apply(_ sender: Any?) {
        let selected = albums.filter {
            selectedAlbumIDs.contains($0.localIdentifier)
        }
        guard !selected.isEmpty else { return }
        onApply?(selected)
    }

    #if DEBUG
    private static let demoAlbums: [LocalAlbumDescriptor] = [
        LocalAlbumDescriptor(
            localIdentifier: "demo-family",
            title: "Family",
            assetCount: 864,
            thumbnailAssetIdentifier: nil
        ),
        LocalAlbumDescriptor(
            localIdentifier: "demo-travel",
            title: "Japan 2026",
            assetCount: 327,
            thumbnailAssetIdentifier: nil
        ),
        LocalAlbumDescriptor(
            localIdentifier: "demo-studio",
            title: "Studio Selects",
            assetCount: 118,
            thumbnailAssetIdentifier: nil
        ),
        LocalAlbumDescriptor(
            localIdentifier: "demo-video",
            title: "Family Videos",
            assetCount: 74,
            thumbnailAssetIdentifier: nil
        )
    ]
    #endif
}

extension MacAlbumPickerViewController:
    NSTableViewDataSource,
    NSTableViewDelegate
{
    func numberOfRows(in tableView: NSTableView) -> Int {
        albums.count
    }

    func tableView(
        _ tableView: NSTableView,
        shouldSelectRow row: Int
    ) -> Bool {
        guard albums.indices.contains(row) else {
            return false
        }
        onOpenAlbum?(albums[row])
        return false
    }

    func tableView(
        _ tableView: NSTableView,
        viewFor tableColumn: NSTableColumn?,
        row: Int
    ) -> NSView? {
        guard albums.indices.contains(row),
              let tableColumn else {
            return nil
        }
        let album = albums[row]
        switch tableColumn.identifier.rawValue {
        case "selected":
            let identifier = NSUserInterfaceItemIdentifier(
                "album-selection"
            )
            let button: MacAlbumSelectionButton
            if let reused = tableView.makeView(
                withIdentifier: identifier,
                owner: self
            ) as? MacAlbumSelectionButton {
                button = reused
            } else {
                button = MacAlbumSelectionButton(
                    checkboxWithTitle: "",
                    target: nil,
                    action: nil
                )
                button.identifier = identifier
            }
            button.state = selectedAlbumIDs.contains(
                album.localIdentifier
            ) ? .on : .off
            button.target = self
            button.action = #selector(toggleAlbum(_:))
            button.albumID = album.localIdentifier
            return button
        case "album":
            let identifier = NSUserInterfaceItemIdentifier("album-name")
            let cell: MacAlbumNameCell
            if let reused = tableView.makeView(
                withIdentifier: identifier,
                owner: self
            ) as? MacAlbumNameCell {
                cell = reused
            } else {
                cell = MacAlbumNameCell()
                cell.identifier = identifier
            }
            cell.configure(album: album)
            loadThumbnail(for: album, into: cell)
            return cell
        default:
            let identifier = NSUserInterfaceItemIdentifier("album-count")
            let label: NSTextField
            if let reused = tableView.makeView(
                withIdentifier: identifier,
                owner: self
            ) as? NSTextField {
                label = reused
            } else {
                label = NSTextField(labelWithString: "")
                label.identifier = identifier
                label.alignment = .right
                label.textColor = .secondaryLabelColor
            }
            label.stringValue = NumberFormatter.localizedString(
                from: NSNumber(value: album.assetCount),
                number: .decimal
            )
            return label
        }
    }

    @objc private func toggleAlbum(
        _ sender: MacAlbumSelectionButton
    ) {
        guard let albumID = sender.albumID else {
            return
        }
        toggle(albumID, enabled: sender.state == .on)
    }

    private func loadThumbnail(
        for album: LocalAlbumDescriptor,
        into cell: MacAlbumNameCell
    ) {
        guard let assetID = album.thumbnailAssetIdentifier else {
            cell.showPlaceholder()
            return
        }
        let result = PHAsset.fetchAssets(
            withLocalIdentifiers: [assetID],
            options: nil
        )
        guard let asset = result.firstObject else {
            cell.showPlaceholder()
            return
        }
        cell.objectValue = album.localIdentifier
        let options = PHImageRequestOptions()
        options.deliveryMode = .opportunistic
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = false
        imageManager.requestImage(
            for: asset,
            targetSize: NSSize(width: 80, height: 80),
            contentMode: .aspectFill,
            options: options
        ) { [weak cell] image, _ in
            Task { @MainActor in
                guard cell?.objectValue as? String
                        == album.localIdentifier,
                      let image else {
                    return
                }
                cell?.imageView?.image = image
            }
        }
    }
}

@MainActor
private final class MacAlbumSelectionButton: NSButton {
    var albumID: String?
}

@MainActor
private final class MacAlbumNameCell: NSTableCellView {
    private let titleLabel = NSTextField(labelWithString: "")

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        let icon = NSImageView()
        icon.wantsLayer = true
        icon.layer?.cornerRadius = 6
        icon.layer?.masksToBounds = true
        icon.imageScaling = .scaleProportionallyUpOrDown
        imageView = icon

        titleLabel.font = .systemFont(ofSize: 13, weight: .medium)
        titleLabel.lineBreakMode = .byTruncatingTail
        textField = titleLabel

        icon.translatesAutoresizingMaskIntoConstraints = false
        titleLabel.translatesAutoresizingMaskIntoConstraints = false
        addSubview(icon)
        addSubview(titleLabel)
        NSLayoutConstraint.activate([
            icon.leadingAnchor.constraint(equalTo: leadingAnchor),
            icon.centerYAnchor.constraint(equalTo: centerYAnchor),
            icon.widthAnchor.constraint(equalToConstant: 38),
            icon.heightAnchor.constraint(equalToConstant: 38),
            titleLabel.leadingAnchor.constraint(
                equalTo: icon.trailingAnchor,
                constant: 10
            ),
            titleLabel.trailingAnchor.constraint(
                equalTo: trailingAnchor,
                constant: -6
            ),
            titleLabel.centerYAnchor.constraint(
                equalTo: centerYAnchor
            )
        ])
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(album: LocalAlbumDescriptor) {
        objectValue = album.localIdentifier
        titleLabel.stringValue = album.title
        showPlaceholder()
    }

    func showPlaceholder() {
        imageView?.image = NSImage(
            systemSymbolName: "rectangle.stack",
            accessibilityDescription: nil
        )
        imageView?.contentTintColor = .secondaryLabelColor
    }
}
