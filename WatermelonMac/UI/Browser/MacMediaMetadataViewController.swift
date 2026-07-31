import AppKit

@MainActor
final class MacMediaMetadataViewController: NSViewController {
    typealias Loader = () async throws -> MediaMetadataDocument?

    private let loader: Loader
    private let stateStack = NSStackView()
    private let stateImageView = NSImageView()
    private let stateTitle = NSTextField(labelWithString: "")
    private let stateMessage = NSTextField(
        wrappingLabelWithString: ""
    )
    private let progressIndicator = NSProgressIndicator()
    private let scrollView = NSScrollView()
    private let documentView = MacMetadataDocumentView()
    private let documentStack = NSStackView()
    private var loadTask: Task<Void, Never>?

    init(loader: @escaping Loader) {
        self.loader = loader
        super.init(nibName: nil, bundle: nil)
        title = String(localized: "mediaBrowser.info.title")
        preferredContentSize = NSSize(width: 640, height: 580)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override func loadView() {
        view = NSView()
        view.wantsLayer = true
        view.layer?.backgroundColor =
            NSColor.windowBackgroundColor.cgColor
        configureState()
        configureDocument()

        let closeButton = NSButton(
            title: String(localized: "common.close"),
            target: self,
            action: #selector(close)
        )
        closeButton.bezelStyle = .rounded
        closeButton.keyEquivalent = "\u{1b}"

        let footer = NSStackView(
            views: [NSView(), closeButton]
        )
        footer.orientation = .horizontal
        footer.alignment = .centerY

        let content = NSView()
        content.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(scrollView)
        content.addSubview(stateStack)

        let root = NSStackView(views: [content, footer])
        root.orientation = .vertical
        root.alignment = .width
        root.spacing = 14
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)

        NSLayoutConstraint.activate([
            root.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 20
            ),
            root.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 22
            ),
            root.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -22
            ),
            root.bottomAnchor.constraint(
                equalTo: view.bottomAnchor,
                constant: -18
            ),
            content.heightAnchor.constraint(
                greaterThanOrEqualToConstant: 440
            ),
            scrollView.topAnchor.constraint(
                equalTo: content.topAnchor
            ),
            scrollView.leadingAnchor.constraint(
                equalTo: content.leadingAnchor
            ),
            scrollView.trailingAnchor.constraint(
                equalTo: content.trailingAnchor
            ),
            scrollView.bottomAnchor.constraint(
                equalTo: content.bottomAnchor
            ),
            stateStack.centerXAnchor.constraint(
                equalTo: content.centerXAnchor
            ),
            stateStack.centerYAnchor.constraint(
                equalTo: content.centerYAnchor
            ),
            stateStack.widthAnchor.constraint(
                lessThanOrEqualTo: content.widthAnchor,
                constant: -80
            )
        ])
        showLoading()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.title = title ?? String(
            localized: "mediaBrowser.info.title"
        )
        loadDocument()
    }

    override func viewDidLayout() {
        super.viewDidLayout()
        layoutDocument()
    }

    override func viewWillDisappear() {
        super.viewWillDisappear()
        loadTask?.cancel()
        loadTask = nil
    }

    private func configureState() {
        progressIndicator.style = .spinning
        progressIndicator.controlSize = .regular
        progressIndicator.isDisplayedWhenStopped = false

        stateImageView.image = NSImage(
            systemSymbolName: "info.circle",
            accessibilityDescription: nil
        )
        stateImageView.contentTintColor = .secondaryLabelColor
        stateImageView.isHidden = true

        stateTitle.font = .systemFont(ofSize: 15, weight: .semibold)
        stateTitle.alignment = .center
        stateMessage.textColor = .secondaryLabelColor
        stateMessage.alignment = .center

        stateStack.setViews(
            [
                progressIndicator,
                stateImageView,
                stateTitle,
                stateMessage
            ],
            in: .center
        )
        stateStack.orientation = .vertical
        stateStack.alignment = .centerX
        stateStack.spacing = 8
        stateStack.translatesAutoresizingMaskIntoConstraints = false
    }

    private func configureDocument() {
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
        scrollView.translatesAutoresizingMaskIntoConstraints = false

        documentStack.orientation = .vertical
        documentStack.alignment = .width
        documentStack.spacing = 22
        documentView.addSubview(documentStack)
        scrollView.documentView = documentView
    }

    private func loadDocument() {
        guard loadTask == nil else { return }
        showLoading()
        loadTask = Task { [weak self] in
            guard let self else { return }
            do {
                let document = try await loader()
                try Task.checkCancellation()
                guard let document else {
                    showUnavailable()
                    return
                }
                show(document)
            } catch is CancellationError {
                return
            } catch {
                showUnavailable(message: error.localizedDescription)
            }
        }
    }

    private func showLoading() {
        scrollView.isHidden = true
        stateStack.isHidden = false
        stateImageView.isHidden = true
        stateTitle.stringValue = String(
            localized: "mediaBrowser.info.loading"
        )
        stateMessage.stringValue = ""
        progressIndicator.startAnimation(nil)
    }

    private func showUnavailable(message: String? = nil) {
        progressIndicator.stopAnimation(nil)
        scrollView.isHidden = true
        stateStack.isHidden = false
        stateImageView.isHidden = false
        stateTitle.stringValue = String(
            localized: "mediaBrowser.info.unavailable.title"
        )
        stateMessage.stringValue = message ?? String(
            localized: "mediaBrowser.info.unavailable.message"
        )
    }

    private func show(_ document: MediaMetadataDocument) {
        progressIndicator.stopAnimation(nil)
        stateStack.isHidden = true
        documentStack.arrangedSubviews.forEach {
            documentStack.removeArrangedSubview($0)
            $0.removeFromSuperview()
        }
        for section in document.sections where !section.rows.isEmpty {
            let sectionView = makeSection(section)
            documentStack.addArrangedSubview(sectionView)
            sectionView.widthAnchor.constraint(
                equalTo: documentStack.widthAnchor
            ).isActive = true
        }
        scrollView.isHidden = false
        layoutDocument()
    }

    private func makeSection(
        _ section: MediaMetadataDocument.Section
    ) -> NSView {
        let title = NSTextField(labelWithString: section.title)
        title.font = .systemFont(ofSize: 13, weight: .semibold)
        title.textColor = .wmMaterialPrimary
        title.alignment = .left

        let rows = section.rows.map { row -> [NSView] in
            let label = NSTextField(labelWithString: row.label)
            label.font = .systemFont(ofSize: 12, weight: .medium)
            label.textColor = .secondaryLabelColor
            label.alignment = .right
            label.lineBreakMode = .byWordWrapping
            label.maximumNumberOfLines = 0

            let value = NSTextField(
                wrappingLabelWithString: row.value
            )
            value.font = .systemFont(ofSize: 12)
            value.isSelectable = true
            value.allowsEditingTextAttributes = false
            value.lineBreakMode = .byWordWrapping
            value.maximumNumberOfLines = 0
            return [label, value]
        }
        let grid = NSGridView(views: rows)
        grid.columnSpacing = 16
        grid.rowSpacing = 9
        grid.column(at: 0).width = 150
        grid.column(at: 0).xPlacement = .trailing
        grid.column(at: 1).xPlacement = .fill
        grid.rowAlignment = .firstBaseline

        let stack = NSStackView(views: [title, grid])
        stack.orientation = .vertical
        stack.alignment = .width
        stack.spacing = 10
        title.widthAnchor.constraint(
            equalTo: stack.widthAnchor
        ).isActive = true
        grid.widthAnchor.constraint(
            equalTo: stack.widthAnchor
        ).isActive = true
        return stack
    }

    private func layoutDocument() {
        guard !scrollView.isHidden else { return }
        let width = max(scrollView.contentSize.width, 1)
        documentStack.frame = NSRect(
            x: 4,
            y: 4,
            width: max(width - 18, 1),
            height: documentStack.frame.height
        )
        documentStack.layoutSubtreeIfNeeded()
        let contentHeight = documentStack.fittingSize.height
        documentView.frame = NSRect(
            x: 0,
            y: 0,
            width: width,
            height: max(
                contentHeight + 20,
                scrollView.contentSize.height
            )
        )
        documentStack.frame.size.height = contentHeight
    }

    @objc private func close() {
        dismiss(nil)
    }
}

private final class MacMetadataDocumentView: NSView {
    override var isFlipped: Bool { true }
}
