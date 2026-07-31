import AppKit

enum MacOnboardingCompletionGate {
    private static let key = "onboarding.completed.v1"

    static var hasCompleted: Bool {
        UserDefaults.standard.bool(forKey: key)
    }

    static func markCompleted() {
        UserDefaults.standard.set(true, forKey: key)
    }
}

@MainActor
final class MacOnboardingViewController: NSViewController {
    var onContinue: (() -> Void)?

    private let isFirstLaunch: Bool
    private let primaryButton = NSButton()

    init(isFirstLaunch: Bool) {
        self.isFirstLaunch = isFirstLaunch
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        let root = NSView()
        root.wantsLayer = true
        root.layer?.backgroundColor = NSColor.windowBackgroundColor.cgColor
        view = root

        let appIconImage = Bundle.main.url(
            forResource: "AppIcon",
            withExtension: "icns"
        ).flatMap(NSImage.init(contentsOf:))
            ?? NSApp.applicationIconImage
            ?? NSImage(
                systemSymbolName: "externaldrive.fill",
                accessibilityDescription: nil
            )
            ?? NSImage()
        let appIcon = NSImageView(image: appIconImage)
        appIcon.imageScaling = .scaleProportionallyUpOrDown
        appIcon.translatesAutoresizingMaskIntoConstraints = false

        let titleLabel = NSTextField(
            labelWithString: String(
                localized: "onboarding.title",
                defaultValue: "Before You Start"
            )
        )
        titleLabel.font = .systemFont(ofSize: 30, weight: .bold)
        titleLabel.alignment = .center

        let header = NSStackView(
            views: [appIcon, titleLabel]
        )
        header.orientation = .vertical
        header.alignment = .centerX
        header.spacing = 8

        let featureList = NSStackView(views: [
            makeFeatureRow(
                symbol: "livephoto",
                title: String(
                    localized: "onboarding.item.live_photo.title"
                ),
                detail: nil,
                warning: false
            ),
            makeFeatureRow(
                symbol: "slider.horizontal.3",
                title: String(
                    localized: "onboarding.item.edited.title"
                ),
                detail: nil,
                warning: false
            ),
            makeFeatureRow(
                symbol: "square.on.square",
                title: String(
                    localized: "onboarding.item.dedup.title"
                ),
                detail: nil,
                warning: false
            ),
            makeFeatureRow(
                symbol: "exclamationmark.triangle.fill",
                title: String(
                    localized:
                        "onboarding.item.single_client.title"
                ),
                detail: nil,
                warning: true
            ),
        ])
        featureList.orientation = .vertical
        featureList.alignment = .centerX
        featureList.spacing = 0

        primaryButton.title = isFirstLaunch
            ? String(
                localized: "onboarding.button.start",
                defaultValue: "Get Started"
            )
            : String(
                localized: "common.done",
                defaultValue: "Done"
            )
        primaryButton.bezelStyle = .rounded
        primaryButton.controlSize = .large
        primaryButton.bezelColor = .wmMaterialPrimary
        primaryButton.keyEquivalent = "\r"
        primaryButton.target = self
        primaryButton.action = #selector(continueToApp(_:))

        let content = NSStackView(
            views: [header, featureList, primaryButton]
        )
        content.orientation = .vertical
        content.alignment = .centerX
        content.spacing = 22
        content.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(content)

        NSLayoutConstraint.activate([
            appIcon.widthAnchor.constraint(equalToConstant: 76),
            appIcon.heightAnchor.constraint(equalToConstant: 76),
            featureList.widthAnchor.constraint(equalToConstant: 572),
            primaryButton.widthAnchor.constraint(equalToConstant: 572),
            content.topAnchor.constraint(equalTo: root.topAnchor, constant: 34),
            content.leadingAnchor.constraint(
                greaterThanOrEqualTo: root.leadingAnchor,
                constant: 30
            ),
            content.trailingAnchor.constraint(
                lessThanOrEqualTo: root.trailingAnchor,
                constant: -30
            ),
            content.centerXAnchor.constraint(equalTo: root.centerXAnchor),
            content.bottomAnchor.constraint(
                lessThanOrEqualTo: root.bottomAnchor,
                constant: -30
            ),
        ])
    }

    private func makeFeatureRow(
        symbol: String,
        title: String,
        detail: String?,
        warning: Bool
    ) -> NSView {
        let symbolView = NSImageView(
            image: NSImage(
                systemSymbolName: symbol,
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        symbolView.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 21,
            weight: .medium
        )
        symbolView.contentTintColor = warning
            ? .wmMaterialOnWarningContainer
            : .wmMaterialPrimary
        symbolView.translatesAutoresizingMaskIntoConstraints = false

        let titleLabel = NSTextField(wrappingLabelWithString: title)
        titleLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        titleLabel.textColor = warning
            ? .wmMaterialOnWarningContainer
            : .labelColor
        titleLabel.maximumNumberOfLines = 2
        titleLabel.lineBreakMode = .byWordWrapping

        let labels = NSStackView(views: [titleLabel])
        if let detail, !detail.isEmpty {
            let detailLabel = NSTextField(
                wrappingLabelWithString: detail
            )
            detailLabel.font = .systemFont(ofSize: 11.5)
            detailLabel.textColor = warning
                ? .wmMaterialWarningDetail
                : .secondaryLabelColor
            detailLabel.maximumNumberOfLines = 2
            labels.addArrangedSubview(detailLabel)
        }
        labels.orientation = .vertical
        labels.alignment = .leading
        labels.spacing = 4
        labels.translatesAutoresizingMaskIntoConstraints = false

        let row = NSStackView(views: [symbolView, labels])
        row.orientation = .horizontal
        row.alignment = .top
        row.spacing = 12
        row.edgeInsets = NSEdgeInsets(
            top: 12,
            left: 4,
            bottom: 12,
            right: 4
        )

        let container = NSView()
        let separator = NSBox()
        separator.boxType = .separator
        container.addSubview(row)
        container.addSubview(separator)
        row.translatesAutoresizingMaskIntoConstraints = false
        separator.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            symbolView.widthAnchor.constraint(equalToConstant: 28),
            symbolView.heightAnchor.constraint(equalToConstant: 28),
            labels.widthAnchor.constraint(lessThanOrEqualToConstant: 502),
            row.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            row.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            row.topAnchor.constraint(equalTo: container.topAnchor),
            separator.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            separator.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            separator.topAnchor.constraint(equalTo: row.bottomAnchor),
            separator.bottomAnchor.constraint(equalTo: container.bottomAnchor),
            container.widthAnchor.constraint(equalToConstant: 572),
            container.heightAnchor.constraint(greaterThanOrEqualToConstant: 68),
        ])
        return container
    }

    @objc
    private func continueToApp(_ sender: Any?) {
        onContinue?()
    }
}
