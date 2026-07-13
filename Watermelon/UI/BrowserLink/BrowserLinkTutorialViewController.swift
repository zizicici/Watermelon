import UIKit

private nonisolated enum BrowserLinkTutorialSectionID: Int, Hashable, Sendable {
    case computer = 0, folder, network, scanner
}

private nonisolated enum BrowserLinkTutorialItemID: Int, Hashable, Sendable {
    case computer = 0, folder, network, scanner
}

@MainActor
final class BrowserLinkTutorialViewController: UIViewController {
    enum CompletionGate {
        private static let key = "browser-link-tutorial.completed.v1"

        static var hasCompleted: Bool {
            UserDefaults.standard.bool(forKey: key)
        }

        static func markCompleted() {
            UserDefaults.standard.set(true, forKey: key)
        }
    }

    var onCompleted: (() -> Void)?
    var onDismissed: (() -> Void)?

    private struct Item {
        let titleKey: String.LocalizationValue
        let subtitleKey: String.LocalizationValue
        let symbolName: String
    }

    private static let sections: [BrowserLinkTutorialSectionID] = [.computer, .folder, .network, .scanner]
    private static let itemIDs: [BrowserLinkTutorialItemID] = [.computer, .folder, .network, .scanner]

    private let items: [Item] = [
        Item(
            titleKey: "link.tutorial.computer.title",
            subtitleKey: "link.tutorial.computer.subtitle",
            symbolName: "desktopcomputer"
        ),
        Item(
            titleKey: "link.tutorial.folder.title",
            subtitleKey: "link.tutorial.folder.subtitle",
            symbolName: "folder.badge.plus"
        ),
        Item(
            titleKey: "link.tutorial.network.title",
            subtitleKey: "link.connection.sameNetworkHint",
            symbolName: "point.3.connected.trianglepath.dotted"
        ),
        Item(
            titleKey: "link.scanner.title",
            subtitleKey: "link.scanner.instruction",
            symbolName: "qrcode.viewfinder"
        ),
    ]

    private let allowsDismissal: Bool
    private var revealedCount = 0
    private var didComplete = false
    private var didNotifyDismissal = false
    private var dataSource: UITableViewDiffableDataSource<BrowserLinkTutorialSectionID, BrowserLinkTutorialItemID>!

    private lazy var tableView: UITableView = {
        let tableView = UITableView(frame: .zero, style: .insetGrouped)
        tableView.backgroundColor = .appBackground
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "Cell")
        tableView.separatorStyle = .none
        tableView.translatesAutoresizingMaskIntoConstraints = false
        return tableView
    }()

    private lazy var actionButton: UIButton = {
        var configuration = UIButton.Configuration.filled()
        configuration.title = String(localized: "onboarding.button.next")
        configuration.cornerStyle = .large
        configuration.baseBackgroundColor = .appTint
        configuration.baseForegroundColor = .materialOnPrimary(dark: UIColor.Material.Green._800)
        configuration.contentInsets = .init(top: 14, leading: 0, bottom: 14, trailing: 0)
        let button = UIButton(configuration: configuration)
        button.translatesAutoresizingMaskIntoConstraints = false
        button.addTarget(self, action: #selector(didTapAction), for: .touchUpInside)
        return button
    }()

    init(allowsDismissal: Bool) {
        self.allowsDismissal = allowsDismissal
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "link.tutorial.title")
        view.backgroundColor = .appBackground
        if allowsDismissal {
            navigationItem.leftBarButtonItem = UIBarButtonItem(
                barButtonSystemItem: .close,
                target: self,
                action: #selector(close)
            )
        }

        view.addSubview(tableView)
        view.addSubview(actionButton)
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.topAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            tableView.bottomAnchor.constraint(equalTo: actionButton.topAnchor, constant: -16),
            actionButton.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 20),
            actionButton.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -20),
            actionButton.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -16),
        ])

        configureDataSource()
        revealNext()
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        guard !didNotifyDismissal, isBeingDismissed || navigationController?.isBeingDismissed == true else { return }
        didNotifyDismissal = true
        onDismissed?()
    }

    private func configureDataSource() {
        dataSource = UITableViewDiffableDataSource(tableView: tableView) { [weak self] tableView, indexPath, itemID in
            guard let self else { return UITableViewCell() }
            let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
            let item = self.items[itemID.rawValue]
            cell.selectionStyle = .none

            var content = UIListContentConfiguration.subtitleCell()
            content.text = String(localized: item.titleKey)
            content.secondaryText = String(localized: item.subtitleKey)
            content.textProperties.font = UIFont.preferredFont(forTextStyle: .headline).withWeight(.semibold)
            content.textProperties.color = .materialOnContainer(
                light: UIColor.Material.Green._900,
                dark: UIColor.Material.Green._100
            )
            content.secondaryTextProperties.font = .preferredFont(forTextStyle: .subheadline)
            content.secondaryTextProperties.color = .materialOnSurfaceVariant(
                light: UIColor.Material.Green._700,
                dark: UIColor.Material.Green._200
            )
            content.secondaryTextProperties.numberOfLines = 0
            content.textToSecondaryTextVerticalPadding = 4
            content.image = UIImage(systemName: item.symbolName)
            content.imageProperties.tintColor = content.textProperties.color
            content.imageProperties.preferredSymbolConfiguration = .init(textStyle: .title2, scale: .medium)
            content.imageToTextPadding = 16
            content.directionalLayoutMargins = .init(top: 16, leading: 16, bottom: 16, trailing: 16)
            cell.contentConfiguration = content

            var background = UIBackgroundConfiguration.listCell()
            background.backgroundColor = .materialAdaptive(
                light: UIColor.Material.Green._100,
                dark: UIColor.Material.Green._800
            )
            cell.backgroundConfiguration = background
            return cell
        }
        dataSource.defaultRowAnimation = .fade
        let snapshot = NSDiffableDataSourceSnapshot<BrowserLinkTutorialSectionID, BrowserLinkTutorialItemID>()
        dataSource.apply(snapshot, animatingDifferences: false)
    }

    private func revealNext() {
        guard revealedCount < items.count else { return }
        let section = Self.sections[revealedCount]
        let item = Self.itemIDs[revealedCount]
        revealedCount += 1

        var snapshot = dataSource.snapshot()
        snapshot.appendSections([section])
        snapshot.appendItems([item], toSection: section)
        dataSource.apply(snapshot, animatingDifferences: ConsideringUser.animated)
        if revealedCount == items.count {
            actionButton.configuration?.title = String(localized: "link.tutorial.button.scan")
        }
    }

    @objc private func didTapAction() {
        if revealedCount < items.count {
            revealNext()
        } else {
            guard !didComplete else { return }
            didComplete = true
            actionButton.isEnabled = false
            onCompleted?()
        }
    }

    @objc private func close() {
        dismiss(animated: ConsideringUser.animated)
    }
}
