//
//  OnboardingViewController.swift
//  Watermelon
//

import UIKit

fileprivate nonisolated enum OnboardingSectionID: Int, Hashable, Sendable {
    case item0 = 0, item1, item2, item3
}

fileprivate nonisolated enum OnboardingItemID: Int, Hashable, Sendable {
    case item0 = 0, item1, item2, item3
}

@MainActor
final class OnboardingViewController: UIViewController {

    enum CompletionGate {
        private static let key = "onboarding.completed.v1"

        static var hasCompleted: Bool {
            UserDefaults.standard.bool(forKey: key)
        }

        static func markCompleted() {
            UserDefaults.standard.set(true, forKey: key)
        }
    }

    var onCompleted: (() -> Void)?

    private struct CardPalette {
        let background: UIColor
        let title: UIColor
        let subtitle: UIColor

        // M3 primary container — main brand tint.
        static let primary = CardPalette(
            background: .materialAdaptive(light: UIColor.Material.Green._100, dark: UIColor.Material.Green._800),
            title: .materialOnContainer(light: UIColor.Material.Green._900, dark: UIColor.Material.Green._100),
            subtitle: .materialOnSurfaceVariant(light: UIColor.Material.Green._700, dark: UIColor.Material.Green._200)
        )

        // M3 tertiary/warning container — used to make the single-client warning stand out.
        static let warning = CardPalette(
            background: .materialAdaptive(light: UIColor.Material.Amber._100, dark: UIColor.Material.Amber._800),
            title: .materialOnContainer(light: UIColor.Material.Amber._900, dark: UIColor.Material.Amber._100),
            subtitle: .materialOnSurfaceVariant(light: UIColor.Material.Amber._800, dark: UIColor.Material.Amber._200)
        )
    }

    private struct OnboardingItem {
        let titleKey: String.LocalizationValue
        let subtitleKey: String.LocalizationValue
        let symbolName: String
        let palette: CardPalette
    }

    private let items: [OnboardingItem] = [
        OnboardingItem(
            titleKey: "onboarding.item.live_photo.title",
            subtitleKey: "onboarding.item.live_photo.subtitle",
            symbolName: "livephoto",
            palette: .primary
        ),
        OnboardingItem(
            titleKey: "onboarding.item.edited.title",
            subtitleKey: "onboarding.item.edited.subtitle",
            symbolName: "slider.horizontal.3",
            palette: .primary
        ),
        OnboardingItem(
            titleKey: "onboarding.item.dedup.title",
            subtitleKey: "onboarding.item.dedup.subtitle",
            symbolName: "square.on.square",
            palette: .primary
        ),
        OnboardingItem(
            titleKey: "onboarding.item.single_client.title",
            subtitleKey: "onboarding.item.single_client.subtitle",
            symbolName: "exclamationmark.triangle.fill",
            palette: .warning
        ),
    ]

    private static let allSections: [OnboardingSectionID] = [.item0, .item1, .item2, .item3]
    private static let allItems: [OnboardingItemID] = [.item0, .item1, .item2, .item3]

    private var revealedCount = 0
    private var diffableDataSource: UITableViewDiffableDataSource<OnboardingSectionID, OnboardingItemID>!

    private lazy var tableView: UITableView = {
        let tv = UITableView(frame: .zero, style: .insetGrouped)
        tv.backgroundColor = .appBackground
        tv.register(UITableViewCell.self, forCellReuseIdentifier: "Cell")
        tv.translatesAutoresizingMaskIntoConstraints = false
        tv.separatorStyle = .none
        return tv
    }()

    private lazy var actionButton: UIButton = {
        var config = UIButton.Configuration.filled()
        config.title = String(localized: "onboarding.button.next")
        config.cornerStyle = .large
        config.baseBackgroundColor = .appTint
        config.baseForegroundColor = .materialOnPrimary(dark: UIColor.Material.Green._800)
        config.contentInsets = NSDirectionalEdgeInsets(top: 14, leading: 0, bottom: 14, trailing: 0)
        let button = UIButton(configuration: config)
        button.translatesAutoresizingMaskIntoConstraints = false
        button.addTarget(self, action: #selector(didTapAction), for: .touchUpInside)
        return button
    }()

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "onboarding.title")
        view.backgroundColor = .appBackground

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

        configureDiffableDataSource()
        revealNext()
    }

    // MARK: - DataSource

    private func configureDiffableDataSource() {
        diffableDataSource = UITableViewDiffableDataSource(tableView: tableView) { [weak self] tableView, indexPath, itemID in
            guard let self else { return UITableViewCell() }
            let cell = tableView.dequeueReusableCell(withIdentifier: "Cell", for: indexPath)
            cell.selectionStyle = .none
            let item = self.items[itemID.rawValue]
            let palette = item.palette

            var config = UIListContentConfiguration.subtitleCell()
            config.text = String(localized: item.titleKey)
            config.secondaryText = String(localized: item.subtitleKey)
            config.textProperties.font = UIFont.preferredFont(forTextStyle: .headline).withWeight(.semibold)
            config.textProperties.color = palette.title
            config.secondaryTextProperties.font = .preferredFont(forTextStyle: .subheadline)
            config.secondaryTextProperties.color = palette.subtitle
            config.secondaryTextProperties.numberOfLines = 0
            config.textToSecondaryTextVerticalPadding = 4

            config.image = UIImage(systemName: item.symbolName)
            config.imageProperties.tintColor = palette.title
            config.imageProperties.preferredSymbolConfiguration = UIImage.SymbolConfiguration(textStyle: .title2, scale: .medium)
            config.imageToTextPadding = 16
            config.directionalLayoutMargins = NSDirectionalEdgeInsets(top: 16, leading: 16, bottom: 16, trailing: 16)
            cell.contentConfiguration = config

            var bgConfig = UIBackgroundConfiguration.listCell()
            bgConfig.backgroundColor = palette.background
            cell.backgroundConfiguration = bgConfig
            return cell
        }
        diffableDataSource.defaultRowAnimation = .fade

        let snapshot = NSDiffableDataSourceSnapshot<OnboardingSectionID, OnboardingItemID>()
        diffableDataSource.apply(snapshot, animatingDifferences: false)
    }

    // MARK: - Actions

    @objc
    private func didTapAction() {
        if revealedCount < items.count {
            revealNext()
        } else {
            onCompleted?()
        }
    }

    private func revealNext() {
        guard revealedCount < items.count else { return }

        let section = Self.allSections[revealedCount]
        let item = Self.allItems[revealedCount]
        revealedCount += 1

        var snapshot = diffableDataSource.snapshot()
        snapshot.appendSections([section])
        snapshot.appendItems([item], toSection: section)
        diffableDataSource.apply(snapshot, animatingDifferences: ConsideringUser.animated)

        if revealedCount == items.count {
            actionButton.configuration?.title = String(localized: "onboarding.button.start")
        }
    }
}
