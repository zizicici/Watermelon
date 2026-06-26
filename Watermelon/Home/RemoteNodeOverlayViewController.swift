import SnapKit
import UIKit

private enum RemoteNodeOverlayStyle {
    static let buttonColor = UIColor.materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)

    static func plainButtonConfiguration(title: String, systemImageName: String? = nil) -> UIButton.Configuration {
        var configuration = UIButton.Configuration.plain()
        configuration.baseForegroundColor = buttonColor
        configuration.title = title
        if let systemImageName {
            configuration.image = UIImage(systemName: systemImageName)
            configuration.imagePlacement = .leading
            configuration.imagePadding = 6
            configuration.preferredSymbolConfigurationForImage = UIImage.SymbolConfiguration(pointSize: 13, weight: .regular)
        }
        return configuration
    }
}

@MainActor
final class RemoteNodeOverlayViewController: UIViewController {
    enum Mode {
        case emptySetup
        case profileSelection
        case progress(message: String, showsDisconnect: Bool)
    }

    private let progressStack = UIStackView()
    private let progressSpinner = UIActivityIndicatorView(style: .medium)
    private let progressLabel = UILabel()
    private let disconnectButton = UIButton(type: .system)

    private let emptySetupStack = UIStackView()
    private let emptySetupLabel = UILabel()
    private let emptySetupOrLabel = UILabel()
    private var emptySetupButtons: [UIButton] = []

    private let profileSelectionStack = UIStackView()
    private let profileSelectionOrLabel = UILabel()
    private let connectNodeButton = UIButton(type: .system)
    private let addNodeButton = UIButton(type: .system)
    private var profileSelectionButtons: [UIButton] { [connectNodeButton, addNodeButton] }

    private var savedProfiles: [ServerProfileRecord] = []
    private var profileReachabilityByID: [Int64: ProfileReachabilityService.Reachability] = [:]
    private var interactionEnabled = true

    var onProfileSelected: ((ServerProfileRecord) -> Void)?
    var onCreateDestinationSelected: ((NewStorageDestination) -> Void)?
    var onDisconnect: (() -> Void)?

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        configureProgressView()
        configureEmptySetupView()
        configureProfileSelectionView()
    }

    func render(
        mode: Mode,
        profiles: [ServerProfileRecord],
        reachability: (Int64) -> ProfileReachabilityService.Reachability,
        isInteractionEnabled: Bool
    ) {
        interactionEnabled = isInteractionEnabled
        savedProfiles = profiles
        profileReachabilityByID = Dictionary(uniqueKeysWithValues: profiles.compactMap { profile in
            profile.id.map { ($0, reachability($0)) }
        })

        switch mode {
        case .progress(let message, let showsDisconnect):
            progressLabel.text = message
            progressStack.isHidden = false
            progressSpinner.startAnimating()
            disconnectButton.isHidden = !showsDisconnect
            disconnectButton.isEnabled = isInteractionEnabled
            disconnectButton.alpha = isInteractionEnabled ? 1.0 : 0.45
            emptySetupStack.isHidden = true
            profileSelectionStack.isHidden = true
        case .emptySetup:
            progressSpinner.stopAnimating()
            progressStack.isHidden = true
            disconnectButton.isHidden = true
            emptySetupStack.isHidden = false
            profileSelectionStack.isHidden = true
            updateButtons(emptySetupButtons, isEnabled: isInteractionEnabled)
        case .profileSelection:
            progressSpinner.stopAnimating()
            progressStack.isHidden = true
            disconnectButton.isHidden = true
            emptySetupStack.isHidden = true
            profileSelectionStack.isHidden = false
            connectNodeButton.menu = makeConnectNodeMenu()
            addNodeButton.menu = makeAddNodeMenu()
            updateButtons(profileSelectionButtons, isEnabled: isInteractionEnabled)
            connectNodeButton.isEnabled = isInteractionEnabled && !savedProfiles.isEmpty
            connectNodeButton.alpha = connectNodeButton.isEnabled ? 1.0 : 0.45
        }
    }

    func stopProgressAnimation() {
        progressSpinner.stopAnimating()
    }

    private func configureProgressView() {
        progressLabel.textAlignment = .center
        progressLabel.numberOfLines = 0
        progressLabel.font = .systemFont(ofSize: 15, weight: .medium)
        progressLabel.textColor = .secondaryLabel

        progressStack.axis = .vertical
        progressStack.spacing = 12
        progressStack.alignment = .center
        progressStack.addArrangedSubview(progressSpinner)
        progressStack.addArrangedSubview(progressLabel)
        progressStack.addArrangedSubview(disconnectButton)
        configureDisconnectButton()

        view.addSubview(progressStack)
        progressStack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(16)
        }
    }

    private func configureDisconnectButton() {
        disconnectButton.configuration = RemoteNodeOverlayStyle.plainButtonConfiguration(
            title: String(localized: "home.menu.disconnect")
        )
        disconnectButton.isHidden = true
        disconnectButton.addTarget(self, action: #selector(disconnectTapped), for: .touchUpInside)
    }

    private func configureEmptySetupView() {
        configurePromptLabel(emptySetupLabel)
        emptySetupLabel.text = String(localized: "home.overlay.backupTo")

        configurePromptLabel(emptySetupOrLabel)
        emptySetupOrLabel.text = String(localized: "home.overlay.or")

        let externalStorageButton = makeTextButton(
            title: String(localized: "home.menu.externalStorage"),
            systemImageName: StorageType.externalVolume.symbolName
        ) { [weak self] in
            self?.onCreateDestinationSelected?(.externalVolume)
        }
        let networkButton = makeTextButton(
            title: String(localized: "home.overlay.network"),
            systemImageName: "network",
            menu: makeNetworkNodeMenu()
        )
        emptySetupButtons = [externalStorageButton, networkButton]

        configureCenteredStack(emptySetupStack)
        emptySetupStack.addArrangedSubview(emptySetupLabel)
        emptySetupStack.addArrangedSubview(externalStorageButton)
        emptySetupStack.addArrangedSubview(emptySetupOrLabel)
        emptySetupStack.addArrangedSubview(networkButton)

        view.addSubview(emptySetupStack)
        emptySetupStack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(16)
        }
    }

    private func configureProfileSelectionView() {
        configurePromptLabel(profileSelectionOrLabel)
        profileSelectionOrLabel.text = String(localized: "home.overlay.or")

        connectNodeButton.configuration = RemoteNodeOverlayStyle.plainButtonConfiguration(
            title: String(localized: "home.overlay.connectNode"),
            systemImageName: "link"
        )
        connectNodeButton.showsMenuAsPrimaryAction = true

        addNodeButton.configuration = RemoteNodeOverlayStyle.plainButtonConfiguration(
            title: String(localized: "home.menu.addStorage"),
            systemImageName: "plus.circle"
        )
        addNodeButton.showsMenuAsPrimaryAction = true

        configureCenteredStack(profileSelectionStack)
        profileSelectionStack.addArrangedSubview(connectNodeButton)
        profileSelectionStack.addArrangedSubview(profileSelectionOrLabel)
        profileSelectionStack.addArrangedSubview(addNodeButton)

        view.addSubview(profileSelectionStack)
        profileSelectionStack.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(16)
        }
    }

    private func configureCenteredStack(_ stack: UIStackView) {
        stack.axis = .vertical
        stack.spacing = 12
        stack.alignment = .center
        stack.isHidden = true
    }

    private func configurePromptLabel(_ label: UILabel) {
        label.textAlignment = .center
        label.numberOfLines = 0
        label.font = .systemFont(ofSize: 15, weight: .medium)
        label.textColor = .secondaryLabel
    }

    private func makeTextButton(
        title: String,
        systemImageName: String? = nil,
        menu: UIMenu? = nil,
        action: (() -> Void)? = nil
    ) -> UIButton {
        let button = UIButton(type: .system)
        button.configuration = RemoteNodeOverlayStyle.plainButtonConfiguration(
            title: title,
            systemImageName: systemImageName
        )
        button.showsMenuAsPrimaryAction = menu != nil
        button.menu = menu
        if let action {
            button.addAction(UIAction { _ in action() }, for: .touchUpInside)
        }
        return button
    }

    private func updateButtons(_ buttons: [UIButton], isEnabled: Bool) {
        buttons.forEach { button in
            button.isEnabled = isEnabled
            button.alpha = isEnabled ? 1.0 : 0.45
        }
    }

    private func makeAddNodeMenu() -> UIMenu {
        UIMenu(children: [
            UIAction(title: String(localized: "home.menu.externalStorage"), image: UIImage(systemName: StorageType.externalVolume.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.externalVolume)
            },
            makeSMBNodeMenu(),
            UIAction(title: "WebDAV", image: UIImage(systemName: StorageType.webdav.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.webdav)
            },
            UIAction(title: "SFTP", image: UIImage(systemName: StorageType.sftp.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.sftp)
            },
            UIAction(title: "S3", image: UIImage(systemName: StorageType.s3.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.s3)
            }
        ])
    }

    private func makeConnectNodeMenu() -> UIMenu {
        let sections = StorageType.sectionDisplayOrder.compactMap { type -> UIMenu? in
            let actions = savedProfiles.compactMap { profile -> UIAction? in
                guard profile.resolvedStorageType == type else { return nil }
                var subtitle = profile.storageProfile.displaySubtitle
                if let id = profile.id, profileReachabilityByID[id] == .unreachable {
                    subtitle = String(localized: "home.menu.offlineMarker") + subtitle
                }
                return UIAction(
                    title: profile.name,
                    subtitle: subtitle,
                    image: UIImage(systemName: type.symbolName)
                ) { [weak self] _ in
                    self?.onProfileSelected?(profile)
                }
            }
            guard !actions.isEmpty else { return nil }
            return UIMenu(title: type.sectionHeaderText, options: .displayInline, children: actions)
        }
        return UIMenu(children: sections)
    }

    private func makeNetworkNodeMenu() -> UIMenu {
        UIMenu(children: [
            makeSMBNodeMenu(),
            UIAction(title: "WebDAV", image: UIImage(systemName: StorageType.webdav.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.webdav)
            },
            UIAction(title: "SFTP", image: UIImage(systemName: StorageType.sftp.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.sftp)
            },
            UIAction(title: "S3", image: UIImage(systemName: StorageType.s3.symbolName)) { [weak self] _ in
                self?.onCreateDestinationSelected?(.s3)
            }
        ])
    }

    private func makeSMBNodeMenu() -> UIMenu {
        UIMenu(
            title: "SMB",
            image: UIImage(systemName: StorageType.smb.symbolName),
            children: [
                UIAction(title: String(localized: "home.menu.smbManual")) { [weak self] _ in
                    self?.onCreateDestinationSelected?(.smb)
                },
                UIAction(title: String(localized: "home.menu.smbDiscovery"), image: UIImage(systemName: "bonjour")) { [weak self] _ in
                    self?.onCreateDestinationSelected?(.smbDiscovery)
                }
            ]
        )
    }

    @objc private func disconnectTapped() {
        guard interactionEnabled else { return }
        onDisconnect?()
    }
}
