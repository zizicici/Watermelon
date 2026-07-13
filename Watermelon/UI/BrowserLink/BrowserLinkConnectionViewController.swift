import SnapKit
import UIKit

@MainActor
final class BrowserLinkSessionNavigationController: UINavigationController {
    var onSessionEnded: (() -> Void)?
    private var didEndSession = false

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if isBeingDismissed || presentingViewController == nil {
            endSessionIfNeeded()
        }
    }

    private func endSessionIfNeeded() {
        guard !didEndSession else { return }
        didEndSession = true
        onSessionEnded?()
    }
}

@MainActor
final class BrowserLinkConnectionViewController: UIViewController {
    var onAuthenticated: ((BrowserLinkClient) -> Void)?
    private let client: BrowserLinkClient
    private let symbolView = UIImageView()
    private let titleLabel = UILabel()
    private let detailLabel = UILabel()
    private let actionButton: UIButton = {
        var configuration = UIButton.Configuration.filled()
        configuration.cornerStyle = .large
        configuration.baseBackgroundColor = .appTint
        configuration.contentInsets = .init(top: 13, leading: 24, bottom: 13, trailing: 24)
        configuration.imagePadding = 8
        return UIButton(configuration: configuration)
    }()
    private var didStart = false
    private var didNotifyAuthenticated = false
    private var handedOff = false

    init(pairing: BrowserLinkPairing) {
        self.client = BrowserLinkClient(pairing: pairing)
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "link.connection.navigationTitle")
        view.backgroundColor = .appBackground
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .close,
            target: self,
            action: #selector(close)
        )
        buildUI()
        client.onStateChange = { [weak self] state in self?.render(state) }
        render(.connecting)
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        guard !didStart else { return }
        didStart = true
        client.start()
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if !handedOff, isBeingDismissed || navigationController?.isBeingDismissed == true {
            client.stop()
        }
    }

    private func buildUI() {
        symbolView.contentMode = .scaleAspectFit
        symbolView.preferredSymbolConfiguration = UIImage.SymbolConfiguration(pointSize: 46, weight: .medium)
        symbolView.tintColor = .appTint

        titleLabel.font = .preferredFont(forTextStyle: .title2)
        titleLabel.adjustsFontForContentSizeCategory = true
        titleLabel.textAlignment = .center
        titleLabel.numberOfLines = 0

        detailLabel.font = .preferredFont(forTextStyle: .body)
        detailLabel.adjustsFontForContentSizeCategory = true
        detailLabel.textColor = .secondaryLabel
        detailLabel.textAlignment = .center
        detailLabel.numberOfLines = 0

        actionButton.addTarget(self, action: #selector(close), for: .touchUpInside)

        let symbolContainer = UIView()
        symbolContainer.addSubview(symbolView)
        symbolView.snp.makeConstraints { $0.center.equalToSuperview() }
        symbolContainer.snp.makeConstraints { $0.height.equalTo(72) }

        let stack = UIStackView(arrangedSubviews: [symbolContainer, titleLabel, detailLabel, actionButton])
        stack.axis = .vertical
        stack.alignment = .fill
        stack.spacing = 18
        stack.setCustomSpacing(28, after: detailLabel)
        view.addSubview(stack)
        stack.snp.makeConstraints { make in
            make.centerY.equalTo(view.safeAreaLayoutGuide)
            make.leading.trailing.equalTo(view.safeAreaLayoutGuide).inset(32)
        }
    }

    private func render(_ state: BrowserLinkClientState) {
        actionButton.isHidden = false
        detailLabel.text = String(localized: "link.connection.sameNetworkHint")
        switch state {
        case .connecting:
            symbolView.image = UIImage(systemName: "link")
            titleLabel.text = String(localized: "link.connection.connecting")
            configureActionButton(title: String(localized: "common.cancel"), isLoading: true)
        case .waitingForDesktop:
            symbolView.image = UIImage(systemName: "desktopcomputer")
            titleLabel.text = String(localized: "link.connection.waiting")
            configureActionButton(title: String(localized: "common.cancel"), isLoading: true)
        case .negotiating:
            symbolView.image = UIImage(systemName: "point.3.connected.trianglepath.dotted")
            titleLabel.text = String(localized: "link.connection.negotiating")
            configureActionButton(title: String(localized: "common.cancel"), isLoading: true)
        case .authenticating:
            symbolView.image = UIImage(systemName: "lock.shield")
            titleLabel.text = String(localized: "link.connection.authenticating")
            configureActionButton(title: String(localized: "common.cancel"), isLoading: true)
        case .connected:
            symbolView.image = UIImage(systemName: "checkmark.circle.fill")
            symbolView.tintColor = .systemGreen
            titleLabel.text = String(localized: "link.connection.connected")
            detailLabel.text = String(localized: "link.connection.connectedDetail")
            configureActionButton(title: String(localized: "link.connection.disconnect"), isLoading: false)
            if !didNotifyAuthenticated {
                didNotifyAuthenticated = true
                showPreparingBackup()
                onAuthenticated?(client)
            }
        case .failed(let message):
            symbolView.image = UIImage(systemName: "exclamationmark.triangle.fill")
            symbolView.tintColor = .systemOrange
            titleLabel.text = String(localized: "link.connection.failed")
            detailLabel.text = message
            configureActionButton(title: String(localized: "common.close"), isLoading: false)
        }
    }

    private func configureActionButton(title: String, isLoading: Bool) {
        var configuration = actionButton.configuration ?? UIButton.Configuration.filled()
        configuration.title = title
        configuration.showsActivityIndicator = isLoading
        actionButton.configuration = configuration
    }

    func stopConnection() {
        client.stop()
    }

    func showPreparingBackup() {
        symbolView.image = UIImage(systemName: "externaldrive.connected.to.line.below")
        symbolView.tintColor = .appTint
        titleLabel.text = String(localized: "home.overlay.scanningIndex")
        detailLabel.text = client.remoteFolderName
        configureActionButton(title: String(localized: "common.cancel"), isLoading: true)
    }

    func showConnectionFailure(_ error: Error) {
        client.stop()
        render(.failed(error.localizedDescription))
    }

    func markHandedOff() {
        handedOff = true
    }

    @objc private func close() {
        if !handedOff { stopConnection() }
        dismiss(animated: ConsideringUser.animated)
    }
}
