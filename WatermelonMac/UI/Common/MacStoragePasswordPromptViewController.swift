import AppKit

@MainActor
final class MacStoragePasswordPromptViewController: NSViewController {
    private let profileName: String
    private let username: String
    private let storageType: StorageType
    private let onSubmit: (String) -> Void
    private let passwordField = NSSecureTextField()

    init(
        profileName: String,
        username: String,
        storageType: StorageType,
        onSubmit: @escaping (String) -> Void
    ) {
        self.profileName = profileName
        self.username = username
        self.storageType = storageType
        self.onSubmit = onSubmit
        super.init(nibName: nil, bundle: nil)
        preferredContentSize = NSSize(width: 400, height: 190)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func loadView() {
        view = NSView()

        let titleLabel = NSTextField(labelWithString: titleText)
        titleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        let messageLabel = NSTextField(
            wrappingLabelWithString: String(
                format: messageFormat,
                profileName,
                username
            )
        )
        messageLabel.font = .systemFont(ofSize: 13)
        messageLabel.textColor = .secondaryLabelColor

        passwordField.placeholderString = fieldLabel
        passwordField.target = self
        passwordField.action = #selector(submit(_:))

        let cancelButton = NSButton(
            title: String(localized: "common.cancel"),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded
        cancelButton.keyEquivalent = "\u{1b}"

        let connectButton = NSButton(
            title: String(localized: "common.connect"),
            target: self,
            action: #selector(submit(_:))
        )
        connectButton.bezelStyle = .rounded
        connectButton.keyEquivalent = "\r"
        connectButton.bezelColor = .wmMaterialPrimary

        let actions = NSStackView(
            views: [NSView(), cancelButton, connectButton]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 8

        let content = NSStackView(
            views: [titleLabel, messageLabel, passwordField, actions]
        )
        content.orientation = .vertical
        content.alignment = .width
        content.spacing = 14
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
                constant: -20
            ),
            actions.widthAnchor.constraint(equalTo: content.widthAnchor),
        ])
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.makeFirstResponder(passwordField)
    }

    private var titleText: String {
        storageType == .s3
            ? String(localized: "home.alert.s3SecretKeyPrompt")
            : String(localized: "home.alert.passwordPrompt")
    }

    private var messageFormat: String {
        storageType == .s3
            ? String(localized: "migration.secretKey.message")
            : String(localized: "migration.password.message")
    }

    private var fieldLabel: String {
        storageType == .s3
            ? String(localized: "auth.s3.field.secretKey")
            : String(localized: "auth.field.password")
    }

    @objc private func cancel(_ sender: Any?) {
        dismiss(self)
    }

    @objc private func submit(_ sender: Any?) {
        let password = passwordField.stringValue
        dismiss(self)
        onSubmit(password)
    }
}
