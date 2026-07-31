import AppKit

nonisolated struct MacSFTPTestedHostKey: Equatable, Sendable {
    let host: String
    let port: Int
    let fingerprint: String
}

nonisolated enum MacSFTPProfileSaveFingerprintResolver {
    static func resolve(
        editingSnapshot: ServerProfileRecord?,
        proposedHost: String,
        proposedPort: Int,
        testedHostKey: MacSFTPTestedHostKey?,
        databaseManager: DatabaseManager
    ) throws -> String {
        let liveProfile: ServerProfileRecord?
        if let editingSnapshot, let profileID = editingSnapshot.id {
            guard let fetched = try databaseManager.fetchServerProfile(
                id: profileID
            ) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            liveProfile = fetched
        } else {
            liveProfile = editingSnapshot
        }

        return SFTPHostKeyPromptPolicy.fingerprintForSave(
            liveProfile: liveProfile,
            proposedHost: proposedHost,
            proposedPort: proposedPort,
            testedHost: testedHostKey?.host,
            testedPort: testedHostKey?.port,
            testedFingerprint: testedHostKey?.fingerprint
        )
    }
}

@MainActor
final class MacSFTPProfileViewController:
    NSViewController,
    NSTextFieldDelegate,
    NSTextViewDelegate
{
    private struct Draft: Sendable {
        let name: String
        let host: String
        let port: Int
        let basePath: String
        let username: String
        let credential: SFTPCredentialBlob
    }

    private let sheetTitle: String
    private let initialProfile: ServerProfileRecord?
    private let editorMode: MacProfileConnectionEditorMode
    private let canPerformActions: () -> Bool
    private let save: (
        _ name: String,
        _ host: String,
        _ port: Int,
        _ basePath: String,
        _ username: String,
        _ credential: SFTPCredentialBlob,
        _ testedHostKey: MacSFTPTestedHostKey?
    ) throws -> Void

    private let nameField = NSTextField()
    private let hostField = NSTextField()
    private let portField = NSTextField()
    private let basePathField = NSTextField()
    private let usernameField = NSTextField()
    private let authMethodControl = NSSegmentedControl(
        labels: [
            String(localized: "auth.sftp.authMethod.password"),
            String(localized: "auth.sftp.authMethod.privateKey")
        ],
        trackingMode: .selectOne,
        target: nil,
        action: nil
    )
    private let passwordField = NSSecureTextField()
    private let privateKeyTextView = NSTextView()
    private let privateKeyScrollView = NSScrollView()
    private let passphraseField = NSSecureTextField()
    private let testButton = NSButton()
    private let saveButton = NSButton()
    private let statusLabel = NSTextField(wrappingLabelWithString: "")
    private let errorLabel = NSTextField(wrappingLabelWithString: "")
    private var formGrid: NSGridView!
    private var testedHostKey: MacSFTPTestedHostKey?
    private var operationTask: Task<Void, Never>?
    private var operationID: UUID?
    private var isWorking = false
    private var activityObserver:
        MacProfileMutationActivityObserver?

    init(
        title: String = String(
            localized: "auth.sftp.title",
            defaultValue: "Add SFTP Destination"
        ),
        initialProfile: ServerProfileRecord? = nil,
        initialCredential: SFTPCredentialBlob? = nil,
        canPerformActions: @escaping () -> Bool = { true },
        save: @escaping (
            _ name: String,
            _ host: String,
            _ port: Int,
            _ basePath: String,
            _ username: String,
            _ credential: SFTPCredentialBlob,
            _ testedHostKey: MacSFTPTestedHostKey?
        ) throws -> Void
    ) {
        sheetTitle = title
        self.initialProfile = initialProfile
        editorMode = MacProfileConnectionEditorMode(
            hasEditingProfile: initialProfile != nil
        )
        self.canPerformActions = canPerformActions
        self.save = save
        super.init(nibName: nil, bundle: nil)

        nameField.stringValue = initialProfile?.name ?? ""
        hostField.stringValue = initialProfile?.host ?? ""
        if let initialProfile {
            portField.stringValue = String(
                SFTPEndpoint.effectivePort(initialProfile.port)
            )
        }
        basePathField.stringValue =
            initialProfile?.basePath ?? "/Watermelon"
        usernameField.stringValue = initialProfile?.username ?? ""

        switch initialCredential {
        case .password(let value):
            authMethodControl.selectedSegment = 0
            passwordField.stringValue = value
        case .privateKey(let pem, let passphrase):
            authMethodControl.selectedSegment = 1
            privateKeyTextView.string = pem
            passphraseField.stringValue = passphrase ?? ""
        case nil:
            authMethodControl.selectedSegment =
                initialProfile?.sftpParams?.authMethod == .privateKey
                    ? 1
                    : 0
        }

        preferredContentSize = NSSize(
            width: 640,
            height: editorMode.showsNameField ? 570 : 530
        )
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        operationTask?.cancel()
    }

    override func loadView() {
        view = NSView()

        let titleLabel = NSTextField(labelWithString: sheetTitle)
        titleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        configure(
            nameField,
            placeholder: String(localized: "auth.sftp.placeholder.name")
        )
        configure(
            hostField,
            placeholder: String(localized: "auth.sftp.placeholder.host")
        )
        configure(portField, placeholder: "22")
        configure(basePathField, placeholder: "/Watermelon")
        configure(
            usernameField,
            placeholder: String(localized: "auth.sftp.placeholder.username")
        )
        configure(
            passwordField,
            placeholder: String(localized: "auth.sftp.placeholder.password")
        )
        configure(
            passphraseField,
            placeholder: String(
                localized: "auth.sftp.placeholder.passphrase"
            )
        )

        authMethodControl.target = self
        authMethodControl.action = #selector(authMethodChanged(_:))
        authMethodControl.setWidth(145, forSegment: 0)
        authMethodControl.setWidth(145, forSegment: 1)

        configurePrivateKeyEditor()

        var rows: [[NSView]] = []
        if editorMode.showsNameField {
            rows.append([
                makeFieldLabel(String(localized: "auth.section.name")),
                nameField
            ])
        }
        rows.append(contentsOf: [
            [
                makeFieldLabel(String(localized: "auth.field.host")),
                hostField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.port")),
                portField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.sftp.field.basePath")
                ),
                basePathField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.username")),
                usernameField
            ],
            [
                makeFieldLabel(String(localized: "auth.sftp.authMethod")),
                authMethodControl
            ],
            [
                makeFieldLabel(String(localized: "auth.field.password")),
                passwordField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.sftp.authMethod.privateKey")
                ),
                privateKeyScrollView
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.sftp.field.passphrase")
                ),
                passphraseField
            ]
        ])
        formGrid = NSGridView(views: rows)
        formGrid.rowSpacing = 10
        formGrid.columnSpacing = 14
        formGrid.column(at: 0).xPlacement = .trailing
        formGrid.column(at: 1).xPlacement = .fill

        statusLabel.textColor = .secondaryLabelColor
        statusLabel.font = .systemFont(ofSize: 12)
        statusLabel.isSelectable = true
        statusLabel.isHidden = true

        errorLabel.textColor = .wmMaterialError
        errorLabel.font = .systemFont(ofSize: 12)
        errorLabel.isSelectable = true
        errorLabel.isHidden = true

        testButton.title = String(localized: "auth.testConnection")
        testButton.bezelStyle = .rounded
        testButton.target = self
        testButton.action = #selector(testConnection(_:))

        let cancelButton = NSButton(
            title: String(localized: "common.cancel"),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.bezelStyle = .rounded
        cancelButton.keyEquivalent = "\u{1b}"

        saveButton.title = String(localized: "common.save")
        saveButton.bezelStyle = .rounded
        saveButton.target = self
        saveButton.action = #selector(commit(_:))
        saveButton.keyEquivalent = "\r"

        let buttons = NSStackView(
            views: [testButton, NSView(), cancelButton, saveButton]
        )
        buttons.orientation = .horizontal
        buttons.alignment = .centerY
        buttons.spacing = 8

        let stack = NSStackView(
            views: [
                titleLabel,
                formGrid,
                statusLabel,
                errorLabel,
                buttons
            ]
        )
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 14
        stack.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(stack)

        formGrid.translatesAutoresizingMaskIntoConstraints = false
        buttons.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(
                equalTo: view.topAnchor,
                constant: 22
            ),
            stack.leadingAnchor.constraint(
                equalTo: view.leadingAnchor,
                constant: 24
            ),
            stack.trailingAnchor.constraint(
                equalTo: view.trailingAnchor,
                constant: -24
            ),
            stack.bottomAnchor.constraint(
                lessThanOrEqualTo: view.bottomAnchor,
                constant: -20
            ),
            formGrid.widthAnchor.constraint(equalTo: stack.widthAnchor),
            buttons.widthAnchor.constraint(equalTo: stack.widthAnchor),
            hostField.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 375
            ),
            privateKeyScrollView.heightAnchor.constraint(
                equalToConstant: 120
            )
        ])

        activityObserver = MacProfileMutationActivityObserver {
            [weak self] in
            self?.updateActions()
        }
        updateCredentialRows()
        updateActions()
    }

    override func viewDidAppear() {
        super.viewDidAppear()
        view.window?.makeFirstResponder(
            editorMode.showsNameField
                && !hostField.stringValue.isEmpty
                ? nameField
                : hostField
        )
    }

    func controlTextDidChange(_ notification: Notification) {
        clearMessages()
        updateActions()
    }

    func textDidChange(_ notification: Notification) {
        clearMessages()
        updateActions()
    }

    private var authMethod: SFTPConnectionParams.AuthMethod {
        authMethodControl.selectedSegment == 1
            ? .privateKey
            : .password
    }

    private var hasMinimumFields: Bool {
        guard !hostField.stringValue.trimmed.isEmpty,
              !usernameField.stringValue.trimmed.isEmpty else {
            return false
        }
        return authMethod == .password
            || !privateKeyTextView.string.trimmed.isEmpty
    }

    private func configure(
        _ field: NSTextField,
        placeholder: String
    ) {
        field.placeholderString = placeholder
        field.delegate = self
    }

    private func configurePrivateKeyEditor() {
        privateKeyTextView.delegate = self
        privateKeyTextView.font = .monospacedSystemFont(
            ofSize: 11,
            weight: .regular
        )
        privateKeyTextView.isAutomaticQuoteSubstitutionEnabled = false
        privateKeyTextView.isAutomaticDashSubstitutionEnabled = false
        privateKeyTextView.isAutomaticTextReplacementEnabled = false
        privateKeyTextView.isRichText = false
        privateKeyTextView.frame = NSRect(
            x: 0,
            y: 0,
            width: 375,
            height: 120
        )
        privateKeyTextView.minSize = NSSize(width: 0, height: 120)
        privateKeyTextView.maxSize = NSSize(
            width: CGFloat.greatestFiniteMagnitude,
            height: CGFloat.greatestFiniteMagnitude
        )
        privateKeyTextView.isVerticallyResizable = true
        privateKeyTextView.isHorizontallyResizable = false
        privateKeyTextView.autoresizingMask = [.width]
        privateKeyTextView.textContainer?.widthTracksTextView = true
        privateKeyTextView.textContainer?.containerSize = NSSize(
            width: 0,
            height: CGFloat.greatestFiniteMagnitude
        )

        privateKeyScrollView.borderType = .bezelBorder
        privateKeyScrollView.hasVerticalScroller = true
        privateKeyScrollView.autohidesScrollers = true
        privateKeyScrollView.documentView = privateKeyTextView
    }

    private func makeFieldLabel(_ text: String) -> NSTextField {
        let label = NSTextField(labelWithString: text)
        label.textColor = .secondaryLabelColor
        label.alignment = .right
        return label
    }

    @objc private func authMethodChanged(_ sender: Any?) {
        clearMessages()
        updateCredentialRows()
        updateActions()
    }

    private func updateCredentialRows() {
        let usesPassword = authMethod == .password
        formGrid.row(at: 6).isHidden = !usesPassword
        formGrid.row(at: 7).isHidden = usesPassword
        formGrid.row(at: 8).isHidden = usesPassword
        preferredContentSize.height = usesPassword ? 455 : 570
    }

    private func makeDraft() throws -> Draft {
        guard let socketHost = RemoteHostEndpoint.socketHost(
            hostField.stringValue
        ) else {
            throw validationError(
                code: 10,
                message: String(
                    localized: "auth.sftp.validation.hostRequired"
                )
            )
        }

        let port: Int
        let trimmedPort = portField.stringValue.trimmed
        if trimmedPort.isEmpty {
            port = SFTPEndpoint.defaultPort
        } else if let value = Int(trimmedPort),
                  (1 ... 65_535).contains(value) {
            port = value
        } else {
            throw validationError(
                code: 1,
                message: String(
                    localized: "auth.sftp.validation.portRange"
                )
            )
        }

        let rawBasePath = basePathField.stringValue.trimmed
        let basePath = try SFTPPathCanonicalizer.canonicalRawPath(
            rawBasePath.isEmpty ? "/Watermelon" : rawBasePath
        )
        let username = usernameField.stringValue.trimmed
        guard !username.isEmpty else {
            throw validationError(
                code: 2,
                message: String(
                    localized: "auth.sftp.validation.usernameRequired"
                )
            )
        }

        let credential: SFTPCredentialBlob
        switch authMethod {
        case .password:
            credential = .password(passwordField.stringValue)
        case .privateKey:
            let key = privateKeyTextView.string.trimmed
            guard !key.isEmpty else {
                throw validationError(
                    code: 3,
                    message: String(
                        localized: "auth.sftp.validation.privateKeyRequired"
                    )
                )
            }
            guard key.contains(
                "-----BEGIN OPENSSH PRIVATE KEY-----"
            ) else {
                throw validationError(
                    code: 4,
                    message: String(
                        localized: "auth.sftp.validation.privateKeyInvalid"
                    )
                )
            }
            credential = .privateKey(
                pem: key,
                passphrase: passphraseField.stringValue.isEmpty
                    ? nil
                    : passphraseField.stringValue
            )
        }

        return Draft(
            name: nameField.stringValue.trimmed,
            host: socketHost,
            port: port,
            basePath: basePath,
            username: username,
            credential: credential
        )
    }

    private func validationError(
        code: Int,
        message: String
    ) -> NSError {
        NSError(
            domain: "MacSFTPProfile",
            code: code,
            userInfo: [NSLocalizedDescriptionKey: message]
        )
    }

    @objc private func testConnection(_ sender: Any?) {
        clearMessages()
        guard canPerformActions() else {
            showError(MacProfileMutationError.taskInProgress)
            updateActions()
            return
        }
        let draft: Draft
        do {
            draft = try makeDraft()
        } catch {
            showError(error)
            return
        }

        let operationID = UUID()
        self.operationID = operationID
        isWorking = true
        setFormEnabled(false)
        updateActions()

        operationTask = Task {
            do {
                let fingerprint = try await SFTPClient
                    .captureHostKeyFingerprint(
                        host: draft.host,
                        port: draft.port
                    )
                try Task.checkCancellation()
                guard self.operationID == operationID else { return }

                let expected = SFTPHostKeyPromptPolicy
                    .retainedFingerprint(
                        existingProfile: initialProfile,
                        proposedHost: draft.host,
                        proposedPort: draft.port
                    )
                let decision = SFTPHostKeyPromptPolicy.decision(
                    existingHost: initialProfile?.host,
                    existingPort: initialProfile?.port,
                    expectedFingerprint: expected,
                    proposedHost: draft.host,
                    proposedPort: draft.port,
                    actualFingerprint: fingerprint
                )
                let trusted = await promptForHostKey(
                    decision: decision,
                    actualFingerprint: fingerprint
                )
                guard trusted else { throw CancellationError() }
                try Task.checkCancellation()
                guard self.operationID == operationID else { return }

                let client = SFTPClient(
                    config: .init(
                        host: draft.host,
                        port: draft.port,
                        username: draft.username,
                        credential: draft.credential,
                        expectedHostKeyFingerprintSHA256: fingerprint
                    )
                )
                do {
                    try await withTaskCancellationHandler {
                        try await client.connect()
                        _ = try await client.list(path: draft.basePath)
                    } onCancel: {
                        client.cancelActiveOperationsForAbandonment()
                    }
                    await client.disconnect()
                } catch {
                    await client.disconnect()
                    throw error
                }
                try Task.checkCancellation()
                guard self.operationID == operationID else { return }

                testedHostKey = MacSFTPTestedHostKey(
                    host: draft.host,
                    port: draft.port,
                    fingerprint: fingerprint
                )
                statusLabel.stringValue = String(
                    localized: "auth.testConnectionSucceededMessage"
                )
                statusLabel.isHidden = false
            } catch is CancellationError {
            } catch {
                guard self.operationID == operationID else { return }
                showError(error)
            }
            finishOperation(id: operationID)
        }
    }

    private func promptForHostKey(
        decision: SFTPHostKeyPromptPolicy.Decision,
        actualFingerprint: String
    ) async -> Bool {
        switch decision {
        case .none:
            return true
        case .firstTrust:
            return await runHostKeyPrompt(
                title: String(
                    localized: "auth.sftp.hostKey.confirmTitle"
                ),
                message: String.localizedStringWithFormat(
                    String(localized: "auth.sftp.hostKey.confirmBody"),
                    actualFingerprint
                ),
                confirmTitle: String(
                    localized: "auth.sftp.hostKey.confirmAction"
                ),
                destructive: false
            )
        case .changedKey(let expected):
            return await runHostKeyPrompt(
                title: String(
                    localized: "auth.sftp.hostKey.changedTitle"
                ),
                message: String.localizedStringWithFormat(
                    String(localized: "auth.sftp.hostKey.changedBody"),
                    expected,
                    actualFingerprint
                ),
                confirmTitle: String(
                    localized: "auth.sftp.hostKey.changedAction"
                ),
                destructive: true
            )
        }
    }

    private func runHostKeyPrompt(
        title: String,
        message: String,
        confirmTitle: String,
        destructive: Bool
    ) async -> Bool {
        guard !Task.isCancelled, let window = view.window else {
            return false
        }
        return await withCheckedContinuation {
            (continuation: CheckedContinuation<Bool, Never>) in
            let alert = NSAlert()
            alert.alertStyle = .warning
            alert.messageText = title
            alert.informativeText = message
            alert.addButton(withTitle: confirmTitle)
            alert.addButton(
                withTitle: String(localized: "common.cancel")
            )
            alert.buttons.first?.hasDestructiveAction = destructive
            alert.beginSheetModal(for: window) { response in
                continuation.resume(
                    returning: response == .alertFirstButtonReturn
                )
            }
        }
    }

    private func finishOperation(id: UUID) {
        guard operationID == id else { return }
        operationID = nil
        operationTask = nil
        isWorking = false
        setFormEnabled(true)
        updateActions()
    }

    private func clearMessages() {
        statusLabel.isHidden = true
        errorLabel.isHidden = true
    }

    private func showError(_ error: Error) {
        errorLabel.stringValue = UserFacingErrorLocalizer.message(
            for: error,
            storageType: .sftp
        )
        errorLabel.isHidden = false
        statusLabel.isHidden = true
    }

    private func setFormEnabled(_ enabled: Bool) {
        [
            nameField,
            hostField,
            portField,
            basePathField,
            usernameField,
            passwordField,
            passphraseField
        ].forEach { $0.isEnabled = enabled }
        authMethodControl.isEnabled = enabled
        privateKeyTextView.isEditable = enabled
    }

    private func updateActions() {
        testButton.title = isWorking
            ? String(localized: "profile.add.s3.verifying")
            : String(localized: "auth.testConnection")
        let actionsAllowed = canPerformActions()
        testButton.isEnabled =
            actionsAllowed && hasMinimumFields && !isWorking
        saveButton.isEnabled =
            actionsAllowed && hasMinimumFields && !isWorking
    }

    @objc private func cancel(_ sender: Any?) {
        operationID = nil
        operationTask?.cancel()
        operationTask = nil
        dismiss(self)
    }

    @objc private func commit(_ sender: Any?) {
        clearMessages()
        guard canPerformActions() else {
            showError(MacProfileMutationError.taskInProgress)
            updateActions()
            return
        }
        let draft: Draft
        do {
            draft = try makeDraft()
        } catch {
            showError(error)
            return
        }

        isWorking = true
        setFormEnabled(false)
        updateActions()
        do {
            try save(
                draft.name,
                draft.host,
                draft.port,
                draft.basePath,
                draft.username,
                draft.credential,
                testedHostKey
            )
            dismiss(self)
        } catch {
            isWorking = false
            setFormEnabled(true)
            showError(error)
            updateActions()
        }
    }
}
