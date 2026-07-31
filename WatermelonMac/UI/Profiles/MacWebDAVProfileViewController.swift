import AppKit

struct WebDAVProfileSnapshot {
    let name: String
    let scheme: String
    let host: String
    let port: Int
    let mountPath: String
    let basePath: String
    let username: String
    let password: String
}

enum MacWebDAVProbePathPolicy {
    static func listPath(for profile: ServerProfileRecord) -> String {
        RemotePathBuilder.normalizePath(profile.basePath)
    }
}

@MainActor
final class MacWebDAVProfileViewController:
    NSViewController,
    NSTextFieldDelegate
{
    private let storageClientFactory: StorageClientFactory
    private let editorMode: MacProfileConnectionEditorMode
    private let sheetTitle: String
    private let basePathLabel: String
    private let basePathDefault: String
    private let canPerformActions: () -> Bool
    private let save: (_ snapshot: WebDAVProfileSnapshot) throws -> Void

    private let nameField = NSTextField()
    private let schemeControl = NSSegmentedControl(
        labels: ["HTTPS", "HTTP"],
        trackingMode: .selectOne,
        target: nil,
        action: nil
    )
    private let hostField = NSTextField()
    private let portField = NSTextField()
    private let mountPathField = NSTextField()
    private let basePathField = NSTextField()
    private let usernameField = NSTextField()
    private let passwordField = NSSecureTextField()
    private let testButton = NSButton()
    private let saveButton = NSButton()
    private let statusLabel = NSTextField(wrappingLabelWithString: "")
    private let errorLabel = NSTextField(wrappingLabelWithString: "")
    private var verificationTask: Task<Void, Never>?
    private var verificationID: UUID?
    private var saving = false
    private var activityObserver:
        MacProfileMutationActivityObserver?

    init(
        storageClientFactory: StorageClientFactory,
        title: String = String(localized: "auth.webdav.title"),
        basePathLabel: String = String(
            localized: "auth.webdav.fieldBasePath"
        ),
        basePathDefault: String = "/Watermelon",
        initial: WebDAVProfileSnapshot? = nil,
        canPerformActions: @escaping () -> Bool = { true },
        save: @escaping (_ snapshot: WebDAVProfileSnapshot) throws -> Void
    ) {
        self.storageClientFactory = storageClientFactory
        editorMode = MacProfileConnectionEditorMode(
            hasEditingProfile: initial != nil
        )
        sheetTitle = title
        self.basePathLabel = basePathLabel
        self.basePathDefault = basePathDefault
        self.canPerformActions = canPerformActions
        self.save = save
        super.init(nibName: nil, bundle: nil)

        nameField.stringValue = initial?.name ?? ""
        schemeControl.selectedSegment =
            initial?.scheme.lowercased() == "http" ? 1 : 0
        hostField.stringValue = initial?.host ?? ""
        portField.stringValue = initial.map {
            String($0.port)
        } ?? ""
        mountPathField.stringValue = initial?.mountPath ?? "/"
        basePathField.stringValue =
            initial?.basePath ?? basePathDefault
        usernameField.stringValue = initial?.username ?? ""
        passwordField.stringValue = initial?.password ?? ""

        preferredContentSize = NSSize(
            width: 570,
            height: editorMode.showsNameField ? 500 : 460
        )
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        verificationTask?.cancel()
    }

    override func loadView() {
        view = NSView()

        let titleLabel = NSTextField(labelWithString: sheetTitle)
        titleLabel.font = .systemFont(ofSize: 17, weight: .semibold)

        configure(
            nameField,
            placeholder: String(
                localized: "auth.smb.login.placeholder.name"
            )
        )
        configure(hostField, placeholder: "example.com")
        configure(
            portField,
            placeholder: String(localized: "auth.field.port")
        )
        configure(mountPathField, placeholder: "/")
        configure(basePathField, placeholder: basePathDefault)
        configure(usernameField, placeholder: "")
        configure(passwordField, placeholder: "")

        schemeControl.selectedSegment =
            schemeControl.selectedSegment < 0 ? 0 : schemeControl.selectedSegment
        schemeControl.target = self
        schemeControl.action = #selector(schemeChanged(_:))
        schemeControl.setWidth(82, forSegment: 0)
        schemeControl.setWidth(82, forSegment: 1)

        var rows: [[NSView]] = []
        if editorMode.showsNameField {
            rows.append([
                makeFieldLabel(String(localized: "auth.section.name")),
                nameField
            ])
        }
        rows.append(contentsOf: [
            [
                makeFieldLabel(
                    String(localized: "auth.webdav.fieldScheme")
                ),
                schemeControl
            ],
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
                    String(localized: "auth.webdav.fieldMountPath")
                ),
                mountPathField
            ],
            [
                makeFieldLabel(basePathLabel),
                basePathField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.username")),
                usernameField
            ],
            [
                makeFieldLabel(String(localized: "auth.field.password")),
                passwordField
            ]
        ])
        let grid = NSGridView(views: rows)
        grid.rowSpacing = 10
        grid.columnSpacing = 14
        grid.column(at: 0).xPlacement = .trailing
        grid.column(at: 1).xPlacement = .fill

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
                grid,
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

        grid.translatesAutoresizingMaskIntoConstraints = false
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
            grid.widthAnchor.constraint(equalTo: stack.widthAnchor),
            buttons.widthAnchor.constraint(equalTo: stack.widthAnchor),
            hostField.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 340
            )
        ])

        activityObserver = MacProfileMutationActivityObserver {
            [weak self] in
            self?.updateActions()
        }
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
        invalidateVerification()
        updateActions()
    }

    private var scheme: String {
        schemeControl.selectedSegment == 1 ? "http" : "https"
    }

    private var hasMinimumFields: Bool {
        !hostField.stringValue.trimmed.isEmpty
            && !usernameField.stringValue.trimmed.isEmpty
    }

    private func configure(
        _ field: NSTextField,
        placeholder: String
    ) {
        field.placeholderString = placeholder
        field.delegate = self
    }

    private func makeFieldLabel(_ text: String) -> NSTextField {
        let label = NSTextField(labelWithString: text)
        label.textColor = .secondaryLabelColor
        label.alignment = .right
        return label
    }

    @objc private func schemeChanged(_ sender: Any?) {
        invalidateVerification()
        updateActions()
    }

    private func resolvedPort() -> Int {
        if let value = Int(portField.stringValue.trimmed),
           value > 0 {
            return value
        }
        return scheme == "https" ? 443 : 80
    }

    private func snapshot() -> WebDAVProfileSnapshot {
        WebDAVProfileSnapshot(
            name: nameField.stringValue.trimmed,
            scheme: scheme,
            host: hostField.stringValue.trimmed,
            port: resolvedPort(),
            mountPath: mountPathField.stringValue.trimmed.isEmpty
                ? "/"
                : mountPathField.stringValue.trimmed,
            basePath: basePathField.stringValue.trimmed.isEmpty
                ? "/"
                : basePathField.stringValue.trimmed,
            username: usernameField.stringValue.trimmed,
            password: passwordField.stringValue
        )
    }

    private func makeProbeRecord() -> ServerProfileRecord? {
        let values = snapshot()
        let normalizedMount = RemotePathBuilder.normalizePath(
            values.mountPath
        )
        guard ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: values.scheme,
            host: values.host,
            port: values.port,
            mountPath: normalizedMount
        ) != nil else {
            return nil
        }
        let params = WebDAVConnectionParams(scheme: values.scheme)
        guard let encoded = try? ServerProfileRecord
            .encodedConnectionParams(params) else {
            return nil
        }
        return ServerProfileRecord(
            id: nil,
            name: values.name.isEmpty ? values.host : values.name,
            storageType: StorageType.webdav.rawValue,
            connectionParams: encoded,
            sortOrder: 0,
            host: values.host,
            port: values.port,
            shareName: normalizedMount,
            basePath: RemotePathBuilder.normalizePath(values.basePath),
            username: values.username,
            domain: nil,
            credentialRef: "",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    @objc private func testConnection(_ sender: Any?) {
        statusLabel.isHidden = true
        errorLabel.isHidden = true
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress
                )
            )
            return
        }
        guard let profile = makeProbeRecord() else {
            showError(
                String(localized: "auth.webdav.invalidEndpoint")
            )
            return
        }
        let credential = passwordField.stringValue
        let verificationID = UUID()
        self.verificationID = verificationID
        verificationTask = Task {
            do {
                let client = try storageClientFactory.makeClient(
                    profile: profile,
                    credentialPayload: credential
                )
                do {
                    try await client.connect()
                    _ = try await client.list(
                        path: MacWebDAVProbePathPolicy.listPath(
                            for: profile
                        )
                    )
                } catch {
                    await client.disconnect()
                    throw error
                }
                await client.disconnect()
                try Task.checkCancellation()
                guard self.verificationID == verificationID else {
                    return
                }
                statusLabel.stringValue = String(
                    localized: "auth.testConnectionSucceeded"
                )
                statusLabel.isHidden = false
            } catch is CancellationError {
            } catch {
                guard self.verificationID == verificationID else {
                    return
                }
                showError(
                    UserFacingErrorLocalizer.message(for: error)
                )
            }
            guard self.verificationID == verificationID else {
                return
            }
            self.verificationID = nil
            verificationTask = nil
            updateActions()
        }
        updateActions()
    }

    private func invalidateVerification() {
        verificationID = nil
        verificationTask?.cancel()
        verificationTask = nil
        statusLabel.isHidden = true
        errorLabel.isHidden = true
    }

    private func updateActions() {
        let verifying = verificationTask != nil
        let actionsAllowed = canPerformActions()
        testButton.title = verifying
            ? String(localized: "profile.add.webdav.verifying")
            : String(localized: "auth.testConnection")
        testButton.isEnabled =
            actionsAllowed
            && hasMinimumFields
            && !verifying
            && !saving
        saveButton.title = saving
            ? String(localized: "mediaBrowser.action.saving")
            : String(localized: "common.save")
        saveButton.isEnabled =
            actionsAllowed
            && hasMinimumFields
            && !verifying
            && !saving
    }

    private func showError(_ message: String) {
        errorLabel.stringValue = message
        errorLabel.isHidden = false
        statusLabel.isHidden = true
    }

    @objc private func cancel(_ sender: Any?) {
        verificationTask?.cancel()
        dismiss(self)
    }

    @objc private func commit(_ sender: Any?) {
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress
                )
            )
            updateActions()
            return
        }
        saving = true
        updateActions()
        do {
            try save(snapshot())
            dismiss(self)
        } catch {
            saving = false
            showError(UserFacingErrorLocalizer.message(for: error))
            updateActions()
        }
    }
}
