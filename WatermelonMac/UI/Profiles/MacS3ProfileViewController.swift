import AppKit

struct S3ProfileSnapshot {
    let name: String
    let scheme: String
    let host: String
    let port: Int
    let region: String
    let bucket: String
    let basePath: String
    let usePathStyle: Bool
    let accessKeyID: String
    let secretAccessKey: String
}

@MainActor
final class MacS3ProfileViewController:
    NSViewController,
    NSTextFieldDelegate
{
    private struct ResolvedDraft {
        let scheme: String
        let host: String
        let port: Int
        let region: String
        let bucket: String
        let basePath: String
        let usePathStyle: Bool
        let name: String
        let accessKeyID: String
    }

    private let storageClientFactory: StorageClientFactory
    private let appRuntimeFlags: AppRuntimeFlags
    private let editorMode: MacProfileConnectionEditorMode
    private let mutationProfileID: Int64?
    private let sheetTitle: String
    private let basePathDefault: String
    private let canPerformActions: () -> Bool
    private let save: (_ snapshot: S3ProfileSnapshot) throws -> Void

    private let nameField = NSTextField()
    private let endpointField = NSTextField()
    private let regionField = NSTextField()
    private let bucketField = NSTextField()
    private let basePathField = NSTextField()
    private let accessKeyField = NSTextField()
    private let secretKeyField = NSSecureTextField()
    private let pathStyleButton = NSButton(
        checkboxWithTitle: String(localized: "auth.s3.pathStyle.label"),
        target: nil,
        action: nil
    )
    private let saveButton = NSButton()
    private let errorLabel = NSTextField(wrappingLabelWithString: "")
    private var pathStyleOverride: Bool?
    private var verificationTask: Task<Void, Never>?
    private var saving = false
    private var activityObserver:
        MacProfileMutationActivityObserver?

    init(
        storageClientFactory: StorageClientFactory,
        appRuntimeFlags: AppRuntimeFlags,
        mutationProfileID: Int64? = nil,
        title: String = String(localized: "auth.s3.title"),
        basePathDefault: String = "/Watermelon",
        initial: S3ProfileSnapshot? = nil,
        canPerformActions: @escaping () -> Bool = { true },
        save: @escaping (_ snapshot: S3ProfileSnapshot) throws -> Void
    ) {
        self.storageClientFactory = storageClientFactory
        self.appRuntimeFlags = appRuntimeFlags
        editorMode = MacProfileConnectionEditorMode(
            hasEditingProfile: initial != nil
        )
        self.mutationProfileID = mutationProfileID
        sheetTitle = title
        self.basePathDefault = basePathDefault
        self.canPerformActions = canPerformActions
        self.save = save
        pathStyleOverride = initial?.usePathStyle
        super.init(nibName: nil, bundle: nil)

        nameField.stringValue = initial?.name ?? ""
        if let initial {
            let defaultPort = initial.scheme == "http" ? 80 : 443
            let portSuffix = initial.port == defaultPort
                ? ""
                : ":\(initial.port)"
            endpointField.stringValue =
                "\(initial.scheme)://\(initial.host)\(portSuffix)"
        }
        regionField.stringValue = initial?.region ?? ""
        bucketField.stringValue = initial?.bucket ?? ""
        basePathField.stringValue =
            initial?.basePath ?? basePathDefault
        accessKeyField.stringValue = initial?.accessKeyID ?? ""
        secretKeyField.stringValue = initial?.secretAccessKey ?? ""

        preferredContentSize = NSSize(
            width: 590,
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
            placeholder: String(localized: "auth.s3.placeholder.name")
        )
        configure(
            endpointField,
            placeholder: String(localized: "auth.s3.placeholder.endpoint")
        )
        configure(
            regionField,
            placeholder: String(localized: "auth.s3.placeholder.region")
        )
        configure(
            bucketField,
            placeholder: String(localized: "auth.s3.placeholder.bucket")
        )
        configure(
            basePathField,
            placeholder: String(localized: "auth.s3.placeholder.basePath")
        )
        configure(
            accessKeyField,
            placeholder: String(localized: "auth.s3.placeholder.accessKeyID")
        )
        configure(
            secretKeyField,
            placeholder: String(localized: "auth.s3.placeholder.secretKey")
        )

        pathStyleButton.target = self
        pathStyleButton.action = #selector(pathStyleChanged(_:))
        pathStyleButton.toolTip = String(
            localized: "auth.s3.pathStyle.hint"
        )
        if let pathStyleOverride {
            pathStyleButton.state = pathStyleOverride ? .on : .off
        } else {
            updatePathStyleDefault()
        }

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
                    String(localized: "auth.s3.section.endpoint")
                ),
                endpointField
            ],
            [
                makeFieldLabel(String(localized: "profile.add.s3.region")),
                regionField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.s3.section.bucket")
                ),
                bucketField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.sftp.field.basePath")
                ),
                basePathField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.s3.field.accessKeyID")
                ),
                accessKeyField
            ],
            [
                makeFieldLabel(
                    String(localized: "auth.s3.field.secretKey")
                ),
                secretKeyField
            ],
            [NSView(), pathStyleButton]
        ])
        let grid = NSGridView(views: rows)
        grid.rowSpacing = 10
        grid.columnSpacing = 14
        grid.column(at: 0).xPlacement = .trailing
        grid.column(at: 1).xPlacement = .fill

        errorLabel.textColor = .wmMaterialError
        errorLabel.font = .systemFont(ofSize: 12)
        errorLabel.isSelectable = true
        errorLabel.isHidden = true

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
            views: [NSView(), cancelButton, saveButton]
        )
        buttons.orientation = .horizontal
        buttons.alignment = .centerY
        buttons.spacing = 8

        let stack = NSStackView(
            views: [titleLabel, grid, errorLabel, buttons]
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
            endpointField.widthAnchor.constraint(
                greaterThanOrEqualToConstant: 350
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
                && !endpointField.stringValue.isEmpty
                ? nameField
                : endpointField
        )
    }

    func controlTextDidChange(_ notification: Notification) {
        errorLabel.isHidden = true
        if notification.object as? NSTextField === endpointField {
            updatePathStyleDefault()
        }
        updateActions()
    }

    private var hasMinimumFields: Bool {
        !endpointField.stringValue.trimmed.isEmpty
            && !bucketField.stringValue.trimmed.isEmpty
            && !accessKeyField.stringValue.trimmed.isEmpty
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

    private func updatePathStyleDefault() {
        guard pathStyleOverride == nil else { return }
        let host =
            S3Client.parseEndpoint(endpointField.stringValue)?.host ?? ""
        pathStyleButton.state =
            S3Client.defaultPathStyle(forHost: host) ? .on : .off
    }

    @objc private func pathStyleChanged(_ sender: NSButton) {
        pathStyleOverride = sender.state == .on
        errorLabel.isHidden = true
    }

    private func resolveDraft() -> ResolvedDraft? {
        guard let parsed = S3Client.parseEndpoint(
            endpointField.stringValue
        ) else {
            return nil
        }
        return ResolvedDraft(
            scheme: parsed.scheme,
            host: parsed.host,
            port: parsed.port,
            region: S3Client.resolveRegion(
                userInput: regionField.stringValue,
                host: parsed.host
            ),
            bucket: bucketField.stringValue.trimmed,
            basePath: basePathField.stringValue.trimmed.isEmpty
                ? "/"
                : basePathField.stringValue.trimmed,
            usePathStyle: pathStyleOverride
                ?? S3Client.defaultPathStyle(forHost: parsed.host),
            name: nameField.stringValue.trimmed,
            accessKeyID: accessKeyField.stringValue.trimmed
        )
    }

    private func snapshot(from draft: ResolvedDraft) -> S3ProfileSnapshot {
        S3ProfileSnapshot(
            name: draft.name,
            scheme: draft.scheme,
            host: draft.host,
            port: draft.port,
            region: draft.region,
            bucket: draft.bucket,
            basePath: draft.basePath,
            usePathStyle: draft.usePathStyle,
            accessKeyID: draft.accessKeyID,
            secretAccessKey: secretKeyField.stringValue
        )
    }

    private func makeProbeRecord(
        from draft: ResolvedDraft
    ) -> ServerProfileRecord? {
        let params = S3ConnectionParams(
            scheme: draft.scheme,
            region: draft.region,
            usePathStyle: draft.usePathStyle
        )
        guard let encoded = try? ServerProfileRecord
            .encodedConnectionParams(params) else {
            return nil
        }
        return ServerProfileRecord(
            id: nil,
            name: draft.name.isEmpty ? draft.bucket : draft.name,
            storageType: StorageType.s3.rawValue,
            connectionParams: encoded,
            sortOrder: 0,
            host: draft.host,
            port: draft.port,
            shareName: draft.bucket,
            basePath: RemotePathBuilder.normalizePath(draft.basePath),
            username: draft.accessKeyID,
            domain: nil,
            credentialRef: "",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    @objc private func cancel(_ sender: Any?) {
        verificationTask?.cancel()
        dismiss(self)
    }

    @objc private func commit(_ sender: Any?) {
        errorLabel.isHidden = true
        guard canPerformActions() else {
            showError(
                UserFacingErrorLocalizer.message(
                    for: MacProfileMutationError.taskInProgress
                )
            )
            updateActions()
            return
        }
        guard let draft = resolveDraft(),
              let profile = makeProbeRecord(from: draft) else {
            showError(String(localized: "auth.s3.validation.endpoint"))
            return
        }
        let snapshot = snapshot(from: draft)
        let secret = secretKeyField.stringValue
        saving = true
        setFormEnabled(false)
        updateActions()

        verificationTask = Task {
            do {
                let saved = try await appRuntimeFlags
                    .withAsyncProfileMutationLease(
                        profileID: mutationProfileID
                    ) {
                        let client = try storageClientFactory.makeClient(
                            profile: profile,
                            credentialPayload: secret
                        )
                        try await S3ProfileVerifier.run(
                            client: client,
                            writeAccessMessageTemplate: String(
                                localized:
                                    "profile.add.s3.error.writeAccess"
                            )
                        )
                        try Task.checkCancellation()
                        try save(snapshot)
                        return true
                    }
                guard saved == true else {
                    throw MacProfileMutationError.taskInProgress
                }
                dismiss(self)
            } catch is CancellationError {
                saving = false
                setFormEnabled(true)
                updateActions()
            } catch {
                saving = false
                setFormEnabled(true)
                showError(
                    UserFacingErrorLocalizer.message(for: error)
                )
                updateActions()
            }
            verificationTask = nil
        }
    }

    private func setFormEnabled(_ enabled: Bool) {
        [
            nameField,
            endpointField,
            regionField,
            bucketField,
            basePathField,
            accessKeyField,
            secretKeyField
        ].forEach { $0.isEnabled = enabled }
        pathStyleButton.isEnabled = enabled
    }

    private func updateActions() {
        saveButton.title = saving
            ? String(localized: "profile.add.s3.verifying")
            : String(localized: "common.save")
        saveButton.isEnabled =
            canPerformActions()
            && hasMinimumFields
            && !saving
    }

    private func showError(_ message: String) {
        errorLabel.stringValue = message
        errorLabel.isHidden = false
    }
}
