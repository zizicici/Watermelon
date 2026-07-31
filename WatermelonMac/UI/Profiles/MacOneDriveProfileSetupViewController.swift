import AppKit

@MainActor
final class MacOneDriveProfileSetupViewController: NSViewController {
    private let setupCoordinator: OneDriveProfileSetupCoordinator
    private let store: ProfileStore
    private let appRuntimeFlags: AppRuntimeFlags
    private let editingProfile: ServerProfileRecord?
    private let editorMode: MacProfileConnectionEditorMode
    private let canPerformActions: () -> Bool
    private let onSaved: (ServerProfileRecord) -> Void

    private let nameField = NSTextField(string: "OneDrive")
    private let accountLabel = NSTextField(
        labelWithString: String(
            localized: "auth.onedrive.account.notSignedIn"
        )
    )
    private let folderLabel = NSTextField(
        wrappingLabelWithString: String(
            localized: "auth.onedrive.folder.createdAfterSignIn"
        )
    )
    private let statusLabel = NSTextField(
        wrappingLabelWithString: ""
    )
    private let signInButton = NSButton()
    private let saveButton = NSButton()
    private let progressIndicator = NSProgressIndicator()
    private let statusRow = NSStackView()

    private var connectionParams: OneDriveConnectionParams?
    private var credentialJSONString: String?
    private var accountDisplayName: String?
    private var pendingAccountLease: PendingOneDriveAccountLease?
    private var signInTask: Task<Void, Never>?
    private var isWorking = false
    private var activityObserver:
        MacProfileMutationActivityObserver?

    init(
        setupCoordinator: OneDriveProfileSetupCoordinator,
        store: ProfileStore,
        appRuntimeFlags: AppRuntimeFlags,
        editingProfile: ServerProfileRecord? = nil,
        canPerformActions: @escaping () -> Bool,
        onSaved: @escaping (ServerProfileRecord) -> Void
    ) {
        self.setupCoordinator = setupCoordinator
        self.store = store
        self.appRuntimeFlags = appRuntimeFlags
        self.editingProfile = editingProfile
        self.canPerformActions = canPerformActions
        editorMode = MacProfileConnectionEditorMode(
            hasEditingProfile: editingProfile != nil
        )
        self.onSaved = onSaved
        if let editingProfile {
            connectionParams = editingProfile.oneDriveParams
            credentialJSONString = try? store.password(
                for: editingProfile
            )
            accountDisplayName = editingProfile.username
        }
        super.init(nibName: nil, bundle: nil)
        preferredContentSize = NSSize(
            width: 540,
            height: editorMode.showsNameField ? 410 : 360
        )
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        signInTask?.cancel()
        OneDriveMSALService.cancelInteractiveSignIn()
    }

    override func loadView() {
        view = NSView()

        let symbol = NSImageView(
            image: NSImage(
                systemSymbolName: "cloud.fill",
                accessibilityDescription: nil
            ) ?? NSImage()
        )
        symbol.symbolConfiguration = NSImage.SymbolConfiguration(
            pointSize: 34,
            weight: .medium
        )
        symbol.contentTintColor = .systemBlue

        let title = NSTextField(
            labelWithString: editingProfile == nil
                ? String(localized: "auth.onedrive.title")
                : String(localized: "auth.onedrive.editTitle")
        )
        title.font = .systemFont(ofSize: 22, weight: .semibold)

        nameField.placeholderString = String(
            localized: "auth.onedrive.defaultName"
        )
        nameField.stringValue = editingProfile?.name
            ?? String(localized: "auth.onedrive.defaultName")

        accountLabel.lineBreakMode = .byTruncatingMiddle
        folderLabel.textColor = .secondaryLabelColor
        folderLabel.lineBreakMode = .byWordWrapping
        folderLabel.maximumNumberOfLines = 2

        signInButton.title = connectionParams == nil
            ? String(localized: "auth.onedrive.signIn.action")
            : String(localized: "auth.onedrive.signIn.again")
        signInButton.bezelStyle = .rounded
        signInButton.target = self
        signInButton.action = #selector(signIn(_:))

        let accountRow = NSStackView(
            views: [accountLabel, NSView(), signInButton]
        )
        accountRow.orientation = .horizontal
        accountRow.alignment = .centerY
        accountRow.spacing = 10

        var gridRows: [[NSView]] = []
        if editorMode.showsNameField {
            gridRows.append([
                makeFieldLabel(
                    String(localized: "auth.section.name")
                ),
                nameField
            ])
        }
        gridRows.append([
            makeFieldLabel(
                String(localized: "auth.onedrive.section.account")
            ),
            accountRow
        ])
        gridRows.append([
            makeFieldLabel(
                String(localized: "auth.onedrive.section.folder")
            ),
            folderLabel
        ])
        let grid = NSGridView(views: gridRows)
        grid.rowSpacing = 14
        grid.columnSpacing = 14
        grid.column(at: 0).xPlacement = .trailing
        grid.column(at: 1).xPlacement = .fill
        grid.column(at: 1).width = 350

        progressIndicator.style = .spinning
        progressIndicator.controlSize = .small
        progressIndicator.isDisplayedWhenStopped = false

        statusLabel.textColor = .secondaryLabelColor
        statusLabel.maximumNumberOfLines = 2

        statusRow.setViews(
            [progressIndicator, statusLabel],
            in: .leading
        )
        statusRow.orientation = .horizontal
        statusRow.alignment = .centerY
        statusRow.spacing = 8

        let cancelButton = NSButton(
            title: String(localized: "common.cancel"),
            target: self,
            action: #selector(cancel(_:))
        )
        cancelButton.keyEquivalent = "\u{1b}"

        saveButton.title = String(localized: "common.save")
        saveButton.bezelStyle = .rounded
        saveButton.keyEquivalent = "\r"
        saveButton.target = self
        saveButton.action = #selector(save(_:))

        let actions = NSStackView(
            views: [NSView(), cancelButton, saveButton]
        )
        actions.orientation = .horizontal
        actions.alignment = .centerY
        actions.spacing = 8

        let header = NSStackView(
            views: [symbol, title]
        )
        header.orientation = .vertical
        header.alignment = .centerX
        header.spacing = 8

        let root = NSStackView(
            views: [header, grid, statusRow, NSView(), actions]
        )
        root.orientation = .vertical
        root.alignment = .width
        root.spacing = 18
        root.edgeInsets = NSEdgeInsets(
            top: 24,
            left: 28,
            bottom: 22,
            right: 28
        )
        root.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(root)
        NSLayoutConstraint.activate([
            root.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            root.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            root.topAnchor.constraint(equalTo: view.topAnchor),
            root.bottomAnchor.constraint(equalTo: view.bottomAnchor)
        ])

        activityObserver = MacProfileMutationActivityObserver {
            [weak self] in
            self?.renderState()
        }
        renderState()
    }

    override func viewDidDisappear() {
        super.viewDidDisappear()
        guard view.window == nil else { return }
        cleanup()
    }

    @objc private func signIn(_ sender: Any?) {
        guard !isWorking,
              view.window != nil else {
            return
        }
        guard canMutate else {
            showMutationBlocked()
            return
        }
        isWorking = true
        statusLabel.textColor = .secondaryLabelColor
        statusLabel.stringValue = String(
            localized: "auth.onedrive.signIn.signingIn"
        )
        renderState()
        let coordinator = setupCoordinator
        signInTask = Task { @MainActor [weak self, coordinator] in
            guard let self else { return }
            do {
                let draft = try await coordinator.prepare(from: self)
                do {
                    try Task.checkCancellation()
                    guard self.view.window != nil else {
                        throw CancellationError()
                    }
                } catch {
                    draft.accountLease.discard()
                    throw error
                }
                self.adopt(draft)
                self.statusLabel.textColor = .wmMaterialPrimary
                self.statusLabel.stringValue = ""
            } catch is CancellationError {
                self.statusLabel.stringValue = ""
            } catch {
                self.statusLabel.textColor = .wmMaterialError
                self.statusLabel.stringValue =
                    UserFacingErrorLocalizer.message(
                        for: error,
                        storageType: .onedrive
                    )
            }
            self.isWorking = false
            self.signInTask = nil
            self.renderState()
        }
    }

    @objc private func save(_ sender: Any?) {
        guard !isWorking,
              let connectionParams,
              let credentialJSONString else {
            return
        }
        guard canMutate else {
            showMutationBlocked()
            return
        }
        isWorking = true
        statusLabel.textColor = .secondaryLabelColor
        statusLabel.stringValue = String(localized: "mediaBrowser.action.saving")
        renderState()
        do {
            let saved: ServerProfileRecord?
            if let editingProfile {
                saved = try appRuntimeFlags.withProfileMutationLease(
                    profileID: editingProfile.id
                ) {
                    try store.updateOneDriveProfile(
                        editingProfile,
                        connectionParams: connectionParams,
                        credentialJSONString: credentialJSONString,
                        username: accountDisplayName
                    )
                }
            } else {
                saved = try appRuntimeFlags.withProfileMutationLease(
                    profileID: nil
                ) {
                    try store.saveOneDriveProfile(
                        name: nameField.stringValue,
                        connectionParams: connectionParams,
                        credentialJSONString: credentialJSONString,
                        username: accountDisplayName
                    )
                }
            }
            guard let saved else {
                throw RemoteStorageClientError.unavailable
            }
            pendingAccountLease?.commit()
            pendingAccountLease = nil
            onSaved(saved)
            dismiss(self)
        } catch {
            isWorking = false
            statusLabel.textColor = .wmMaterialError
            statusLabel.stringValue = error.localizedDescription
            renderState()
        }
    }

    @objc private func cancel(_ sender: Any?) {
        cleanup()
        dismiss(self)
    }

    private func adopt(_ draft: OneDriveProfileSetupDraft) {
        if let pendingAccountLease {
            if pendingAccountLease.credential.homeAccountIdentifier
                == draft.accountLease.credential
                    .homeAccountIdentifier {
                pendingAccountLease.relinquishToReplacement()
            } else {
                pendingAccountLease.discard()
            }
        }
        pendingAccountLease = draft.accountLease
        connectionParams = draft.connectionParams
        credentialJSONString = draft.credentialJSONString
        accountDisplayName = draft.username
    }

    private func cleanup() {
        signInTask?.cancel()
        signInTask = nil
        OneDriveMSALService.cancelInteractiveSignIn()
        pendingAccountLease?.discard()
        pendingAccountLease = nil
    }

    private func renderState() {
        if let accountDisplayName, !accountDisplayName.isEmpty {
            accountLabel.stringValue = accountDisplayName
        } else if credentialJSONString != nil {
            accountLabel.stringValue = String(
                localized: "auth.onedrive.accountFallback"
            )
        } else {
            accountLabel.stringValue = String(
                localized: "auth.onedrive.account.notSignedIn"
            )
        }
        folderLabel.stringValue = connectionParams?.displayRootPath
            ?? String(
                localized: "auth.onedrive.folder.createdAfterSignIn"
            )
        signInButton.title = connectionParams == nil
            ? String(localized: "auth.onedrive.signIn.action")
            : String(localized: "auth.onedrive.signIn.again")
        nameField.isEnabled = canMutate && !isWorking
        signInButton.isEnabled = canMutate && !isWorking
        saveButton.isEnabled = !isWorking
            && canMutate
            && connectionParams != nil
            && credentialJSONString != nil
        if isWorking {
            progressIndicator.startAnimation(nil)
        } else {
            progressIndicator.stopAnimation(nil)
        }
        statusRow.isHidden =
            !isWorking && statusLabel.stringValue.isEmpty
    }

    private var executionActive: Bool {
        appRuntimeFlags.isExecuting
    }

    private var canMutate: Bool {
        canPerformActions()
            && MacProfileMutationAvailabilityPolicy.canMutate(
                executionActive: executionActive,
                maintenanceActive: false,
                connectionActive:
                    appRuntimeFlags.isConnecting(
                        profileID: editingProfile?.id
                    )
                )
    }

    private func showMutationBlocked() {
        statusLabel.textColor = .wmMaterialError
        statusLabel.stringValue =
            MacProfileMutationError.taskInProgress
                .localizedDescription
        renderState()
    }

    private func makeFieldLabel(_ value: String) -> NSTextField {
        let label = NSTextField(labelWithString: value)
        label.textColor = .secondaryLabelColor
        return label
    }
}
