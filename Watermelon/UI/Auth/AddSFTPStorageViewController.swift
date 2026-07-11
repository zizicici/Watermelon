import SnapKit
import UIKit

nonisolated enum SFTPHostKeyPromptPolicy {
    enum Decision: Equatable {
        case none
        case firstTrust
        case changedKey(expected: String)
    }

    static func decision(
        existingHost: String?,
        existingPort: Int?,
        expectedFingerprint: String?,
        proposedHost: String,
        proposedPort: Int,
        actualFingerprint: String
    ) -> Decision {
        guard actualFingerprint != expectedFingerprint else { return .none }
        let sameEndpoint: Bool
        if let existingHost, let existingPort {
            sameEndpoint = RemoteHostIdentity.canonical(existingHost) == RemoteHostIdentity.canonical(proposedHost)
                && SFTPEndpoint.effectivePort(existingPort) == SFTPEndpoint.effectivePort(proposedPort)
        } else {
            sameEndpoint = false
        }
        if sameEndpoint, let expectedFingerprint, !expectedFingerprint.isEmpty {
            return .changedKey(expected: expectedFingerprint)
        }
        return .firstTrust
    }
}

final class AddSFTPStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case server
        case credentials
    }

    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var saveBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.save"),
        style: .prominentStyle,
        target: self,
        action: #selector(saveTapped)
    )
    private lazy var loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)
    private lazy var keyboardToolbar: UIToolbar = {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(barButtonSystemItem: .done, target: self, action: #selector(dismissKeyboard))
        ]
        return toolbar
    }()

    private var keyboardObservers: [NSObjectProtocol] = []
    private var saveTask: Task<Void, Never>?
    private var pendingPromptContinuation: CheckedContinuation<Bool, Never>?

    private var nameText = ""
    private var hostText = ""
    private var portText = ""
    private var basePathText = "/Watermelon"
    private var usernameText = ""
    private var authMethod: SFTPConnectionParams.AuthMethod = .password
    private var passwordText = ""
    private var privateKeyText = ""
    private var passphraseText = ""
    private var passwordChanged = false
    private var passwordRevealed = false
    private var revealedSavedPassword: String?
    private var privateKeyChanged = false
    private var privateKeyRevealed = false
    private var revealedSavedPrivateKey: String?
    private var passphraseChanged = false
    private var passphraseRevealed = false
    private var revealedSavedPassphrase: String?

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.server, .credentials]
    }

    private func resolvedSection(at index: Int) -> Section? {
        guard visibleSections.indices.contains(index) else { return nil }
        return visibleSections[index]
    }

    private func sectionIndex(for section: Section) -> Int {
        visibleSections.firstIndex(of: section) ?? section.rawValue
    }

    init(
        dependencies: DependencyContainer,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.editingProfile = editingProfile
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onSaved = onSaved
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = editingProfile == nil
            ? String(localized: "auth.sftp.title")
            : String(localized: "auth.sftp.editTitle")

        fillInitialValues()
        configureUI()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            saveTask?.cancel()
            // CheckedContinuation crashes if dropped unresumed — release any in-flight host-key prompt.
            resolvePendingPrompt(false)
        }
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            setOperationNavigationLocked(false)
        }
    }

    private func fillInitialValues() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
        usernameText = editingProfile.username
        hostText = editingProfile.host
        let effectivePort = SFTPEndpoint.effectivePort(editingProfile.port)
        portText = effectivePort == SFTPEndpoint.defaultPort ? "" : String(effectivePort)
        basePathText = editingProfile.basePath.isEmpty ? "/Watermelon" : editingProfile.basePath
        if let params = editingProfile.sftpParams {
            authMethod = params.authMethod
        }
    }

    // Switching auth mode must force re-entry; reusing a stored credential of the wrong type is broken.
    private func storedCredentialIfMatchingMode() -> SFTPCredentialBlob? {
        guard let editingProfile,
              let json = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef),
              let blob = try? SFTPCredentialBlob.decode(from: json) else { return nil }
        switch (blob, authMethod) {
        case (.password, .password), (.privateKey, .privateKey):
            return blob
        default:
            return nil
        }
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = saveBarButtonItem

        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.keyboardDismissMode = .interactive
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.register(SettingsTextFieldCell.self, forCellReuseIdentifier: SettingsTextFieldCell.reuseIdentifier)
        tableView.register(CredentialTextFieldCell.self, forCellReuseIdentifier: CredentialTextFieldCell.reuseIdentifier)
        tableView.register(CredentialTextViewCell.self, forCellReuseIdentifier: CredentialTextViewCell.reuseIdentifier)
        tableView.register(SFTPAuthMethodCell.self, forCellReuseIdentifier: SFTPAuthMethodCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.top.equalToSuperview()
            make.leading.trailing.bottom.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard saveTask == nil else { return }
        guard !rejectIfProfileMutationBlocked() else { return }

        let draft: ValidatedDraft
        do {
            draft = try validateInputs()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .sftp)
            )
            return
        }

        setSaving(true)
        let existingFingerprint = editingProfile?.sftpParams?.hostKeyFingerprintSHA256
        let runtimeFlags = dependencies.appRuntimeFlags
        let editingProfileID = editingProfile?.id
        saveTask = Task { [weak self] in
            do {
                let fingerprint = try await SFTPClient.captureHostKeyFingerprint(host: draft.host, port: draft.port)
                try Task.checkCancellation()

                let decision = SFTPHostKeyPromptPolicy.decision(
                    existingHost: self?.editingProfile?.host,
                    existingPort: self?.editingProfile?.port,
                    expectedFingerprint: existingFingerprint,
                    proposedHost: draft.host,
                    proposedPort: draft.port,
                    actualFingerprint: fingerprint
                )
                if decision != .none {
                    let trusted: Bool
                    switch decision {
                    case .none:
                        trusted = true
                    case .firstTrust:
                        trusted = await self?.promptUserForFingerprint(fingerprint) ?? false
                    case .changedKey(let expected):
                        trusted = await self?.promptUserForFingerprintChange(expected: expected, actual: fingerprint) ?? false
                    }
                    try Task.checkCancellation()
                    if !trusted {
                        await MainActor.run { [weak self] in self?.endSave() }
                        return
                    }
                }

                let config = SFTPClient.Config(
                    host: draft.host,
                    port: draft.port,
                    username: draft.username,
                    credential: draft.credential,
                    expectedHostKeyFingerprintSHA256: fingerprint
                )
                guard let result = try await runtimeFlags.withAsyncProfileMutationLease(
                    profileID: editingProfileID,
                    {
                        try await SFTPClient.verifyBasePathWritable(config: config, basePath: draft.basePath)
                        try Task.checkCancellation()

                        return try await MainActor.run { [weak self] in
                            guard let self, !Task.isCancelled else { throw CancellationError() }
                            let blobJSON = try draft.credential.encodedJSONString()
                            guard let profile = try self.dependencies.appRuntimeFlags.withProfileMutationLease(
                                profileID: self.editingProfile?.id,
                                {
                                    let profile = try self.persistProfile(draft: draft, fingerprint: fingerprint)
                                    if self.editingProfile != nil {
                                        self.onSaved(profile, blobJSON)
                                    }
                                    return profile
                                }
                            ) else {
                                throw RemoteStorageClientError.unavailable
                            }
                            return (profile, blobJSON)
                        }
                    }
                ) else {
                    await MainActor.run { [weak self] in
                        guard let self else { return }
                        self.endSave()
                        self.presentMutationBlockedAlert()
                    }
                    return
                }
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    self.endSave()
                    if self.editingProfile == nil {
                        self.onSaved(result.0, result.1)
                    }
                    self.popAfterSave()
                }
            } catch is CancellationError {
                await MainActor.run { [weak self] in self?.endSave() }
                return
            } catch {
                if Task.isCancelled {
                    await MainActor.run { [weak self] in self?.endSave() }
                    return
                }
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    self.endSave()
                    self.presentAlert(
                        title: String(localized: "auth.sftp.testConnectionFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .sftp)
                    )
                }
            }
        }
    }

    private func endSave() {
        saveTask = nil
        setSaving(false)
    }

    private struct ValidatedDraft {
        let profileName: String
        let host: String
        let port: Int
        let basePath: String
        let username: String
        let credential: SFTPCredentialBlob
        let credentialRef: String
        let baseProfile: ServerProfileRecord?
    }

    private func validateInputs() throws -> ValidatedDraft {
        guard let host = RemoteHostEndpoint.socketHost(hostText) else {
            throw NSError(domain: "AddSFTPStorage", code: 10, userInfo: [
                NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.hostRequired")
            ])
        }

        let trimmedPort = portText.trimmingCharacters(in: .whitespacesAndNewlines)
        let port: Int
        if trimmedPort.isEmpty {
            port = SFTPEndpoint.defaultPort
        } else {
            guard let parsed = Int(trimmedPort), (1 ... 65535).contains(parsed) else {
                throw NSError(domain: "AddSFTPStorage", code: 11, userInfo: [
                    NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.portRange")
                ])
            }
            port = parsed
        }

        let rawBase = basePathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let basePath = RemotePathBuilder.normalizePath(rawBase.isEmpty ? "/Watermelon" : rawBase)

        let username = usernameText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !username.isEmpty else {
            throw NSError(domain: "AddSFTPStorage", code: 1, userInfo: [
                NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.usernameRequired")
            ])
        }

        let stored = storedCredentialIfMatchingMode()
        let credential: SFTPCredentialBlob
        switch authMethod {
        case .password:
            if !passwordChanged, case .password(let saved) = stored {
                credential = .password(saved)
            } else {
                credential = .password(passwordText)
            }
        case .privateKey:
            let pem = privateKeyText.trimmingCharacters(in: .whitespacesAndNewlines)
            if privateKeyChanged {
                guard !pem.isEmpty else {
                    throw NSError(domain: "AddSFTPStorage", code: 4, userInfo: [
                        NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.privateKeyRequired")
                    ])
                }
                guard pem.contains("-----BEGIN OPENSSH PRIVATE KEY-----") else {
                    throw NSError(domain: "AddSFTPStorage", code: 3, userInfo: [
                        NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.privateKeyInvalid")
                    ])
                }
                let savedPassphrase: String?
                if case .privateKey(_, let value) = stored {
                    savedPassphrase = value
                } else {
                    savedPassphrase = nil
                }
                let effective = passphraseChanged ? (passphraseText.isEmpty ? nil : passphraseText) : savedPassphrase
                credential = .privateKey(pem: pem, passphrase: effective)
            } else if case .privateKey(let savedPEM, let savedPassphrase) = stored {
                if !privateKeyChanged, savedPassphrase == nil, passphraseChanged, !passphraseText.isEmpty {
                    throw NSError(domain: "AddSFTPStorage", code: 12, userInfo: [
                        NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.savedKeyHasNoPassphrase")
                    ])
                }
                let effective = passphraseChanged ? (passphraseText.isEmpty ? nil : passphraseText) : savedPassphrase
                credential = .privateKey(pem: savedPEM, passphrase: effective)
            } else {
                throw NSError(domain: "AddSFTPStorage", code: 4, userInfo: [
                    NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.privateKeyRequired")
                ])
            }
        }

        let existing = try findExistingProfile(host: host, port: port, basePath: basePath, username: username)
        if let existing, editingProfile == nil || existing.id != editingProfile?.id {
            throw NSError(domain: "AddSFTPStorage", code: 5, userInfo: [
                NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.duplicate")
            ])
        }

        let baseProfile = editingProfile ?? existing
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = editingProfile?.name ?? (finalName.isEmpty ? host : finalName)
        let credentialRef = StorageProfilePersistence.credentialRef(
            for: ProfileDuplicateIdentity.sftp(
                host: host,
                port: port,
                basePath: basePath,
                username: username
            )
        )

        return ValidatedDraft(
            profileName: profileName,
            host: host,
            port: port,
            basePath: basePath,
            username: username,
            credential: credential,
            credentialRef: credentialRef,
            baseProfile: baseProfile
        )
    }

    private func findExistingProfile(host: String, port: Int, basePath: String, username: String) throws -> ServerProfileRecord? {
        let expected = ProfileDuplicateIdentity.sftp(
            host: host,
            port: port,
            basePath: basePath,
            username: username
        )
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.id != editingProfile?.id && profile.duplicateIdentity == expected
        }
    }

    private func persistProfile(draft: ValidatedDraft, fingerprint: String) throws -> ServerProfileRecord {
        let params = SFTPConnectionParams(
            authMethod: authMethod,
            hostKeyFingerprintSHA256: fingerprint
        )
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(params)

        var profile = ServerProfileRecord(
            id: draft.baseProfile?.id,
            name: draft.profileName,
            storageType: StorageType.sftp.rawValue,
            connectionParams: connectionParams,
            sortOrder: draft.baseProfile?.sortOrder ?? 0,
            host: draft.host,
            port: draft.port,
            shareName: "",
            basePath: draft.basePath,
            username: draft.username,
            domain: nil,
            credentialRef: draft.credentialRef,
            backgroundBackupEnabled: draft.baseProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: draft.baseProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: draft.baseProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: draft.baseProfile?.generateRemoteThumbnails ?? false,
            createdAt: draft.baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        let blobJSON = try draft.credential.encodedJSONString()
        try StorageProfilePersistence.saveRemoteProfile(
            dependencies: dependencies,
            profile: &profile,
            credential: blobJSON,
            replacing: draft.baseProfile
        )
        return profile
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func setSaving(_ saving: Bool) {
        setOperationNavigationLocked(saving)
        tableView.isUserInteractionEnabled = !saving
        if saving {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
        }
    }

    private func setOperationNavigationLocked(_ locked: Bool) {
        isModalInPresentation = locked
        navigationController?.interactivePopGestureRecognizer?.isEnabled = !locked
        if !locked {
            navigationController?.isModalInPresentation = false
        } else if navigationController?.presentingViewController != nil || navigationController?.isBeingPresented == true {
            navigationController?.isModalInPresentation = locked
        }
    }

    private func rejectIfProfileMutationBlocked() -> Bool {
        let blocked = dependencies.appRuntimeFlags.isExecuting ||
            dependencies.remoteMaintenanceController.isBusy ||
            dependencies.appRuntimeFlags.isConnecting(profileID: editingProfile?.id)
        if blocked {
            presentMutationBlockedAlert()
        }
        return blocked
    }

    private func presentMutationBlockedAlert() {
        presentAlert(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress")
        )
    }

    @MainActor
    private func promptUserForFingerprint(_ fingerprint: String) async -> Bool {
        await runFingerprintPrompt(
            title: String(localized: "auth.sftp.hostKey.confirmTitle"),
            message: String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.confirmBody"),
                fingerprint
            ),
            confirmTitle: String(localized: "auth.sftp.hostKey.confirmAction"),
            confirmStyle: .default
        )
    }

    @MainActor
    private func promptUserForFingerprintChange(expected: String, actual: String) async -> Bool {
        await runFingerprintPrompt(
            title: String(localized: "auth.sftp.hostKey.changedTitle"),
            message: String.localizedStringWithFormat(
                String(localized: "auth.sftp.hostKey.changedBody"),
                expected,
                actual
            ),
            confirmTitle: String(localized: "auth.sftp.hostKey.changedAction"),
            confirmStyle: .destructive
        )
    }

    @MainActor
    private func runFingerprintPrompt(
        title: String,
        message: String,
        confirmTitle: String,
        confirmStyle: UIAlertAction.Style
    ) async -> Bool {
        guard !Task.isCancelled,
              viewIfLoaded?.window != nil,
              navigationController?.topViewController === self,
              pendingPromptContinuation == nil else { return false }
        return await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            guard !Task.isCancelled,
                  viewIfLoaded?.window != nil,
                  navigationController?.topViewController === self,
                  pendingPromptContinuation == nil else {
                continuation.resume(returning: false)
                return
            }
            pendingPromptContinuation = continuation
            let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { [weak self] _ in
                self?.resolvePendingPrompt(false)
            })
            alert.addAction(UIAlertAction(title: confirmTitle, style: confirmStyle) { [weak self] _ in
                self?.resolvePendingPrompt(true)
            })
            present(alert, animated: true)
        }
    }

    // First-write-wins: alert action and viewWillDisappear can both call this; whichever runs first resumes.
    private func resolvePendingPrompt(_ value: Bool) {
        guard let cont = pendingPromptContinuation else { return }
        pendingPromptContinuation = nil
        cont.resume(returning: value)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    private func registerKeyboardNotifications() {
        let center = NotificationCenter.default
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillShowNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: true)
        })
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillHideNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: false)
        })
    }

    private func handleKeyboard(note: Notification, showing: Bool) {
        guard let info = note.userInfo,
              let frame = info[UIResponder.keyboardFrameEndUserInfoKey] as? CGRect,
              let duration = info[UIResponder.keyboardAnimationDurationUserInfoKey] as? TimeInterval else {
            return
        }
        let keyboardFrame = view.convert(frame, from: nil)
        let overlap = max(0, view.bounds.maxY - keyboardFrame.minY - view.safeAreaInsets.bottom)
        let insetBottom = showing ? overlap : 0
        UIView.animate(withDuration: duration) {
            self.tableView.contentInset.bottom = insetBottom
            self.tableView.verticalScrollIndicatorInsets.bottom = insetBottom
        }
    }

    private func credentialsRowCount() -> Int {
        switch authMethod {
        case .password:
            return 3
        case .privateKey:
            return 4
        }
    }

    private func reloadCredentialsSection() {
        tableView.reloadSections(IndexSet(integer: sectionIndex(for: .credentials)), with: .automatic)
    }
}

extension AddSFTPStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = resolvedSection(at: section) else { return 0 }
        switch section {
        case .name: return 1
        case .server: return 3
        case .credentials: return credentialsRowCount()
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name: return String(localized: "auth.section.name")
        case .server: return String(localized: "auth.section.server")
        case .credentials: return String(localized: "auth.section.auth")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name, .server:
            return nil
        case .credentials:
            let compatibility = String(localized: "auth.sftp.footerCompatibility")
            if authMethod == .privateKey, storedCredentialIfMatchingMode() != nil {
                return [
                    privateKeyReuseFooterText(),
                    compatibility
                ].joined(separator: "\n\n")
            }
            return compatibility
        }
    }

    private func privateKeyReuseFooterText() -> String {
        guard case .privateKey(_, let savedPassphrase) = storedCredentialIfMatchingMode(),
              savedPassphrase == nil else {
            return String(localized: "auth.sftp.privateKeyReuseFooter", defaultValue: "Saved keys and passphrases are shown as ********. Leave them unchanged to keep them; type to replace them. Clear the passphrase to remove it.")
        }
        return String(localized: "auth.sftp.privateKeyReuseUnencryptedFooter", defaultValue: "The saved key is shown as ********. Leave it unchanged to keep it; paste a new key to replace it. The saved key has no passphrase; paste an encrypted key to add one.")
    }

    private func passwordDisplayText() -> String {
        if passwordChanged { return passwordText }
        if passwordRevealed { return revealedSavedPassword ?? "" }
        return passwordText
    }

    private func shouldMaskPassword() -> Bool {
        !passwordRevealed && !passwordChanged && storedCredentialIfMatchingMode() != nil
    }

    private func passphraseDisplayText() -> String {
        if passphraseChanged { return passphraseText }
        if passphraseRevealed { return revealedSavedPassphrase ?? "" }
        return passphraseText
    }

    private func privateKeyDisplayText() -> String {
        if privateKeyChanged { return privateKeyText }
        if privateKeyRevealed { return revealedSavedPrivateKey ?? "" }
        return privateKeyText
    }

    private func shouldMaskPrivateKey() -> Bool {
        !privateKeyRevealed && !privateKeyChanged && storedCredentialIfMatchingMode() != nil
    }

    private func shouldMaskPassphrase() -> Bool {
        guard case .privateKey(_, let savedPassphrase) = storedCredentialIfMatchingMode() else {
            return false
        }
        return !passphraseRevealed && !passphraseChanged && savedPassphrase != nil
    }

    private func resetRevealedCredentials() {
        passwordRevealed = false
        revealedSavedPassword = nil
        privateKeyRevealed = false
        revealedSavedPrivateKey = nil
        passphraseRevealed = false
        revealedSavedPassphrase = nil
    }

    @MainActor
    private func revealPasswordTapped() async {
        let wasRevealed = passwordRevealed
        view.endEditing(true)
        if wasRevealed {
            passwordRevealed = false
            revealedSavedPassword = nil
            reloadCredentialsSection()
            return
        }
        if passwordChanged || storedCredentialIfMatchingMode() == nil {
            passwordRevealed = true
            reloadCredentialsSection()
            return
        }
        guard await CredentialRevealAuthenticator.authenticate(localizedReason: String(localized: "auth.password.revealReason")),
              case .password(let saved) = storedCredentialIfMatchingMode() else { return }
        revealedSavedPassword = saved
        passwordRevealed = true
        reloadCredentialsSection()
    }

    @MainActor
    private func revealPassphraseTapped() async {
        let wasRevealed = passphraseRevealed
        view.endEditing(true)
        if wasRevealed {
            passphraseRevealed = false
            revealedSavedPassphrase = nil
            reloadCredentialsSection()
            return
        }
        if passphraseChanged {
            passphraseRevealed = true
            reloadCredentialsSection()
            return
        }
        guard await CredentialRevealAuthenticator.authenticate(localizedReason: String(localized: "auth.sftp.passphrase.revealReason")),
              case .privateKey(_, let savedPassphrase) = storedCredentialIfMatchingMode(),
              let savedPassphrase else { return }
        revealedSavedPassphrase = savedPassphrase
        passphraseRevealed = true
        reloadCredentialsSection()
    }

    @MainActor
    private func revealPrivateKeyTapped() async {
        let wasRevealed = privateKeyRevealed
        view.endEditing(true)
        if wasRevealed {
            privateKeyRevealed = false
            revealedSavedPrivateKey = nil
            reloadCredentialsSection()
            return
        }
        if privateKeyChanged || storedCredentialIfMatchingMode() == nil {
            privateKeyRevealed = true
            reloadCredentialsSection()
            return
        }
        guard await CredentialRevealAuthenticator.authenticate(localizedReason: String(localized: "auth.sftp.privateKey.revealReason")),
              case .privateKey(let savedPEM, _) = storedCredentialIfMatchingMode() else { return }
        revealedSavedPrivateKey = savedPEM
        privateKeyRevealed = true
        reloadCredentialsSection()
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(at: indexPath.section) else { return UITableViewCell() }
        switch section {
        case .name:
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: nil,
                text: nameText,
                placeholder: String(localized: "auth.sftp.placeholder.name"),
                autocapitalizationType: .words,
                onChange: { [weak self] in self?.nameText = $0 }
            )
        case .server:
            return serverCell(in: tableView, at: indexPath)
        case .credentials:
            return credentialsCell(in: tableView, at: indexPath)
        }
    }

    private func serverCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch indexPath.row {
        case 0:
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.field.host"),
                text: hostText,
                placeholder: String(localized: "auth.sftp.placeholder.host"),
                keyboardType: .URL,
                onChange: { [weak self] in self?.hostText = $0 }
            )
        case 1:
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.field.port"),
                text: portText,
                placeholder: String(SFTPEndpoint.defaultPort),
                keyboardType: .numberPad,
                onChange: { [weak self] in self?.portText = $0 }
            )
        default:
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.sftp.field.basePath"),
                text: basePathText,
                placeholder: "/Watermelon",
                onChange: { [weak self] in self?.basePathText = $0 }
            )
        }
    }

    private func credentialsCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        if indexPath.row == 0 {
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.field.username"),
                text: usernameText,
                placeholder: String(localized: "auth.sftp.placeholder.username"),
                onChange: { [weak self] in self?.usernameText = $0 }
            )
        }

        if indexPath.row == 1 {
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SFTPAuthMethodCell.reuseIdentifier,
                for: indexPath
            ) as? SFTPAuthMethodCell else { return UITableViewCell() }
            cell.configure(
                title: String(localized: "auth.sftp.authMethod"),
                selectedIndex: authMethod == .password ? 0 : 1
            )
            cell.onValueChanged = { [weak self] index in
                guard let self else { return }
                let next: SFTPConnectionParams.AuthMethod = index == 0 ? .password : .privateKey
                guard self.authMethod != next else { return }
                self.view.endEditing(true)
                self.resetRevealedCredentials()
                self.authMethod = next
                self.reloadCredentialsSection()
            }
            return cell
        }

        switch authMethod {
        case .password:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: CredentialTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? CredentialTextFieldCell else { return UITableViewCell() }
            cell.configure(
                title: String(localized: "auth.field.password"),
                text: passwordDisplayText(),
                placeholder: String(localized: "auth.sftp.placeholder.password"),
                isMasked: shouldMaskPassword(),
                isRevealed: passwordRevealed,
                revealAccessibilityLabel: String(localized: "auth.password.reveal"),
                hideAccessibilityLabel: String(localized: "auth.password.hide"),
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] value in
                self?.passwordChanged = true
                self?.passwordText = value
            }
            cell.onMaskedCredentialEdited = { [weak self] value in
                self?.passwordChanged = true
                self?.passwordRevealed = false
                self?.passwordText = value
            }
            cell.onRevealTapped = { [weak self] in
                Task { @MainActor [weak self] in await self?.revealPasswordTapped() }
            }
            cell.onEndEditing = { [weak self] in
                self?.passwordRevealed = false
                self?.revealedSavedPassword = nil
                self?.reloadCredentialsSection()
            }
            cell.onReturn = { [weak self] in self?.dismissKeyboard() }
            return cell
        case .privateKey:
            if indexPath.row == 2 {
                guard let cell = tableView.dequeueReusableCell(
                    withIdentifier: CredentialTextViewCell.reuseIdentifier,
                    for: indexPath
                ) as? CredentialTextViewCell else { return UITableViewCell() }
                cell.configure(
                    title: String(localized: "auth.sftp.authMethod.privateKey"),
                    placeholder: String(localized: "auth.sftp.placeholder.privateKey"),
                    text: privateKeyDisplayText(),
                    isMasked: shouldMaskPrivateKey(),
                    isRevealed: privateKeyRevealed,
                    hidesEnteredText: storedCredentialIfMatchingMode() != nil,
                    revealAccessibilityLabel: String(localized: "auth.sftp.privateKey.reveal"),
                    hideAccessibilityLabel: String(localized: "auth.sftp.privateKey.hide")
                )
                cell.onTextChanged = { [weak self] value in
                    self?.privateKeyChanged = true
                    self?.privateKeyRevealed = true
                    self?.privateKeyText = value
                }
                cell.onMaskedCredentialEdited = { [weak self] value in
                    self?.privateKeyChanged = true
                    self?.privateKeyRevealed = true
                    self?.privateKeyText = value
                }
                cell.onRevealTapped = { [weak self] in
                    Task { @MainActor [weak self] in await self?.revealPrivateKeyTapped() }
                }
                return cell
            }
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: CredentialTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? CredentialTextFieldCell else { return UITableViewCell() }
            cell.configure(
                title: String(localized: "auth.sftp.field.passphrase"),
                text: passphraseDisplayText(),
                placeholder: String(localized: "auth.sftp.placeholder.passphrase"),
                isMasked: shouldMaskPassphrase(),
                isRevealed: passphraseRevealed,
                revealAccessibilityLabel: String(localized: "auth.sftp.passphrase.reveal"),
                hideAccessibilityLabel: String(localized: "auth.sftp.passphrase.hide"),
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] value in
                self?.passphraseChanged = true
                self?.passphraseText = value
            }
            cell.onMaskedCredentialEdited = { [weak self] value in
                self?.passphraseChanged = true
                self?.passphraseRevealed = false
                self?.passphraseText = value
            }
            cell.onRevealTapped = { [weak self] in
                Task { @MainActor [weak self] in await self?.revealPassphraseTapped() }
            }
            cell.onEndEditing = { [weak self] in
                self?.passphraseRevealed = false
                self?.revealedSavedPassphrase = nil
                self?.reloadCredentialsSection()
            }
            cell.onReturn = { [weak self] in self?.dismissKeyboard() }
            return cell
        }
    }

    private func makeTextField(
        tableView: UITableView,
        indexPath: IndexPath,
        title: String?,
        text: String,
        placeholder: String,
        keyboardType: UIKeyboardType = .default,
        autocapitalizationType: UITextAutocapitalizationType = .none,
        isSecure: Bool = false,
        onChange: @escaping (String) -> Void
    ) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else { return UITableViewCell() }
        cell.configure(
            title: title,
            text: text,
            placeholder: placeholder,
            isSecure: isSecure,
            keyboardType: keyboardType,
            autocapitalizationType: autocapitalizationType,
            returnKeyType: .default,
            inputAccessoryView: keyboardToolbar
        )
        cell.onTextChanged = onChange
        cell.onReturn = { [weak self] in self?.dismissKeyboard() }
        return cell
    }
}

private final class SFTPAuthMethodCell: UITableViewCell {
    static let reuseIdentifier = "SFTPAuthMethodCell"

    private let titleLabel = UILabel()
    private let segmentedControl = UISegmentedControl(items: [
        String(localized: "auth.sftp.authMethod.password"),
        String(localized: "auth.sftp.authMethod.privateKey")
    ])
    private var compactConstraints: [Constraint] = []
    private var accessibilityConstraints: [Constraint] = []

    var onValueChanged: ((Int) -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        selectionStyle = .none

        var background: UIBackgroundConfiguration
        if #available(iOS 18.0, *) {
            background = .listCell()
        } else {
            background = .listGroupedCell()
        }
        background.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = background

        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.adjustsFontForContentSizeCategory = true
        titleLabel.textColor = .label
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        segmentedControl.addTarget(self, action: #selector(valueChanged), for: .valueChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(segmentedControl)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
        }

        segmentedControl.snp.makeConstraints { make in
            make.trailing.equalToSuperview().inset(16)
        }
        compactConstraints = titleLabel.snp.prepareConstraints { make in
            make.centerY.equalToSuperview()
        } + segmentedControl.snp.prepareConstraints { make in
            make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(12)
            make.centerY.equalToSuperview()
            make.top.bottom.equalToSuperview().inset(8)
        }
        accessibilityConstraints = titleLabel.snp.prepareConstraints { make in
            make.top.equalToSuperview().inset(12)
            make.trailing.lessThanOrEqualToSuperview().inset(16)
        } + segmentedControl.snp.prepareConstraints { make in
            make.top.equalTo(titleLabel.snp.bottom).offset(8)
            make.leading.equalToSuperview().inset(16)
            make.bottom.equalToSuperview().inset(8)
        }
        updateLayoutConstraints()
        registerForTraitChanges([UITraitPreferredContentSizeCategory.self]) { (cell: SFTPAuthMethodCell, _) in
            cell.updateLayoutConstraints()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onValueChanged = nil
    }

    func configure(title: String, selectedIndex: Int) {
        titleLabel.text = title
        segmentedControl.selectedSegmentIndex = selectedIndex
    }

    @objc
    private func valueChanged() {
        onValueChanged?(segmentedControl.selectedSegmentIndex)
    }

    private func updateLayoutConstraints() {
        compactConstraints.forEach { $0.deactivate() }
        accessibilityConstraints.forEach { $0.deactivate() }
        let useVerticalLayout = SettingsFormLayoutPolicy.usesVerticalLayout(
            for: traitCollection.preferredContentSizeCategory
        )
        titleLabel.numberOfLines = useVerticalLayout ? 0 : 1
        (useVerticalLayout ? accessibilityConstraints : compactConstraints).forEach { $0.activate() }
    }
}
