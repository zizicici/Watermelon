import SnapKit
import UIKit

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
        if isMovingFromParent || isBeingDismissed {
            saveTask?.cancel()
            saveTask = nil
            // CheckedContinuation crashes if dropped unresumed — release any in-flight host-key prompt.
            resolvePendingPrompt(false)
        }
    }

    private func fillInitialValues() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
        usernameText = editingProfile.username
        hostText = editingProfile.host
        portText = editingProfile.port == 0 || editingProfile.port == 22 ? "" : String(editingProfile.port)
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
        tableView.register(SFTPAuthMethodCell.self, forCellReuseIdentifier: SFTPAuthMethodCell.reuseIdentifier)
        tableView.register(SFTPPrivateKeyCell.self, forCellReuseIdentifier: SFTPPrivateKeyCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard saveTask == nil else { return }

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
        let isReusingSameEndpoint = editingProfile.map { $0.host == draft.host && $0.port == draft.port } ?? false
        saveTask = Task { [weak self] in
            guard let self else { return }
            do {
                let fingerprint = try await SFTPClient.captureHostKeyFingerprint(host: draft.host, port: draft.port)
                try Task.checkCancellation()

                if fingerprint != existingFingerprint {
                    let trusted: Bool
                    // Same endpoint but different key = known-host mismatch, not first-time trust.
                    if isReusingSameEndpoint, let expected = existingFingerprint {
                        trusted = await self.promptUserForFingerprintChange(expected: expected, actual: fingerprint)
                    } else {
                        trusted = await self.promptUserForFingerprint(fingerprint)
                    }
                    try Task.checkCancellation()
                    if !trusted {
                        await MainActor.run { self.endSave() }
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
                try await SFTPClient.verifyBasePathWritable(config: config, basePath: draft.basePath)
                try Task.checkCancellation()

                await MainActor.run {
                    self.saveTask = nil
                    self.finishCommit(draft: draft, fingerprint: fingerprint)
                }
            } catch is CancellationError {
                return
            } catch {
                if Task.isCancelled { return }
                await MainActor.run {
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

    private func finishCommit(draft: ValidatedDraft, fingerprint: String) {
        do {
            let profile = try persistProfile(draft: draft, fingerprint: fingerprint)
            let blobJSON = try draft.credential.encodedJSONString()
            onSaved(profile, blobJSON)
            popAfterSave()
        } catch {
            setSaving(false)
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .sftp)
            )
        }
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
        let host = hostText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !host.isEmpty else {
            throw NSError(domain: "AddSFTPStorage", code: 10, userInfo: [
                NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.hostRequired")
            ])
        }

        let trimmedPort = portText.trimmingCharacters(in: .whitespacesAndNewlines)
        let port: Int
        if trimmedPort.isEmpty {
            port = 22
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
            // Don't trim — leading/trailing whitespace is legal in SSH passwords.
            if !passwordText.isEmpty {
                credential = .password(passwordText)
            } else if case .password(let saved) = stored {
                credential = .password(saved)
            } else {
                throw NSError(domain: "AddSFTPStorage", code: 2, userInfo: [
                    NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.passwordRequired")
                ])
            }
        case .privateKey:
            let pem = privateKeyText.trimmingCharacters(in: .whitespacesAndNewlines)
            if !pem.isEmpty {
                guard pem.contains("-----BEGIN OPENSSH PRIVATE KEY-----") else {
                    throw NSError(domain: "AddSFTPStorage", code: 3, userInfo: [
                        NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.privateKeyInvalid")
                    ])
                }
                credential = .privateKey(pem: pem, passphrase: passphraseText.isEmpty ? nil : passphraseText)
            } else if case .privateKey(let savedPEM, let savedPassphrase) = stored {
                let effective = passphraseText.isEmpty ? savedPassphrase : passphraseText
                credential = .privateKey(pem: savedPEM, passphrase: effective)
            } else {
                throw NSError(domain: "AddSFTPStorage", code: 4, userInfo: [
                    NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.privateKeyRequired")
                ])
            }
        }

        let existing = try findExistingProfile(host: host, port: port, basePath: basePath, username: username)
        if let editingProfile, let existing, existing.id != editingProfile.id {
            throw NSError(domain: "AddSFTPStorage", code: 5, userInfo: [
                NSLocalizedDescriptionKey: String(localized: "auth.sftp.validation.duplicate")
            ])
        }

        let baseProfile = editingProfile ?? existing
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? host : finalName
        let credentialRef = "sftp|\(host):\(port)|\(username)|\(basePath)"

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
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .sftp &&
                profile.host == host &&
                profile.port == port &&
                RemotePathBuilder.normalizePath(profile.basePath) == basePath &&
                profile.username == username
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
            createdAt: draft.baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        let blobJSON = try draft.credential.encodedJSONString()
        try dependencies.keychainService.save(password: blobJSON, account: draft.credentialRef)
        if let oldRef = editingProfile?.credentialRef, oldRef != draft.credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return profile
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        if let manageVC = navigationController.viewControllers.first(where: { $0 is ManageStorageProfilesViewController }) {
            navigationController.popToViewController(manageVC, animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func setSaving(_ saving: Bool) {
        tableView.isUserInteractionEnabled = !saving
        if saving {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
        }
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
        await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
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
        tableView.reloadSections(IndexSet(integer: Section.credentials.rawValue), with: .automatic)
    }
}

extension AddSFTPStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = Section(rawValue: section) else { return 0 }
        switch section {
        case .name: return 1
        case .server: return 3
        case .credentials: return credentialsRowCount()
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name: return String(localized: "auth.section.name")
        case .server: return String(localized: "auth.section.server")
        case .credentials: return String(localized: "auth.section.auth")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section) else { return UITableViewCell() }
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
                placeholder: "22",
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
                self.authMethod = next
                self.reloadCredentialsSection()
            }
            return cell
        }

        switch authMethod {
        case .password:
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.field.password"),
                text: passwordText,
                placeholder: editingProfile == nil
                    ? String(localized: "auth.sftp.placeholder.password")
                    : String(localized: "auth.passwordPlaceholderEdit"),
                isSecure: true,
                onChange: { [weak self] in self?.passwordText = $0 }
            )
        case .privateKey:
            if indexPath.row == 2 {
                guard let cell = tableView.dequeueReusableCell(
                    withIdentifier: SFTPPrivateKeyCell.reuseIdentifier,
                    for: indexPath
                ) as? SFTPPrivateKeyCell else { return UITableViewCell() }
                cell.configure(
                    placeholder: String(localized: "auth.sftp.placeholder.privateKey"),
                    text: privateKeyText
                )
                cell.onTextChanged = { [weak self] in self?.privateKeyText = $0 }
                return cell
            }
            return makeTextField(
                tableView: tableView,
                indexPath: indexPath,
                title: String(localized: "auth.sftp.field.passphrase"),
                text: passphraseText,
                placeholder: String(localized: "auth.sftp.placeholder.passphrase"),
                isSecure: true,
                onChange: { [weak self] in self?.passphraseText = $0 }
            )
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
        titleLabel.textColor = .label
        titleLabel.setContentHuggingPriority(.required, for: .horizontal)
        titleLabel.setContentCompressionResistancePriority(.required, for: .horizontal)

        segmentedControl.addTarget(self, action: #selector(valueChanged), for: .valueChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(segmentedControl)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }

        segmentedControl.snp.makeConstraints { make in
            make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(12)
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
            make.top.bottom.equalToSuperview().inset(8)
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
}

private final class SFTPPrivateKeyCell: UITableViewCell, UITextViewDelegate {
    static let reuseIdentifier = "SFTPPrivateKeyCell"

    private let textView = UITextView()
    private let placeholderLabel = UILabel()

    var onTextChanged: ((String) -> Void)?

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

        textView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        textView.backgroundColor = .clear
        textView.autocapitalizationType = .none
        textView.autocorrectionType = .no
        textView.spellCheckingType = .no
        textView.smartQuotesType = .no
        textView.smartDashesType = .no
        textView.delegate = self
        textView.isScrollEnabled = false
        textView.textContainerInset = .zero
        textView.textContainer.lineFragmentPadding = 0

        placeholderLabel.font = textView.font
        placeholderLabel.textColor = .placeholderText
        placeholderLabel.numberOfLines = 0

        contentView.addSubview(textView)
        contentView.addSubview(placeholderLabel)

        textView.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(UIEdgeInsets(top: 12, left: 16, bottom: 12, right: 16))
            make.height.greaterThanOrEqualTo(120)
        }
        placeholderLabel.snp.makeConstraints { make in
            make.top.equalTo(textView)
            make.leading.trailing.equalTo(textView)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onTextChanged = nil
        textView.text = ""
        placeholderLabel.isHidden = false
    }

    func configure(placeholder: String, text: String) {
        placeholderLabel.text = placeholder
        textView.text = text
        placeholderLabel.isHidden = !text.isEmpty
    }

    func textViewDidChange(_ textView: UITextView) {
        placeholderLabel.isHidden = !textView.text.isEmpty
        onTextChanged?(textView.text)
    }
}
