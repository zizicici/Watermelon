import SnapKit
import UIKit

final class AddWebDAVStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case server
        case paths
        case credentials
        case testConnection
    }

    private enum Field {
        case name
        case host
        case port
        case mountPath
        case basePath
        case username
        case password
    }

    private static let schemes = ["http", "https"]

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
    private var commitGate = StorageProfileCommitGate()
    private lazy var connectionTestRunner = ScreenBoundAsyncRunner<Void>(
        isScreenActive: { [weak self] in self?.isScreenActiveForAsyncCompletion ?? false },
        onStateChanged: { [weak self] in self?.applyConnectionTestState() }
    )

    private var nameText = ""
    private var schemeIndex = 1 // default HTTPS
    private var hostText = ""
    private var portText = ""
    private var mountPathText = "/"
    private var basePathText = ""
    private var usernameText = ""
    private var passwordText = ""
    private let hasSavedPassword: Bool
    private var passwordChanged = false
    private var passwordRevealed = false
    private var revealedSavedPassword: String?

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.server, .paths, .credentials, .testConnection]
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
        self.hasSavedPassword = Self.hasSavedPassword(dependencies: dependencies, editingProfile: editingProfile)
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = editingProfile == nil ? String(localized: "auth.webdav.title") : String(localized: "auth.webdav.editTitle")

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
            connectionTestRunner.cancel()
        }
    }

    private func fillInitialValues() {
        if let editingProfile {
            nameText = editingProfile.name
            usernameText = editingProfile.username
            basePathText = editingProfile.basePath

            let scheme = (editingProfile.webDAVParams?.scheme ?? "https").lowercased()
            schemeIndex = Self.schemes.firstIndex(of: scheme) ?? 1
            hostText = editingProfile.host
            portText = editingProfile.port == 0 ? "" : String(editingProfile.port)
            let rawMount = editingProfile.shareName.trimmingCharacters(in: .whitespacesAndNewlines)
            mountPathText = rawMount.isEmpty ? "/" : RemotePathBuilder.normalizePath(rawMount)
            return
        }
        basePathText = "/Watermelon"
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
        tableView.register(WebDAVSchemeCell.self, forCellReuseIdentifier: WebDAVSchemeCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.top.equalToSuperview()
            make.leading.trailing.bottom.equalTo(view.safeAreaLayoutGuide)
        }
    }

    private func currentScheme() -> String {
        Self.schemes[schemeIndex]
    }

    private func defaultPortForCurrentScheme() -> Int {
        currentScheme() == "https" ? 443 : 80
    }

    private func portPlaceholder() -> String {
        "\(defaultPortForCurrentScheme())"
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard !connectionTestRunner.isRunning, commitGate.begin() else { return }
        updateSaveCommitState()
        var didCommit = false
        defer {
            if !didCommit {
                commitGate.releaseAfterFailure()
                updateSaveCommitState()
            }
        }
        guard !rejectIfProfileMutationBlocked() else { return }

        let draft: ValidatedDraft
        do {
            draft = try validateInputs()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
            )
            return
        }

        do {
            guard let profile = try dependencies.storageProfileMutationService.saveRemoteProfile(
                editingProfile: editingProfile,
                credential: draft.password,
                makeProfile: { liveProfile in
                    try self.makeProfile(draft: draft, baseProfile: liveProfile)
                }
            ) else {
                presentMutationBlockedAlert()
                return
            }
            didCommit = true
            if editingProfile == nil {
                let savedCallback = onSaved
                StorageProfileSaveTransition.completeCreate(
                    from: self,
                    shouldPopToRoot: shouldPopToRootOnSave
                ) {
                    savedCallback(profile, draft.password)
                }
            } else {
                onSaved(profile, draft.password)
                popAfterSave()
            }
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
            )
        }
    }

    private func testConnectionTapped() {
        dismissKeyboard()
        guard !connectionTestRunner.isRunning, !commitGate.isCommitting, !rejectIfProfileMutationBlocked() else { return }
        let draft: ValidatedDraft
        do {
            draft = try validateInputs()
        } catch {
            presentAlert(
                title: String(localized: "auth.testConnectionFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
            )
            return
        }

        connectionTestRunner.start(
            operation: {
                try await Self.testConnection(draft: draft)
            },
            completion: { [weak self] result in
                guard let self else { return }
                switch result {
                case .success:
                    self.presentAlert(
                        title: String(localized: "auth.testConnectionSucceeded"),
                        message: String(localized: "auth.testConnectionSucceededMessage")
                    )
                case .failure(let error):
                    self.presentAlert(
                        title: String(localized: "auth.testConnectionFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
                    )
                }
            }
        )
    }

    private struct ValidatedDraft {
        let scheme: String
        let host: String
        let port: Int
        let normalizedMountPath: String
        let normalizedBasePath: String
        let username: String
        let password: String
        let endpointURL: URL
        let credentialRef: String
        let profileName: String
    }

    private func validateInputs() throws -> ValidatedDraft {
        let scheme = currentScheme()

        guard let host = RemoteHostEndpoint.socketHost(hostText) else {
            throw NSError(domain: "AddWebDAVStorage", code: 10, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationHost")])
        }

        let trimmedPort = portText.trimmingCharacters(in: .whitespacesAndNewlines)
        let port: Int
        if trimmedPort.isEmpty {
            port = defaultPortForCurrentScheme()
        } else {
            guard let parsed = Int(trimmedPort), (1 ... 65535).contains(parsed) else {
                throw NSError(domain: "AddWebDAVStorage", code: 11, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationPort")])
            }
            port = parsed
        }

        let rawMount = mountPathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedMountPath = try WebDAVPathCanonicalizer.canonicalRawPath(rawMount.isEmpty ? "/" : rawMount)

        let rawBase = basePathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedBasePath = try WebDAVPathCanonicalizer.canonicalRawPath(rawBase.isEmpty ? "/Watermelon" : rawBase)

        let username = usernameText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !username.isEmpty else {
            throw NSError(domain: "AddWebDAVStorage", code: 1, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationUsername")])
        }

        let password: String
        if hasSavedPassword, !passwordChanged, let editingProfile {
            password = try dependencies.keychainService.readPassword(account: editingProfile.credentialRef)
        } else {
            // Remote credentials may intentionally start or end with whitespace.
            password = passwordText
        }

        let connection = try CanonicalWebDAVConnection(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: normalizedMountPath,
            basePath: normalizedBasePath,
            username: username
        )
        guard let endpointURL = connection.endpointURL else {
            throw NSError(domain: "AddWebDAVStorage", code: 5, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.invalidEndpoint")])
        }
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = editingProfile?.name ?? (finalName.isEmpty ? connection.host.socketHost : finalName)
        let identity = CanonicalProfileConnection.webDAV(connection).duplicateIdentity
        let credentialRef = StorageProfilePersistence.credentialRef(for: identity)

        return ValidatedDraft(
            scheme: connection.scheme.rawValue,
            host: connection.host.socketHost,
            port: connection.port.value,
            normalizedMountPath: connection.mountPath,
            normalizedBasePath: connection.basePath,
            username: connection.username,
            password: password,
            endpointURL: endpointURL,
            credentialRef: credentialRef,
            profileName: profileName
        )
    }

    private static func hasSavedPassword(dependencies: DependencyContainer, editingProfile: ServerProfileRecord?) -> Bool {
        guard let editingProfile else { return false }
        return (try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef)) != nil
    }

    private static func testConnection(draft: ValidatedDraft) async throws {
        let config = WebDAVClient.Config(
            endpointURL: draft.endpointURL,
            username: draft.username,
            password: draft.password
        )
        let client = WebDAVClient(config: config)
        do {
            try await client.connect()
            _ = try await client.list(path: draft.normalizedBasePath)
            await client.disconnect()
        } catch {
            await client.disconnect()
            throw error
        }
    }

    private func makeProfile(
        draft: ValidatedDraft,
        baseProfile: ServerProfileRecord?
    ) throws -> ServerProfileRecord {
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(scheme: draft.scheme)
        )

        return ServerProfileRecord(
            id: baseProfile?.id,
            name: baseProfile?.name ?? draft.profileName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: connectionParams,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: draft.host,
            port: draft.port,
            shareName: draft.normalizedMountPath,
            basePath: draft.normalizedBasePath,
            username: draft.username,
            domain: nil,
            credentialRef: draft.credentialRef,
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: baseProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: baseProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: baseProfile?.generateRemoteThumbnails ?? false,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private var isScreenActiveForAsyncCompletion: Bool {
        viewIfLoaded?.window != nil && (navigationController.map { $0.topViewController === self } ?? true)
    }

    private func applyConnectionTestState() {
        let isRunning = connectionTestRunner.isRunning
        tableView.isUserInteractionEnabled = !isRunning
        if isRunning {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
            updateSaveCommitState()
        }
        reloadTestConnectionRow()
    }

    private func updateSaveCommitState() {
        saveBarButtonItem.isEnabled = !commitGate.isCommitting
        reloadTestConnectionRow()
    }

    private func reloadTestConnectionRow() {
        guard let section = visibleSections.firstIndex(of: .testConnection),
              tableView.numberOfSections > section,
              tableView.numberOfRows(inSection: section) > 0 else { return }
        tableView.reloadRows(at: [IndexPath(row: 0, section: section)], with: .none)
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

    private func focusField(_ field: Field?) {
        guard let field else {
            dismissKeyboard()
            return
        }

        let indexPath = indexPath(for: field)
        tableView.scrollToRow(at: indexPath, at: .middle, animated: true)
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            if let cell = self.tableView.cellForRow(at: indexPath) as? SettingsTextFieldCell {
                cell.focus()
            } else if let cell = self.tableView.cellForRow(at: indexPath) as? CredentialTextFieldCell {
                cell.focus()
            }
        }
    }

    private func indexPath(for field: Field) -> IndexPath {
        switch field {
        case .name:
            return IndexPath(row: 0, section: sectionIndex(for: .name))
        case .host:
            return IndexPath(row: 1, section: sectionIndex(for: .server))
        case .port:
            return IndexPath(row: 2, section: sectionIndex(for: .server))
        case .mountPath:
            return IndexPath(row: 0, section: sectionIndex(for: .paths))
        case .basePath:
            return IndexPath(row: 1, section: sectionIndex(for: .paths))
        case .username:
            return IndexPath(row: 0, section: sectionIndex(for: .credentials))
        case .password:
            return IndexPath(row: 1, section: sectionIndex(for: .credentials))
        }
    }

    private func passwordDisplayText() -> String {
        if passwordChanged { return passwordText }
        return passwordRevealed ? (revealedSavedPassword ?? "") : passwordText
    }

    private func shouldMaskPassword() -> Bool {
        !passwordRevealed && hasSavedPassword && !passwordChanged
    }

    @MainActor
    private func revealPasswordTapped() async {
        let wasRevealed = passwordRevealed
        view.endEditing(true)
        if wasRevealed {
            passwordRevealed = false
            revealedSavedPassword = nil
            tableView.reloadRows(at: [indexPath(for: .password)], with: .none)
            return
        }
        if passwordChanged || !hasSavedPassword {
            passwordRevealed = true
            tableView.reloadRows(at: [indexPath(for: .password)], with: .none)
            return
        }
        guard await CredentialRevealAuthenticator.authenticate(localizedReason: String(localized: "auth.password.revealReason")),
              let editingProfile,
              let saved = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef) else {
            return
        }
        revealedSavedPassword = saved
        passwordRevealed = true
        tableView.reloadRows(at: [indexPath(for: .password)], with: .none)
    }

    private func reloadPortCell() {
        tableView.reloadRows(at: [indexPath(for: .port)], with: .none)
    }
}

extension AddWebDAVStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = resolvedSection(at: section) else { return 0 }
        switch section {
        case .name:
            return 1
        case .server:
            return 3
        case .paths:
            return 2
        case .credentials:
            return 2
        case .testConnection:
            return 1
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .server:
            return String(localized: "auth.section.server")
        case .paths:
            return String(localized: "auth.section.paths")
        case .credentials:
            return String(localized: "auth.section.auth")
        case .testConnection:
            return nil
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .server:
            return String(localized: "auth.webdav.footerServer")
        case .paths:
            return String(localized: "auth.webdav.footerNew")
        case .credentials:
            return editingProfile == nil ? nil : String(localized: "auth.webdav.footerEdit")
        case .testConnection:
            return nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(at: indexPath.section) else {
            return UITableViewCell()
        }

        switch section {
        case .name:
            return nameCell(in: tableView, at: indexPath)
        case .server:
            return serverCell(in: tableView, at: indexPath)
        case .paths:
            return pathsCell(in: tableView, at: indexPath)
        case .credentials:
            return credentialsCell(in: tableView, at: indexPath)
        case .testConnection:
            let cell = tableView.dequeueReusableCell(withIdentifier: "TestConnectionCell")
                ?? UITableViewCell(style: .default, reuseIdentifier: "TestConnectionCell")
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "auth.testConnection")
            let isLocked = connectionTestRunner.isRunning || commitGate.isCommitting
            content.textProperties.color = isLocked ? .secondaryLabel : .systemBlue
            content.textProperties.alignment = .center
            cell.contentConfiguration = content
            cell.selectionStyle = isLocked ? .none : .default
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard resolvedSection(at: indexPath.section) == .testConnection else { return }
        testConnectionTapped()
    }

    private func nameCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else {
            return UITableViewCell()
        }
        cell.configure(
            title: nil,
            text: nameText,
            placeholder: String(localized: "auth.webdav.placeholder.name"),
            autocapitalizationType: .words,
            returnKeyType: .next,
            inputAccessoryView: keyboardToolbar
        )
        cell.onTextChanged = { [weak self] in self?.nameText = $0 }
        cell.onReturn = { [weak self] in self?.focusField(.host) }
        return cell
    }

    private func serverCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch indexPath.row {
        case 0:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: WebDAVSchemeCell.reuseIdentifier,
                for: indexPath
            ) as? WebDAVSchemeCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: String(localized: "auth.webdav.fieldScheme"),
                selectedIndex: schemeIndex
            )
            cell.onValueChanged = { [weak self] index in
                guard let self else { return }
                self.schemeIndex = index
                self.reloadPortCell()
            }
            return cell
        case 1:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: String(localized: "auth.field.host"),
                text: hostText,
                placeholder: String(localized: "auth.webdav.placeholder.host"),
                keyboardType: .URL,
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.hostText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.port) }
            return cell
        default:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: String(localized: "auth.field.port"),
                text: portText,
                placeholder: portPlaceholder(),
                keyboardType: .numberPad,
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.portText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.mountPath) }
            return cell
        }
    }

    private func pathsCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else {
            return UITableViewCell()
        }

        if indexPath.row == 0 {
            cell.configure(
                title: String(localized: "auth.webdav.fieldMountPath"),
                text: mountPathText,
                placeholder: String(localized: "auth.webdav.placeholder.mountPath"),
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.mountPathText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.basePath) }
        } else {
            cell.configure(
                title: String(localized: "auth.webdav.fieldBasePath"),
                text: basePathText,
                placeholder: String(localized: "auth.webdav.placeholder.basePath"),
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.basePathText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.username) }
        }
        return cell
    }

    private func credentialsCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        if indexPath.row == 0 {
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: String(localized: "auth.field.username"),
                text: usernameText,
                placeholder: String(localized: "auth.webdav.placeholder.username"),
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.usernameText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.password) }
            return cell
        } else {
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: CredentialTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? CredentialTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: String(localized: "auth.field.password"),
                text: passwordDisplayText(),
                placeholder: String(localized: "auth.webdav.placeholder.password"),
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
                guard let self else { return }
                self.passwordRevealed = false
                self.revealedSavedPassword = nil
                self.tableView.reloadRows(at: [self.indexPath(for: .password)], with: .none)
            }
            cell.onReturn = { [weak self] in self?.focusField(nil) }
            return cell
        }
    }
}

private final class WebDAVSchemeCell: UITableViewCell {
    static let reuseIdentifier = "WebDAVSchemeCell"

    private let titleLabel = UILabel()
    private let segmentedControl = UISegmentedControl(items: ["HTTP", "HTTPS"])
    private var compactConstraints: [Constraint] = []
    private var accessibilityConstraints: [Constraint] = []

    var onValueChanged: ((Int) -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)

        selectionStyle = .none

        let background: UIBackgroundConfiguration
        if #available(iOS 18.0, *) {
            background = .listCell()
        } else {
            background = .listGroupedCell()
        }
        var configuredBackground = background
        configuredBackground.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = configuredBackground

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
        registerForTraitChanges([UITraitPreferredContentSizeCategory.self]) { (cell: WebDAVSchemeCell, _) in
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
