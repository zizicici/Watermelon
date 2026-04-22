import SnapKit
import UIKit

final class AddWebDAVStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case server
        case paths
        case credentials
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
    private var isSaving = false

    private var nameText = ""
    private var schemeIndex = 1 // default HTTPS
    private var hostText = ""
    private var portText = ""
    private var mountPathText = "/"
    private var basePathText = ""
    private var usernameText = ""
    private var passwordText = ""

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
        title = editingProfile == nil ? String(localized: "auth.webdav.title") : String(localized: "auth.webdav.editTitle")

        fillInitialValues()
        configureUI()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
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
        tableView.register(WebDAVSchemeCell.self, forCellReuseIdentifier: WebDAVSchemeCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
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
        guard !isSaving else { return }

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

        setSaving(true)
        Task { [weak self] in
            guard let self else { return }
            do {
                try await Self.verifyConnection(draft: draft)
            } catch {
                await MainActor.run {
                    self.setSaving(false)
                    self.presentAlert(
                        title: String(localized: "auth.saveFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
                    )
                }
                return
            }

            await MainActor.run {
                do {
                    let profile = try self.commitProfile(draft: draft)
                    self.onSaved(profile, draft.password)
                    self.popAfterSave()
                } catch {
                    self.setSaving(false)
                    self.presentAlert(
                        title: String(localized: "auth.saveFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .webdav)
                    )
                }
            }
        }
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
        let endpointURLString: String
        let credentialRef: String
        let profileName: String
        let baseProfile: ServerProfileRecord?
    }

    private func validateInputs() throws -> ValidatedDraft {
        let scheme = currentScheme()

        let host = hostText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !host.isEmpty else {
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
        let normalizedMountPath = RemotePathBuilder.normalizePath(rawMount.isEmpty ? "/" : rawMount)

        let rawBase = basePathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedBasePath = RemotePathBuilder.normalizePath(rawBase.isEmpty ? "/Watermelon" : rawBase)

        let username = usernameText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !username.isEmpty else {
            throw NSError(domain: "AddWebDAVStorage", code: 1, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationUsername")])
        }

        let trimmedPassword = passwordText.trimmingCharacters(in: .whitespacesAndNewlines)
        let password: String
        if !trimmedPassword.isEmpty {
            password = trimmedPassword
        } else if let editingProfile,
                  let saved = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef),
                  !saved.isEmpty {
            password = saved
        } else {
            throw NSError(domain: "AddWebDAVStorage", code: 2, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationPassword")])
        }

        guard let endpointURL = ServerProfileRecord.buildWebDAVEndpointURL(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: normalizedMountPath
        ) else {
            throw NSError(domain: "AddWebDAVStorage", code: 5, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.invalidEndpoint")])
        }
        let endpointURLString = endpointURL.absoluteString

        let existing = try findExistingProfile(
            scheme: scheme,
            host: host,
            port: port,
            mountPath: normalizedMountPath,
            basePath: normalizedBasePath,
            username: username
        )
        if let editingProfile,
           let existing,
           existing.id != editingProfile.id {
            throw NSError(
                domain: "AddWebDAVStorage",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.duplicateConfig")]
            )
        }

        let baseProfile = editingProfile ?? existing
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? host : finalName
        let credentialRef = "webdav|\(endpointURLString)|\(username)"

        return ValidatedDraft(
            scheme: scheme,
            host: host,
            port: port,
            normalizedMountPath: normalizedMountPath,
            normalizedBasePath: normalizedBasePath,
            username: username,
            password: password,
            endpointURL: endpointURL,
            endpointURLString: endpointURLString,
            credentialRef: credentialRef,
            profileName: profileName,
            baseProfile: baseProfile
        )
    }

    private static func verifyConnection(draft: ValidatedDraft) async throws {
        let client = WebDAVClient(config: WebDAVClient.Config(
            endpointURL: draft.endpointURL,
            username: draft.username,
            password: draft.password
        ))
        do {
            try await client.connect()
        } catch {
            await client.disconnect()
            throw error
        }
        await client.disconnect()
    }

    private func commitProfile(draft: ValidatedDraft) throws -> ServerProfileRecord {
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(scheme: draft.scheme)
        )

        var profile = ServerProfileRecord(
            id: draft.baseProfile?.id,
            name: draft.profileName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: connectionParams,
            sortOrder: draft.baseProfile?.sortOrder ?? 0,
            host: draft.host,
            port: draft.port,
            shareName: draft.normalizedMountPath,
            basePath: draft.normalizedBasePath,
            username: draft.username,
            domain: nil,
            credentialRef: draft.credentialRef,
            backgroundBackupEnabled: draft.baseProfile?.backgroundBackupEnabled ?? true,
            createdAt: draft.baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: draft.password, account: draft.credentialRef)
        if let oldRef = editingProfile?.credentialRef,
           oldRef != draft.credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return profile
    }

    private func findExistingProfile(
        scheme: String,
        host: String,
        port: Int,
        mountPath: String,
        basePath: String,
        username: String
    ) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .webdav &&
                profile.webDAVParams?.scheme == scheme &&
                profile.host == host &&
                profile.port == port &&
                RemotePathBuilder.normalizePath(profile.shareName) == mountPath &&
                RemotePathBuilder.normalizePath(profile.basePath) == basePath &&
                profile.username == username
        }
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
        isSaving = saving
        tableView.isUserInteractionEnabled = !saving
        if saving {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
        }
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
            guard let self,
                  let cell = self.tableView.cellForRow(at: indexPath) as? SettingsTextFieldCell else { return }
            cell.focus()
        }
    }

    private func indexPath(for field: Field) -> IndexPath {
        switch field {
        case .name:
            return IndexPath(row: 0, section: Section.name.rawValue)
        case .host:
            return IndexPath(row: 1, section: Section.server.rawValue)
        case .port:
            return IndexPath(row: 2, section: Section.server.rawValue)
        case .mountPath:
            return IndexPath(row: 0, section: Section.paths.rawValue)
        case .basePath:
            return IndexPath(row: 1, section: Section.paths.rawValue)
        case .username:
            return IndexPath(row: 0, section: Section.credentials.rawValue)
        case .password:
            return IndexPath(row: 1, section: Section.credentials.rawValue)
        }
    }

    private func reloadPortCell() {
        tableView.reloadRows(at: [indexPath(for: .port)], with: .none)
    }
}

extension AddWebDAVStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = Section(rawValue: section) else { return 0 }
        switch section {
        case .name:
            return 1
        case .server:
            return 3
        case .paths:
            return 2
        case .credentials:
            return 2
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .server:
            return String(localized: "auth.section.server")
        case .paths:
            return String(localized: "auth.section.paths")
        case .credentials:
            return String(localized: "auth.section.auth")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .server:
            return nil
        case .paths:
            return String(localized: "auth.webdav.footerNew")
        case .credentials:
            return editingProfile == nil ? nil : String(localized: "auth.smb.login.footerEdit")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section) else {
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
        }
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
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else {
            return UITableViewCell()
        }

        if indexPath.row == 0 {
            cell.configure(
                title: String(localized: "auth.field.username"),
                text: usernameText,
                placeholder: String(localized: "auth.webdav.placeholder.username"),
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.usernameText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.password) }
        } else {
            cell.configure(
                title: String(localized: "auth.field.password"),
                text: passwordText,
                placeholder: editingProfile == nil
                    ? String(localized: "auth.webdav.placeholder.password")
                    : String(localized: "auth.passwordPlaceholderEdit"),
                isSecure: true,
                returnKeyType: .done,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.passwordText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(nil) }
        }
        return cell
    }
}

private final class WebDAVSchemeCell: UITableViewCell {
    static let reuseIdentifier = "WebDAVSchemeCell"

    private let titleLabel = UILabel()
    private let segmentedControl = UISegmentedControl(items: ["HTTP", "HTTPS"])

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
