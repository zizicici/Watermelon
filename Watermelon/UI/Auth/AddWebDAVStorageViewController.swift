import SnapKit
import UIKit

final class AddWebDAVStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case connection
        case credentials
    }

    private enum Field {
        case name
        case endpoint
        case basePath
        case username
        case password
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
    private var isSaving = false

    private var nameText = ""
    private var endpointText = ""
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
            endpointText = editingProfile.webDAVParams?.endpointURLString ?? fallbackEndpointString(for: editingProfile)
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

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    private func fallbackEndpointString(for profile: ServerProfileRecord) -> String {
        let scheme = profile.port == 443 ? "https" : "http"
        let defaultPort = scheme == "https" ? 443 : 80
        let portPart = profile.port == defaultPort ? "" : ":\(profile.port)"
        return "\(scheme)://\(profile.host)\(portPart)\(profile.shareName)"
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard !isSaving else { return }

        do {
            setSaving(true)
            let (profile, password) = try saveProfile()
            onSaved(profile, password)
            popAfterSave()
        } catch {
            setSaving(false)
            presentAlert(title: String(localized: "auth.saveFailed"), message: error.localizedDescription)
        }
    }

    private func saveProfile() throws -> (ServerProfileRecord, String) {
        let endpointURL = try parseEndpointURL(endpointText.trimmingCharacters(in: .whitespacesAndNewlines))
        let endpointURLString = Self.normalizedEndpointURLString(endpointURL)

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

        let rawBasePath = basePathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedBasePath = RemotePathBuilder.normalizePath(rawBasePath.isEmpty ? "/Watermelon" : rawBasePath)

        let existing = try findExistingProfile(
            endpointURLString: endpointURLString,
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
        let profileName = finalName.isEmpty ? (endpointURL.host ?? "WebDAV") : finalName
        let endpointPath = endpointURL.path.isEmpty ? "/" : endpointURL.path
        let credentialRef = "webdav|\(endpointURLString)|\(username)"
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(endpointURLString: endpointURLString)
        )

        var profile = ServerProfileRecord(
            id: baseProfile?.id,
            name: profileName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: connectionParams,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: endpointURL.host ?? "",
            port: endpointURL.port ?? defaultPort(for: endpointURL),
            shareName: endpointPath,
            basePath: normalizedBasePath,
            username: username,
            domain: nil,
            credentialRef: credentialRef,
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? true,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: password, account: credentialRef)
        if let oldRef = editingProfile?.credentialRef,
           oldRef != credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return (profile, password)
    }

    private func parseEndpointURL(_ input: String) throws -> URL {
        guard !input.isEmpty else {
            throw NSError(domain: "AddWebDAVStorage", code: 4, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationEndpoint")])
        }

        let normalizedInput = input.contains("://") ? input : "https://\(input)"
        guard let url = URL(string: normalizedInput),
              let scheme = url.scheme?.lowercased(),
              (scheme == "http" || scheme == "https"),
              url.host != nil else {
            throw NSError(domain: "AddWebDAVStorage", code: 5, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.invalidEndpoint")])
        }
        return url
    }

    private static func normalizedEndpointURLString(_ url: URL) -> String {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }
        components.query = nil
        components.fragment = nil
        components.user = nil
        components.password = nil
        if components.path.isEmpty {
            components.path = "/"
        } else if components.path.count > 1, components.path.hasSuffix("/") {
            components.path.removeLast()
        }
        return (components.url ?? url).absoluteString
    }

    private func defaultPort(for endpointURL: URL) -> Int {
        endpointURL.scheme?.lowercased() == "https" ? 443 : 80
    }

    private func findExistingProfile(endpointURLString: String, basePath: String, username: String) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .webdav &&
                profile.webDAVParams?.endpointURLString == endpointURLString &&
                RemotePathBuilder.normalizePath(profile.basePath) == RemotePathBuilder.normalizePath(basePath) &&
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
        case .endpoint:
            return IndexPath(row: 0, section: Section.connection.rawValue)
        case .basePath:
            return IndexPath(row: 1, section: Section.connection.rawValue)
        case .username:
            return IndexPath(row: 0, section: Section.credentials.rawValue)
        case .password:
            return IndexPath(row: 1, section: Section.credentials.rawValue)
        }
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
        case .connection:
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
        case .connection:
            return String(localized: "auth.section.connection")
        case .credentials:
            return String(localized: "auth.section.auth")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .connection:
            return String(localized: "auth.webdav.footerNew")
        case .credentials:
            return editingProfile == nil ? nil : String(localized: "auth.smb.login.footerEdit")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section),
              let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
              ) as? SettingsTextFieldCell else {
            return UITableViewCell()
        }

        switch section {
        case .name:
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: "WebDAV",
                autocapitalizationType: .words,
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.endpoint) }
        case .connection:
            if indexPath.row == 0 {
                cell.configure(
                    title: String(localized: "auth.webdav.fieldEndpoint"),
                    text: endpointText,
                    placeholder: "https://example.com/dav",
                    keyboardType: .URL,
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.endpointText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.basePath) }
            } else {
                cell.configure(
                    title: String(localized: "auth.webdav.fieldBasePath"),
                    text: basePathText,
                    placeholder: "/Watermelon",
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.basePathText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.username) }
            }
        case .credentials:
            if indexPath.row == 0 {
                cell.configure(
                    title: "Username",
                    text: usernameText,
                    placeholder: "user",
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.usernameText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.password) }
            } else {
                cell.configure(
                    title: "Password",
                    text: passwordText,
                    placeholder: editingProfile == nil ? "password" : String(localized: "auth.passwordPlaceholderEdit"),
                    isSecure: true,
                    returnKeyType: .done,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.passwordText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(nil) }
            }
        }

        return cell
    }
}
