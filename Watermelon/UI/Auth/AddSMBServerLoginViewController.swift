import SnapKit
import UIKit

final class AddSMBServerLoginViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case server
        case credentials
        case share
        case folder
    }

    private enum Field {
        case name
        case host
        case port
        case username
        case password
        case domain
    }

    private let dependencies: DependencyContainer
    private let draft: SMBServerLoginDraft
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let setupService = SMBSetupService()
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
    private var isLoading = false
    private var operationTask: Task<Void, Never>?

    private var nameText = ""
    private var hostText = ""
    private var portText = "445"
    private var usernameText = ""
    private var passwordText = ""
    private var domainText = ""
    private let hasSavedPassword: Bool
    private var passwordChanged = false
    private var passwordRevealed = false
    private var revealedSavedPassword: String?
    private var selectedShareName: String?
    private var selectedBasePath = "/"

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.server, .credentials, .share, .folder]
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
        draft: SMBServerLoginDraft,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.draft = draft
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
        title = editingProfile == nil ? String(localized: "auth.smb.login.title") : String(localized: "auth.smb.login.editTitle")

        fillDraft()
        configureUI()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            operationTask?.cancel()
        }
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            setOperationNavigationLocked(false)
        }
    }

    private func fillDraft() {
        nameText = draft.name
        hostText = RemoteHostIdentity.canonicalSMB(draft.host)
        portText = String(draft.port)
        usernameText = draft.username
        domainText = draft.domain ?? ""
        selectedShareName = editingProfile?.shareName
        selectedBasePath = RemotePathBuilder.normalizePath(editingProfile?.basePath ?? "/")
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
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "SelectionCell")

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.top.equalToSuperview()
            make.leading.trailing.bottom.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard !isLoading else { return }
        guard !rejectIfProfileMutationBlocked() else { return }
        guard let shareName = selectedShareName else {
            presentAlert(
                title: String(localized: "auth.smb.share.noShareSelected"),
                message: String(localized: "auth.smb.share.selectShareFirst")
            )
            return
        }
        let auth: SMBServerAuthContext
        do {
            auth = try buildAuthContext()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
            )
            return
        }
        let normalizedPath = RemotePathBuilder.normalizePath(selectedBasePath)
        let context = SMBServerPathContext(
            auth: auth,
            shareName: shareName,
            basePath: normalizedPath
        )
        do {
            try SMBProfileSaver.ensureNoDuplicate(
                dependencies: dependencies,
                context: context,
                editingProfile: editingProfile
            )
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
            )
            return
        }

        setLoading(true)
        let runtimeFlags = dependencies.appRuntimeFlags
        let editingProfileID = editingProfile?.id
        operationTask = Task { [weak self] in
            do {
                guard let result = try await runtimeFlags.withAsyncProfileMutationLease(
                    profileID: editingProfileID,
                    {
                        let client = try AMSMB2Client(config: SMBServerConfig(
                            host: auth.host,
                            port: auth.port,
                            shareName: shareName,
                            basePath: normalizedPath,
                            username: auth.username,
                            password: auth.password,
                            domain: auth.domain
                        ))
                        try await RemoteStorageWriteVerifier.verify(
                            client: client,
                            basePath: normalizedPath
                        )
                        try Task.checkCancellation()

                        return try await MainActor.run { [weak self] in
                            guard let self, !Task.isCancelled else { throw CancellationError() }
                            guard let result = try self.dependencies.appRuntimeFlags.withProfileMutationLease(
                                profileID: self.editingProfile?.id,
                                {
                                    let result = try SMBProfileSaver.save(
                                        dependencies: self.dependencies,
                                        context: context,
                                        editingProfile: self.editingProfile,
                                        name: self.nameText
                                    )
                                    if self.editingProfile != nil {
                                        self.onSaved(result.0, result.1)
                                    }
                                    return result
                                }
                            ) else {
                                throw RemoteStorageClientError.unavailable
                            }
                            return result
                        }
                    }
                ) else {
                    await MainActor.run { [weak self] in
                        guard let self else { return }
                        self.endOperation()
                        self.presentMutationBlockedAlert()
                    }
                    return
                }
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    self.endOperation()
                    if self.editingProfile == nil {
                        self.onSaved(result.0, result.1)
                    }
                    self.popAfterSave()
                }
            } catch is CancellationError {
                await MainActor.run { [weak self] in self?.endOperation() }
            } catch {
                if Task.isCancelled {
                    await MainActor.run { [weak self] in self?.endOperation() }
                    return
                }
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    self.endOperation()
                    self.presentAlert(
                        title: String(localized: "auth.saveFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
                    )
                }
            }
        }
    }

    private func presentShareSelection() {
        dismissKeyboard()
        guard !isLoading else { return }

        let auth: SMBServerAuthContext
        do {
            auth = try buildAuthContext()
        } catch {
            presentAlert(
                title: String(localized: "auth.smb.login.loginFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
            )
            return
        }

        setLoading(true)
        let setupService = setupService
        operationTask = Task { [weak self] in
            do {
                let shares = try await setupService.listShares(auth: auth)
                try Task.checkCancellation()
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    guard !Task.isCancelled else {
                        self.endOperation()
                        return
                    }
                    self.endOperation()
                    guard !shares.isEmpty else {
                        self.presentAlert(
                            title: String(localized: "auth.smb.login.noShares"),
                            message: String(localized: "auth.smb.login.noSharesMessage")
                        )
                        return
                    }
                    let picker = SMBShareSelectionViewController(
                        shares: shares,
                        selectedShareName: self.selectedShareName
                    ) { [weak self] shareName in
                        guard let self else { return }
                        if self.selectedShareName != shareName {
                            self.selectedBasePath = "/"
                        }
                        self.selectedShareName = shareName
                        self.reloadSelectionSections()
                    }
                    self.presentSelectionController(picker)
                }
            } catch is CancellationError {
                await MainActor.run { [weak self] in self?.endOperation() }
            } catch {
                if Task.isCancelled {
                    await MainActor.run { [weak self] in self?.endOperation() }
                    return
                }
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    self.endOperation()
                    self.presentAlert(
                        title: String(localized: "auth.smb.login.loginFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
                    )
                }
            }
        }
    }

    private func presentFolderSelection() {
        dismissKeyboard()
        guard !isLoading else { return }
        guard let shareName = selectedShareName else {
            presentAlert(
                title: String(localized: "auth.smb.share.noShareSelected"),
                message: String(localized: "auth.smb.share.selectShareFirst")
            )
            return
        }
        do {
            let auth = try buildAuthContext()
            let picker = SMBFolderSelectionViewController(
                auth: auth,
                shareName: shareName,
                initialPath: selectedBasePath
            ) { [weak self] path in
                self?.selectedBasePath = RemotePathBuilder.normalizePath(path)
                self?.reloadSelectionSections()
            }
            presentSelectionController(picker)
        } catch {
            presentAlert(
                title: String(localized: "auth.smb.login.loginFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .smb)
            )
        }
    }

    private func presentSelectionController(_ viewController: UIViewController) {
        let navigationController = UINavigationController(rootViewController: viewController)
        if let sheet = navigationController.sheetPresentationController {
            sheet.detents = [.medium(), .large()]
            sheet.prefersGrabberVisible = true
        }
        present(navigationController, animated: true)
    }

    private func reloadSelectionSections() {
        let sections = IndexSet([
            sectionIndex(for: .share),
            sectionIndex(for: .folder)
        ])
        tableView.reloadSections(sections, with: .none)
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if editingProfile == nil,
           !shouldPopToRootOnSave,
           navigationController.presentingViewController != nil {
            return
        }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func buildAuthContext() throws -> SMBServerAuthContext {
        let host = RemoteHostIdentity.canonicalSMB(
            hostText.trimmingCharacters(in: .whitespacesAndNewlines)
        )
        let username = usernameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let domain = domainText.trimmingCharacters(in: .whitespacesAndNewlines)
        let name = editingProfile?.name ?? nameText.trimmingCharacters(in: .whitespacesAndNewlines)

        let password: String
        if hasSavedPassword, !passwordChanged, let editingProfile {
            password = try dependencies.keychainService.readPassword(account: editingProfile.credentialRef)
        } else {
            // Remote credentials may intentionally start or end with whitespace.
            password = passwordText
        }

        guard !host.isEmpty, !username.isEmpty else {
            throw NSError(domain: "AddSMBServerLogin", code: 1, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.smb.login.validation")])
        }

        let trimmedPort = portText.trimmingCharacters(in: .whitespacesAndNewlines)
        let port: Int
        if trimmedPort.isEmpty {
            port = 445
        } else {
            guard let parsed = Int(trimmedPort), (1 ... 65535).contains(parsed) else {
                throw NSError(
                    domain: "AddSMBServerLogin",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.webdav.validationPort")]
                )
            }
            port = parsed
        }

        return SMBServerAuthContext(
            name: name.isEmpty ? host : name,
            host: host,
            port: port,
            username: username,
            password: password,
            domain: domain.isEmpty ? nil : domain
        )
    }

    private static func hasSavedPassword(dependencies: DependencyContainer, editingProfile: ServerProfileRecord?) -> Bool {
        guard let editingProfile else { return false }
        return (try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef)) != nil
    }

    @MainActor
    private func setLoading(_ loading: Bool) {
        isLoading = loading
        setOperationNavigationLocked(loading)
        tableView.isUserInteractionEnabled = !loading
        if loading {
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

    @MainActor
    private func endOperation() {
        operationTask = nil
        setLoading(false)
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
            return IndexPath(row: 0, section: sectionIndex(for: .server))
        case .port:
            return IndexPath(row: 1, section: sectionIndex(for: .server))
        case .username:
            return IndexPath(row: 0, section: sectionIndex(for: .credentials))
        case .password:
            return IndexPath(row: 1, section: sectionIndex(for: .credentials))
        case .domain:
            return IndexPath(row: 2, section: sectionIndex(for: .credentials))
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
}

extension AddSMBServerLoginViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = resolvedSection(at: section) else { return 0 }
        switch section {
        case .name:
            return 1
        case .server:
            return 2
        case .credentials:
            return 3
        case .share, .folder:
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
        case .credentials:
            return String(localized: "auth.section.auth")
        case .share:
            return String(localized: "auth.smb.share.title")
        case .folder:
            return String(localized: "auth.smb.folder.sectionTitle")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .server:
            return String(localized: "auth.smb.login.footerServer")
        case .credentials:
            return editingProfile == nil ? nil : String(localized: "auth.smb.login.footerEdit")
        case .share:
            return nil
        case .folder:
            return editingProfile == nil ? String(localized: "auth.smb.save.footer") : nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(at: indexPath.section) else {
            return UITableViewCell()
        }

        func makeTextCell() -> SettingsTextFieldCell? {
            tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell
        }

        switch section {
        case .name:
            guard let cell = makeTextCell() else { return UITableViewCell() }
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: String(localized: "auth.smb.login.placeholder.name"),
                autocapitalizationType: .words,
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.host) }
            return cell
        case .server:
            guard let cell = makeTextCell() else { return UITableViewCell() }
            if indexPath.row == 0 {
                cell.configure(
                    title: String(localized: "auth.field.host"),
                    text: hostText,
                    placeholder: String(localized: "auth.smb.login.placeholder.host"),
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.hostText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.port) }
            } else {
                cell.configure(
                    title: String(localized: "auth.field.port"),
                    text: portText,
                    placeholder: String(localized: "auth.smb.login.placeholder.port"),
                    keyboardType: .numberPad,
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.portText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.username) }
            }
            return cell
        case .credentials:
            switch indexPath.row {
            case 0:
                guard let cell = makeTextCell() else { return UITableViewCell() }
                cell.configure(
                    title: String(localized: "auth.field.username"),
                    text: usernameText,
                    placeholder: String(localized: "auth.smb.login.placeholder.username"),
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.usernameText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.password) }
                return cell
            case 1:
                guard let cell = tableView.dequeueReusableCell(
                    withIdentifier: CredentialTextFieldCell.reuseIdentifier,
                    for: indexPath
                ) as? CredentialTextFieldCell else { return UITableViewCell() }
                cell.configure(
                    title: String(localized: "auth.field.password"),
                    text: passwordDisplayText(),
                    placeholder: String(localized: "auth.smb.login.placeholder.password"),
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
                cell.onReturn = { [weak self] in self?.focusField(.domain) }
                return cell
            default:
                guard let cell = makeTextCell() else { return UITableViewCell() }
                cell.configure(
                    title: String(localized: "auth.field.domain"),
                    text: domainText,
                    placeholder: String(localized: "auth.smb.login.placeholder.domain"),
                    returnKeyType: .done,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.domainText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(nil) }
                return cell
            }
        case .share:
            let cell = tableView.dequeueReusableCell(withIdentifier: "SelectionCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = selectedShareName ?? String(localized: "auth.smb.share.noShareSelected")
            content.textProperties.color = selectedShareName == nil ? .secondaryLabel : .label
            cell.contentConfiguration = content
            cell.selectionStyle = .default
            cell.accessoryType = .disclosureIndicator
            cell.accessoryView = nil
            return cell
        case .folder:
            let cell = tableView.dequeueReusableCell(withIdentifier: "SelectionCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = selectedBasePath
            content.textProperties.color = .label
            cell.contentConfiguration = content
            cell.selectionStyle = .default
            cell.accessoryType = .disclosureIndicator
            cell.accessoryView = nil
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = resolvedSection(at: indexPath.section) else { return }
        switch section {
        case .share:
            presentShareSelection()
        case .folder:
            presentFolderSelection()
        case .name, .server, .credentials:
            return
        }
    }
}
