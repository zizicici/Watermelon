import SnapKit
import SafariServices
import UIKit

final class AddGoogleDriveStorageViewController: UIViewController {
    private enum Section: CaseIterable {
        case name
        case clientID
        case account
        case folder
    }

    private enum AccountRow: Int {
        case account
        case signIn

        static let count = 2
    }

    private enum ClientIDRow: Int {
        case input
        case setupGuide

        static let count = 2
    }

    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var saveButton = UIBarButtonItem(
        title: String(localized: "common.save"),
        style: .prominentStyle,
        target: self,
        action: #selector(saveTapped)
    )
    private let activityIndicator = UIActivityIndicatorView(style: .medium)
    private lazy var activityButton = UIBarButtonItem(customView: activityIndicator)

    private var nameText = "Google Drive"
    private var clientIDText = ""
    private var accountDisplayName: String?
    private var connectionParams: GoogleDriveConnectionParams?
    private var credentialJSONString: String?
    private var commitGate = StorageProfileCommitGate()
    private var signInTask: Task<Void, Never>?
    private var isSigningIn = false

    private static var setupGuideURL: URL {
        let identifier = Bundle.main.preferredLocalizations.first
            ?? Locale.preferredLanguages.first
            ?? "en"
        let language = setupGuideLanguage(for: identifier)
        let prefix = language == "en" ? "" : "\(language)/"
        return URL(
            string: "https://watermelonbackup.com/\(prefix)kb/google-drive-cloud-project-oauth-client/"
        )!
    }

    private static func setupGuideLanguage(for identifier: String) -> String {
        let value = identifier.replacingOccurrences(of: "_", with: "-").lowercased()
        if value == "es-419" || value.hasPrefix("es-419-") { return "es-419" }
        if value.hasPrefix("pt-br") { return "pt-BR" }
        if value.hasPrefix("pt-pt") { return "pt-PT" }
        if value == "zh-hk" || value.hasPrefix("zh-hant")
            || value.hasPrefix("zh-tw") || value.hasPrefix("zh-mo") {
            return "zh-Hant"
        }
        if value.hasPrefix("zh-hans") || value.hasPrefix("zh-cn") || value.hasPrefix("zh-sg") {
            return "zh-Hans"
        }
        let base = value.split(separator: "-").first.map(String.init) ?? "en"
        return ["de", "es", "fr", "ja", "ko", "ru", "uk"].contains(base) ? base : "en"
    }

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.clientID, .account, .folder]
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
            ? String(localized: "auth.googleDrive.title")
            : String(localized: "auth.googleDrive.editTitle")
        loadExistingProfile()
        configureUI()
        updateState()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        guard isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true else { return }
        guard let coordinator = transitionCoordinator else {
            cancelSignIn()
            return
        }
        let registered = coordinator.animate(alongsideTransition: nil) { [weak self] context in
            guard !context.isCancelled else { return }
            self?.cancelSignIn()
        }
        if !registered { cancelSignIn() }
    }

    deinit {
        signInTask?.cancel()
    }

    private func loadExistingProfile() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
        accountDisplayName = editingProfile.username.isEmpty ? nil : editingProfile.username
        connectionParams = editingProfile.googleDriveParams
        clientIDText = connectionParams?.clientID ?? ""
        guard let connectionParams,
              let rawCredential = try? dependencies.keychainService.readPassword(
                account: editingProfile.credentialRef
              ),
              let credential = try? GoogleDriveCredentialBlob.decode(from: rawCredential),
              credential.accountSubject == connectionParams.accountSubject else {
            credentialJSONString = nil
            return
        }
        credentialJSONString = rawCredential
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = saveButton
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.keyboardDismissMode = .interactive
        tableView.register(SettingsTextFieldCell.self, forCellReuseIdentifier: SettingsTextFieldCell.reuseIdentifier)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "ValueCell")
        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func signInTapped() {
        guard !isSigningIn, !commitGate.isCommitting, !rejectIfProfileMutationBlocked() else { return }
        let clientID = clientIDText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard GoogleDriveOAuthClientConfiguration.isValidClientID(clientID) else {
            presentAlert(
                title: String(localized: "auth.googleDrive.signInFailed"),
                message: String(localized: "googledrive.error.auth.invalidClientID")
            )
            return
        }
        isSigningIn = true
        updateState()
        let coordinator = dependencies.googleDriveProfileSetupCoordinator
        let forceReauthentication = credentialJSONString != nil
        signInTask = Task { @MainActor [weak self, coordinator] in
            defer {
                self?.isSigningIn = false
                self?.signInTask = nil
                self?.updateState()
            }
            do {
                guard let self else { throw CancellationError() }
                let draft = try await coordinator.prepare(
                    clientID: clientID,
                    from: self,
                    forceReauthentication: forceReauthentication
                )
                try Task.checkCancellation()
                guard self.viewIfLoaded?.window != nil else { throw CancellationError() }
                self.clientIDText = draft.connectionParams.clientID
                self.connectionParams = draft.connectionParams
                self.credentialJSONString = draft.credentialJSONString
                self.accountDisplayName = draft.username
                if draft.refreshTokenExpiresAt != nil {
                    self.presentAlert(
                        title: String(localized: "auth.googleDrive.authorizationLimited.title"),
                        message: String(localized: "auth.googleDrive.authorizationLimited.message")
                    )
                }
            } catch is CancellationError {
                return
            } catch {
                guard let self, self.viewIfLoaded?.window != nil else { return }
                self.presentAlert(
                    title: String(localized: "auth.googleDrive.signInFailed"),
                    message: UserFacingErrorLocalizer.message(for: error, storageType: .googleDrive)
                )
            }
        }
    }

    private func cancelSignIn() {
        signInTask?.cancel()
        dependencies.googleDriveProfileSetupCoordinator.cancelInteractiveSignIn()
    }

    private func updateClientID(_ text: String) {
        clientIDText = text
        let normalized = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if connectionParams?.clientID != normalized {
            connectionParams = nil
            credentialJSONString = nil
            accountDisplayName = nil
        }
        saveButton.isEnabled = canSave
    }

    @objc
    private func saveTapped() {
        view.endEditing(true)
        guard !isSigningIn, commitGate.begin() else { return }
        updateState()
        var didCommit = false
        defer {
            if !didCommit {
                commitGate.releaseAfterFailure()
                updateState()
            }
        }
        guard !rejectIfProfileMutationBlocked(),
              let connectionParams,
              let credentialJSONString else {
            presentAlert(
                title: String(localized: "auth.googleDrive.editTitle"),
                message: String(localized: "auth.googleDrive.validation.signInRequired")
            )
            return
        }
        do {
            guard let profile = try dependencies.storageProfileMutationService.saveRemoteProfile(
                editingProfile: editingProfile,
                credential: credentialJSONString,
                makeProfile: { liveProfile in
                    try self.makeProfile(connectionParams: connectionParams, baseProfile: liveProfile)
                }
            ) else {
                presentMutationBlockedAlert()
                return
            }
            didCommit = true
            if editingProfile == nil {
                let callback = onSaved
                StorageProfileSaveTransition.completeCreate(
                    from: self,
                    shouldPopToRoot: shouldPopToRootOnSave
                ) {
                    callback(profile, credentialJSONString)
                }
            } else {
                onSaved(profile, credentialJSONString)
                popAfterSave()
            }
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .googleDrive)
            )
        }
    }

    private func makeProfile(
        connectionParams: GoogleDriveConnectionParams,
        baseProfile: ServerProfileRecord?
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalGoogleDriveConnection(params: connectionParams)
        let credentialRef = StorageProfilePersistence.credentialRef(
            for: CanonicalProfileConnection.googleDrive(connection).duplicateIdentity
        )
        let enteredName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        return ServerProfileRecord(
            id: baseProfile?.id,
            name: baseProfile?.name ?? (enteredName.isEmpty ? "Google Drive" : enteredName),
            storageType: StorageType.googleDrive.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(connectionParams),
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: "www.googleapis.com",
            port: 443,
            shareName: connection.rootFolderID,
            basePath: "/",
            username: accountDisplayName ?? baseProfile?.username ?? String(localized: "auth.googleDrive.accountFallback"),
            domain: nil,
            credentialRef: credentialRef,
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? false,
            backgroundBackupMinIntervalMinutes: baseProfile?.backgroundBackupMinIntervalMinutes
                ?? BackgroundBackupInterval.default.minutes,
            backgroundBackupRequiresWiFi: baseProfile?.backgroundBackupRequiresWiFi ?? true,
            generateRemoteThumbnails: baseProfile?.generateRemoteThumbnails ?? false,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date(),
            writerID: baseProfile?.writerID
        )
    }

    private var canSave: Bool {
        connectionParams != nil && credentialJSONString != nil && !isSigningIn && !commitGate.isCommitting
    }

    private func updateState() {
        saveButton.isEnabled = canSave
        tableView.isUserInteractionEnabled = !isSigningIn && !commitGate.isCommitting
        if isSigningIn {
            activityIndicator.startAnimating()
            navigationItem.rightBarButtonItem = activityButton
        } else {
            activityIndicator.stopAnimating()
            navigationItem.rightBarButtonItem = saveButton
        }
        if tableView.window != nil { tableView.reloadData() }
    }

    private func rejectIfProfileMutationBlocked() -> Bool {
        let blocked = dependencies.appRuntimeFlags.isExecuting
            || dependencies.remoteMaintenanceController.isBusy
            || dependencies.appRuntimeFlags.isConnecting(profileID: editingProfile?.id)
        if blocked { presentMutationBlockedAlert() }
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

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
        } else {
            navigationController.popViewController(animated: true)
        }
    }
}

extension AddGoogleDriveStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in _: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_: UITableView, numberOfRowsInSection section: Int) -> Int {
        switch visibleSections[section] {
        case .clientID: return ClientIDRow.count
        case .account: return AccountRow.count
        case .name, .folder: return 1
        }
    }

    func tableView(_: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch visibleSections[section] {
        case .name: return String(localized: "auth.section.name")
        case .clientID: return String(localized: "auth.googleDrive.section.clientID")
        case .account: return String(localized: "auth.googleDrive.section.account")
        case .folder: return String(localized: "auth.googleDrive.section.folder")
        }
    }

    func tableView(_: UITableView, titleForFooterInSection section: Int) -> String? {
        switch visibleSections[section] {
        case .folder: return String(localized: "auth.googleDrive.folder.footer")
        case .name, .clientID, .account: return nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        switch visibleSections[indexPath.section] {
        case .name:
            return textFieldCell(
                tableView,
                indexPath: indexPath,
                text: nameText,
                placeholder: "Google Drive",
                autocapitalizationType: .words,
                onChange: { [weak self] in self?.nameText = $0 }
            )
        case .clientID:
            guard let row = ClientIDRow(rawValue: indexPath.row) else { return UITableViewCell() }
            switch row {
            case .input:
                return textFieldCell(
                    tableView,
                    indexPath: indexPath,
                    text: clientIDText,
                    placeholder: "000000000000-xxxx.apps.googleusercontent.com",
                    autocapitalizationType: .none,
                    onChange: { [weak self] in self?.updateClientID($0) }
                )
            case .setupGuide:
                let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "auth.googleDrive.clientID.setupGuide")
                content.textProperties.color = .appTint
                content.image = UIImage(systemName: "questionmark.circle")
                cell.contentConfiguration = content
                cell.accessoryType = .disclosureIndicator
                cell.selectionStyle = .default
                return cell
            }
        case .account:
            guard let row = AccountRow(rawValue: indexPath.row) else { return UITableViewCell() }
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            switch row {
            case .account:
                content.text = accountDisplayName ?? String(localized: "auth.googleDrive.account.notSignedIn")
                content.image = UIImage(systemName: "person.crop.circle")
                cell.accessoryType = .none
                cell.selectionStyle = .none
            case .signIn:
                content.text = isSigningIn
                    ? String(localized: "auth.googleDrive.signIn.signingIn")
                    : (credentialJSONString == nil
                       ? String(localized: "auth.googleDrive.signIn.action")
                       : String(localized: "auth.googleDrive.signIn.again"))
                content.textProperties.color = .appTint
                content.image = UIImage(systemName: "person.badge.key")
                cell.accessoryType = .disclosureIndicator
                cell.selectionStyle = .default
            }
            cell.contentConfiguration = content
            return cell
        case .folder:
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = connectionParams?.displayRootPath
                ?? String(localized: "auth.googleDrive.folder.createdAfterSignIn")
            content.image = UIImage(systemName: "folder")
            cell.contentConfiguration = content
            cell.accessoryType = .none
            cell.selectionStyle = .none
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        switch visibleSections[indexPath.section] {
        case .clientID:
            guard ClientIDRow(rawValue: indexPath.row) == .setupGuide else { return }
            view.endEditing(true)
            present(SFSafariViewController(url: Self.setupGuideURL), animated: true)
        case .account:
            guard AccountRow(rawValue: indexPath.row) == .signIn else { return }
            signInTapped()
        case .name, .folder:
            return
        }
    }

    private func textFieldCell(
        _ tableView: UITableView,
        indexPath: IndexPath,
        text: String,
        placeholder: String,
        autocapitalizationType: UITextAutocapitalizationType,
        onChange: @escaping (String) -> Void
    ) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else { return UITableViewCell() }
        cell.configure(
            title: nil,
            text: text,
            placeholder: placeholder,
            autocapitalizationType: autocapitalizationType,
            returnKeyType: .done
        )
        cell.onTextChanged = onChange
        cell.onReturn = { [weak self] in self?.view.endEditing(true) }
        return cell
    }
}
