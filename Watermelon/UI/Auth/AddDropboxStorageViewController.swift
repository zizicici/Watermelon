import SnapKit
import UIKit

final class AddDropboxStorageViewController: UIViewController {
    private enum Section: CaseIterable {
        case name
        case account
        case folder
    }

    private enum AccountRow: Int {
        case account
        case signIn

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

    private var nameText = "Dropbox"
    private var accountDisplayName: String?
    private var connectionParams: DropboxConnectionParams?
    private var credentialJSONString: String?
    private var commitGate = StorageProfileCommitGate()
    private var signInTask: Task<Void, Never>?
    private var isSigningIn = false

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.account, .folder]
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
            ? String(localized: "auth.dropbox.title")
            : String(localized: "auth.dropbox.editTitle")
        loadExistingProfile()
        configureUI()
        updateState()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        guard isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true else { return }
        cancelSignIn()
    }

    deinit {
        signInTask?.cancel()
    }

    private func loadExistingProfile() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
        accountDisplayName = editingProfile.username.isEmpty ? nil : editingProfile.username
        connectionParams = editingProfile.dropboxParams
        credentialJSONString = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef)
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
        isSigningIn = true
        updateState()
        let coordinator = dependencies.dropboxProfileSetupCoordinator
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
                    from: self,
                    forceReauthentication: forceReauthentication
                )
                try Task.checkCancellation()
                guard self.viewIfLoaded?.window != nil else { throw CancellationError() }
                self.connectionParams = draft.connectionParams
                self.credentialJSONString = draft.credentialJSONString
                self.accountDisplayName = draft.username
                self.tableView.reloadData()
            } catch is CancellationError {
                return
            } catch {
                guard let self, self.viewIfLoaded?.window != nil else { return }
                self.presentAlert(
                    title: String(localized: "auth.dropbox.signInFailed"),
                    message: UserFacingErrorLocalizer.message(for: error, storageType: .dropbox)
                )
            }
        }
    }

    private func cancelSignIn() {
        signInTask?.cancel()
        dependencies.dropboxProfileSetupCoordinator.cancelInteractiveSignIn()
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
                title: String(localized: "auth.dropbox.editTitle"),
                message: String(localized: "auth.dropbox.validation.signInRequired")
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
                message: UserFacingErrorLocalizer.message(for: error, storageType: .dropbox)
            )
        }
    }

    private func makeProfile(
        connectionParams: DropboxConnectionParams,
        baseProfile: ServerProfileRecord?
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalDropboxConnection(params: connectionParams)
        let credentialRef = StorageProfilePersistence.credentialRef(
            for: CanonicalProfileConnection.dropbox(connection).duplicateIdentity
        )
        let enteredName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        return ServerProfileRecord(
            id: baseProfile?.id,
            name: baseProfile?.name ?? (enteredName.isEmpty ? "Dropbox" : enteredName),
            storageType: StorageType.dropbox.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(connectionParams),
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: "api.dropboxapi.com",
            port: 443,
            shareName: connection.accountID,
            basePath: "/",
            username: accountDisplayName ?? baseProfile?.username ?? String(localized: "auth.dropbox.accountFallback"),
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

    private func updateState() {
        saveButton.isEnabled = connectionParams != nil
            && credentialJSONString != nil
            && !isSigningIn
            && !commitGate.isCommitting
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

extension AddDropboxStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in _: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_: UITableView, numberOfRowsInSection section: Int) -> Int {
        visibleSections[section] == .account ? AccountRow.count : 1
    }

    func tableView(_: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch visibleSections[section] {
        case .name: return String(localized: "auth.section.name")
        case .account: return String(localized: "auth.dropbox.section.account")
        case .folder: return String(localized: "auth.dropbox.section.folder")
        }
    }

    func tableView(_: UITableView, titleForFooterInSection section: Int) -> String? {
        visibleSections[section] == .folder ? String(localized: "auth.dropbox.folder.footer") : nil
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        switch visibleSections[indexPath.section] {
        case .name:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else { return UITableViewCell() }
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: "Dropbox",
                autocapitalizationType: .words,
                returnKeyType: .done
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.view.endEditing(true) }
            return cell
        case .account:
            guard let row = AccountRow(rawValue: indexPath.row) else { return UITableViewCell() }
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            switch row {
            case .account:
                content.text = accountDisplayName ?? String(localized: "auth.dropbox.account.notSignedIn")
                content.image = UIImage(systemName: "person.crop.circle")
                cell.accessoryType = .none
                cell.selectionStyle = .none
            case .signIn:
                content.text = isSigningIn
                    ? String(localized: "auth.dropbox.signIn.signingIn")
                    : (credentialJSONString == nil
                       ? String(localized: "auth.dropbox.signIn.action")
                       : String(localized: "auth.dropbox.signIn.again"))
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
                ?? String(localized: "auth.dropbox.folder.createdAfterSignIn")
            content.image = UIImage(systemName: "folder")
            cell.contentConfiguration = content
            cell.accessoryType = .none
            cell.selectionStyle = .none
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard visibleSections[indexPath.section] == .account,
              AccountRow(rawValue: indexPath.row) == .signIn else { return }
        signInTapped()
    }
}
