import SnapKit
import UIKit

final class AddOneDriveStorageViewController: UIViewController {
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

    private enum SetupState {
        case idle
        case signingIn
        case ready
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

    private var nameText = String(localized: "auth.onedrive.defaultName")
    private var accountDisplayName: String?
    private var connectionParams: OneDriveConnectionParams?
    private var credentialJSONString: String?
    private var originalCredentialJSONString: String?
    private var pendingAccountLease: PendingOneDriveAccountLease?
    private var commitGate = StorageProfileCommitGate()
    private var signInTask: Task<Void, Never>?
    private var setupState: SetupState = .idle

    private var isSigningIn: Bool {
        if case .signingIn = setupState { return true }
        return false
    }

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
            ? String(localized: "auth.onedrive.title")
            : String(localized: "auth.onedrive.editTitle")
        loadExistingProfile()
        configureUI()
        updateState()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        guard isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true else { return }
        guard let coordinator = transitionCoordinator else {
            cleanupForCompletedDeparture()
            return
        }
        let registered = coordinator.animate(alongsideTransition: nil) { [weak self] context in
            guard !context.isCancelled else { return }
            self?.cleanupForCompletedDeparture()
        }
        if !registered {
            cleanupForCompletedDeparture()
        }
    }

    deinit {
        cancelSignIn()
        discardPendingAccountLease()
    }

    private func loadExistingProfile() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
        accountDisplayName = editingProfile.username.isEmpty ? nil : editingProfile.username
        connectionParams = editingProfile.oneDriveParams
        credentialJSONString = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef)
        originalCredentialJSONString = credentialJSONString
        if connectionParams != nil, credentialJSONString != nil {
            setupState = .ready
        }
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
        setupState = .signingIn
        updateState()
        let coordinator = dependencies.oneDriveProfileSetupCoordinator
        signInTask = Task { @MainActor [weak self, coordinator] in
            var succeeded = false
            defer {
                self?.finishSignIn(succeeded: succeeded)
            }
            do {
                guard let parent = self else { throw CancellationError() }
                let draft = try await coordinator.prepare(from: parent)
                try Task.checkCancellation()
                guard parent.viewIfLoaded?.window != nil else { throw CancellationError() }
                parent.adoptSetupDraft(draft)
                succeeded = true
            } catch is CancellationError {
                return
            } catch {
                guard let self, self.viewIfLoaded?.window != nil else { return }
                self.presentAlert(
                    title: String(localized: "auth.onedrive.signInFailed"),
                    message: UserFacingErrorLocalizer.message(for: error, storageType: .onedrive)
                )
            }
        }
    }

    private func cancelSignIn() {
        signInTask?.cancel()
        OneDriveMSALService.cancelInteractiveSignIn()
    }

    private func cleanupForCompletedDeparture() {
        cancelSignIn()
        discardPendingAccountLease()
    }

    private func finishSignIn(succeeded: Bool) {
        signInTask = nil
        setupState = succeeded || (connectionParams != nil && credentialJSONString != nil) ? .ready : .idle
        updateState()
    }

    private func adoptSetupDraft(_ draft: OneDriveProfileSetupDraft) {
        if let pendingAccountLease {
            if pendingAccountLease.credential.homeAccountIdentifier
                == draft.accountLease.credential.homeAccountIdentifier {
                pendingAccountLease.relinquishToReplacement()
            } else {
                pendingAccountLease.discard()
            }
        }
        pendingAccountLease = draft.accountLease
        accountDisplayName = draft.username
        connectionParams = draft.connectionParams
        credentialJSONString = draft.credentialJSONString
        tableView.reloadData()
    }

    private func discardPendingAccountLease() {
        pendingAccountLease?.discard()
        pendingAccountLease = nil
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
            if self.connectionParams == nil || self.credentialJSONString == nil {
                presentAlert(
                    title: String(localized: "auth.onedrive.editTitle"),
                    message: String(localized: "auth.onedrive.validation.signInRequired")
                )
            }
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
            pendingAccountLease?.commit()
            pendingAccountLease = nil
            if let originalCredentialJSONString,
               originalCredentialJSONString != credentialJSONString {
                dependencies.oneDriveCredentialLifecycleService.removeCachedAccountIfUnused(
                    credentialJSONString: originalCredentialJSONString
                )
            }
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
                message: UserFacingErrorLocalizer.message(for: error, storageType: .onedrive)
            )
        }
    }

    private func makeProfile(
        connectionParams: OneDriveConnectionParams,
        baseProfile: ServerProfileRecord?
    ) throws -> ServerProfileRecord {
        let connection = try CanonicalOneDriveConnection(params: connectionParams)
        let credentialRef = StorageProfilePersistence.credentialRef(
            for: CanonicalProfileConnection.oneDrive(connection).duplicateIdentity
        )
        let encodedParams = try ServerProfileRecord.encodedConnectionParams(connectionParams)
        let enteredName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        return ServerProfileRecord(
            id: baseProfile?.id,
            name: baseProfile?.name ?? (enteredName.isEmpty ? String(localized: "auth.onedrive.defaultName") : enteredName),
            storageType: StorageType.onedrive.rawValue,
            connectionParams: encodedParams,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: "graph.microsoft.com",
            port: 443,
            shareName: connection.rootItemID,
            basePath: "/",
            username: accountDisplayName ?? baseProfile?.username ?? String(localized: "auth.onedrive.accountFallback"),
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

extension AddOneDriveStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        visibleSections[section] == .account ? AccountRow.count : 1
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch visibleSections[section] {
        case .name: return String(localized: "auth.section.name")
        case .account: return String(localized: "auth.onedrive.section.account")
        case .folder: return String(localized: "auth.onedrive.section.folder")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard visibleSections[section] == .folder else { return nil }
        return String(localized: "auth.onedrive.folder.footer")
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
                placeholder: String(localized: "auth.onedrive.defaultName"),
                autocapitalizationType: .words,
                returnKeyType: .done
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.view.endEditing(true) }
            return cell
        case .account:
            guard let row = AccountRow(rawValue: indexPath.row) else { return UITableViewCell() }
            switch row {
            case .account:
                let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = accountDisplayName ?? String(localized: "auth.onedrive.account.notSignedIn")
                content.image = UIImage(systemName: "person.crop.circle")
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.selectionStyle = .none
                return cell
            case .signIn:
                let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = isSigningIn
                    ? String(localized: "auth.onedrive.signIn.signingIn")
                    : (credentialJSONString == nil
                       ? String(localized: "auth.onedrive.signIn.action")
                       : String(localized: "auth.onedrive.signIn.again"))
                content.textProperties.color = .appTint
                content.image = UIImage(systemName: "person.badge.key")
                cell.contentConfiguration = content
                cell.accessoryType = .disclosureIndicator
                cell.selectionStyle = .default
                return cell
            }
        case .folder:
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = connectionParams?.displayRootPath ?? String(localized: "auth.onedrive.folder.createdAfterSignIn")
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
