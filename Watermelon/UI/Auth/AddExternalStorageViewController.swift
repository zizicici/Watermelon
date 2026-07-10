import SnapKit
import UniformTypeIdentifiers
import UIKit

final class AddExternalStorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case location
    }

    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void
    private let bookmarkStore = SecurityScopedBookmarkStore()

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var saveBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.save"),
        style: .prominentStyle,
        target: self,
        action: #selector(saveTapped)
    )
    private let loadingIndicatorView = UIActivityIndicatorView(style: .medium)
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

    private var nameText = ""
    private var selectedDirectoryURL: URL?
    private var didAutoPresentDirectoryPicker = false

    private var visibleSections: [Section] {
        editingProfile == nil ? Section.allCases : [.location]
    }

    private func resolvedSection(at index: Int) -> Section? {
        guard visibleSections.indices.contains(index) else { return nil }
        return visibleSections[index]
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
        title = editingProfile == nil ? String(localized: "auth.external.title") : String(localized: "auth.external.editTitle")

        fillInitialValues()
        configureUI()
        updateSaveButtonState()
        registerKeyboardNotifications()
        scheduleInitialDirectoryPickerPresentation()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            saveTask?.cancel()
        }
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        if isMovingFromParent || isBeingDismissed || navigationController?.isBeingDismissed == true {
            setSaving(false)
        }
    }

    private func fillInitialValues() {
        guard let editingProfile else { return }
        nameText = editingProfile.name
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
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "ValueCell")

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    private func updateSaveButtonState() {
        saveBarButtonItem.isEnabled = selectedDirectoryURL != nil || !(editingProfile?.externalVolumeParams?.displayPath ?? "").isEmpty
    }

    private func currentDisplayPath() -> String? {
        if let selectedDirectoryURL {
            return selectedDirectoryURL.path
        }
        return editingProfile?.externalVolumeParams?.displayPath
    }

    @objc
    private func pickDirectoryTapped() {
        presentDirectoryPicker(animated: true)
    }

    private func scheduleInitialDirectoryPickerPresentation() {
        guard editingProfile == nil, !didAutoPresentDirectoryPicker else { return }
        didAutoPresentDirectoryPicker = true
        DispatchQueue.main.async { [weak self] in
            self?.presentInitialDirectoryPickerWhenReady(remainingAttempts: 30)
        }
    }

    private func presentInitialDirectoryPickerWhenReady(remainingAttempts: Int) {
        guard selectedDirectoryURL == nil, presentedViewController == nil else { return }
        guard view.window != nil else {
            guard remainingAttempts > 0 else { return }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.03) { [weak self] in
                self?.presentInitialDirectoryPickerWhenReady(remainingAttempts: remainingAttempts - 1)
            }
            return
        }

        let presentPicker = { [weak self] in
            guard let self,
                  self.selectedDirectoryURL == nil,
                  self.presentedViewController == nil,
                  self.view.window != nil else { return }
            self.presentDirectoryPicker(animated: true)
        }
        if let coordinator = transitionCoordinator {
            let registered = coordinator.animate(alongsideTransition: nil) { _ in presentPicker() }
            if !registered {
                presentPicker()
            }
        } else {
            presentPicker()
        }
    }

    private func presentDirectoryPicker(animated: Bool) {
        guard presentedViewController == nil else { return }
        dismissKeyboard()
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [.folder], asCopy: false)
        picker.delegate = self
        picker.allowsMultipleSelection = false
        present(picker, animated: animated)
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard saveTask == nil else { return }
        guard !rejectIfProfileMutationBlocked() else { return }
        do {
            let initialDisplayPath: String
            let initialBookmarkData: Data
            if let selectedDirectoryURL {
                let scoped = selectedDirectoryURL.startAccessingSecurityScopedResource()
                defer {
                    if scoped {
                        selectedDirectoryURL.stopAccessingSecurityScopedResource()
                    }
                }
                let bookmarkData = try bookmarkStore.makeBookmarkData(for: selectedDirectoryURL)
                initialDisplayPath = selectedDirectoryURL.path
                initialBookmarkData = bookmarkData
            } else if let editingProfile,
                      let existingPath = editingProfile.externalVolumeParams?.displayPath,
                      let externalParams = editingProfile.externalVolumeParams {
                initialDisplayPath = existingPath
                initialBookmarkData = externalParams.rootBookmarkData
            } else {
                presentAlert(title: String(localized: "auth.external.noDirSelected"), message: String(localized: "auth.external.noDirMessage"))
                return
            }
            if let duplicate = try findExistingProfile(displayPath: initialDisplayPath),
               editingProfile == nil || duplicate.id != editingProfile?.id {
                throw NSError(
                    domain: "AddExternalStorage",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.external.duplicateDir")]
                )
            }

            let baseProfile = editingProfile
            let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
            let credentialRef = baseProfile?.credentialRef ?? "external:\(UUID().uuidString)"
            let shareName = baseProfile?.shareName ?? "external-\(UUID().uuidString)"
            let refreshCapture = ExternalBookmarkRefreshCapture()
            setSaving(true)
            let runtimeFlags = dependencies.appRuntimeFlags
            let editingProfileID = editingProfile?.id
            saveTask = Task { [weak self] in
                do {
                    guard let savedProfile = try await runtimeFlags.withAsyncProfileMutationLease(
                        profileID: editingProfileID,
                        {
                            let client = LocalVolumeClient(config: .init(
                                rootBookmarkData: initialBookmarkData,
                                onBookmarkRefreshed: { payload in
                                    refreshCapture.record(payload)
                                }
                            ))
                            try await RemoteStorageWriteVerifier.verify(
                                client: client,
                                basePath: "/",
                                timeout: RemoteStorageWriteVerifier.externalVolumeTimeout
                            )
                            try Task.checkCancellation()

                            let refreshed = refreshCapture.snapshot()
                            let finalBookmarkData = refreshed?.bookmarkData ?? initialBookmarkData
                            let finalDisplayPath = refreshed.flatMap {
                                $0.displayPath.isEmpty ? nil : $0.displayPath
                            } ?? initialDisplayPath
                            let finalParams = ExternalVolumeConnectionParams(
                                rootBookmarkData: finalBookmarkData,
                                displayPath: finalDisplayPath
                            )
                            let encodedParams = try ServerProfileRecord.encodedConnectionParams(finalParams)
                            let profileName = baseProfile?.name
                                ?? (finalName.isEmpty ? URL(fileURLWithPath: finalDisplayPath).lastPathComponent : finalName)
                            let profile = ServerProfileRecord(
                                id: baseProfile?.id,
                                name: profileName,
                                storageType: StorageType.externalVolume.rawValue,
                                connectionParams: encodedParams,
                                sortOrder: baseProfile?.sortOrder ?? 0,
                                host: "external",
                                port: 0,
                                shareName: shareName,
                                basePath: "/",
                                username: "local",
                                domain: nil,
                                credentialRef: credentialRef,
                                backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? false,
                                backgroundBackupMinIntervalMinutes: baseProfile?.backgroundBackupMinIntervalMinutes ?? BackgroundBackupInterval.default.minutes,
                                backgroundBackupRequiresWiFi: baseProfile?.backgroundBackupRequiresWiFi ?? true,
                                generateRemoteThumbnails: baseProfile?.generateRemoteThumbnails ?? false,
                                createdAt: baseProfile?.createdAt ?? Date(),
                                updatedAt: Date()
                            )

                            return try await MainActor.run { [weak self] in
                                guard let self, !Task.isCancelled else { throw CancellationError() }
                                return try self.persistVerifiedProfile(profile, finalDisplayPath: finalDisplayPath)
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
                            self.onSaved(savedProfile, "")
                        }
                        self.popAfterSave()
                    }
                } catch is CancellationError {
                    await MainActor.run { [weak self] in self?.endSave() }
                } catch {
                    await MainActor.run { [weak self] in
                        guard let self else { return }
                        self.endSave()
                        self.presentAlert(
                            title: String(localized: "auth.saveFailed"),
                            message: UserFacingErrorLocalizer.message(for: error, storageType: .externalVolume)
                        )
                    }
                }
            }
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .externalVolume)
            )
        }
    }

    private func persistVerifiedProfile(
        _ proposedProfile: ServerProfileRecord,
        finalDisplayPath: String
    ) throws -> ServerProfileRecord {
        var profile = proposedProfile
        guard let savedProfile = try dependencies.appRuntimeFlags.withProfileMutationLease(
            profileID: editingProfile?.id,
            {
                if let duplicate = try findExistingProfile(displayPath: finalDisplayPath),
                   editingProfile == nil || duplicate.id != editingProfile?.id {
                    throw NSError(
                        domain: "AddExternalStorage",
                        code: 2,
                        userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.external.duplicateDir")]
                    )
                }
                try dependencies.databaseManager.saveConnectionProfile(
                    &profile,
                    editingProfileID: editingProfile?.id
                )
                if editingProfile != nil {
                    onSaved(profile, "")
                }
                return profile
            }
        ) else {
            throw RemoteStorageClientError.unavailable
        }
        return savedProfile
    }

    private func setSaving(_ saving: Bool) {
        tableView.isUserInteractionEnabled = !saving
        saveBarButtonItem.isEnabled = !saving
        isModalInPresentation = saving
        navigationController?.interactivePopGestureRecognizer?.isEnabled = !saving
        if !saving {
            navigationController?.isModalInPresentation = false
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
        } else {
            if navigationController?.presentingViewController != nil || navigationController?.isBeingPresented == true {
                navigationController?.isModalInPresentation = true
            }
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        }
    }

    private func endSave() {
        saveTask = nil
        setSaving(false)
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func findExistingProfile(displayPath: String) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .externalVolume &&
                profile.externalVolumeParams?.displayPath == displayPath
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
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
}

private final class ExternalBookmarkRefreshCapture: @unchecked Sendable {
    private let lock = NSLock()
    private var payload: LocalVolumeClient.BookmarkRefreshPayload?

    func record(_ payload: LocalVolumeClient.BookmarkRefreshPayload) {
        lock.lock()
        self.payload = payload
        lock.unlock()
    }

    func snapshot() -> LocalVolumeClient.BookmarkRefreshPayload? {
        lock.lock()
        defer { lock.unlock() }
        return payload
    }
}

extension AddExternalStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        visibleSections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        1
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .location:
            return String(localized: "auth.section.directory")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = resolvedSection(at: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .location:
            return String(localized: "auth.external.footerNoDir")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = resolvedSection(at: indexPath.section) else { return UITableViewCell() }

        switch section {
        case .name:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
            ) as? SettingsTextFieldCell else {
                return UITableViewCell()
            }
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: String(localized: "auth.external.placeholder.name"),
                autocapitalizationType: .words,
                returnKeyType: .done,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.dismissKeyboard() }
            return cell
        case .location:
            let cell = tableView.dequeueReusableCell(withIdentifier: "ValueCell", for: indexPath)
            cell.accessoryType = .disclosureIndicator
            var content = UIListContentConfiguration.valueCell()
            if let path = currentDisplayPath() {
                content.text = URL(fileURLWithPath: path).lastPathComponent
            } else {
                content.text = String(localized: "auth.external.selectDir")
            }
            content.textProperties.color = .label
            cell.contentConfiguration = content
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = resolvedSection(at: indexPath.section), section == .location else { return }
        pickDirectoryTapped()
    }
}

extension AddExternalStorageViewController: UIDocumentPickerDelegate {
    func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
        guard let url = urls.first else { return }

        selectedDirectoryURL = url
        if nameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            nameText = url.lastPathComponent
        }
        updateSaveButtonState()
        tableView.reloadData()
    }
}
