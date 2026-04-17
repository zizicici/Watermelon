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

    private var nameText = ""
    private var selectedDirectoryURL: URL?

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
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
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
        dismissKeyboard()
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [.folder], asCopy: false)
        picker.delegate = self
        picker.allowsMultipleSelection = false
        present(picker, animated: true)
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        do {
            let selectedDisplayPath: String
            let encodedParams: Data
            if let selectedDirectoryURL {
                let scoped = selectedDirectoryURL.startAccessingSecurityScopedResource()
                defer {
                    if scoped {
                        selectedDirectoryURL.stopAccessingSecurityScopedResource()
                    }
                }
                let bookmarkData = try bookmarkStore.makeBookmarkData(for: selectedDirectoryURL)
                selectedDisplayPath = selectedDirectoryURL.path
                let params = ExternalVolumeConnectionParams(
                    rootBookmarkData: bookmarkData,
                    displayPath: selectedDisplayPath
                )
                encodedParams = try ServerProfileRecord.encodedConnectionParams(params)
            } else if let editingProfile,
                      let existingPath = editingProfile.externalVolumeParams?.displayPath,
                      let existingParams = editingProfile.connectionParams {
                selectedDisplayPath = existingPath
                encodedParams = existingParams
            } else {
                presentAlert(title: String(localized: "auth.external.noDirSelected"), message: String(localized: "auth.external.noDirMessage"))
                return
            }

            let existing = try findExistingProfile(displayPath: selectedDisplayPath)
            if let editingProfile,
               let existing,
               existing.id != editingProfile.id {
                throw NSError(
                    domain: "AddExternalStorage",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.external.duplicateDir")]
                )
            }

            let baseProfile = editingProfile ?? existing
            let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
            let profileName = finalName.isEmpty ? URL(fileURLWithPath: selectedDisplayPath).lastPathComponent : finalName
            let credentialRef = baseProfile?.credentialRef ?? "external:\(UUID().uuidString)"
            let shareName = baseProfile?.shareName ?? "external-\(UUID().uuidString)"

            var profile = ServerProfileRecord(
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
                backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? true,
                createdAt: baseProfile?.createdAt ?? Date(),
                updatedAt: Date()
            )

            try dependencies.databaseManager.saveServerProfile(&profile)
            onSaved(profile, "")
            popAfterSave()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .externalVolume)
            )
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

extension AddExternalStorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        1
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .location:
            return String(localized: "auth.section.directory")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .location:
            return currentDisplayPath() ?? String(localized: "auth.external.footerNoDir")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section) else { return UITableViewCell() }

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
        guard let section = Section(rawValue: indexPath.section), section == .location else { return }
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
