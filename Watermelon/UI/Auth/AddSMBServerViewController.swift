import SnapKit
import UIKit

final class AddSMBServerViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case summary
    }

    private let dependencies: DependencyContainer
    private let context: SMBServerPathContext
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
    private lazy var keyboardToolbar: UIToolbar = {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(barButtonSystemItem: .done, target: self, action: #selector(dismissKeyboard))
        ]
        return toolbar
    }()

    private var nameText = ""

    init(
        dependencies: DependencyContainer,
        context: SMBServerPathContext,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.context = context
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
        title = editingProfile == nil ? String(localized: "auth.smb.save.title") : String(localized: "auth.smb.save.editTitle")

        nameText = editingProfile?.name ?? context.auth.name
        configureUI()
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
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "SummaryCell")

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        do {
            let (profile, password) = try saveProfile()
            onSaved(profile, password)
            popAfterSave()
        } catch {
            presentAlert(title: String(localized: "auth.saveFailed"), message: error.localizedDescription)
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

    private func saveProfile() throws -> (ServerProfileRecord, String) {
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? context.auth.host : finalName

        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        let existing = try dependencies.databaseManager.findServerProfile(
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain
        )

        if let editingProfile,
           let duplicate = existing,
           duplicate.id != editingProfile.id {
            throw NSError(
                domain: "AddSMBServerViewController",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "已存在相同的 SMB 连接配置"]
            )
        }

        let credentialRef = [
            "smb",
            context.auth.host,
            String(context.auth.port),
            context.shareName,
            context.auth.domain ?? "",
            context.auth.username
        ].joined(separator: "|")
        let baseProfile = editingProfile ?? existing

        var profile = ServerProfileRecord(
            id: baseProfile?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain,
            credentialRef: credentialRef,
            backgroundBackupEnabled: baseProfile?.backgroundBackupEnabled ?? true,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: context.auth.password, account: credentialRef)
        if let oldRef = baseProfile?.credentialRef,
           oldRef != credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return (profile, context.auth.password)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension AddSMBServerViewController: UITableViewDataSource, UITableViewDelegate {
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
        case .summary:
            return String(localized: "auth.section.connection")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return nil
        case .summary:
            return String(localized: "auth.smb.save.footer")
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
                placeholder: "Home NAS",
                autocapitalizationType: .words,
                returnKeyType: .done,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.dismissKeyboard() }
            return cell
        case .summary:
            let cell = tableView.dequeueReusableCell(withIdentifier: "SummaryCell", for: indexPath)
            cell.selectionStyle = .none
            var content = UIListContentConfiguration.subtitleCell()
            content.secondaryText = [
                "Host: \(context.auth.host):\(context.auth.port)",
                "Share: \(context.shareName)",
                "Path: \(context.basePath)",
                "Username: \(context.auth.username)",
                "Domain: \(context.auth.domain ?? "(none)")"
            ].joined(separator: "\n")
            content.secondaryTextProperties.color = .secondaryLabel
            content.secondaryTextProperties.numberOfLines = 0
            cell.contentConfiguration = content
            return cell
        }
    }
}
