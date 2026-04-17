import SnapKit
import UIKit

final class StorageProfileDetailViewController: UIViewController {
    private enum Row {
        case backgroundBackup
        case editConnection
        case delete
    }

    private struct SectionLayout {
        let rows: [Row]
        let footer: String?
    }

    private let dependencies: DependencyContainer
    private var profile: ServerProfileRecord
    private let onProfilesChanged: () -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let toggleCellID = "ToggleCell"
    private let actionCellID = "ActionCell"

    private lazy var backgroundBackupSwitch: UISwitch = {
        let toggle = UISwitch()
        toggle.addTarget(self, action: #selector(backgroundBackupToggleChanged(_:)), for: .valueChanged)
        return toggle
    }()

    init(
        dependencies: DependencyContainer,
        profile: ServerProfileRecord,
        onProfilesChanged: @escaping () -> Void
    ) {
        self.dependencies = dependencies
        self.profile = profile
        self.onProfilesChanged = onProfilesChanged
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = profile.storageProfile.displayTitle
        configureTableView()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        if let refreshed = (try? dependencies.databaseManager.fetchServerProfiles())?.first(where: { $0.id == profile.id }) {
            profile = refreshed
            title = profile.storageProfile.displayTitle
            tableView.reloadData()
        }
    }

    private func configureTableView() {
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: toggleCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: actionCellID)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
    }

    private var sectionLayouts: [SectionLayout] {
        var sections: [SectionLayout] = [
            SectionLayout(rows: [.editConnection], footer: profile.storageProfile.displaySubtitle)
        ]
        if profile.resolvedStorageType != .externalVolume {
            sections.append(SectionLayout(rows: [.backgroundBackup], footer: nil))
        }
        sections.append(SectionLayout(rows: [.delete], footer: nil))
        return sections
    }

    private func editConnectionParameters() {
        switch profile.resolvedStorageType {
        case .smb:
            openSMBEditor()
        case .webdav:
            openWebDAVEditor()
        case .externalVolume:
            openExternalEditor()
        }
    }

    private func openSMBEditor() {
        let draft = SMBServerLoginDraft(
            name: profile.name,
            host: profile.host,
            port: profile.port,
            username: profile.username,
            domain: profile.domain
        )
        let editor = AddSMBServerLoginViewController(
            dependencies: dependencies,
            draft: draft,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionEdited()
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openWebDAVEditor() {
        let editor = AddWebDAVStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionEdited()
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openExternalEditor() {
        let editor = AddExternalStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionEdited()
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func handleConnectionEdited() {
        if dependencies.appSession.activeProfile?.id == profile.id {
            try? dependencies.databaseManager.setActiveServerProfileID(nil)
            dependencies.appSession.clear()
        }
        onProfilesChanged()
    }

    private func confirmDelete() {
        let alert = UIAlertController(
            title: String(localized: "storage.detail.deleteConfirm.title"),
            message: String(localized: "storage.detail.deleteConfirm.message"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.delete"), style: .destructive) { [weak self] _ in
            self?.deleteProfile()
        })
        present(alert, animated: true)
    }

    private func deleteProfile() {
        guard let id = profile.id else { return }
        do {
            try dependencies.databaseManager.deleteServerProfile(id: id)
            if profile.storageProfile.requiresPassword {
                try? dependencies.keychainService.delete(account: profile.credentialRef)
            }
            if dependencies.appSession.activeProfile?.id == id {
                try? dependencies.databaseManager.setActiveServerProfileID(nil)
                dependencies.appSession.clear()
            }
            onProfilesChanged()
            navigationController?.popViewController(animated: true)
        } catch {
            presentAlert(
                title: String(localized: "auth.manage.deleteFailed"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    @objc private func backgroundBackupToggleChanged(_ sender: UISwitch) {
        guard let profileID = profile.id else { return }
        try? dependencies.databaseManager.setBackgroundBackupEnabled(sender.isOn, profileID: profileID)
        profile.backgroundBackupEnabled = sender.isOn
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension StorageProfileDetailViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        sectionLayouts.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        sectionLayouts[section].rows.count
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        sectionLayouts[section].footer
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let row = sectionLayouts[indexPath.section].rows[indexPath.row]
        switch row {
        case .backgroundBackup:
            let cell = tableView.dequeueReusableCell(withIdentifier: toggleCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "more.item.backgroundBackup")
            cell.contentConfiguration = content
            cell.selectionStyle = .none

            backgroundBackupSwitch.isOn = profile.backgroundBackupEnabled
            cell.accessoryView = backgroundBackupSwitch
            cell.accessoryType = .none
            return cell
        case .editConnection:
            let cell = tableView.dequeueReusableCell(withIdentifier: actionCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "storage.detail.editConnection")
            content.textProperties.color = .label
            cell.contentConfiguration = content
            cell.accessoryType = .disclosureIndicator
            cell.accessoryView = nil
            cell.selectionStyle = .default
            return cell
        case .delete:
            let cell = tableView.dequeueReusableCell(withIdentifier: actionCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "storage.detail.deleteStorage")
            content.textProperties.color = .systemRed
            cell.contentConfiguration = content
            cell.accessoryType = .none
            cell.accessoryView = nil
            cell.selectionStyle = .default
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let row = sectionLayouts[indexPath.section].rows[indexPath.row]
        switch row {
        case .backgroundBackup:
            break
        case .editConnection:
            editConnectionParameters()
        case .delete:
            confirmDelete()
        }
    }
}
