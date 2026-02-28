import SnapKit
import UIKit

final class ManageStorageProfilesViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let onProfilesChanged: () -> Void

    private var profiles: [ServerProfileRecord] = []
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    init(dependencies: DependencyContainer, onProfilesChanged: @escaping () -> Void) {
        self.dependencies = dependencies
        self.onProfilesChanged = onProfilesChanged
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "管理存储"
        navigationItem.rightBarButtonItem = editButtonItem
        configureTableView()
        reloadProfiles()
    }

    override func setEditing(_ editing: Bool, animated: Bool) {
        super.setEditing(editing, animated: animated)
        tableView.setEditing(editing, animated: animated)
    }

    private func configureTableView() {
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "profile")
        tableView.dataSource = self
        tableView.delegate = self
        tableView.allowsSelection = true

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
    }

    private func reloadProfiles() {
        profiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        tableView.reloadData()
        editButtonItem.isEnabled = profiles.count > 1
    }

    private func symbolImage(for profile: ServerProfileRecord) -> UIImage? {
        switch profile.resolvedStorageType {
        case .smb:
            return UIImage(systemName: "network")
        case .webdav:
            return UIImage(systemName: "globe")
        case .externalVolume:
            return UIImage(systemName: "externaldrive")
        }
    }

    private func persistSortOrder() {
        let ids = profiles.compactMap(\.id)
        guard !ids.isEmpty else { return }
        do {
            try dependencies.databaseManager.saveServerProfileSortOrder(profileIDs: ids)
            onProfilesChanged()
        } catch {
            presentAlert(title: "排序保存失败", message: error.localizedDescription)
            reloadProfiles()
        }
    }

    private func editConnectionParameters(at index: Int) {
        guard index >= 0, index < profiles.count else { return }
        let profile = profiles[index]
        switch profile.resolvedStorageType {
        case .smb:
            openSMBEditor(for: profile)
        case .webdav:
            openWebDAVEditor(for: profile)
        case .externalVolume:
            openExternalEditor(for: profile)
        }
    }

    private func openSMBEditor(for profile: ServerProfileRecord) {
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
            self?.handleConnectionProfileEdited(editedProfileID: profile.id)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openExternalEditor(for profile: ServerProfileRecord) {
        let editor = AddExternalStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionProfileEdited(editedProfileID: profile.id)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openWebDAVEditor(for profile: ServerProfileRecord) {
        let editor = AddWebDAVStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionProfileEdited(editedProfileID: profile.id)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func handleConnectionProfileEdited(editedProfileID: Int64?) {
        if dependencies.appSession.activeProfile?.id == editedProfileID {
            try? dependencies.databaseManager.setActiveServerProfileID(nil)
            dependencies.appSession.clear()
        }
        reloadProfiles()
        onProfilesChanged()
    }

    private func deleteProfile(at index: Int) {
        guard index >= 0, index < profiles.count else { return }
        let profile = profiles[index]
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
            profiles.remove(at: index)
            try dependencies.databaseManager.saveServerProfileSortOrder(profileIDs: profiles.compactMap(\.id))
            tableView.deleteRows(at: [IndexPath(row: index, section: 0)], with: .automatic)
            editButtonItem.isEnabled = profiles.count > 1
            onProfilesChanged()
        } catch {
            presentAlert(title: "删除失败", message: error.localizedDescription)
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }
}

extension ManageStorageProfilesViewController: UITableViewDataSource, UITableViewDelegate {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        profiles.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "profile", for: indexPath)
        let profile = profiles[indexPath.row]

        var content = cell.defaultContentConfiguration()
        content.text = profile.storageProfile.displayTitle
        content.secondaryText = profile.storageProfile.displaySubtitle
        content.image = symbolImage(for: profile)
        cell.contentConfiguration = content
        cell.showsReorderControl = true
        cell.accessoryType = (dependencies.appSession.activeProfile?.id == profile.id) ? .checkmark : .none
        return cell
    }

    func tableView(_ tableView: UITableView, canMoveRowAt indexPath: IndexPath) -> Bool {
        indexPath.row < profiles.count
    }

    func tableView(_ tableView: UITableView, moveRowAt sourceIndexPath: IndexPath, to destinationIndexPath: IndexPath) {
        guard sourceIndexPath.row < profiles.count, destinationIndexPath.row < profiles.count else { return }
        let moved = profiles.remove(at: sourceIndexPath.row)
        profiles.insert(moved, at: destinationIndexPath.row)
        persistSortOrder()
    }

    func tableView(_ tableView: UITableView, canEditRowAt indexPath: IndexPath) -> Bool {
        indexPath.row < profiles.count
    }

    func tableView(_ tableView: UITableView, trailingSwipeActionsConfigurationForRowAt indexPath: IndexPath) -> UISwipeActionsConfiguration? {
        let delete = UIContextualAction(style: .destructive, title: "删除") { [weak self] _, _, completion in
            self?.deleteProfile(at: indexPath.row)
            completion(true)
        }
        return UISwipeActionsConfiguration(actions: [delete])
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard !tableView.isEditing else { return }
        editConnectionParameters(at: indexPath.row)
    }
}
