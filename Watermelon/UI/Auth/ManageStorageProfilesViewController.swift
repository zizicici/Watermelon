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
        view.backgroundColor = .appBackground
        title = String(localized: "auth.manage.title")
        navigationItem.rightBarButtonItem = editButtonItem
        configureTableView()
        reloadProfiles()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        reloadProfiles()
    }

    override func setEditing(_ editing: Bool, animated: Bool) {
        super.setEditing(editing, animated: animated)
        tableView.setEditing(editing, animated: animated)
    }

    private func configureTableView() {
        tableView.backgroundColor = .appBackground
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

    private func persistSortOrder() {
        let ids = profiles.compactMap(\.id)
        guard !ids.isEmpty else { return }
        do {
            try dependencies.databaseManager.saveServerProfileSortOrder(profileIDs: ids)
            onProfilesChanged()
        } catch {
            presentAlert(title: String(localized: "auth.manage.sortFailed"), message: error.localizedDescription)
            reloadProfiles()
        }
    }

    private func showDetail(for profile: ServerProfileRecord) {
        let detail = StorageProfileDetailViewController(
            dependencies: dependencies,
            profile: profile,
            onProfilesChanged: onProfilesChanged
        )
        navigationController?.pushViewController(detail, animated: true)
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
            presentAlert(title: String(localized: "auth.manage.deleteFailed"), message: error.localizedDescription)
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
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
        content.image = StorageProfileIcon.image(for: profile.resolvedStorageType)
        cell.contentConfiguration = content
        cell.showsReorderControl = true
        cell.accessoryView = nil
        cell.accessoryType = .disclosureIndicator
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
        let delete = UIContextualAction(style: .destructive, title: String(localized: "common.delete")) { [weak self] _, _, completion in
            self?.deleteProfile(at: indexPath.row)
            completion(true)
        }
        return UISwipeActionsConfiguration(actions: [delete])
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard !tableView.isEditing,
              indexPath.row < profiles.count
        else { return }
        showDetail(for: profiles[indexPath.row])
    }
}
