import SnapKit
import UIKit

final class ManageStorageProfilesViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let onProfilesChanged: () -> Void
    private let onConnectRequested: ((ServerProfileRecord) -> Void)?

    private var sections: [StorageProfileSection] = []
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    init(
        dependencies: DependencyContainer,
        onProfilesChanged: @escaping () -> Void,
        onConnectRequested: ((ServerProfileRecord) -> Void)? = nil
    ) {
        self.dependencies = dependencies
        self.onProfilesChanged = onProfilesChanged
        self.onConnectRequested = onConnectRequested
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
        let all = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        sections = all.groupedByStorageType()
        tableView.reloadData()
        editButtonItem.isEnabled = sections.contains { $0.profiles.count > 1 }
    }

    private func profile(at indexPath: IndexPath) -> ServerProfileRecord? {
        guard sections.indices.contains(indexPath.section),
              sections[indexPath.section].profiles.indices.contains(indexPath.row) else { return nil }
        return sections[indexPath.section].profiles[indexPath.row]
    }

    private func persistSortOrder() {
        let ids = sections.flatMap { $0.profiles }.compactMap(\.id)
        guard !ids.isEmpty else { return }
        do {
            try dependencies.databaseManager.saveServerProfileSortOrder(profileIDs: ids)
            onProfilesChanged()
        } catch {
            presentAlert(
                title: String(localized: "auth.manage.sortFailed"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
            reloadProfiles()
        }
    }

    private func showDetail(for profile: ServerProfileRecord) {
        let detail = StorageProfileDetailViewController(
            dependencies: dependencies,
            profile: profile,
            onProfilesChanged: onProfilesChanged,
            onConnectRequested: onConnectRequested
        )
        navigationController?.pushViewController(detail, animated: true)
    }

    private func deleteProfile(at indexPath: IndexPath) {
        guard let profile = profile(at: indexPath), let id = profile.id else { return }

        // A maintenance task (verify / leftover scan / delete) captures profile/password by value; deleting
        // mid-op lets it write to a freed id.
        let isActiveProfile = dependencies.appSession.activeProfile?.id == id
        let isBusy = dependencies.remoteMaintenanceController.isBusy(profileID: id)
            || (isActiveProfile && dependencies.appRuntimeFlags.isExecuting)
        if isBusy {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }

        do {
            try dependencies.databaseManager.deleteServerProfile(id: id)
            if profile.storageProfile.requiresPassword {
                try? dependencies.keychainService.delete(account: profile.credentialRef)
            }
            try? RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
                .deleteProfileKey(profileID: id)
            if dependencies.appSession.activeProfile?.id == id {
                try? dependencies.databaseManager.setActiveServerProfileID(nil)
                dependencies.appSession.clear()
            }
            sections[indexPath.section].profiles.remove(at: indexPath.row)
            let sectionEmptied = sections[indexPath.section].profiles.isEmpty
            if sectionEmptied {
                sections.remove(at: indexPath.section)
            }
            try dependencies.databaseManager.saveServerProfileSortOrder(
                profileIDs: sections.flatMap { $0.profiles }.compactMap(\.id)
            )
            if sectionEmptied {
                tableView.deleteSections(IndexSet(integer: indexPath.section), with: .automatic)
            } else {
                tableView.deleteRows(at: [indexPath], with: .automatic)
            }
            editButtonItem.isEnabled = sections.contains { $0.profiles.count > 1 }
            onProfilesChanged()
        } catch {
            presentAlert(
                title: String(localized: "auth.manage.deleteFailed"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension ManageStorageProfilesViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        sections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        sections[section].profiles.count
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        sections[section].type.sectionHeaderText
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "profile", for: indexPath)
        let profile = sections[indexPath.section].profiles[indexPath.row]

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
        profile(at: indexPath) != nil
    }

    func tableView(
        _ tableView: UITableView,
        targetIndexPathForMoveFromRowAt sourceIndexPath: IndexPath,
        toProposedIndexPath proposedDestinationIndexPath: IndexPath
    ) -> IndexPath {
        guard sourceIndexPath.section != proposedDestinationIndexPath.section else {
            return proposedDestinationIndexPath
        }
        let lastRow = max(0, sections[sourceIndexPath.section].profiles.count - 1)
        let row = proposedDestinationIndexPath.section < sourceIndexPath.section ? 0 : lastRow
        return IndexPath(row: row, section: sourceIndexPath.section)
    }

    func tableView(_ tableView: UITableView, moveRowAt sourceIndexPath: IndexPath, to destinationIndexPath: IndexPath) {
        guard sourceIndexPath.section == destinationIndexPath.section,
              sourceIndexPath.section < sections.count,
              sourceIndexPath.row < sections[sourceIndexPath.section].profiles.count,
              destinationIndexPath.row < sections[sourceIndexPath.section].profiles.count else { return }
        let moved = sections[sourceIndexPath.section].profiles.remove(at: sourceIndexPath.row)
        sections[sourceIndexPath.section].profiles.insert(moved, at: destinationIndexPath.row)
        persistSortOrder()
    }

    func tableView(_ tableView: UITableView, canEditRowAt indexPath: IndexPath) -> Bool {
        profile(at: indexPath) != nil
    }

    func tableView(_ tableView: UITableView, trailingSwipeActionsConfigurationForRowAt indexPath: IndexPath) -> UISwipeActionsConfiguration? {
        let delete = UIContextualAction(style: .destructive, title: String(localized: "common.delete")) { [weak self] _, _, completion in
            self?.deleteProfile(at: indexPath)
            completion(true)
        }
        return UISwipeActionsConfiguration(actions: [delete])
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard !tableView.isEditing, let profile = profile(at: indexPath) else { return }
        showDetail(for: profile)
    }
}
