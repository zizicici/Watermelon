import UIKit

final class StorageProfileUploadWorkerCountViewController: UIViewController {
    private let dependencies: DependencyContainer
    private var profile: ServerProfileRecord
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let cellID = "Cell"

    init(dependencies: DependencyContainer, profile: ServerProfileRecord) {
        self.dependencies = dependencies
        self.profile = profile
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "more.item.workerCount")
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellID)
        view.addSubview(tableView)
        tableView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.topAnchor),
            tableView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
    }

    private var isMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting ||
            dependencies.remoteMaintenanceController.isBusy ||
            dependencies.appRuntimeFlags.isConnecting(profileID: profile.id)
    }

    private func select(_ selection: NodeBackupWorkerCountSelection) {
        guard let profileID = profile.id else { return }
        guard !isMutationBlocked else {
            presentAlert(message: String(localized: "home.alert.maintenanceInProgress"))
            return
        }
        guard selection.persistedMode != profile.uploadWorkerCountMode else { return }

        do {
            guard let _ = try dependencies.appRuntimeFlags.withProfileMutationLease(profileID: profileID, {
                try dependencies.databaseManager.setUploadWorkerCountMode(
                    selection.persistedMode,
                    profileID: profileID
                )
                dependencies.appSession.setActiveUploadWorkerCountMode(
                    selection.persistedMode,
                    profileID: profileID
                )
                profile.uploadWorkerCountMode = selection.persistedMode
            }) else {
                presentAlert(message: String(localized: "home.alert.maintenanceInProgress"))
                return
            }
            tableView.reloadData()
        } catch {
            presentAlert(message: UserFacingErrorLocalizer.message(for: error))
        }
    }

    private func presentAlert(message: String) {
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }
}

extension StorageProfileUploadWorkerCountViewController: UITableViewDataSource, UITableViewDelegate {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        NodeBackupWorkerCountSelection.allCases.count
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        profile.name
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        String(localized: "settings.worker.node.footer")
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let selection = NodeBackupWorkerCountSelection.allCases[indexPath.row]
        let current = NodeBackupWorkerCountSelection(persistedMode: profile.uploadWorkerCountMode)
        let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
        var content = cell.defaultContentConfiguration()
        content.text = selection.getName()
        cell.contentConfiguration = content
        cell.accessoryType = selection == current ? .checkmark : .none
        cell.selectionStyle = .default
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        select(NodeBackupWorkerCountSelection.allCases[indexPath.row])
    }
}
