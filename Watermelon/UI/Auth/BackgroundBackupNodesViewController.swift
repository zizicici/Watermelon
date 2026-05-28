import Foundation
import SnapKit
import UIKit

final class BackgroundBackupNodesViewController: UIViewController {
    private let dependencies: DependencyContainer
    private var sections: [StorageProfileSection] = []
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let toggleCellID = "ToggleCell"

    private var executionObserver: NSObjectProtocol?
    private var maintenanceObserver: NSObjectProtocol?

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let executionObserver {
            NotificationCenter.default.removeObserver(executionObserver)
        }
        if let maintenanceObserver {
            NotificationCenter.default.removeObserver(maintenanceObserver)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "more.item.backgroundBackup.nodes")
        configureTableView()
        observeLifecycle()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        reloadProfiles()
    }

    private func configureTableView() {
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: toggleCellID)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
    }

    private func observeLifecycle() {
        executionObserver = NotificationCenter.default.addObserver(
            forName: .ExecutionLifecycleDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.tableView.reloadData()
        }
        maintenanceObserver = NotificationCenter.default.addObserver(
            forName: .RemoteMaintenanceDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.tableView.reloadData()
        }
    }

    private func reloadProfiles() {
        let all = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        sections = all.groupedByStorageType(excluding: [.externalVolume])
        tableView.reloadData()
    }

    private var isProfileMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting
            || dependencies.appRuntimeFlags.isVerifying
            || dependencies.remoteMaintenanceController.isVerifying
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func setBackgroundBackup(profileID: Int64, enabled: Bool, sender: UISwitch) {
        if isProfileMutationBlocked {
            sender.setOn(!enabled, animated: true)
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }
        do {
            try dependencies.databaseManager.setBackgroundBackupEnabled(enabled, profileID: profileID)
            dependencies.appSession.setActiveBackgroundBackupEnabled(enabled, profileID: profileID)
            updateLocalState(profileID: profileID, enabled: enabled)
            NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
        } catch {
            sender.setOn(!enabled, animated: true)
            presentAlert(
                title: String(localized: "common.error"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    private func updateLocalState(profileID: Int64, enabled: Bool) {
        for s in sections.indices {
            if let r = sections[s].profiles.firstIndex(where: { $0.id == profileID }) {
                sections[s].profiles[r].backgroundBackupEnabled = enabled
                return
            }
        }
    }
}

extension BackgroundBackupNodesViewController: UITableViewDataSource, UITableViewDelegate {
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
        let cell = tableView.dequeueReusableCell(withIdentifier: toggleCellID, for: indexPath)
        let profile = sections[indexPath.section].profiles[indexPath.row]
        let blocked = isProfileMutationBlocked

        var content = cell.defaultContentConfiguration()
        content.text = profile.storageProfile.displayTitle
        content.secondaryText = profile.storageProfile.displaySubtitle
        content.image = StorageProfileIcon.image(for: profile.resolvedStorageType)
        content.textProperties.color = blocked ? .secondaryLabel : .label
        cell.contentConfiguration = content

        let toggle = UISwitch()
        toggle.isOn = profile.backgroundBackupEnabled
        toggle.isEnabled = !blocked
        if let profileID = profile.id {
            let action = UIAction { [weak self, weak toggle] _ in
                guard let self, let toggle else { return }
                self.setBackgroundBackup(profileID: profileID, enabled: toggle.isOn, sender: toggle)
            }
            toggle.addAction(action, for: .valueChanged)
        }
        cell.accessoryView = toggle
        cell.accessoryType = .none
        cell.selectionStyle = .none
        return cell
    }
}
