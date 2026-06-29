import Foundation
import UIKit

final class BackgroundBackupNodeDetailViewController: UIViewController {
    private let dependencies: DependencyContainer
    private var profile: ServerProfileRecord
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let cellID = "Cell"

    private enum Section: Int, CaseIterable {
        case enable
        case interval
        case network
    }

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
        title = profile.storageProfile.displayTitle
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: cellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "Value")
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
        dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isBusy
    }

    private func presentBlockedAlert(revert: () -> Void) {
        revert()
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func presentError(_ error: Error) {
        let alert = UIAlertController(
            title: String(localized: "common.error"),
            message: UserFacingErrorLocalizer.message(for: error),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    // MARK: - Mutations

    private func setEnabled(_ enabled: Bool, sender: UISwitch) {
        guard let profileID = profile.id else { return }
        if isMutationBlocked {
            presentBlockedAlert { sender.setOn(!enabled, animated: true) }
            return
        }
        do {
            try dependencies.databaseManager.setBackgroundBackupEnabled(enabled, profileID: profileID)
            dependencies.appSession.setActiveBackgroundBackupEnabled(enabled, profileID: profileID)
            profile.backgroundBackupEnabled = enabled
            NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
        } catch {
            sender.setOn(!enabled, animated: true)
            presentError(error)
        }
    }

    private func setRequiresWiFi(_ requiresWiFi: Bool, sender: UISwitch) {
        guard let profileID = profile.id else { return }
        if isMutationBlocked {
            presentBlockedAlert { sender.setOn(!requiresWiFi, animated: true) }
            return
        }
        do {
            try dependencies.databaseManager.setBackgroundBackupRequiresWiFi(requiresWiFi, profileID: profileID)
            profile.backgroundBackupRequiresWiFi = requiresWiFi
            NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
        } catch {
            sender.setOn(!requiresWiFi, animated: true)
            presentError(error)
        }
    }

    private func setInterval(_ interval: BackgroundBackupInterval) {
        guard let profileID = profile.id else { return }
        if isMutationBlocked {
            presentBlockedAlert {}
            return
        }
        guard profile.backgroundBackupMinIntervalMinutes != interval.minutes else { return }
        do {
            try dependencies.databaseManager.setBackgroundBackupMinIntervalMinutes(interval.minutes, profileID: profileID)
            profile.backgroundBackupMinIntervalMinutes = interval.minutes
            NotificationCenter.default.post(name: .BackgroundBackupProfileChanged, object: nil)
            tableView.reloadSections(IndexSet(integer: Section.interval.rawValue), with: .none)
        } catch {
            presentError(error)
        }
    }
}

extension BackgroundBackupNodeDetailViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        switch Section(rawValue: section)! {
        case .enable: return 1
        case .interval: return BackgroundBackupInterval.allCases.count
        case .network: return 1
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch Section(rawValue: section)! {
        case .enable: return nil
        case .interval: return String(localized: "backgroundBackup.interval.header")
        case .network: return String(localized: "backgroundBackup.wifi.header")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        switch Section(rawValue: section)! {
        case .enable: return enableSectionFooter
        case .interval: return String(localized: "backgroundBackup.interval.footer")
        case .network: return String(localized: "backgroundBackup.wifi.footer")
        }
    }

    // SMB and plain-HTTP WebDAV expose credentials to a same-address impostor on an untrusted network; warn and
    // point at the per-node Shortcuts toggle. TLS WebDAV, S3 (SigV4) and SFTP (host-key pinned) are safe.
    private var enableSectionFooter: String {
        switch profile.resolvedStorageType {
        case .smb:
            return String(localized: "backgroundBackup.risk.smb")
        case .webdav where profile.webDAVParams?.scheme.lowercased() == "http":
            return String(localized: "backgroundBackup.risk.httpWebdav")
        default:
            return String(localized: "backgroundBackup.node.enableFooter")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        switch Section(rawValue: indexPath.section)! {
        case .enable:
            let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "more.item.backgroundBackup")
            cell.contentConfiguration = content
            let toggle = UISwitch()
            toggle.isOn = profile.backgroundBackupEnabled
            toggle.addAction(UIAction { [weak self, weak toggle] _ in
                guard let self, let toggle else { return }
                self.setEnabled(toggle.isOn, sender: toggle)
            }, for: .valueChanged)
            cell.accessoryView = toggle
            cell.selectionStyle = .none
            return cell

        case .interval:
            let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
            let interval = BackgroundBackupInterval.allCases[indexPath.row]
            var content = cell.defaultContentConfiguration()
            content.text = interval.localizedText
            cell.contentConfiguration = content
            cell.accessoryView = nil
            cell.accessoryType = interval.minutes == profile.backgroundBackupMinIntervalMinutes ? .checkmark : .none
            cell.selectionStyle = .default
            return cell

        case .network:
            let cell = tableView.dequeueReusableCell(withIdentifier: cellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "backgroundBackup.wifi.label")
            cell.contentConfiguration = content
            let toggle = UISwitch()
            toggle.isOn = profile.backgroundBackupRequiresWiFi
            toggle.addAction(UIAction { [weak self, weak toggle] _ in
                guard let self, let toggle else { return }
                self.setRequiresWiFi(toggle.isOn, sender: toggle)
            }, for: .valueChanged)
            cell.accessoryView = toggle
            cell.selectionStyle = .none
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard Section(rawValue: indexPath.section) == .interval else { return }
        setInterval(BackgroundBackupInterval.allCases[indexPath.row])
    }
}
