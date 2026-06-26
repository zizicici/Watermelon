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

        var content = cell.defaultContentConfiguration()
        content.text = profile.storageProfile.displayTitle
        content.secondaryText = profile.backgroundBackupSummary
        content.image = StorageProfileIcon.image(for: profile.resolvedStorageType)
        cell.contentConfiguration = content
        cell.accessoryView = nil
        cell.accessoryType = .disclosureIndicator
        cell.selectionStyle = .default
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let profile = sections[indexPath.section].profiles[indexPath.row]
        let vc = BackgroundBackupNodeDetailViewController(dependencies: dependencies, profile: profile)
        navigationController?.pushViewController(vc, animated: true)
    }
}
