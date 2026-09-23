import Foundation
import SnapKit
import UIKit

final class BackgroundBackupNodesViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let onProfilesChanged: (() -> Void)?
    private var sections: [StorageProfileSection] = []
    private var dataSourceErrors: [Int64: LocalDataSourceError] = [:]
    private var dataSourceErrorsTask: Task<Void, Never>?
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let toggleCellID = "ToggleCell"

    private var executionObserver: NSObjectProtocol?
    private var maintenanceObserver: NSObjectProtocol?
    private var foregroundObserver: NSObjectProtocol?

    init(dependencies: DependencyContainer, onProfilesChanged: (() -> Void)? = nil) {
        self.dependencies = dependencies
        self.onProfilesChanged = onProfilesChanged
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        dataSourceErrorsTask?.cancel()
        if let executionObserver {
            NotificationCenter.default.removeObserver(executionObserver)
        }
        if let maintenanceObserver {
            NotificationCenter.default.removeObserver(maintenanceObserver)
        }
        if let foregroundObserver {
            NotificationCenter.default.removeObserver(foregroundObserver)
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
        foregroundObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.reloadProfiles()
        }
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
        reloadDataSourceErrors()
    }

    // Album validation hits PhotoKit; keep it off the main thread and fold the result back in.
    private func reloadDataSourceErrors() {
        let profiles = sections.flatMap(\.profiles)
        let service = dependencies.photoLibraryService
        dataSourceErrorsTask?.cancel()
        dataSourceErrorsTask = Task { [weak self] in
            let errors = await withCancellableDetachedValue { service.nodeDataSourceErrors(for: profiles) }
            guard !Task.isCancelled, let self, self.dataSourceErrors != errors else { return }
            self.dataSourceErrors = errors
            self.tableView.reloadData()
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
        let summary = profile.backgroundBackupEnabled
            ? "\(profile.defaultBackupDataSource().title), \(profile.backgroundBackupSummary)"
            : profile.backgroundBackupSummary

        var content = cell.defaultContentConfiguration()
        content.text = profile.name
        content.secondaryText = summary
        if let id = profile.id, let error = dataSourceErrors[id] {
            content.secondaryText = "\(summary)\n\(error.localizedDescription)"
            content.secondaryTextProperties.color = .systemRed
            content.secondaryTextProperties.numberOfLines = 0
        }
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
        let vc = StorageProfileDetailViewController(dependencies: dependencies, profile: profile, onProfilesChanged: { [weak self] in
            self?.reloadProfiles()
            self?.onProfilesChanged?()
        })
        navigationController?.pushViewController(vc, animated: true)
    }
}
