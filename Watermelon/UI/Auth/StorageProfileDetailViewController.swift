import Foundation
import SnapKit
import UIKit

final class StorageProfileDetailViewController: UIViewController {
    private enum SectionID {
        case editConnection
        case backgroundBackup
        case remoteOverview
        case delete
    }

    private struct RowSpec {
        let reuseID: String
        let cellBuilder: (UITableView, IndexPath) -> UITableViewCell
        let onTap: (() -> Void)?
    }

    private struct SectionLayout {
        let id: SectionID
        let rows: [RowSpec]
        let footer: String?
    }

    private let dependencies: DependencyContainer
    private var profile: ServerProfileRecord
    private let onProfilesChanged: () -> Void
    private let onConnectRequested: ((ServerProfileRecord) -> Void)?

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let toggleCellID = "ToggleCell"
    private let actionCellID = "ActionCell"
    private let valueCellID = "ValueCell"
    private let placeholderCellID = "PlaceholderCell"

    private var sectionLayouts: [SectionLayout] = []
    private var executionObserver: NSObjectProtocol?
    private var maintenanceObserver: NSObjectProtocol?

    private lazy var backgroundBackupSwitch: UISwitch = {
        let toggle = UISwitch()
        toggle.addTarget(self, action: #selector(backgroundBackupToggleChanged(_:)), for: .valueChanged)
        return toggle
    }()

    init(
        dependencies: DependencyContainer,
        profile: ServerProfileRecord,
        onProfilesChanged: @escaping () -> Void,
        onConnectRequested: ((ServerProfileRecord) -> Void)? = nil
    ) {
        self.dependencies = dependencies
        self.profile = profile
        self.onProfilesChanged = onProfilesChanged
        self.onConnectRequested = onConnectRequested
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
        title = profile.storageProfile.displayTitle
        configureTableView()
        observeLifecycle()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        if let refreshed = (try? dependencies.databaseManager.fetchServerProfiles())?.first(where: { $0.id == profile.id }) {
            profile = refreshed
            title = profile.storageProfile.displayTitle
        }
        rebuildSectionLayouts()
        tableView.reloadData()
    }

    private func configureTableView() {
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: toggleCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: actionCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: valueCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: placeholderCellID)

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
            self?.refreshOverviewSection()
        }
        maintenanceObserver = NotificationCenter.default.addObserver(
            forName: .RemoteMaintenanceDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.refreshOverviewSection()
        }
    }

    private func refreshOverviewSection() {
        // Full reload: blocked state from verify/execution affects edit/delete/background
        // rows too, not just the overview section. Per-section reload would leave stale.
        rebuildSectionLayouts()
        tableView.reloadData()
    }

    // MARK: - Sections

    private func rebuildSectionLayouts() {
        var sections: [SectionLayout] = []
        sections.append(SectionLayout(
            id: .editConnection,
            rows: [makeEditConnectionRow()],
            footer: profile.storageProfile.displaySubtitle
        ))

        if profile.resolvedStorageType != .externalVolume {
            sections.append(SectionLayout(
                id: .backgroundBackup,
                rows: [makeBackgroundBackupRow()],
                footer: nil
            ))
        }

        sections.append(SectionLayout(
            id: .remoteOverview,
            rows: makeRemoteOverviewRows(),
            footer: nil
        ))

        sections.append(SectionLayout(
            id: .delete,
            rows: [makeDeleteRow()],
            footer: nil
        ))

        sectionLayouts = sections
    }

    // MARK: - Rows

    /// Locked while verify/execution holds this profile to avoid orphaning the in-flight task.
    private var isProfileMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isVerifying
    }

    private func makeEditConnectionRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        return RowSpec(
            reuseID: actionCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.actionCellID ?? "ActionCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "storage.detail.editConnection")
                content.textProperties.color = blocked ? .secondaryLabel : .label
                cell.contentConfiguration = content
                cell.accessoryType = blocked ? .none : .disclosureIndicator
                cell.accessoryView = nil
                cell.selectionStyle = blocked ? .none : .default
                return cell
            },
            onTap: { [weak self] in
                guard let self, !self.rejectIfProfileMutationBlocked() else { return }
                self.editConnectionParameters()
            }
        )
    }

    private func makeBackgroundBackupRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        return RowSpec(
            reuseID: toggleCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.toggleCellID ?? "ToggleCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "more.item.backgroundBackup")
                content.textProperties.color = blocked ? .secondaryLabel : .label
                cell.contentConfiguration = content
                cell.selectionStyle = .none
                if let self {
                    self.backgroundBackupSwitch.isOn = self.profile.backgroundBackupEnabled
                    self.backgroundBackupSwitch.isEnabled = !blocked
                    cell.accessoryView = self.backgroundBackupSwitch
                }
                cell.accessoryType = .none
                return cell
            },
            onTap: nil
        )
    }

    private func makeDeleteRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        return RowSpec(
            reuseID: actionCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.actionCellID ?? "ActionCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "storage.detail.deleteStorage")
                content.textProperties.color = blocked ? .secondaryLabel : .systemRed
                content.textProperties.alignment = .center
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = blocked ? .none : .default
                return cell
            },
            onTap: { [weak self] in
                guard let self, !self.rejectIfProfileMutationBlocked() else { return }
                self.confirmDelete()
            }
        )
    }

    /// Defensive guard for the race where verify/execution starts AFTER the row was
    /// rendered enabled — `refreshOverviewSection` is async (notification-driven),
    /// so the user can tap an already-visible enabled row before reload arrives.
    private func rejectIfProfileMutationBlocked() -> Bool {
        guard isProfileMutationBlocked else { return false }
        presentAlert(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress")
        )
        return true
    }

    // MARK: - Remote overview

    private func makeRemoteOverviewRows() -> [RowSpec] {
        let isActive = dependencies.appSession.activeProfile?.id == profile.id
        guard isActive else {
            return [makeDisconnectedPlaceholderRow()]
        }

        let isExecuting = dependencies.appRuntimeFlags.isExecuting
        let isVerifying = dependencies.remoteMaintenanceController.isVerifying
        if isExecuting || isVerifying {
            return [makeBusyPlaceholderRow(isVerifying: isVerifying)]
        }

        let digest = dependencies.backupCoordinator.healthDigest()
        let lastVerifiedAt = profile.id.flatMap {
            try? dependencies.databaseManager.remoteVerifiedAt(profileID: $0)
        }

        var rows: [RowSpec] = [
            makeValueRow(
                title: String(localized: "storage.detail.overview.assetCount"),
                value: Self.formatCount(digest.totalAssets)
            ),
            makeValueRow(
                title: String(localized: "storage.detail.overview.resourceCount"),
                value: Self.formatCount(digest.totalResources)
            ),
            makeValueRow(
                title: String(localized: "storage.detail.overview.diskUsage"),
                value: ByteCountFormatter.string(fromByteCount: digest.totalSizeBytes, countStyle: .file)
            ),
            makeIncompleteAssetsRow(entries: digest.incompleteAssets),
            makeValueRow(
                title: String(localized: "storage.detail.overview.lastIndexSyncedAt"),
                value: Self.formatDate(digest.lastIndexSyncedAt)
            ),
            makeValueRow(
                title: String(localized: "storage.detail.overview.lastVerifiedAt"),
                value: Self.formatDate(lastVerifiedAt)
            ),
        ]

        if let lastError = dependencies.remoteMaintenanceController.lastError,
           lastError.profileID == profile.id {
            rows.append(makeLastErrorRow(message: lastError.message))
        }

        rows.append(makeRefreshButtonRow())
        return rows
    }

    private func makeValueRow(title: String, value: String) -> RowSpec {
        RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = title
                content.secondaryText = value
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = .none
                return cell
            },
            onTap: nil
        )
    }

    private func makeIncompleteAssetsRow(entries: [IncompleteAssetEntry]) -> RowSpec {
        let valueText = Self.formatCount(entries.count)
        return RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "storage.detail.overview.incompleteAssets")
                content.secondaryText = valueText
                cell.contentConfiguration = content
                cell.accessoryType = entries.isEmpty ? .none : .disclosureIndicator
                cell.accessoryView = nil
                cell.selectionStyle = entries.isEmpty ? .none : .default
                return cell
            },
            onTap: entries.isEmpty ? nil : { [weak self] in
                self?.showIncompleteAssets(entries: entries)
            }
        )
    }

    private func makeLastErrorRow(message: String) -> RowSpec {
        RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "storage.detail.overview.lastError")
                content.secondaryText = message
                content.textProperties.color = .systemRed
                content.secondaryTextProperties.numberOfLines = 0
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = .default
                return cell
            },
            onTap: { [weak self] in
                self?.dependencies.remoteMaintenanceController.dismissLastError()
            }
        )
    }

    private func makeRefreshButtonRow() -> RowSpec {
        RowSpec(
            reuseID: actionCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.actionCellID ?? "ActionCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "storage.detail.overview.refreshButton")
                content.textProperties.color = .systemBlue
                content.textProperties.alignment = .center
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = .default
                return cell
            },
            onTap: { [weak self] in self?.refreshAndVerify() }
        )
    }

    private func makeBusyPlaceholderRow(isVerifying: Bool) -> RowSpec {
        let title: String
        if isVerifying,
           let progress = dependencies.remoteMaintenanceController.currentProgress,
           progress.total > 0 {
            // total == 0 during the pre-loop reload phase — avoid showing 0/0.
            title = String.localizedStringWithFormat(
                String(localized: "storage.detail.overview.placeholder.verifying"),
                progress.current,
                progress.total
            )
        } else if isVerifying {
            title = String(localized: "storage.detail.overview.placeholder.verifyingStarting")
        } else {
            title = String(localized: "storage.detail.overview.placeholder.executing")
        }
        return RowSpec(
            reuseID: placeholderCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.placeholderCellID ?? "PlaceholderCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = title
                content.textProperties.color = .secondaryLabel
                content.textProperties.alignment = .center
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = .none
                return cell
            },
            onTap: nil
        )
    }

    private func makeDisconnectedPlaceholderRow() -> RowSpec {
        let canConnect = onConnectRequested != nil
        return RowSpec(
            reuseID: actionCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.actionCellID ?? "ActionCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "storage.detail.overview.placeholder.disconnected")
                content.textProperties.color = canConnect ? .systemBlue : .secondaryLabel
                content.textProperties.alignment = .center
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = canConnect ? .default : .none
                return cell
            },
            onTap: canConnect ? { [weak self] in
                guard let self else { return }
                let refreshed = (try? self.dependencies.databaseManager.fetchServerProfiles())?
                    .first(where: { $0.id == self.profile.id }) ?? self.profile
                self.onConnectRequested?(refreshed)
            } : nil
        )
    }

    // MARK: - Actions

    private func refreshAndVerify() {
        // Both failure modes have a row state that should already hide the button,
        // but tap can land between observer fires — surface an alert instead of
        // silently dropping it.
        guard let password = dependencies.appSession.activePassword else {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "storage.detail.overview.placeholder.disconnected")
            )
            return
        }
        let started = dependencies.remoteMaintenanceController.startFullVerify(
            profile: profile,
            password: password
        )
        if !started {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
        }
    }

    private func showIncompleteAssets(entries: [IncompleteAssetEntry]) {
        let viewController = RemoteIncompleteAssetsViewController(entries: entries)
        navigationController?.pushViewController(viewController, animated: true)
    }

    // MARK: - Edit / Delete

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

    /// Editor can repoint the same id at a different remote (host/basePath/share); the prior verify timestamp would attribute to the new endpoint.
    private func handleConnectionEdited() {
        if dependencies.appSession.activeProfile?.id == profile.id {
            try? dependencies.databaseManager.setActiveServerProfileID(nil)
            dependencies.appSession.clear()
        }
        if let profileID = profile.id {
            try? dependencies.databaseManager.clearRemoteVerifiedAt(profileID: profileID)
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
            dismissAfterDelete()
        } catch {
            presentAlert(
                title: String(localized: "auth.manage.deleteFailed"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    /// `popViewController` is a no-op when detail is the nav root (modal-sheet
    /// entry path), which would leave a stale page on a deleted profile.
    private func dismissAfterDelete() {
        if let nav = navigationController, nav.viewControllers.first === self {
            (nav.presentingViewController ?? nav).dismiss(animated: true)
        } else {
            navigationController?.popViewController(animated: true)
        }
    }

    /// Live guard: render-time `isEnabled` doesn't cover verify/execution starting
    /// between render and tap. Revert the UI so the switch stays in sync with the model.
    @objc private func backgroundBackupToggleChanged(_ sender: UISwitch) {
        guard let profileID = profile.id else { return }
        if isProfileMutationBlocked {
            sender.setOn(profile.backgroundBackupEnabled, animated: true)
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }
        try? dependencies.databaseManager.setBackgroundBackupEnabled(sender.isOn, profileID: profileID)
        profile.backgroundBackupEnabled = sender.isOn
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    // MARK: - Formatters

    private static let numberFormatter: NumberFormatter = {
        let f = NumberFormatter()
        f.numberStyle = .decimal
        return f
    }()

    private static func formatDate(_ date: Date?) -> String {
        guard let date else { return String(localized: "storage.detail.overview.notAvailable") }
        return date.formatted(date: .abbreviated, time: .shortened)
    }

    private static func formatCount(_ value: Int) -> String {
        numberFormatter.string(from: NSNumber(value: value)) ?? String(value)
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

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch sectionLayouts[section].id {
        case .remoteOverview:
            return String(localized: "storage.detail.overview.title")
        case .editConnection, .backgroundBackup, .delete:
            return nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let row = sectionLayouts[indexPath.section].rows[indexPath.row]
        return row.cellBuilder(tableView, indexPath)
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        sectionLayouts[indexPath.section].rows[indexPath.row].onTap?()
    }
}
