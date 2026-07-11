import Foundation
import SnapKit
import UIKit

final class StorageProfileDetailViewController: UIViewController {
    private enum SectionID {
        case name
        case editConnection
        case backgroundBackup
        case remoteThumbnails
        case remoteOverview
        case leftoverCleanup
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
    private var connectionObserver: NSObjectProtocol?

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
        if let connectionObserver {
            NotificationCenter.default.removeObserver(connectionObserver)
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
        connectionObserver = NotificationCenter.default.addObserver(
            forName: .ConnectionLifecycleDidChange,
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
            id: .name,
            rows: [makeNameRow()],
            footer: nil
        ))
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
            id: .remoteThumbnails,
            rows: [makeRemoteThumbnailsRow()],
            footer: nil
        ))

        sections.append(SectionLayout(
            id: .remoteOverview,
            rows: makeRemoteOverviewRows(),
            footer: nil
        ))

        if dependencies.appSession.activeProfile?.id == profile.id {
            sections.append(SectionLayout(
                id: .leftoverCleanup,
                rows: [makeLeftoverCleanupRow()],
                footer: String(localized: "storage.detail.leftover.footer")
            ))
        }

        sections.append(SectionLayout(
            id: .delete,
            rows: [makeDeleteRow()],
            footer: nil
        ))

        sectionLayouts = sections
    }

    // MARK: - Rows

    private func makeNameRow() -> RowSpec {
        RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "auth.section.name")
                content.secondaryText = self?.profile.name
                cell.contentConfiguration = content
                cell.accessoryType = .disclosureIndicator
                cell.accessoryView = nil
                cell.selectionStyle = .default
                return cell
            },
            onTap: { [weak self] in
                self?.presentRenamePrompt()
            }
        )
    }

    /// Locked while any remote maintenance op (verify / leftover scan / leftover delete) or execution holds
    /// this profile, to avoid orphaning the in-flight task.
    private var isProfileMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting ||
            dependencies.remoteMaintenanceController.isBusy ||
            dependencies.appRuntimeFlags.isConnecting(profileID: profile.id)
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
        let summary = profile.backgroundBackupSummary
        return RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "more.item.backgroundBackup")
                content.secondaryText = summary
                content.textProperties.color = blocked ? .secondaryLabel : .label
                cell.contentConfiguration = content
                cell.accessoryType = blocked ? .none : .disclosureIndicator
                cell.accessoryView = nil
                cell.selectionStyle = blocked ? .none : .default
                return cell
            },
            onTap: { [weak self] in
                guard let self, !self.rejectIfProfileMutationBlocked() else { return }
                let vc = BackgroundBackupNodeDetailViewController(dependencies: self.dependencies, profile: self.profile)
                self.navigationController?.pushViewController(vc, animated: true)
            }
        )
    }

    private func makeRemoteThumbnailsRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        let summary = profile.generateRemoteThumbnails
            ? String(localized: "remoteThumbnails.state.on")
            : String(localized: "remoteThumbnails.state.off")
        return RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "remoteThumbnails.title")
                content.secondaryText = summary
                content.textProperties.color = blocked ? .secondaryLabel : .label
                cell.contentConfiguration = content
                cell.accessoryType = blocked ? .none : .disclosureIndicator
                cell.accessoryView = nil
                cell.selectionStyle = blocked ? .none : .default
                return cell
            },
            onTap: { [weak self] in
                guard let self, !self.rejectIfProfileMutationBlocked() else { return }
                let vc = RemoteThumbnailSettingsViewController(dependencies: self.dependencies, profile: self.profile)
                self.navigationController?.pushViewController(vc, animated: true)
            }
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
        presentMutationBlockedAlert()
        return true
    }

    private func presentMutationBlockedAlert() {
        presentAlert(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress")
        )
    }

    // MARK: - Remote overview

    private func makeRemoteOverviewRows() -> [RowSpec] {
        let isActive = dependencies.appSession.activeProfile?.id == profile.id
        guard isActive else {
            return [makeDisconnectedPlaceholderRow()]
        }

        // Verify runs inline here; leftover scan/delete run in their own modal, so only verify/execution
        // collapse the overview to a progress placeholder.
        if dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isVerifying {
            return [makeBusyPlaceholderRow()]
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

    private func makeLeftoverCleanupRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        return RowSpec(
            reuseID: actionCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.actionCellID ?? "ActionCell", for: indexPath)
                var content = cell.defaultContentConfiguration()
                content.text = String(localized: "storage.detail.overview.checkLeftover")
                content.textProperties.color = blocked ? .secondaryLabel : .systemBlue
                content.textProperties.alignment = .center
                cell.contentConfiguration = content
                cell.accessoryType = .none
                cell.accessoryView = nil
                cell.selectionStyle = blocked ? .none : .default
                return cell
            },
            onTap: { [weak self] in
                guard let self, !self.rejectIfProfileMutationBlocked() else { return }
                self.presentLeftoverCleanup()
            }
        )
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

    // total == 0 during the pre-loop reload phase — show the "starting" copy instead of "0/0".
    private func busyPlaceholderTitle() -> String {
        let controller = dependencies.remoteMaintenanceController
        if controller.isVerifying {
            if let p = controller.currentProgress, p.total > 0 {
                return String.localizedStringWithFormat(
                    String(localized: "storage.detail.overview.placeholder.verifying"), p.current, p.total
                )
            }
            return String(localized: "storage.detail.overview.placeholder.verifyingStarting")
        }
        return String(localized: "storage.detail.overview.placeholder.executing")
    }

    private func makeBusyPlaceholderRow() -> RowSpec {
        let title = busyPlaceholderTitle()
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

    // MARK: - Leftover cleanup

    private func presentLeftoverCleanup() {
        let viewController = LeftoverCleanupViewController(dependencies: dependencies, profile: profile)
        let nav = UINavigationController(rootViewController: viewController)
        present(nav, animated: true)
    }

    // MARK: - Edit / Delete

    private func presentRenamePrompt() {
        let alert = UIAlertController(
            title: String(localized: "auth.section.name"),
            message: nil,
            preferredStyle: .alert
        )
        let saveAction = UIAlertAction(title: String(localized: "common.save"), style: .default) { [weak self, weak alert] _ in
            guard let self,
                  let value = alert?.textFields?.first?.text?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !value.isEmpty else { return }
            self.renameProfile(to: value)
        }
        alert.addTextField { [profileName = profile.name, weak saveAction] textField in
            textField.text = profileName
            textField.clearButtonMode = .whileEditing
            textField.autocapitalizationType = .words
            textField.addAction(UIAction { [weak saveAction] action in
                guard let textField = action.sender as? UITextField else { return }
                saveAction?.isEnabled = !(textField.text ?? "")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .isEmpty
            }, for: .editingChanged)
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(saveAction)
        present(alert, animated: true)
    }

    private func renameProfile(to name: String) {
        guard let profileID = profile.id, name != profile.name else { return }
        guard !rejectIfProfileMutationBlocked() else { return }
        do {
            guard let _ = try dependencies.appRuntimeFlags.withProfileMutationLease(
                profileID: profileID,
                { try dependencies.databaseManager.setServerProfileName(name, profileID: profileID) }
            ) else {
                presentMutationBlockedAlert()
                return
            }
            profile.name = name
            dependencies.appSession.setActiveName(name, profileID: profileID)
            title = profile.storageProfile.displayTitle
            rebuildSectionLayouts()
            tableView.reloadData()
            onProfilesChanged()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    private func editConnectionParameters() {
        switch profile.resolvedStorageType {
        case .smb:
            openSMBEditor()
        case .webdav:
            openWebDAVEditor()
        case .externalVolume:
            openExternalEditor()
        case .s3:
            openS3Editor()
        case .sftp:
            openSFTPEditor()
        }
    }

    private func openSMBEditor() {
        let draft = SMBServerLoginDraft(
            name: profile.name,
            host: RemoteHostIdentity.canonicalSMB(profile.host),
            port: SMBEndpoint.effectivePort(profile.port),
            username: profile.username,
            domain: profile.domain
        )
        let editor = AddSMBServerLoginViewController(
            dependencies: dependencies,
            draft: draft,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] savedProfile, password in
            self?.handleConnectionEdited(savedProfile: savedProfile, password: password)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openWebDAVEditor() {
        let editor = AddWebDAVStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] savedProfile, password in
            self?.handleConnectionEdited(savedProfile: savedProfile, password: password)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openExternalEditor() {
        let editor = AddExternalStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] savedProfile, password in
            self?.handleConnectionEdited(savedProfile: savedProfile, password: password)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openS3Editor() {
        let editor = AddS3StorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] savedProfile, password in
            self?.handleConnectionEdited(savedProfile: savedProfile, password: password)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openSFTPEditor() {
        let editor = AddSFTPStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] savedProfile, password in
            self?.handleConnectionEdited(savedProfile: savedProfile, password: password)
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func handleConnectionEdited(savedProfile: ServerProfileRecord, password: String) {
        let destinationChanged = !profile.hasSameRemoteDestination(as: savedProfile)
        if dependencies.appSession.activeProfile?.id == profile.id {
            if destinationChanged {
                dependencies.appSession.clear()
            } else {
                dependencies.appSession.activate(profile: savedProfile, password: password)
            }
        }
        profile = savedProfile
        title = profile.storageProfile.displayTitle
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
        guard !rejectIfProfileMutationBlocked() else { return }
        do {
            guard let _ = try dependencies.appRuntimeFlags.withProfileMutationLease(
                profileID: id,
                {
                    try dependencies.databaseManager.deleteServerProfile(id: id)
                    if profile.storageProfile.requiresPassword {
                        StorageProfilePersistence.deleteCredentialIfUnused(
                            dependencies: dependencies,
                            credentialRef: profile.credentialRef
                        )
                    }
                    if dependencies.appSession.activeProfile?.id == id {
                        dependencies.appSession.clear()
                    }
                }
            ) else {
                presentMutationBlockedAlert()
                return
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
        case .name, .editConnection, .backgroundBackup, .remoteThumbnails, .leftoverCleanup, .delete:
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
