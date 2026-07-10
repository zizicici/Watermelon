import Foundation
import SnapKit
import UIKit

final class StorageProfileDetailViewController: UIViewController {
    private enum SectionID {
        case editConnection
        case backgroundBackup
        case remoteThumbnails
        case resourceEncryption
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
            id: .remoteThumbnails,
            rows: [makeRemoteThumbnailsRow()],
            footer: nil
        ))

        let resourceEncryptionFooter = profile.defaultResourceStorageIsEncrypted
            ? String(localized: "storage.detail.resourceEncryption.footer.enabled", defaultValue: "New uploads to this storage are encrypted. Existing plaintext files may remain mixed with encrypted files.")
            : String(localized: "storage.detail.resourceEncryption.footer.disabled", defaultValue: "Encrypts future uploaded file contents and hides original filenames. Existing files are not migrated. This cannot be turned off for this repository.")
        sections.append(SectionLayout(
            id: .resourceEncryption,
            rows: [makeResourceEncryptionRow()],
            footer: resourceEncryptionFooter
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

    /// Locked while any remote maintenance op (verify / leftover scan / leftover delete) or execution holds
    /// this profile, to avoid orphaning the in-flight task.
    private var isProfileMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isBusy
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

    private func makeResourceEncryptionRow() -> RowSpec {
        let blocked = isProfileMutationBlocked
        let encrypted = profile.defaultResourceStorageIsEncrypted
        let summary = encrypted
            ? String(localized: "storage.detail.resourceEncryption.state.on", defaultValue: "On")
            : String(localized: "storage.detail.resourceEncryption.state.off", defaultValue: "Off")
        return RowSpec(
            reuseID: valueCellID,
            cellBuilder: { [weak self] tv, indexPath in
                let cell = tv.dequeueReusableCell(withIdentifier: self?.valueCellID ?? "ValueCell", for: indexPath)
                var content = UIListContentConfiguration.valueCell()
                content.text = String(localized: "storage.detail.resourceEncryption.title", defaultValue: "File Encryption")
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
                if encrypted {
                    self.presentResourceEncryptionRecoveryOptions()
                } else {
                    self.presentResourceEncryptionStartOptions()
                }
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

    private func presentResourceEncryptionStartOptions() {
        guard let profileID = profile.id else { return }
        guard dependencies.appSession.activeProfile?.id == profileID,
              let password = dependencies.appSession.activePassword else {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "storage.detail.resourceEncryption.needConnection", defaultValue: "Connect to this storage before enabling file encryption.")
            )
            return
        }
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.title", defaultValue: "File Encryption"),
            message: String(localized: "storage.detail.resourceEncryption.start.message", defaultValue: "Enable encryption for this repository, or import a recovery key for an encrypted repository already created on another device."),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.import.title", defaultValue: "Import Recovery Key"),
            style: .default
        ) { [weak self] _ in
            self?.promptImportResourceEncryptionRecoveryKey(profileID: profileID, password: password)
        })
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.confirm.enable", defaultValue: "Enable"),
            style: .default
        ) { [weak self] _ in
            self?.confirmEnableResourceEncryption(profileID: profileID, password: password)
        })
        present(alert, animated: true)
    }

    private func presentResourceEncryptionRecoveryOptions() {
        guard let profileID = profile.id else { return }
        guard dependencies.appSession.activeProfile?.id == profileID,
              let password = dependencies.appSession.activePassword else {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "storage.detail.resourceEncryption.needConnection", defaultValue: "Connect to this storage before enabling file encryption.")
            )
            return
        }
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.title", defaultValue: "File Encryption"),
            message: String(localized: "storage.detail.resourceEncryption.recovery.message", defaultValue: "Use the recovery key to restore access on another device, or import it here after reinstalling the app."),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.import.title", defaultValue: "Import Recovery Key"),
            style: .default
        ) { [weak self] _ in
            self?.promptImportResourceEncryptionRecoveryKey(profileID: profileID, password: password)
        })
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.showRecoveryKey", defaultValue: "Show Recovery Key"),
            style: .default
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.showResourceEncryptionRecoveryKey(profileID: profileID, password: password) }
        })
        present(alert, animated: true)
    }

    private func confirmEnableResourceEncryption(profileID: Int64, password: String) {
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.confirm.title", defaultValue: "Enable File Encryption?"),
            message: String(localized: "storage.detail.resourceEncryption.confirm.message", defaultValue: "Future uploads will encrypt file contents and hide original filenames. Existing files are not migrated, and this repository cannot be turned back into a plaintext repository. Keep the recovery key shown after setup."),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.confirm.enable", defaultValue: "Enable"),
            style: .default
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.enableResourceEncryption(profileID: profileID, password: password) }
        })
        present(alert, animated: true)
    }

    private func promptImportResourceEncryptionRecoveryKey(profileID: Int64, password: String) {
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.import.title", defaultValue: "Import Recovery Key"),
            message: String(localized: "storage.detail.resourceEncryption.import.message", defaultValue: "Paste the recovery key for this encrypted repository."),
            preferredStyle: .alert
        )
        alert.addTextField { field in
            field.placeholder = "WMENC1..."
            field.autocapitalizationType = .none
            field.autocorrectionType = .no
            field.clearButtonMode = .whileEditing
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default) { [weak self, weak alert] _ in
            guard let self,
                  let recoveryKey = alert?.textFields?.first?.text,
                  !recoveryKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
            Task {
                await self.importResourceEncryptionRecoveryKey(
                    profileID: profileID,
                    password: password,
                    recoveryKey: recoveryKey
                )
            }
        })
        present(alert, animated: true)
    }

    private func showResourceEncryptionRecoveryKey(profileID: Int64, password: String) async {
        let progress = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.recovery.loadingTitle", defaultValue: "Loading Recovery Key..."),
            message: nil,
            preferredStyle: .alert
        )
        present(progress, animated: true)
        let result: Result<String, Error>
        do {
            result = .success(try await performShowResourceEncryptionRecoveryKey(profileID: profileID, password: password))
        } catch {
            result = .failure(error)
        }
        progress.dismiss(animated: true) { [weak self] in
            guard let self else { return }
            switch result {
            case .success(let recoveryKey):
                self.presentRecoveryKey(recoveryKey)
            case .failure(let error):
                self.presentAlert(
                    title: String(localized: "common.error"),
                    message: UserFacingErrorLocalizer.message(for: error, profile: self.profile)
                )
            }
        }
    }

    private func importResourceEncryptionRecoveryKey(
        profileID: Int64,
        password: String,
        recoveryKey: String
    ) async {
        guard dependencies.appRuntimeFlags.tryEnterExecution() else {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }
        let flags = dependencies.appRuntimeFlags
        let progress = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.import.progressTitle", defaultValue: "Importing Recovery Key..."),
            message: nil,
            preferredStyle: .alert
        )
        present(progress, animated: true)
        let result: Result<RepoEncryptionSetupResult, Error>
        do {
            result = .success(try await performImportResourceEncryptionRecoveryKey(
                profileID: profileID,
                password: password,
                recoveryKey: recoveryKey
            ))
        } catch {
            result = .failure(error)
        }
        flags.exitExecution()
        progress.dismiss(animated: true) { [weak self] in
            guard let self else { return }
            switch result {
            case .success:
                self.presentAlert(
                    title: String(localized: "storage.detail.resourceEncryption.import.successTitle", defaultValue: "Recovery Key Imported"),
                    message: String(localized: "storage.detail.resourceEncryption.import.successMessage", defaultValue: "This device can now decrypt files in the encrypted repository.")
                )
            case .failure(let error):
                self.presentAlert(
                    title: String(localized: "common.error"),
                    message: UserFacingErrorLocalizer.message(for: error, profile: self.profile)
                )
            }
        }
    }

    private func enableResourceEncryption(profileID: Int64, password: String) async {
        guard dependencies.appRuntimeFlags.tryEnterExecution() else {
            presentAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }
        let flags = dependencies.appRuntimeFlags
        let progress = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.progress.title", defaultValue: "Enabling Encryption..."),
            message: String(localized: "storage.detail.resourceEncryption.progress.message", defaultValue: "Updating the repository format and saving the local encryption key."),
            preferredStyle: .alert
        )
        present(progress, animated: true)

        let result: Result<RepoEncryptionSetupResult, Error>
        do {
            result = .success(try await performEnableResourceEncryption(profileID: profileID, password: password))
        } catch {
            result = .failure(error)
        }
        flags.exitExecution()

        progress.dismiss(animated: true) { [weak self] in
            guard let self else { return }
            switch result {
            case .success(let setupResult):
                self.presentResourceEncryptionSuccess(setupResult, profileID: profileID)
            case .failure(let error):
                self.presentAlert(
                    title: String(localized: "common.error"),
                    message: UserFacingErrorLocalizer.message(for: error, profile: self.profile)
                )
            }
        }
    }

    private func performShowResourceEncryptionRecoveryKey(
        profileID: Int64,
        password: String
    ) async throws -> String {
        let resolvedProfile = try dependencies.databaseManager.profileWithBackfilledWriterID(profile)
        guard resolvedProfile.id == profileID else { throw LiteRepoError.writerIdentityUnavailable }
        return try await withConnectedStorageClient(profile: resolvedProfile, password: password) { client in
            let context = try await RepoEncryptionSetupService(
                keyStore: RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
            ).loadExistingContext(client: client, basePath: resolvedProfile.basePath)
            let material = try RepoEncryptionKeyMaterial(
                repoID: context.repoID,
                keyID: context.activeKeyID,
                keyData: context.contentKey
            )
            return RepoEncryptionKeyCodec.recoveryKeyString(for: material)
        }
    }

    private func performImportResourceEncryptionRecoveryKey(
        profileID: Int64,
        password: String,
        recoveryKey: String
    ) async throws -> RepoEncryptionSetupResult {
        let resolvedProfile = try dependencies.databaseManager.profileWithBackfilledWriterID(profile)
        guard resolvedProfile.id == profileID else { throw LiteRepoError.writerIdentityUnavailable }
        let keyStore = RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
        let result = try await withConnectedStorageClient(profile: resolvedProfile, password: password) { client in
            try await RepoEncryptionSetupService(
                keyStore: keyStore
            ).importRecoveryKey(
                client: client,
                basePath: resolvedProfile.basePath,
                recoveryKey: recoveryKey
            )
        }
        try keyStore.saveProfileKeyReference(profileID: profileID, material: result.keyMaterial)
        try markResourceEncryptionEnabled(profileID: profileID)
        return result
    }

    private func performEnableResourceEncryption(
        profileID: Int64,
        password: String
    ) async throws -> RepoEncryptionSetupResult {
        let resolvedProfile = try dependencies.databaseManager.profileWithBackfilledWriterID(profile)
        guard resolvedProfile.id == profileID else { throw LiteRepoError.writerIdentityUnavailable }
        let client = try dependencies.storageClientFactory.makeClient(profile: resolvedProfile, password: password)
        do {
            try await client.connect()

            let storageClientFactory = dependencies.storageClientFactory
            let databaseManager = dependencies.databaseManager
            let makeLockClient: ConnectedLockClientProvider = { [storageClientFactory, resolvedProfile, password] in
                let fresh = try storageClientFactory.makeClient(profile: resolvedProfile, password: password)
                try await fresh.connect()
                return LiteLockClientHandle(client: fresh)
            }
            let keyStore = RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
            if let recovered = try await recoverLocallyConfirmedResourceEncryptionIfNeeded(
                profileID: profileID,
                profile: resolvedProfile,
                client: client,
                keyStore: keyStore
            ) {
                await client.disconnectSafely()
                return recovered
            }

            let lockClient = try dependencies.storageClientFactory.makeClient(profile: resolvedProfile, password: password)
            do {
                try await lockClient.connect()
                return try await performEnableResourceEncryptionUnderLock(
                    profileID: profileID,
                    resolvedProfile: resolvedProfile,
                    client: client,
                    lockClient: lockClient,
                    makeLockClient: makeLockClient,
                    keyStore: keyStore,
                    databaseManager: databaseManager
                )
            } catch {
                await lockClient.disconnectSafely()
                throw error
            }
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    private func performEnableResourceEncryptionUnderLock(
        profileID: Int64,
        resolvedProfile: ServerProfileRecord,
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        makeLockClient: @escaping ConnectedLockClientProvider,
        keyStore: RepoEncryptionKeychainStore,
        databaseManager: DatabaseManager
    ) async throws -> RepoEncryptionSetupResult {
        do {
            let formatGate: LiteRepoGateway.WriteFormatGate = { probe in
                try await RepoEncryptionWriteGate.validate(
                    profile: resolvedProfile,
                    probe: probe,
                    client: client,
                    keyStore: keyStore
                )
            }
            let plan = try await LiteRepoGateway.prepareEncryptionSetupWrite(
                client: client,
                lockClient: lockClient,
                ownsLockClient: true,
                basePath: resolvedProfile.basePath,
                writerID: resolvedProfile.writerID,
                reconnectLockClient: makeLockClient,
                formatGate: formatGate,
                onForeignWriterObserved: MultiDeviceMarkerFactory.make(
                    for: resolvedProfile,
                    databaseManager: databaseManager
                )
            )
            do {
                let setupResult = try await RepoEncryptionSetupService(
                    keyStore: keyStore
                ).enableEncryption(
                    client: client,
                    basePath: resolvedProfile.basePath,
                    createdAt: Self.isoTimestamp(Date()),
                    createdBy: resolvedProfile.writerID ?? "Watermelon iOS \(dependencies.appVersion)",
                    assertOwnership: { try await plan.session.assertLeaseProvenForWrite() }
                )
                try keyStore.saveProfileKeyReference(profileID: profileID, material: setupResult.keyMaterial)
                try markResourceEncryptionEnabled(profileID: profileID, fallback: resolvedProfile)
                await plan.session.stopAndRelease()
                await client.disconnectSafely()
                return setupResult
            } catch {
                await plan.session.stopAndRelease()
                throw error
            }
        } catch {
            throw error
        }
    }

    private func presentResourceEncryptionSuccess(_ result: RepoEncryptionSetupResult, profileID: Int64) {
        let message = resourceEncryptionRecoveryMessage(result.recoveryKey)
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.success.title", defaultValue: "Encryption Enabled"),
            message: message,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(
            title: String(localized: "storage.detail.resourceEncryption.success.savedAction", defaultValue: "I Saved It"),
            style: .default
        ))
        present(alert, animated: true)
    }

    private func recoverLocallyConfirmedResourceEncryptionIfNeeded(
        profileID: Int64,
        profile resolvedProfile: ServerProfileRecord,
        client: any RemoteStorageClientProtocol,
        keyStore: RepoEncryptionKeychainStore
    ) async throws -> RepoEncryptionSetupResult? {
        guard !resolvedProfile.defaultResourceStorageIsEncrypted else { return nil }
        let service = RepoEncryptionSetupService(keyStore: keyStore)
        let result: RepoEncryptionSetupResult
        do {
            let material = try keyStore.readProfileKey(profileID: profileID)
            result = try await service.importRecoveryKey(
                client: client,
                basePath: resolvedProfile.basePath,
                recoveryKey: RepoEncryptionKeyCodec.recoveryKeyString(for: material)
            )
        } catch {
            do {
                result = try await service.verifyExistingEncryptedRepo(
                    client: client,
                    basePath: resolvedProfile.basePath
                )
                try keyStore.saveProfileKeyReference(profileID: profileID, material: result.keyMaterial)
            } catch {
                return nil
            }
        }
        try markResourceEncryptionEnabled(profileID: profileID, fallback: resolvedProfile)
        return result
    }

    private func presentRecoveryKey(_ recoveryKey: String) {
        let alert = UIAlertController(
            title: String(localized: "storage.detail.resourceEncryption.recovery.title", defaultValue: "Recovery Key"),
            message: resourceEncryptionRecoveryMessage(recoveryKey),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    private func resourceEncryptionRecoveryMessage(_ recoveryKey: String) -> String {
        String.localizedStringWithFormat(
            String(localized: "storage.detail.resourceEncryption.success.message", defaultValue: "Save this recovery key somewhere safe:\n\n%@"),
            recoveryKey
        )
    }

    private func markResourceEncryptionEnabled(
        profileID: Int64,
        fallback: ServerProfileRecord? = nil
    ) throws {
        try dependencies.databaseManager.setDefaultResourceStorageCodec(
            RemoteManifestResource.encryptedStorageCodec,
            profileID: profileID
        )
        dependencies.appSession.setActiveDefaultResourceStorageCodec(
            RemoteManifestResource.encryptedStorageCodec,
            profileID: profileID
        )
        if let refreshed = (try? dependencies.databaseManager.fetchServerProfiles())?.first(where: { $0.id == profileID }) {
            profile = refreshed
        } else if var fallback {
            fallback.defaultResourceStorageCodec = RemoteManifestResource.encryptedStorageCodec
            profile = fallback
        } else {
            profile.defaultResourceStorageCodec = RemoteManifestResource.encryptedStorageCodec
        }
        onProfilesChanged()
        rebuildSectionLayouts()
        tableView.reloadData()
    }

    private func withConnectedStorageClient<T>(
        profile: ServerProfileRecord,
        password: String,
        _ body: (any RemoteStorageClientProtocol) async throws -> T
    ) async throws -> T {
        let client = try dependencies.storageClientFactory.makeClient(profile: profile, password: password)
        do {
            try await client.connect()
            let result = try await body(client)
            await client.disconnectSafely()
            return result
        } catch {
            await client.disconnectSafely()
            throw error
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

    private func openS3Editor() {
        let editor = AddS3StorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionEdited()
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func openSFTPEditor() {
        let editor = AddSFTPStorageViewController(
            dependencies: dependencies,
            editingProfile: profile,
            shouldPopToRootOnSave: false
        ) { [weak self] _, _ in
            self?.handleConnectionEdited()
        }
        navigationController?.pushViewController(editor, animated: true)
    }

    private func handleConnectionEdited() {
        let previousProfile = profile
        let refreshedProfile = (try? dependencies.databaseManager.fetchServerProfiles())?.first { $0.id == previousProfile.id }
        let remoteIdentityChanged = refreshedProfile?.shouldResetDefaultResourceStorageCodec(afterEditingFrom: previousProfile) ?? false

        if dependencies.appSession.activeProfile?.id == previousProfile.id {
            try? dependencies.databaseManager.setActiveServerProfileID(nil)
            dependencies.appSession.clear()
        }
        if let profileID = previousProfile.id {
            try? dependencies.databaseManager.clearRemoteVerifiedAt(profileID: profileID)
            try? dependencies.databaseManager.clearBackgroundBackupRunMarkers(profileID: profileID)
            if remoteIdentityChanged {
                try? dependencies.databaseManager.setDefaultResourceStorageCodec(
                    RemoteManifestResource.plaintextStorageCodec,
                    profileID: profileID
                )
                try? RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
                    .deleteProfileKeyReference(profileID: profileID)
                dependencies.appSession.setActiveDefaultResourceStorageCodec(
                    RemoteManifestResource.plaintextStorageCodec,
                    profileID: profileID
                )
            }
        }
        if let refreshed = (try? dependencies.databaseManager.fetchServerProfiles())?.first(where: { $0.id == previousProfile.id }) {
            profile = refreshed
        } else if var refreshedProfile {
            if remoteIdentityChanged {
                refreshedProfile.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
            }
            profile = refreshedProfile
        } else if remoteIdentityChanged {
            profile.defaultResourceStorageCodec = RemoteManifestResource.plaintextStorageCodec
        }
        title = profile.storageProfile.displayTitle
        onProfilesChanged()
        rebuildSectionLayouts()
        tableView.reloadData()
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
            try? RepoEncryptionKeychainStore(keychain: dependencies.keychainService)
                .deleteProfileKey(profileID: id)
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

    private static func isoTimestamp(_ date: Date) -> String {
        ISO8601DateFormatter().string(from: date)
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
        case .editConnection, .backgroundBackup, .remoteThumbnails, .resourceEncryption, .leftoverCleanup, .delete:
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
