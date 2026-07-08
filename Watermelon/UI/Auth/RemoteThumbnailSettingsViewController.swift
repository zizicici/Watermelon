import Foundation
import UIKit

// Per-node settings for the remote thumbnail sidecar feature. Default off, opt-in, editable anytime.
// The enable footer explains principle / benefit / cost. The maintenance section (active node only)
// backfills sidecars for already-backed-up content and purges them to reclaim space.
final class RemoteThumbnailSettingsViewController: UIViewController {
    private let dependencies: DependencyContainer
    private var profile: ServerProfileRecord
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)

    private let toggleCellID = "ToggleCell"
    private let actionCellID = "ActionCell"

    private enum Section {
        case enable
        case maintenance
    }

    private enum MaintenanceRow: Int, CaseIterable {
        case backfill
        case purge
    }

    private var sections: [Section] = [.enable]

    private var maintenanceTask: Task<Void, Never>?
    private var maintenanceCancelFlag: CancelFlag?

    init(dependencies: DependencyContainer, profile: ServerProfileRecord) {
        self.dependencies = dependencies
        self.profile = profile
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        maintenanceCancelFlag?.cancel()
        maintenanceTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "remoteThumbnails.title")
        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: toggleCellID)
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: actionCellID)
        view.addSubview(tableView)
        tableView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.topAnchor),
            tableView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
        ])
        rebuildSections()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        rebuildSections()
        tableView.reloadData()
    }

    private var isActiveProfile: Bool {
        dependencies.appSession.activeProfile?.id == profile.id
    }

    // Maintenance needs a live connection (snapshot in memory + reachable node), so it is offered
    // only for the active node.
    private func rebuildSections() {
        sections = isActiveProfile ? [.enable, .maintenance] : [.enable]
    }

    private var isMutationBlocked: Bool {
        dependencies.appRuntimeFlags.isExecuting || dependencies.remoteMaintenanceController.isBusy
    }

    @discardableResult
    private func rejectIfBlocked() -> Bool {
        guard isMutationBlocked else { return false }
        presentSimpleAlert(
            title: String(localized: "common.error"),
            message: String(localized: "home.alert.maintenanceInProgress")
        )
        return true
    }

    private func presentSimpleAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    // MARK: - Enable

    private func setEnabled(_ enabled: Bool, sender: UISwitch) {
        guard let profileID = profile.id else { return }
        if isMutationBlocked {
            sender.setOn(!enabled, animated: true)
            presentSimpleAlert(
                title: String(localized: "common.error"),
                message: String(localized: "home.alert.maintenanceInProgress")
            )
            return
        }
        do {
            try dependencies.databaseManager.setGenerateRemoteThumbnails(enabled, profileID: profileID)
            dependencies.appSession.setActiveGenerateRemoteThumbnails(enabled, profileID: profileID)
            profile.generateRemoteThumbnails = enabled
            // Turning it on only affects future uploads — nudge the user to backfill existing content.
            if enabled, isActiveProfile {
                promptBackfillAfterEnable()
            }
        } catch {
            sender.setOn(!enabled, animated: true)
            presentSimpleAlert(
                title: String(localized: "common.error"),
                message: UserFacingErrorLocalizer.message(for: error)
            )
        }
    }

    private func promptBackfillAfterEnable() {
        let alert = UIAlertController(
            title: String(localized: "remoteThumbnails.backfill.promptTitle"),
            message: String(localized: "remoteThumbnails.backfill.promptMessage"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.notNow"), style: .cancel))
        alert.addAction(UIAlertAction(title: String(localized: "remoteThumbnails.maintenance.backfill"), style: .default) { [weak self] _ in
            self?.runBackfill()
        })
        present(alert, animated: true)
    }

    // MARK: - Maintenance

    private func makeMaintenanceService(password: String) -> RemoteThumbnailService {
        // Backfill only needs the fingerprint→localIdentifier map; a private index is sufficient.
        let presenceIndex = LibraryPresenceIndex(
            hashIndexRepository: dependencies.hashIndexRepository,
            coordinator: dependencies.backupCoordinator,
            profileKey: { [profile] in RemoteIndexSyncService.remoteProfileKey(profile) }
        )
        return RemoteThumbnailService(
            storageClientFactory: dependencies.storageClientFactory,
            presenceIndex: presenceIndex,
            profile: profile,
            password: password
        )
    }

    private func runBackfill() {
        guard !rejectIfBlocked() else { return }
        guard isActiveProfile, let password = dependencies.appSession.activePassword else {
            presentSimpleAlert(
                title: String(localized: "common.error"),
                message: String(localized: "remoteThumbnails.needConnection")
            )
            return
        }
        // Claim the app-wide execution mutex for the whole backfill so a backup can't run concurrently
        // (rejectIfBlocked only *checks* — it never claimed). `flags` is captured strongly below so the
        // Task's defer releases it even if this VC is torn down mid-run.
        guard dependencies.appRuntimeFlags.tryEnterExecution() else {
            presentSimpleAlert(title: String(localized: "common.error"), message: String(localized: "home.alert.maintenanceInProgress"))
            return
        }
        let flags = dependencies.appRuntimeFlags

        let service = makeMaintenanceService(password: password)
        let cancelFlag = CancelFlag()
        maintenanceCancelFlag = cancelFlag
        let coordinator = dependencies.backupCoordinator
        let expectedKey = RemoteIndexSyncService.remoteProfileKey(profile)

        maintenanceTask = Task { [weak self] in
            defer { flags.exitExecution() }
            // Build the fingerprint list off the main thread — the snapshot can be very large — and
            // decide empty-vs-work BEFORE presenting any alert (avoids a present-then-dismiss flash). Gate on
            // the snapshot belonging to THIS settings profile: a profile-switch/reconnect window could
            // otherwise backfill another profile's fingerprints as orphan thumbnails.
            let fingerprints = await withCancellableDetachedValue {
                let state = coordinator.currentRemoteSnapshotState(since: nil)
                guard state.profileKey == nil || state.profileKey == expectedKey else { return [Data]() }
                return state.monthDeltas.flatMap { $0.assets.map(\.assetFingerprint) }
            }
            guard let self, !cancelFlag.isCancelled else {
                await service.shutdown()
                return
            }
            guard !fingerprints.isEmpty else {
                await service.shutdown()
                self.presentSimpleAlert(
                    title: String(localized: "remoteThumbnails.backfill.doneTitle"),
                    message: String(localized: "remoteThumbnails.backfill.empty")
                )
                return
            }

            let alert = UIAlertController(
                title: String(localized: "remoteThumbnails.backfill.progressTitle"),
                message: self.progressText(done: 0, total: fingerprints.count),
                preferredStyle: .alert
            )
            alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel) { [weak self] _ in
                cancelFlag.cancel()
                self?.maintenanceTask?.cancel()
            })
            self.present(alert, animated: true)

            let result = await service.backfillSidecars(
                fingerprints: fingerprints,
                progress: { done, total in
                    // Throttle: per-item main-actor updates would flood a large run.
                    guard done == total || done % 25 == 0 else { return }
                    alert.message = String.localizedStringWithFormat(
                        String(localized: "remoteThumbnails.backfill.progressFormat"), done, total
                    )
                },
                isCancelled: { cancelFlag.isCancelled }
            )
            await service.shutdown()
            alert.dismiss(animated: true) {
                // Upload/connection failures are not benign skips — report them so the user knows the run
                // must be repeated instead of believing every sidecar landed.
                if result.failed > 0 {
                    self.presentSimpleAlert(
                        title: String(localized: "common.error"),
                        message: String.localizedStringWithFormat(
                            String(localized: "remoteThumbnails.backfill.failedMessage"),
                            result.generated,
                            result.skipped,
                            result.failed
                        )
                    )
                } else {
                    self.presentSimpleAlert(
                        title: String(localized: "remoteThumbnails.backfill.doneTitle"),
                        message: String.localizedStringWithFormat(
                            String(localized: "remoteThumbnails.backfill.doneMessage"),
                            result.generated,
                            result.skipped
                        )
                    )
                }
            }
        }
    }

    private func runPurge() {
        guard !rejectIfBlocked() else { return }
        guard isActiveProfile, let password = dependencies.appSession.activePassword else {
            presentSimpleAlert(
                title: String(localized: "common.error"),
                message: String(localized: "remoteThumbnails.needConnection")
            )
            return
        }
        let confirm = UIAlertController(
            title: String(localized: "remoteThumbnails.purge.confirmTitle"),
            message: String(localized: "remoteThumbnails.purge.confirmMessage"),
            preferredStyle: .alert
        )
        confirm.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        confirm.addAction(UIAlertAction(title: String(localized: "common.delete"), style: .destructive) { [weak self] _ in
            self?.performPurge(password: password)
        })
        present(confirm, animated: true)
    }

    private func performPurge(password: String) {
        // Claim the app-wide execution mutex (released by the Task's defer, teardown-safe via strong `flags`).
        guard dependencies.appRuntimeFlags.tryEnterExecution() else {
            presentSimpleAlert(title: String(localized: "common.error"), message: String(localized: "home.alert.maintenanceInProgress"))
            return
        }
        let flags = dependencies.appRuntimeFlags
        let service = makeMaintenanceService(password: password)
        let progress = UIAlertController(
            title: String(localized: "remoteThumbnails.purge.progressTitle"),
            message: nil,
            preferredStyle: .alert
        )
        present(progress, animated: true)
        maintenanceTask = Task { [weak self] in
            defer { flags.exitExecution() }
            let ok = await service.purgeRemoteThumbnails()
            await service.shutdown()
            guard let self else { return }
            progress.dismiss(animated: true) {
                self.presentSimpleAlert(
                    title: ok
                        ? String(localized: "remoteThumbnails.purge.doneTitle")
                        : String(localized: "common.error"),
                    message: ok
                        ? String(localized: "remoteThumbnails.purge.doneMessage")
                        : String(localized: "remoteThumbnails.purge.failedMessage")
                )
            }
        }
    }

    private func progressText(done: Int, total: Int) -> String {
        String.localizedStringWithFormat(String(localized: "remoteThumbnails.backfill.progressFormat"), done, total)
    }
}

// Thread-safe cancellation flag passed into the off-main backfill loop.
private final class CancelFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false
    var isCancelled: Bool { lock.withLock { cancelled } }
    func cancel() { lock.withLock { cancelled = true } }
}

extension RemoteThumbnailSettingsViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        sections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        switch sections[section] {
        case .enable: return 1
        case .maintenance: return MaintenanceRow.allCases.count
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        switch sections[section] {
        case .enable: return nil
        case .maintenance: return String(localized: "remoteThumbnails.maintenance.header")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        switch sections[section] {
        case .enable: return String(localized: "remoteThumbnails.enable.footer")
        case .maintenance: return String(localized: "remoteThumbnails.maintenance.footer")
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        switch sections[indexPath.section] {
        case .enable:
            let cell = tableView.dequeueReusableCell(withIdentifier: toggleCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            content.text = String(localized: "remoteThumbnails.enable.label")
            cell.contentConfiguration = content
            let toggle = UISwitch()
            toggle.isOn = profile.generateRemoteThumbnails
            toggle.addAction(UIAction { [weak self, weak toggle] _ in
                guard let self, let toggle else { return }
                self.setEnabled(toggle.isOn, sender: toggle)
            }, for: .valueChanged)
            cell.accessoryView = toggle
            cell.selectionStyle = .none
            return cell

        case .maintenance:
            let cell = tableView.dequeueReusableCell(withIdentifier: actionCellID, for: indexPath)
            var content = cell.defaultContentConfiguration()
            let row = MaintenanceRow(rawValue: indexPath.row)!
            switch row {
            case .backfill:
                content.text = String(localized: "remoteThumbnails.maintenance.backfill")
                content.textProperties.color = .systemBlue
            case .purge:
                content.text = String(localized: "remoteThumbnails.maintenance.purge")
                content.textProperties.color = .systemRed
            }
            cell.contentConfiguration = content
            cell.accessoryType = .none
            cell.accessoryView = nil
            cell.selectionStyle = .default
            return cell
        }
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard case .maintenance = sections[indexPath.section] else { return }
        switch MaintenanceRow(rawValue: indexPath.row)! {
        case .backfill: runBackfill()
        case .purge: runPurge()
        }
    }
}
