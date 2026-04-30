import MoreKit
import SnapKit
import UIKit

@MainActor
final class HomeExecutionLogViewController: UIViewController {
    private enum Section: Hashable { case main }

    private let coordinator: HomeExecutionCoordinator

    private let statusCardView = UIView()
    private let statusTitleLabel = UILabel()
    private let statusLabel = UILabel()
    private let logTableView = UITableView(frame: .zero, style: .plain)
    private let emptyLabel = UILabel()
    private let exportButton = UIButton(type: .system)

    private lazy var filterBarButtonItem = UIBarButtonItem(
        title: String(localized: "log.filter"),
        image: UIImage(systemName: "line.3.horizontal.decrease.circle"),
        primaryAction: nil,
        menu: makeFilterMenu()
    )

    private lazy var focusBackupBarButtonItem = UIBarButtonItem(
        title: String(localized: "log.focusMode.button"),
        style: .plain,
        target: self,
        action: #selector(focusBackupTapped)
    )

    private var logObserverID: UUID?
    private var stateObserverID: UUID?
    private var statusText = String(localized: "home.execution.notStarted")
    private var currentEntries: [ExecutionLogEntry] = []
    private var entriesByID: [UUID: ExecutionLogEntry] = [:]
    private var lastAppliedSignature: (count: Int, lastID: UUID?)?
    private var selectedLevels = ExecutionLogFilterPreference.getValue().enabledLevels
    private var dataSource: UITableViewDiffableDataSource<Section, UUID>!

    init(coordinator: HomeExecutionCoordinator) {
        self.coordinator = coordinator
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        let coordinator = coordinator
        let logID = logObserverID
        let stateID = stateObserverID
        Task { @MainActor in
            if let logID { coordinator.removeLogObserver(logID) }
            if let stateID { coordinator.removeStateObserver(stateID) }
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "log.title")
        navigationItem.leftBarButtonItem = focusBackupBarButtonItem
        navigationItem.rightBarButtonItem = filterBarButtonItem

        buildUI()
        configureDataSource()

        logObserverID = coordinator.addLogObserver { [weak self] snapshot in
            self?.apply(snapshot)
        }
        stateObserverID = coordinator.addStateObserver { [weak self] in
            self?.updateFocusBackupAvailability()
        }
        updateFocusBackupAvailability()
    }

    private func updateFocusBackupAvailability() {
        focusBackupBarButtonItem.isEnabled = coordinator.isRunning
    }

    private func buildUI() {
        statusCardView.backgroundColor = .appPaper
        statusCardView.layer.cornerRadius = 12
        statusCardView.layer.masksToBounds = true

        statusTitleLabel.font = .systemFont(ofSize: 13, weight: .semibold)
        statusTitleLabel.textColor = .secondaryLabel
        statusTitleLabel.text = String(localized: "log.status")

        statusLabel.font = .systemFont(ofSize: 18, weight: .semibold)
        statusLabel.textColor = .label
        statusLabel.numberOfLines = 0
        statusLabel.text = statusText

        logTableView.backgroundColor = .appPaper
        logTableView.layer.cornerRadius = 12
        logTableView.layer.masksToBounds = true
        logTableView.separatorStyle = .none
        logTableView.estimatedRowHeight = 32
        logTableView.rowHeight = UITableView.automaticDimension
        logTableView.keyboardDismissMode = .onDrag
        logTableView.contentInset = UIEdgeInsets(top: 6, left: 0, bottom: 6, right: 0)
        logTableView.register(ExecutionLogEntryCell.self, forCellReuseIdentifier: ExecutionLogEntryCell.reuseIdentifier)

        emptyLabel.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        emptyLabel.textColor = ExecutionLogPalette.secondary
        emptyLabel.textAlignment = .center
        emptyLabel.numberOfLines = 0
        emptyLabel.text = String(localized: "log.empty")
        emptyLabel.isHidden = true

        var exportConfig = UIButton.Configuration.filled()
        exportConfig.title = String(localized: "log.exportButton")
        exportConfig.cornerStyle = .medium
        exportConfig.baseBackgroundColor = .materialPrimary(light: .Material.Green._600, dark: .Material.Green._200)
        exportConfig.baseForegroundColor = .materialOnPrimary(dark: .Material.Green._800)
        exportButton.configuration = exportConfig
        exportButton.addTarget(self, action: #selector(exportTapped), for: .touchUpInside)

        view.addSubview(statusCardView)
        statusCardView.addSubview(statusTitleLabel)
        statusCardView.addSubview(statusLabel)
        view.addSubview(logTableView)
        view.addSubview(emptyLabel)
        view.addSubview(exportButton)

        statusCardView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
        }
        statusTitleLabel.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview().inset(12)
        }
        statusLabel.snp.makeConstraints { make in
            make.top.equalTo(statusTitleLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalToSuperview().inset(12)
        }
        logTableView.snp.makeConstraints { make in
            make.top.equalTo(statusCardView.snp.bottom).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(exportButton.snp.top).offset(-12)
        }
        emptyLabel.snp.makeConstraints { make in
            make.center.equalTo(logTableView)
            make.leading.trailing.equalTo(logTableView).inset(12)
        }
        exportButton.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
            make.height.greaterThanOrEqualTo(44)
        }
    }

    private func configureDataSource() {
        dataSource = UITableViewDiffableDataSource<Section, UUID>(tableView: logTableView) { [weak self] tableView, indexPath, id in
            let cell = tableView.dequeueReusableCell(withIdentifier: ExecutionLogEntryCell.reuseIdentifier, for: indexPath)
            if let cell = cell as? ExecutionLogEntryCell, let entry = self?.entriesByID[id] {
                cell.configure(with: entry)
            }
            return cell
        }
        dataSource.defaultRowAnimation = .none
    }

    private func apply(_ snapshot: HomeExecutionLogSnapshot) {
        statusText = snapshot.statusText
        statusLabel.text = snapshot.statusText

        let signature: (count: Int, lastID: UUID?) = (snapshot.entries.count, snapshot.entries.last?.id)
        if let last = lastAppliedSignature, last == signature {
            updateExportAvailability()
            return
        }

        currentEntries = snapshot.entries
        entriesByID = Dictionary(uniqueKeysWithValues: snapshot.entries.map { ($0.id, $0) })
        lastAppliedSignature = signature

        applyFilteredSnapshot(considerStickyBottom: true)
        updateExportAvailability()
    }

    private func applyFilteredSnapshot(considerStickyBottom: Bool) {
        let ids = currentEntries.compactMap { selectedLevels.contains($0.level) ? $0.id : nil }

        var snap = NSDiffableDataSourceSnapshot<Section, UUID>()
        snap.appendSections([.main])
        snap.appendItems(ids, toSection: .main)

        let stickToBottom = considerStickyBottom && shouldStickToBottom()

        dataSource.apply(snap, animatingDifferences: false) { [weak self] in
            guard let self else { return }
            self.refreshEmptyState(itemCount: ids.count)
            guard stickToBottom, let lastID = ids.last,
                  let indexPath = self.dataSource.indexPath(for: lastID) else { return }
            self.logTableView.scrollToRow(at: indexPath, at: .bottom, animated: false)
        }
    }

    private func refreshEmptyState(itemCount: Int) {
        emptyLabel.isHidden = itemCount > 0
    }

    private func updateExportAvailability() {
        exportButton.isEnabled = coordinator.currentSessionLogURL != nil
    }

    private func shouldStickToBottom() -> Bool {
        let visibleBottom = logTableView.contentOffset.y + logTableView.bounds.height - logTableView.adjustedContentInset.bottom
        let contentBottom = logTableView.contentSize.height
        return visibleBottom >= contentBottom - 80 || dataSource.snapshot().numberOfItems == 0
    }

    private func makeFilterMenu() -> UIMenu {
        let allAction = UIAction(
            title: String(localized: "log.showAll"),
            image: UIImage(systemName: "line.3.horizontal.decrease.circle"),
            state: selectedLevels.count == ExecutionLogLevel.allCases.count ? .on : .off
        ) { [weak self] _ in
            guard let self else { return }
            self.selectedLevels = Set(ExecutionLogLevel.allCases)
            self.persistSelectedLevels()
            self.refreshFilteredDisplay()
        }

        let levelActions = ExecutionLogLevel.allCases.map { level in
            UIAction(
                title: title(for: level),
                state: selectedLevels.contains(level) ? .on : .off
            ) { [weak self] _ in
                guard let self else { return }
                if self.selectedLevels.contains(level) {
                    self.selectedLevels.remove(level)
                } else {
                    self.selectedLevels.insert(level)
                }
                self.persistSelectedLevels()
                self.refreshFilteredDisplay()
            }
        }

        return UIMenu(
            title: String(localized: "log.filterTitle"),
            children: [
                allAction,
                UIMenu(title: String(localized: "log.levelSection"), options: .displayInline, children: levelActions)
            ]
        )
    }

    private func refreshFilterMenu() {
        filterBarButtonItem.menu = makeFilterMenu()
    }

    private func refreshFilteredDisplay() {
        refreshFilterMenu()
        applyFilteredSnapshot(considerStickyBottom: false)
    }

    private func title(for level: ExecutionLogLevel) -> String {
        switch level {
        case .debug:
            return String(localized: "log.level.debug")
        case .info:
            return String(localized: "log.level.info")
        case .warning:
            return String(localized: "log.level.warning")
        case .error:
            return String(localized: "log.level.error")
        }
    }

    private func persistSelectedLevels() {
        var preference = ExecutionLogFilterPreference(rawValue: 0)
        for level in ExecutionLogLevel.allCases {
            preference = preference.updating(level, isEnabled: selectedLevels.contains(level))
        }
        ExecutionLogFilterPreference.setValue(preference)
    }

    @objc
    private func focusBackupTapped() {
        let alert = UIAlertController(
            title: String(localized: "log.focusMode.alertTitle"),
            message: String(localized: "log.focusMode.alertMessage"),
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(
            title: String(localized: "log.focusMode.alertConfirm"),
            style: .default
        ) { [weak self] _ in
            self?.startFocusBackupIfAllowed()
        })
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        present(alert, animated: true)
    }

    private func startFocusBackupIfAllowed() {
        guard coordinator.isRunning else { return }
        guard ProStatus.isPro else {
            presentProUpgradeAlert()
            return
        }
        let focus = FocusModeViewController(coordinator: coordinator)
        present(focus, animated: true)
    }

    private func presentProUpgradeAlert() {
        let alert = UIAlertController(
            title: String(localized: "home.alert.upgradeTitle"),
            message: String(localized: "home.alert.upgradeMessage"),
            preferredStyle: .alert
        )
        if let price = Store.shared.membershipDisplayPrice() {
            alert.addAction(UIAlertAction(
                title: String(format: String(localized: "home.alert.upgradeAction"), price),
                style: .default
            ) { [weak self] _ in
                Task { [weak self] in
                    do {
                        _ = try await Store.shared.purchaseLifetimeMembership()
                    } catch {
                        self?.presentPurchaseError(error)
                    }
                }
            })
        }
        alert.addAction(UIAlertAction(title: String(localized: "common.cancel"), style: .cancel))
        present(alert, animated: true)
    }

    private func presentPurchaseError(_ error: Error) {
        let alert = UIAlertController(
            title: nil,
            message: error.localizedDescription,
            preferredStyle: .alert
        )
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    @objc
    private func exportTapped() {
        guard let url = coordinator.currentSessionLogURL else { return }
        let activityController = UIActivityViewController(activityItems: [url], applicationActivities: nil)
        activityController.popoverPresentationController?.sourceView = exportButton
        activityController.popoverPresentationController?.sourceRect = exportButton.bounds
        present(activityController, animated: true)
    }
}
