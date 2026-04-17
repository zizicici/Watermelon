import MoreKit
import SnapKit
import UIKit

@MainActor
final class HomeExecutionLogViewController: UIViewController {
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

    private var logObserverID: UUID?
    private var statusText = String(localized: "home.execution.notStarted")
    private var allEntries: [ExecutionLogEntry] = []
    private var displayedEntries: [ExecutionLogEntry] = []
    private var lastEntryAnchor: (index: Int, id: UUID)?
    private var selectedLevels = ExecutionLogFilterPreference.getValue().enabledLevels

    init(coordinator: HomeExecutionCoordinator) {
        self.coordinator = coordinator
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        guard let logObserverID else { return }
        let coordinator = coordinator
        Task { @MainActor in
            coordinator.removeLogObserver(logObserverID)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = String(localized: "log.title")
        navigationItem.rightBarButtonItem = filterBarButtonItem

        buildUI()

        logObserverID = coordinator.addLogObserver { [weak self] snapshot in
            self?.apply(snapshot)
        }
    }

    private func buildUI() {
        statusCardView.backgroundColor = .secondarySystemBackground
        statusCardView.layer.cornerRadius = 12
        statusCardView.layer.masksToBounds = true

        statusTitleLabel.font = .systemFont(ofSize: 13, weight: .semibold)
        statusTitleLabel.textColor = .secondaryLabel
        statusTitleLabel.text = String(localized: "log.status")

        statusLabel.font = .systemFont(ofSize: 18, weight: .semibold)
        statusLabel.textColor = .label
        statusLabel.numberOfLines = 0
        statusLabel.text = statusText

        logTableView.backgroundColor = .secondarySystemBackground
        logTableView.layer.cornerRadius = 12
        logTableView.layer.masksToBounds = true
        logTableView.separatorStyle = .none
        logTableView.dataSource = self
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

    private func apply(_ snapshot: HomeExecutionLogSnapshot) {
        statusText = snapshot.statusText
        statusLabel.text = snapshot.statusText

        let previousAllCount = allEntries.count
        let newAllCount = snapshot.entries.count

        let anchorMatches: Bool = {
            guard let anchor = lastEntryAnchor else { return previousAllCount == 0 }
            guard anchor.index < newAllCount else { return false }
            return snapshot.entries[anchor.index].id == anchor.id
        }()

        let isAppendOnly = anchorMatches && newAllCount > previousAllCount

        if isAppendOnly {
            let appended = snapshot.entries[previousAllCount..<newAllCount]
            allEntries = snapshot.entries
            updateLastEntryAnchor()
            let appendedDisplayed = appended.filter { selectedLevels.contains($0.level) }
            if !appendedDisplayed.isEmpty {
                let stickToBottom = shouldStickToBottom()
                let startIndex = displayedEntries.count
                displayedEntries.append(contentsOf: appendedDisplayed)
                let indexPaths = (startIndex..<displayedEntries.count).map { IndexPath(row: $0, section: 0) }
                logTableView.performBatchUpdates({
                    logTableView.insertRows(at: indexPaths, with: .none)
                }, completion: { [weak self] _ in
                    guard let self, stickToBottom, let last = indexPaths.last else { return }
                    self.logTableView.scrollToRow(at: last, at: .bottom, animated: false)
                })
            }
            refreshEmptyState()
        } else {
            allEntries = snapshot.entries
            updateLastEntryAnchor()
            reloadDisplayed(scrollToBottom: true)
        }

        updateExportAvailability()
    }

    private func updateLastEntryAnchor() {
        if let last = allEntries.last {
            lastEntryAnchor = (allEntries.count - 1, last.id)
        } else {
            lastEntryAnchor = nil
        }
    }

    private func reloadDisplayed(scrollToBottom: Bool) {
        displayedEntries = allEntries.filter { selectedLevels.contains($0.level) }
        logTableView.reloadData()
        refreshEmptyState()
        guard scrollToBottom, !displayedEntries.isEmpty else { return }
        let last = IndexPath(row: displayedEntries.count - 1, section: 0)
        logTableView.scrollToRow(at: last, at: .bottom, animated: false)
    }

    private func refreshEmptyState() {
        emptyLabel.isHidden = !displayedEntries.isEmpty
    }

    private func updateExportAvailability() {
        exportButton.isEnabled = coordinator.currentSessionLogURL != nil
    }

    private func shouldStickToBottom() -> Bool {
        let visibleBottom = logTableView.contentOffset.y + logTableView.bounds.height - logTableView.adjustedContentInset.bottom
        let contentBottom = logTableView.contentSize.height
        return visibleBottom >= contentBottom - 80 || displayedEntries.isEmpty
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
        reloadDisplayed(scrollToBottom: false)
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
    private func exportTapped() {
        guard let url = coordinator.currentSessionLogURL else { return }
        let activityController = UIActivityViewController(activityItems: [url], applicationActivities: nil)
        activityController.popoverPresentationController?.sourceView = exportButton
        activityController.popoverPresentationController?.sourceRect = exportButton.bounds
        present(activityController, animated: true)
    }
}

extension HomeExecutionLogViewController: UITableViewDataSource {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        displayedEntries.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: ExecutionLogEntryCell.reuseIdentifier, for: indexPath)
        if let cell = cell as? ExecutionLogEntryCell {
            cell.configure(with: displayedEntries[indexPath.row])
        }
        return cell
    }
}
