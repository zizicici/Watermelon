import SnapKit
import UIKit

@MainActor
final class ExecutionLogHistoryViewController: UIViewController {
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let emptyLabel = UILabel()

    private var sections: [Section] = []

    private struct Section {
        let kind: ExecutionLogKind
        let title: String
        let sessions: [ExecutionLogSessionInfo]
    }

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .medium
        return f
    }()

    private static let byteFormatter: ByteCountFormatter = {
        let f = ByteCountFormatter()
        f.allowedUnits = [.useKB, .useMB]
        f.countStyle = .file
        return f
    }()

    override func viewDidLoad() {
        super.viewDidLoad()
        title = String(localized: "log.history.title")
        view.backgroundColor = .systemGroupedBackground

        tableView.dataSource = self
        tableView.delegate = self
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "session")

        emptyLabel.font = .preferredFont(forTextStyle: .body)
        emptyLabel.textColor = .secondaryLabel
        emptyLabel.textAlignment = .center
        emptyLabel.numberOfLines = 0
        emptyLabel.text = String(localized: "log.history.empty")
        emptyLabel.isHidden = true

        view.addSubview(tableView)
        view.addSubview(emptyLabel)

        tableView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
        emptyLabel.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(24)
        }
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        reload()
    }

    private func reload() {
        let all = ExecutionLogFileStore.listSessions()
        let manual = all.filter { $0.kind == .manual }
        let auto = all.filter { $0.kind == .auto }
        sections = [
            Section(kind: .manual, title: String(localized: "log.history.section.manual"), sessions: manual),
            Section(kind: .auto, title: String(localized: "log.history.section.auto"), sessions: auto)
        ].filter { !$0.sessions.isEmpty }

        emptyLabel.isHidden = !sections.isEmpty
        tableView.isHidden = sections.isEmpty
        tableView.reloadData()
    }
}

extension ExecutionLogHistoryViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        sections.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        sections[section].sessions.count
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        sections[section].title
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard section == sections.count - 1 else { return nil }
        return String(localized: "log.history.retentionFooter")
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "session", for: indexPath)
        let info = sections[indexPath.section].sessions[indexPath.row]
        var config = cell.defaultContentConfiguration()
        config.text = Self.dateFormatter.string(from: info.startedAt)
        config.secondaryText = Self.byteFormatter.string(fromByteCount: info.sizeBytes)
        cell.contentConfiguration = config
        cell.accessoryType = .disclosureIndicator
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        let info = sections[indexPath.section].sessions[indexPath.row]
        let detail = ExecutionLogHistoryDetailViewController(session: info)
        navigationController?.pushViewController(detail, animated: ConsideringUser.pushAnimated)
    }

    func tableView(_ tableView: UITableView, trailingSwipeActionsConfigurationForRowAt indexPath: IndexPath) -> UISwipeActionsConfiguration? {
        let info = sections[indexPath.section].sessions[indexPath.row]
        let delete = UIContextualAction(style: .destructive, title: String(localized: "log.history.delete")) { [weak self] _, _, completion in
            try? FileManager.default.removeItem(at: info.url)
            self?.reload()
            completion(true)
        }
        return UISwipeActionsConfiguration(actions: [delete])
    }
}

@MainActor
final class ExecutionLogHistoryDetailViewController: UIViewController {
    private let session: ExecutionLogSessionInfo
    private let tableView = UITableView(frame: .zero, style: .plain)
    private let emptyLabel = UILabel()
    private var entries: [ExecutionLogEntry] = []

    init(session: ExecutionLogSessionInfo) {
        self.session = session
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = Self.titleDateFormatter.string(from: session.startedAt)

        navigationItem.rightBarButtonItem = UIBarButtonItem(
            image: UIImage(systemName: "square.and.arrow.up"),
            style: .plain,
            target: self,
            action: #selector(exportTapped)
        )

        tableView.backgroundColor = .secondarySystemBackground
        tableView.separatorStyle = .none
        tableView.dataSource = self
        tableView.estimatedRowHeight = 32
        tableView.rowHeight = UITableView.automaticDimension
        tableView.layer.cornerRadius = 12
        tableView.layer.masksToBounds = true
        tableView.contentInset = UIEdgeInsets(top: 6, left: 0, bottom: 6, right: 0)
        tableView.register(ExecutionLogEntryCell.self, forCellReuseIdentifier: ExecutionLogEntryCell.reuseIdentifier)

        emptyLabel.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        emptyLabel.textColor = ExecutionLogPalette.secondary
        emptyLabel.textAlignment = .center
        emptyLabel.numberOfLines = 0
        emptyLabel.text = String(localized: "log.empty")
        emptyLabel.isHidden = true

        view.addSubview(tableView)
        view.addSubview(emptyLabel)

        tableView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).inset(12)
            make.leading.trailing.equalToSuperview().inset(12)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
        }
        emptyLabel.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.trailing.equalToSuperview().inset(24)
        }

        loadEntries()
    }

    private func loadEntries() {
        let captured = session
        Task { [weak self] in
            let parsed = await Task.detached {
                (try? captured.readEntries()) ?? []
            }.value
            guard let self else { return }
            self.entries = parsed
            self.tableView.reloadData()
            self.emptyLabel.isHidden = !parsed.isEmpty
        }
    }

    @objc
    private func exportTapped() {
        let activityController = UIActivityViewController(activityItems: [session.url], applicationActivities: nil)
        activityController.popoverPresentationController?.barButtonItem = navigationItem.rightBarButtonItem
        present(activityController, animated: true)
    }

    private static let titleDateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .short
        f.timeStyle = .short
        return f
    }()
}

extension ExecutionLogHistoryDetailViewController: UITableViewDataSource {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        entries.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: ExecutionLogEntryCell.reuseIdentifier, for: indexPath)
        if let cell = cell as? ExecutionLogEntryCell {
            cell.configure(with: entries[indexPath.row])
        }
        return cell
    }
}
