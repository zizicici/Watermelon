import SnapKit
import UIKit

final class SMBSharePathPickerViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let setupService = SMBSetupService()
    private let auth: SMBServerAuthContext
    private let onSaved: (ServerProfileRecord, String) -> Void

    private var shares: [SMBShareInfo]
    private var selectedShare: SMBShareInfo?
    private var currentPath: String = "/"
    private var directories: [SMBRemoteEntry] = []
    private var loadTask: Task<Void, Never>?
    private var loadRequestID: UInt64 = 0

    private let pathLabel = UILabel()
    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let nextButton = UIButton(type: .system)
    private let loadingView = UIActivityIndicatorView(style: .medium)

    init(
        dependencies: DependencyContainer,
        auth: SMBServerAuthContext,
        initialShares: [SMBShareInfo],
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.auth = auth
        self.shares = initialShares
        self.onSaved = onSaved
        self.selectedShare = initialShares.first
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "选择 Share 与路径"

        configureUI()
        refreshPathLabel()

        startLoadDirectories()
    }

    deinit {
        loadTask?.cancel()
    }

    private func configureUI() {
        pathLabel.font = .systemFont(ofSize: 13)
        pathLabel.textColor = .secondaryLabel
        pathLabel.numberOfLines = 2

        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "dir")
        tableView.dataSource = self
        tableView.delegate = self

        nextButton.configuration = .filled()
        nextButton.configuration?.title = "下一步"
        nextButton.addTarget(self, action: #selector(nextTapped), for: .touchUpInside)

        loadingView.hidesWhenStopped = true

        view.addSubview(pathLabel)
        view.addSubview(tableView)
        view.addSubview(nextButton)
        view.addSubview(loadingView)

        pathLabel.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        nextButton.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(16)
            make.bottom.equalTo(view.safeAreaLayoutGuide).inset(12)
            make.height.equalTo(44)
        }

        tableView.snp.makeConstraints { make in
            make.top.equalTo(pathLabel.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview()
            make.bottom.equalTo(nextButton.snp.top).offset(-8)
        }

        loadingView.snp.makeConstraints { make in
            make.center.equalToSuperview()
        }
    }

    @objc
    private func nextTapped() {
        guard let selectedShare else {
            presentAlert(title: "未选择 Share", message: "请先选择 Share")
            return
        }

        let context = SMBServerPathContext(auth: auth, shareName: selectedShare.name, basePath: currentPath)
        let finalizeVC = AddSMBServerViewController(dependencies: dependencies, context: context, onSaved: onSaved)
        navigationController?.pushViewController(finalizeVC, animated: true)
    }

    private func refreshPathLabel() {
        pathLabel.text = "当前路径: \(currentPath)"
    }

    private func startLoadDirectories() {
        loadTask?.cancel()
        loadRequestID &+= 1
        let requestID = loadRequestID

        guard let selectedShare else {
            directories = []
            tableView.reloadSections(IndexSet(integer: TableSection.paths.rawValue), with: .none)
            return
        }

        let shareName = selectedShare.name
        let targetPath = currentPath
        setLoading(true)

        loadTask = Task { [weak self] in
            guard let self else { return }
            do {
                let dirs = try await self.setupService.listDirectories(auth: self.auth, shareName: shareName, path: targetPath)
                try Task.checkCancellation()
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    guard self.selectedShare?.name == shareName, self.currentPath == targetPath else { return }
                    self.directories = dirs
                    self.setLoading(false)
                    self.tableView.reloadSections(IndexSet(integer: TableSection.paths.rawValue), with: .none)
                    self.refreshPathLabel()
                }
            } catch is CancellationError {
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    self.setLoading(false)
                }
            } catch {
                await MainActor.run {
                    guard requestID == self.loadRequestID else { return }
                    self.setLoading(false)
                    self.presentAlert(title: "读取目录失败", message: error.localizedDescription)
                }
            }
        }
    }

    private func parentPath(of path: String) -> String {
        let normalized = RemotePathBuilder.normalizePath(path)
        if normalized == "/" { return "/" }
        let ns = normalized as NSString
        let parent = ns.deletingLastPathComponent
        return parent.isEmpty ? "/" : RemotePathBuilder.normalizePath(parent)
    }

    @MainActor
    private func setLoading(_ loading: Bool) {
        nextButton.isEnabled = !loading
        tableView.isUserInteractionEnabled = !loading
        if loading {
            loadingView.startAnimating()
        } else {
            loadingView.stopAnimating()
        }
    }

    @MainActor
    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }
}

extension SMBSharePathPickerViewController: UITableViewDataSource, UITableViewDelegate {
    private enum TableSection: Int, CaseIterable {
        case shares
        case paths
    }

    func numberOfSections(in tableView: UITableView) -> Int {
        TableSection.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = TableSection(rawValue: section) else { return 0 }
        switch section {
        case .shares:
            return max(shares.count, 1)
        case .paths:
            let hasParent = currentPath != "/"
            return directories.count + (hasParent ? 1 : 0)
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = TableSection(rawValue: section) else { return nil }
        switch section {
        case .shares:
            return "Share 列表"
        case .paths:
            return "路径列表"
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "dir", for: indexPath)
        var content = cell.defaultContentConfiguration()

        guard let section = TableSection(rawValue: indexPath.section) else {
            cell.contentConfiguration = content
            return cell
        }

        switch section {
        case .shares:
            if shares.isEmpty {
                content.text = "未发现可用 Share"
                content.secondaryText = "请返回上一步检查权限"
                cell.selectionStyle = .none
                cell.accessoryType = .none
            } else {
                guard indexPath.row < shares.count else {
                    content.text = "Share 数据已变更"
                    content.secondaryText = "请稍后重试"
                    cell.selectionStyle = .none
                    cell.accessoryType = .none
                    cell.contentConfiguration = content
                    return cell
                }
                let share = shares[indexPath.row]
                content.text = share.name
                content.secondaryText = share.comment.isEmpty ? "(no comment)" : share.comment
                cell.selectionStyle = .default
                cell.accessoryType = (selectedShare?.name == share.name) ? .checkmark : .none
            }
        case .paths:
            let hasParent = currentPath != "/"
            if hasParent && indexPath.row == 0 {
                content.text = ".."
                content.secondaryText = "返回上一级"
                cell.accessoryType = .disclosureIndicator
                cell.selectionStyle = .default
                cell.contentConfiguration = content
                return cell
            }

            let index = hasParent ? indexPath.row - 1 : indexPath.row
            guard index >= 0, index < directories.count else {
                content.text = "目录已变更"
                content.secondaryText = "请稍后重试"
                cell.selectionStyle = .none
                cell.accessoryType = .none
                cell.contentConfiguration = content
                return cell
            }
            let entry = directories[index]
            content.text = entry.name
            content.secondaryText = entry.path
            cell.accessoryType = .disclosureIndicator
            cell.selectionStyle = .default
        }

        cell.contentConfiguration = content
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)

        guard let section = TableSection(rawValue: indexPath.section) else { return }
        switch section {
        case .shares:
            guard !shares.isEmpty else { return }
            guard indexPath.row < shares.count else { return }
            selectedShare = shares[indexPath.row]
            currentPath = "/"
            directories = []
            tableView.reloadSections(IndexSet([TableSection.shares.rawValue, TableSection.paths.rawValue]), with: .none)
            refreshPathLabel()
            startLoadDirectories()
            return
        case .paths:
            let hasParent = currentPath != "/"

            if hasParent && indexPath.row == 0 {
                currentPath = parentPath(of: currentPath)
                startLoadDirectories()
                return
            }

            let index = hasParent ? indexPath.row - 1 : indexPath.row
            guard index >= 0, index < directories.count else { return }
            let entry = directories[index]
            currentPath = entry.path
            startLoadDirectories()
        }
    }
}
