import SnapKit
import UIKit

final class ServerSelectionViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case discovered
        case saved

        var title: String {
            switch self {
            case .discovered: return "局域网发现"
            case .saved: return "已保存"
            }
        }
    }

    private let dependencies: DependencyContainer
    private let discoveryService = SMBDiscoveryService()
    private let onAuthenticated: (ServerProfileRecord, String) -> Void
    private let autoLoginOnAppear: Bool

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let hintLabel = UILabel()
    private let loadingView = UIActivityIndicatorView(style: .large)

    private var discoveredServers: [DiscoveredSMBServer] = []
    private var savedProfiles: [ServerProfileRecord] = []
    private var activeProfileID: Int64?
    private var hasAttemptedAutoLogin = false

    init(
        dependencies: DependencyContainer,
        autoLoginOnAppear: Bool = true,
        onAuthenticated: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.autoLoginOnAppear = autoLoginOnAppear
        self.onAuthenticated = onAuthenticated
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "选择 SMB 服务器"

        configureUI()
        loadSavedProfiles()

        discoveryService.onUpdate = { [weak self] discovered in
            DispatchQueue.main.async {
                self?.discoveredServers = discovered
                self?.tableView.reloadData()
            }
        }
        discoveryService.start()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        loadSavedProfiles()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        attemptAutoLoginIfNeeded()
    }

    deinit {
        discoveryService.stop()
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = UIBarButtonItem(
            image: UIImage(systemName: "plus"),
            style: .plain,
            target: self,
            action: #selector(addTapped)
        )

        hintLabel.text = "如果未发现目标服务器，请点击右上角 + 手动添加。"
        hintLabel.textColor = .secondaryLabel
        hintLabel.numberOfLines = 0
        hintLabel.font = .systemFont(ofSize: 13)

        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "cell")
        tableView.dataSource = self
        tableView.delegate = self

        loadingView.hidesWhenStopped = true

        view.addSubview(hintLabel)
        view.addSubview(tableView)
        view.addSubview(loadingView)

        hintLabel.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        tableView.snp.makeConstraints { make in
            make.top.equalTo(hintLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalToSuperview()
        }

        loadingView.snp.makeConstraints { make in
            make.center.equalToSuperview()
        }
    }

    private func loadSavedProfiles() {
        savedProfiles = (try? dependencies.databaseManager.fetchServerProfiles()) ?? []
        activeProfileID = try? dependencies.databaseManager.activeServerProfileID()
        tableView.reloadData()
    }

    private func attemptAutoLoginIfNeeded() {
        guard autoLoginOnAppear, !hasAttemptedAutoLogin else { return }
        hasAttemptedAutoLogin = true

        guard !savedProfiles.isEmpty else { return }
        let candidate = preferredAutoLoginProfile(from: savedProfiles)
        guard let password = try? dependencies.keychainService.readPassword(account: candidate.credentialRef),
              !password.isEmpty else {
            return
        }

        login(profile: candidate, password: password, showFailureAlert: false)
    }

    private func preferredAutoLoginProfile(from profiles: [ServerProfileRecord]) -> ServerProfileRecord {
        if let activeID = activeProfileID,
           let active = profiles.first(where: { $0.id == activeID }) {
            return active
        }
        return profiles[0]
    }

    @objc
    private func addTapped() {
        let draft = SMBServerLoginDraft(
            name: "",
            host: "",
            port: 445,
            username: "",
            domain: nil
        )

        let addVC = AddSMBServerLoginViewController(dependencies: dependencies, draft: draft) { [weak self] profile, password in
            guard let self else { return }
            self.savedProfiles = (try? self.dependencies.databaseManager.fetchServerProfiles()) ?? []
            self.tableView.reloadData()
            self.login(profile: profile, password: password)
        }
        navigationController?.pushViewController(addVC, animated: true)
    }

    private func addFromDiscovered(_ discovered: DiscoveredSMBServer) {
        let draft = SMBServerLoginDraft(
            name: discovered.serviceName,
            host: discovered.host,
            port: discovered.port > 0 ? discovered.port : 445,
            username: "",
            domain: nil
        )

        let addVC = AddSMBServerLoginViewController(dependencies: dependencies, draft: draft) { [weak self] profile, password in
            self?.login(profile: profile, password: password)
        }
        navigationController?.pushViewController(addVC, animated: true)
    }

    private func login(profile: ServerProfileRecord, password: String, showFailureAlert: Bool = true) {
        setLoading(true)

        Task { [weak self] in
            guard let self else { return }

            do {
                _ = try await self.dependencies.backupExecutor.reloadRemoteIndex(
                    profile: profile,
                    password: password
                )

                try self.dependencies.databaseManager.setActiveServerProfileID(profile.id)
                self.dependencies.appSession.activate(profile: profile, password: password)

                await MainActor.run {
                    self.setLoading(false)
                    self.onAuthenticated(profile, password)
                }
            } catch {
                await MainActor.run {
                    self.setLoading(false)
                    guard showFailureAlert else { return }
                    let alert = UIAlertController(title: "登录失败", message: error.localizedDescription, preferredStyle: .alert)
                    alert.addAction(UIAlertAction(title: "确定", style: .default))
                    self.present(alert, animated: true)
                }
            }
        }
    }

    private func promptPasswordAndLogin(profile: ServerProfileRecord) {
        if let saved = try? dependencies.keychainService.readPassword(account: profile.credentialRef), !saved.isEmpty {
            login(profile: profile, password: saved)
            return
        }

        let alert = UIAlertController(title: "输入密码", message: profile.name, preferredStyle: .alert)
        alert.addTextField { textField in
            textField.placeholder = "Password"
            textField.isSecureTextEntry = true
        }

        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        alert.addAction(UIAlertAction(title: "登录", style: .default, handler: { [weak self] _ in
            guard let self,
                  let password = alert.textFields?.first?.text,
                  !password.isEmpty else { return }

            try? self.dependencies.keychainService.save(password: password, account: profile.credentialRef)
            self.login(profile: profile, password: password)
        }))
        present(alert, animated: true)
    }

    private func setLoading(_ loading: Bool) {
        tableView.isUserInteractionEnabled = !loading
        navigationItem.rightBarButtonItem?.isEnabled = !loading
        if loading {
            loadingView.startAnimating()
        } else {
            loadingView.stopAnimating()
        }
    }
}

extension ServerSelectionViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = Section(rawValue: section) else { return 0 }
        switch section {
        case .discovered:
            return max(discoveredServers.count, 1)
        case .saved:
            return max(savedProfiles.count, 1)
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        Section(rawValue: section)?.title
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "cell", for: indexPath)
        var content = cell.defaultContentConfiguration()
        cell.accessoryType = .none

        guard let section = Section(rawValue: indexPath.section) else {
            return cell
        }

        switch section {
        case .discovered:
            if discoveredServers.isEmpty {
                content.text = "未发现 SMB 服务"
                content.secondaryText = "请确认局域网和 NAS 广播设置"
                cell.selectionStyle = .none
            } else {
                let server = discoveredServers[indexPath.row]
                content.text = server.serviceName
                content.secondaryText = "\(server.host):\(server.port)"
                cell.accessoryType = .disclosureIndicator
                cell.selectionStyle = .default
            }
        case .saved:
            if savedProfiles.isEmpty {
                content.text = "暂无已保存服务器"
                content.secondaryText = "点击右上角 + 添加"
                cell.selectionStyle = .none
            } else {
                let profile = savedProfiles[indexPath.row]
                content.text = profile.name
                content.secondaryText = "\(profile.host)/\(profile.shareName) · \(profile.username)"
                cell.accessoryType = (profile.id == activeProfileID) ? .checkmark : .disclosureIndicator
                cell.selectionStyle = .default
            }
        }

        cell.contentConfiguration = content
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard let section = Section(rawValue: indexPath.section) else { return }

        switch section {
        case .discovered:
            guard !discoveredServers.isEmpty else { return }
            addFromDiscovered(discoveredServers[indexPath.row])
        case .saved:
            guard !savedProfiles.isEmpty else { return }
            promptPasswordAndLogin(profile: savedProfiles[indexPath.row])
        }
    }
}
