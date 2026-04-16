import SnapKit
import UIKit

final class AddSMBServerLoginViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case server
        case credentials
    }

    private enum Field {
        case name
        case host
        case port
        case username
        case password
        case domain
    }

    private let dependencies: DependencyContainer
    private let draft: SMBServerLoginDraft
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let setupService = SMBSetupService()
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var nextBarButtonItem = UIBarButtonItem(
        title: "下一步",
        style: .prominentStyle,
        target: self,
        action: #selector(nextTapped)
    )
    private lazy var loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)
    private lazy var keyboardToolbar: UIToolbar = {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(title: "Done", style: .plain, target: self, action: #selector(dismissKeyboard))
        ]
        return toolbar
    }()

    private var keyboardObservers: [NSObjectProtocol] = []
    private var isLoading = false

    private var nameText = ""
    private var hostText = ""
    private var portText = "445"
    private var usernameText = ""
    private var passwordText = ""
    private var domainText = ""

    init(
        dependencies: DependencyContainer,
        draft: SMBServerLoginDraft,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
        self.draft = draft
        self.editingProfile = editingProfile
        self.shouldPopToRootOnSave = shouldPopToRootOnSave
        self.onSaved = onSaved
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = editingProfile == nil ? "登录 SMB" : "编辑 SMB"

        fillDraft()
        configureUI()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    private func fillDraft() {
        nameText = draft.name
        hostText = draft.host
        portText = String(draft.port)
        usernameText = draft.username
        domainText = draft.domain ?? ""
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = nextBarButtonItem

        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.keyboardDismissMode = .interactive
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.register(SettingsTextFieldCell.self, forCellReuseIdentifier: SettingsTextFieldCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    @objc
    private func nextTapped() {
        dismissKeyboard()
        guard !isLoading else { return }

        Task { [weak self] in
            guard let self else { return }
            do {
                let auth = try self.buildAuthContext()
                await MainActor.run {
                    self.setLoading(true)
                }

                let shares = try await self.setupService.listShares(auth: auth)

                await MainActor.run {
                    self.setLoading(false)
                    if shares.isEmpty {
                        self.presentAlert(title: "未发现 Share", message: "登录成功，但服务器没有可用 Share。")
                        return
                    }
                    let picker = SMBSharePathPickerViewController(
                        dependencies: self.dependencies,
                        auth: auth,
                        initialShares: shares,
                        editingProfile: self.editingProfile,
                        shouldPopToRootOnSave: self.shouldPopToRootOnSave,
                        onSaved: self.onSaved
                    )
                    self.navigationController?.pushViewController(picker, animated: true)
                }
            } catch {
                await MainActor.run {
                    self.setLoading(false)
                    self.presentAlert(title: "登录失败", message: error.localizedDescription)
                }
            }
        }
    }

    private func buildAuthContext() throws -> SMBServerAuthContext {
        let host = hostText.trimmingCharacters(in: .whitespacesAndNewlines)
        let username = usernameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let passwordInput = passwordText.trimmingCharacters(in: .whitespacesAndNewlines)
        let domain = domainText.trimmingCharacters(in: .whitespacesAndNewlines)
        let name = nameText.trimmingCharacters(in: .whitespacesAndNewlines)

        let password: String
        if !passwordInput.isEmpty {
            password = passwordInput
        } else if let editingProfile,
                  let saved = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef),
                  !saved.isEmpty {
            password = saved
        } else {
            password = ""
        }

        guard !host.isEmpty, !username.isEmpty, !password.isEmpty else {
            throw NSError(domain: "AddSMBServerLogin", code: 1, userInfo: [NSLocalizedDescriptionKey: "请填写 host / username / password"])
        }

        return SMBServerAuthContext(
            name: name.isEmpty ? host : name,
            host: host,
            port: Int(portText) ?? 445,
            username: username,
            password: password,
            domain: domain.isEmpty ? nil : domain
        )
    }

    @MainActor
    private func setLoading(_ loading: Bool) {
        isLoading = loading
        tableView.isUserInteractionEnabled = !loading
        if loading {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = nextBarButtonItem
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }

    private func registerKeyboardNotifications() {
        let center = NotificationCenter.default
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillShowNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: true)
        })
        keyboardObservers.append(center.addObserver(
            forName: UIResponder.keyboardWillHideNotification,
            object: nil,
            queue: .main
        ) { [weak self] note in
            self?.handleKeyboard(note: note, showing: false)
        })
    }

    private func handleKeyboard(note: Notification, showing: Bool) {
        guard let info = note.userInfo,
              let frame = info[UIResponder.keyboardFrameEndUserInfoKey] as? CGRect,
              let duration = info[UIResponder.keyboardAnimationDurationUserInfoKey] as? TimeInterval else {
            return
        }

        let keyboardFrame = view.convert(frame, from: nil)
        let overlap = max(0, view.bounds.maxY - keyboardFrame.minY - view.safeAreaInsets.bottom)
        let insetBottom = showing ? overlap : 0

        UIView.animate(withDuration: duration) {
            self.tableView.contentInset.bottom = insetBottom
            self.tableView.verticalScrollIndicatorInsets.bottom = insetBottom
        }
    }

    private func focusField(_ field: Field?) {
        guard let field else {
            dismissKeyboard()
            return
        }

        let indexPath = indexPath(for: field)
        tableView.scrollToRow(at: indexPath, at: .middle, animated: true)
        DispatchQueue.main.async { [weak self] in
            guard let self,
                  let cell = self.tableView.cellForRow(at: indexPath) as? SettingsTextFieldCell else { return }
            cell.focus()
        }
    }

    private func indexPath(for field: Field) -> IndexPath {
        switch field {
        case .name:
            return IndexPath(row: 0, section: Section.name.rawValue)
        case .host:
            return IndexPath(row: 0, section: Section.server.rawValue)
        case .port:
            return IndexPath(row: 1, section: Section.server.rawValue)
        case .username:
            return IndexPath(row: 0, section: Section.credentials.rawValue)
        case .password:
            return IndexPath(row: 1, section: Section.credentials.rawValue)
        case .domain:
            return IndexPath(row: 2, section: Section.credentials.rawValue)
        }
    }
}

extension AddSMBServerLoginViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = Section(rawValue: section) else { return 0 }
        switch section {
        case .name:
            return 1
        case .server:
            return 2
        case .credentials:
            return 3
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return "名称"
        case .server:
            return "服务器"
        case .credentials:
            return "认证"
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name, .server:
            return nil
        case .credentials:
            return editingProfile == nil ? "登录成功后继续选择 Share 与路径。" : "密码留空时会继续使用已保存的密码。"
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section),
              let cell = tableView.dequeueReusableCell(
                withIdentifier: SettingsTextFieldCell.reuseIdentifier,
                for: indexPath
              ) as? SettingsTextFieldCell else {
            return UITableViewCell()
        }

        switch section {
        case .name:
            cell.configure(
                title: nil,
                text: nameText,
                placeholder: "Home NAS",
                autocapitalizationType: .words,
                returnKeyType: .next,
                inputAccessoryView: keyboardToolbar
            )
            cell.onTextChanged = { [weak self] in self?.nameText = $0 }
            cell.onReturn = { [weak self] in self?.focusField(.host) }
        case .server:
            if indexPath.row == 0 {
                cell.configure(
                    title: "Host",
                    text: hostText,
                    placeholder: "192.168.1.20",
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.hostText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.port) }
            } else {
                cell.configure(
                    title: "Port",
                    text: portText,
                    placeholder: "445",
                    keyboardType: .numberPad,
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.portText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.username) }
            }
        case .credentials:
            switch indexPath.row {
            case 0:
                cell.configure(
                    title: "Username",
                    text: usernameText,
                    placeholder: "admin",
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.usernameText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.password) }
            case 1:
                cell.configure(
                    title: "Password",
                    text: passwordText,
                    placeholder: editingProfile == nil ? "password" : "留空表示不变",
                    isSecure: true,
                    returnKeyType: .next,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.passwordText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(.domain) }
            default:
                cell.configure(
                    title: "Domain",
                    text: domainText,
                    placeholder: "WORKGROUP",
                    returnKeyType: .done,
                    inputAccessoryView: keyboardToolbar
                )
                cell.onTextChanged = { [weak self] in self?.domainText = $0 }
                cell.onReturn = { [weak self] in self?.focusField(nil) }
            }
        }

        return cell
    }
}
