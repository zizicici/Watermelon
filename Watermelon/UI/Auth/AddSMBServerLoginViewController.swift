import SnapKit
import UIKit

final class AddSMBServerLoginViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let draft: SMBServerLoginDraft
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let setupService = SMBSetupService()
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let scrollView = UIScrollView()
    private let contentView = UIView()
    private let stackView = UIStackView()

    private let nameRow = FormRowView(title: "名称", placeholder: "Home NAS")
    private let hostRow = FormRowView(title: "Host", placeholder: "192.168.1.20")
    private let portRow = FormRowView(title: "Port", placeholder: "445")
    private let usernameRow = FormRowView(title: "Username", placeholder: "admin")
    private let passwordRow = FormRowView(title: "Password", placeholder: "password", isSecure: true)
    private let domainRow = FormRowView(title: "Domain(可选)", placeholder: "WORKGROUP")

    private let nextButton = UIButton(type: .system)
    private let loadingView = UIActivityIndicatorView(style: .medium)
    private var keyboardObservers: [NSObjectProtocol] = []

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
        view.backgroundColor = .systemBackground
        title = editingProfile == nil ? "登录 SMB" : "编辑 SMB"

        configureUI()
        fillDraft()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    private func configureUI() {
        scrollView.keyboardDismissMode = .interactive

        let tap = UITapGestureRecognizer(target: self, action: #selector(dismissKeyboard))
        tap.cancelsTouchesInView = false
        scrollView.addGestureRecognizer(tap)

        navigationItem.rightBarButtonItem = UIBarButtonItem(
            image: UIImage(systemName: "keyboard.chevron.compact.down"),
            style: .plain,
            target: self,
            action: #selector(dismissKeyboard)
        )

        stackView.axis = .vertical
        stackView.spacing = 14

        view.addSubview(scrollView)
        scrollView.addSubview(contentView)
        contentView.addSubview(stackView)

        scrollView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }

        contentView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
            make.width.equalTo(scrollView.snp.width)
        }

        stackView.snp.makeConstraints { make in
            make.edges.equalToSuperview().inset(16)
        }

        [nameRow, hostRow, portRow, usernameRow, passwordRow, domainRow].forEach {
            $0.textField.delegate = self
            stackView.addArrangedSubview($0)
        }

        installKeyboardAccessoryToolbar()

        nextButton.configuration = .filled()
        nextButton.configuration?.title = editingProfile == nil ? "登录并选择 Share" : "验证并选择 Share"
        nextButton.addTarget(self, action: #selector(nextTapped), for: .touchUpInside)
        stackView.addArrangedSubview(nextButton)

        loadingView.hidesWhenStopped = true
        stackView.addArrangedSubview(loadingView)

        portRow.textField.keyboardType = .numberPad
    }

    private func fillDraft() {
        nameRow.textField.text = draft.name
        hostRow.textField.text = draft.host
        portRow.textField.text = String(draft.port)
        usernameRow.textField.text = draft.username
        domainRow.textField.text = draft.domain
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
    }

    @objc
    private func nextTapped() {
        dismissKeyboard()

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
                        let alert = UIAlertController(title: "未发现 Share", message: "登录成功，但服务器没有可用 Share。", preferredStyle: .alert)
                        alert.addAction(UIAlertAction(title: "确定", style: .default))
                        self.present(alert, animated: true)
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
                    let alert = UIAlertController(title: "登录失败", message: error.localizedDescription, preferredStyle: .alert)
                    alert.addAction(UIAlertAction(title: "确定", style: .default))
                    self.present(alert, animated: true)
                }
            }
        }
    }

    private func buildAuthContext() throws -> SMBServerAuthContext {
        let host = (hostRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let username = (usernameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let inputPassword = passwordRow.textField.text ?? ""
        let domain = (domainRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let name = (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let password: String

        if !inputPassword.isEmpty {
            password = inputPassword
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
            port: Int(portRow.textField.text ?? "") ?? 445,
            username: username,
            password: password,
            domain: domain.isEmpty ? nil : domain
        )
    }

    @MainActor
    private func setLoading(_ loading: Bool) {
        nextButton.isEnabled = !loading
        if loading {
            loadingView.startAnimating()
        } else {
            loadingView.stopAnimating()
        }
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
        let insetBottom = showing ? overlap + 16 : 0

        UIView.animate(withDuration: duration) {
            self.scrollView.contentInset.bottom = insetBottom
            self.scrollView.verticalScrollIndicatorInsets.bottom = insetBottom
        }
    }

    private func installKeyboardAccessoryToolbar() {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(title: "Done", style: .plain, target: self, action: #selector(dismissKeyboard))
        ]

        [nameRow, hostRow, portRow, usernameRow, passwordRow, domainRow].forEach {
            $0.textField.inputAccessoryView = toolbar
        }
    }
}

extension AddSMBServerLoginViewController: UITextFieldDelegate {
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        let fields: [UITextField] = [
            nameRow.textField,
            hostRow.textField,
            portRow.textField,
            usernameRow.textField,
            passwordRow.textField,
            domainRow.textField
        ]

        guard let index = fields.firstIndex(of: textField) else {
            textField.resignFirstResponder()
            return true
        }

        let nextIndex = index + 1
        if nextIndex < fields.count {
            fields[nextIndex].becomeFirstResponder()
        } else {
            textField.resignFirstResponder()
        }
        return true
    }
}
