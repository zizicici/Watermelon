import SnapKit
import UIKit

final class AddWebDAVStorageViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let stackView = UIStackView()
    private let nameRow = FormRowView(title: "名称", placeholder: "WebDAV")
    private let endpointRow = FormRowView(title: "Endpoint URL", placeholder: "https://example.com/dav")
    private let basePathRow = FormRowView(title: "备份根路径", placeholder: "/Watermelon")
    private let usernameRow = FormRowView(title: "Username", placeholder: "user")
    private let passwordRow = FormRowView(title: "Password", placeholder: "password", isSecure: true)
    private let saveButton = UIButton(type: .system)

    init(
        dependencies: DependencyContainer,
        editingProfile: ServerProfileRecord? = nil,
        shouldPopToRootOnSave: Bool = true,
        onSaved: @escaping (ServerProfileRecord, String) -> Void
    ) {
        self.dependencies = dependencies
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
        title = editingProfile == nil ? "添加 WebDAV 存储" : "编辑 WebDAV 存储"
        configureUI()
        fillInitialValues()
    }

    private func configureUI() {
        stackView.axis = .vertical
        stackView.spacing = 14

        endpointRow.textField.keyboardType = .URL
        endpointRow.textField.autocapitalizationType = .none

        saveButton.configuration = .filled()
        saveButton.configuration?.title = editingProfile == nil ? "保存并连接" : "保存更改"
        saveButton.addTarget(self, action: #selector(saveTapped), for: .touchUpInside)

        view.addSubview(stackView)
        stackView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        stackView.addArrangedSubview(nameRow)
        stackView.addArrangedSubview(endpointRow)
        stackView.addArrangedSubview(basePathRow)
        stackView.addArrangedSubview(usernameRow)
        stackView.addArrangedSubview(passwordRow)
        stackView.addArrangedSubview(saveButton)
    }

    private func fillInitialValues() {
        if let editingProfile {
            nameRow.textField.text = editingProfile.name
            usernameRow.textField.text = editingProfile.username
            basePathRow.textField.text = editingProfile.basePath
            endpointRow.textField.text = editingProfile.webDAVParams?.endpointURLString ?? fallbackEndpointString(for: editingProfile)
            return
        }
        basePathRow.textField.text = "/Watermelon"
    }

    private func fallbackEndpointString(for profile: ServerProfileRecord) -> String {
        let scheme = profile.port == 443 ? "https" : "http"
        let host = profile.host
        let defaultPort = scheme == "https" ? 443 : 80
        let portPart = profile.port == defaultPort ? "" : ":\(profile.port)"
        return "\(scheme)://\(host)\(portPart)\(profile.shareName)"
    }

    @objc
    private func saveTapped() {
        do {
            let (profile, password) = try saveProfile()
            onSaved(profile, password)
            popAfterSave()
        } catch {
            presentAlert(title: "保存失败", message: error.localizedDescription)
        }
    }

    private func saveProfile() throws -> (ServerProfileRecord, String) {
        let endpointURL = try parseEndpointURL((endpointRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines))
        let endpointURLString = Self.normalizedEndpointURLString(endpointURL)

        let username = (usernameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !username.isEmpty else {
            throw NSError(domain: "AddWebDAVStorage", code: 1, userInfo: [NSLocalizedDescriptionKey: "请填写用户名"])
        }

        let inputPassword = passwordRow.textField.text ?? ""
        let password: String
        if !inputPassword.isEmpty {
            password = inputPassword
        } else if let editingProfile,
                  let saved = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef),
                  !saved.isEmpty {
            password = saved
        } else {
            throw NSError(domain: "AddWebDAVStorage", code: 2, userInfo: [NSLocalizedDescriptionKey: "请填写密码"])
        }

        let rawBasePath = (basePathRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedBasePath = RemotePathBuilder.normalizePath(rawBasePath.isEmpty ? "/Watermelon" : rawBasePath)

        let existing = try findExistingProfile(
            endpointURLString: endpointURLString,
            basePath: normalizedBasePath,
            username: username
        )
        if let editingProfile,
           let existing,
           existing.id != editingProfile.id {
            throw NSError(
                domain: "AddWebDAVStorage",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: "已存在相同的 WebDAV 连接配置"]
            )
        }

        let baseProfile = editingProfile ?? existing
        let finalName = (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? (endpointURL.host ?? "WebDAV") : finalName
        let endpointPath = endpointURL.path.isEmpty ? "/" : endpointURL.path
        let credentialRef = "webdav|\(endpointURLString)|\(username)"
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(
            WebDAVConnectionParams(endpointURLString: endpointURLString)
        )

        var profile = ServerProfileRecord(
            id: baseProfile?.id,
            name: profileName,
            storageType: StorageType.webdav.rawValue,
            connectionParams: connectionParams,
            sortOrder: baseProfile?.sortOrder ?? 0,
            host: endpointURL.host ?? "",
            port: endpointURL.port ?? defaultPort(for: endpointURL),
            shareName: endpointPath,
            basePath: normalizedBasePath,
            username: username,
            domain: nil,
            credentialRef: credentialRef,
            createdAt: baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: password, account: credentialRef)
        if let oldRef = editingProfile?.credentialRef,
           oldRef != credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return (profile, password)
    }

    private func parseEndpointURL(_ input: String) throws -> URL {
        guard !input.isEmpty else {
            throw NSError(domain: "AddWebDAVStorage", code: 4, userInfo: [NSLocalizedDescriptionKey: "请填写 Endpoint URL"])
        }

        let normalizedInput: String
        if input.contains("://") {
            normalizedInput = input
        } else {
            normalizedInput = "https://\(input)"
        }

        guard let url = URL(string: normalizedInput),
              let scheme = url.scheme?.lowercased(),
              (scheme == "http" || scheme == "https"),
              url.host != nil else {
            throw NSError(domain: "AddWebDAVStorage", code: 5, userInfo: [NSLocalizedDescriptionKey: "Endpoint URL 格式不正确"])
        }
        return url
    }

    private static func normalizedEndpointURLString(_ url: URL) -> String {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }
        components.query = nil
        components.fragment = nil
        components.user = nil
        components.password = nil
        if components.path.isEmpty {
            components.path = "/"
        } else if components.path.count > 1, components.path.hasSuffix("/") {
            components.path.removeLast()
        }
        return (components.url ?? url).absoluteString
    }

    private func defaultPort(for endpointURL: URL) -> Int {
        endpointURL.scheme?.lowercased() == "https" ? 443 : 80
    }

    private func findExistingProfile(endpointURLString: String, basePath: String, username: String) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .webdav &&
                profile.webDAVParams?.endpointURLString == endpointURLString &&
                RemotePathBuilder.normalizePath(profile.basePath) == RemotePathBuilder.normalizePath(basePath) &&
                profile.username == username
        }
    }

    private func popAfterSave() {
        guard let navigationController else { return }
        if shouldPopToRootOnSave {
            navigationController.popToRootViewController(animated: true)
            return
        }
        if let manageVC = navigationController.viewControllers.first(where: { $0 is ManageStorageProfilesViewController }) {
            navigationController.popToViewController(manageVC, animated: true)
            return
        }
        navigationController.popViewController(animated: true)
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }
}
