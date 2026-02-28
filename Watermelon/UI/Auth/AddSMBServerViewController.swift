import SnapKit
import UIKit

final class AddSMBServerViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let context: SMBServerPathContext
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let stackView = UIStackView()
    private let nameRow = FormRowView(title: "名称", placeholder: "Home NAS")

    private let summaryLabel = UILabel()
    private let saveButton = UIButton(type: .system)

    init(dependencies: DependencyContainer, context: SMBServerPathContext, onSaved: @escaping (ServerProfileRecord, String) -> Void) {
        self.dependencies = dependencies
        self.context = context
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
        title = "确认并保存"

        configureUI()
        fillData()
    }

    private func configureUI() {
        stackView.axis = .vertical
        stackView.spacing = 14

        summaryLabel.numberOfLines = 0
        summaryLabel.font = .systemFont(ofSize: 14)
        summaryLabel.textColor = .secondaryLabel

        saveButton.configuration = .filled()
        saveButton.configuration?.title = "保存并登录"
        saveButton.addTarget(self, action: #selector(saveTapped), for: .touchUpInside)

        view.addSubview(stackView)
        stackView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        stackView.addArrangedSubview(nameRow)
        stackView.addArrangedSubview(summaryLabel)
        stackView.addArrangedSubview(saveButton)
    }

    private func fillData() {
        nameRow.textField.text = context.auth.name
        summaryLabel.text = [
            "Host: \(context.auth.host):\(context.auth.port)",
            "Share: \(context.shareName)",
            "Path: \(context.basePath)",
            "Username: \(context.auth.username)",
            "Domain: \(context.auth.domain ?? "(none)")"
        ].joined(separator: "\n")
    }

    @objc
    private func saveTapped() {
        do {
            let (profile, password) = try saveProfile()
            onSaved(profile, password)
            navigationController?.popToRootViewController(animated: true)
        } catch {
            let alert = UIAlertController(title: "保存失败", message: error.localizedDescription, preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "确定", style: .default))
            present(alert, animated: true)
        }
    }

    private func saveProfile() throws -> (ServerProfileRecord, String) {
        let finalName = (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? context.auth.host : finalName

        let normalizedPath = RemotePathBuilder.normalizePath(context.basePath)
        let existing = try dependencies.databaseManager.findServerProfile(
            host: context.auth.host,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username
        )

        let credentialRef = "\(context.auth.host)|\(context.shareName)|\(context.auth.username)"

        var profile = ServerProfileRecord(
            id: existing?.id,
            name: profileName,
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            host: context.auth.host,
            port: context.auth.port,
            shareName: context.shareName,
            basePath: normalizedPath,
            username: context.auth.username,
            domain: context.auth.domain,
            credentialRef: credentialRef,
            createdAt: existing?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: context.auth.password, account: credentialRef)
        return (profile, context.auth.password)
    }
}
