import SnapKit
import UniformTypeIdentifiers
import UIKit

final class AddExternalStorageViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let onSaved: (ServerProfileRecord, String) -> Void
    private let bookmarkStore = SecurityScopedBookmarkStore()

    private let stackView = UIStackView()
    private let nameRow = FormRowView(title: "名称", placeholder: "外接硬盘")
    private let pathLabel = UILabel()
    private let pickButton = UIButton(type: .system)
    private let saveButton = UIButton(type: .system)

    private var selectedDirectoryURL: URL?
    private var selectedDisplayPath: String?

    init(dependencies: DependencyContainer, onSaved: @escaping (ServerProfileRecord, String) -> Void) {
        self.dependencies = dependencies
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
        title = "添加外接存储"
        configureUI()
        updateSaveButtonState()
    }

    private func configureUI() {
        stackView.axis = .vertical
        stackView.spacing = 14

        pathLabel.numberOfLines = 0
        pathLabel.font = .systemFont(ofSize: 13)
        pathLabel.textColor = .secondaryLabel
        pathLabel.text = "尚未选择目录"

        pickButton.configuration = .tinted()
        pickButton.configuration?.title = "选择目录"
        pickButton.addTarget(self, action: #selector(pickDirectoryTapped), for: .touchUpInside)

        saveButton.configuration = .filled()
        saveButton.configuration?.title = "保存并连接"
        saveButton.addTarget(self, action: #selector(saveTapped), for: .touchUpInside)

        view.addSubview(stackView)
        stackView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        stackView.addArrangedSubview(nameRow)
        stackView.addArrangedSubview(pathLabel)
        stackView.addArrangedSubview(pickButton)
        stackView.addArrangedSubview(saveButton)
    }

    private func updateSaveButtonState() {
        saveButton.isEnabled = selectedDirectoryURL != nil
    }

    @objc
    private func pickDirectoryTapped() {
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [.folder], asCopy: false)
        picker.delegate = self
        picker.allowsMultipleSelection = false
        present(picker, animated: true)
    }

    @objc
    private func saveTapped() {
        guard let selectedDirectoryURL,
              let selectedDisplayPath else {
            presentAlert(title: "未选择目录", message: "请先选择要备份到的外接存储目录")
            return
        }

        do {
            let scoped = selectedDirectoryURL.startAccessingSecurityScopedResource()
            defer {
                if scoped {
                    selectedDirectoryURL.stopAccessingSecurityScopedResource()
                }
            }
            let bookmarkData = try bookmarkStore.makeBookmarkData(for: selectedDirectoryURL)
            let params = ExternalVolumeConnectionParams(
                rootBookmarkData: bookmarkData,
                displayPath: selectedDisplayPath
            )
            let encodedParams = try ServerProfileRecord.encodedConnectionParams(params)

            let existing = try findExistingProfile(displayPath: selectedDisplayPath)
            let finalName = (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let profileName = finalName.isEmpty
                ? selectedDirectoryURL.lastPathComponent
                : finalName
            let credentialRef = existing?.credentialRef ?? "external:\(UUID().uuidString)"
            let shareName = existing?.shareName ?? "external-\(UUID().uuidString)"

            var profile = ServerProfileRecord(
                id: existing?.id,
                name: profileName,
                storageType: StorageType.externalVolume.rawValue,
                connectionParams: encodedParams,
                host: "external",
                port: 0,
                shareName: shareName,
                basePath: "/",
                username: "local",
                domain: nil,
                credentialRef: credentialRef,
                createdAt: existing?.createdAt ?? Date(),
                updatedAt: Date()
            )

            try dependencies.databaseManager.saveServerProfile(&profile)
            onSaved(profile, "")
            navigationController?.popToRootViewController(animated: true)
        } catch {
            presentAlert(title: "保存失败", message: error.localizedDescription)
        }
    }

    private func findExistingProfile(displayPath: String) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .externalVolume &&
                profile.externalVolumeParams?.displayPath == displayPath
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }
}

extension AddExternalStorageViewController: UIDocumentPickerDelegate {
    func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
        guard let url = urls.first else { return }

        selectedDirectoryURL = url
        selectedDisplayPath = url.path
        pathLabel.text = "已选择: \(url.path)"

        if (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            nameRow.textField.text = url.lastPathComponent
        }
        updateSaveButtonState()
    }
}
