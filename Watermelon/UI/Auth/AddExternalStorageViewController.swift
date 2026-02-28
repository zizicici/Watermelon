import SnapKit
import UniformTypeIdentifiers
import UIKit

final class AddExternalStorageViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void
    private let bookmarkStore = SecurityScopedBookmarkStore()

    private let stackView = UIStackView()
    private let nameRow = FormRowView(title: "名称", placeholder: "外接硬盘")
    private let pathLabel = UILabel()
    private let pickButton = UIButton(type: .system)
    private let saveButton = UIButton(type: .system)

    private var selectedDirectoryURL: URL?

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
        title = editingProfile == nil ? "添加外接存储" : "编辑外接存储"
        configureUI()
        fillInitialValues()
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
        pickButton.configuration?.title = editingProfile == nil ? "选择目录" : "重新选择目录"
        pickButton.addTarget(self, action: #selector(pickDirectoryTapped), for: .touchUpInside)

        saveButton.configuration = .filled()
        saveButton.configuration?.title = editingProfile == nil ? "保存并连接" : "保存更改"
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

    private func fillInitialValues() {
        guard let editingProfile else { return }
        nameRow.textField.text = editingProfile.name
        if let path = editingProfile.externalVolumeParams?.displayPath, !path.isEmpty {
            pathLabel.text = "当前目录: \(path)"
        }
    }

    private func updateSaveButtonState() {
        if selectedDirectoryURL != nil {
            saveButton.isEnabled = true
            return
        }
        if let path = editingProfile?.externalVolumeParams?.displayPath, !path.isEmpty {
            saveButton.isEnabled = true
            return
        }
        saveButton.isEnabled = false
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
        do {
            let selectedDisplayPath: String
            let encodedParams: Data
            if let selectedDirectoryURL {
                let scoped = selectedDirectoryURL.startAccessingSecurityScopedResource()
                defer {
                    if scoped {
                        selectedDirectoryURL.stopAccessingSecurityScopedResource()
                    }
                }
                let bookmarkData = try bookmarkStore.makeBookmarkData(for: selectedDirectoryURL)
                selectedDisplayPath = selectedDirectoryURL.path
                let params = ExternalVolumeConnectionParams(
                    rootBookmarkData: bookmarkData,
                    displayPath: selectedDisplayPath
                )
                encodedParams = try ServerProfileRecord.encodedConnectionParams(params)
            } else if let editingProfile,
                      let existingPath = editingProfile.externalVolumeParams?.displayPath,
                      let existingParams = editingProfile.connectionParams {
                selectedDisplayPath = existingPath
                encodedParams = existingParams
            } else {
                presentAlert(title: "未选择目录", message: "请先选择要备份到的外接存储目录")
                return
            }

            let existing = try findExistingProfile(displayPath: selectedDisplayPath)
            if let editingProfile,
               let existing,
               existing.id != editingProfile.id {
                throw NSError(
                    domain: "AddExternalStorage",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: "已存在相同的外接存储目录"]
                )
            }
            let baseProfile = editingProfile ?? existing
            let finalName = (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let profileName = finalName.isEmpty
                ? URL(fileURLWithPath: selectedDisplayPath).lastPathComponent
                : finalName
            let credentialRef = baseProfile?.credentialRef ?? "external:\(UUID().uuidString)"
            let shareName = baseProfile?.shareName ?? "external-\(UUID().uuidString)"

            var profile = ServerProfileRecord(
                id: baseProfile?.id,
                name: profileName,
                storageType: StorageType.externalVolume.rawValue,
                connectionParams: encodedParams,
                sortOrder: baseProfile?.sortOrder ?? 0,
                host: "external",
                port: 0,
                shareName: shareName,
                basePath: "/",
                username: "local",
                domain: nil,
                credentialRef: credentialRef,
                createdAt: baseProfile?.createdAt ?? Date(),
                updatedAt: Date()
            )

            try dependencies.databaseManager.saveServerProfile(&profile)
            onSaved(profile, "")
            popAfterSave()
        } catch {
            presentAlert(title: "保存失败", message: error.localizedDescription)
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
        pathLabel.text = "已选择: \(url.path)"

        if (nameRow.textField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            nameRow.textField.text = url.lastPathComponent
        }
        updateSaveButtonState()
    }
}
