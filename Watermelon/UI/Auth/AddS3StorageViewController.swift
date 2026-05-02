import SnapKit
import UIKit

final class AddS3StorageViewController: UIViewController {
    private enum Section: Int, CaseIterable {
        case name
        case endpoint
        case bucket
        case path
        case credentials
    }

    private enum Field {
        case name
        case endpoint
        case region
        case bucket
        case basePath
        case accessKeyID
        case secretAccessKey
    }

    private let dependencies: DependencyContainer
    private let editingProfile: ServerProfileRecord?
    private let shouldPopToRootOnSave: Bool
    private let onSaved: (ServerProfileRecord, String) -> Void

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private lazy var saveBarButtonItem = UIBarButtonItem(
        title: String(localized: "common.save"),
        style: .prominentStyle,
        target: self,
        action: #selector(saveTapped)
    )
    private lazy var loadingIndicatorView = UIActivityIndicatorView(style: .medium)
    private lazy var loadingBarButtonItem = UIBarButtonItem(customView: loadingIndicatorView)
    private lazy var keyboardToolbar: UIToolbar = {
        let toolbar = UIToolbar()
        toolbar.sizeToFit()
        toolbar.items = [
            UIBarButtonItem(barButtonSystemItem: .flexibleSpace, target: nil, action: nil),
            UIBarButtonItem(barButtonSystemItem: .done, target: self, action: #selector(dismissKeyboard))
        ]
        return toolbar
    }()

    private var keyboardObservers: [NSObjectProtocol] = []
    private var isSaving = false

    private var nameText = ""
    private var endpointText = ""
    private var regionText = ""
    private var bucketText = ""
    private var basePathText = ""
    private var accessKeyText = ""
    private var secretKeyText = ""
    private var pathStyleOverride: Bool?

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
        view.backgroundColor = .appBackground
        title = editingProfile == nil
            ? String(localized: "auth.s3.title")
            : String(localized: "auth.s3.editTitle")

        fillInitialValues()
        configureUI()
        registerKeyboardNotifications()
    }

    deinit {
        keyboardObservers.forEach { NotificationCenter.default.removeObserver($0) }
    }

    private func fillInitialValues() {
        if let editingProfile {
            nameText = editingProfile.name
            accessKeyText = editingProfile.username
            basePathText = editingProfile.basePath
            bucketText = editingProfile.shareName
            if let params = editingProfile.s3Params {
                regionText = params.region
                pathStyleOverride = params.usePathStyle
                let scheme = params.scheme.isEmpty ? "https" : params.scheme
                endpointText = Self.formatEndpoint(scheme: scheme, host: editingProfile.host, port: editingProfile.port)
            }
            return
        }
        basePathText = "/Watermelon"
    }

    private func configureUI() {
        navigationItem.rightBarButtonItem = saveBarButtonItem

        tableView.backgroundColor = .appBackground
        tableView.dataSource = self
        tableView.delegate = self
        tableView.keyboardDismissMode = .interactive
        tableView.rowHeight = UITableView.automaticDimension
        tableView.estimatedRowHeight = 44
        tableView.register(SettingsTextFieldCell.self, forCellReuseIdentifier: SettingsTextFieldCell.reuseIdentifier)
        tableView.register(S3PathStyleCell.self, forCellReuseIdentifier: S3PathStyleCell.reuseIdentifier)

        view.addSubview(tableView)
        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }
    }

    @objc
    private func saveTapped() {
        dismissKeyboard()
        guard !isSaving else { return }

        let draft: ValidatedDraft
        do {
            draft = try validateInputs()
        } catch {
            presentAlert(
                title: String(localized: "auth.saveFailed"),
                message: UserFacingErrorLocalizer.message(for: error, storageType: .s3)
            )
            return
        }

        setSaving(true)
        Task { [weak self] in
            guard let self else { return }
            do {
                try await Self.verifyConnection(draft: draft)
            } catch {
                await MainActor.run {
                    self.setSaving(false)
                    self.presentAlert(
                        title: String(localized: "auth.saveFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .s3)
                    )
                }
                return
            }

            await MainActor.run {
                do {
                    let profile = try self.commitProfile(draft: draft)
                    self.onSaved(profile, draft.secretAccessKey)
                    self.popAfterSave()
                } catch {
                    self.setSaving(false)
                    self.presentAlert(
                        title: String(localized: "auth.saveFailed"),
                        message: UserFacingErrorLocalizer.message(for: error, storageType: .s3)
                    )
                }
            }
        }
    }

    private struct ValidatedDraft {
        let scheme: String
        let host: String
        let port: Int
        let region: String
        let bucket: String
        let normalizedBasePath: String
        let usePathStyle: Bool
        let accessKeyID: String
        let secretAccessKey: String
        let credentialRef: String
        let profileName: String
        let baseProfile: ServerProfileRecord?
    }

    private func validateInputs() throws -> ValidatedDraft {
        let endpointRaw = endpointText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !endpointRaw.isEmpty,
              let parsed = S3Client.parseEndpoint(endpointRaw) else {
            throw NSError(domain: "AddS3Storage", code: 1, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.s3.validation.endpoint")])
        }

        let bucket = bucketText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !bucket.isEmpty else {
            throw NSError(domain: "AddS3Storage", code: 2, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.s3.validation.bucket")])
        }

        let region = regionText.trimmingCharacters(in: .whitespacesAndNewlines)

        let accessKey = accessKeyText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !accessKey.isEmpty else {
            throw NSError(domain: "AddS3Storage", code: 3, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.s3.validation.accessKey")])
        }

        let trimmedSecret = secretKeyText.trimmingCharacters(in: .whitespacesAndNewlines)
        let secretKey: String
        if !trimmedSecret.isEmpty {
            secretKey = trimmedSecret
        } else if let editingProfile,
                  let saved = try? dependencies.keychainService.readPassword(account: editingProfile.credentialRef),
                  !saved.isEmpty {
            secretKey = saved
        } else {
            throw NSError(domain: "AddS3Storage", code: 4, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.s3.validation.secretKey")])
        }

        let rawBase = basePathText.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedBasePath = RemotePathBuilder.normalizePath(rawBase.isEmpty ? "/Watermelon" : rawBase)

        let usePathStyle = pathStyleOverride ?? S3Client.defaultPathStyle(forHost: parsed.host)

        let existing = try findExistingProfile(host: parsed.host, port: parsed.port, bucket: bucket, basePath: normalizedBasePath, accessKeyID: accessKey)
        if let editingProfile,
           let existing,
           existing.id != editingProfile.id {
            throw NSError(domain: "AddS3Storage", code: 5, userInfo: [NSLocalizedDescriptionKey: String(localized: "auth.s3.validation.duplicate")])
        }

        let baseProfile = editingProfile ?? existing
        let finalName = nameText.trimmingCharacters(in: .whitespacesAndNewlines)
        let profileName = finalName.isEmpty ? bucket : finalName
        let credentialRef = "s3|\(parsed.host):\(parsed.port)|\(bucket)|\(accessKey)"

        return ValidatedDraft(
            scheme: parsed.scheme,
            host: parsed.host,
            port: parsed.port,
            region: region,
            bucket: bucket,
            normalizedBasePath: normalizedBasePath,
            usePathStyle: usePathStyle,
            accessKeyID: accessKey,
            secretAccessKey: secretKey,
            credentialRef: credentialRef,
            profileName: profileName,
            baseProfile: baseProfile
        )
    }

    private static func verifyConnection(draft: ValidatedDraft) async throws {
        let client = S3Client(config: S3Client.Config(
            endpointHost: draft.host,
            endpointPort: draft.port,
            scheme: draft.scheme,
            region: draft.region,
            bucket: draft.bucket,
            usePathStyle: draft.usePathStyle,
            accessKeyID: draft.accessKeyID,
            secretAccessKey: draft.secretAccessKey,
            sessionToken: nil
        ))
        do {
            try await client.connect()
        } catch {
            await client.disconnect()
            throw error
        }
        await client.disconnect()
    }

    private func commitProfile(draft: ValidatedDraft) throws -> ServerProfileRecord {
        let connectionParams = try ServerProfileRecord.encodedConnectionParams(
            S3ConnectionParams(scheme: draft.scheme, region: draft.region, usePathStyle: draft.usePathStyle)
        )

        var profile = ServerProfileRecord(
            id: draft.baseProfile?.id,
            name: draft.profileName,
            storageType: StorageType.s3.rawValue,
            connectionParams: connectionParams,
            sortOrder: draft.baseProfile?.sortOrder ?? 0,
            host: draft.host,
            port: draft.port,
            shareName: draft.bucket,
            basePath: draft.normalizedBasePath,
            username: draft.accessKeyID,
            domain: nil,
            credentialRef: draft.credentialRef,
            backgroundBackupEnabled: draft.baseProfile?.backgroundBackupEnabled ?? true,
            createdAt: draft.baseProfile?.createdAt ?? Date(),
            updatedAt: Date()
        )

        try dependencies.databaseManager.saveServerProfile(&profile)
        try dependencies.keychainService.save(password: draft.secretAccessKey, account: draft.credentialRef)
        if let oldRef = editingProfile?.credentialRef,
           oldRef != draft.credentialRef {
            try? dependencies.keychainService.delete(account: oldRef)
        }
        return profile
    }

    private func findExistingProfile(host: String, port: Int, bucket: String, basePath: String, accessKeyID: String) throws -> ServerProfileRecord? {
        let profiles = try dependencies.databaseManager.fetchServerProfiles()
        return profiles.first { profile in
            profile.resolvedStorageType == .s3 &&
                profile.host == host &&
                profile.port == port &&
                profile.shareName == bucket &&
                RemotePathBuilder.normalizePath(profile.basePath) == basePath &&
                profile.username == accessKeyID
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

    private func setSaving(_ saving: Bool) {
        isSaving = saving
        tableView.isUserInteractionEnabled = !saving
        if saving {
            loadingIndicatorView.startAnimating()
            navigationItem.rightBarButtonItem = loadingBarButtonItem
        } else {
            loadingIndicatorView.stopAnimating()
            navigationItem.rightBarButtonItem = saveBarButtonItem
        }
    }

    private func presentAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
        present(alert, animated: true)
    }

    @objc
    private func dismissKeyboard() {
        view.endEditing(true)
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
        case .endpoint:
            return IndexPath(row: 0, section: Section.endpoint.rawValue)
        case .region:
            return IndexPath(row: 1, section: Section.endpoint.rawValue)
        case .bucket:
            return IndexPath(row: 0, section: Section.bucket.rawValue)
        case .basePath:
            return IndexPath(row: 0, section: Section.path.rawValue)
        case .accessKeyID:
            return IndexPath(row: 0, section: Section.credentials.rawValue)
        case .secretAccessKey:
            return IndexPath(row: 1, section: Section.credentials.rawValue)
        }
    }

    private static func formatEndpoint(scheme: String, host: String, port: Int) -> String {
        let defaultPort = scheme == "https" ? 443 : 80
        if port == 0 || port == defaultPort {
            return "\(scheme)://\(host)"
        }
        return "\(scheme)://\(host):\(port)"
    }

    private func reloadPathStyleCell() {
        let indexPath = IndexPath(row: 1, section: Section.bucket.rawValue)
        guard tableView.cellForRow(at: indexPath) != nil else { return }
        tableView.reloadRows(at: [indexPath], with: .none)
    }
}

extension AddS3StorageViewController: UITableViewDataSource, UITableViewDelegate {
    func numberOfSections(in tableView: UITableView) -> Int {
        Section.allCases.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        guard let section = Section(rawValue: section) else { return 0 }
        switch section {
        case .name:
            return 1
        case .endpoint:
            return 2
        case .bucket:
            return 2
        case .path:
            return 1
        case .credentials:
            return 2
        }
    }

    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .name:
            return String(localized: "auth.section.name")
        case .endpoint:
            return String(localized: "auth.s3.section.endpoint")
        case .bucket:
            return String(localized: "auth.s3.section.bucket")
        case .path:
            return String(localized: "auth.section.paths")
        case .credentials:
            return String(localized: "auth.section.auth")
        }
    }

    func tableView(_ tableView: UITableView, titleForFooterInSection section: Int) -> String? {
        guard let section = Section(rawValue: section) else { return nil }
        switch section {
        case .endpoint:
            return String(localized: "auth.s3.endpoint.footer")
        case .bucket:
            return String(localized: "auth.s3.pathStyle.hint")
        case .path:
            return String(localized: "auth.s3.path.footer")
        case .credentials:
            return editingProfile == nil ? nil : String(localized: "auth.smb.login.footerEdit")
        case .name:
            return nil
        }
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let section = Section(rawValue: indexPath.section) else { return UITableViewCell() }
        switch section {
        case .name:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: nameText,
                placeholder: String(localized: "auth.s3.placeholder.name"),
                autocapitalizationType: .words,
                returnKeyType: .next,
                onChanged: { [weak self] in self?.nameText = $0 },
                onReturn: { [weak self] in self?.focusField(.endpoint) }
            )
        case .endpoint:
            return endpointCell(in: tableView, at: indexPath)
        case .bucket:
            return bucketCell(in: tableView, at: indexPath)
        case .path:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: basePathText,
                placeholder: String(localized: "auth.s3.placeholder.basePath"),
                returnKeyType: .next,
                onChanged: { [weak self] in self?.basePathText = $0 },
                onReturn: { [weak self] in self?.focusField(.accessKeyID) }
            )
        case .credentials:
            return credentialsCell(in: tableView, at: indexPath)
        }
    }

    private func endpointCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch indexPath.row {
        case 0:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: endpointText,
                placeholder: String(localized: "auth.s3.placeholder.endpoint"),
                keyboardType: .URL,
                returnKeyType: .next,
                onChanged: { [weak self] in
                    self?.endpointText = $0
                    self?.reloadPathStyleCell()
                },
                onReturn: { [weak self] in self?.focusField(.region) }
            )
        default:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: regionText,
                placeholder: String(localized: "auth.s3.placeholder.region"),
                returnKeyType: .next,
                onChanged: { [weak self] in self?.regionText = $0 },
                onReturn: { [weak self] in self?.focusField(.bucket) }
            )
        }
    }

    private func bucketCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch indexPath.row {
        case 0:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: bucketText,
                placeholder: String(localized: "auth.s3.placeholder.bucket"),
                returnKeyType: .next,
                onChanged: { [weak self] in self?.bucketText = $0 },
                onReturn: { [weak self] in self?.focusField(.basePath) }
            )
        default:
            guard let cell = tableView.dequeueReusableCell(
                withIdentifier: S3PathStyleCell.reuseIdentifier,
                for: indexPath
            ) as? S3PathStyleCell else { return UITableViewCell() }
            let host = endpointText.trimmingCharacters(in: .whitespacesAndNewlines)
            let parsedHost = S3Client.parseEndpoint(host)?.host ?? ""
            let resolved = pathStyleOverride ?? S3Client.defaultPathStyle(forHost: parsedHost)
            cell.configure(
                title: String(localized: "auth.s3.pathStyle.label"),
                isOn: resolved
            )
            cell.onValueChanged = { [weak self] value in
                self?.pathStyleOverride = value
            }
            return cell
        }
    }

    private func credentialsCell(in tableView: UITableView, at indexPath: IndexPath) -> UITableViewCell {
        switch indexPath.row {
        case 0:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: accessKeyText,
                placeholder: String(localized: "auth.s3.placeholder.accessKeyID"),
                returnKeyType: .next,
                onChanged: { [weak self] in self?.accessKeyText = $0 },
                onReturn: { [weak self] in self?.focusField(.secretAccessKey) }
            )
        default:
            return makeTextCell(
                in: tableView, at: indexPath,
                text: secretKeyText,
                placeholder: editingProfile == nil
                    ? String(localized: "auth.s3.placeholder.secretKey")
                    : String(localized: "auth.passwordPlaceholderEdit"),
                isSecure: true,
                returnKeyType: .done,
                onChanged: { [weak self] in self?.secretKeyText = $0 },
                onReturn: { [weak self] in self?.focusField(nil) }
            )
        }
    }

    private func makeTextCell(
        in tableView: UITableView,
        at indexPath: IndexPath,
        text: String,
        placeholder: String,
        keyboardType: UIKeyboardType = .default,
        autocapitalizationType: UITextAutocapitalizationType = .none,
        isSecure: Bool = false,
        returnKeyType: UIReturnKeyType = .next,
        onChanged: ((String) -> Void)?,
        onReturn: (() -> Void)?
    ) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: SettingsTextFieldCell.reuseIdentifier,
            for: indexPath
        ) as? SettingsTextFieldCell else { return UITableViewCell() }
        cell.configure(
            title: nil,
            text: text,
            placeholder: placeholder,
            isSecure: isSecure,
            keyboardType: keyboardType,
            autocapitalizationType: autocapitalizationType,
            returnKeyType: returnKeyType,
            inputAccessoryView: keyboardToolbar
        )
        cell.onTextChanged = onChanged
        cell.onReturn = onReturn
        return cell
    }
}

private final class S3PathStyleCell: UITableViewCell {
    static let reuseIdentifier = "S3PathStyleCell"

    private let titleLabel = UILabel()
    private let toggle = UISwitch()

    var onValueChanged: ((Bool) -> Void)?

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super.init(style: style, reuseIdentifier: reuseIdentifier)
        selectionStyle = .none

        let background: UIBackgroundConfiguration = .listCell()
        var configuredBackground = background
        configuredBackground.backgroundColor = .secondarySystemGroupedBackground
        backgroundConfiguration = configuredBackground

        titleLabel.font = .preferredFont(forTextStyle: .body)
        titleLabel.textColor = .label
        titleLabel.numberOfLines = 0

        toggle.addTarget(self, action: #selector(valueChanged), for: .valueChanged)

        contentView.addSubview(titleLabel)
        contentView.addSubview(toggle)

        titleLabel.snp.makeConstraints { make in
            make.leading.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
            make.top.bottom.equalToSuperview().inset(11)
        }
        toggle.snp.makeConstraints { make in
            make.leading.greaterThanOrEqualTo(titleLabel.snp.trailing).offset(12)
            make.trailing.equalToSuperview().inset(16)
            make.centerY.equalToSuperview()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        onValueChanged = nil
    }

    func configure(title: String, isOn: Bool) {
        titleLabel.text = title
        toggle.isOn = isOn
    }

    @objc
    private func valueChanged() {
        onValueChanged?(toggle.isOn)
    }
}
