import SnapKit
import UIKit

final class BackupViewController: UIViewController {
    private let dependencies: DependencyContainer

    private let scrollView = UIScrollView()
    private let contentView = UIView()
    private let stackView = UIStackView()

    private let profileNameRow = FormRowView(title: "Profile Name", placeholder: "Home NAS")
    private let hostRow = FormRowView(title: "Host", placeholder: "192.168.1.20")
    private let portRow = FormRowView(title: "Port", placeholder: "445")
    private let shareRow = FormRowView(title: "Share Name", placeholder: "photos")
    private let basePathRow = FormRowView(title: "Base Path", placeholder: "/backup/iphone")
    private let userRow = FormRowView(title: "Username", placeholder: "admin")
    private let domainRow = FormRowView(title: "Domain (Optional)", placeholder: "WORKGROUP")
    private let passwordRow = FormRowView(title: "Password", placeholder: "password", isSecure: true)

    private let testButton = UIButton(type: .system)
    private let saveButton = UIButton(type: .system)
    private let startBackupButton = UIButton(type: .system)

    private let statusLabel = UILabel()
    private let progressView = UIProgressView(progressViewStyle: .default)
    private let progressLabel = UILabel()
    private let logTextView = UITextView()

    private var runningTask: Task<Void, Never>?
    private var editingProfileID: Int64?

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        navigationItem.largeTitleDisplayMode = .never

        buildUI()
        configureActions()
        loadLatestProfile()
    }

    private func buildUI() {
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

        [profileNameRow, hostRow, portRow, shareRow, basePathRow, userRow, domainRow, passwordRow].forEach { row in
            stackView.addArrangedSubview(row)
        }

        let buttonRow = UIStackView()
        buttonRow.axis = .horizontal
        buttonRow.spacing = 8
        buttonRow.distribution = .fillEqually

        [testButton, saveButton, startBackupButton].forEach { button in
            button.configuration = .filled()
            buttonRow.addArrangedSubview(button)
        }

        testButton.configuration?.title = "Test"
        saveButton.configuration?.title = "Save"
        startBackupButton.configuration?.title = "Start Backup"

        stackView.addArrangedSubview(buttonRow)

        statusLabel.font = .systemFont(ofSize: 14, weight: .semibold)
        statusLabel.textColor = .secondaryLabel
        statusLabel.numberOfLines = 0
        statusLabel.text = "Idle"

        progressLabel.font = .systemFont(ofSize: 13)
        progressLabel.textColor = .secondaryLabel
        progressLabel.text = "成功 0 · 失败 0 · 跳过 0"

        logTextView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        logTextView.isEditable = false
        logTextView.backgroundColor = .secondarySystemBackground
        logTextView.layer.cornerRadius = 10
        logTextView.textContainerInset = UIEdgeInsets(top: 8, left: 8, bottom: 8, right: 8)
        logTextView.text = ""

        stackView.addArrangedSubview(statusLabel)
        stackView.addArrangedSubview(progressView)
        stackView.addArrangedSubview(progressLabel)
        stackView.addArrangedSubview(logTextView)

        logTextView.snp.makeConstraints { make in
            make.height.equalTo(240)
        }

        portRow.textField.keyboardType = .numberPad
        portRow.textField.text = "445"
        basePathRow.textField.text = "/backup/iphone"
    }

    private func configureActions() {
        testButton.addTarget(self, action: #selector(testConnection), for: .touchUpInside)
        saveButton.addTarget(self, action: #selector(saveProfile), for: .touchUpInside)
        startBackupButton.addTarget(self, action: #selector(startBackup), for: .touchUpInside)
    }

    private func loadLatestProfile() {
        guard let profile = try? dependencies.databaseManager.latestServerProfile() else { return }
        editingProfileID = profile.id

        profileNameRow.textField.text = profile.name
        hostRow.textField.text = profile.host
        portRow.textField.text = String(profile.port)
        shareRow.textField.text = profile.shareName
        basePathRow.textField.text = profile.basePath
        userRow.textField.text = profile.username
        domainRow.textField.text = profile.domain
        passwordRow.textField.text = try? dependencies.keychainService.readPassword(account: profile.credentialRef)
    }

    @objc
    private func saveProfile() {
        do {
            var profile = try buildProfileFromForm(existingID: editingProfileID)
            try dependencies.databaseManager.saveServerProfile(&profile)
            editingProfileID = profile.id
            try dependencies.keychainService.save(password: passwordRow.textField.text ?? "", account: profile.credentialRef)
            appendLog("Saved profile: \(profile.name)")
            statusLabel.text = "Profile saved"
        } catch {
            presentError(error)
        }
    }

    @objc
    private func testConnection() {
        guard runningTask == nil else { return }

        runningTask = Task { [weak self] in
            guard let self else { return }
            await MainActor.run { self.setBusy(true) }
            defer {
                Task { @MainActor [weak self] in
                    self?.setBusy(false)
                }
            }

            do {
                let profile = try self.buildProfileFromForm(existingID: self.editingProfileID)
                let password = self.passwordRow.textField.text ?? ""
                let client = try AMSMB2Client(config: SMBServerConfig(
                    host: profile.host,
                    port: profile.port,
                    shareName: profile.shareName,
                    basePath: profile.basePath,
                    username: profile.username,
                    password: password,
                    domain: profile.domain
                ))

                try await client.connect()
                defer { Task { await client.disconnect() } }
                try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))

                await MainActor.run {
                    self.statusLabel.text = "Connection OK"
                    self.appendLog("SMB connection test passed.")
                }
            } catch {
                await MainActor.run {
                    self.statusLabel.text = "Connection failed"
                    self.presentError(error)
                }
            }

            await MainActor.run {
                self.runningTask = nil
            }
        }
    }

    @objc
    private func startBackup() {
        guard runningTask == nil else { return }

        runningTask = Task { [weak self] in
            guard let self else { return }
            await MainActor.run { self.setBusy(true) }
            defer {
                Task { @MainActor [weak self] in
                    self?.setBusy(false)
                }
            }

            do {
                var profile = try self.buildProfileFromForm(existingID: self.editingProfileID)
                try self.dependencies.databaseManager.saveServerProfile(&profile)
                self.editingProfileID = profile.id
                let password = self.passwordRow.textField.text ?? ""
                try self.dependencies.keychainService.save(password: password, account: profile.credentialRef)

                await MainActor.run {
                    self.progressView.progress = 0
                    self.progressLabel.text = "成功 0 · 失败 0 · 跳过 0"
                    self.statusLabel.text = "Backup running"
                    self.appendLog("Starting backup...")
                }

                let result = try await self.dependencies.backupExecutor.runBackup(
                    profile: profile,
                    password: password,
                    appVersion: self.dependencies.appVersion,
                    onProgress: { [weak self] progress in
                        self?.progressView.progress = progress.fraction
                        if progress.total > 0 {
                            self?.progressLabel.text = "成功 \(progress.succeeded) · 失败 \(progress.failed) · 跳过 \(progress.skipped) · 总计 \(progress.total)"
                        } else {
                            self?.progressLabel.text = "成功 \(progress.succeeded) · 失败 \(progress.failed) · 跳过 \(progress.skipped)"
                        }
                        self?.statusLabel.text = progress.message
                    },
                    onLog: { [weak self] line in
                        self?.appendLog(line)
                    }
                )

                await MainActor.run {
                    self.statusLabel.text = result.failed == 0 ? "Backup completed" : "Backup completed with errors"
                    self.appendLog("Completed: Succeeded \(result.succeeded), Failed \(result.failed), Skipped \(result.skipped)")
                }
            } catch {
                await MainActor.run {
                    self.statusLabel.text = "Backup failed"
                    self.presentError(error)
                }
            }

            await MainActor.run {
                self.runningTask = nil
            }
        }
    }

    private func buildProfileFromForm(existingID: Int64?) throws -> ServerProfileRecord {
        guard let host = hostRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines), !host.isEmpty,
              let share = shareRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines), !share.isEmpty,
              let user = userRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines), !user.isEmpty,
              let basePath = basePathRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines), !basePath.isEmpty,
              let rawPassword = passwordRow.textField.text, !rawPassword.isEmpty else {
            throw NSError(domain: "BackupViewController", code: 1, userInfo: [NSLocalizedDescriptionKey: "Please complete host/share/basePath/username/password."])
        }

        let name = profileNameRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
            ? (profileNameRow.textField.text ?? "")
            : "Default NAS"

        let port = Int(portRow.textField.text ?? "") ?? 445
        let credentialRef = credentialRef(host: host, share: share, username: user)

        return ServerProfileRecord(
            id: existingID,
            name: name,
            host: host,
            port: port,
            shareName: share,
            basePath: RemotePathBuilder.normalizePath(basePath),
            username: user,
            domain: domainRow.textField.text?.trimmingCharacters(in: .whitespacesAndNewlines),
            credentialRef: credentialRef,
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    private func credentialRef(host: String, share: String, username: String) -> String {
        "\(host)|\(share)|\(username)"
    }

    @MainActor
    private func setBusy(_ busy: Bool) {
        testButton.isEnabled = !busy
        saveButton.isEnabled = !busy
        startBackupButton.isEnabled = !busy
    }

    @MainActor
    private func appendLog(_ text: String) {
        let existing = logTextView.text ?? ""
        let line = "[\(Self.timeFormatter.string(from: Date()))] \(text)"
        logTextView.text = existing.isEmpty ? line : existing + "\n" + line
        let range = NSRange(location: max(logTextView.text.count - 1, 0), length: 1)
        logTextView.scrollRangeToVisible(range)
    }

    @MainActor
    private func presentError(_ error: Error) {
        appendLog("Error: \(error.localizedDescription)")
        let alert = UIAlertController(title: "Error", message: error.localizedDescription, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "OK", style: .default))
        present(alert, animated: true)
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()
}
