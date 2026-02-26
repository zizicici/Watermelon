import Photos
import GRDB
import SnapKit
import UIKit

final class SettingsViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let onSwitchServer: () -> Void

    private let currentServerLabel = UILabel()
    private let permissionLabel = UILabel()
    private let statsLabel = UILabel()

    private let switchServerButton = UIButton(type: .system)
    private let requestPermissionButton = UIButton(type: .system)
    private let pullManifestButton = UIButton(type: .system)
    private let rebuildIndexButton = UIButton(type: .system)

    init(dependencies: DependencyContainer, onSwitchServer: @escaping () -> Void) {
        self.dependencies = dependencies
        self.onSwitchServer = onSwitchServer
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

        configureUI()
        refreshStatus()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        refreshStatus()
    }

    private func configureUI() {
        [currentServerLabel, permissionLabel, statsLabel].forEach {
            $0.numberOfLines = 0
            $0.font = .systemFont(ofSize: 14)
        }

        switchServerButton.configuration = .filled()
        switchServerButton.configuration?.title = "切换 SMB 服务器"
        switchServerButton.addTarget(self, action: #selector(switchServerTapped), for: .touchUpInside)

        requestPermissionButton.configuration = .filled()
        requestPermissionButton.configuration?.title = "请求照片权限"
        requestPermissionButton.addTarget(self, action: #selector(requestPermission), for: .touchUpInside)

        pullManifestButton.configuration = .filled()
        pullManifestButton.configuration?.title = "重新同步远端索引"
        pullManifestButton.addTarget(self, action: #selector(resyncManifest), for: .touchUpInside)

        rebuildIndexButton.configuration = .tinted()
        rebuildIndexButton.configuration?.title = "清理本地索引"
        rebuildIndexButton.addTarget(self, action: #selector(clearLocalIndex), for: .touchUpInside)

        let stack = UIStackView(arrangedSubviews: [
            currentServerLabel,
            permissionLabel,
            statsLabel,
            switchServerButton,
            requestPermissionButton,
            pullManifestButton,
            rebuildIndexButton
        ])
        stack.axis = .vertical
        stack.spacing = 12

        view.addSubview(stack)
        stack.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }
    }

    private func refreshStatus() {
        let auth = dependencies.photoLibraryService.authorizationStatus()
        permissionLabel.text = "照片权限: \(Self.permissionText(auth))"

        if let profile = dependencies.appSession.activeProfile {
            currentServerLabel.text = "当前服务器: \(profile.name)\n\(profile.host)/\(profile.shareName)\n用户: \(profile.username)"
        } else {
            currentServerLabel.text = "当前服务器: 未登录"
        }

        do {
            let profiles = try dependencies.databaseManager.fetchServerProfiles()
            let stats = try dependencies.databaseManager.read { db -> Int in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM content_hash_index") ?? 0
            }

            let remoteCount = dependencies.backupExecutor.currentRemoteSnapshot().totalCount
            statsLabel.text = "已保存服务器: \(profiles.count)\n本地 Hash 索引: \(stats)\n远端索引项: \(remoteCount)"
        } catch {
            statsLabel.text = "状态读取失败"
        }
    }

    @objc
    private func switchServerTapped() {
        onSwitchServer()
    }

    @objc
    private func requestPermission() {
        Task { [weak self] in
            guard let self else { return }
            _ = await self.dependencies.photoLibraryService.requestAuthorization()
            await MainActor.run {
                self.refreshStatus()
            }
        }
    }

    @objc
    private func resyncManifest() {
        Task { [weak self] in
            guard let self else { return }

            do {
                guard let profile = self.dependencies.appSession.activeProfile,
                      let password = self.dependencies.appSession.activePassword else {
                    throw BackupError.missingServerProfile
                }
                let snapshot = try await self.dependencies.backupExecutor.reloadRemoteIndex(
                    profile: profile,
                    password: password
                )

                await MainActor.run {
                    self.refreshStatus()
                    self.presentSimpleAlert(title: "完成", message: "远端索引同步完成，共 \(snapshot.totalCount) 项")
                }
            } catch {
                await MainActor.run {
                    self.presentSimpleAlert(title: "同步失败", message: error.localizedDescription)
                }
            }
        }
    }

    @objc
    private func clearLocalIndex() {
        do {
            try dependencies.databaseManager.write { db in
                try db.execute(sql: "DELETE FROM content_hash_index")
            }
            refreshStatus()
            presentSimpleAlert(title: "已清理", message: "本地索引已清空")
        } catch {
            presentSimpleAlert(title: "失败", message: error.localizedDescription)
        }
    }

    private func presentSimpleAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }

    private static func permissionText(_ status: PHAuthorizationStatus) -> String {
        switch status {
        case .authorized:
            return "authorized"
        case .limited:
            return "limited"
        case .denied:
            return "denied"
        case .restricted:
            return "restricted"
        case .notDetermined:
            return "notDetermined"
        @unknown default:
            return "unknown"
        }
    }
}
