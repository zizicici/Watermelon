import SnapKit
import UIKit

final class RemoteBrowserViewController: UIViewController {
    private let dependencies: DependencyContainer

    private let collectionView: UICollectionView
    private let statusLabel = UILabel()

    private var resources: [RemoteManifestResource] = []
    private var selectedResourceIDs = Set<String>()

    private var activeProfile: ServerProfileRecord?
    private var activePassword: String?

    private let imageCache = NSCache<NSString, UIImage>()

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies

        let layout = UICollectionViewFlowLayout()
        layout.minimumInteritemSpacing = 8
        layout.minimumLineSpacing = 8
        self.collectionView = UICollectionView(frame: .zero, collectionViewLayout: layout)

        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground

        configureUI()
        configureNav()
        reloadData()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        reloadData()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        guard let flow = collectionView.collectionViewLayout as? UICollectionViewFlowLayout else { return }

        let spacing: CGFloat = 8
        let columns: CGFloat = traitCollection.horizontalSizeClass == .compact ? 3 : 5
        let width = collectionView.bounds.width - (columns - 1) * spacing
        let item = floor(width / columns)
        flow.itemSize = CGSize(width: item, height: item + 36)
    }

    private func configureUI() {
        statusLabel.font = .systemFont(ofSize: 13, weight: .medium)
        statusLabel.textColor = .secondaryLabel

        collectionView.backgroundColor = .clear
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.allowsMultipleSelection = true
        collectionView.register(PhotoGridCell.self, forCellWithReuseIdentifier: PhotoGridCell.reuseID)

        view.addSubview(statusLabel)
        view.addSubview(collectionView)

        statusLabel.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        collectionView.snp.makeConstraints { make in
            make.top.equalTo(statusLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalTo(view.safeAreaLayoutGuide).inset(16)
        }
    }

    private func configureNav() {
        navigationItem.rightBarButtonItems = [
            UIBarButtonItem(title: "Restore", style: .plain, target: self, action: #selector(restoreSelected)),
            UIBarButtonItem(barButtonSystemItem: .refresh, target: self, action: #selector(refreshTapped))
        ]
    }

    @objc
    private func refreshTapped() {
        reloadData()
    }

    private func reloadData() {
        do {
            if let sessionProfile = dependencies.appSession.activeProfile {
                activeProfile = sessionProfile
            } else {
                activeProfile = try dependencies.databaseManager.latestServerProfile()
            }
            if let activePassword = dependencies.appSession.activePassword, !activePassword.isEmpty {
                self.activePassword = activePassword
            } else if let profile = activeProfile {
                self.activePassword = try? dependencies.keychainService.readPassword(account: profile.credentialRef)
            } else {
                self.activePassword = nil
            }

            resources = dependencies.backupExecutor.currentRemoteSnapshot().resources.sorted { lhs, rhs in
                if lhs.creationDate != rhs.creationDate {
                    return lhs.creationDate > rhs.creationDate
                }
                return lhs.fileName < rhs.fileName
            }
            selectedResourceIDs.removeAll()

            statusLabel.text = "\(resources.count) remote resource(s)"
            collectionView.reloadData()
        } catch {
            statusLabel.text = "Failed to load remote resources"
        }
    }

    @objc
    private func restoreSelected() {
        guard let profile = activeProfile,
              let password = activePassword,
              !password.isEmpty else {
            presentSimpleAlert(title: "Missing Profile", message: "Configure server profile and password first.")
            return
        }

        let selected = resources.filter { selectedResourceIDs.contains($0.id) }
        if selected.isEmpty {
            presentSimpleAlert(title: "No Selection", message: "Select at least one item to restore.")
            return
        }

        Task { [weak self] in
            guard let self else { return }

            do {
                try await self.dependencies.restoreService.restore(
                    resources: selected,
                    profile: profile,
                    password: password,
                    onLog: { _ in }
                )

                await MainActor.run {
                    self.presentSimpleAlert(title: "Restore Complete", message: "Imported \(selected.count) item(s) back to Photos.")
                }
            } catch {
                await MainActor.run {
                    self.presentSimpleAlert(title: "Restore Failed", message: error.localizedDescription)
                }
            }
        }
    }

    private func loadPreview(for resource: RemoteManifestResource, into cell: PhotoGridCell) {
        if let cached = imageCache.object(forKey: resource.remoteRelativePath as NSString) {
            cell.imageView.image = cached
            return
        }

        let imageLike = ResourceTypeCode.isPhotoLike(resource.resourceType)
            || ["jpg", "jpeg", "png", "heic", "gif", "webp"].contains((resource.fileName as NSString).pathExtension.lowercased())
        guard imageLike,
              let profile = activeProfile,
              let password = activePassword,
              !password.isEmpty else {
            cell.imageView.image = UIImage(systemName: ResourceTypeCode.isVideoLike(resource.resourceType) ? "video" : "doc")
            return
        }

        cell.imageView.image = UIImage(systemName: "photo")

        Task {
            do {
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

                let temp = FileManager.default.temporaryDirectory.appendingPathComponent("thumb_\(UUID().uuidString)_\(resource.fileName)")
                let remotePath = RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: resource.remoteRelativePath
                )
                try await client.download(remotePath: remotePath, localURL: temp)
                let image = UIImage(contentsOfFile: temp.path)
                try? FileManager.default.removeItem(at: temp)

                guard let image else { return }
                imageCache.setObject(image, forKey: resource.remoteRelativePath as NSString)

                await MainActor.run {
                    if cell.representedID == resource.id {
                        cell.imageView.image = image
                    }
                }
            } catch {
                await MainActor.run {
                    if cell.representedID == resource.id {
                        cell.imageView.image = UIImage(systemName: "exclamationmark.triangle")
                    }
                }
            }
        }
    }

    private func presentSimpleAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "OK", style: .default))
        present(alert, animated: true)
    }
}

extension RemoteBrowserViewController: UICollectionViewDataSource, UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        resources.count
    }

    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        guard let cell = collectionView.dequeueReusableCell(withReuseIdentifier: PhotoGridCell.reuseID, for: indexPath) as? PhotoGridCell else {
            return UICollectionViewCell()
        }

        let resource = resources[indexPath.item]
        cell.representedID = resource.id
        cell.titleLabel.text = resource.fileName

        if selectedResourceIDs.contains(resource.id) {
            cell.layer.borderWidth = 2
            cell.layer.borderColor = UIColor.systemBlue.cgColor
        } else {
            cell.layer.borderWidth = 0
            cell.layer.borderColor = nil
        }

        loadPreview(for: resource, into: cell)
        return cell
    }

    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        let resource = resources[indexPath.item]

        if collectionView.indexPathsForSelectedItems?.count ?? 0 > 1 {
            selectedResourceIDs.insert(resource.id)
            collectionView.reloadItems(at: [indexPath])
            return
        }

        selectedResourceIDs.insert(resource.id)

        guard let profile = activeProfile,
              let password = activePassword,
              !password.isEmpty else {
            collectionView.reloadItems(at: [indexPath])
            return
        }

        let detail = RemoteResourceDetailViewController(
            dependencies: dependencies,
            resource: resource,
            profile: profile,
            password: password
        )
        detail.title = "Remote Detail"
        navigationController?.pushViewController(detail, animated: true)
    }

    func collectionView(_ collectionView: UICollectionView, didDeselectItemAt indexPath: IndexPath) {
        let resource = resources[indexPath.item]
        selectedResourceIDs.remove(resource.id)
        collectionView.reloadItems(at: [indexPath])
    }
}
