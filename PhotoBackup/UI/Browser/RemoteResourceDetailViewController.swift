import SnapKit
import UIKit

final class RemoteResourceDetailViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let resource: BackupResourceRecord
    private let profile: ServerProfileRecord
    private let password: String

    private let imageView = UIImageView()
    private let infoLabel = UILabel()
    private let restoreButton = UIButton(type: .system)

    init(dependencies: DependencyContainer, resource: BackupResourceRecord, profile: ServerProfileRecord, password: String) {
        self.dependencies = dependencies
        self.resource = resource
        self.profile = profile
        self.password = password
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
        populateInfo()
        Task { await loadPreview() }
    }

    private func configureUI() {
        imageView.contentMode = .scaleAspectFit
        imageView.backgroundColor = .secondarySystemBackground

        infoLabel.numberOfLines = 0
        infoLabel.font = .systemFont(ofSize: 14)

        restoreButton.configuration = .filled()
        restoreButton.configuration?.title = "Restore This Item"
        restoreButton.addTarget(self, action: #selector(restoreTapped), for: .touchUpInside)

        view.addSubview(imageView)
        view.addSubview(infoLabel)
        view.addSubview(restoreButton)

        imageView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(16)
            make.height.equalTo(imageView.snp.width)
        }

        infoLabel.snp.makeConstraints { make in
            make.top.equalTo(imageView.snp.bottom).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        restoreButton.snp.makeConstraints { make in
            make.top.equalTo(infoLabel.snp.bottom).offset(20)
            make.leading.trailing.equalToSuperview().inset(16)
            make.height.equalTo(44)
        }
    }

    private func populateInfo() {
        infoLabel.text = [
            "Asset: \(resource.assetLocalIdentifier)",
            "Name: \(resource.originalFilename)",
            "Type: \(resource.resourceType)",
            "UTI: \(resource.uti ?? "N/A")",
            "Size: \(ByteCountFormatter.string(fromByteCount: resource.fileSize, countStyle: .file))",
            "Remote Path: \(resource.remoteRelativePath)",
            "Backed Up: \(Self.dateFormatter.string(from: resource.backedUpAt))"
        ].joined(separator: "\n")
    }

    private func loadPreview() async {
        let imageLike = dependencies.metadataService.isImage(uti: resource.uti) || resource.originalFilename.lowercased().hasSuffix(".jpg") || resource.originalFilename.lowercased().hasSuffix(".jpeg") || resource.originalFilename.lowercased().hasSuffix(".png") || resource.originalFilename.lowercased().hasSuffix(".heic")

        guard imageLike else {
            await MainActor.run {
                self.imageView.image = UIImage(systemName: "video")
            }
            return
        }

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
            defer {
                Task {
                    await client.disconnect()
                }
            }

            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("preview_\(UUID().uuidString)_\(resource.originalFilename)")
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: resource.remoteRelativePath
            )
            try await client.download(remotePath: remotePath, localURL: tempURL)
            let image = UIImage(contentsOfFile: tempURL.path)
            try? FileManager.default.removeItem(at: tempURL)

            await MainActor.run {
                self.imageView.image = image ?? UIImage(systemName: "photo")
            }
        } catch {
            await MainActor.run {
                self.imageView.image = UIImage(systemName: "photo")
            }
        }
    }

    @objc
    private func restoreTapped() {
        Task { [weak self] in
            guard let self else { return }

            do {
                try await self.dependencies.restoreService.restore(
                    resources: [self.resource],
                    profile: self.profile,
                    password: self.password,
                    onLog: { _ in }
                )

                await MainActor.run {
                    let alert = UIAlertController(title: "Restored", message: "Item was imported back to Photos.", preferredStyle: .alert)
                    alert.addAction(UIAlertAction(title: "OK", style: .default))
                    self.present(alert, animated: true)
                }
            } catch {
                await MainActor.run {
                    let alert = UIAlertController(title: "Restore Failed", message: error.localizedDescription, preferredStyle: .alert)
                    alert.addAction(UIAlertAction(title: "OK", style: .default))
                    self.present(alert, animated: true)
                }
            }
        }
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .medium
        return formatter
    }()
}
