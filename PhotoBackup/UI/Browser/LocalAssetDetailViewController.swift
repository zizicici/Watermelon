import Photos
import SnapKit
import UIKit

final class LocalAssetDetailViewController: UIViewController {
    private let dependencies: DependencyContainer
    private let asset: PHAsset
    private let isBackedUp: Bool

    private let imageView = UIImageView()
    private let infoLabel = UILabel()

    init(dependencies: DependencyContainer, asset: PHAsset, isBackedUp: Bool) {
        self.dependencies = dependencies
        self.asset = asset
        self.isBackedUp = isBackedUp
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground

        imageView.contentMode = .scaleAspectFit
        imageView.backgroundColor = .secondarySystemBackground

        infoLabel.numberOfLines = 0
        infoLabel.font = .systemFont(ofSize: 14)

        view.addSubview(imageView)
        view.addSubview(infoLabel)

        imageView.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(16)
            make.height.equalTo(imageView.snp.width)
        }

        infoLabel.snp.makeConstraints { make in
            make.top.equalTo(imageView.snp.bottom).offset(16)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        dependencies.photoLibraryService.requestThumbnail(for: asset, targetSize: CGSize(width: 1200, height: 1200)) { [weak self] image in
            self?.imageView.image = image
        }

        let creation = asset.creationDate.map { Self.dateFormatter.string(from: $0) } ?? "N/A"
        let modified = asset.modificationDate.map { Self.dateFormatter.string(from: $0) } ?? "N/A"
        let mediaType = PhotoLibraryService.mediaTypeName(for: asset)

        infoLabel.text = [
            "Local Identifier: \(asset.localIdentifier)",
            "Type: \(mediaType)",
            "Creation: \(creation)",
            "Modification: \(modified)",
            "Resolution: \(asset.pixelWidth)x\(asset.pixelHeight)",
            "Duration: \(String(format: "%.1fs", asset.duration))",
            "Live Photo: \(PhotoLibraryService.isLivePhoto(asset) ? "Yes" : "No")",
            "Backed Up: \(isBackedUp ? "Yes" : "No")"
        ].joined(separator: "\n")
    }

    private static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .medium
        return formatter
    }()
}
