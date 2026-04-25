import Kingfisher
import Photos
import SnapKit
import UIKit

fileprivate struct LocalAlbumAssetItem: Hashable, Sendable {
    enum MediaKind: Hashable, Sendable {
        case image
        case video
    }

    let localIdentifier: String
    let mediaKind: MediaKind
    let duration: TimeInterval
    let isLivePhoto: Bool
}

final class LocalAlbumDetailViewController: UIViewController {
    private enum Section: Hashable {
        case assets
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<Section, LocalAlbumAssetItem>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<Section, LocalAlbumAssetItem>

    private enum Layout {
        static let spacing: CGFloat = 2
        static let maximumItemWidth: CGFloat = 132
        static let minimumColumnCount = 3

        struct Metrics {
            let columnCount: Int
            let itemWidth: CGFloat
        }

        static func metrics(for availableWidth: CGFloat) -> Metrics {
            guard availableWidth > 0 else {
                return Metrics(columnCount: minimumColumnCount, itemWidth: maximumItemWidth)
            }

            let rawColumnCount = Int(ceil((availableWidth + spacing) / (maximumItemWidth + spacing)))
            let columnCount = max(minimumColumnCount, rawColumnCount)
            let itemWidth = floor((availableWidth - CGFloat(columnCount - 1) * spacing) / CGFloat(columnCount))
            return Metrics(columnCount: columnCount, itemWidth: itemWidth)
        }
    }

    private let album: LocalAlbumDescriptor
    private let photoLibraryService: PhotoLibraryService

    private var assets: [LocalAlbumAssetItem] = []
    private var assetLoadTask: Task<Void, Never>?
    private var dataSource: DataSource?
    private lazy var collectionView = UICollectionView(
        frame: .zero,
        collectionViewLayout: Self.makeCollectionLayout()
    )

    private lazy var emptyStateView: UIView = makeAlbumEmptyStateView(
        title: String(localized: "home.localAlbumDetail.empty"),
        message: String(localized: "home.localAlbumDetail.emptyMessage")
    )

    init(album: LocalAlbumDescriptor, photoLibraryService: PhotoLibraryService) {
        self.album = album
        self.photoLibraryService = photoLibraryService
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        assetLoadTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = album.title

        configureUI()
        reloadAssets()
    }

    private func configureUI() {
        collectionView.backgroundColor = .appBackground
        collectionView.alwaysBounceVertical = true
        collectionView.contentInsetAdjustmentBehavior = .always
        collectionView.delegate = self

        view.addSubview(collectionView)
        collectionView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }

        configureDataSource()
    }

    private func configureDataSource() {
        let cellRegistration = UICollectionView.CellRegistration<LocalAlbumAssetCell, LocalAlbumAssetItem> { cell, _, item in
            cell.configure(with: item, thumbnailPixelSide: cell.thumbnailPixelSide(fallback: Layout.maximumItemWidth))
        }

        dataSource = DataSource(collectionView: collectionView) { collectionView, indexPath, item in
            collectionView.dequeueConfiguredReusableCell(
                using: cellRegistration,
                for: indexPath,
                item: item
            )
        }
    }

    private func reloadAssets() {
        assetLoadTask?.cancel()
        let photoLibraryService = photoLibraryService
        let albumIdentifier = album.localIdentifier
        assetLoadTask = Task { [weak self] in
            let assets = await withCancellableDetachedValue(priority: .userInitiated) {
                photoLibraryService
                    .fetchAssets(inAlbumIdentifiers: [albumIdentifier], shouldCancel: { Task.isCancelled })
                    .map(Self.makeAssetItem)
            }

            guard let self, !Task.isCancelled else { return }
            self.assets = assets
            self.updateEmptyState()
            self.applySnapshot(animatingDifferences: false)
        }
    }

    private func applySnapshot(animatingDifferences: Bool) {
        var snapshot = Snapshot()
        snapshot.appendSections([.assets])
        snapshot.appendItems(assets, toSection: .assets)
        dataSource?.apply(snapshot, animatingDifferences: animatingDifferences)
    }

    private func updateEmptyState() {
        collectionView.backgroundView = assets.isEmpty ? emptyStateView : nil
    }

    nonisolated private static func makeAssetItem(from asset: PHAsset) -> LocalAlbumAssetItem {
        LocalAlbumAssetItem(
            localIdentifier: asset.localIdentifier,
            mediaKind: asset.mediaType == .video ? .video : .image,
            duration: asset.duration,
            isLivePhoto: asset.mediaSubtypes.contains(.photoLive)
        )
    }

    private static func makeCollectionLayout() -> UICollectionViewCompositionalLayout {
        UICollectionViewCompositionalLayout { _, environment in
            let metrics = Layout.metrics(for: environment.container.effectiveContentSize.width)
            let itemSize = NSCollectionLayoutSize(
                widthDimension: .absolute(metrics.itemWidth),
                heightDimension: .absolute(metrics.itemWidth)
            )
            let item = NSCollectionLayoutItem(layoutSize: itemSize)
            let groupSize = NSCollectionLayoutSize(
                widthDimension: .fractionalWidth(1.0),
                heightDimension: .absolute(metrics.itemWidth)
            )
            let group = NSCollectionLayoutGroup.horizontal(
                layoutSize: groupSize,
                repeatingSubitem: item,
                count: metrics.columnCount
            )
            group.interItemSpacing = .fixed(Layout.spacing)

            let section = NSCollectionLayoutSection(group: group)
            section.interGroupSpacing = Layout.spacing
            return section
        }
    }

}

extension LocalAlbumDetailViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
    }
}

private final class LocalAlbumAssetCell: UICollectionViewCell {
    private let imageView = UIImageView()
    private let bottomGradientView = GradientView(
        colors: [
            UIColor.black.withAlphaComponent(0.0),
            UIColor.black.withAlphaComponent(0.52)
        ],
        startPoint: CGPoint(x: 0.5, y: 0),
        endPoint: CGPoint(x: 0.5, y: 1),
        locations: [0, 1]
    )
    private let durationIconView = UIImageView()
    private let durationLabel = UILabel()
    private let livePhotoIconView = UIImageView()
    private var thumbnailRequest: PHAssetThumbnailRequest?

    private static let placeholderImage = UIImage(systemName: "photo")
    private static let videoPlaceholderImage = UIImage(systemName: "video")

    override init(frame: CGRect) {
        super.init(frame: frame)
        configureUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        thumbnailRequest?.cancel()
    }

    override var isHighlighted: Bool {
        didSet {
            contentView.alpha = isHighlighted ? 0.82 : 1.0
        }
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        thumbnailRequest?.cancel()
        thumbnailRequest = nil
        imageView.image = nil
        bottomGradientView.isHidden = true
        durationIconView.isHidden = true
        durationLabel.isHidden = true
        livePhotoIconView.isHidden = true
    }

    func configure(with item: LocalAlbumAssetItem, thumbnailPixelSide: Int) {
        imageView.contentMode = .scaleAspectFill
        imageView.tintColor = .tertiaryLabel
        imageView.image = item.mediaKind == .video ? Self.videoPlaceholderImage : Self.placeholderImage

        thumbnailRequest?.cancel()
        thumbnailRequest = PHAssetThumbnailLoader.setImage(
            assetLocalIdentifier: item.localIdentifier,
            pixelSide: thumbnailPixelSide,
            on: imageView,
            fadeDuration: 0.12
        )

        configureBadges(for: item)
    }

    private func configureUI() {
        contentView.backgroundColor = .secondarySystemGroupedBackground
        contentView.clipsToBounds = true

        imageView.backgroundColor = .secondarySystemGroupedBackground
        imageView.clipsToBounds = true

        durationIconView.image = UIImage(systemName: "video.fill")
        durationIconView.tintColor = .white
        durationIconView.contentMode = .scaleAspectFit

        durationLabel.textColor = .white
        durationLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .semibold)
        durationLabel.adjustsFontSizeToFitWidth = true
        durationLabel.minimumScaleFactor = 0.75
        durationLabel.shadowColor = UIColor.black.withAlphaComponent(0.45)
        durationLabel.shadowOffset = CGSize(width: 0, height: 1)

        livePhotoIconView.image = UIImage(systemName: "livephoto")
        livePhotoIconView.tintColor = .white
        livePhotoIconView.contentMode = .scaleAspectFit
        livePhotoIconView.layer.shadowColor = UIColor.black.cgColor
        livePhotoIconView.layer.shadowOpacity = 0.35
        livePhotoIconView.layer.shadowRadius = 2
        livePhotoIconView.layer.shadowOffset = CGSize(width: 0, height: 1)

        contentView.addSubview(imageView)
        contentView.addSubview(bottomGradientView)
        contentView.addSubview(durationIconView)
        contentView.addSubview(durationLabel)
        contentView.addSubview(livePhotoIconView)

        imageView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
        bottomGradientView.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalToSuperview().multipliedBy(0.42)
        }
        durationLabel.snp.makeConstraints { make in
            make.trailing.bottom.equalToSuperview().inset(6)
        }
        durationIconView.snp.makeConstraints { make in
            make.centerY.equalTo(durationLabel)
            make.trailing.equalTo(durationLabel.snp.leading).offset(-4)
            make.size.equalTo(12)
            make.leading.greaterThanOrEqualToSuperview().offset(6)
        }
        livePhotoIconView.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview().inset(6)
            make.size.equalTo(16)
        }
    }

    private func configureBadges(for item: LocalAlbumAssetItem) {
        switch item.mediaKind {
        case .image:
            bottomGradientView.isHidden = true
            durationIconView.isHidden = true
            durationLabel.isHidden = true
        case .video:
            bottomGradientView.isHidden = false
            durationIconView.isHidden = false
            durationLabel.isHidden = false
            durationLabel.text = Self.formatDuration(item.duration)
        }

        livePhotoIconView.isHidden = !item.isLivePhoto
    }

    private static func formatDuration(_ duration: TimeInterval) -> String {
        let totalSeconds = max(0, Int(duration.rounded()))
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let seconds = totalSeconds % 60

        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%d:%02d", minutes, seconds)
    }
}
