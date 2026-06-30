import Kingfisher
import Photos
import SnapKit
import UIKit

struct RemoteBrowserAsset: Hashable, Sendable {
    let fingerprint: Data
    let month: LibraryMonthKey
    let creationDateMs: Int64
    let isVideo: Bool
    let isLivePhoto: Bool
    let photoRemoteRelativePath: String?
    let videoRemoteRelativePath: String?

    var fingerprintHex: String { fingerprint.hexString }
}

// Projects a remote snapshot into per-month, date-sorted browser assets. Mirrors the resolver drop
// rule (assets without a resolvable link are skipped) and the display-resource priority used elsewhere.
enum RemoteBrowserAssetBuilder {
    static func build(from state: RemoteLibrarySnapshotState) -> (months: [LibraryMonthKey], assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]]) {
        var assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]] = [:]
        for delta in state.monthDeltas {
            let resourceByHash = Dictionary(delta.resources.map { ($0.contentHash, $0) }, uniquingKeysWith: { first, _ in first })
            var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
            for link in delta.assetResourceLinks {
                linksByFingerprint[link.assetFingerprint, default: []].append(link)
            }
            var items: [RemoteBrowserAsset] = []
            for asset in delta.assets {
                // Mirror RemoteMonthResolver's drop rule: only links whose resource is actually present
                // count (partial-flush assets are skipped), and classification runs over resolvable links.
                let links = (linksByFingerprint[asset.assetFingerprint] ?? [])
                    .filter { resourceByHash[$0.resourceHash] != nil }
                guard !links.isEmpty else { continue }
                items.append(makeAsset(asset: asset, links: links, resourceByHash: resourceByHash, month: delta.month))
            }
            items.sort { $0.creationDateMs > $1.creationDateMs }
            if !items.isEmpty { assetsByMonth[delta.month] = items }
        }
        let months = assetsByMonth.keys.sorted(by: >)
        return (months, assetsByMonth)
    }

    private static func makeAsset(
        asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        resourceByHash: [Data: RemoteManifestResource],
        month: LibraryMonthKey
    ) -> RemoteBrowserAsset {
        let roles = links.map(\.role)
        let hasPaired = roles.contains { ResourceTypeCode.isPairedVideo($0) }
        let hasPhoto = roles.contains { ResourceTypeCode.isPhotoLike($0) }
        let hasVideo = roles.contains { ResourceTypeCode.isVideoLike($0) }
        let isLivePhoto = hasPaired && hasPhoto
        // Match RemoteMonthResolver: anything with a video resource that isn't a Live Photo is a video.
        let isVideo = !isLivePhoto && hasVideo

        func resource(preferring rolePriority: [Int]) -> RemoteManifestResource? {
            for role in rolePriority {
                if let link = links.first(where: { $0.role == role && $0.slot == 0 }) ?? links.first(where: { $0.role == role }),
                   let resource = resourceByHash[link.resourceHash] {
                    return resource
                }
            }
            return nil
        }
        let photoResource = resource(preferring: [
            ResourceTypeCode.photo, ResourceTypeCode.fullSizePhoto,
            ResourceTypeCode.alternatePhoto, ResourceTypeCode.photoProxy,
        ])
        let videoResource = resource(preferring: [
            ResourceTypeCode.video, ResourceTypeCode.fullSizeVideo, ResourceTypeCode.pairedVideo,
        ])

        return RemoteBrowserAsset(
            fingerprint: asset.assetFingerprint,
            month: month,
            creationDateMs: asset.creationDateMs ?? asset.backedUpAtMs,
            isVideo: isVideo,
            isLivePhoto: isLivePhoto,
            photoRemoteRelativePath: photoResource?.remoteRelativePath,
            videoRemoteRelativePath: videoResource?.remoteRelativePath
        )
    }
}

final class RemoteLibraryBrowserViewController: UIViewController {
    private enum Layout {
        static let spacing: CGFloat = 2
        static let maximumItemWidth: CGFloat = 132
        static let minimumColumnCount = 3
        static let headerHeight: CGFloat = 44

        static func metrics(for availableWidth: CGFloat) -> (columnCount: Int, itemWidth: CGFloat) {
            guard availableWidth > 0 else { return (minimumColumnCount, maximumItemWidth) }
            let rawColumnCount = Int(ceil((availableWidth + spacing) / (maximumItemWidth + spacing)))
            let columnCount = max(minimumColumnCount, rawColumnCount)
            let itemWidth = floor((availableWidth - CGFloat(columnCount - 1) * spacing) / CGFloat(columnCount))
            return (columnCount, itemWidth)
        }
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<LibraryMonthKey, RemoteBrowserAsset>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<LibraryMonthKey, RemoteBrowserAsset>
    private static let headerKind = "month-header"

    private let dependencies: DependencyContainer
    private let service: RemoteThumbnailService

    private var months: [LibraryMonthKey] = []
    private var assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]] = [:]
    private var dataSource: DataSource?
    private var loadTask: Task<Void, Never>?

    private lazy var collectionView = UICollectionView(frame: .zero, collectionViewLayout: makeLayout())
    private lazy var emptyStateView = makeAlbumEmptyStateView(
        title: String(localized: "remoteBrowser.empty.title"),
        message: String(localized: "remoteBrowser.empty.message")
    )

    init(dependencies: DependencyContainer, service: RemoteThumbnailService) {
        self.dependencies = dependencies
        self.service = service
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        let service = service
        Task { await service.shutdown() }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "remoteBrowser.title")
        configureUI()
        configureDataSource()
        load()
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
    }

    private func configureDataSource() {
        let service = service
        let cellRegistration = UICollectionView.CellRegistration<RemoteBrowserCell, RemoteBrowserAsset> { cell, _, asset in
            cell.configure(with: asset, service: service)
        }
        let dataSource = DataSource(collectionView: collectionView) { collectionView, indexPath, asset in
            collectionView.dequeueConfiguredReusableCell(using: cellRegistration, for: indexPath, item: asset)
        }
        let headerRegistration = UICollectionView.SupplementaryRegistration<RemoteBrowserHeaderView>(
            elementKind: Self.headerKind
        ) { [weak self] header, _, indexPath in
            guard let self, indexPath.section < self.months.count else { return }
            header.configure(title: self.months[indexPath.section].displayText)
        }
        dataSource.supplementaryViewProvider = { collectionView, _, indexPath in
            collectionView.dequeueConfiguredReusableSupplementary(using: headerRegistration, for: indexPath)
        }
        self.dataSource = dataSource
    }

    private func load() {
        loadTask?.cancel()
        let coordinator = dependencies.backupCoordinator
        loadTask = Task { [weak self] in
            await self?.service.prepareLocalIndex()
            let built = await withCancellableDetachedValue(priority: .userInitiated) {
                RemoteBrowserAssetBuilder.build(from: coordinator.currentRemoteSnapshotState(since: nil))
            }
            guard let self, !Task.isCancelled else { return }
            self.months = built.months
            self.assetsByMonth = built.assetsByMonth
            self.applySnapshot()
            self.collectionView.backgroundView = built.months.isEmpty ? self.emptyStateView : nil
        }
    }

    private func applySnapshot() {
        var snapshot = Snapshot()
        snapshot.appendSections(months)
        for month in months {
            snapshot.appendItems(assetsByMonth[month] ?? [], toSection: month)
        }
        dataSource?.apply(snapshot, animatingDifferences: false)
    }

    private func makeLayout() -> UICollectionViewCompositionalLayout {
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
            let headerSize = NSCollectionLayoutSize(
                widthDimension: .fractionalWidth(1.0),
                heightDimension: .absolute(Layout.headerHeight)
            )
            let header = NSCollectionLayoutBoundarySupplementaryItem(
                layoutSize: headerSize,
                elementKind: Self.headerKind,
                alignment: .top
            )
            header.pinToVisibleBounds = true
            section.boundarySupplementaryItems = [header]
            section.contentInsets = NSDirectionalEdgeInsets(top: 0, leading: 0, bottom: Layout.spacing * 4, trailing: 0)
            return section
        }
    }
}

extension RemoteLibraryBrowserViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard presentedViewController == nil else { return }
        guard let asset = dataSource?.itemIdentifier(for: indexPath) else { return }
        let viewer = RemoteMediaViewerViewController(service: service, asset: asset)
        let nav = UINavigationController(rootViewController: viewer)
        nav.modalPresentationStyle = .fullScreen
        present(nav, animated: true)
    }
}

private final class RemoteBrowserHeaderView: UICollectionReusableView {
    private let label = UILabel()

    override init(frame: CGRect) {
        super.init(frame: frame)
        let blur = UIBlurEffect(style: .systemThinMaterial)
        let background = UIVisualEffectView(effect: blur)
        addSubview(background)
        background.snp.makeConstraints { make in make.edges.equalToSuperview() }

        label.font = .preferredFont(forTextStyle: .headline)
        label.adjustsFontForContentSizeCategory = true
        label.textColor = .label
        addSubview(label)
        label.snp.makeConstraints { make in
            make.leading.trailing.equalToSuperview().inset(12)
            make.centerY.equalToSuperview()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func configure(title: String) {
        label.text = title
    }
}

private final class RemoteBrowserCell: UICollectionViewCell {
    private let imageView = UIImageView()
    private let bottomGradientView = GradientView(
        colors: [UIColor.black.withAlphaComponent(0.0), UIColor.black.withAlphaComponent(0.52)],
        startPoint: CGPoint(x: 0.5, y: 0),
        endPoint: CGPoint(x: 0.5, y: 1),
        locations: [0, 1]
    )
    private let videoIconView = UIImageView()
    private let livePhotoIconView = UIImageView()
    private let needsLoadIconView = UIImageView()
    private var loadTask: Task<Void, Never>?
    private var currentFingerprint: Data?

    private static let photoPlaceholder = UIImage(systemName: "photo")
    private static let videoPlaceholder = UIImage(systemName: "video")

    override init(frame: CGRect) {
        super.init(frame: frame)
        configureUI()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
    }

    override var isHighlighted: Bool {
        didSet { contentView.alpha = isHighlighted ? 0.82 : 1.0 }
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        loadTask?.cancel()
        loadTask = nil
        currentFingerprint = nil
        imageView.image = nil
        bottomGradientView.isHidden = true
        videoIconView.isHidden = true
        livePhotoIconView.isHidden = true
        needsLoadIconView.isHidden = true
    }

    func configure(with asset: RemoteBrowserAsset, service: RemoteThumbnailService) {
        currentFingerprint = asset.fingerprint
        imageView.contentMode = .scaleAspectFill
        imageView.tintColor = .tertiaryLabel
        imageView.image = asset.isVideo ? Self.videoPlaceholder : Self.photoPlaceholder

        bottomGradientView.isHidden = !asset.isVideo
        videoIconView.isHidden = !asset.isVideo
        livePhotoIconView.isHidden = !asset.isLivePhoto
        needsLoadIconView.isHidden = true

        if let cached = service.memoryCachedThumbnail(for: asset.fingerprint) {
            imageView.image = cached
            return
        }

        let fingerprint = asset.fingerprint
        let isVideo = asset.isVideo
        loadTask = Task { [weak self] in
            let image = await service.resolveAutoThumbnail(for: fingerprint)
            guard let self, !Task.isCancelled, self.currentFingerprint == fingerprint else { return }
            if let image {
                self.setThumbnail(image)
            } else if !isVideo {
                // Not on device and no sidecar — show a "tap to view/download" hint, no auto original pull.
                self.needsLoadIconView.isHidden = false
            }
        }
    }

    private func setThumbnail(_ image: UIImage) {
        UIView.transition(with: imageView, duration: 0.12, options: [.transitionCrossDissolve, .allowUserInteraction]) {
            self.imageView.image = image
        }
    }

    private func configureUI() {
        contentView.backgroundColor = .secondarySystemGroupedBackground
        contentView.clipsToBounds = true

        imageView.backgroundColor = .secondarySystemGroupedBackground
        imageView.clipsToBounds = true

        videoIconView.image = UIImage(systemName: "play.circle.fill")
        videoIconView.tintColor = .white
        videoIconView.contentMode = .scaleAspectFit
        videoIconView.layer.shadowColor = UIColor.black.cgColor
        videoIconView.layer.shadowOpacity = 0.35
        videoIconView.layer.shadowRadius = 2
        videoIconView.layer.shadowOffset = CGSize(width: 0, height: 1)

        livePhotoIconView.image = UIImage(systemName: "livephoto")
        livePhotoIconView.tintColor = .white
        livePhotoIconView.contentMode = .scaleAspectFit
        livePhotoIconView.layer.shadowColor = UIColor.black.cgColor
        livePhotoIconView.layer.shadowOpacity = 0.35
        livePhotoIconView.layer.shadowRadius = 2
        livePhotoIconView.layer.shadowOffset = CGSize(width: 0, height: 1)

        needsLoadIconView.image = UIImage(systemName: "arrow.down.circle")
        needsLoadIconView.tintColor = .secondaryLabel
        needsLoadIconView.contentMode = .scaleAspectFit

        contentView.addSubview(imageView)
        contentView.addSubview(bottomGradientView)
        contentView.addSubview(videoIconView)
        contentView.addSubview(livePhotoIconView)
        contentView.addSubview(needsLoadIconView)

        imageView.snp.makeConstraints { make in make.edges.equalToSuperview() }
        bottomGradientView.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalToSuperview().multipliedBy(0.42)
        }
        videoIconView.snp.makeConstraints { make in
            make.leading.bottom.equalToSuperview().inset(6)
            make.size.equalTo(18)
        }
        livePhotoIconView.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview().inset(6)
            make.size.equalTo(16)
        }
        needsLoadIconView.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.size.equalTo(24)
        }
    }
}
