import Kingfisher
import SnapKit
import UIKit

final class LocalAlbumPickerViewController: UIViewController {
    private enum Section: Hashable {
        case albums
    }

    private typealias DataSource = UICollectionViewDiffableDataSource<Section, LocalAlbumDescriptor>
    private typealias Snapshot = NSDiffableDataSourceSnapshot<Section, LocalAlbumDescriptor>

    private enum Layout {
        static let baseInset: CGFloat = 16
        static let itemSpacing: CGFloat = 14
        static let maximumItemWidth: CGFloat = 220
        static let minimumItemWidth: CGFloat = 96

        struct Metrics {
            let itemWidth: CGFloat
            let contentWidth: CGFloat
            let sideInset: CGFloat
        }

        static func metrics(for availableWidth: CGFloat) -> Metrics {
            let twoColumnWidth = floor((availableWidth - baseInset * 2 - itemSpacing) / 2)
            let itemWidth = max(minimumItemWidth, min(maximumItemWidth, twoColumnWidth))
            let contentWidth = itemWidth * 2 + itemSpacing
            let sideInset = max(baseInset, floor((availableWidth - contentWidth) / 2))

            return Metrics(
                itemWidth: itemWidth,
                contentWidth: contentWidth,
                sideInset: sideInset
            )
        }
    }

    private let photoLibraryService: PhotoLibraryService
    private let onDone: ([LocalAlbumDescriptor]) -> Void

    private var albums: [LocalAlbumDescriptor] = []
    private var selectedAlbumIDs: Set<String>
    private var albumLoadTask: Task<Void, Never>?

    private var dataSource: DataSource?
    private lazy var collectionView = UICollectionView(
        frame: .zero,
        collectionViewLayout: Self.makeCollectionLayout()
    )
    private lazy var doneBarButtonItem = UIBarButtonItem(
        systemItem: .done,
        primaryAction: UIAction { [weak self] _ in
            self?.doneTapped()
        }
    )

    private lazy var emptyStateView: UIView = makeAlbumEmptyStateView(
        title: String(localized: "home.localAlbums.empty"),
        message: String(localized: "home.localAlbums.emptyMessage")
    )

    init(
        photoLibraryService: PhotoLibraryService,
        selectedAlbumIDs: Set<String>,
        onDone: @escaping ([LocalAlbumDescriptor]) -> Void
    ) {
        self.photoLibraryService = photoLibraryService
        self.selectedAlbumIDs = selectedAlbumIDs
        self.onDone = onDone
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        albumLoadTask?.cancel()
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .appBackground
        title = String(localized: "home.localAlbums.title")

        configureUI()
        reloadAlbums()
    }

    private func configureUI() {
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            systemItem: .cancel,
            primaryAction: UIAction { [weak self] _ in
                self?.dismiss(animated: ConsideringUser.animated)
            }
        )
        navigationItem.rightBarButtonItem = doneBarButtonItem

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

    private func reloadAlbums() {
        albumLoadTask?.cancel()
        let photoLibraryService = photoLibraryService
        albumLoadTask = Task { [weak self] in
            let albums = await withCancellableDetachedValue(priority: .userInitiated) {
                photoLibraryService.fetchUserAlbums(shouldCancel: { Task.isCancelled })
            }

            guard let self, !Task.isCancelled else { return }
            self.albums = albums
            self.selectedAlbumIDs.formIntersection(Set(albums.map(\.localIdentifier)))
            self.updateDoneButton()
            self.updateEmptyState()
            self.applySnapshot(animatingDifferences: false)
        }
    }

    private func configureDataSource() {
        let cellRegistration = UICollectionView.CellRegistration<LocalAlbumCell, LocalAlbumDescriptor> { [weak self] cell, _, album in
            guard let self else { return }
            cell.configure(
                with: album,
                isSelected: selectedAlbumIDs.contains(album.localIdentifier),
                thumbnailPixelSide: cell.thumbnailPixelSide(fallback: Layout.maximumItemWidth)
            )
            cell.onToggleTapped = { [weak self] in
                self?.toggleAlbum(withLocalIdentifier: album.localIdentifier)
            }
        }

        dataSource = DataSource(collectionView: collectionView) { collectionView, indexPath, album in
            collectionView.dequeueConfiguredReusableCell(
                using: cellRegistration,
                for: indexPath,
                item: album
            )
        }
    }

    private func applySnapshot(animatingDifferences: Bool) {
        var snapshot = Snapshot()
        snapshot.appendSections([.albums])
        snapshot.appendItems(albums, toSection: .albums)
        dataSource?.apply(snapshot, animatingDifferences: animatingDifferences)
    }

    private func reconfigureAlbum(withLocalIdentifier localIdentifier: String) {
        guard let album = albums.first(where: { $0.localIdentifier == localIdentifier }),
              var snapshot = dataSource?.snapshot()
        else { return }

        snapshot.reconfigureItems([album])
        dataSource?.apply(snapshot, animatingDifferences: true)
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
                widthDimension: .absolute(metrics.contentWidth),
                heightDimension: .absolute(metrics.itemWidth)
            )
            let group = NSCollectionLayoutGroup.horizontal(layoutSize: groupSize, repeatingSubitem: item, count: 2)
            group.interItemSpacing = .fixed(Layout.itemSpacing)

            let section = NSCollectionLayoutSection(group: group)
            section.interGroupSpacing = Layout.itemSpacing
            section.contentInsets = NSDirectionalEdgeInsets(
                top: Layout.baseInset,
                leading: metrics.sideInset,
                bottom: Layout.baseInset,
                trailing: metrics.sideInset
            )
            return section
        }
    }

    private func updateDoneButton() {
        doneBarButtonItem.isEnabled = !selectedAlbumIDs.isEmpty
    }

    private func updateEmptyState() {
        collectionView.backgroundView = albums.isEmpty ? emptyStateView : nil
    }

    private func doneTapped() {
        let selectedAlbums = albums.filter { selectedAlbumIDs.contains($0.localIdentifier) }
        guard !selectedAlbums.isEmpty else { return }
        onDone(selectedAlbums)
        dismiss(animated: ConsideringUser.animated)
    }

    private func toggleAlbum(withLocalIdentifier localIdentifier: String) {
        if selectedAlbumIDs.contains(localIdentifier) {
            selectedAlbumIDs.remove(localIdentifier)
        } else {
            selectedAlbumIDs.insert(localIdentifier)
        }

        updateDoneButton()
        reconfigureAlbum(withLocalIdentifier: localIdentifier)
    }
}

extension LocalAlbumPickerViewController: UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        collectionView.deselectItem(at: indexPath, animated: true)
        guard let album = dataSource?.itemIdentifier(for: indexPath) else { return }
        let viewController = LocalAlbumDetailViewController(
            album: album,
            photoLibraryService: photoLibraryService
        )
        if let sheetPresentationController = navigationController?.sheetPresentationController {
            sheetPresentationController.animateChanges {
                sheetPresentationController.selectedDetentIdentifier = .large
            }
        }
        navigationController?.pushViewController(viewController, animated: ConsideringUser.animated)
    }
}

private final class LocalAlbumCell: UICollectionViewCell {
    var onToggleTapped: (() -> Void)?

    private let imageView = UIImageView()
    private let topGradientView = GradientView(
        colors: [
            UIColor.black.withAlphaComponent(0.48),
            UIColor.black.withAlphaComponent(0.0)
        ],
        startPoint: CGPoint(x: 0.5, y: 0),
        endPoint: CGPoint(x: 0.5, y: 1),
        locations: [0, 1]
    )
    private let bottomGradientView = GradientView(
        colors: [
            UIColor.black.withAlphaComponent(0.0),
            UIColor.black.withAlphaComponent(0.58)
        ],
        startPoint: CGPoint(x: 0.5, y: 0),
        endPoint: CGPoint(x: 0.5, y: 1),
        locations: [0, 1]
    )
    private let countLabel = UILabel()
    private let titleLabel = UILabel()
    private let toggleButton = UIButton(type: .system)
    private var thumbnailRequest: PHAssetThumbnailRequest?
    private var loadedThumbnailKey: ThumbnailKey?

    private struct ThumbnailKey: Equatable {
        let assetIdentifier: String
        let pixelSide: Int
    }

    private static let placeholderImage = UIImage(systemName: "photo.on.rectangle")

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
        loadedThumbnailKey = nil
        imageView.image = nil
        onToggleTapped = nil
    }

    func configure(with album: LocalAlbumDescriptor, isSelected: Bool, thumbnailPixelSide: Int) {
        titleLabel.text = album.title
        countLabel.text = NumberFormatter.localizedString(from: NSNumber(value: album.assetCount), number: .decimal)
        updateToggle(isSelected: isSelected)

        accessibilityLabel = album.title
        accessibilityValue = String.localizedStringWithFormat(
            String(localized: "home.localAlbums.assetCount"),
            album.assetCount
        )
        toggleButton.accessibilityLabel = album.title

        guard let thumbnailAssetIdentifier = album.thumbnailAssetIdentifier else {
            thumbnailRequest?.cancel()
            thumbnailRequest = nil
            loadedThumbnailKey = nil
            imageView.contentMode = .center
            imageView.tintColor = .tertiaryLabel
            imageView.image = Self.placeholderImage
            return
        }

        let key = ThumbnailKey(assetIdentifier: thumbnailAssetIdentifier, pixelSide: thumbnailPixelSide)
        guard loadedThumbnailKey != key else { return }

        thumbnailRequest?.cancel()
        loadedThumbnailKey = key
        imageView.contentMode = .scaleAspectFill
        imageView.tintColor = nil
        imageView.image = nil
        thumbnailRequest = PHAssetThumbnailLoader.setImage(
            assetLocalIdentifier: thumbnailAssetIdentifier,
            pixelSide: thumbnailPixelSide,
            on: imageView,
            fadeDuration: 0.15
        )
    }

    private func configureUI() {
        contentView.backgroundColor = .secondarySystemGroupedBackground
        contentView.layer.cornerRadius = 10
        contentView.layer.cornerCurve = .continuous
        contentView.clipsToBounds = true

        imageView.backgroundColor = .secondarySystemGroupedBackground
        imageView.clipsToBounds = true

        countLabel.textColor = .white
        countLabel.font = .preferredFont(forTextStyle: .caption1).withWeight(.semibold)
        countLabel.adjustsFontForContentSizeCategory = true
        countLabel.textAlignment = .right
        countLabel.shadowColor = UIColor.black.withAlphaComponent(0.45)
        countLabel.shadowOffset = CGSize(width: 0, height: 1)

        titleLabel.textColor = .white
        titleLabel.font = .preferredFont(forTextStyle: .subheadline).withWeight(.semibold)
        titleLabel.adjustsFontForContentSizeCategory = true
        titleLabel.numberOfLines = 2
        titleLabel.lineBreakMode = .byTruncatingTail
        titleLabel.shadowColor = UIColor.black.withAlphaComponent(0.45)
        titleLabel.shadowOffset = CGSize(width: 0, height: 1)

        toggleButton.backgroundColor = .clear
        toggleButton.layer.cornerRadius = 15
        toggleButton.layer.cornerCurve = .continuous
        toggleButton.tintColor = .white
        toggleButton.setPreferredSymbolConfiguration(
            UIImage.SymbolConfiguration(pointSize: 21, weight: .medium),
            forImageIn: .normal
        )
        toggleButton.addTarget(self, action: #selector(toggleTapped), for: .touchUpInside)

        contentView.addSubview(imageView)
        contentView.addSubview(topGradientView)
        contentView.addSubview(bottomGradientView)
        contentView.addSubview(countLabel)
        contentView.addSubview(titleLabel)
        contentView.addSubview(toggleButton)

        imageView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
        }
        topGradientView.snp.makeConstraints { make in
            make.top.leading.trailing.equalToSuperview()
            make.height.equalToSuperview().multipliedBy(0.44)
        }
        bottomGradientView.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview()
            make.height.equalToSuperview().multipliedBy(0.52)
        }
        toggleButton.snp.makeConstraints { make in
            make.top.leading.equalToSuperview().offset(10)
            make.size.equalTo(CGSize(width: 30, height: 30))
        }
        countLabel.snp.makeConstraints { make in
            make.top.trailing.equalToSuperview().inset(12)
            make.leading.greaterThanOrEqualTo(toggleButton.snp.trailing).offset(8)
        }
        titleLabel.snp.makeConstraints { make in
            make.leading.trailing.bottom.equalToSuperview().inset(12)
        }
    }

    private func updateToggle(isSelected: Bool) {
        let imageName = isSelected ? "checkmark.circle.fill" : "circle"
        toggleButton.setImage(UIImage(systemName: imageName), for: .normal)
        toggleButton.backgroundColor = .clear
        toggleButton.tintColor = isSelected ? .appTint : UIColor.white.withAlphaComponent(0.66)
        toggleButton.accessibilityTraits = isSelected ? [.button, .selected] : .button
    }

    @objc private func toggleTapped() {
        onToggleTapped?()
    }
}
