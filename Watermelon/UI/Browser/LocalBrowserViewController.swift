import Photos
import GRDB
import SnapKit
import UIKit

final class LocalBrowserViewController: UIViewController {
    private let dependencies: DependencyContainer

    private let filterControl = UISegmentedControl(items: ["All", "Not Backed Up"])
    private let collectionView: UICollectionView
    private let statusLabel = UILabel()

    private var allAssets: [PHAsset] = []
    private var visibleAssets: [PHAsset] = []
    private var backedUpAssetIDs: Set<String> = []

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
        Task { await loadData() }
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
        filterControl.selectedSegmentIndex = 0
        filterControl.addTarget(self, action: #selector(filterChanged), for: .valueChanged)

        statusLabel.font = .systemFont(ofSize: 13, weight: .medium)
        statusLabel.textColor = .secondaryLabel

        collectionView.backgroundColor = .clear
        collectionView.dataSource = self
        collectionView.delegate = self
        collectionView.alwaysBounceVertical = true
        collectionView.register(PhotoGridCell.self, forCellWithReuseIdentifier: PhotoGridCell.reuseID)

        view.addSubview(filterControl)
        view.addSubview(statusLabel)
        view.addSubview(collectionView)

        filterControl.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(8)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        statusLabel.snp.makeConstraints { make in
            make.top.equalTo(filterControl.snp.bottom).offset(8)
            make.leading.trailing.equalToSuperview().inset(16)
        }

        collectionView.snp.makeConstraints { make in
            make.top.equalTo(statusLabel.snp.bottom).offset(8)
            make.leading.trailing.bottom.equalTo(view.safeAreaLayoutGuide).inset(16)
        }
    }

    private func loadData() async {
        let status = dependencies.photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await dependencies.photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }

        guard authorized else {
            await MainActor.run {
                self.statusLabel.text = "Photo access denied"
            }
            return
        }

        let assets = dependencies.photoLibraryService.fetchAssets()
        let backedUpIDs: Set<String>

        do {
            backedUpIDs = try dependencies.databaseManager.read { db in
                let ids = try String.fetchAll(db, sql: "SELECT DISTINCT assetLocalIdentifier FROM resources")
                return Set(ids)
            }
        } catch {
            backedUpIDs = []
        }

        await MainActor.run {
            self.allAssets = assets
            self.backedUpAssetIDs = backedUpIDs
            self.applyFilterAndReload()
        }
    }

    @objc
    private func filterChanged() {
        applyFilterAndReload()
    }

    private func applyFilterAndReload() {
        if filterControl.selectedSegmentIndex == 1 {
            visibleAssets = allAssets.filter { !backedUpAssetIDs.contains($0.localIdentifier) }
        } else {
            visibleAssets = allAssets
        }

        statusLabel.text = "\(visibleAssets.count) items"
        collectionView.reloadData()
    }
}

extension LocalBrowserViewController: UICollectionViewDataSource, UICollectionViewDelegate {
    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        visibleAssets.count
    }

    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        guard let cell = collectionView.dequeueReusableCell(withReuseIdentifier: PhotoGridCell.reuseID, for: indexPath) as? PhotoGridCell else {
            return UICollectionViewCell()
        }

        let asset = visibleAssets[indexPath.item]
        cell.representedID = asset.localIdentifier
        cell.titleLabel.text = backedUpAssetIDs.contains(asset.localIdentifier) ? "Backed Up" : "Not Backed"
        cell.imageView.image = UIImage(systemName: "photo")

        dependencies.photoLibraryService.requestThumbnail(for: asset, targetSize: CGSize(width: 400, height: 400)) { image in
            if cell.representedID == asset.localIdentifier {
                cell.imageView.image = image
            }
        }

        return cell
    }

    func collectionView(_ collectionView: UICollectionView, didSelectItemAt indexPath: IndexPath) {
        let asset = visibleAssets[indexPath.item]
        let detail = LocalAssetDetailViewController(
            dependencies: dependencies,
            asset: asset,
            isBackedUp: backedUpAssetIDs.contains(asset.localIdentifier)
        )
        detail.title = "Local Detail"
        navigationController?.pushViewController(detail, animated: true)
    }
}
