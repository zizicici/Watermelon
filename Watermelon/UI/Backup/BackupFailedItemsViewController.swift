import SnapKit
import UIKit
import Photos

@MainActor
final class BackupFailedItemsViewController: UIViewController {
    private let sessionController: BackupSessionController

    private let tableView = UITableView(frame: .zero, style: .insetGrouped)
    private let emptyLabel = UILabel()
    private let imageManager = PHCachingImageManager()
    private let thumbnailCache = NSCache<NSString, UIImage>()

    private var observerID: UUID?
    private var currentSnapshot: BackupSessionController.Snapshot?
    private var items: [BackupSessionController.FailedItem] = []

    init(sessionController: BackupSessionController) {
        self.sessionController = sessionController
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "失败项"
        navigationItem.largeTitleDisplayMode = .never

        navigationItem.rightBarButtonItem = UIBarButtonItem(title: "重试全部", style: .plain, target: self, action: #selector(retryAllTapped))

        buildUI()

        observerID = sessionController.addObserver { [weak self] snapshot in
            self?.render(snapshot: snapshot)
        }
        sessionController.refreshFailedItems()
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if (isBeingDismissed || isMovingFromParent), let observerID {
            sessionController.removeObserver(observerID)
            self.observerID = nil
        }
    }

    private func buildUI() {
        tableView.register(UITableViewCell.self, forCellReuseIdentifier: "failed_item")
        tableView.dataSource = self
        tableView.delegate = self

        emptyLabel.text = "当前没有失败项"
        emptyLabel.textColor = .secondaryLabel
        emptyLabel.font = .systemFont(ofSize: 14)
        emptyLabel.textAlignment = .center
        emptyLabel.numberOfLines = 0
        emptyLabel.isHidden = true

        view.addSubview(tableView)
        view.addSubview(emptyLabel)

        tableView.snp.makeConstraints { make in
            make.edges.equalTo(view.safeAreaLayoutGuide)
        }

        emptyLabel.snp.makeConstraints { make in
            make.center.equalTo(view.safeAreaLayoutGuide)
            make.leading.trailing.equalToSuperview().inset(20)
        }
    }

    private func render(snapshot: BackupSessionController.Snapshot) {
        currentSnapshot = snapshot
        items = snapshot.failedItems
        tableView.reloadData()
        emptyLabel.isHidden = !items.isEmpty
        navigationItem.rightBarButtonItem?.isEnabled = !items.isEmpty && snapshot.state != .running
    }

    @objc
    private func retryAllTapped() {
        guard !items.isEmpty else { return }
        let started = sessionController.retryFailedItems()
        if !started {
            presentSimpleAlert(title: "无法开始", message: "当前已有任务运行，或没有可重试项")
        }
    }

    private func retry(item: BackupSessionController.FailedItem) {
        let started = sessionController.retryFailedItems(assetIDs: [item.assetLocalIdentifier])
        if !started {
            presentSimpleAlert(title: "无法重试", message: "当前已有任务运行，或该失败项已不存在")
        }
    }

    private func presentSimpleAlert(title: String, message: String) {
        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
        alert.addAction(UIAlertAction(title: "确定", style: .default))
        present(alert, animated: true)
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()
}

extension BackupFailedItemsViewController: UITableViewDataSource, UITableViewDelegate {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        items.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "failed_item", for: indexPath)
        let item = items[indexPath.row]

        let thumbnail = thumbnailCache.object(forKey: item.assetLocalIdentifier as NSString)
        applyContent(on: cell, item: item, thumbnail: thumbnail)
        if thumbnail == nil {
            requestThumbnail(for: item)
        }

        cell.accessoryType = .detailButton
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        tableView.deselectRow(at: indexPath, animated: true)
        guard indexPath.row < items.count else { return }
        let item = items[indexPath.row]

        let alert = UIAlertController(
            title: item.displayName,
            message: "最后失败时间: \(Self.timeFormatter.string(from: item.updatedAt))\n重试次数: \(item.retryCount)\n\n错误信息:\n\(item.errorMessage)",
            preferredStyle: .actionSheet
        )
        if let popover = alert.popoverPresentationController {
            popover.sourceView = tableView
            popover.sourceRect = tableView.rectForRow(at: indexPath)
        }
        alert.addAction(UIAlertAction(title: "重试此项", style: .default) { [weak self] _ in
            self?.retry(item: item)
        })
        alert.addAction(UIAlertAction(title: "取消", style: .cancel))
        present(alert, animated: true)
    }

    func tableView(_ tableView: UITableView, accessoryButtonTappedForRowWith indexPath: IndexPath) {
        guard indexPath.row < items.count else { return }
        retry(item: items[indexPath.row])
    }

    func tableView(_ tableView: UITableView, trailingSwipeActionsConfigurationForRowAt indexPath: IndexPath) -> UISwipeActionsConfiguration? {
        guard indexPath.row < items.count else { return nil }
        let item = items[indexPath.row]
        let retryAction = UIContextualAction(style: .normal, title: "重试") { [weak self] _, _, completion in
            self?.retry(item: item)
            completion(true)
        }
        retryAction.backgroundColor = .systemBlue
        return UISwipeActionsConfiguration(actions: [retryAction])
    }

    private func applyContent(on cell: UITableViewCell, item: BackupSessionController.FailedItem, thumbnail: UIImage?) {
        var content = cell.defaultContentConfiguration()
        content.text = item.displayName
        content.secondaryText = "\(Self.timeFormatter.string(from: item.updatedAt))\n\(item.errorMessage)"
        content.secondaryTextProperties.numberOfLines = 2
        content.textProperties.numberOfLines = 1
        content.image = thumbnail ?? UIImage(systemName: "photo")
        content.imageProperties.maximumSize = CGSize(width: 52, height: 52)
        content.imageProperties.cornerRadius = 6
        cell.contentConfiguration = content
    }

    private func requestThumbnail(for item: BackupSessionController.FailedItem) {
        let cacheKey = item.assetLocalIdentifier as NSString
        if thumbnailCache.object(forKey: cacheKey) != nil {
            return
        }

        let auth = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard auth == .authorized || auth == .limited else {
            return
        }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [item.assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return }

        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        imageManager.requestImage(
            for: asset,
            targetSize: CGSize(width: 140, height: 140),
            contentMode: .aspectFill,
            options: options
        ) { [weak self] image, _ in
            guard let self, let image else { return }
            self.thumbnailCache.setObject(image, forKey: cacheKey)

            guard let row = self.items.firstIndex(where: { $0.id == item.id }) else { return }
            let indexPath = IndexPath(row: row, section: 0)
            guard let cell = self.tableView.cellForRow(at: indexPath) else { return }
            self.applyContent(on: cell, item: item, thumbnail: image)
        }
    }
}
