import SnapKit
import UIKit

@MainActor
final class BackupFailedItemDetailViewController: UIViewController {
    private let sessionController: BackupSessionController
    private let item: BackupSessionController.FailedItem

    private let imageView = UIImageView()
    private let filenameLabel = UILabel()
    private let metaLabel = UILabel()
    private let errorLabel = UILabel()
    private let retryButton = UIButton(type: .system)
    private let loadingView = UIActivityIndicatorView(style: .medium)

    init(sessionController: BackupSessionController, item: BackupSessionController.FailedItem) {
        self.sessionController = sessionController
        self.item = item
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "失败详情"
        navigationItem.largeTitleDisplayMode = .never
        buildUI()

        Task { [weak self] in
            await self?.loadPreviewImage()
        }
    }

    private func buildUI() {
        imageView.backgroundColor = .secondarySystemBackground
        imageView.contentMode = .scaleAspectFit
        imageView.clipsToBounds = true
        imageView.image = UIImage(systemName: "photo")

        filenameLabel.font = .systemFont(ofSize: 17, weight: .semibold)
        filenameLabel.textColor = .label
        filenameLabel.numberOfLines = 2
        filenameLabel.text = item.originalFilename

        metaLabel.font = .systemFont(ofSize: 13)
        metaLabel.textColor = .secondaryLabel
        metaLabel.numberOfLines = 0
        metaLabel.text = "失败时间: \(Self.timeFormatter.string(from: item.updatedAt))\n重试次数: \(item.retryCount)"

        errorLabel.font = .systemFont(ofSize: 14)
        errorLabel.textColor = .systemRed
        errorLabel.numberOfLines = 0
        errorLabel.text = item.errorMessage

        retryButton.configuration = .filled()
        retryButton.configuration?.title = "重试此项"
        retryButton.addTarget(self, action: #selector(retryTapped), for: .touchUpInside)

        loadingView.hidesWhenStopped = true

        let stack = UIStackView(arrangedSubviews: [imageView, filenameLabel, metaLabel, errorLabel, retryButton])
        stack.axis = .vertical
        stack.spacing = 12

        view.addSubview(stack)
        view.addSubview(loadingView)

        stack.snp.makeConstraints { make in
            make.top.equalTo(view.safeAreaLayoutGuide).offset(12)
            make.leading.trailing.equalToSuperview().inset(12)
        }

        imageView.snp.makeConstraints { make in
            make.height.equalTo(280)
        }

        retryButton.snp.makeConstraints { make in
            make.height.equalTo(44)
        }

        loadingView.snp.makeConstraints { make in
            make.center.equalTo(imageView)
        }
    }

    private func loadPreviewImage() async {
        loadingView.startAnimating()
        let image = await sessionController.loadPreviewImage(for: item)
        loadingView.stopAnimating()
        if let image {
            imageView.image = image
            return
        }

        imageView.image = UIImage(systemName: "photo.slash")
        let failText = "无法加载预览（可能是本地资源已删除、无权限，或远端文件不可读）"
        metaLabel.text = (metaLabel.text ?? "") + "\n" + failText
    }

    @objc
    private func retryTapped() {
        let started = sessionController.retryFailedItems(resourceIDs: [item.resourceLocalIdentifier])
        if started {
            let alert = UIAlertController(title: "已开始", message: "该失败项已加入重试任务", preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "确定", style: .default))
            present(alert, animated: true)
        } else {
            let alert = UIAlertController(title: "无法重试", message: "当前已有任务运行，或该失败项已不存在", preferredStyle: .alert)
            alert.addAction(UIAlertAction(title: "确定", style: .default))
            present(alert, animated: true)
        }
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter
    }()
}
