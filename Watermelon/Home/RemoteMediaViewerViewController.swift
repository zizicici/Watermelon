import AVKit
import SnapKit
import UIKit

// Full-screen viewer for a single remote asset: downloads the original (or reads it directly on an
// external volume), then shows a zoomable photo or plays the video. Best-effort — failures show a
// message rather than crashing.
final class RemoteMediaViewerViewController: UIViewController {
    private let service: RemoteThumbnailService
    private let asset: RemoteBrowserAsset

    private let scrollView = UIScrollView()
    private let imageView = UIImageView()
    private let activityIndicator = UIActivityIndicatorView(style: .large)
    private let messageLabel = UILabel()

    private var materialized: RemoteThumbnailService.MaterializedOriginal?
    private var loadTask: Task<Void, Never>?
    private var player: AVPlayer?
    private var playerController: AVPlayerViewController?

    init(service: RemoteThumbnailService, asset: RemoteBrowserAsset) {
        self.service = service
        self.asset = asset
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        loadTask?.cancel()
        // Detach the item before deleting the backing file — AVPlayer holds it open during playback.
        player?.pause()
        player?.replaceCurrentItem(with: nil)
        if let materialized, materialized.isTemporary {
            try? FileManager.default.removeItem(at: materialized.url)
        }
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .done,
            target: self,
            action: #selector(close)
        )
        configureUI()
        load()
    }

    private func configureUI() {
        scrollView.delegate = self
        scrollView.minimumZoomScale = 1
        scrollView.maximumZoomScale = 4
        scrollView.showsVerticalScrollIndicator = false
        scrollView.showsHorizontalScrollIndicator = false
        view.addSubview(scrollView)
        scrollView.snp.makeConstraints { make in make.edges.equalToSuperview() }

        imageView.contentMode = .scaleAspectFit
        scrollView.addSubview(imageView)
        imageView.snp.makeConstraints { make in
            make.edges.equalToSuperview()
            make.width.height.equalToSuperview()
        }

        activityIndicator.color = .white
        activityIndicator.hidesWhenStopped = true
        view.addSubview(activityIndicator)
        activityIndicator.snp.makeConstraints { make in make.center.equalToSuperview() }
        activityIndicator.startAnimating()

        messageLabel.textColor = .secondaryLabel
        messageLabel.font = .preferredFont(forTextStyle: .subheadline)
        messageLabel.textAlignment = .center
        messageLabel.numberOfLines = 0
        messageLabel.isHidden = true
        view.addSubview(messageLabel)
        messageLabel.snp.makeConstraints { make in
            make.center.equalToSuperview()
            make.leading.greaterThanOrEqualToSuperview().offset(32)
            make.trailing.lessThanOrEqualToSuperview().offset(-32)
        }
    }

    private func load() {
        let relativePath = asset.isVideo ? asset.videoRemoteRelativePath : asset.photoRemoteRelativePath
        guard let relativePath else {
            showMessage(String(localized: "remoteBrowser.viewer.unavailable"))
            return
        }
        let asset = asset
        loadTask = Task { [weak self] in
            let result = await self?.service.materializeOriginal(remoteRelativePath: relativePath)
            guard let self, !Task.isCancelled else {
                // Dismissed mid-download: clean up the orphaned temp original we'll never display.
                if let result, result.isTemporary { try? FileManager.default.removeItem(at: result.url) }
                return
            }
            self.activityIndicator.stopAnimating()
            guard let result else {
                self.showMessage(String(localized: "remoteBrowser.viewer.failed"))
                return
            }
            self.materialized = result
            if asset.isVideo {
                self.playVideo(url: result.url)
            } else {
                self.showImage(url: result.url, fingerprint: asset.fingerprint)
            }
        }
    }

    private func showImage(url: URL, fingerprint: Data) {
        guard let image = UIImage(contentsOfFile: url.path) else {
            showMessage(String(localized: "remoteBrowser.viewer.failed"))
            return
        }
        imageView.image = image
        // Seed the grid thumbnail so the placeholder cell self-heals next time.
        let service = service
        Task { await service.cacheThumbnail(fromOriginalFile: url, for: fingerprint) }
    }

    private func playVideo(url: URL) {
        scrollView.isHidden = true
        let player = AVPlayer(url: url)
        let controller = AVPlayerViewController()
        controller.player = player
        self.player = player
        self.playerController = controller
        addChild(controller)
        view.insertSubview(controller.view, belowSubview: activityIndicator)
        controller.view.snp.makeConstraints { make in make.edges.equalToSuperview() }
        controller.didMove(toParent: self)
        player.play()
    }

    private func showMessage(_ text: String) {
        activityIndicator.stopAnimating()
        scrollView.isHidden = true
        messageLabel.text = text
        messageLabel.isHidden = false
    }

    @objc private func close() {
        dismiss(animated: true)
    }
}

extension RemoteMediaViewerViewController: UIScrollViewDelegate {
    func viewForZooming(in scrollView: UIScrollView) -> UIView? {
        imageView.image == nil ? nil : imageView
    }
}
