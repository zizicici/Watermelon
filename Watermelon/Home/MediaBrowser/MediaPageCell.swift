import AVKit
import PhotosUI
import UIKit

// One full-screen page in the pager. It renders exactly one of: a zoomable photo, a native Live Photo,
// or a video (poster → inline player). All view visibility flows through a single `apply(_:)` on the
// `Display` state — there is no ad-hoc `isHidden` toggling elsewhere. Playback is gated on `isActive`,
// which the viewer sets for the single centered page, so nothing plays off-screen or leaks across reuse.
final class MediaPageCell: UICollectionViewCell {
    static let reuseID = "MediaPageCell"

    var onSingleTap: (() -> Void)?
    var onZoomChanged: ((Bool) -> Void)?
    // The viewer, so an inline AVPlayerViewController is embedded as a proper child view controller.
    weak var hostViewController: UIViewController?

    private enum Display: Equatable {
        case idle          // recycled / no content
        case loading       // spinner
        case photo         // zoomable still
        case livePhoto     // native PHLivePhotoView
        case videoPoster   // still poster + play button (also the Live-fallback state)
        case videoPlaying  // embedded AVPlayerViewController
        case failed
    }

    private let scrollView = UIScrollView()
    private let imageView = UIImageView()
    private lazy var livePhotoView = PHLivePhotoView()
    private let videoContainer = UIView()
    private let playButton = UIButton(type: .system)
    private let activityIndicator = UIActivityIndicatorView(style: .large)

    private var display: Display = .idle
    private var item: MediaBrowserItem?
    private var itemToken: String?
    private var source: MediaBrowserSource?
    private var isActive = false

    private var loadTask: Task<Void, Never>?
    private var videoTask: Task<Void, Never>?
    private var playerController: AVPlayerViewController?
    private var videoTempURL: URL?

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
        // Not just cancel the video task: also pause the player, detach the child VC, and delete the temp
        // video file — dealloc (vs. recycle) otherwise leaks the file and leaves the player pipeline alive.
        removeInlinePlayer()
    }

    override func prepareForReuse() {
        super.prepareForReuse()
        reset()
    }

    // MARK: - State machine

    // The ONLY place view visibility changes. Every transition goes through here.
    private func apply(_ next: Display) {
        display = next
        let showsScroll = next == .photo || next == .videoPoster
        scrollView.isHidden = !showsScroll
        livePhotoView.isHidden = next != .livePhoto
        videoContainer.isHidden = next != .videoPlaying
        playButton.isHidden = next != .videoPoster
        if next == .loading { activityIndicator.startAnimating() } else { activityIndicator.stopAnimating() }

        // Only real photos zoom; posters are shown 1:1.
        scrollView.maximumZoomScale = next == .photo ? 4 : 1
        if scrollView.zoomScale != 1 { scrollView.setZoomScale(1, animated: false) }
    }

    // Full reset to a blank, content-free cell. Cancels work and releases heavy resources.
    private func reset() {
        loadTask?.cancel(); loadTask = nil
        removeInlinePlayer()
        isActive = false
        item = nil
        itemToken = nil
        source = nil
        imageView.image = nil
        livePhotoView.stopPlayback()
        livePhotoView.livePhoto = nil
        apply(.idle)
    }

    func configure(with item: MediaBrowserItem, source: MediaBrowserSource) {
        reset()
        self.item = item
        self.itemToken = item.id
        self.source = source
        apply(.loading)

        let token = item.id
        let targetSize = liveTargetSize()
        loadTask = Task { [weak self] in
            switch item.kind {
            case .photo:
                let image = await source.photoImage(for: item)
                guard let self, self.itemToken == token else { return }
                self.imageView.image = image
                self.apply(image == nil ? .failed : .photo)

            case .video:
                let poster = await source.thumbnail(for: item)
                guard let self, self.itemToken == token else { return }
                self.imageView.image = poster
                self.apply(.videoPoster)

            case .livePhoto:
                if let live = await source.livePhoto(for: item, targetSize: targetSize) {
                    guard let self, self.itemToken == token else { return }
                    self.livePhotoView.livePhoto = live
                    self.apply(.livePhoto)
                    if self.isActive { self.livePhotoView.startPlayback(with: .full) }
                    // Keep a sharp still behind the live view for the hero transition — rendering a
                    // PHLivePhotoView on demand yields a low-res still.
                    if let still = await source.photoImage(for: item), self.itemToken == token {
                        self.imageView.image = still
                    }
                } else {
                    // Reconstruction failed → still + play button that plays the paired video inline.
                    let image = await source.photoImage(for: item)
                    guard let self, self.itemToken == token else { return }
                    self.imageView.image = image
                    self.apply(.videoPoster)
                }
            }
        }
    }

    // Marks this page as the centered one. Drives Live playback and pauses inline video when leaving.
    func setActive(_ active: Bool) {
        guard active != isActive else { return }
        isActive = active
        switch display {
        case .livePhoto:
            if active { livePhotoView.startPlayback(with: .full) } else { livePhotoView.stopPlayback() }
        case .videoPlaying:
            if !active { stopInlineVideo() }
        case .videoPoster:
            // A play was requested but the video was still loading when we left: drop the in-flight task
            // and restore the poster so the spinner doesn't spin forever on an off-screen page.
            if !active { videoTask?.cancel(); videoTask = nil; apply(.videoPoster) }
        default:
            break
        }
    }

    // MARK: - Hero transition

    // The currently displayed image (photo, or video/live poster) and its on-screen aspect-fit frame in
    // window coordinates. Nil when nothing is displayed yet (e.g. still loading, or a native Live Photo).
    // The displayed still (photo, video/live poster, or the live-photo still kept behind the live view) and
    // its on-screen aspect-fit frame in window coords. Nil while a video plays, while zoomed, or before the
    // still is available — those fall back to a plain fade.
    func heroSnapshot() -> (image: UIImage, frameInWindow: CGRect)? {
        guard display != .videoPlaying, scrollView.zoomScale == 1,
              let image = imageView.image, imageView.bounds.width > 0 else { return nil }
        let fitted = AVMakeRect(aspectRatio: image.size, insideRect: imageView.bounds)
        return (image, imageView.convert(fitted, to: nil))
    }

    func setHeroContentHidden(_ hidden: Bool) {
        let alpha: CGFloat = hidden ? 0 : 1
        imageView.alpha = alpha
        livePhotoView.alpha = alpha
    }

    // MARK: - Inline video

    @objc private func playTapped() {
        guard display == .videoPoster, let item, let source, let host = hostViewController else { return }
        videoTask?.cancel()   // a rapid double-tap must not spawn a second player / leak its temp file
        playButton.isHidden = true
        activityIndicator.startAnimating()
        let token = item.id
        videoTask = Task { [weak self] in
            let material = await source.video(for: item)
            guard let self, self.itemToken == token else {
                if let material, material.isTemporary { try? FileManager.default.removeItem(at: material.url) }
                return
            }
            // Paged away before the video finished loading: restore the poster, don't play off-screen.
            guard self.isActive else {
                if let material, material.isTemporary { try? FileManager.default.removeItem(at: material.url) }
                self.apply(.videoPoster)
                return
            }
            guard let material else {
                self.apply(.videoPoster)
                return
            }
            // A prior tap's task (cancellation isn't observed past the await on cached/local/direct paths)
            // may already have embedded a player — don't stack a second one (double audio + leaked temp).
            guard self.playerController == nil else {
                if material.isTemporary { try? FileManager.default.removeItem(at: material.url) }
                return
            }
            self.embedPlayer(url: material.url, isTemporary: material.isTemporary, host: host)
            self.apply(.videoPlaying)
            self.playerController?.player?.play()
        }
    }

    // Returns a playing video to its poster. No-op unless a video is actually playing (so a swipe on a
    // Live/photo page never spawns a bogus play button).
    func stopInlineVideo() {
        guard display == .videoPlaying else { return }
        removeInlinePlayer()
        apply(.videoPoster)
    }

    private func embedPlayer(url: URL, isTemporary: Bool, host: UIViewController) {
        let controller = AVPlayerViewController()
        controller.player = AVPlayer(url: url)
        controller.videoGravity = .resizeAspect
        controller.showsPlaybackControls = true
        controller.view.backgroundColor = .black
        // Inset the native transport controls clear of the viewer's top/bottom chrome bars (which overlay
        // this inline player), with breathing room so they don't sit flush against the bars — the video
        // itself still fills the screen.
        controller.additionalSafeAreaInsets = UIEdgeInsets(top: 60, left: 0, bottom: 86, right: 0)

        host.addChild(controller)
        videoContainer.addSubview(controller.view)
        controller.view.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            controller.view.topAnchor.constraint(equalTo: videoContainer.topAnchor),
            controller.view.bottomAnchor.constraint(equalTo: videoContainer.bottomAnchor),
            controller.view.leadingAnchor.constraint(equalTo: videoContainer.leadingAnchor),
            controller.view.trailingAnchor.constraint(equalTo: videoContainer.trailingAnchor),
        ])
        controller.didMove(toParent: host)
        playerController = controller
        videoTempURL = isTemporary ? url : nil
    }

    private func removeInlinePlayer() {
        videoTask?.cancel(); videoTask = nil
        if let controller = playerController {
            controller.player?.pause()
            controller.willMove(toParent: nil)
            controller.view.removeFromSuperview()
            controller.removeFromParent()
            playerController = nil
        }
        if let url = videoTempURL {
            try? FileManager.default.removeItem(at: url)
            videoTempURL = nil
        }
    }

    private func liveTargetSize() -> CGSize {
        let size = contentView.bounds.size == .zero ? UIScreen.main.bounds.size : contentView.bounds.size
        let scale = UIScreen.main.scale
        return CGSize(width: size.width * scale, height: size.height * scale)
    }

    // MARK: - Layout

    private func configureUI() {
        contentView.backgroundColor = .black

        scrollView.delegate = self
        scrollView.minimumZoomScale = 1
        scrollView.maximumZoomScale = 4
        scrollView.showsVerticalScrollIndicator = false
        scrollView.showsHorizontalScrollIndicator = false
        scrollView.contentInsetAdjustmentBehavior = .never
        scrollView.isHidden = true
        contentView.addSubview(scrollView)

        imageView.contentMode = .scaleAspectFit
        scrollView.addSubview(imageView)

        livePhotoView.isHidden = true
        livePhotoView.contentMode = .scaleAspectFit
        contentView.addSubview(livePhotoView)

        videoContainer.backgroundColor = .black
        videoContainer.isHidden = true
        contentView.addSubview(videoContainer)

        var config = UIButton.Configuration.plain()
        config.image = UIImage(systemName: "play.circle.fill", withConfiguration: UIImage.SymbolConfiguration(pointSize: 64, weight: .regular))
        playButton.configuration = config
        playButton.tintColor = .white
        playButton.isHidden = true
        playButton.addTarget(self, action: #selector(playTapped), for: .touchUpInside)
        contentView.addSubview(playButton)

        activityIndicator.color = .white
        activityIndicator.hidesWhenStopped = true
        contentView.addSubview(activityIndicator)

        for v in [scrollView, livePhotoView, videoContainer, playButton, activityIndicator] {
            v.translatesAutoresizingMaskIntoConstraints = false
        }
        NSLayoutConstraint.activate([
            scrollView.topAnchor.constraint(equalTo: contentView.topAnchor),
            scrollView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            scrollView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            scrollView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            livePhotoView.topAnchor.constraint(equalTo: contentView.topAnchor),
            livePhotoView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            livePhotoView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            livePhotoView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            videoContainer.topAnchor.constraint(equalTo: contentView.topAnchor),
            videoContainer.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            videoContainer.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            videoContainer.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            playButton.centerXAnchor.constraint(equalTo: contentView.centerXAnchor),
            playButton.centerYAnchor.constraint(equalTo: contentView.centerYAnchor),
            activityIndicator.centerXAnchor.constraint(equalTo: contentView.centerXAnchor),
            activityIndicator.centerYAnchor.constraint(equalTo: contentView.centerYAnchor),
        ])

        // Constrained zoom pattern: image view fills the scroll viewport (edges + size == scrollView) and
        // aspect-fits, so photos/posters stay pinned + correctly proportioned while zoom still scales.
        imageView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            imageView.topAnchor.constraint(equalTo: scrollView.topAnchor),
            imageView.bottomAnchor.constraint(equalTo: scrollView.bottomAnchor),
            imageView.leadingAnchor.constraint(equalTo: scrollView.leadingAnchor),
            imageView.trailingAnchor.constraint(equalTo: scrollView.trailingAnchor),
            imageView.widthAnchor.constraint(equalTo: scrollView.widthAnchor),
            imageView.heightAnchor.constraint(equalTo: scrollView.heightAnchor),
        ])

        let singleTap = UITapGestureRecognizer(target: self, action: #selector(handleSingleTap))
        let doubleTap = UITapGestureRecognizer(target: self, action: #selector(handleDoubleTap))
        doubleTap.numberOfTapsRequired = 2
        singleTap.require(toFail: doubleTap)
        singleTap.delegate = self
        doubleTap.delegate = self
        contentView.addGestureRecognizer(singleTap)
        contentView.addGestureRecognizer(doubleTap)
    }

    @objc private func handleSingleTap() {
        onSingleTap?()
    }

    @objc private func handleDoubleTap(_ gesture: UITapGestureRecognizer) {
        guard display == .photo, imageView.image != nil else { return }
        if scrollView.zoomScale > 1 {
            scrollView.setZoomScale(1, animated: true)
        } else {
            let point = gesture.location(in: imageView)
            let size = CGSize(width: scrollView.bounds.width / 2.5, height: scrollView.bounds.height / 2.5)
            let rect = CGRect(x: point.x - size.width / 2, y: point.y - size.height / 2, width: size.width, height: size.height)
            scrollView.zoom(to: rect, animated: true)
        }
    }
}

extension MediaPageCell: UIScrollViewDelegate {
    func viewForZooming(in scrollView: UIScrollView) -> UIView? {
        display == .photo ? imageView : nil
    }

    func scrollViewDidZoom(_ scrollView: UIScrollView) {
        onZoomChanged?(scrollView.zoomScale > 1.01)
    }
}

extension MediaPageCell: UIGestureRecognizerDelegate {
    // Don't let taps on the play button or inside the video player toggle chrome / zoom.
    func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer, shouldReceive touch: UITouch) -> Bool {
        guard let touched = touch.view else { return true }
        if touched.isDescendant(of: playButton) { return false }
        if touched.isDescendant(of: videoContainer) { return false }
        return true
    }
}
