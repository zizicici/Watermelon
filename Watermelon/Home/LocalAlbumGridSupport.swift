import Kingfisher
import Photos
import SnapKit
import UIKit

final class GradientView: UIView {
    override class var layerClass: AnyClass {
        CAGradientLayer.self
    }

    private var gradientLayer: CAGradientLayer {
        layer as! CAGradientLayer
    }

    init(colors: [UIColor], startPoint: CGPoint, endPoint: CGPoint, locations: [NSNumber]) {
        super.init(frame: .zero)
        isUserInteractionEnabled = false
        gradientLayer.colors = colors.map(\.cgColor)
        gradientLayer.startPoint = startPoint
        gradientLayer.endPoint = endPoint
        gradientLayer.locations = locations
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}

final class PHAssetThumbnailRequest: @unchecked Sendable {
    private let imageManager: PHImageManager
    private let lock = NSLock()
    private var requestID = PHInvalidImageRequestID
    private var isCancelled = false

    init(imageManager: PHImageManager) {
        self.imageManager = imageManager
    }

    var cancelled: Bool {
        lock.withLock { isCancelled }
    }

    func bind(_ requestID: PHImageRequestID) {
        let shouldCancel = lock.withLock {
            if isCancelled {
                return true
            }
            self.requestID = requestID
            return false
        }

        if shouldCancel {
            imageManager.cancelImageRequest(requestID)
        }
    }

    func cancel() {
        let id = lock.withLock {
            guard !isCancelled else { return PHInvalidImageRequestID }
            isCancelled = true
            let id = requestID
            requestID = PHInvalidImageRequestID
            return id
        }

        if id != PHInvalidImageRequestID {
            imageManager.cancelImageRequest(id)
        }
    }

    func finish() {
        lock.withLock {
            requestID = PHInvalidImageRequestID
        }
    }
}

enum PHAssetThumbnailLoader {
    private static let imageManager = PHCachingImageManager()

    static func cacheKey(assetLocalIdentifier: String, pixelSide: Int) -> String {
        "phasset-thumbnail-\(assetLocalIdentifier)-\(pixelSide)"
    }

    @discardableResult
    static func setImage(
        assetLocalIdentifier: String,
        pixelSide: Int,
        on imageView: UIImageView,
        fadeDuration: TimeInterval
    ) -> PHAssetThumbnailRequest {
        let request = PHAssetThumbnailRequest(imageManager: imageManager)
        let cacheKey = cacheKey(assetLocalIdentifier: assetLocalIdentifier, pixelSide: pixelSide)

        if let cachedImage = ImageCache.default.retrieveImageInMemoryCache(forKey: cacheKey) {
            imageView.image = cachedImage
            return request
        }

        loadFromPhotoLibrary(
            assetLocalIdentifier: assetLocalIdentifier,
            pixelSide: pixelSide,
            cacheKey: cacheKey,
            request: request,
            imageView: imageView,
            fadeDuration: fadeDuration
        )
        return request
    }

    private static func loadFromPhotoLibrary(
        assetLocalIdentifier: String,
        pixelSide: Int,
        cacheKey: String,
        request: PHAssetThumbnailRequest,
        imageView: UIImageView,
        fadeDuration: TimeInterval
    ) {
        guard !request.cancelled else { return }
        let result = PHAsset.fetchAssets(withLocalIdentifiers: [assetLocalIdentifier], options: nil)
        guard result.count > 0 else { return }

        let asset = result.object(at: 0)
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = false
        options.isSynchronous = false

        let requestID = imageManager.requestImage(
            for: asset,
            targetSize: CGSize(width: pixelSide, height: pixelSide),
            contentMode: .aspectFill,
            options: options
        ) { [weak imageView] image, info in
            if let isDegraded = info?[PHImageResultIsDegradedKey] as? Bool, isDegraded {
                return
            }

            request.finish()

            if request.cancelled { return }
            if let isCancelled = info?[PHImageCancelledKey] as? Bool, isCancelled {
                return
            }
            if info?[PHImageErrorKey] as? Error != nil {
                return
            }
            guard let image else { return }

            ImageCache.default.store(image, forKey: cacheKey)

            DispatchQueue.main.async { [weak imageView] in
                guard let imageView, !request.cancelled else { return }
                guard fadeDuration > 0 else {
                    imageView.image = image
                    return
                }
                UIView.transition(
                    with: imageView,
                    duration: fadeDuration,
                    options: [.transitionCrossDissolve, .allowUserInteraction]
                ) {
                    imageView.image = image
                }
            }
        }
        request.bind(requestID)
    }
}

func withCancellableDetachedValue<Value: Sendable>(
    priority: TaskPriority? = nil,
    operation: @escaping @Sendable () -> Value
) async -> Value {
    let task = Task.detached(priority: priority, operation: operation)
    return await withTaskCancellationHandler {
        await task.value
    } onCancel: {
        task.cancel()
    }
}

extension UIFont {
    func withWeight(_ weight: UIFont.Weight) -> UIFont {
        let descriptor = fontDescriptor.addingAttributes([
            .traits: [UIFontDescriptor.TraitKey.weight: weight.rawValue]
        ])
        return UIFont(descriptor: descriptor, size: pointSize)
    }
}

extension UICollectionViewCell {
    func thumbnailPixelSide(fallback: CGFloat) -> Int {
        let width = bounds.width > 0 ? bounds.width : fallback
        let scale = window?.screen.scale ?? UIScreen.main.scale
        return max(1, Int(width * scale))
    }
}

func makeAlbumEmptyStateView(title: String, message: String) -> UIView {
    let view = UIView()

    let titleLabel = UILabel()
    titleLabel.text = title
    titleLabel.textColor = .secondaryLabel
    titleLabel.font = .preferredFont(forTextStyle: .headline)
    titleLabel.textAlignment = .center
    titleLabel.adjustsFontForContentSizeCategory = true

    let messageLabel = UILabel()
    messageLabel.text = message
    messageLabel.textColor = .tertiaryLabel
    messageLabel.font = .preferredFont(forTextStyle: .subheadline)
    messageLabel.textAlignment = .center
    messageLabel.numberOfLines = 0
    messageLabel.adjustsFontForContentSizeCategory = true

    let stackView = UIStackView(arrangedSubviews: [titleLabel, messageLabel])
    stackView.axis = .vertical
    stackView.alignment = .fill
    stackView.spacing = 8

    view.addSubview(stackView)
    stackView.snp.makeConstraints { make in
        make.center.equalToSuperview()
        make.leading.greaterThanOrEqualToSuperview().offset(32)
        make.trailing.lessThanOrEqualToSuperview().offset(-32)
    }
    return view
}
