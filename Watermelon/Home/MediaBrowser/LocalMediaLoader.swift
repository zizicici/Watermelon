import Kingfisher
import Photos
import UIKit

// Loads media directly from on-device PHAssets (the user's own library; network is allowed for full
// media so iCloud-optimized originals can be fetched). Shared by LocalMediaSource and the merged
// source's local-first path. All requests use .highQualityFormat, so each callback fires exactly once.
enum LocalMediaLoader {
    // Matches the remote browser's thumbnail cap so shared fingerprint-keyed entries are interchangeable.
    static let thumbnailPixelSide = RemoteThumbnailService.thumbnailMaxPixel

    static func thumbnail(localIdentifier: String, fingerprint: Data?) async -> UIImage? {
        // Ensure the shared ImageCache has its disk/memory caps applied even in a local-only session.
        MediaThumbnailCache.configureIfNeeded()
        // A fingerprinted asset shares the content-addressed entry with the remote path — a `.both` asset
        // is cached once, and browsing locally warms the Remote tab.
        if let fingerprint {
            if let cached = await MediaThumbnailCache.cached(for: fingerprint) { return cached }
            guard let image = await render(localIdentifier) else { return nil }
            MediaThumbnailCache.store(image, for: fingerprint)
            return image
        }
        // Not yet fingerprinted: memory only — nothing reads these local-id keys from disk, and a PhotoKit
        // re-render is cheap.
        let key = PHAssetThumbnailLoader.cacheKey(assetLocalIdentifier: localIdentifier, pixelSide: thumbnailPixelSide)
        if let cached = ImageCache.default.retrieveImageInMemoryCache(forKey: key) { return cached }
        guard let image = await render(localIdentifier) else { return nil }
        try? await ImageCache.default.store(image, forKey: key, toDisk: false)
        return image
    }

    private static func render(_ localIdentifier: String) async -> UIImage? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = false
        options.isSynchronous = false
        guard let targetLongSide = ThumbnailSizing.targetLongSide(
            originalWidth: asset.pixelWidth,
            originalHeight: asset.pixelHeight
        ) else { return nil }
        guard let image = await requestImage(
            asset,
            target: CGSize(width: targetLongSide, height: targetLongSide),
            contentMode: .aspectFit,
            options: options
        ) else { return nil }
        return ThumbnailSizing.fittedImage(image, maximumLongSide: targetLongSide)
    }

    static func photoImage(localIdentifier: String, maxPixel: Int) async -> UIImage? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = true
        options.isSynchronous = false
        options.version = .current
        return await requestImage(
            asset,
            target: CGSize(width: maxPixel, height: maxPixel),
            contentMode: .aspectFit,
            options: options
        )
    }

    static func video(localIdentifier: String) async -> MaterializedVideo? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHVideoRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .highQualityFormat
        options.version = .current
        let avAsset: AVAsset? = await withCheckedContinuation { continuation in
            let once = ResumeOnce()
            PHImageManager.default().requestAVAsset(forVideo: asset, options: options) { avAsset, _, _ in
                if once.tryResume() { continuation.resume(returning: avAsset) }
            }
        }
        // Only a URL-backed (non-composited) asset yields a directly playable file URL.
        guard let urlAsset = avAsset as? AVURLAsset else { return nil }
        return MaterializedVideo(url: urlAsset.url, isTemporary: false)
    }

    static func livePhoto(localIdentifier: String, targetSize: CGSize) async -> PHLivePhoto? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHLivePhotoRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.isNetworkAccessAllowed = true
        return await withCheckedContinuation { continuation in
            let once = ResumeOnce()
            PHImageManager.default().requestLivePhoto(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFit,
                options: options
            ) { live, _ in
                if once.tryResume() { continuation.resume(returning: live) }
            }
        }
    }

    private static func fetchAsset(_ localIdentifier: String) -> PHAsset? {
        PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil).firstObject
    }

    // Cancellation-aware: when the awaiting Task is cancelled (e.g. a grid cell scrolled off-screen), the
    // in-flight PHImageManager request is cancelled too — bounding memory/CPU during fast scrolling.
    private static func requestImage(
        _ asset: PHAsset,
        target: CGSize,
        contentMode: PHImageContentMode,
        options: PHImageRequestOptions
    ) async -> UIImage? {
        let box = RequestIDBox()
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                let once = ResumeOnce()
                let requestID = PHImageManager.default().requestImage(
                    for: asset,
                    targetSize: target,
                    contentMode: contentMode,
                    options: options
                ) { image, _ in
                    if once.tryResume() { continuation.resume(returning: image) }
                }
                box.setOrCancel(requestID)
            }
        } onCancel: {
            box.cancel()
        }
    }
}

// Holds a PHImageRequestID so a cancelled Task can cancel the underlying PhotoKit request.
final class RequestIDBox: @unchecked Sendable {
    private let lock = NSLock()
    private var id: PHImageRequestID?
    private var cancelled = false

    func setOrCancel(_ requestID: PHImageRequestID) {
        lock.withLock {
            if cancelled {
                PHImageManager.default().cancelImageRequest(requestID)
            } else {
                id = requestID
            }
        }
    }

    func cancel() {
        lock.withLock {
            cancelled = true
            if let id { PHImageManager.default().cancelImageRequest(id) }
        }
    }
}

// Resumes a continuation at most once (PhotoKit handlers may fire more than once).
final class ResumeOnce: @unchecked Sendable {
    private let lock = NSLock()
    private var done = false
    func tryResume() -> Bool {
        lock.withLock {
            if done { return false }
            done = true
            return true
        }
    }
}
