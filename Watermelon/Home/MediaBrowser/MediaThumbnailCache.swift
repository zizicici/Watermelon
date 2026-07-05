import Foundation
import Kingfisher
import MoreKit
import UIKit

// The browser's shared L1 thumbnail cache: one content-addressed (fingerprint-keyed) entry per asset,
// shared by the local and remote paths, bounded on disk (LRU) by the settable cap.
enum MediaThumbnailCache {
    private static var configured = false
    private static let configureLock = NSLock()

    // Legacy prefix kept so entries written before the cache was renamed stay valid.
    static func cacheKey(for fingerprint: Data) -> String {
        "remote-thumb-\(fingerprint.hexString)"
    }

    static func cached(for fingerprint: Data) async -> UIImage? {
        await withCheckedContinuation { continuation in
            ImageCache.default.retrieveImage(forKey: cacheKey(for: fingerprint)) { result in
                switch result {
                case .success(let value):
                    continuation.resume(returning: value.image)
                case .failure:
                    continuation.resume(returning: nil)
                }
            }
        }
    }

    // Disk entries are always JPEG: Kingfisher's default serializer PNG-encodes original-less stores,
    // which is 4-8× larger for photographic content and collapses the cap's effective capacity.
    static func store(_ image: UIImage, original: Data? = nil, for fingerprint: Data) {
        let jpeg = original ?? image.jpegData(compressionQuality: 0.8)
        ImageCache.default.store(
            image,
            original: jpeg,
            forKey: cacheKey(for: fingerprint),
            cacheSerializer: jpegSerializer
        )
    }

    private static let jpegSerializer: DefaultCacheSerializer = {
        var serializer = DefaultCacheSerializer()
        serializer.preferCacheOriginalData = true
        return serializer
    }()

    static func configureIfNeeded() {
        configureLock.withLock {
            guard !configured else { return }
            configured = true
            ImageCache.default.diskStorage.config.sizeLimit = ThumbnailCacheSizeLimit.getValue().maxBytes
            ImageCache.default.diskStorage.config.expiration = .days(30)
            // Bound runtime memory: Kingfisher's default in-RAM cost limit is ~25% of physical memory.
            // Cap decoded thumbnails held in RAM; anything evicted is re-read cheaply from disk.
            ImageCache.default.memoryStorage.config.totalCostLimit = 64 * 1024 * 1024
            ImageCache.default.memoryStorage.config.countLimit = 256
        }
    }

    // Applies a new size cap and trims — fire-and-forget: Kingfisher skips the completion when its
    // cleanup throws, which would leak an awaited continuation.
    static func applySizeLimit(_ bytes: UInt) async {
        ImageCache.default.diskStorage.config.sizeLimit = bytes
        ImageCache.default.cleanExpiredDiskCache(completion: nil)
    }

    // Ensures the cap is configured (works even in a session where no RemoteThumbnailService was
    // created) and trims now. Call at launch and when leaving the browser.
    static func enforceLimit() async {
        configureIfNeeded()
        await applySizeLimit(ThumbnailCacheSizeLimit.getValue().maxBytes)
    }

    static func diskSizeBytes() async -> UInt {
        await withCheckedContinuation { continuation in
            ImageCache.default.calculateDiskStorageSize { result in
                switch result {
                case .success(let size): continuation.resume(returning: size)
                case .failure: continuation.resume(returning: 0)
                }
            }
        }
    }

    static func clear() async {
        ImageCache.default.clearMemoryCache()
        await withCheckedContinuation { continuation in
            ImageCache.default.clearDiskCache {
                continuation.resume()
            }
        }
    }
}
