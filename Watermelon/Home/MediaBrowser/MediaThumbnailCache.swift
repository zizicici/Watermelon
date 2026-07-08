import Foundation
import Kingfisher
import MoreKit
import UIKit

// The browser's shared L1 thumbnail cache: one content-addressed (fingerprint-keyed) entry per asset,
// shared by the local and remote paths, bounded on disk (LRU) by the settable cap.
enum MediaThumbnailCache {
    // Browser-owned instance (not ImageCache.default) so size/clear/config are decoupled from other stacks.
    private static let cache = ImageCache(name: "MediaBrowserThumbnails")
    private static var configured = false
    private static let configureLock = NSLock()

    static func cacheKey(for fingerprint: Data) -> String {
        "thumb-\(fingerprint.hexString)"
    }

    static func cached(for fingerprint: Data) async -> UIImage? {
        // Launch runs the one-time legacy purge unawaited; a first read must never race past it.
        await purgeUnverifiedLegacyEntriesIfNeeded()
        return await withCheckedContinuation { continuation in
            cache.retrieveImage(forKey: cacheKey(for: fingerprint)) { result in
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
        let jpeg = original ?? image.jpegData(compressionQuality: ThumbnailSizing.jpegCompressionQuality)
        cache.store(
            image,
            original: jpeg,
            forKey: cacheKey(for: fingerprint),
            cacheSerializer: jpegSerializer
        )
    }

    // Un-fingerprinted local thumbnails: memory-only on the same instance (local-only, not
    // content-addressed, cheap to re-render), so they never touch disk or the album stack's namespace.
    static func localCacheKey(localIdentifier: String) -> String {
        "browser-local-\(localIdentifier)"
    }

    static func cachedInMemory(forKey key: String) -> UIImage? {
        cache.retrieveImageInMemoryCache(forKey: key)
    }

    static func storeInMemory(_ image: UIImage, forKey key: String) async {
        try? await cache.store(image, forKey: key, toDisk: false)
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
            cache.diskStorage.config.sizeLimit = ThumbnailCacheSizeLimit.getValue().maxBytes
            cache.diskStorage.config.expiration = .days(30)
            // Bound runtime memory: Kingfisher's default in-RAM cost limit is ~25% of physical memory.
            // Cap decoded thumbnails held in RAM; anything evicted is re-read cheaply from disk.
            cache.memoryStorage.config.totalCostLimit = 64 * 1024 * 1024
            cache.memoryStorage.config.countLimit = 256
        }
    }

    // Applies a new size cap and trims — fire-and-forget: Kingfisher skips the completion when its
    // cleanup throws, which would leak an awaited continuation.
    static func applySizeLimit(_ bytes: UInt) async {
        cache.diskStorage.config.sizeLimit = bytes
        cache.cleanExpiredDiskCache(completion: nil)
    }

    // Ensures the cap is configured (works even in a session where no RemoteThumbnailService was
    // created) and trims now. Call at launch and when leaving the browser.
    static func enforceLimit() async {
        configureIfNeeded()
        await purgeUnverifiedLegacyEntriesIfNeeded()
        await applySizeLimit(ThumbnailCacheSizeLimit.getValue().maxBytes)
    }

    // One-time: entries stored before remote-derived L1 writes were manifest-hash gated may hold wrong
    // bytes the fingerprint-only key can't detect — drop them once; the gated writers repopulate on view.
    static func purgeUnverifiedLegacyEntriesIfNeeded() async {
        let key = "com.zizicici.common.migration.browserThumbnailWritersVerified"
        guard !UserDefaults.standard.bool(forKey: key) else { return }
        await clear()
        UserDefaults.standard.set(true, forKey: key)
    }

    // One-time: the browser moved off ImageCache.default to its own instance, so reclaim the orphaned
    // thumbnails the default cache's disk still holds. No-op after the first successful run.
    static func purgeLegacyDefaultCacheIfNeeded() async {
        let key = "com.zizicici.common.migration.browserThumbnailCacheIsolated"
        guard !UserDefaults.standard.bool(forKey: key) else { return }
        await withCheckedContinuation { continuation in
            ImageCache.default.clearDiskCache { continuation.resume() }
        }
        UserDefaults.standard.set(true, forKey: key)
    }

    static func diskSizeBytes() async -> UInt {
        await withCheckedContinuation { continuation in
            cache.calculateDiskStorageSize { result in
                switch result {
                case .success(let size): continuation.resume(returning: size)
                case .failure: continuation.resume(returning: 0)
                }
            }
        }
    }

    static func clear() async {
        cache.clearMemoryCache()
        await withCheckedContinuation { continuation in
            cache.clearDiskCache {
                continuation.resume()
            }
        }
    }
}
