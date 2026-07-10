import Foundation
import Kingfisher
import MoreKit
import UIKit

// Shared fingerprint thumbnail cache for local/remote convergence and remote sidecars.
enum MediaThumbnailCache {
    // Keep the storage name stable so existing content-addressed disk entries remain reusable.
    private static let cache = ImageCache(name: "MediaBrowserThumbnails")
    private static let memoryCountLimit = 256
    private static var configured = false
    private static let configureLock = NSLock()
    private static let migrationCoordinator = MigrationCoordinator()
    static let storedFingerprintUserInfoKey = "fingerprint"
    static let storedImageUserInfoKey = "image"

    private actor MigrationCoordinator {
        private struct Operation {
            let id: UUID
            let task: Task<Bool, Never>
        }

        private var operations: [String: Operation] = [:]

        func run(key: String, operation: @escaping @Sendable () async -> Bool) async -> Bool {
            if UserDefaults.standard.bool(forKey: key) { return true }
            let current: Operation
            if let existing = operations[key] {
                current = existing
            } else {
                current = Operation(id: UUID(), task: Task { await operation() })
                operations[key] = current
            }
            let succeeded = await current.task.value
            if succeeded {
                UserDefaults.standard.set(true, forKey: key)
            }
            if operations[key]?.id == current.id {
                operations[key] = nil
            }
            return succeeded
        }
    }

    private static func key(for fingerprint: Data) -> String {
        "thumb-\(fingerprint.hexString)"
    }

    static func cached(for fingerprint: Data) async -> UIImage? {
        // Launch runs the one-time legacy purge unawaited; a first read must never race past it.
        guard await purgeUnverifiedLegacyEntriesIfNeeded() else { return nil }
        return await withCheckedContinuation { continuation in
            cache.retrieveImage(forKey: key(for: fingerprint)) { result in
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
        let jpeg = original ?? ThumbnailSizing.jpegData(from: image)
        cache.store(
            image,
            original: jpeg,
            forKey: key(for: fingerprint),
            cacheSerializer: jpegSerializer
        )
        NotificationCenter.default.post(
            name: .MediaBrowserThumbnailDidStore,
            object: nil,
            userInfo: [
                storedFingerprintUserInfoKey: fingerprint,
                storedImageUserInfoKey: image
            ]
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
            cache.diskStorage.config.sizeLimit = ThumbnailCacheSizeLimit.getValue().maxBytes
            cache.diskStorage.config.expiration = .days(30)
            // Bound runtime memory: Kingfisher's default in-RAM cost limit is ~25% of physical memory.
            // Cap decoded thumbnails held in RAM; anything evicted is re-read cheaply from disk.
            cache.memoryStorage.config.totalCostLimit = 64 * 1024 * 1024
            cache.memoryStorage.config.countLimit = memoryCountLimit
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
    @discardableResult
    static func purgeUnverifiedLegacyEntriesIfNeeded() async -> Bool {
        let key = "com.zizicici.common.migration.browserThumbnailWritersVerified"
        return await migrationCoordinator.run(key: key) {
            await clear()
            return await isDiskCacheEmpty()
        }
    }

    // One-time: the browser moved off ImageCache.default to its own instance, so reclaim the orphaned
    // thumbnails the default cache's disk still holds. No-op after the first successful run.
    static func purgeLegacyDefaultCacheIfNeeded() async {
        let key = "com.zizicici.common.migration.browserThumbnailCacheIsolated"
        _ = await migrationCoordinator.run(key: key) {
            await withCheckedContinuation { continuation in
                ImageCache.default.clearDiskCache { continuation.resume() }
            }
            return await isDefaultDiskCacheEmpty()
        }
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

    private static func isDiskCacheEmpty() async -> Bool {
        await withCheckedContinuation { continuation in
            cache.calculateDiskStorageSize { result in
                continuation.resume(returning: (try? result.get()) == 0)
            }
        }
    }

    private static func isDefaultDiskCacheEmpty() async -> Bool {
        await withCheckedContinuation { continuation in
            ImageCache.default.calculateDiskStorageSize { result in
                continuation.resume(returning: (try? result.get()) == 0)
            }
        }
    }
}
