import Foundation
import ImageIO
import Kingfisher
import Photos
import UIKit

// Resolves thumbnails for the remote browser across the source priority chain (Part B):
//   L1 (Kingfisher, key = fingerprint, node-agnostic) → local PHAsset render (free) → L2 remote
//   sidecar → on-tap original download. Reads are always allowed; writing the shared L2 sidecar
//   (opportunistic writeback) is gated on the per-node generate-thumbnails flag.
//
// One instance per browser session: built with the active profile + password, owns a small
// connection pool, and a fingerprint→localIdentifier reverse map prepared once off-main.
final class RemoteThumbnailService: @unchecked Sendable {
    static let thumbnailMaxPixel = 512

    private let storageClientFactory: StorageClientFactory
    private let hashIndexRepository: ContentHashIndexRepository
    private let profile: ServerProfileRecord
    private let generateRemoteThumbnails: Bool
    private let pool: StorageClientPool

    private let lock = NSLock()
    private var localIdentifiersByFingerprint: [Data: String] = [:]
    private var localIndexReady = false

    init(
        storageClientFactory: StorageClientFactory,
        hashIndexRepository: ContentHashIndexRepository,
        profile: ServerProfileRecord,
        password: String,
        maxConnections: Int = 3
    ) {
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.profile = profile
        self.generateRemoteThumbnails = profile.generateRemoteThumbnails
        self.pool = StorageClientPool(
            maxConnections: maxConnections,
            makeClient: { [storageClientFactory, profile, password] in
                try storageClientFactory.makeClient(profile: profile, password: password)
            }
        )
        RemoteThumbnailCache.configureIfNeeded()
    }

    // Build the fingerprint→localIdentifier reverse map once, off the main thread. Idempotent — a
    // second caller (e.g. backfill after the browser already prepared it) returns immediately.
    func prepareLocalIndex() async {
        if lock.withLock({ localIndexReady }) { return }
        let map = await withCancellableDetachedValue { [hashIndexRepository] in
            (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint()) ?? [:]
        }
        lock.withLock {
            localIdentifiersByFingerprint = map
            localIndexReady = true
        }
    }

    func shutdown() async {
        await pool.shutdown()
    }

    func localIdentifier(for fingerprint: Data) -> String? {
        lock.withLock { localIdentifiersByFingerprint[fingerprint] }
    }

    // MARK: - Cache

    static func cacheKey(for fingerprint: Data) -> String {
        "remote-thumb-\(fingerprint.hexString)"
    }

    func memoryCachedThumbnail(for fingerprint: Data) -> UIImage? {
        ImageCache.default.retrieveImageInMemoryCache(forKey: Self.cacheKey(for: fingerprint))
    }

    private func diskCachedThumbnail(for fingerprint: Data) async -> UIImage? {
        let key = Self.cacheKey(for: fingerprint)
        return await withCheckedContinuation { continuation in
            ImageCache.default.retrieveImage(forKey: key) { result in
                switch result {
                case .success(let value):
                    continuation.resume(returning: value.image)
                case .failure:
                    continuation.resume(returning: nil)
                }
            }
        }
    }

    private func store(_ image: UIImage, for fingerprint: Data) {
        ImageCache.default.store(image, forKey: Self.cacheKey(for: fingerprint))
    }

    // MARK: - Auto resolution (grid, no full-size download)

    // L1 disk → local PHAsset render → L2 sidecar. Returns nil when nothing is available without a
    // full original download (the cell then shows a tap-to-load affordance). Stores into L1 and, when
    // it rendered locally and the node opts in, opportunistically uploads the shared L2 sidecar.
    func resolveAutoThumbnail(for fingerprint: Data) async -> UIImage? {
        if let cached = await diskCachedThumbnail(for: fingerprint) {
            return cached
        }
        if Task.isCancelled { return nil }

        if let localID = localIdentifier(for: fingerprint),
           let rendered = await renderLocalThumbnail(localIdentifier: localID) {
            store(rendered, for: fingerprint)
            await uploadSidecarIfEnabled(rendered, fingerprint: fingerprint)
            return rendered
        }
        if Task.isCancelled { return nil }

        if let sidecar = await downloadSidecarThumbnail(for: fingerprint) {
            store(sidecar, for: fingerprint)
            return sidecar
        }
        return nil
    }

    // MARK: - On-tap resolution (download original, downsample — photos only)

    func resolveOnTapThumbnail(for fingerprint: Data, primaryRemoteRelativePath: String?) async -> UIImage? {
        guard let primaryRemoteRelativePath else { return nil }
        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: primaryRemoteRelativePath
        )
        let image = await withClient { client -> UIImage in
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_src_\(UUID().uuidString)")
            defer { try? FileManager.default.removeItem(at: tempURL) }
            try await client.download(remotePath: remotePath, localURL: tempURL)
            guard let image = Self.downsampledImage(at: tempURL, maxPixel: Self.thumbnailMaxPixel) else {
                throw RemoteThumbnailError.decodeFailed
            }
            return image
        }
        guard let image else { return nil }
        store(image, for: fingerprint)
        await uploadSidecarIfEnabled(image, fingerprint: fingerprint)
        return image
    }

    // MARK: - Original (full-size) materialization for full-screen viewing

    struct MaterializedOriginal {
        let url: URL
        let isTemporary: Bool   // false for external-volume direct reads — caller must not delete
    }

    func materializeOriginal(remoteRelativePath: String) async -> MaterializedOriginal? {
        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: remoteRelativePath
        )
        return await withClient { client -> MaterializedOriginal in
            if let direct = await client.directReadURL(forRemotePath: remotePath) {
                return MaterializedOriginal(url: direct, isTemporary: false)
            }
            let ext = (remoteRelativePath as NSString).pathExtension
            let name = "orig_\(UUID().uuidString)" + (ext.isEmpty ? "" : ".\(ext)")
            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(name)
            try await client.download(remotePath: remotePath, localURL: tempURL)
            return MaterializedOriginal(url: tempURL, isTemporary: true)
        }
    }

    // After a photo is viewed full-screen, seed the grid thumbnail (L1 + opportunistic L2) so the
    // placeholder cell self-heals next time.
    func cacheThumbnail(fromOriginalFile url: URL, for fingerprint: Data) async {
        guard let image = Self.downsampledImage(at: url, maxPixel: Self.thumbnailMaxPixel) else { return }
        store(image, for: fingerprint)
        await uploadSidecarIfEnabled(image, fingerprint: fingerprint)
    }

    // MARK: - L2 sidecar download

    private func downloadSidecarThumbnail(for fingerprint: Data) async -> UIImage? {
        let remotePath = RemoteThumbnailPaths.absolutePath(
            basePath: profile.basePath,
            fingerprintHex: fingerprint.hexString
        )
        return await withClient { client -> UIImage in
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_dl_\(UUID().uuidString).jpg")
            defer { try? FileManager.default.removeItem(at: tempURL) }
            try await client.download(remotePath: remotePath, localURL: tempURL)
            guard let image = UIImage(contentsOfFile: tempURL.path) else {
                throw RemoteThumbnailError.decodeFailed
            }
            return image
        }
    }

    private enum RemoteThumbnailError: Error {
        case decodeFailed
    }

    // MARK: - L2 opportunistic writeback (P3)

    private func uploadSidecarIfEnabled(_ image: UIImage, fingerprint: Data) async {
        guard generateRemoteThumbnails else { return }
        _ = await uploadSidecar(image, fingerprint: fingerprint)
    }

    // Uploads the sidecar unconditionally (the explicit backfill / opportunistic-writeback primitive).
    // Returns true only when a new file was written (skips when one already exists). Best-effort.
    @discardableResult
    private func uploadSidecar(_ image: UIImage, fingerprint: Data) async -> Bool {
        guard let data = image.jpegData(compressionQuality: 0.8) else { return false }
        let fingerprintHex = fingerprint.hexString
        let thumbPath = RemoteThumbnailPaths.absolutePath(basePath: profile.basePath, fingerprintHex: fingerprintHex)
        let shardDir = RemoteThumbnailPaths.shardDirectoryAbsolutePath(basePath: profile.basePath, fingerprintHex: fingerprintHex)
        let result = await withClient { client -> Bool in
            if (try? await client.exists(path: thumbPath)) == true { return false }
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_up_\(fingerprintHex)_\(UUID().uuidString).jpg")
            defer { try? FileManager.default.removeItem(at: tempURL) }
            try data.write(to: tempURL)
            try? await client.createDirectory(path: shardDir)
            try await client.upload(localURL: tempURL, remotePath: thumbPath, respectTaskCancellation: true, onProgress: nil)
            return true
        }
        return result ?? false
    }

    // MARK: - Maintenance (backfill / purge)

    struct BackfillResult: Sendable {
        var generated = 0
        var skipped = 0
    }

    // Generates sidecars for assets still present locally (by fingerprint) that lack one on the node.
    // Explicit action, so it uploads regardless of the per-node flag. Reports 1-based progress.
    func backfillSidecars(
        fingerprints: [Data],
        progress: @MainActor @Sendable @escaping (_ done: Int, _ total: Int) -> Void,
        isCancelled: @Sendable @escaping () -> Bool
    ) async -> BackfillResult {
        await prepareLocalIndex()
        var result = BackfillResult()
        let total = fingerprints.count
        for (index, fingerprint) in fingerprints.enumerated() {
            if isCancelled() { break }
            await progress(index + 1, total)
            guard let localID = localIdentifier(for: fingerprint) else { result.skipped += 1; continue }
            let thumbPath = RemoteThumbnailPaths.absolutePath(basePath: profile.basePath, fingerprintHex: fingerprint.hexString)
            if (await withClient { client -> Bool in try await client.exists(path: thumbPath) }) == true {
                result.skipped += 1
                continue
            }
            guard let image = await renderLocalThumbnail(localIdentifier: localID) else { result.skipped += 1; continue }
            // Don't populate L1 here — a large backfill would flood the on-device cache with thumbnails
            // the user isn't viewing. The upload (shared L2) is the point.
            if await uploadSidecar(image, fingerprint: fingerprint) { result.generated += 1 } else { result.skipped += 1 }
        }
        return result
    }

    // Deletes the node's entire thumbnail tree and clears the local cache.
    func purgeRemoteThumbnails() async -> Bool {
        let root = RemoteThumbnailPaths.rootAbsolutePath(basePath: profile.basePath)
        let ok = await withClient { client -> Bool in
            try await Self.recursiveDelete(path: root, client: client)
            return true
        } ?? false
        await RemoteThumbnailCache.clear()
        return ok
    }

    private static func recursiveDelete(path: String, client: any RemoteStorageClientProtocol) async throws {
        let entries = (try? await client.list(path: path)) ?? []
        for entry in entries {
            if entry.isDirectory {
                try await recursiveDelete(path: entry.path, client: client)
            } else {
                try? await client.delete(path: entry.path)
            }
        }
        try? await client.delete(path: path)
    }

    // MARK: - Local render

    private func renderLocalThumbnail(localIdentifier: String) async -> UIImage? {
        let result = PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil)
        guard result.count > 0 else { return nil }
        let asset = result.object(at: 0)
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = false
        options.isSynchronous = false

        return await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            PHImageManager.default().requestImage(
                for: asset,
                targetSize: CGSize(width: Self.thumbnailMaxPixel, height: Self.thumbnailMaxPixel),
                contentMode: .aspectFill,
                options: options
            ) { image, _ in
                if resumed.tryResume() {
                    continuation.resume(returning: image)
                }
            }
        }
    }

    // MARK: - Pool helper

    // Returns nil on any error (best-effort browse reads). A connection-unavailable error drops the
    // client so the pool reconnects; an expected miss (e.g. sidecar absent) keeps it for reuse.
    private func withClient<T>(_ body: (any RemoteStorageClientProtocol) async throws -> T) async -> T? {
        // Bail before parking on the pool so a scrolled-away (cancelled) cell doesn't hold a slot.
        if Task.isCancelled { return nil }
        guard let client = try? await pool.acquire() else { return nil }
        do {
            let result = try await body(client)
            await pool.release(client, reusable: true)
            return result
        } catch {
            let reusable = !profile.isConnectionUnavailableError(error)
            await pool.release(client, reusable: reusable)
            return nil
        }
    }

    // MARK: - Downsampling

    static func downsampledImage(at url: URL, maxPixel: Int) -> UIImage? {
        let sourceOptions = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(url as CFURL, sourceOptions) else { return nil }
        let options: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceThumbnailMaxPixelSize: maxPixel,
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary) else { return nil }
        return UIImage(cgImage: cgImage)
    }

    private final class ResumeGuard: @unchecked Sendable {
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
}

// Bounds the on-disk thumbnail cache so it self-evicts (LRU) instead of growing without limit.
enum RemoteThumbnailCache {
    private static var configured = false
    private static let configureLock = NSLock()

    static func configureIfNeeded() {
        configureLock.withLock {
            guard !configured else { return }
            configured = true
            // ~256 MB on disk, expire after 30 days idle.
            ImageCache.default.diskStorage.config.sizeLimit = 256 * 1024 * 1024
            ImageCache.default.diskStorage.config.expiration = .days(30)
        }
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
