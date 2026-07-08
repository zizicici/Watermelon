import AVFoundation
import Foundation
import ImageIO
import Kingfisher
import MoreKit
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
    private let storageClientFactory: StorageClientFactory
    private let profile: ServerProfileRecord
    private let generateRemoteThumbnails: Bool
    // Browser-dedicated pool: long-lived sessions with a hard cap, never shared with backup/sync transfers.
    private let pool: MediaBrowserConnectionPool
    // Single source of truth for the fingerprint→localIdentifier map (and remote-presence set).
    private let presenceIndex: LibraryPresenceIndex

    // Stable identity of this service's profile — matched against the shared snapshot's owner so a source
    // built for one profile never renders another profile's snapshot (profile-switch window).
    var remoteProfileKey: String { RemoteIndexSyncService.remoteProfileKey(profile) }

    // Bounds concurrent connection use to the pool size and, unlike the pool's own parking, observes
    // cancellation — so a scrolled-away cell waiting for a slot is freed immediately instead of
    // stranding it (priority inversion). Sized to match the pool.
    private let connectionGate: AsyncSemaphore

    // Fingerprints whose L2 sidecar is known present this session — lets the opportunistic writeback skip a
    // redundant `exists` round trip (and the detached task) on re-browse. Cleared on purge.
    private let knownSidecarLock = NSLock()
    private var knownSidecarFingerprints: Set<Data> = []

    init(
        storageClientFactory: StorageClientFactory,
        presenceIndex: LibraryPresenceIndex,
        profile: ServerProfileRecord,
        password: String,
        maxConnections: Int = 3
    ) {
        self.storageClientFactory = storageClientFactory
        self.presenceIndex = presenceIndex
        self.profile = profile
        self.generateRemoteThumbnails = profile.generateRemoteThumbnails
        self.pool = MediaBrowserConnectionPool(
            maxConnections: maxConnections,
            makeClient: { [storageClientFactory, profile, password] in
                try storageClientFactory.makeClient(profile: profile, password: password)
            }
        )
        self.connectionGate = AsyncSemaphore(value: maxConnections)
        MediaThumbnailCache.configureIfNeeded()
    }

    // Presence map lifecycle delegates to the single LibraryPresenceIndex.
    func prepareLocalIndex() async { await presenceIndex.refresh() }
    func invalidateLocalIndex() { presenceIndex.invalidate() }

    func shutdown() async {
        await pool.shutdown()
    }

    func localIdentifier(for fingerprint: Data) -> String? {
        presenceIndex.localIdentifier(for: fingerprint)
    }

    // MARK: - Auto resolution (grid, no full-size download)

    // L1 memory/disk cache → local PHAsset render (cached) → cached original → L2 sidecar. Returns nil
    // when nothing is available without a full download (the cell shows a tap-to-load affordance).
    // Checking the cache first makes re-scrolling instant; local renders are persisted (bounded by cap).
    func resolveAutoThumbnail(for fingerprint: Data) async -> UIImage? {
        if let cached = await MediaThumbnailCache.cached(for: fingerprint) {
            return cached
        }
        if Task.isCancelled { return nil }

        if let localID = localIdentifier(for: fingerprint),
           let rendered = await renderLocalThumbnail(localIdentifier: localID) {
            MediaThumbnailCache.store(rendered, for: fingerprint)
            scheduleSidecarWriteback(rendered, fingerprint: fingerprint)
            return rendered
        }
        if Task.isCancelled { return nil }

        // A photo original may still be cached (e.g. the thumbnail cache was cleared separately) — derive
        // the thumbnail from it locally instead of pulling the remote sidecar. Skipped when the original
        // cache is Off (fully disabled): the read path must not resurrect a disabled persistent cache.
        if OriginalPhotoCacheSizeLimit.getValue().maxBytes != nil,
           let originalURL = OriginalPhotoCache.shared.url(forKey: OriginalPhotoCache.photoKey(fingerprintHex: fingerprint.hexString)),
           let derived = Self.downsampledThumbnail(at: originalURL) {
            MediaThumbnailCache.store(derived, for: fingerprint)
            return derived
        }
        if Task.isCancelled { return nil }

        if let sidecar = await downloadSidecarThumbnail(for: fingerprint) {
            MediaThumbnailCache.store(sidecar.image, original: sidecar.data, for: fingerprint)
            markSidecarPresent(fingerprint)
            return sidecar.image
        }
        return nil
    }

    // MARK: - Thumbnail warm-up from a viewed original

    // Derives the grid thumbnail (L1 + opportunistic L2 sidecar) from an original the viewer just
    // materialized, so a remote-only photo with no sidecar stops showing the load affordance — and re-fetching
    // its full original — after it has been opened once. No-op when a thumbnail is already cached; the sidecar
    // write still respects the per-node generate-thumbnails flag. Read-only on `url` (never deletes it).
    func cacheThumbnail(fromOriginalAt url: URL, fingerprint: Data) async {
        if await MediaThumbnailCache.cached(for: fingerprint) != nil { return }
        guard let image = Self.downsampledThumbnail(at: url) else { return }
        MediaThumbnailCache.store(image, for: fingerprint)
        scheduleSidecarWriteback(image, fingerprint: fingerprint)
    }

    // MARK: - Original (full-size) materialization for full-screen viewing

    struct MaterializedOriginal {
        let url: URL
        let isTemporary: Bool   // false for external-volume direct reads — caller must not delete
    }

    // When cacheKey + cacheCapBytes are provided (cache enabled), the download is persisted in
    // OriginalPhotoCache and reused on later views; otherwise it is a view-once temp file. A non-nil
    // maxEntryBytes keeps oversized files (large videos) out of the cache. Local-present assets pass nil.
    func materializeOriginal(
        remoteRelativePath: String,
        cacheKey: String? = nil,
        cacheCapBytes: Int64? = nil,
        maxEntryBytes: Int64? = nil
    ) async -> MaterializedOriginal? {
        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: remoteRelativePath
        )
        if let key = cacheKey, cacheCapBytes != nil, let cached = OriginalPhotoCache.shared.url(forKey: key) {
            return MaterializedOriginal(url: cached, isTemporary: false)
        }
        return await withClient { client -> MaterializedOriginal in
            if let direct = await client.directReadURL(forRemotePath: remotePath) {
                return MaterializedOriginal(url: direct, isTemporary: false)
            }
            let ext = (remoteRelativePath as NSString).pathExtension
            let name = "orig_\(UUID().uuidString)" + (ext.isEmpty ? "" : ".\(ext)")
            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(name)
            do {
                try await client.download(remotePath: remotePath, localURL: tempURL)
                // A cancelled download can return a truncated file (client-dependent) — never cache/serve it.
                try Task.checkCancellation()
            } catch {
                try? FileManager.default.removeItem(at: tempURL)
                throw error
            }
            // Also require the file to fit the whole cap: a single entry larger than the cap would be
            // evicted by the very next enforceCap, handing the caller a URL to a file that no longer exists.
            // Such an entry stays a view-once temp instead.
            if let key = cacheKey, let capBytes = cacheCapBytes,
               Self.fits(tempURL, maxEntryBytes: maxEntryBytes), Self.fits(tempURL, maxEntryBytes: capBytes),
               let cachedURL = OriginalPhotoCache.shared.store(movingFrom: tempURL, forKey: key) {
                OriginalPhotoCache.shared.enforceCap(maxBytes: capBytes)
                return MaterializedOriginal(url: cachedURL, isTemporary: false)
            }
            return MaterializedOriginal(url: tempURL, isTemporary: true)
        }
    }

    // A per-entry ceiling (keeps large videos out of the cache); nil means no per-entry limit.
    private static func fits(_ url: URL, maxEntryBytes: Int64?) -> Bool {
        guard let maxEntryBytes else { return true }
        let size = (try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0
        return Int64(size) <= maxEntryBytes
    }

    // Materializes a photo/video original from the on-device PHAsset without network. Returns nil when
    // the original isn't local (e.g. iCloud-only, not downloaded) so the caller falls back to remote.
    // Local originals are never persisted in OriginalPhotoCache — they're always free to re-fetch.
    func materializeLocalOriginal(localIdentifier: String, isVideo: Bool) async -> MaterializedOriginal? {
        let result = PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil)
        guard result.count > 0 else { return nil }
        let asset = result.object(at: 0)
        return isVideo ? await requestLocalVideo(asset) : await requestLocalPhoto(asset)
    }

    private func requestLocalPhoto(_ asset: PHAsset) async -> MaterializedOriginal? {
        let options = PHImageRequestOptions()
        options.isNetworkAccessAllowed = false
        options.deliveryMode = .highQualityFormat
        options.isSynchronous = false
        options.version = .current
        let data: Data? = await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            PHImageManager.default().requestImageDataAndOrientation(for: asset, options: options) { data, _, _, _ in
                if resumed.tryResume() { continuation.resume(returning: data) }
            }
        }
        guard let data else { return nil }
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("orig_local_\(UUID().uuidString)")
        do {
            try data.write(to: tempURL)
            return MaterializedOriginal(url: tempURL, isTemporary: true)
        } catch {
            return nil
        }
    }

    private func requestLocalVideo(_ asset: PHAsset) async -> MaterializedOriginal? {
        let options = PHVideoRequestOptions()
        options.isNetworkAccessAllowed = false
        options.deliveryMode = .highQualityFormat
        options.version = .current
        let avAsset: AVAsset? = await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            PHImageManager.default().requestAVAsset(forVideo: asset, options: options) { avAsset, _, _ in
                if resumed.tryResume() { continuation.resume(returning: avAsset) }
            }
        }
        // Only a URL-backed (non-composited) asset yields a directly playable file. The PhotoKit file is
        // library-managed — must NOT be deleted, so isTemporary is false.
        guard let urlAsset = avAsset as? AVURLAsset else { return nil }
        return MaterializedOriginal(url: urlAsset.url, isTemporary: false)
    }

    // MARK: - L2 sidecar download

    // Returns the raw downloaded bytes too, so the caller can persist them verbatim (no re-encode).
    private func downloadSidecarThumbnail(for fingerprint: Data) async -> (image: UIImage, data: Data)? {
        let remotePath = RemoteThumbnailPaths.absolutePath(
            basePath: profile.basePath,
            fingerprintHex: fingerprint.hexString
        )
        return await withClient { client -> (image: UIImage, data: Data) in
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_dl_\(UUID().uuidString).jpg")
            defer { try? FileManager.default.removeItem(at: tempURL) }
            try await client.download(remotePath: remotePath, localURL: tempURL)
            guard let data = try? Data(contentsOf: tempURL), let image = UIImage(data: data) else {
                throw RemoteThumbnailError.decodeFailed
            }
            return (image, data)
        }
    }

    private enum RemoteThumbnailError: Error {
        case decodeFailed
    }

    // MARK: - L2 opportunistic writeback (P3)

    // Opportunistic writeback must never block thumbnail display — detach it so the rendered image
    // returns now; the upload rides the shared connection gate in the background.
    private func scheduleSidecarWriteback(_ image: UIImage, fingerprint: Data) {
        guard generateRemoteThumbnails, !sidecarKnownPresent(fingerprint) else { return }
        Task { [weak self] in _ = await self?.uploadSidecar(image, fingerprint: fingerprint) }
    }

    private func sidecarKnownPresent(_ fingerprint: Data) -> Bool {
        knownSidecarLock.withLock { knownSidecarFingerprints.contains(fingerprint) }
    }

    private func markSidecarPresent(_ fingerprint: Data) {
        knownSidecarLock.withLock { _ = knownSidecarFingerprints.insert(fingerprint) }
    }

    private func forgetAllSidecars() {
        knownSidecarLock.withLock { knownSidecarFingerprints.removeAll() }
    }

    // Uploads the sidecar unconditionally (the explicit backfill / opportunistic-writeback primitive).
    // Returns true only when a new file was written (skips when one already exists). Best-effort.
    @discardableResult
    private func uploadSidecar(_ image: UIImage, fingerprint: Data) async -> Bool {
        guard let data = image.jpegData(compressionQuality: ThumbnailSizing.jpegCompressionQuality) else { return false }
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
            do {
                // Atomic create-if-absent, not replace: the exists check above is a non-atomic fast path, and an
                // opportunistic local render may be non-authoritative (edited-after-backup) — never overwrite a
                // sidecar a concurrent authoritative writer (backup / another device / backfill) just published.
                try await client.upload(localURL: tempURL, remotePath: thumbPath, mode: .createIfAbsent, respectTaskCancellation: true, onProgress: nil)
                return true
            } catch {
                if SMBErrorClassifier.isNameCollision(error) { return false }   // already present → skip, not a write failure
                throw error
            }
        }
        // exists==true / collision (result false) or a fresh upload (true) all mean it's present now; a nil is a
        // connection failure — leave it unknown so a later browse retries.
        if result != nil { markSidecarPresent(fingerprint) }
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

    // Deletes the node's entire thumbnail tree and clears the local cache. Reports success only when a client
    // was acquired and no list/delete failure was observed — a partial failure leaves sidecars on the node, so
    // the maintenance UI must not claim the purge completed. The local L1/known-sidecar state is cleared either
    // way (regenerable, and re-checked on the next browse).
    func purgeRemoteThumbnails() async -> Bool {
        let root = RemoteThumbnailPaths.rootAbsolutePath(basePath: profile.basePath)
        let failures = await withClient { client -> Int in
            try await Self.recursiveDelete(path: root, client: client)
        }
        await MediaThumbnailCache.clear()
        forgetAllSidecars()
        // nil = no client acquired (or cancelled mid-delete); a positive count = observed list/delete failures.
        return failures == 0
    }

    // Best-effort recursive delete; returns the count of observed list/delete failures so the caller can report
    // partial failure instead of unconditional success. A not-found subtree is nothing to delete (success);
    // cancellation propagates so a torn-down purge stops promptly. Mirrors ThumbnailOrphanScanner's fault handling.
    // `internal` (not `private`) only so RemoteThumbnailPurgeTests can exercise the failure-count path directly.
    static func recursiveDelete(path: String, client: any RemoteStorageClientProtocol) async throws -> Int {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: path)
        } catch {
            switch RemoteFaultLite.classify(error) {
            case .cancelled: throw error
            case .notFound: return 0             // nothing here to delete
            case .retryable, .terminal: return 1 // can't enumerate a directory that should exist
            }
        }
        var failures = 0
        for entry in entries {
            if entry.isDirectory {
                failures += try await Self.recursiveDelete(path: entry.path, client: client)
            } else {
                do {
                    try await client.delete(path: entry.path)
                } catch {
                    if RemoteFaultLite.classify(error) == .cancelled { throw error }
                    failures += 1
                }
            }
        }
        do {
            try await client.delete(path: path)
        } catch {
            switch RemoteFaultLite.classify(error) {
            case .cancelled: throw error
            case .notFound: break                // already gone (e.g. a child delete emptied then removed it)
            case .retryable, .terminal: failures += 1
            }
        }
        return failures
    }

    // MARK: - Local render

    private func renderLocalThumbnail(localIdentifier: String) async -> UIImage? {
        let result = PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil)
        guard result.count > 0 else { return nil }
        let asset = result.object(at: 0)
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = false
        options.isSynchronous = false
        guard let targetLongSide = ThumbnailSizing.targetLongSide(
            originalWidth: asset.pixelWidth,
            originalHeight: asset.pixelHeight
        ) else { return nil }

        // Cancellation-aware: a grid cell scrolled off-screen cancels its request instead of clogging
        // PhotoKit's queue ahead of the cells now on screen.
        let box = RequestIDBox()
        let image = await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                let resumed = ResumeGuard()
                let requestID = PHImageManager.default().requestImage(
                    for: asset,
                    targetSize: CGSize(width: targetLongSide, height: targetLongSide),
                    contentMode: .aspectFit,
                    options: options
                ) { image, _ in
                    if resumed.tryResume() {
                        continuation.resume(returning: image)
                    }
                }
                box.setOrCancel(requestID)
            }
        } onCancel: {
            box.cancel()
        }
        guard let image else { return nil }
        return ThumbnailSizing.fittedImage(image, maximumLongSide: targetLongSide)
    }

    // MARK: - Pool helper

    // Returns nil on any error (best-effort browse reads). A connection-unavailable error drops the
    // client so the pool reconnects; an expected miss (e.g. sidecar absent) keeps it for reuse.
    private func withClient<T>(_ body: (any RemoteStorageClientProtocol) async throws -> T) async -> T? {
        // Wait on the cancellation-aware gate first, so a scrolled-away cell parked here is released
        // immediately. Cancellation past this point stops the WAIT only — the pool never aborts a connect.
        guard await connectionGate.wait() else { return nil }
        defer { connectionGate.signal() }
        guard let client = await pool.acquire() else { return nil }
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

    static func downsampledThumbnail(at url: URL) -> UIImage? {
        let sourceOptions = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(url as CFURL, sourceOptions),
              let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
              let width = intProperty(kCGImagePropertyPixelWidth, in: properties),
              let height = intProperty(kCGImagePropertyPixelHeight, in: properties),
              let maxPixel = ThumbnailSizing.targetLongSide(originalWidth: width, originalHeight: height) else {
            return nil
        }
        return downsampledImage(source: source, maxPixel: maxPixel)
    }

    static func downsampledImage(at url: URL, maxPixel: Int) -> UIImage? {
        let sourceOptions = [kCGImageSourceShouldCache: false] as CFDictionary
        guard let source = CGImageSourceCreateWithURL(url as CFURL, sourceOptions) else { return nil }
        return downsampledImage(source: source, maxPixel: maxPixel)
    }

    private static func downsampledImage(source: CGImageSource, maxPixel: Int) -> UIImage? {
        let options: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceThumbnailMaxPixelSize: maxPixel,
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary) else { return nil }
        return UIImage(cgImage: cgImage)
    }

    private static func intProperty(_ key: CFString, in properties: [CFString: Any]) -> Int? {
        if let value = properties[key] as? Int { return value }
        return (properties[key] as? NSNumber)?.intValue
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
