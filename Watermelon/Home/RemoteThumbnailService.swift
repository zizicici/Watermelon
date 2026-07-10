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
    private let password: String
    private let generateRemoteThumbnails: Bool
    // Browser-dedicated pool: long-lived sessions with a hard cap, never shared with backup/sync transfers.
    private let pool: MediaBrowserConnectionPool
    // Single source of truth for the fingerprint→localIdentifier map (and remote-presence set).
    private let presenceIndex: LibraryPresenceIndex
    private let encryptionContextCache = RemoteThumbnailEncryptionContextCache()

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
    private var knownSidecarKeys: Set<ThumbnailSidecarKey> = []

    // In-flight opportunistic writebacks, tracked so shutdown can cancel + drain them — an untracked Task
    // would outlive the browser session and could race a purge started right after dismissal.
    private let writebackLock = NSLock()
    private var writebackTasks: [UUID: Task<Void, Never>] = [:]
    private var isShutdown = false

    // Writebacks across ALL live service instances, plus a purge gate: purge runs on its own service and
    // never awaits the browser's fire-and-forget shutdown, so it must drain foreign writers itself and
    // refuse new ones for the whole sweep. Where nested, writebackLock is outer, this lock inner.
    private static let globalWritebackLock = NSLock()
    private static var globalWritebackTasks: [UUID: Task<Void, Never>] = [:]
    private static var isPurgeInProgress = false

    // Cache keys whose entry already passed the manifest-hash check this session (skip re-hashing per view).
    private let verifiedOriginals = VerifiedOriginalLatch()

    // Latches each verified cache key to the hash it passed against: same-fingerprint twin records can
    // share one cache key with different manifest hashes, so bytes verified for one record must never be
    // trusted for another. `internal` only so the key+hash contract is directly pinnable by tests.
    final class VerifiedOriginalLatch: @unchecked Sendable {
        private let lock = NSLock()
        private var verifiedHashByKey: [String: Data] = [:]

        func isVerified(key: String, contentHash: Data) -> Bool {
            lock.withLock { verifiedHashByKey[key] == contentHash }
        }

        func mark(key: String, contentHash: Data) {
            lock.withLock { verifiedHashByKey[key] = contentHash }
        }

        func clear(key: String) {
            lock.withLock { _ = verifiedHashByKey.removeValue(forKey: key) }
        }
    }

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
        self.password = password
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
        // Drain writebacks before the pool: a survivor's late create-if-absent could re-create a sidecar a
        // subsequent purge just swept (spurious purge failure, or a surviving object on S3). The flag is
        // flipped under the same lock hold as the snapshot, so no writeback can register behind the drain.
        let tasks = writebackLock.withLock { () -> [Task<Void, Never>] in
            isShutdown = true
            defer { writebackTasks.removeAll() }
            return Array(writebackTasks.values)
        }
        for task in tasks { task.cancel() }
        for task in tasks { await task.value }
        await pool.shutdown()
    }

    // Current-bytes handles only (batch): a stale hash row's handle must not bind to its pre-edit fingerprint.
    func localIdentifiersForCurrentBytes(_ fingerprints: [Data]) -> [Data: String] {
        presenceIndex.localIdentifiersForCurrentBytes(fingerprints)
    }

    // Current-bytes check for specific handles (the sources' use-time gate before local-first materialization).
    func currentFingerprints(forAssetIDs assetIDs: some Collection<String>) -> [String: Data] {
        presenceIndex.currentFingerprints(forAssetIDs: assetIDs)
    }

    // MARK: - Auto resolution (grid, no full-size download)

    // L1 memory/disk cache → local PHAsset render (cached) → cached original → L2 sidecar. Returns nil
    // when nothing is available without a full download (the cell shows a tap-to-load affordance).
    // Checking the cache first makes re-scrolling instant; local renders are persisted (bounded by cap).
    func resolveAutoThumbnail(
        for fingerprint: Data,
        expectedPhotoContentHash: Data? = nil,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        encryptionKeyID: String? = nil
    ) async -> UIImage? {
        let cachePolicy = await thumbnailCachePolicy(storageCodec: storageCodec, encryptionKeyID: encryptionKeyID)
        if cachePolicy.canUseSharedCaches,
           let cached = await MediaThumbnailCache.cached(
               for: fingerprint,
               storageCodec: cachePolicy.storageCodec,
               encryptionKeyID: cachePolicy.encryptionKeyID
           ) {
            return cached
        }
        if Task.isCancelled { return nil }

        // Current-bytes handle only: an edited-after-backup asset's render must not seed L1 (nor publish the
        // shared L2 sidecar) under the fingerprint of the pre-edit backup.
        if let localID = presenceIndex.localIdentifierForCurrentBytes(fingerprint),
           let rendered = await renderLocalThumbnail(localIdentifier: localID) {
            if cachePolicy.canUseSharedCaches {
                MediaThumbnailCache.store(
                    rendered,
                    for: fingerprint,
                    storageCodec: cachePolicy.storageCodec,
                    encryptionKeyID: cachePolicy.encryptionKeyID
                )
                scheduleSidecarWriteback(rendered, fingerprint: fingerprint, cachePolicy: cachePolicy)
            }
            return rendered
        }
        if Task.isCancelled { return nil }

        // A photo original may still be cached (e.g. the thumbnail cache was cleared separately) — derive
        // the thumbnail from it locally instead of pulling the remote sidecar, but only after it passes the
        // manifest-hash check (a poisoned entry must not seed the shared L1). Skipped when the original
        // cache is Off (fully disabled): the read path must not resurrect a disabled persistent cache.
        if cachePolicy.canUseSharedCaches, OriginalPhotoCacheSizeLimit.getValue().maxBytes != nil {
            let key = OriginalPhotoCache.photoKey(fingerprintHex: fingerprint.hexString)
            if let originalURL = OriginalPhotoCache.shared.url(forKey: key),
                verifyCachedOriginal(at: originalURL, key: key, expectedContentHash: expectedPhotoContentHash),
               let derived = Self.downsampledThumbnail(at: originalURL) {
                MediaThumbnailCache.store(
                    derived,
                    for: fingerprint,
                    storageCodec: cachePolicy.storageCodec,
                    encryptionKeyID: cachePolicy.encryptionKeyID
                )
                return derived
            }
        }
        if Task.isCancelled { return nil }

        if cachePolicy.canUseSharedCaches, let sidecar = await downloadSidecarThumbnail(for: fingerprint, cachePolicy: cachePolicy) {
            MediaThumbnailCache.store(
                sidecar.image,
                original: sidecar.data,
                for: fingerprint,
                storageCodec: cachePolicy.storageCodec,
                encryptionKeyID: cachePolicy.encryptionKeyID
            )
            markSidecarPresent(fingerprint, cachePolicy: cachePolicy)
            return sidecar.image
        }
        return nil
    }

    // MARK: - Thumbnail warm-up from a viewed original

    // Derives the grid thumbnail (L1 + opportunistic L2 sidecar) from an original the viewer just
    // materialized, so a remote-only photo with no sidecar stops showing the load affordance — and re-fetching
    // its full original — after it has been opened once. No-op when a thumbnail is already cached; the sidecar
    // write still respects the per-node generate-thumbnails flag. Read-only on `url` (never deletes it).
    func cacheThumbnail(
        fromOriginalAt url: URL,
        fingerprint: Data,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        encryptionKeyID: String? = nil
    ) async {
        let cachePolicy = await thumbnailCachePolicy(storageCodec: storageCodec, encryptionKeyID: encryptionKeyID)
        guard cachePolicy.canUseSharedCaches else { return }
        if await MediaThumbnailCache.cached(
            for: fingerprint,
            storageCodec: cachePolicy.storageCodec,
            encryptionKeyID: cachePolicy.encryptionKeyID
        ) != nil { return }
        guard let image = Self.downsampledThumbnail(at: url) else { return }
        MediaThumbnailCache.store(
            image,
            for: fingerprint,
            storageCodec: cachePolicy.storageCodec,
            encryptionKeyID: cachePolicy.encryptionKeyID
        )
        scheduleSidecarWriteback(image, fingerprint: fingerprint, cachePolicy: cachePolicy)
    }

    func cacheThumbnail(
        fromVideoOriginalAt url: URL,
        fingerprint: Data,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        encryptionKeyID: String? = nil
    ) async {
        let cachePolicy = await thumbnailCachePolicy(storageCodec: storageCodec, encryptionKeyID: encryptionKeyID)
        guard cachePolicy.canUseSharedCaches else { return }
        if await MediaThumbnailCache.cached(
            for: fingerprint,
            storageCodec: cachePolicy.storageCodec,
            encryptionKeyID: cachePolicy.encryptionKeyID
        ) != nil { return }
        guard let image = Self.thumbnailFromVideo(at: url) else { return }
        MediaThumbnailCache.store(
            image,
            for: fingerprint,
            storageCodec: cachePolicy.storageCodec,
            encryptionKeyID: cachePolicy.encryptionKeyID
        )
        scheduleSidecarWriteback(image, fingerprint: fingerprint, cachePolicy: cachePolicy)
    }

    // MARK: - Original (full-size) materialization for full-screen viewing

    struct MaterializedOriginal {
        let url: URL
        let isTemporary: Bool   // false for external-volume direct reads — caller must not delete
        var originalFilename: String? = nil
        // False only when downloaded bytes failed the manifest-hash check: display view-once, but never
        // persist them or derive the shared L1/L2 from them.
        var contentMatchesManifest: Bool = true
    }

    // When cacheKey + cacheCapBytes are provided (cache enabled), the download is persisted in
    // OriginalPhotoCache and reused on later views; otherwise it is a view-once temp file. A non-nil
    // maxEntryBytes keeps oversized files (large videos) out of the cache. Local-present assets pass nil.
    // `expectedContentHash` (the manifest's recorded hash) is checked before any persistence; pass
    // `verifyForSharedCaches` when the caller derives L1/L2 from the bytes even without a cache store.
    func materializeOriginal(
        remoteRelativePath: String,
        cacheKey: String? = nil,
        cacheCapBytes: Int64? = nil,
        maxEntryBytes: Int64? = nil,
        expectedContentHash: Data? = nil,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        storedFileSize _: Int64? = nil,
        encryptionKeyID: String? = nil,
        verifyForSharedCaches: Bool = false
    ) async -> MaterializedOriginal? {
        let encryptionContext: RepoEncryptionContext?
        switch storageCodec {
        case RemoteManifestResource.plaintextStorageCodec:
            encryptionContext = nil
        case RemoteManifestResource.encryptedStorageCodec:
            guard let expectedContentHash,
                  expectedContentHash.count == RemoteManifestResource.contentHashByteCount,
                  let context = await thumbnailEncryptionContext(),
                  encryptionKeyID == context.activeKeyID else {
                return nil
            }
            encryptionContext = context
        default:
            return nil
        }
        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: remoteRelativePath
        )
        if encryptionContext == nil, let key = cacheKey, cacheCapBytes != nil, let cached = OriginalPhotoCache.shared.url(forKey: key) {
            // Cached bytes are remote bytes too (pre-verification builds, corruption): re-check against the
            // manifest before display/reuse. A mismatch is evicted and falls through to a fresh download.
            if verifyCachedOriginal(at: cached, key: key, expectedContentHash: expectedContentHash) {
                return MaterializedOriginal(url: cached, isTemporary: false)
            }
        }
        return await withClient { client -> MaterializedOriginal in
            if let direct = await client.directReadURL(forRemotePath: remotePath) {
                if let encryptionContext, let expectedContentHash {
                    return try Self.decryptMaterializedOriginal(
                        encryptedURL: direct,
                        expectedContentHash: expectedContentHash,
                        encryptionContext: encryptionContext,
                        remoteRelativePath: remoteRelativePath
                    )
                }
                // Volume bytes at a recorded path can diverge like any remote's (foreign-writer name reuse,
                // direct file manipulation) — unverified bytes must not seed the shared L1/L2.
                var matches = true
                if let expectedContentHash, !expectedContentHash.isEmpty, verifyForSharedCaches {
                    matches = Self.contentHashCheck(at: direct, expectedContentHash: expectedContentHash) == .match
                }
                return MaterializedOriginal(url: direct, isTemporary: false, contentMatchesManifest: matches)
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
            if let encryptionContext, let expectedContentHash {
                defer { try? FileManager.default.removeItem(at: tempURL) }
                return try Self.decryptMaterializedOriginal(
                    encryptedURL: tempURL,
                    expectedContentHash: expectedContentHash,
                    encryptionContext: encryptionContext,
                    remoteRelativePath: remoteRelativePath
                )
            }
            // Also require the file to fit the whole cap: a single entry larger than the cap would be
            // evicted by the very next enforceCap, handing the caller a URL to a file that no longer exists.
            // Such an entry stays a view-once temp instead.
            let storeEligible = cacheKey != nil && cacheCapBytes != nil
                && Self.fits(tempURL, maxEntryBytes: maxEntryBytes)
                && Self.fits(tempURL, maxEntryBytes: cacheCapBytes)
            // Bytes at a recorded path can diverge from the manifest's hash (name reused by another writer,
            // server-side replacement). A mismatch stays view-once: never stored under the content-addressed
            // key, never laundered into the shared L1/L2.
            var contentMatchesManifest = true
            if let expectedContentHash, !expectedContentHash.isEmpty, storeEligible || verifyForSharedCaches {
                contentMatchesManifest = (try? AssetProcessor.contentHash(of: tempURL)) == expectedContentHash
            }
            if contentMatchesManifest, storeEligible, let key = cacheKey, let capBytes = cacheCapBytes,
               let stored = OriginalPhotoCache.shared.store(movingFrom: tempURL, forKey: key) {
                if stored.storedIncoming {
                    if let expectedContentHash, !expectedContentHash.isEmpty {
                        verifiedOriginals.mark(key: key, contentHash: expectedContentHash)
                    } else {
                        // A no-hash store replaced whatever file an earlier latch entry described (evict →
                        // legacy re-store) — drop the entry so a hashed twin's next view re-hashes the bytes.
                        verifiedOriginals.clear(key: key)
                    }
                    OriginalPhotoCache.shared.enforceCap(maxBytes: capBytes)
                    return MaterializedOriginal(url: stored.url, isTemporary: false)
                }
                // Store collision: the resident entry's bytes were verified by whoever stored them — which
                // may be a same-fingerprint twin with a different manifest hash. Serve them only if they
                // pass THIS record's check; otherwise (mismatch evicted them) serve our own verified bytes
                // view-once. Never latch bytes this call didn't hash.
                if verifyCachedOriginal(at: stored.url, key: key, expectedContentHash: expectedContentHash) {
                    try? FileManager.default.removeItem(at: tempURL)
                    OriginalPhotoCache.shared.enforceCap(maxBytes: capBytes)
                    return MaterializedOriginal(url: stored.url, isTemporary: false)
                }
            }
            return MaterializedOriginal(url: tempURL, isTemporary: true, contentMatchesManifest: contentMatchesManifest)
        }
    }

    // MARK: - Cached-original / direct-read verification

    // Distinguishes a manifest-hash mismatch (evict/reject) from a cancelled read (don't trust the bytes
    // for shared caches, but don't evict a possibly-good entry — a scrolled-away cell's cancellation would
    // thrash the cache). An unreadable file counts as mismatch: it is as unusable as divergent bytes.
    // `internal` only so the decision is directly pinnable by tests.
    enum ContentHashCheck: Equatable {
        case match
        case mismatch
        case cancelled
    }

    static func contentHashCheck(at url: URL, expectedContentHash: Data) -> ContentHashCheck {
        do {
            return try AssetProcessor.contentHash(of: url) == expectedContentHash ? .match : .mismatch
        } catch is CancellationError {
            return .cancelled
        } catch {
            return .mismatch
        }
    }

    private static func decryptMaterializedOriginal(
        encryptedURL: URL,
        expectedContentHash: Data,
        encryptionContext: RepoEncryptionContext,
        remoteRelativePath: String
    ) throws -> MaterializedOriginal {
        let decryptedURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("orig_dec_\(UUID().uuidString)")
        do {
            let keyMaterial = try RepoEncryptionKeyMaterial(
                repoID: encryptionContext.repoID,
                keyID: encryptionContext.activeKeyID,
                keyData: encryptionContext.contentKey
            )
            let metadata = try FileEncryptionService().decrypt(
                encryptedURL: encryptedURL,
                plaintextURL: decryptedURL,
                keyMaterial: keyMaterial,
                externalAAD: FileEncryptionService.resourceExternalAAD(contentHash: expectedContentHash)
            )
            let matches = (try? AssetProcessor.contentHash(of: decryptedURL)) == expectedContentHash
            return MaterializedOriginal(
                url: decryptedURL,
                isTemporary: true,
                originalFilename: metadata.originalFileName.isEmpty ? (remoteRelativePath as NSString).lastPathComponent : metadata.originalFileName,
                contentMatchesManifest: matches
            )
        } catch {
            try? FileManager.default.removeItem(at: decryptedURL)
            throw error
        }
    }

    // True when the cached entry may be displayed and reused. Checked once per session per key+hash; a
    // mismatch evicts the entry so the caller falls through to a fresh, verified download.
    private func verifyCachedOriginal(at url: URL, key: String, expectedContentHash: Data?) -> Bool {
        guard let expectedContentHash, !expectedContentHash.isEmpty else { return true }
        if verifiedOriginals.isVerified(key: key, contentHash: expectedContentHash) { return true }
        switch Self.contentHashCheck(at: url, expectedContentHash: expectedContentHash) {
        case .match:
            verifiedOriginals.mark(key: key, contentHash: expectedContentHash)
            return true
        case .mismatch:
            OriginalPhotoCache.shared.remove(forKey: key)
            verifiedOriginals.clear(key: key)
            return false
        case .cancelled:
            return false
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
    private func downloadSidecarThumbnail(
        for fingerprint: Data,
        cachePolicy: ThumbnailCachePolicy
    ) async -> (image: UIImage, data: Data)? {
        let remotePath = RemoteThumbnailPaths.absolutePath(
            basePath: profile.basePath,
            fingerprintHex: fingerprint.hexString,
            storageCodec: cachePolicy.storageCodec
        )
        return await withClient { client -> (image: UIImage, data: Data) in
            try await Self.readSidecar(
                remotePath: remotePath,
                client: client,
                encryptionContext: cachePolicy.sidecarEncryptionContext,
                fingerprintHex: fingerprint.hexString
            )
        }
    }

    // `internal` only so the torn-sidecar self-heal contract is directly pinnable by tests.
    static func readSidecar(
        remotePath: String,
        client: any RemoteStorageClientProtocol,
        encryptionContext: RepoEncryptionContext? = nil,
        fingerprintHex: String? = nil
    ) async throws -> (image: UIImage, data: Data) {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("thumb_dl_\(UUID().uuidString).jpg")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        try await client.download(remotePath: remotePath, localURL: tempURL)
        // A cancelled download can return a truncated local file (client-dependent) — never judge the
        // remote bytes (let alone delete them) from it.
        try Task.checkCancellation()
        guard let data = try? Data(contentsOf: tempURL) else { throw RemoteThumbnailError.decodeFailed }
        if Self.isCompleteJPEG(data), let image = UIImage(data: data) {
            if encryptionContext != nil {
                throw RemoteThumbnailError.decodeFailed
            }
            return (image, data)
        }
        if let encryptionContext, let fingerprintHex {
            let decryptedURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_dec_\(UUID().uuidString).jpg")
            defer { try? FileManager.default.removeItem(at: decryptedURL) }
            let keyMaterial = try RepoEncryptionKeyMaterial(
                repoID: encryptionContext.repoID,
                keyID: encryptionContext.activeKeyID,
                keyData: encryptionContext.contentKey
            )
            do {
                try FileEncryptionService().decrypt(
                    encryptedURL: tempURL,
                    plaintextURL: decryptedURL,
                    keyMaterial: keyMaterial,
                    externalAAD: FileEncryptionService.thumbnailExternalAAD(fingerprintHex: fingerprintHex)
                )
            } catch {
                throw RemoteThumbnailError.decodeFailed
            }
            guard let plainData = try? Data(contentsOf: decryptedURL),
                  Self.isCompleteJPEG(plainData),
                  let image = UIImage(data: plainData) else {
                throw RemoteThumbnailError.decodeFailed
            }
            return (image, plainData)
        } else {
            throw RemoteThumbnailError.decodeFailed
        }
    }

    // Sidecars are whole JPEGs written by this app; SOI/EOI framing detects a torn partial even when
    // ImageIO would still decode it (a truncated JPEG renders with a gray region).
    static func isCompleteJPEG(_ data: Data) -> Bool {
        data.count >= 4
            && data.prefix(2) == Data([0xFF, 0xD8])
            && data.suffix(2) == Data([0xFF, 0xD9])
    }

    private enum RemoteThumbnailError: Error {
        case decodeFailed
    }

    // MARK: - L2 opportunistic writeback (P3)

    // Opportunistic writeback must never block thumbnail display — detach it so the rendered image
    // returns now; the upload rides the shared connection gate in the background. Creation and
    // registration are atomic with the shutdown/purge checks so no task can escape a drain.
    private func scheduleSidecarWriteback(
        _ image: UIImage,
        fingerprint: Data,
        cachePolicy: ThumbnailCachePolicy
    ) {
        guard cachePolicy.canUseSharedCaches,
              generateRemoteThumbnails,
              !sidecarKnownPresent(fingerprint, cachePolicy: cachePolicy) else { return }
        let id = UUID()
        writebackLock.lock()
        defer { writebackLock.unlock() }
        guard !isShutdown else { return }
        Self.globalWritebackLock.lock()
        defer { Self.globalWritebackLock.unlock() }
        guard !Self.isPurgeInProgress else { return }
        let task = Task { [weak self] in
            if let self {
                _ = await self.uploadSidecar(image, fingerprint: fingerprint, cachePolicy: cachePolicy)
                self.writebackLock.withLock { _ = self.writebackTasks.removeValue(forKey: id) }
            }
            Self.globalWritebackLock.withLock { _ = Self.globalWritebackTasks.removeValue(forKey: id) }
        }
        writebackTasks[id] = task
        Self.globalWritebackTasks[id] = task
    }

    private func sidecarKnownPresent(_ fingerprint: Data, cachePolicy: ThumbnailCachePolicy) -> Bool {
        knownSidecarLock.withLock { knownSidecarKeys.contains(Self.sidecarKey(fingerprint, cachePolicy: cachePolicy)) }
    }

    private func markSidecarPresent(_ fingerprint: Data, cachePolicy: ThumbnailCachePolicy) {
        knownSidecarLock.withLock { _ = knownSidecarKeys.insert(Self.sidecarKey(fingerprint, cachePolicy: cachePolicy)) }
    }

    private func forgetAllSidecars() {
        knownSidecarLock.withLock { knownSidecarKeys.removeAll() }
    }

    private func thumbnailEncryptionContext() async -> RepoEncryptionContext? {
        guard let state = await thumbnailEncryptionState() else { return nil }
        if case .encrypted(let context) = state { return context }
        return nil
    }

    struct ThumbnailCachePolicy: Sendable {
        let canUseSharedCaches: Bool
        let storageCodec: Int
        let encryptionKeyID: String?
        let sidecarEncryptionContext: RepoEncryptionContext?
    }

    private func thumbnailCachePolicy(storageCodec: Int, encryptionKeyID: String?) async -> ThumbnailCachePolicy {
        Self.thumbnailCachePolicy(
            storageCodec: storageCodec,
            encryptionKeyID: encryptionKeyID,
            state: await thumbnailEncryptionState()
        )
    }

    static func thumbnailCachePolicy(
        storageCodec: Int,
        encryptionKeyID: String?,
        state: RemoteThumbnailEncryptionState?
    ) -> ThumbnailCachePolicy {
        guard let state else {
            return ThumbnailCachePolicy(
                canUseSharedCaches: false,
                storageCodec: storageCodec,
                encryptionKeyID: encryptionKeyID,
                sidecarEncryptionContext: nil
            )
        }
        switch state {
        case .encrypted(let context):
            return ThumbnailCachePolicy(
                canUseSharedCaches: true,
                storageCodec: RemoteManifestResource.encryptedStorageCodec,
                encryptionKeyID: context.activeKeyID,
                sidecarEncryptionContext: context
            )
        case .encryptedUnavailable:
            return ThumbnailCachePolicy(
                canUseSharedCaches: false,
                storageCodec: RemoteManifestResource.encryptedStorageCodec,
                encryptionKeyID: encryptionKeyID,
                sidecarEncryptionContext: nil
            )
        case .plaintext:
            guard storageCodec == RemoteManifestResource.plaintextStorageCodec else {
                return ThumbnailCachePolicy(
                    canUseSharedCaches: false,
                    storageCodec: storageCodec,
                    encryptionKeyID: encryptionKeyID,
                    sidecarEncryptionContext: nil
                )
            }
            return ThumbnailCachePolicy(
                canUseSharedCaches: true,
                storageCodec: storageCodec,
                encryptionKeyID: nil,
                sidecarEncryptionContext: nil
            )
        }
    }

    private struct ThumbnailSidecarKey: Hashable {
        let fingerprint: Data
        let storageCodec: Int
        let encryptionKeyID: String?
    }

    private static func sidecarKey(_ fingerprint: Data, cachePolicy: ThumbnailCachePolicy) -> ThumbnailSidecarKey {
        ThumbnailSidecarKey(
            fingerprint: fingerprint,
            storageCodec: cachePolicy.storageCodec,
            encryptionKeyID: cachePolicy.encryptionKeyID
        )
    }

    private func thumbnailEncryptionState() async -> RemoteThumbnailEncryptionState? {
        await encryptionContextCache.value {
            guard let result = await self.withClientResult({ client -> RemoteThumbnailEncryptionState in
                do {
                    let context = try await RepoEncryptionSetupService(
                        keyStore: RepoEncryptionKeychainStore(keychain: KeychainService())
                    ).loadExistingContext(client: client, basePath: profile.basePath)
                    return .encrypted(context)
                } catch let error as RepoEncryptionSetupError {
                    switch error {
                    case .missingEncryptedRepo:
                        return .plaintext
                    case .missingLocalKey, .keyVerificationFailed, .damagedVersion, .unsupportedVersion:
                        return .encryptedUnavailable
                    }
                }
            }) else {
                return nil
            }
            switch result {
            case .success(let state):
                return state
            case .failure:
                return nil
            }
        }
    }

    private enum SidecarUploadOutcome {
        case written
        case alreadyPresent
        case failed
    }

    // Uploads the sidecar unconditionally (the explicit backfill / opportunistic-writeback primitive).
    // `.written` only when a new file landed; `.alreadyPresent` for an exists-probe hit or create collision;
    // `.failed` = connection/upload failure, left unknown so a later browse retries. Best-effort.
    @discardableResult
    private func uploadSidecar(
        _ image: UIImage,
        fingerprint: Data,
        cachePolicy: ThumbnailCachePolicy
    ) async -> SidecarUploadOutcome {
        guard let data = ThumbnailSizing.jpegData(from: image) else { return .failed }
        let fingerprintHex = fingerprint.hexString
        guard cachePolicy.canUseSharedCaches else { return .failed }
        let result: (outcome: SidecarUploadOutcome, policy: ThumbnailCachePolicy)? = await withThumbnailWriteLease { lease in
            let client = lease.client
            let writePolicy = try await thumbnailCachePolicyForWrite(
                storageCodec: cachePolicy.storageCodec,
                encryptionKeyID: cachePolicy.encryptionKeyID,
                client: client
            )
            guard writePolicy.canUseSharedCaches else { return (.failed, writePolicy) }
            let thumbPath = RemoteThumbnailPaths.absolutePath(
                basePath: profile.basePath,
                fingerprintHex: fingerprintHex,
                storageCodec: writePolicy.storageCodec
            )
            let shardDir = RemoteThumbnailPaths.shardDirectoryAbsolutePath(basePath: profile.basePath, fingerprintHex: fingerprintHex)
            let written = try await Self.writeSidecar(
                data,
                fingerprintHex: fingerprintHex,
                thumbPath: thumbPath,
                shardDir: shardDir,
                client: client,
                encryptionContext: writePolicy.sidecarEncryptionContext,
                assertOwnership: lease.assertOwnership
            )
            return (written ? .written : .alreadyPresent, writePolicy)
        }
        guard let result else { return .failed }
        if result.outcome != .failed {
            markSidecarPresent(fingerprint, cachePolicy: result.policy)
        }
        return result.outcome
    }

    // `internal` (not `private`) only so the cancellation-shield contract is directly pinnable by tests.
    static func writeSidecar(
        _ data: Data,
        fingerprintHex: String,
        thumbPath: String,
        shardDir: String,
        client: any RemoteStorageClientProtocol,
        encryptionContext: RepoEncryptionContext? = nil,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) async throws -> Bool {
        if (try? await client.exists(path: thumbPath)) == true {
            do {
                _ = try await readSidecar(
                    remotePath: thumbPath,
                    client: client,
                    encryptionContext: encryptionContext,
                    fingerprintHex: fingerprintHex
                )
                return false
            } catch RemoteThumbnailError.decodeFailed {
                try await assertOwnership?()
                try? await client.delete(path: thumbPath)
            }
        }
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("thumb_up_\(fingerprintHex)_\(UUID().uuidString).jpg")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        try data.write(to: tempURL)
        let uploadURL: URL
        if let encryptionContext {
            let encryptedURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_up_\(fingerprintHex)_\(UUID().uuidString)")
                .appendingPathExtension(RemoteFileNaming.encryptedFileExtension)
            do {
                let keyMaterial = try RepoEncryptionKeyMaterial(
                    repoID: encryptionContext.repoID,
                    keyID: encryptionContext.activeKeyID,
                    keyData: encryptionContext.contentKey
                )
                try FileEncryptionService().encrypt(
                    plaintextURL: tempURL,
                    encryptedURL: encryptedURL,
                    metadata: FileEncryptionMetadata(
                        originalFileName: "\(fingerprintHex).jpg",
                        plainSize: Int64(data.count)
                    ),
                    keyMaterial: keyMaterial,
                    externalAAD: FileEncryptionService.thumbnailExternalAAD(fingerprintHex: fingerprintHex)
                )
                uploadURL = encryptedURL
            } catch {
                try? FileManager.default.removeItem(at: encryptedURL)
                throw error
            }
        } else {
            uploadURL = tempURL
        }
        defer {
            if uploadURL != tempURL {
                try? FileManager.default.removeItem(at: uploadURL)
            }
        }
        let mkdir = Task.detached {
            try await assertOwnership?()
            try await client.createDirectory(path: shardDir)
        }
        try await mkdir.value
        do {
            // Atomic create-if-absent, not replace: the exists check above is a non-atomic fast path, and an
            // opportunistic local render may be non-authoritative (edited-after-backup) — never overwrite a
            // sidecar a concurrent authoritative writer (backup / another device / backfill) just published.
            // Detached + cancellation-blind: a cancelled transfer aborts mid-body and the torn partial at the
            // canonical path (SMB keeps it) then passes exists/collision probes as a valid sidecar. The small
            // upload runs to completion instead — shutdown/purge drains await it.
            let transfer = Task.detached {
                try await assertOwnership?()
                try await client.upload(localURL: uploadURL, remotePath: thumbPath, mode: .createIfAbsent, respectTaskCancellation: false, onProgress: nil)
            }
            try await transfer.value
            return true
        } catch {
            if SMBErrorClassifier.isNameCollision(error) { return false }   // already present → skip, not a write failure
            throw error
        }
    }

    // MARK: - Maintenance (backfill / purge)

    struct BackfillResult: Sendable {
        var generated = 0
        var skipped = 0
        // Connection/upload failures — distinct from benign skips, so the run can't report as completed
        // while most sidecars never landed.
        var failed = 0
    }

    struct BackfillRequest: Hashable, Sendable {
        let fingerprint: Data
        let storageCodec: Int
        let encryptionKeyID: String?
    }

    // Generates sidecars for assets still present locally (by fingerprint) that lack one on the node.
    // Explicit action, so it uploads regardless of the per-node flag. Reports 1-based progress.
    func backfillSidecars(
        requests: [BackfillRequest],
        progress: @MainActor @Sendable @escaping (_ done: Int, _ total: Int) -> Void,
        isCancelled: @Sendable @escaping () -> Bool
    ) async -> BackfillResult {
        await prepareLocalIndex()
        var result = BackfillResult()
        let total = requests.count
        for (index, request) in requests.enumerated() {
            if isCancelled() { break }
            await progress(index + 1, total)
            let fingerprint = request.fingerprint
            let cachePolicy = await thumbnailCachePolicy(
                storageCodec: request.storageCodec,
                encryptionKeyID: request.encryptionKeyID
            )
            guard cachePolicy.canUseSharedCaches else { result.skipped += 1; continue }
            // Current-bytes handle only — backfill must not publish an edited-after-backup render as this
            // fingerprint's shared sidecar.
            guard let localID = presenceIndex.localIdentifierForCurrentBytes(fingerprint) else { result.skipped += 1; continue }
            guard let image = await renderLocalThumbnail(localIdentifier: localID) else { result.skipped += 1; continue }
            // Don't populate L1 here — a large backfill would flood the on-device cache with thumbnails
            // the user isn't viewing. The upload (shared L2) is the point.
            switch await uploadSidecar(image, fingerprint: fingerprint, cachePolicy: cachePolicy) {
            case .written: result.generated += 1
            case .alreadyPresent: result.skipped += 1
            case .failed: result.failed += 1
            }
        }
        return result
    }

    // Deletes the node's entire thumbnail tree and clears the local cache. Reports success only when a client
    // was acquired and no list/delete failure was observed — a partial failure leaves sidecars on the node, so
    // the maintenance UI must not claim the purge completed. The local L1/known-sidecar state is cleared either
    // way (regenerable, and re-checked on the next browse).
    func purgeRemoteThumbnails() async -> Bool {
        // Serialize against browser writebacks — they run on other service instances and never claim the
        // execution lease, and browser teardown launches its drain fire-and-forget. Gate first, then AWAIT
        // the in-flight set: awaiting also covers backends whose uploads ignore a post-start cancel (S3).
        Self.globalWritebackLock.withLock { Self.isPurgeInProgress = true }
        defer { Self.globalWritebackLock.withLock { Self.isPurgeInProgress = false } }
        let writebacks = Self.globalWritebackLock.withLock { Array(Self.globalWritebackTasks.values) }
        for task in writebacks { task.cancel() }
        for task in writebacks { await task.value }

        let root = RemoteThumbnailPaths.rootAbsolutePath(basePath: profile.basePath)
        let failures: Int? = await withThumbnailWriteLease { lease -> Int in
            try await Self.recursiveDelete(
                path: root,
                client: lease.client,
                assertOwnership: lease.assertOwnership
            )
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
    static func recursiveDelete(
        path: String,
        client: any RemoteStorageClientProtocol,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) async throws -> Int {
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
                failures += try await Self.recursiveDelete(
                    path: entry.path,
                    client: client,
                    assertOwnership: assertOwnership
                )
            } else {
                try await assertOwnership?()
                do {
                    try await client.delete(path: entry.path)
                } catch {
                    if RemoteFaultLite.classify(error) == .cancelled { throw error }
                    failures += 1
                }
            }
        }
        try await assertOwnership?()
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
        guard let result = await withClientResult(body) else { return nil }
        switch result {
        case .success(let value):
            return value
        case .failure:
            return nil
        }
    }

    private func withClientResult<T>(_ body: (any RemoteStorageClientProtocol) async throws -> T) async -> Result<T, Error>? {
        // Wait on the cancellation-aware gate first, so a scrolled-away cell parked here is released
        // immediately. Cancellation past this point stops the WAIT only — the pool never aborts a connect.
        guard await connectionGate.wait() else { return nil }
        defer { connectionGate.signal() }
        guard let client = await pool.acquire() else { return nil }
        do {
            let result = try await body(client)
            await pool.release(client, reusable: true)
            return .success(result)
        } catch {
            let reusable = !profile.isConnectionUnavailableError(error)
            await pool.release(client, reusable: reusable)
            return .failure(error)
        }
    }

    private func thumbnailCachePolicyForWrite(
        storageCodec: Int,
        encryptionKeyID: String?,
        client: any RemoteStorageClientProtocol
    ) async throws -> ThumbnailCachePolicy {
        let state: RemoteThumbnailEncryptionState
        do {
            let context = try await RepoEncryptionSetupService(
                keyStore: RepoEncryptionKeychainStore(keychain: KeychainService())
            ).loadExistingContext(client: client, basePath: profile.basePath)
            state = .encrypted(context)
        } catch let error as RepoEncryptionSetupError {
            switch error {
            case .missingEncryptedRepo:
                state = .plaintext
            case .missingLocalKey, .keyVerificationFailed, .damagedVersion, .unsupportedVersion:
                state = .encryptedUnavailable
            }
        }
        return Self.thumbnailCachePolicy(
            storageCodec: storageCodec,
            encryptionKeyID: encryptionKeyID,
            state: state
        )
    }

    private struct ThumbnailWriteLease {
        let client: any RemoteStorageClientProtocol
        let assertOwnership: MonthManifestOwnershipAssertion
    }

    private func withThumbnailWriteLease<T>(
        _ body: (ThumbnailWriteLease) async throws -> T
    ) async -> T? {
        let dataClient: any RemoteStorageClientProtocol
        do {
            dataClient = try await makeConnectedThumbnailWriteClient()
        } catch {
            return nil
        }
        var lockHandle: LiteLockClientHandle?
        do {
            var initialLockHandle = try await makeThumbnailWriteLockClient()
            lockHandle = initialLockHandle
            let formatGate: LiteRepoGateway.WriteFormatGate = { [profile] probe in
                try await RepoEncryptionWriteGate.validate(
                    profile: profile,
                    probe: probe,
                    client: dataClient,
                    keyStore: RepoEncryptionKeychainStore(keychain: KeychainService())
                )
            }
            let plan = try await LiteRepoGateway.prepareMaintenance(
                client: dataClient,
                lockClient: initialLockHandle.client,
                ownsLockClient: initialLockHandle.ownsClient,
                basePath: profile.basePath,
                writerID: profile.writerID,
                reconnectLockClient: { [self] in
                    return try await self.makeThumbnailWriteLockClient()
                },
                formatGate: formatGate
            )
            guard let session = plan.session else {
                await initialLockHandle.disconnectIfOwned()
                await dataClient.disconnectSafely()
                return nil
            }
            initialLockHandle.transferToSession()
            lockHandle = initialLockHandle
            do {
                let assertOwnership: MonthManifestOwnershipAssertion = {
                    try await session.assertLeaseProvenForWrite()
                }
                try await assertOwnership()
                let value = try await body(ThumbnailWriteLease(client: dataClient, assertOwnership: assertOwnership))
                await session.stopAndRelease()
                await dataClient.disconnectSafely()
                return value
            } catch {
                await session.stopAndRelease()
                await dataClient.disconnectSafely()
                return nil
            }
        } catch {
            await lockHandle?.disconnectIfOwned()
            await dataClient.disconnectSafely()
            return nil
        }
    }

    private func makeConnectedThumbnailWriteClient() async throws -> any RemoteStorageClientProtocol {
        let client = try storageClientFactory.makeClient(profile: profile, password: password)
        try await client.connect()
        return client
    }

    private func makeThumbnailWriteLockClient() async throws -> LiteLockClientHandle {
        let client = try await makeConnectedThumbnailWriteClient()
        return LiteLockClientHandle(client: client)
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

    private static func thumbnailFromVideo(at url: URL) -> UIImage? {
        let asset = AVURLAsset(url: url)
        let generator = AVAssetImageGenerator(asset: asset)
        generator.appliesPreferredTrackTransform = true
        generator.maximumSize = CGSize(width: ThumbnailSizing.maximumLongSide, height: ThumbnailSizing.maximumLongSide)
        guard let cgImage = try? generator.copyCGImage(at: CMTime(seconds: 0.1, preferredTimescale: 600), actualTime: nil) else {
            return nil
        }
        return ThumbnailSizing.fittedImage(UIImage(cgImage: cgImage), maximumLongSide: ThumbnailSizing.maximumLongSide)
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

enum RemoteThumbnailEncryptionState: Sendable {
    case plaintext
    case encrypted(RepoEncryptionContext)
    case encryptedUnavailable

    var isCacheable: Bool {
        switch self {
        case .plaintext, .encrypted:
            return true
        case .encryptedUnavailable:
            return false
        }
    }
}

private actor RemoteThumbnailEncryptionContextCache {
    private var cached: RemoteThumbnailEncryptionState?

    func value(load: () async -> RemoteThumbnailEncryptionState?) async -> RemoteThumbnailEncryptionState? {
        if let cached { return cached }
        guard let loaded = await load() else { return nil }
        if loaded.isCacheable {
            cached = loaded
        }
        return loaded
    }
}
