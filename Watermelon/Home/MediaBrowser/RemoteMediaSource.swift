import MoreKit
import Photos
import UIKit

// Remote backup data source. Reuses RemoteThumbnailService for the whole local-first load chain
// (on-device original → original cache → download) and OriginalPhotoCache for persistence.
final class RemoteMediaSource: MediaBrowserSource {
    let mode: MediaBrowserMode = .remote

    private let service: RemoteThumbnailService
    private let coordinator: BackupCoordinator
    // Temp originals used to reconstruct remote-only Live Photos. PHLivePhoto reads them lazily, so they
    // can't be deleted immediately; we hold them until this source is released (browser close / mode switch).
    // Deduped by fingerprint so re-viewing an asset (or its grouping-TZ twin) reuses one pair instead of
    // minting fresh temps every view. Cached originals aren't tracked (the cache manages them).
    private let liveTempLock = NSLock()
    private var liveTempURLs: Set<URL> = []
    private var liveTempPairByFingerprint: [Data: (photo: URL, video: URL)] = [:]

    init(service: RemoteThumbnailService, coordinator: BackupCoordinator) {
        self.service = service
        self.coordinator = coordinator
    }

    func prepare() async {
        await service.prepareLocalIndex()
    }

    func loadSections() async -> [MediaBrowserSection] {
        await service.prepareLocalIndex()
        let coordinator = coordinator
        let service = service
        let expectedKey = service.remoteProfileKey
        let built = await withCancellableDetachedValue(priority: .userInitiated) { () -> (months: [LibraryMonthKey], assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]], deviceHandles: [Data: String]) in
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            // The shared snapshot can belong to a different profile than this service during a switch
            // (reset to B before the session activates B). Reject rather than show B's paths via A.
            if let ownerKey = state.profileKey, ownerKey != expectedKey { return ([], [:], [:]) }
            let built = RemoteBrowserAssetBuilder.build(from: state)
            // Current-bytes handles only: a stale hash row (asset edited after backup) must not bind the
            // device handle to the pre-edit fingerprint — the item would project `.both`, prefer the edited
            // local bytes for full-size/share, and offer Delete-from-Device for bytes the backup doesn't hold.
            let deviceHandles = service.localIdentifiersForCurrentBytes(built.assetsByMonth.values.joined().map(\.fingerprint))
            return (built.months, built.assetsByMonth, deviceHandles)
        }
        return built.months.map { month in
            let items = (built.assetsByMonth[month] ?? []).map { asset -> MediaBrowserItem in
                let localID = built.deviceHandles[asset.fingerprint]
                let kind: AlbumMediaKind = asset.isLivePhoto ? .livePhoto : (asset.isVideo ? .video : .photo)
                // The same fingerprint can legitimately live in two remote months (grouping-TZ re-upload),
                // so fold the unique remote path into the id — else the two share an id and defeat the
                // per-cell reuse-token guard. `fingerprint` stays the dedup key.
                let uniquePath = asset.photoRemoteRelativePath ?? asset.videoRemoteRelativePath ?? ""
                return MediaBrowserItem(
                    id: asset.fingerprintHex + "#" + uniquePath,
                    kind: kind,
                    creationDateMs: asset.creationDateMs,
                    presence: .of(onDevice: localID != nil, onRemote: true),
                    localIdentifier: localID,
                    fingerprint: asset.fingerprint,
                    photoRemoteRelativePath: asset.photoRemoteRelativePath,
                    videoRemoteRelativePath: asset.videoRemoteRelativePath,
                    photoContentHash: asset.photoContentHash,
                    videoContentHash: asset.videoContentHash,
                    remoteMonth: asset.month,
                    isIncomplete: asset.isIncomplete
                )
            }
            return MediaBrowserSection(month: month, items: items)
        }
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let fp = item.fingerprint else { return nil }
        return await service.resolveAutoThumbnail(for: fp, expectedPhotoContentHash: item.photoContentHash)
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        // The viewer fetches a sharp still right after the Live Photo view; reuse the photo original already
        // downloaded for that fingerprint's reconstruction instead of materializing it a second time.
        if let fp = item.fingerprint, let pair = cachedLivePair(for: fp) {
            return RemoteThumbnailService.downsampledImage(at: pair.photo, maxPixel: MediaDisplay.maxPixel)
        }
        guard let material = await photoOriginal(item) else { return nil }
        let image = RemoteThumbnailService.downsampledImage(at: material.url, maxPixel: MediaDisplay.maxPixel)
        // Warm the grid thumbnail from the just-downloaded original so a remote-only, sidecar-less photo isn't
        // re-fetched for its tile. Local-present items are already warmed by the local render path. Skipped for
        // bytes that failed the manifest-hash check — they must not seed the shared L1/L2.
        if item.localIdentifier == nil, let fp = item.fingerprint, material.contentMatchesManifest {
            await service.cacheThumbnail(fromOriginalAt: material.url, fingerprint: fp)
        }
        if material.isTemporary { try? FileManager.default.removeItem(at: material.url) }
        return image
    }

    // Use-time freshness gate for the local-first branches: a handle validated at load goes stale when
    // Photos edits the asset while the browser/viewer stays open (bound handles are never revalidated
    // in-session), and the edited bytes must not materialize as this fingerprint. Re-prove the item's own
    // handle against its live row; when it fails, a current twin row may still serve the bytes locally.
    // Off-main only (single-row SQL + PHAsset fetch) — callers are the nonisolated async materializers.
    func currentLocalHandle(for item: MediaBrowserItem) -> String? {
        guard let localID = item.localIdentifier else { return nil }
        guard let fingerprint = item.fingerprint else { return localID }
        if service.currentFingerprints(forAssetIDs: [localID])[localID] == fingerprint { return localID }
        return service.localIdentifiersForCurrentBytes([fingerprint])[fingerprint]
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        if let localID = currentLocalHandle(for: item),
           let local = await service.materializeLocalOriginal(localIdentifier: localID, isVideo: true) {
            return MaterializedVideo(url: local.url, isTemporary: local.isTemporary)
        }
        guard let path = item.videoRemoteRelativePath, let fp = item.fingerprint else { return nil }
        let cap = OriginalPhotoCacheSizeLimit.getValue().maxBytes
        guard let mat = await service.materializeOriginal(
            remoteRelativePath: path,
            cacheKey: cap != nil ? OriginalPhotoCache.videoKey(fingerprintHex: fp.hexString) : nil,
            cacheCapBytes: cap,
            maxEntryBytes: OriginalPhotoCache.videoCacheMaxEntryBytes,
            expectedContentHash: item.videoContentHash,
            verifyForSharedCaches: true
        ) else { return nil }
        // AVPlayer resolves the container from the path extension; a cached original is extensionless
        // (OriginalPhotoCache keys by fingerprint hex), so normalize before it reaches the inline player.
        let f = ImportReadyFile.make(url: mat.url, type: .video, isTemporary: mat.isTemporary, extensionFrom: path)
        if item.localIdentifier == nil, mat.contentMatchesManifest {
            await service.cacheThumbnail(fromVideoOriginalAt: f.url, fingerprint: fp)
        }
        return MaterializedVideo(url: f.url, isTemporary: f.isTemporary)
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        if let localID = currentLocalHandle(for: item),
           let live = await Self.requestLocalLivePhoto(localIdentifier: localID, targetSize: targetSize) {
            return live
        }
        // Remote-only: reconstruct from the downloaded photo + video originals. PHLivePhoto reads them
        // lazily, so temps can't be deleted now; track them for deletion when this source is released.
        // Reuse a pair already reconstructed for this fingerprint (re-view / grouping-TZ twin) instead of
        // re-materializing — else each view mints fresh temps and growth is unbounded in a long session.
        if let fp = item.fingerprint, let pair = cachedLivePair(for: fp) {
            return await Self.buildLivePhoto(photoURL: pair.photo, videoURL: pair.video, targetSize: targetSize)
        }
        guard let photo = await photoOriginal(item) else { return nil }
        // Warm the grid thumbnail from the just-downloaded photo side too (same root cause as photoImage): a
        // remote-only Live Photo without a sidecar otherwise re-fetches its original for the tile on every view.
        if item.localIdentifier == nil, let fp = item.fingerprint, photo.contentMatchesManifest {
            await service.cacheThumbnail(fromOriginalAt: photo.url, fingerprint: fp)
        }
        guard let video = await video(for: item) else {
            // Photo downloaded but video failed: the photo temp was never handed to PHLivePhoto — drop it.
            if photo.isTemporary { try? FileManager.default.removeItem(at: photo.url) }
            return nil
        }
        // PHLivePhoto.request pairs the files by extension; cached originals have none, so normalize first.
        let photoF = ImportReadyFile.make(url: photo.url, type: .photo, isTemporary: photo.isTemporary, extensionFrom: item.photoRemoteRelativePath)
        let videoF = ImportReadyFile.make(url: video.url, type: .pairedVideo, isTemporary: video.isTemporary, extensionFrom: item.videoRemoteRelativePath)
        let pair = trackLivePair(fingerprint: item.fingerprint, photo: photoF, video: videoF)
        return await Self.buildLivePhoto(photoURL: pair.photo, videoURL: pair.video, targetSize: targetSize)
    }

    // Presence-driven (like MergedMediaSource), not localIdentifier-blind: an item the viewer recomputed to
    // `.localOnly` (its remote copy was deleted elsewhere while open) drops Download / Delete-from-backup and
    // offers Upload instead — so the user can re-back-up without leaving the Remote tab (matches Merged's
    // `.localOnly`). `.remoteOnly`/`.both` items still came from the manifest, so Delete-from-backup stays
    // available (it must work even for an incomplete remote asset, to clean it up).
    func actions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] {
        switch item.presence {
        case .remoteOnly: return [.share, .download, .deleteRemote]
        case .both: return [.share, .deleteLocal, .deleteRemote]
        case .localOnly: return [.share, .upload, .deleteLocal]
        }
    }

    // Override the default share so a remote video is handed over with a valid extension (the default returns
    // the raw materialized URL, which for a cached original is extensionless). Photos share as a UIImage.
    func shareItems(for item: MediaBrowserItem) async -> [Any] {
        if item.isVideo, let video = await video(for: item) {
            let f = ImportReadyFile.make(url: video.url, type: .video, isTemporary: video.isTemporary, extensionFrom: item.videoRemoteRelativePath)
            return [f.url]
        }
        if let image = await photoImage(for: item) { return [image] }
        return []
    }

    func shutdown() async {
        await service.shutdown()
    }

    // Delete Live-reconstruction temps only when the source itself is released — by then the viewer (and
    // its on-screen PHLivePhoto) is gone, so it's safe. Doing this at shutdown() could delete files a
    // still-presented viewer is reading.
    deinit {
        let urls = liveTempLock.withLock { liveTempURLs }
        for url in urls { try? FileManager.default.removeItem(at: url) }
    }

    private func cachedLivePair(for fingerprint: Data) -> (photo: URL, video: URL)? {
        liveTempLock.withLock { liveTempPairByFingerprint[fingerprint] }
    }

    // Record the reconstructed pair for source-lifetime cleanup and fingerprint-keyed reuse. Only temporary
    // URLs join the delete set; the returned pair is what gets handed to PHLivePhoto.
    private func trackLivePair(fingerprint: Data?, photo: ImportReadyFile, video: ImportReadyFile) -> (photo: URL, video: URL) {
        liveTempLock.withLock {
            if photo.isTemporary { liveTempURLs.insert(photo.url) }
            if video.isTemporary { liveTempURLs.insert(video.url) }
            if let fingerprint { liveTempPairByFingerprint[fingerprint] = (photo.url, video.url) }
        }
        return (photo.url, video.url)
    }

    // MARK: - Helpers

    private func photoOriginal(_ item: MediaBrowserItem) async -> RemoteThumbnailService.MaterializedOriginal? {
        if let localID = currentLocalHandle(for: item),
           let local = await service.materializeLocalOriginal(localIdentifier: localID, isVideo: false) {
            return local
        }
        guard let path = item.photoRemoteRelativePath, let fp = item.fingerprint else { return nil }
        let cap = OriginalPhotoCacheSizeLimit.getValue().maxBytes
        // verifyForSharedCaches: the callers derive the L1 (and opportunistic L2) from photo bytes even when
        // the originals cache is off, so a hash mismatch must be detected regardless of the store decision.
        return await service.materializeOriginal(
            remoteRelativePath: path,
            cacheKey: cap != nil ? OriginalPhotoCache.photoKey(fingerprintHex: fp.hexString) : nil,
            cacheCapBytes: cap,
            maxEntryBytes: nil,
            expectedContentHash: item.photoContentHash,
            verifyForSharedCaches: true
        )
    }

    private static func requestLocalLivePhoto(localIdentifier: String, targetSize: CGSize) async -> PHLivePhoto? {
        let result = PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil)
        guard result.count > 0 else { return nil }
        let asset = result.object(at: 0)
        let options = PHLivePhotoRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.isNetworkAccessAllowed = false
        return await withCheckedContinuation { continuation in
            let once = ResumeOnce()
            PHImageManager.default().requestLivePhoto(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFit,
                options: options
            ) { live, info in
                let degraded = (info?[PHImageResultIsDegradedKey] as? Bool) ?? false
                if !degraded, once.tryResume() { continuation.resume(returning: live) }
            }
        }
    }

    private static func buildLivePhoto(photoURL: URL, videoURL: URL, targetSize: CGSize) async -> PHLivePhoto? {
        await withCheckedContinuation { continuation in
            let once = ResumeOnce()
            PHLivePhoto.request(
                withResourceFileURLs: [photoURL, videoURL],
                placeholderImage: nil,
                targetSize: targetSize,
                contentMode: .aspectFit
            ) { live, info in
                let degraded = (info[PHLivePhotoInfoIsDegradedKey] as? Bool) ?? false
                if !degraded, once.tryResume() { continuation.resume(returning: live) }
            }
        }
    }
}
