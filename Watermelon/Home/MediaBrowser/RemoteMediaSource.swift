import MoreKit
import Photos
import UIKit

// Remote backup data source. Reuses RemoteThumbnailService for the whole local-first load chain
// (on-device original → original cache → download) and OriginalPhotoCache for persistence.
final class RemoteMediaSource: MediaBrowserSource, @unchecked Sendable {
    enum ProjectionPreparation: Sendable {
        case ready(RemoteBrowserProjection)
        case stale
        case cancelled
    }

    let mode: MediaBrowserMode = .remote

    private let service: RemoteThumbnailService
    // Temp originals used to reconstruct remote-only Live Photos. PHLivePhoto reads them lazily, so they
    // can't be deleted immediately; we hold them until this source is released (browser close / mode switch).
    // Deduped by fingerprint so re-viewing an asset (or its grouping-TZ twin) reuses one pair instead of
    // minting fresh temps every view. Cached originals aren't tracked (the cache manages them).
    private let liveTempLock = NSLock()
    private var liveTempURLs: Set<URL> = []
    private var liveTempPairByFingerprint: [Data: (photo: URL, video: URL)] = [:]

    init(service: RemoteThumbnailService) {
        self.service = service
    }

    func load() async -> MediaBrowserLoadResult {
        switch await prepareProjection() {
        case .ready(let projection):
            return await load(projection: projection)
        case .stale:
            return .stale
        case .cancelled:
            return .cancelled
        }
    }

    func prepareProjection() async -> ProjectionPreparation {
        guard let projection = await service.prepareRemoteBrowserProjection(
            notifyOnCommit: false
        ) else {
            return Task.isCancelled ? .cancelled : .stale
        }
        guard !Task.isCancelled else { return .cancelled }
        guard service.isRemoteBrowserProjectionRenderable(projection) else {
            return .stale
        }
        return .ready(projection)
    }

    private func project(projection: RemoteBrowserProjection) async -> [MediaBrowserSection] {
        let service = service
        let trace = MediaBrowserLoadTrace.context
        return await withCancellableDetachedValue(priority: .userInitiated) { () -> [MediaBrowserSection] in
            let startedAt = CFAbsoluteTimeGetCurrent()
            guard !Task.isCancelled else { return [] }
            // Current-bytes handles only: a stale hash row (asset edited after backup) must not bind the
            // device handle to the pre-edit fingerprint — the item would project `.both`, prefer the edited
            // local bytes for full-size/share, and offer Delete-from-Device for bytes the backup doesn't hold.
            let handlesStartedAt = CFAbsoluteTimeGetCurrent()
            let deviceHandles: [Data: String]
            if let preparedHandles = projection.deviceHandles {
                deviceHandles = preparedHandles
                MediaBrowserLoadTrace.emit(
                    "remoteHandlesCacheHit",
                    context: trace,
                    details: "handles=\(preparedHandles.count)"
                )
            } else {
                deviceHandles = service.localIdentifiersForCurrentBytes(
                    projection.assetsByMonth.values.joined().map(\.fingerprint),
                    trace: trace
                )
            }
            let handlesMs = (CFAbsoluteTimeGetCurrent() - handlesStartedAt) * 1_000
            guard !Task.isCancelled else { return [] }

            let itemsStartedAt = CFAbsoluteTimeGetCurrent()
            var itemsByDisplayMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            itemsByDisplayMonth.reserveCapacity(projection.months.count)
            for storageMonth in projection.months {
                guard !Task.isCancelled else { return [] }
                let assets = projection.assetsByMonth[storageMonth] ?? []
                for asset in assets {
                    guard !Task.isCancelled else { return [] }
                    let localID = deviceHandles[asset.fingerprint]
                    let kind: AlbumMediaKind = asset.isLivePhoto ? .livePhoto : (asset.isVideo ? .video : .photo)
                    itemsByDisplayMonth[asset.displayMonth, default: []].append(
                        MediaBrowserItem(
                            kind: kind,
                            creationDateMs: asset.creationDateMs,
                            localIdentifier: localID,
                            remote: RemoteMediaReference(
                                fingerprint: asset.fingerprint,
                                photoRelativePath: asset.photoRemoteRelativePath,
                                videoRelativePath: asset.videoRemoteRelativePath,
                                photoContentHash: asset.photoContentHash,
                                videoContentHash: asset.videoContentHash,
                                storageMonth: asset.month,
                                isIncomplete: asset.isIncomplete
                            )
                        )
                    )
                }
            }
            let sections = itemsByDisplayMonth.keys.sorted(by: >).map { month in
                MediaBrowserSection(
                    month: month,
                    items: Self.sortedIfNeeded(itemsByDisplayMonth[month] ?? [])
                )
            }
            let itemsMs = (CFAbsoluteTimeGetCurrent() - itemsStartedAt) * 1_000
            let assetCount = sections.reduce(0) { $0 + $1.items.count }
            MediaBrowserLoadTrace.emit(
                "remoteFinalize",
                context: trace,
                startedAt: startedAt,
                details: "sections=\(sections.count) assets=\(assetCount) handles=\(deviceHandles.count) handlesMs=\(String(format: "%.1f", handlesMs)) itemsMs=\(String(format: "%.1f", itemsMs))"
            )
            return sections
        }
    }

    private static func sortedIfNeeded(
        _ items: [MediaBrowserItem]
    ) -> [MediaBrowserItem] {
        guard items.count > 1 else { return items }
        for index in 1 ..< items.count
        where precedes(items[index], items[index - 1]) {
            return items.sorted(by: precedes)
        }
        return items
    }

    private static func precedes(
        _ lhs: MediaBrowserItem,
        _ rhs: MediaBrowserItem
    ) -> Bool {
        if lhs.creationDateMs != rhs.creationDateMs {
            return lhs.creationDateMs > rhs.creationDateMs
        }
        return lhs.id < rhs.id
    }

    func load(projection: RemoteBrowserProjection) async -> MediaBrowserLoadResult {
        let sections = await project(projection: projection)
        guard !Task.isCancelled else { return .cancelled }
        guard service.isRemoteBrowserProjectionRenderable(projection) else {
            return .stale
        }
        return .loaded(MediaBrowserContent(sections: sections))
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
        if let localID = currentLocalHandle(for: item),
           let image = await LocalMediaLoader.photoImage(
               localIdentifier: localID,
               maxPixel: MediaDisplay.maxPixel,
               allowNetworkAccess: false
           ) {
            return image
        }
        guard let material = await remotePhotoOriginal(item) else { return nil }
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
    // Photos edits the asset while the browser/viewer stays open (the projected handle itself is immutable),
    // and the edited bytes must not materialize as this fingerprint. Re-prove the item's own
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
           let local = await service.materializeLocalVideo(localIdentifier: localID) {
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
        var reconstructedLocalLivePhoto: PHLivePhoto?
        if let localID = currentLocalHandle(for: item),
           let local = await service.materializeLocalLivePhotoPair(
               localIdentifier: localID,
               validate: { pair in
                   let live = await Self.buildLivePhoto(
                       photoURL: pair.photo.url,
                       videoURL: pair.video.url,
                       targetSize: targetSize
                   )
                   reconstructedLocalLivePhoto = live
                   return live != nil
               }
           ),
           let live = reconstructedLocalLivePhoto {
            let photo = ImportReadyFile.make(
                url: local.photo.url,
                type: .photo,
                isTemporary: local.photo.isTemporary,
                extensionFrom: local.photo.url.path
            )
            let video = ImportReadyFile.make(
                url: local.video.url,
                type: .pairedVideo,
                isTemporary: local.video.isTemporary,
                extensionFrom: local.video.url.path
            )
            _ = trackLivePair(fingerprint: item.fingerprint, photo: photo, video: video)
            return live
        }
        guard let photo = await remotePhotoOriginal(item) else { return nil }
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

    private func remotePhotoOriginal(_ item: MediaBrowserItem) async -> RemoteThumbnailService.MaterializedOriginal? {
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
        return await PhotoKitImageLoader.requestLivePhoto(
            for: asset,
            targetSize: targetSize,
            contentMode: .aspectFit,
            options: options
        )
    }

    private static func buildLivePhoto(photoURL: URL, videoURL: URL, targetSize: CGSize) async -> PHLivePhoto? {
        await PhotoKitImageLoader.requestLivePhoto(
            resourceFileURLs: [photoURL, videoURL],
            placeholderImage: nil,
            targetSize: targetSize,
            contentMode: .aspectFit
        )
    }
}
