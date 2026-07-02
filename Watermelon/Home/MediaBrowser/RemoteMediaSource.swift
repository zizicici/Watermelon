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
    // can't be deleted immediately; we hold them until shutdown (browser close / mode switch) to bound
    // temp growth. Cached originals aren't tracked (the cache manages them).
    private let liveTempLock = NSLock()
    private var liveTempURLs: Set<URL> = []

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
        let expectedKey = service.remoteProfileKey
        let built = await withCancellableDetachedValue(priority: .userInitiated) { () -> (months: [LibraryMonthKey], assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]]) in
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            // The shared snapshot can belong to a different profile than this service during a switch
            // (reset to B before the session activates B). Reject rather than show B's paths via A.
            if let ownerKey = state.profileKey, ownerKey != expectedKey { return ([], [:]) }
            return RemoteBrowserAssetBuilder.build(from: state)
        }
        let service = service
        return built.months.map { month in
            let items = (built.assetsByMonth[month] ?? []).map { asset -> MediaBrowserItem in
                let localID = service.localIdentifier(for: asset.fingerprint)
                let kind: AlbumMediaKind = asset.isLivePhoto ? .livePhoto : (asset.isVideo ? .video : .photo)
                // The same fingerprint can legitimately live in two remote months (grouping-TZ re-upload),
                // so fold the unique remote path into the id — else the two share an id and defeat the
                // per-cell reuse-token guard. `fingerprint` stays the dedup key.
                let uniquePath = asset.photoRemoteRelativePath ?? asset.videoRemoteRelativePath ?? ""
                return MediaBrowserItem(
                    id: asset.fingerprintHex + "#" + uniquePath,
                    kind: kind,
                    creationDateMs: asset.creationDateMs,
                    presence: localID != nil ? .both : .remoteOnly,
                    localIdentifier: localID,
                    fingerprint: asset.fingerprint,
                    photoRemoteRelativePath: asset.photoRemoteRelativePath,
                    videoRemoteRelativePath: asset.videoRemoteRelativePath
                )
            }
            return MediaBrowserSection(month: month, items: items)
        }
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let fp = item.fingerprint else { return nil }
        return await service.resolveAutoThumbnail(for: fp)
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        guard let material = await photoOriginal(item) else { return nil }
        let image = RemoteThumbnailService.downsampledImage(at: material.url, maxPixel: MediaDisplay.maxPixel)
        if material.isTemporary { try? FileManager.default.removeItem(at: material.url) }
        return image
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        if let localID = item.localIdentifier,
           let local = await service.materializeLocalOriginal(localIdentifier: localID, isVideo: true) {
            return MaterializedVideo(url: local.url, isTemporary: local.isTemporary)
        }
        guard let path = item.videoRemoteRelativePath, let fp = item.fingerprint else { return nil }
        let cap = OriginalPhotoCacheSizeLimit.getValue().maxBytes
        guard let mat = await service.materializeOriginal(
            remoteRelativePath: path,
            cacheKey: cap != nil ? OriginalPhotoCache.videoKey(fingerprintHex: fp.hexString) : nil,
            cacheCapBytes: cap,
            maxEntryBytes: OriginalPhotoCache.videoCacheMaxEntryBytes
        ) else { return nil }
        return MaterializedVideo(url: mat.url, isTemporary: mat.isTemporary)
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        if let localID = item.localIdentifier,
           let live = await Self.requestLocalLivePhoto(localIdentifier: localID, targetSize: targetSize) {
            return live
        }
        // Remote-only: reconstruct from the downloaded photo + video originals. PHLivePhoto reads them
        // lazily, so temps can't be deleted now; track them for deletion when this source is released.
        guard let photo = await photoOriginal(item) else { return nil }
        guard let video = await video(for: item) else {
            // Photo downloaded but video failed: the photo temp was never handed to PHLivePhoto — drop it.
            if photo.isTemporary { try? FileManager.default.removeItem(at: photo.url) }
            return nil
        }
        trackLiveTemp(photo.isTemporary ? photo.url : nil, video.isTemporary ? video.url : nil)
        return await Self.buildLivePhoto(photoURL: photo.url, videoURL: video.url, targetSize: targetSize)
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

    private func trackLiveTemp(_ photo: URL?, _ video: URL?) {
        liveTempLock.withLock {
            if let photo { liveTempURLs.insert(photo) }
            if let video { liveTempURLs.insert(video) }
        }
    }

    // MARK: - Helpers

    private func photoOriginal(_ item: MediaBrowserItem) async -> RemoteThumbnailService.MaterializedOriginal? {
        if let localID = item.localIdentifier,
           let local = await service.materializeLocalOriginal(localIdentifier: localID, isVideo: false) {
            return local
        }
        guard let path = item.photoRemoteRelativePath, let fp = item.fingerprint else { return nil }
        let cap = OriginalPhotoCacheSizeLimit.getValue().maxBytes
        return await service.materializeOriginal(
            remoteRelativePath: path,
            cacheKey: cap != nil ? OriginalPhotoCache.photoKey(fingerprintHex: fp.hexString) : nil,
            cacheCapBytes: cap,
            maxEntryBytes: nil
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
