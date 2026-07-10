import Photos
import UIKit

// Browses an on-device library scope (the whole library, or one/more albums). Presence is `.both` when an
// asset's fingerprint is present in the (cached) remote snapshot, else `.localOnly`. Works offline.
final class LocalMediaSource: MediaBrowserSource {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    // Single source of truth for local/remote/both — owns the profile-gated remote fingerprint set.
    private let presenceIndex: LibraryPresenceIndex
    // Which on-device assets this source browses.
    private let query: PhotoLibraryQuery

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository, presenceIndex: LibraryPresenceIndex, query: PhotoLibraryQuery = .allAssets) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.presenceIndex = presenceIndex
        self.query = query
    }

    func prepare() async { await presenceIndex.refresh() }

    func loadSections() async -> [MediaBrowserSection] {
        await presenceIndex.refresh()
        let photoLibraryService = photoLibraryService
        let hashIndexRepository = hashIndexRepository
        let presenceIndex = presenceIndex
        let query = query
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let fingerprintByLocalID = (try? hashIndexRepository.fetchAssetFingerprintRecords()) ?? [:]
            let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())

            var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            func append(_ asset: PHAsset) {
                let localID = asset.localIdentifier
                let kind: AlbumMediaKind = PhotoLibraryService.isLivePhoto(asset)
                    ? .livePhoto
                    : (asset.mediaType == .video ? .video : .photo)
                // Home's staleness rule: a row older than the asset's edit no longer fingerprints the current
                // bytes — treat the asset as unfingerprinted so it reads `.localOnly`, offers Upload, and its
                // render never lands in the shared L1 under the pre-edit fingerprint.
                let record = fingerprintByLocalID[localID]
                let fingerprint: Data? = record.flatMap { r in
                    LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: r.updatedAt, assetModificationDate: asset.modificationDate) ? r.fingerprint : nil
                }
                // "Backed up" = the remote record has real media (a partial-but-has-media record counts). A local
                // twin of a config-only / phantom record isn't backed up, so it reads `.localOnly` and offers Upload.
                let onRemote = fingerprint.map { presenceIndex.isBackedUp($0) } ?? false
                let created = LibraryCreationDate.normalized(asset.creationDate)
                let month = LibraryMonthKey.from(date: created.date, calendar: calendar)
                let item = MediaBrowserItem(
                    id: localID,
                    kind: kind,
                    creationDateMs: created.milliseconds,
                    presence: .of(onDevice: true, onRemote: onRemote),
                    localIdentifier: localID,
                    fingerprint: fingerprint,
                    photoRemoteRelativePath: nil,
                    videoRemoteRelativePath: nil,
                    remoteMonth: nil
                )
                byMonth[month, default: []].append(item)
            }

            // Resolver yields newest-first by creation date, so within-month order is already correct.
            for asset in photoLibraryService.fetchAssets(for: query, shouldCancel: { Task.isCancelled }) {
                append(asset)
            }
            return byMonth.keys.sorted(by: >).map { MediaBrowserSection(month: $0, items: byMonth[$0] ?? []) }
        }
    }

    func actions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] {
        var actions: [MediaBrowserActionKind] = [.share]
        if item.presence == .localOnly { actions.append(.upload) }  // on device but not yet backed up
        actions.append(.deleteLocal)
        return actions
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        guard let fingerprint = item.fingerprint else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        MediaThumbnailCache.configureIfNeeded()
        if let cached = await MediaThumbnailCache.cached(for: fingerprint) { return cached }
        // A merged tile's handle may come from the presence map (remote record grafted onto a device asset):
        // re-validate row freshness so an edited-after-backup render never persists under the shared
        // fingerprint key. Stale assets render through PhotoKit without entering the content-addressed cache.
        guard presenceIndex.localIdentifierForCurrentBytes(fingerprint) == id else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        guard let image = await PhotoKitImageLoader.thumbnail(localIdentifier: id) else { return nil }
        guard presenceIndex.localIdentifierForCurrentBytes(fingerprint) == id else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        MediaThumbnailCache.store(image, for: fingerprint)
        return image
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.photoImage(localIdentifier: id, maxPixel: MediaDisplay.maxPixel)
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.video(localIdentifier: id)
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.livePhoto(localIdentifier: id, targetSize: targetSize)
    }
}
