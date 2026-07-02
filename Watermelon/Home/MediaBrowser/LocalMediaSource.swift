import Photos
import UIKit

// Browses the whole on-device photo library. Presence is `.both` when an asset's fingerprint is present
// in the (cached) remote snapshot, else `.localOnly`. Works offline.
final class LocalMediaSource: MediaBrowserSource {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    private let coordinator: BackupCoordinator

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository, coordinator: BackupCoordinator) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
    }

    func prepare() async {}

    func loadSections() async -> [MediaBrowserSection] {
        let photoLibraryService = photoLibraryService
        let hashIndexRepository = hashIndexRepository
        let coordinator = coordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let fetch = photoLibraryService.fetchAssetsResult()
            let fingerprintByLocalID = (try? hashIndexRepository.fetchAssetFingerprintRecords()) ?? [:]
            let remoteFingerprints = Set(
                coordinator.currentRemoteSnapshotState(since: nil).monthDeltas.flatMap { $0.assets.map(\.assetFingerprint) }
            )
            let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())

            var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            for index in 0 ..< fetch.count {
                let asset = fetch.object(at: index)
                let localID = asset.localIdentifier
                let kind: AlbumMediaKind = PhotoLibraryService.isLivePhoto(asset)
                    ? .livePhoto
                    : (asset.mediaType == .video ? .video : .photo)
                let fingerprint = fingerprintByLocalID[localID]?.fingerprint
                let onRemote = fingerprint.map { remoteFingerprints.contains($0) } ?? false
                let created = asset.creationDate ?? Date(timeIntervalSince1970: 0)
                let month = LibraryMonthKey.from(date: created, calendar: calendar)
                let item = MediaBrowserItem(
                    id: localID,
                    kind: kind,
                    creationDateMs: Int64(created.timeIntervalSince1970 * 1000),
                    presence: onRemote ? .both : .localOnly,
                    localIdentifier: localID,
                    fingerprint: fingerprint,
                    photoRemoteRelativePath: nil,
                    videoRemoteRelativePath: nil
                )
                byMonth[month, default: []].append(item)
            }
            // Fetch was descending by creation date, so within-month order is already newest-first.
            return byMonth.keys.sorted(by: >).map { MediaBrowserSection(month: $0, items: byMonth[$0] ?? []) }
        }
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.thumbnail(localIdentifier: id)
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
