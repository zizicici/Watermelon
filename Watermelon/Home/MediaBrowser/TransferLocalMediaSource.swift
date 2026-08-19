import Photos
import UIKit

final class TransferLocalMediaSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService

    init(photoLibraryService: PhotoLibraryService) {
        self.photoLibraryService = photoLibraryService
    }

    func load() async -> MediaBrowserLoadResult {
        let photoLibraryService = photoLibraryService
        let sections = await withCancellableDetachedValue(priority: .userInitiated) { () -> [MediaBrowserSection]? in
            let result = photoLibraryService.fetchAssetsResult()
            let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())
            var monthMemo = MediaBrowserMonthMemo(calendar: calendar)
            var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            var cancelled = false

            result.enumerateObjects { asset, _, stop in
                guard !Task.isCancelled else {
                    cancelled = true
                    stop.pointee = true
                    return
                }
                let created = LibraryCreationDate.normalized(asset.creationDate)
                let month = monthMemo.month(for: created.date)
                let kind: AlbumMediaKind = PhotoLibraryService.isLivePhoto(asset)
                    ? .livePhoto
                    : (asset.mediaType == .video ? .video : .photo)
                byMonth[month, default: []].append(MediaBrowserItem(
                    kind: kind,
                    creationDateMs: created.milliseconds,
                    localIdentifier: asset.localIdentifier,
                    fingerprint: nil,
                    isBackedUp: false
                ))
            }
            guard !cancelled else { return nil }

            return byMonth.keys.sorted(by: >).map { month in
                MediaBrowserSection(
                    month: month,
                    items: (byMonth[month] ?? []).sorted {
                        if $0.creationDateMs != $1.creationDateMs {
                            return $0.creationDateMs > $1.creationDateMs
                        }
                        return $0.id < $1.id
                    }
                )
            }
        }
        guard let sections else { return .cancelled }
        guard !Task.isCancelled else { return .cancelled }
        return .loaded(MediaBrowserContent(sections: sections))
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.thumbnail(localIdentifier: id)
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.photoImage(
            localIdentifier: id,
            maxPixel: MediaDisplay.maxPixel,
            allowNetworkAccess: true
        )
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.livePhoto(localIdentifier: id, targetSize: targetSize)
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.video(localIdentifier: id)
    }

    func metadata(for item: MediaBrowserItem) async -> MediaMetadataDocument? {
        guard let id = item.localIdentifier else { return nil }
        return await MediaMetadataLoader.localDocument(localIdentifier: id, item: item)
    }
}
