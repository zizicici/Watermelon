import Photos
import UIKit

final class TransferLocalMediaSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService
    private let query: PhotoLibraryQuery

    init(photoLibraryService: PhotoLibraryService, query: PhotoLibraryQuery = .library(.all)) {
        self.photoLibraryService = photoLibraryService
        self.query = query
    }

    func load() async -> MediaBrowserLoadResult {
        let photoLibraryService = photoLibraryService
        let query = query
        let sections = await withCancellableDetachedValue(priority: .userInitiated) { () -> [MediaBrowserSection]? in
            guard !Task.isCancelled else { return nil }
            let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())
            var monthMemo = MediaBrowserMonthMemo(calendar: calendar)
            var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            func append(_ asset: PHAsset) {
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

            switch query {
            case .library(let filter):
                let result = photoLibraryService.fetchAssetsResult(mediaFilter: filter)
                var cancelled = false
                result.enumerateObjects { asset, _, stop in
                    guard !Task.isCancelled else {
                        cancelled = true
                        stop.pointee = true
                        return
                    }
                    append(asset)
                }
                guard !cancelled else { return nil }
            case .albums(let identifiers):
                guard photoLibraryService.enumerateAssets(
                    inAlbumIdentifiers: identifiers,
                    shouldCancel: { Task.isCancelled },
                    visit: append
                ) else { return nil }
            }
            guard !Task.isCancelled else { return nil }

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
