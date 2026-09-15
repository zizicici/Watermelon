import Photos
import UIKit

final class TransferLocalMediaSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService
    private let query: PhotoLibraryQuery
    private let albumNames: [String: String]
    private let onDataSourceError: (@MainActor @Sendable (LocalDataSourceError) -> Void)?

    init(
        photoLibraryService: PhotoLibraryService,
        query: PhotoLibraryQuery = .library(.all),
        albumNames: [String: String] = [:],
        onDataSourceError: (@MainActor @Sendable (LocalDataSourceError) -> Void)? = nil
    ) {
        self.photoLibraryService = photoLibraryService
        self.query = query
        self.albumNames = albumNames
        self.onDataSourceError = onDataSourceError
    }

    func load() async -> MediaBrowserLoadResult {
        if let failure = await validateSelection() { return failure }
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
        if let failure = await validateSelection() { return failure }
        return .loaded(MediaBrowserContent(sections: sections))
    }

    private func validateSelection() async -> MediaBrowserLoadResult? {
        guard case .albums(let ids) = query else { return nil }
        do {
            try photoLibraryService.validateAlbumSelection(ids, names: albumNames)
            return nil
        } catch let error as LocalDataSourceError {
            guard !Task.isCancelled else { return .cancelled }
            await onDataSourceError?(error)
            return .loaded(MediaBrowserContent(sections: []))
        } catch {
            return .cancelled
        }
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
