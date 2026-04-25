import Foundation
@preconcurrency import Photos

func libraryAssetMediaKind(for asset: PHAsset) -> AlbumMediaKind {
    if PhotoLibraryService.isLivePhoto(asset) {
        return .livePhoto
    }
    if asset.mediaType == .video {
        return .video
    }
    return .photo
}

private func snapshot(_ asset: PHAsset) -> LibraryAssetSnapshot {
    LibraryAssetSnapshot(
        localIdentifier: asset.localIdentifier,
        creationDate: asset.creationDate,
        mediaKind: libraryAssetMediaKind(for: asset)
    )
}

final class PhotoKitAssetCollection: LibraryAssetCollection, @unchecked Sendable {
    fileprivate let fetchResult: PHFetchResult<PHAsset>
    private let cachedSnapshots: [LibraryAssetSnapshot]

    init(fetchResult: PHFetchResult<PHAsset>) {
        self.fetchResult = fetchResult
        var built: [LibraryAssetSnapshot] = []
        built.reserveCapacity(fetchResult.count)
        for index in 0 ..< fetchResult.count {
            built.append(snapshot(fetchResult.object(at: index)))
        }
        self.cachedSnapshots = built
    }

    var assetSnapshots: [LibraryAssetSnapshot] { cachedSnapshots }
}

final class PhotoKitChangeProvider: LibraryChangeProvider, @unchecked Sendable {
    let change: PHChange

    init(change: PHChange) { self.change = change }

    func change(for collection: LibraryAssetCollection) -> LibraryCollectionChange? {
        guard let pkc = collection as? PhotoKitAssetCollection else { return nil }
        guard let details = change.changeDetails(for: pkc.fetchResult) else { return nil }

        let nextCollection = PhotoKitAssetCollection(fetchResult: details.fetchResultAfterChanges)

        guard details.hasIncrementalChanges else {
            return LibraryCollectionChange(
                nextCollection: nextCollection,
                hasIncrementalChanges: false,
                removedAssetIDs: [],
                insertedAssets: [],
                changedAssets: [],
                movedAssets: []
            )
        }

        var removedIDs: [String] = []
        if let indexes = details.removedIndexes {
            removedIDs.reserveCapacity(indexes.count)
            for idx in indexes {
                removedIDs.append(pkc.fetchResult.object(at: idx).localIdentifier)
            }
        }

        var inserted: [LibraryAssetSnapshot] = []
        if let indexes = details.insertedIndexes {
            inserted.reserveCapacity(indexes.count)
            for idx in indexes {
                inserted.append(snapshot(details.fetchResultAfterChanges.object(at: idx)))
            }
        }

        var changed: [LibraryAssetSnapshot] = []
        if let indexes = details.changedIndexes {
            changed.reserveCapacity(indexes.count)
            for idx in indexes {
                changed.append(snapshot(details.fetchResultAfterChanges.object(at: idx)))
            }
        }

        var moved: [LibraryAssetSnapshot] = []
        if details.hasMoves {
            details.enumerateMoves { _, toIndex in
                moved.append(snapshot(details.fetchResultAfterChanges.object(at: toIndex)))
            }
        }

        return LibraryCollectionChange(
            nextCollection: nextCollection,
            hasIncrementalChanges: true,
            removedAssetIDs: removedIDs,
            insertedAssets: inserted,
            changedAssets: changed,
            movedAssets: moved
        )
    }
}
