import Foundation

enum AlbumMediaKind: Sendable {
    case photo
    case video
    case livePhoto
}

/// Test-friendly value snapshot of a `PHAsset`. Engines work against this so
/// production paths and test paths share one code path.
struct LibraryAssetSnapshot: Hashable, Sendable {
    let localIdentifier: String
    let creationDate: Date?
    let modificationDate: Date?
    let mediaKind: AlbumMediaKind
}

/// Identity-stable handle for a "fetched group" the index tracks. Production wraps
/// `PHFetchResult<PHAsset>`; tests can provide an in-memory list. `Sendable` is
/// required because the engine captures collections constructed on MainActor and
/// uses them inside `processingQueue.async`.
protocol LibraryAssetCollection: AnyObject, Sendable {
    var assetSnapshots: [LibraryAssetSnapshot] { get }
}

/// One collection's worth of changes, pre-resolved off PhotoKit's index-based API
/// so the engine never reaches back into a `PHFetchResult`. `nextCollection` is a
/// fresh handle the engine uses to identify the same logical group on later changes.
struct LibraryCollectionChange {
    let nextCollection: LibraryAssetCollection
    let hasIncrementalChanges: Bool
    let removedAssetIDs: [String]
    let insertedAssets: [LibraryAssetSnapshot]
    let changedAssets: [LibraryAssetSnapshot]
    let movedAssets: [LibraryAssetSnapshot]
}

protocol LibraryChangeProvider {
    /// Returns the change for `collection` if the underlying source recorded one,
    /// `nil` otherwise. The engine treats `nil` as "no relevant change".
    func change(for collection: LibraryAssetCollection) -> LibraryCollectionChange?
}
