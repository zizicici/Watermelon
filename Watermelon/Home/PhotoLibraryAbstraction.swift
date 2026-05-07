import Foundation

enum AlbumMediaKind: Sendable {
    case photo
    case video
    case livePhoto
}

struct LibraryAssetSnapshot: Hashable, Sendable {
    let localIdentifier: String
    let creationDate: Date?
    let modificationDate: Date?
    let mediaKind: AlbumMediaKind
}

struct LibraryInitialPayload: Sendable {
    let collections: [[LibraryAssetSnapshot]]
}

struct LibraryChangePayload: Sendable {
    enum CollectionChange: Sendable {
        case incremental(
            collectionIndex: Int,
            removed: [String],
            inserted: [LibraryAssetSnapshot],
            changed: [LibraryAssetSnapshot],
            moved: [LibraryAssetSnapshot]
        )
        case nonIncremental(
            collectionIndex: Int,
            nextSnapshots: [LibraryAssetSnapshot]
        )

        var collectionIndex: Int {
            switch self {
            case .incremental(let i, _, _, _, _): return i
            case .nonIncremental(let i, _): return i
            }
        }
    }

    let collectionChanges: [CollectionChange]
}
