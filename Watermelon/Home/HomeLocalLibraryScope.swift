import Foundation

enum HomeLocalLibraryScope: Hashable, Sendable {
    case allPhotos
    case albums(Set<String>)

    var isSpecificAlbums: Bool {
        if case .albums = self { return true }
        return false
    }

    var selectedAlbumIdentifiers: Set<String> {
        switch self {
        case .allPhotos:
            return []
        case .albums(let ids):
            return ids
        }
    }

    var photoLibraryQuery: PhotoLibraryQuery {
        switch self {
        case .allPhotos:
            return .allAssets
        case .albums(let ids):
            return .albums(ids)
        }
    }
}
