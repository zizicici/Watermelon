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

    func reconciled(
        existingAlbumIdentifiers: Set<String>
    ) -> HomeLocalLibraryScope {
        switch self {
        case .allPhotos:
            return .allPhotos
        case .albums(let identifiers):
            let existing = identifiers.intersection(
                existingAlbumIdentifiers
            )
            return existing.isEmpty
                ? .allPhotos
                : .albums(existing)
        }
    }
}
