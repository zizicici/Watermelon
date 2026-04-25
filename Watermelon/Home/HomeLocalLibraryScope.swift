import Foundation

/// Equality is identity-only (album IDs); descriptor metadata is treated as
/// display-only so a rename in Photos.app isn't classified as a scope change that
/// would trigger a full reload. The trade-off is that renamed titles can stay stale
/// in the header until the next picker re-pick (handled in `HomeScreenStore`).
enum HomeLocalLibraryScope: Equatable, Sendable {
    case allPhotos
    case albums([LocalAlbumDescriptor])

    static func == (lhs: HomeLocalLibraryScope, rhs: HomeLocalLibraryScope) -> Bool {
        switch (lhs, rhs) {
        case (.allPhotos, .allPhotos):
            return true
        case (.albums, .albums):
            return lhs.selectedAlbumIdentifiers == rhs.selectedAlbumIdentifiers
        case (.allPhotos, .albums), (.albums, .allPhotos):
            return false
        }
    }

    var isSpecificAlbums: Bool {
        if case .albums = self { return true }
        return false
    }

    var selectedAlbumIdentifiers: Set<String> {
        switch self {
        case .allPhotos:
            return []
        case .albums(let albums):
            return Set(albums.map(\.localIdentifier))
        }
    }

    var photoLibraryQuery: PhotoLibraryQuery {
        switch self {
        case .allPhotos:
            return .allAssets
        case .albums(let albums):
            return .albums(Set(albums.map(\.localIdentifier)))
        }
    }
}
