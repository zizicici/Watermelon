import Foundation

enum HomeLocalLibraryScope: Hashable, Sendable {
    case device(PhotoLibraryMediaFilter)
    case albums(Set<String>)

    var deviceMediaFilter: PhotoLibraryMediaFilter? {
        guard case .device(let filter) = self else { return nil }
        return filter
    }

    var isEntireLibrary: Bool { self == .device(.all) }

    var isSpecificAlbums: Bool {
        if case .albums = self { return true }
        return false
    }

    var selectedAlbumIdentifiers: Set<String> {
        switch self {
        case .device:
            return []
        case .albums(let ids):
            return ids
        }
    }

    var photoLibraryQuery: PhotoLibraryQuery {
        switch self {
        case .device(let filter):
            return .library(filter)
        case .albums(let ids):
            return .albums(ids)
        }
    }
}
