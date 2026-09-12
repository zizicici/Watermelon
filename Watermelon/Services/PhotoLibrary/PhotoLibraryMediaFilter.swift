import Photos

enum PhotoLibraryMediaFilter: String, CaseIterable, Hashable, Sendable {
    case all
    case photos
    case videos

    var localizedTitle: String {
        switch self {
        case .all: return String(localized: "home.localSource.allPhotos")
        case .photos: return String(localized: "home.localSource.photos")
        case .videos: return String(localized: "home.localSource.videos")
        }
    }

    var predicate: NSPredicate? {
        switch self {
        case .videos:
            return NSPredicate(format: "mediaType == %d", PHAssetMediaType.video.rawValue)
        case .photos:
            return NSPredicate(format: "mediaType == %d", PHAssetMediaType.image.rawValue)
        case .all:
            return nil
        }
    }

    func includes(_ kind: AlbumMediaKind) -> Bool {
        switch self {
        case .videos: return kind == .video
        case .photos: return kind != .video
        case .all: return true
        }
    }
}
