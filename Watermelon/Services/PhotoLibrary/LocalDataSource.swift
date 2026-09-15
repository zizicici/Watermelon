import Foundation
import MoreKit
import Photos

struct LocalAlbumReference: Codable, Hashable, Sendable {
    let id: String
    let name: String

    init(id: String, name: String) {
        self.id = id
        self.name = name
    }

    init(_ album: LocalAlbumDescriptor) {
        id = album.localIdentifier
        name = album.title
    }
}

struct LocalDataSource: Codable, Hashable, Sendable {
    enum Kind: String, Codable, CaseIterable, Sendable {
        case all, photos, videos, albums

        var mediaFilter: PhotoLibraryMediaFilter? {
            switch self {
            case .all: .all
            case .photos: .photos
            case .videos: .videos
            case .albums: nil
            }
        }
    }

    let kind: Kind
    var albums: [LocalAlbumReference] = []

    var id: String { kind.rawValue }
    var scope: HomeLocalLibraryScope {
        if let filter = kind.mediaFilter { return .device(filter) }
        return .albums(Set(albums.map(\.id)))
    }

    var title: String {
        switch kind {
        case .all: String(localized: "dataSource.all")
        case .photos: String(localized: "dataSource.photos")
        case .videos: String(localized: "dataSource.videos")
        case .albums:
            if albums.isEmpty {
                String(localized: "home.localSource.specificAlbums")
            } else if albums.count <= 2 {
                ListFormatter.localizedString(byJoining: albums.map(\.name))
            } else {
                String(format: String(localized: "dataSource.albumCount"), albums.count)
            }
        }
    }

    var subtitle: String? {
        guard kind == .albums else { return nil }
        return albums.isEmpty ? String(localized: "common.none") : ListFormatter.localizedString(byJoining: albums.map(\.name))
    }
}

final class LocalDataSourceStore: @unchecked Sendable {
    static let shared = LocalDataSourceStore(defaults: DefaultDeviceMediaScopeSetting.userDefaults)
    static let storageKey = "com.zizicici.watermelon.LocalDataSource"
    private let defaults: UserDefaults
    private let lock = NSLock()

    private struct Selection: Codable {
        var kind: LocalDataSource.Kind
        var albums: [LocalAlbumReference]
    }

    init(defaults: UserDefaults) {
        self.defaults = defaults
    }

    var defaultSource: LocalDataSource {
        lock.withLock {
            let selection = load()
            return LocalDataSource(kind: selection.kind, albums: selection.kind == .albums ? selection.albums : [])
        }
    }

    var albumReferences: [LocalAlbumReference] { lock.withLock { load().albums } }

    func source(for kind: LocalDataSource.Kind) -> LocalDataSource {
        LocalDataSource(kind: kind, albums: kind == .albums ? albumReferences : [])
    }

    func setDefault(_ source: LocalDataSource) throws {
        try lock.withLock {
            var selection = load()
            selection.kind = source.kind == .albums && source.albums.isEmpty ? .all : source.kind
            if source.kind == .albums {
                var seen = Set<String>()
                selection.albums = source.albums.filter { seen.insert($0.id).inserted }
            }
            defaults.set(try JSONEncoder().encode(selection), forKey: Self.storageKey)
        }
    }

    func replaceAlbumSelection(_ albums: [LocalAlbumReference]) throws {
        try lock.withLock {
            var selection = load()
            if albums.isEmpty, selection.kind == .albums { selection.kind = .all }
            var seen = Set<String>()
            selection.albums = albums.filter { seen.insert($0.id).inserted }
            defaults.set(try JSONEncoder().encode(selection), forKey: Self.storageKey)
        }
    }

    private func load() -> Selection {
        if let saved = defaults.object(forKey: Self.storageKey) {
            // Keep a damaged selection blocked instead of expanding it to the entire library.
            guard let data = saved as? Data else { return Selection(kind: .albums, albums: []) }
            return (try? JSONDecoder().decode(Selection.self, from: data)) ?? Selection(kind: .albums, albums: [])
        }
        let legacy = defaults.integer(forKey: DefaultDeviceMediaScopeSetting.getKey())
        let kind: LocalDataSource.Kind = legacy == 1 ? .photos : legacy == 2 ? .videos : .all
        return Selection(kind: kind, albums: [])
    }
}

enum LocalDataSourceError: LocalizedError, Equatable {
    case fullPhotoAccessRequired
    case emptyAlbums
    case unavailableAlbums([String])
    case sourceUnavailable

    var errorDescription: String? {
        switch self {
        case .fullPhotoAccessRequired: String(localized: "dataSource.error.fullAccess")
        case .emptyAlbums: String(localized: "dataSource.error.empty")
        case .unavailableAlbums(let names):
            String(format: String(localized: "dataSource.error.unavailable"), ListFormatter.localizedString(byJoining: names))
        case .sourceUnavailable: String(localized: "dataSource.error.sourceUnavailable")
        }
    }

    static func validateAlbums(
        _ identifiers: Set<String>,
        authorization: PHAuthorizationStatus,
        existing: () -> Set<String>,
        names: [String: String]
    ) throws {
        guard authorization == .authorized else { throw Self.fullPhotoAccessRequired }
        guard !identifiers.isEmpty else { throw Self.emptyAlbums }
        let missing = identifiers.subtracting(existing())
        guard missing.isEmpty else {
            throw Self.unavailableAlbums(missing.sorted().map { names[$0] ?? String(localized: "home.localAlbums.untitled") })
        }
    }
}

extension PhotoLibraryService {
    func validateAlbumSelection(_ ids: Set<String>, names: [String: String] = [:]) throws {
        var cachedNames = Dictionary(LocalDataSourceStore.shared.albumReferences.map { ($0.id, $0.name) }, uniquingKeysWith: { _, new in new })
        cachedNames.merge(names, uniquingKeysWith: { _, new in new })
        try LocalDataSourceError.validateAlbums(
            ids,
            authorization: authorizationStatus(),
            existing: { existingUserAlbumIdentifiers(in: ids) },
            names: cachedNames
        )
    }
}
