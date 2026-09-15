import AppIntents
import Foundation
import MoreKit
import Photos

@available(iOS 27.0, *)
struct LocalAlbumEntity: AppEntity {
    let id: String
    let title: String

    init(_ reference: LocalAlbumReference) {
        id = reference.id
        title = reference.name
    }

    var reference: LocalAlbumReference { LocalAlbumReference(id: id, name: title) }

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("dataSource.album", defaultValue: "Album"))
    }

    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(title: "\(title)", image: .init(systemName: "photo.on.rectangle"))
    }

    static var defaultQuery = LocalAlbumQuery()
}

@available(iOS 27.0, *)
struct LocalAlbumQuery: EntityStringQuery {
    private let names: LocalAlbumNameCache
    private let authorizationStatus: @Sendable () -> PHAuthorizationStatus
    private let fetchAlbums: @Sendable (Set<String>?) -> [LocalAlbumReference]

    init() {
        let service = PhotoLibraryService()
        self.init(
            names: .shared,
            authorizationStatus: { service.authorizationStatus() },
            fetchAlbums: { service.fetchUserAlbumReferences(in: $0) }
        )
    }

    init(
        names: LocalAlbumNameCache,
        authorizationStatus: @escaping @Sendable () -> PHAuthorizationStatus,
        fetchAlbums: @escaping @Sendable (Set<String>?) -> [LocalAlbumReference]
    ) {
        self.names = names
        self.authorizationStatus = authorizationStatus
        self.fetchAlbums = fetchAlbums
    }

    func entities(for identifiers: [String]) async throws -> [LocalAlbumEntity] {
        try Task.checkCancellation()
        guard !identifiers.isEmpty else { return [] }
        if authorizationStatus() == .authorized {
            names.remember(fetchAlbums(Set(identifiers)))
        }
        try Task.checkCancellation()
        let cached = names.allNames
        // Preserve missing IDs so execution rejects the entire selection instead of silently shrinking it.
        return identifiers.map {
            LocalAlbumEntity(LocalAlbumReference(id: $0, name: cached[$0] ?? String(localized: "home.localAlbums.untitled")))
        }
    }

    func suggestedEntities() async throws -> [LocalAlbumEntity] {
        try Task.checkCancellation()
        guard authorizationStatus() == .authorized else { throw LocalDataSourceError.fullPhotoAccessRequired }
        let albums = fetchAlbums(nil)
        try Task.checkCancellation()
        names.remember(albums)
        return albums.map(LocalAlbumEntity.init)
    }

    func entities(matching string: String) async throws -> [LocalAlbumEntity] {
        try await suggestedEntities().filter { $0.title.localizedCaseInsensitiveContains(string) }
    }
}

final class LocalAlbumNameCache: @unchecked Sendable {
    static let shared = LocalAlbumNameCache(defaults: DefaultDeviceMediaScopeSetting.userDefaults)
    private static let storageKey = "com.zizicici.watermelon.IntentAlbumNames"
    private let defaults: UserDefaults
    private let lock = NSLock()

    init(defaults: UserDefaults) {
        self.defaults = defaults
    }

    var allNames: [String: String] {
        lock.withLock { defaults.dictionary(forKey: Self.storageKey) as? [String: String] ?? [:] }
    }

    func remember(_ albums: [LocalAlbumReference]) {
        guard !albums.isEmpty else { return }
        lock.withLock {
            let previous = defaults.dictionary(forKey: Self.storageKey) as? [String: String] ?? [:]
            var updated = previous
            for album in albums { updated[album.id] = album.name }
            if updated != previous { defaults.set(updated, forKey: Self.storageKey) }
        }
    }
}
