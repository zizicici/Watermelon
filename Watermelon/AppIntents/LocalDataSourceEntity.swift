import AppIntents
import Foundation

@available(iOS 27.0, *)
struct LocalDataSourceEntity: AppEntity {
    let id: String
    let title: String

    init(kind: LocalDataSource.Kind) {
        id = kind.rawValue
        title = LocalDataSource(kind: kind).title
    }

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("dataSource.title", defaultValue: "Data Source"))
    }

    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(title: "\(title)")
    }

    static var defaultQuery = LocalDataSourceQuery()

    func resolve(albums: [LocalAlbumEntity]?) throws -> LocalDataSource {
        guard let kind = LocalDataSource.Kind(rawValue: id) else { throw LocalDataSourceError.sourceUnavailable }
        guard kind == .albums else { return LocalDataSource(kind: kind) }
        guard let albums, !albums.isEmpty else { throw LocalDataSourceError.emptyAlbums }
        var seen = Set<String>()
        let references = albums.filter { seen.insert($0.id).inserted }.map(\.reference)
        return LocalDataSource(kind: .albums, albums: references)
    }
}

@available(iOS 27.0, *)
struct LocalDataSourceQuery: EntityQuery {
    func entities(for identifiers: [String]) async throws -> [LocalDataSourceEntity] {
        identifiers.compactMap { id in
            LocalDataSource.Kind(rawValue: id).map { LocalDataSourceEntity(kind: $0) }
        }
    }

    func suggestedEntities() async throws -> [LocalDataSourceEntity] {
        LocalDataSource.Kind.allCases.map { LocalDataSourceEntity(kind: $0) }
    }

    func defaultResult() async -> LocalDataSourceEntity? {
        LocalDataSourceEntity(kind: LocalDataSourceStore.shared.defaultSource.kind)
    }
}
