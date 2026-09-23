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

    private init(id: String, title: String) {
        self.id = id
        self.title = title
    }

    static var nodeDefault: Self {
        Self(id: "nodeDefault", title: String(localized: "dataSource.useNodeDefault"))
    }

    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: LocalizedStringResource("dataSource.title", defaultValue: "Data Source"))
    }

    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(title: "\(title)")
    }

    static var defaultQuery = LocalDataSourceQuery()

    func resolve(albums: [LocalAlbumEntity]?) throws -> LocalDataSource? {
        if id == Self.nodeDefault.id { return nil }
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
            if id == LocalDataSourceEntity.nodeDefault.id { return .nodeDefault }
            return LocalDataSource.Kind(rawValue: id).map { LocalDataSourceEntity(kind: $0) }
        }
    }

    func suggestedEntities() async throws -> [LocalDataSourceEntity] {
        [.nodeDefault] + LocalDataSource.Kind.allCases.map { LocalDataSourceEntity(kind: $0) }
    }

    func defaultResult() async -> LocalDataSourceEntity? {
        .nodeDefault
    }
}
