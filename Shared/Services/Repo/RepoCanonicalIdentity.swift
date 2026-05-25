import Foundation

enum RepoCanonicalIdentity {
    static func normalize(_ raw: String) -> String? {
        UUID(uuidString: raw)?.uuidString.lowercased()
    }

    static func normalizeLossy(_ raw: String) -> String {
        UUID(uuidString: raw)?.uuidString.lowercased() ?? raw.lowercased()
    }

    static func validate(_ raw: String, field: String) throws -> String {
        try RepoWireValidator.validateRepoID(raw, field: field)
    }
}

struct RepoCanonicalIdentityReader: Sendable {
    enum Load: Sendable, Equatable {
        case absent
        case found(String)
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    func loadCanonical() async throws -> Load {
        switch try await RepoBootstrap(client: client, basePath: basePath).loadRepoIDStrict() {
        case .absent: return .absent
        case .found(let id): return .found(id)
        }
    }

    func requireCanonical(absentError: () -> Error) async throws -> String {
        switch try await loadCanonical() {
        case .found(let id): return id
        case .absent: throw absentError()
        }
    }
}
