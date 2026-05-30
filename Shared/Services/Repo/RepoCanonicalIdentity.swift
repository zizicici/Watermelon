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

    /// For callers that have already proven the endpoint is in post-bootstrap V2 shape (e.g. a visible
    /// `version.json`): a first-read `.absent` can be read-after-write lag of the identity finalization
    /// marker / claim directory behind the already-visible V2 format marker, not a genuinely
    /// identity-less repo. Spend the read-after-write budget before reporting `.absent`. Malformed
    /// identity and non-not-found transport failures throw out of `loadCanonical` (they never surface
    /// as `.absent`), so this stays fail-closed; a persistent absence past the deadline is still `.absent`.
    func loadCanonicalProvenV2() async throws -> Load {
        let id = try await GracefulRead.retryWithinGrace(
            client: client,
            floorSeconds: 1,
            backoff: .exponential(baseMs: 200, maxShift: 3)
        ) {
            if case .found(let id) = try await loadCanonical() { return id }
            return nil
        }
        if let id { return .found(id) }
        // Zero-grace or persistent absence past the deadline both report `.absent`; malformed
        // identity / transport failures already threw out of `loadCanonical` (fail closed).
        return .absent
    }

    func requireCanonical(absentError: () -> Error) async throws -> String {
        switch try await loadCanonical() {
        case .found(let id): return id
        case .absent: throw absentError()
        }
    }

    func requireCanonicalProvenV2(absentError: () -> Error) async throws -> String {
        switch try await loadCanonicalProvenV2() {
        case .found(let id): return id
        case .absent: throw absentError()
        }
    }
}
