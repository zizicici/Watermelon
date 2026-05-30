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
        let first = try await loadCanonical()
        if case .found = first { return first }
        guard client.readAfterWriteGraceSeconds > 0 else { return first }
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        while Date() < deadline {
            try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
            attempt += 1
            if case .found(let id) = try await loadCanonical() { return .found(id) }
        }
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
