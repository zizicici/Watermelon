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

    // Finalized identity is authoritative; retry it specifically so stale or malformed claims
    // can't abort the grace budget when the finalized marker is hidden.
    func loadCanonicalProvenV2() async throws -> Load {
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let claimStore = IdentityClaimStore(client: client, basePath: basePath)
        var claimFallback: String? = nil
        var claimError: (any Error)?
        let finalized = try await GracefulRead.retryWithinGrace(
            client: client,
            floorSeconds: 1,
            backoff: .exponential(baseMs: 200, maxShift: 3)
        ) {
            if let finalized = try await bootstrap.loadFinalizedRepoIDToleratingDownloadVisibilityLag() {
                return finalized
            }
            do {
                if let claim = try await claimStore.canonicalRepoID() {
                    claimFallback = claim
                }
            } catch {
                claimError = error
            }
            return nil
        }
        if let finalized { return .found(finalized) }
        if let claimFallback { return .found(claimFallback) }
        if let claimError { throw claimError }
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
