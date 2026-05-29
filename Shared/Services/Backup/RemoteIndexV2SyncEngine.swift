import Foundation

struct RemoteIndexV2SyncEngine: Sendable {
    // Deterministic refusal raised when an accepted-`.v2` endpoint has no canonical identity:
    // backup-flow can repair it, but sync must fail closed. Surfaced as a plain NSError (not a
    // BackupCompatibilityError) so callers can distinguish it; the domain/code let the sync
    // service recognise it and drop its stale committed view.
    static let missingCanonicalIdentityErrorDomain = "RemoteIndexSyncService"
    static let missingCanonicalIdentityErrorCode = -50

    func materialize(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        preMaterialized: RepoMaterializer.MaterializeOutput?,
        localRepoID: String? = nil
    ) async throws -> RepoMaterializer.MaterializeOutput {
        if let preMaterialized {
            if let localRepoID, let outputRepoID = preMaterialized.repoID, localRepoID != outputRepoID {
                throw BackupCompatibilityError.repoIdentityMismatch(stored: localRepoID, observed: outputRepoID)
            }
            if let localRepoID {
                let liveRepoID = try await loadExpectedRepoIDReadOnly(client: client, basePath: basePath)
                if localRepoID != liveRepoID {
                    throw BackupCompatibilityError.repoIdentityMismatch(stored: localRepoID, observed: liveRepoID)
                }
            }
            return preMaterialized
        }
        let expectedRepoID = try await loadExpectedRepoIDReadOnly(client: client, basePath: basePath)
        if let localRepoID, localRepoID != expectedRepoID {
            throw BackupCompatibilityError.repoIdentityMismatch(stored: localRepoID, observed: expectedRepoID)
        }
        return try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: expectedRepoID)
    }

    func loadExpectedRepoIDReadOnly(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> String {
        do {
            return try await RepoCanonicalIdentityReader(client: client, basePath: basePath)
                .requireCanonical(absentError: {
                    NSError(
                        domain: Self.missingCanonicalIdentityErrorDomain,
                        code: Self.missingCanonicalIdentityErrorCode,
                        userInfo: [NSLocalizedDescriptionKey: "V2 repo missing canonical identity - backup-flow can repair, sync cannot"]
                    )
                })
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            throw BackupV2RuntimeOpenErrorMapping.translateToCompatibilityError(bootstrapError: bootstrap)
        }
    }
}
