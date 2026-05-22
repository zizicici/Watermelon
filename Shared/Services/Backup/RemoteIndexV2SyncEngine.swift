import Foundation

struct RemoteIndexV2SyncEngine: Sendable {
    func materialize(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        preMaterialized: RepoMaterializer.MaterializeOutput?,
        localRepoID: String? = nil
    ) async throws -> RepoMaterializer.MaterializeOutput {
        if let preMaterialized {
            if let localRepoID, let outputRepoID = preMaterialized.repoID, localRepoID != outputRepoID {
                throw BackupCompatibilityError.repoIdentityMismatch
            }
            if let localRepoID {
                let liveRepoID = try await loadExpectedRepoIDReadOnly(client: client, basePath: basePath)
                if localRepoID != liveRepoID {
                    throw BackupCompatibilityError.repoIdentityMismatch
                }
            }
            return preMaterialized
        }
        let expectedRepoID = try await loadExpectedRepoIDReadOnly(client: client, basePath: basePath)
        if let localRepoID, localRepoID != expectedRepoID {
            throw BackupCompatibilityError.repoIdentityMismatch
        }
        return try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: expectedRepoID)
    }

    func loadExpectedRepoIDReadOnly(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> String {
        do {
            switch try await RepoBootstrap(client: client, basePath: basePath).loadRepoIDStrict() {
            case .absent:
                throw NSError(
                    domain: "RemoteIndexSyncService",
                    code: -50,
                    userInfo: [NSLocalizedDescriptionKey: "V2 repo missing .watermelon/repo.json - backup-flow can repair, sync cannot"]
                )
            case .found(let id):
                return id
            }
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            switch bootstrap {
            case .futureFormatVersion(let minAppVersion):
                throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
            case .ioFailure(let underlying):
                // Surface external-volume loss directly; the run-level classifier doesn't peel BootstrapError.
                if RemoteStorageClientError.isLikelyExternalStorageUnavailable(underlying) {
                    throw underlying
                }
                throw BackupCompatibilityError.damagedV2Repo
            }
        }
    }
}
