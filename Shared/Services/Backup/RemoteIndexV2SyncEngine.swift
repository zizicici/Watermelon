import Foundation

struct RemoteIndexV2SyncEngine: Sendable {
    func materialize(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        preMaterialized: RepoMaterializer.MaterializeOutput?
    ) async throws -> RepoMaterializer.MaterializeOutput {
        if let preMaterialized {
            return preMaterialized
        }
        let expectedRepoID = try await loadExpectedRepoIDReadOnly(client: client, basePath: basePath)
        return try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: expectedRepoID)
    }

    func loadExpectedRepoIDReadOnly(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> String {
        switch try await RepoBootstrap(client: client, basePath: basePath).loadRepoIDStrict() {
        case .absent:
            throw NSError(
                domain: "RemoteIndexSyncService",
                code: -50,
                userInfo: [NSLocalizedDescriptionKey: "V2 repo missing .watermelon/repo.json — backup-flow can repair, sync cannot"]
            )
        case .found(let id):
            return id
        }
    }
}
