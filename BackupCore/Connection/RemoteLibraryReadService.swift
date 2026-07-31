import Foundation

struct RemoteLibraryReadResult: Sendable {
    let digest: RemoteIndexSyncDigest
    let snapshotState: RemoteLibrarySnapshotState
    let layout: MonthManifestStore.ManifestLayout
}

final class RemoteLibraryReadService: @unchecked Sendable {
    private let storageClientFactory: StorageClientFactory
    private let remoteIndexService: RemoteIndexSyncService

    init(
        storageClientFactory: StorageClientFactory,
        remoteIndexService: RemoteIndexSyncService = RemoteIndexSyncService()
    ) {
        self.storageClientFactory = storageClientFactory
        self.remoteIndexService = remoteIndexService
    }

    func reload(
        profile: ServerProfileRecord,
        credentialPayload: String,
        onProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil
    ) async throws -> RemoteLibraryReadResult {
        let client = try storageClientFactory.makeClient(
            profile: profile,
            credentialPayload: credentialPayload
        )
        do {
            try await NetworkRecovery.boundedConnect(
                client,
                deadline: Date().addingTimeInterval(
                    NetworkRecoveryPolicy.connectTimeout
                )
            )
            onProgress?(
                RemoteSyncProgress(
                    current: 0,
                    total: 0,
                    kind: .scanningRemoteIndex
                )
            )
            let layout = try await resolveReadOnlyLayout(
                client: client,
                basePath: profile.basePath
            )
            let digest = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                onSyncProgress: onProgress,
                layout: layout,
                makeClient: {
                    try self.storageClientFactory.makeClient(
                        profile: profile,
                        credentialPayload: credentialPayload
                    )
                },
                downloadConcurrency: layout == .lite ? 2 : 1
            )
            let state = remoteIndexService.currentState(since: nil)
            await client.disconnectSafely()
            return RemoteLibraryReadResult(
                digest: digest,
                snapshotState: state,
                layout: layout
            )
        } catch {
            await client.disconnectSafely()
            throw error
        }
    }

    func currentSnapshotState(
        since revision: UInt64? = nil
    ) -> RemoteLibrarySnapshotState {
        remoteIndexService.currentState(since: revision)
    }

    private func resolveReadOnlyLayout(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> MonthManifestStore.ManifestLayout {
        let decision: RepoFormatDecision
        do {
            decision = try await RepoFormatRouter(
                client: client,
                basePath: basePath
            ).classifyForRead()
        } catch let RepoFormatRouterError.probeFault(category, detail) {
            throw LiteRepoError.probeFault(category, detail: detail)
        }

        switch decision {
        case .current, .fresh:
            return .lite
        case .v1Migrate:
            throw LiteRepoError.repoMaintenanceUnavailable
        case .damaged, .malformedVersion:
            throw LiteRepoError.repoDamaged
        case .unsupported(let minAppVersion):
            throw LiteRepoError.repoUnsupported(
                minAppVersion: minAppVersion
            )
        }
    }
}
