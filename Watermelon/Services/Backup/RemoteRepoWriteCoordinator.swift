import Foundation

struct RemoteRepoWriteCoordinator: RepoWriteCoordinator {
    let client: any RemoteStorageClientProtocol
    let lockClientHandle: LiteLockClientHandle
    let reconnectLockClient: ConnectedLockClientProvider?
    let allowsFreshOwnLockTakeover: Bool
    let freshOwnLockTakeoverScopes: Set<String>
    let ownLockTakeoverScope: String?
    let onForeignWriterObserved: (@Sendable () async -> Void)?
    let diagnosticLogger: RepoLeaseDiagnosticLogger?

    func acquire(
        basePath: String,
        writerID: String?,
        mode: RepoWritePreparationMode,
        now: Date
    ) async throws -> RepoWriteAcquisition<RepoLeaseSession> {
        guard let writerID else { return .declined(.writerIdentityUnavailable) }
        guard let lock = WriteLockService(
            basePath: basePath,
            writerID: writerID,
            client: lockClientHandle.client,
            allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
            freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
            ownLockTakeoverScope: ownLockTakeoverScope,
            onForeignWriterObserved: onForeignWriterObserved,
            onDiagnostic: diagnosticLogger
        ) else {
            return .declined(.writerIdentityUnavailable)
        }

        try await client.createDirectory(path: RemotePathBuilder.normalizePath(basePath))
        let lockMode: WriteLockService.Mode = mode == .background ? .background : .foreground
        switch await lock.acquire(mode: lockMode, now: now) {
        case .acquired:
            let session = RepoLeaseSession(
                lock: lock,
                lockClientHandle: lockClientHandle,
                reconnectLockClient: reconnectLockClient,
                diagnosticLogger: diagnosticLogger
            )
            return .acquired(AcquiredRepoWriteAuthority(
                session: session,
                authorID: writerID,
                cleansCoordinationArtifacts: true
            ))
        case .blocked, .skipped:
            return .declined(.lockConflict)
        case .blockedByOwnLock(let block), .skippedByOwnLock(let block):
            return .declined(.ownLockConflict(block))
        case .faulted(let category):
            if mode == .background, category == .cancelled {
                throw CancellationError()
            }
            return .declined(.lockFault(category))
        }
    }
}
