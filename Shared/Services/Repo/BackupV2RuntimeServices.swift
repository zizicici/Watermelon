import Foundation

actor InitialMaterializeOutputBox {
    private var value: RepoMaterializer.MaterializeOutput?
    init(_ value: RepoMaterializer.MaterializeOutput?) {
        self.value = value
    }
    func consume() -> RepoMaterializer.MaterializeOutput? {
        let v = value
        value = nil
        return v
    }
    func peek() -> RepoMaterializer.MaterializeOutput? {
        return value
    }
}

struct BackupV2RuntimeServices: Sendable {
    let writerID: String
    let repoID: String
    let runID: String
    let basePath: String
    let postOpenSyncInspection: RemoteFormatInspection?
    let database: DatabaseManager
    let identity: RepoIdentity
    let seqAllocator: SeqAllocator
    let lamport: PersistedLamportClock
    let commitWriter: CommitLogWriter
    let snapshotWriter: SnapshotWriter
    let compactionPolicy: RepoCompactionPolicy
    let isLocalVolume: Bool
    let metadataClient: any RemoteStorageClientProtocol
    let ownsMetadataClient: Bool
    let initialMaterializeOutput: InitialMaterializeOutputBox

    static let shutdownTimeoutSeconds: TimeInterval = 10

    func shutdown() async {
        let latch = ShutdownLatch()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            var cleanupTask: Task<Void, Never>?
            cleanupTask = Task { [metadataClient, ownsMetadataClient] in
                if ownsMetadataClient {
                    await metadataClient.disconnectSafely()
                }
                await latch.resumeOnce(cont)
            }
            Task { [cleanupTask] in
                try? await Task.sleep(for: .seconds(BackupV2RuntimeServices.shutdownTimeoutSeconds))
                cleanupTask?.cancel()
                await latch.resumeOnce(cont)
            }
        }
    }
}

private actor ShutdownLatch {
    private var resumed = false
    func resumeOnce(_ cont: CheckedContinuation<Void, Never>) {
        guard !resumed else { return }
        resumed = true
        cont.resume()
    }
}
