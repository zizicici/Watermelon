import Foundation

/// One-shot holder for the builder's cold-start materialize output — first read
/// consumes, subsequent reads return nil so a stale snapshot can't be reused mid-run.
/// `peek()` returns without consuming; caller consumes only after a successful use
/// (e.g., syncIndex that may throw — eager consume would lose the value on the
/// re-run path).
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
    let database: DatabaseManager
    let identity: RepoIdentity
    let seqAllocator: SeqAllocator
    let lamport: PersistedLamportClock
    let commitWriter: CommitLogWriter
    let snapshotWriter: SnapshotWriter
    let liveness: LivenessTracker
    /// Dedicated connection for V2 metadata writes — sharing with worker uploads would
    /// serialize them at the wire, breaking the pool's "1 worker = 1 connection" invariant.
    let metadataClient: any RemoteStorageClientProtocol
    /// false when metadataClient is borrowed; shutdown skips the disconnect.
    let ownsMetadataClient: Bool
    /// Cold-start materialize output, consumed once by `prepareRun` to avoid running
    /// materialize twice. Nil on the V1-migration path (phase1 already advanced state).
    let initialMaterializeOutput: InitialMaterializeOutputBox
    /// Background orphan-metadata sweep started during bootstrap. shutdown awaits
    /// it so we don't drop the metadata connection while it's still issuing deletes.
    let sweepTask: Task<Void, Never>?

    static let shutdownTimeoutSeconds: TimeInterval = 10

    func shutdown() async {
        // Race clean shutdown vs deadline: SMB/SFTP socket hangs don't honor Swift cancellation.
        let latch = ShutdownLatch()
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            Task { [liveness, sweepTask, metadataClient, ownsMetadataClient] in
                await liveness.stopAndWait()
                sweepTask?.cancel()
                _ = await sweepTask?.value
                if ownsMetadataClient {
                    await metadataClient.disconnectSafely()
                }
                await latch.resumeOnce(cont)
            }
            Task {
                try? await Task.sleep(for: .seconds(BackupV2RuntimeServices.shutdownTimeoutSeconds))
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
