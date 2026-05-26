import Foundation

struct RepoRetentionBarrierPublishResult: Sendable, Equatable {
    let manifest: RetentionManifest
    let filename: String
    let writeOutcome: RetentionManifestWriteResult.Outcome
    let barrierSet: RetentionBarrierSet
    let loadInvalidEntries: [InvalidRetentionManifestEntry]
}

enum RepoRetentionBarrierError: Error, Equatable {
    case checkpointNotWritten
    case checkpointNotAccepted(snapshotName: String)
    case checkpointCoverageMismatch(snapshotName: String)
    case checkpointReadFailed(snapshotName: String)
    case checkpointSHAMismatch(snapshotName: String)
    case invalidRunID(String)
    case invalidBarrierLamport(UInt64)
    case invalidBarrierSet([InvalidRetentionManifestEntry])
}

struct RepoRetentionBarrierService: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let repoID: String
    let writerID: String
    let runID: String
    let policy: RepoCompactionPolicy
    let nowMs: @Sendable () -> Int64

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        repoID: String,
        writerID: String,
        runID: String,
        policy: RepoCompactionPolicy = .default,
        nowMs: @escaping @Sendable () -> Int64 = {
            Int64(Date().timeIntervalSince1970 * 1000)
        }
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.repoID = UUID(uuidString: repoID)?.uuidString.lowercased() ?? repoID
        self.writerID = writerID.lowercased()
        self.runID = UUID(uuidString: runID)?.uuidString.lowercased() ?? runID
        self.policy = policy
        self.nowMs = nowMs
    }

    func publishBarrier(
        for checkpoint: RepoCheckpointResult,
        respectTaskCancellation: Bool
    ) async throws -> RepoRetentionBarrierPublishResult {
        guard checkpoint.outcome == .writtenAccepted,
              let snapshotName = checkpoint.snapshotName,
              let checkpointLamport = checkpoint.lamport else {
            throw RepoRetentionBarrierError.checkpointNotWritten
        }
        guard let runUUID = UUID(uuidString: runID) else {
            throw RepoRetentionBarrierError.invalidRunID(runID)
        }
        guard checkpointLamport < LamportClock.maxAdoptableValue else {
            throw RepoRetentionBarrierError.invalidBarrierLamport(checkpointLamport)
        }
        do {
            let materialized = try await RepoMaterializer(client: client, basePath: basePath)
                .materializeMonth(checkpoint.month, expectedRepoID: repoID)
            guard let accepted = materialized.acceptedSnapshotBaselinesByMonth[checkpoint.month],
                  accepted.filename == snapshotName,
                  accepted.lamport == checkpointLamport else {
                throw RepoRetentionBarrierError.checkpointNotAccepted(snapshotName: snapshotName)
            }
            guard accepted.covered == checkpoint.covered else {
                throw RepoRetentionBarrierError.checkpointCoverageMismatch(snapshotName: snapshotName)
            }
            let snapshotFile: SnapshotFile
            do {
                snapshotFile = try await SnapshotReader(client: client, basePath: basePath).read(filename: snapshotName)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                throw RepoRetentionBarrierError.checkpointReadFailed(snapshotName: snapshotName)
            }
            guard snapshotFile.header.repoID == repoID,
                  snapshotFile.header.scope == CommitHeader.monthScope(checkpoint.month),
                  snapshotFile.header.writerID == writerID,
                  snapshotFile.header.covered == accepted.covered else {
                throw RepoRetentionBarrierError.checkpointSHAMismatch(snapshotName: snapshotName)
            }
            guard accepted.lamport < LamportClock.maxAdoptableValue else {
                throw RepoRetentionBarrierError.invalidBarrierLamport(accepted.lamport)
            }

            let manifest = RetentionManifest(
                version: RetentionManifest.currentVersion,
                repoID: repoID,
                month: checkpoint.month,
                createdByWriterID: writerID,
                runID: runUUID,
                createdAtMs: nowMs(),
                barrierLamport: accepted.lamport,
                checkpointSnapshotName: snapshotName,
                checkpointSHA256Hex: snapshotFile.sha256Hex,
                coveredRanges: accepted.covered,
                deletePrefixByWriter: policy.conservativeDeletePrefixByWriter(covered: accepted.covered),
                observedSeqHighByWriter: materialized.observedSeqByWriter,
                policy: RetentionManifestPolicy(
                    keepUncoveredCommits: true,
                    keepCorruptOrUntrustedCommits: true,
                    keepTombstones: true,
                    snapshotKeepCount: policy.snapshotFallbackKeepCount
                ),
                livenessGate: RetentionLivenessGate(
                    requiredCompleteView: true,
                    requiredNoActiveNonSelfWriters: true,
                    legacyClientGraceMs: Int64(BackupV2Constants.unknownRetentionCapabilityGraceSeconds) * 1000
                )
            )
            let store = RetentionManifestRemoteStore(client: client, basePath: basePath)
            let write = try await store.writeVerified(manifest, respectTaskCancellation: respectTaskCancellation)
            let loaded = try await store.loadBarrierSet(expectedRepoID: repoID, month: checkpoint.month)
            guard loaded.valid.contains(manifest) else {
                throw RepoRetentionBarrierError.invalidBarrierSet(loaded.invalid)
            }
            // Publishing fails closed while same-month corrupt siblings are present.
            guard loaded.invalid.isEmpty else {
                throw RepoRetentionBarrierError.invalidBarrierSet(loaded.invalid)
            }
            guard loaded.barrierSet.unionCovered.superset(of: manifest.coveredRanges) else {
                throw RepoRetentionBarrierError.invalidBarrierSet([])
            }
            return RepoRetentionBarrierPublishResult(
                manifest: manifest,
                filename: write.filename,
                writeOutcome: write.outcome,
                barrierSet: loaded.barrierSet,
                loadInvalidEntries: []
            )
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }
}
