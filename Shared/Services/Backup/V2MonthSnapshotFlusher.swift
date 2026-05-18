import Foundation

final class V2MonthSnapshotFlusher {
    private let services: BackupV2RuntimeServices
    private let monthKey: LibraryMonthKey
    private let materializedCovered: CoveredRanges
    private let indexes: V2MonthIndexes

    /// Tracks durable commits so later snapshots do not replay them.
    private(set) var sessionWrittenCovered: CoveredRanges = .empty

    /// Stranded commits must be snapshotted before new ops.
    private(set) var pendingSnapshotRetrySeq: UInt64?

    /// Corrupted snapshots force a clean baseline before normal flushing resumes.
    private(set) var pendingRebaselineOnly: Bool = false

    init(
        services: BackupV2RuntimeServices,
        monthKey: LibraryMonthKey,
        materializedCovered: CoveredRanges,
        indexes: V2MonthIndexes
    ) {
        self.services = services
        self.monthKey = monthKey
        self.materializedCovered = materializedCovered
        self.indexes = indexes
    }

    func requestRebaseline() {
        pendingRebaselineOnly = true
    }

    func flushRetryIfPending(ignoreCancellation: Bool) async throws -> Bool {
        guard let retrySeq = pendingSnapshotRetrySeq, !indexes.hasUncommittedOps else {
            return false
        }
        try await writeSnapshot(ownCommitSeq: retrySeq, ignoreCancellation: ignoreCancellation)
        pendingSnapshotRetrySeq = nil
        pendingRebaselineOnly = false
        return true
    }

    func flushRebaselineIfPending(ignoreCancellation: Bool) async throws -> Bool {
        guard pendingRebaselineOnly else { return false }
        try await writeSnapshot(ownCommitSeq: nil, ignoreCancellation: ignoreCancellation)
        pendingRebaselineOnly = false
        return true
    }

    func flushAfterCommit(seq: UInt64, ignoreCancellation: Bool) async throws {
        // Record covered seq before snapshot write so retry does not replay a durable commit.
        sessionWrittenCovered.add(writerID: services.writerID, seq: seq)
        do {
            try await writeSnapshot(ownCommitSeq: seq, ignoreCancellation: ignoreCancellation)
            pendingSnapshotRetrySeq = nil
            pendingRebaselineOnly = false
        } catch {
            pendingSnapshotRetrySeq = seq
            pendingRebaselineOnly = false
            throw error
        }
    }

    private func writeSnapshot(ownCommitSeq: UInt64?, ignoreCancellation: Bool) async throws {
        // Snapshot state must stay unfiltered to preserve covered-range replay.
        let snapshotState = indexes.currentMaterializedState()
        var covered = materializedCovered.merging(sessionWrittenCovered)
        // Peer-derived rebaseline values come through materializedCovered, not own seqs.
        if let ownSeq = ownCommitSeq {
            covered.add(writerID: services.writerID, seq: ownSeq)
        }
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: services.writerID,
            repoID: services.repoID,
            covered: covered
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: snapshotState)
        // Tick: retry needs a fresh filename, else alreadyExists loops forever.
        let lamportRange = try await services.lamport.tickRange(count: 1)
        _ = try await services.snapshotWriter.write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: lamportRange.high,
            runID: services.runID,
            respectTaskCancellation: !ignoreCancellation
        )
    }
}
