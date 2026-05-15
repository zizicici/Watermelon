import Foundation

/// Owns the snapshot-side state machine for a single `V2MonthSession`:
/// the in-session coverage ledger, the stranded-snapshot-retry pin, and the
/// rebaseline request flag. Writes the snapshot file when called.
///
/// Reference semantics — `V2MonthSession` mutates this object across `flushToRemote`
/// invocations to coordinate retry / rebaseline / post-commit branches.
final class V2MonthSnapshotFlusher {
    private let services: BackupV2RuntimeServices
    private let monthKey: LibraryMonthKey
    private let materializedCovered: CoveredRanges
    private let indexes: V2MonthIndexes

    /// In-session ledger: every commit `seq` we successfully wrote this session.
    /// Read by `V2MonthCommitFlusher` for per-writer-max-seq, merged into `covered`
    /// when writing snapshot.
    private(set) var sessionWrittenCovered: CoveredRanges = .empty

    /// Pinned when a commit landed but the matching snapshot write failed — drives
    /// a standalone snapshot retry on the next flush before any new commit.
    private(set) var pendingSnapshotRetrySeq: UInt64?

    /// Set by `requestRebaseline` (corrupted snapshot detected at load); cleared
    /// after any successful snapshot write.
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

    /// Standalone snapshot retry when a prior commit's snapshot write failed.
    /// Returns true if a retry was actually performed.
    func flushRetryIfPending(ignoreCancellation: Bool) async throws -> Bool {
        guard let retrySeq = pendingSnapshotRetrySeq, !indexes.hasUncommittedOps else {
            return false
        }
        try await writeSnapshot(ownCommitSeq: retrySeq, ignoreCancellation: ignoreCancellation)
        pendingSnapshotRetrySeq = nil
        pendingRebaselineOnly = false
        return true
    }

    /// Rebaseline-only snapshot (no own commit seq added to covered) when the
    /// load-time materializer flagged a corrupted snapshot. Returns true if it ran.
    func flushRebaselineIfPending(ignoreCancellation: Bool) async throws -> Bool {
        guard pendingRebaselineOnly else { return false }
        try await writeSnapshot(ownCommitSeq: nil, ignoreCancellation: ignoreCancellation)
        pendingRebaselineOnly = false
        return true
    }

    /// Records the committed seq in the session ledger and writes the matching
    /// snapshot. On snapshot failure, pins the seq for retry on the next flush and
    /// rethrows — caller wraps in `FlushError.snapshotWriteFailed` (it has the
    /// committed fingerprint sets needed by upstream catch).
    func flushAfterCommit(seq: UInt64, ignoreCancellation: Bool) async throws {
        // Coverage must reflect the durable commit even if snapshot write later fails; otherwise the next materialize would replay this seq atop a future baseline.
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
        // Emit un-filtered state; snapshot must satisfy `state == fold(commits in covered)`.
        let snapshotState = indexes.currentMaterializedState()
        var covered = materializedCovered.merging(sessionWrittenCovered)
        // Only own-writer commit seqs may be added; peer-derived rebaseline values come through `materializedCovered`.
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
