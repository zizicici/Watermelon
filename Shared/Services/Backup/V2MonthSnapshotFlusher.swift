import Foundation

final class V2MonthSnapshotFlusher {
    private let services: BackupV2RuntimeServices
    private let monthKey: LibraryMonthKey
    private let materializedCovered: CoveredRanges
    private let indexes: V2MonthIndexes

    /// Tracks durable commits so later snapshots do not replay them.
    private(set) var sessionWrittenCovered: CoveredRanges = .empty

    private var pendingSnapshotWork = false

    /// Corrupted snapshots force a clean baseline before normal flushing resumes.
    private(set) var pendingRebaselineOnly: Bool = false

    var hasPendingSnapshotWork: Bool {
        pendingSnapshotWork || pendingRebaselineOnly
    }

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

    func recordCommitted(seq: UInt64) {
        sessionWrittenCovered.add(writerID: services.writerID, seq: seq)
        pendingSnapshotWork = true
        pendingRebaselineOnly = false
    }

    func flushSnapshotIfPending(ignoreCancellation: Bool) async throws -> Bool {
        guard hasPendingSnapshotWork else { return false }
        try await writeSnapshot(ignoreCancellation: ignoreCancellation)
        pendingSnapshotWork = false
        pendingRebaselineOnly = false
        return true
    }

    private func writeSnapshot(ignoreCancellation: Bool) async throws {
        // Snapshot state must stay unfiltered to preserve covered-range replay.
        let snapshotState = indexes.currentMaterializedState()
        let covered = materializedCovered.merging(sessionWrittenCovered)
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
