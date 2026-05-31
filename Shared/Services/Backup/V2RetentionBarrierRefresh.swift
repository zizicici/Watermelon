import Foundation

enum V2RetentionBarrierRefreshError: Error, Equatable {
    case invalidBarrierSet([InvalidRetentionManifestEntry])
    case freshCoverageMissingBarrier(month: LibraryMonthKey, fresh: CoveredRanges, barrier: CoveredRanges)
    case freshCoverageMissingSessionWrites(month: LibraryMonthKey, fresh: CoveredRanges, sessionWritten: CoveredRanges)
    case ambiguousMaterialization(LibraryMonthKey)
}

struct BarrierAwareCommitRefreshResult: Sendable {
    let barrierSet: RetentionBarrierSet
    let freshOutput: RepoMaterializer.MaterializeOutput
    let clockFloor: UInt64
}

struct BarrierAwareSnapshotRefreshResult: Sendable {
    let barrierSet: RetentionBarrierSet
    let freshOutput: RepoMaterializer.MaterializeOutput
    let state: RepoMonthState
    let covered: CoveredRanges
    let clockFloor: UInt64
}

struct V2RetentionBarrierRefresh {
    let services: BackupV2RuntimeServices
    let monthKey: LibraryMonthKey

    func commitRefresh(ignoreCancellation: Bool) async throws -> BarrierAwareCommitRefreshResult? {
        if !ignoreCancellation { try Task.checkCancellation() }
        let barrierSet = try await loadCompleteBarrierSet()
        guard !barrierSet.unionCovered.isEmpty else { return nil }

        let fresh = try await freshMaterialize()
        guard fresh.outcomeByMonth[monthKey] != .ambiguous else {
            throw V2RetentionBarrierRefreshError.ambiguousMaterialization(monthKey)
        }
        let freshCovered = fresh.coveredByMonth[monthKey] ?? .empty
        guard freshCovered.superset(of: barrierSet.unionCovered) else {
            throw V2RetentionBarrierRefreshError.freshCoverageMissingBarrier(
                month: monthKey,
                fresh: freshCovered,
                barrier: barrierSet.unionCovered
            )
        }
        try await RepoStateAuthority.observeSameWriterSeq(
            writerID: services.writerID,
            observedSeqByWriter: fresh.observedSeqByWriter,
            allocator: services.seqAllocator
        )
        try await services.lamport.observe(fresh.state.observedClock)
        return BarrierAwareCommitRefreshResult(
            barrierSet: barrierSet,
            freshOutput: fresh,
            clockFloor: fresh.state.observedClock
        )
    }

    func snapshotRefresh(
        sessionWrittenCovered: CoveredRanges,
        ignoreCancellation: Bool
    ) async throws -> BarrierAwareSnapshotRefreshResult? {
        if !ignoreCancellation { try Task.checkCancellation() }
        let barrierSet = try await loadCompleteBarrierSet()
        guard !barrierSet.unionCovered.isEmpty else { return nil }

        let fresh = try await freshMaterialize()
        guard fresh.outcomeByMonth[monthKey] != .ambiguous else {
            throw V2RetentionBarrierRefreshError.ambiguousMaterialization(monthKey)
        }
        let freshCovered = fresh.coveredByMonth[monthKey] ?? .empty
        guard freshCovered.superset(of: barrierSet.unionCovered) else {
            throw V2RetentionBarrierRefreshError.freshCoverageMissingBarrier(
                month: monthKey,
                fresh: freshCovered,
                barrier: barrierSet.unionCovered
            )
        }
        guard freshCovered.superset(of: sessionWrittenCovered) else {
            throw V2RetentionBarrierRefreshError.freshCoverageMissingSessionWrites(
                month: monthKey,
                fresh: freshCovered,
                sessionWritten: sessionWrittenCovered
            )
        }
        try await services.lamport.observe(fresh.state.observedClock)
        return BarrierAwareSnapshotRefreshResult(
            barrierSet: barrierSet,
            freshOutput: fresh,
            state: fresh.state.months[monthKey] ?? .empty,
            covered: freshCovered,
            clockFloor: fresh.state.observedClock
        )
    }

    private func loadCompleteBarrierSet() async throws -> RetentionBarrierSet {
        let store = RetentionManifestRemoteStore(client: services.metadataClient, basePath: services.basePath)
        let loaded = try await store.loadBarrierSet(expectedRepoID: services.repoID, month: monthKey)
        guard loaded.isComplete else {
            throw V2RetentionBarrierRefreshError.invalidBarrierSet(loaded.invalid)
        }
        return loaded.barrierSet
    }

    private func freshMaterialize() async throws -> RepoMaterializer.MaterializeOutput {
        try await RepoMaterializer(client: services.metadataClient, basePath: services.basePath)
            .materializeMonth(monthKey, expectedRepoID: services.repoID)
    }
}

private extension CoveredRanges {
    var isEmpty: Bool {
        rangesByWriter.values.allSatisfy(\.isEmpty)
    }
}
