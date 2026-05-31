import Foundation

enum RepoCheckpointPhaseOutcome: Sendable, Equatable {
    case skippedEmptyFold
    case skippedBelowThreshold
    case checkpointWritten
}

struct RepoCheckpointPhaseResult: Sendable, Equatable {
    let outcome: RepoCheckpointPhaseOutcome
    let checkpoint: RepoCheckpointResult
}

extension RepoCheckpointPhaseResult {
    var snapshotName: String? { checkpoint.snapshotName }
    var lamport: UInt64? { checkpoint.lamport }
    var covered: CoveredRanges { checkpoint.covered }
    var month: LibraryMonthKey { checkpoint.month }
}

enum RepoMaintenanceSnapshotGCSkipReason: Sendable, Equatable {
    case skippedAfterCommitCleanupStopped
    case skippedAfterCommitCleanupVerificationFailed
    case skippedAfterCommitCleanupVerificationInconclusive
    case skippedCancellation
    case skippedMaintenanceFrozen
}

enum RepoMaintenanceSnapshotGCDisposition: Sendable, Equatable {
    case ran(RepoSnapshotGCResult)
    case skipped(RepoMaintenanceSnapshotGCSkipReason)
}

struct RepoMaintenanceMonthResult: Sendable, Equatable {
    let month: LibraryMonthKey
    let checkpoint: RepoCheckpointPhaseResult
    let commitCleanup: RepoRetentionCommitDeleteResult?
    let snapshotGC: RepoMaintenanceSnapshotGCDisposition

    var outcome: RepoCheckpointPhaseOutcome { checkpoint.outcome }

    var deleteResult: RepoRetentionCommitDeleteResult? { commitCleanup }
}

struct RepoMaintenanceStartupResult: Sendable, Equatable {
    let monthResults: [LibraryMonthKey: RepoMaintenanceMonthResult]

    subscript(month: LibraryMonthKey) -> RepoRetentionCommitDeleteResult? {
        monthResults[month]?.commitCleanup
    }
}
