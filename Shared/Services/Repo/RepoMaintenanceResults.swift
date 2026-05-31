import Foundation

/// Phase A (checkpoint + barrier publish) outcome — distinguishes the four cases
/// the per-flush hook historically surfaced.
enum RepoCheckpointPhaseOutcome: Sendable, Equatable {
    case skippedEmptyFold
    case skippedBelowThreshold
    case checkpointWrittenBarrierPublished
    case checkpointWrittenBarrierAlreadyExisted
}

struct RepoCheckpointPhaseResult: Sendable, Equatable {
    let outcome: RepoCheckpointPhaseOutcome
    let checkpoint: RepoCheckpointResult
    let barrier: RepoRetentionBarrierPublishResult?
}

extension RepoCheckpointPhaseResult {
    var snapshotName: String? { checkpoint.snapshotName }
    var lamport: UInt64? { checkpoint.lamport }
    var covered: CoveredRanges { checkpoint.covered }
    var month: LibraryMonthKey { checkpoint.month }
}

/// Why Phase C did not run.
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

/// Per-month result of the maintenance coordinator. Replaces the implicit bundling
/// in the old `RepoCheckpointBarrierHookResult` — checkpoint, commit cleanup, and
/// snapshot GC each occupy a separate typed slot.
struct RepoMaintenanceMonthResult: Sendable, Equatable {
    let month: LibraryMonthKey
    let checkpoint: RepoCheckpointPhaseResult
    let commitCleanup: RepoRetentionCommitDeleteResult?
    let snapshotGC: RepoMaintenanceSnapshotGCDisposition

    /// Phase A outcome — kept addressable at the top level for callers and existing
    /// test assertions that historically reached for `result.outcome`.
    var outcome: RepoCheckpointPhaseOutcome { checkpoint.outcome }

    /// Compatibility shim for tests that referenced the old `deleteResult` field
    /// before commit cleanup was renamed.
    var deleteResult: RepoRetentionCommitDeleteResult? { commitCleanup }

    /// Phase A's barrier publish result, if any.
    var barrier: RepoRetentionBarrierPublishResult? { checkpoint.barrier }
}

/// Backwards-compatible alias so the in-tree references to the old result name
/// keep compiling while the rename propagates.
typealias RepoCheckpointBarrierHookResult = RepoMaintenanceMonthResult

/// Typed result of a multi-month startup maintenance sweep.
struct RepoMaintenanceStartupResult: Sendable, Equatable {
    let monthResults: [LibraryMonthKey: RepoMaintenanceMonthResult]

    /// Compatibility shim: yields the commit-cleanup branch for callers that only
    /// care about Phase B (matches the legacy `[LibraryMonthKey: ...]` surface).
    subscript(month: LibraryMonthKey) -> RepoRetentionCommitDeleteResult? {
        monthResults[month]?.commitCleanup
    }
}
