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
    case skippedDisabled
    case skippedReportOnly
    case skippedBelowThreshold
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

enum RepoMaintenanceStartupStage: Sendable, Equatable {
    case repair
    case startupCompaction
}

/// Best-effort, in-memory record of the V2 startup maintenance pass. Non-cancellation failures are
/// captured here rather than failing runtime open, so callers and tests can observe what happened.
struct RepoMaintenanceStartupDiagnostic: Sendable, Equatable {
    let mode: RepoMaintenanceStartupMode
    var ran: Bool
    var repairedCount: Int?
    var startupResult: RepoMaintenanceStartupResult?
    var failureStage: RepoMaintenanceStartupStage?
    var failureDescription: String?

    var failed: Bool { failureStage != nil }

    init(
        mode: RepoMaintenanceStartupMode,
        ran: Bool,
        repairedCount: Int? = nil,
        startupResult: RepoMaintenanceStartupResult? = nil,
        failureStage: RepoMaintenanceStartupStage? = nil,
        failureDescription: String? = nil
    ) {
        self.mode = mode
        self.ran = ran
        self.repairedCount = repairedCount
        self.startupResult = startupResult
        self.failureStage = failureStage
        self.failureDescription = failureDescription
    }

    static func disabled(_ mode: RepoMaintenanceStartupMode) -> RepoMaintenanceStartupDiagnostic {
        RepoMaintenanceStartupDiagnostic(mode: mode, ran: false)
    }
}
