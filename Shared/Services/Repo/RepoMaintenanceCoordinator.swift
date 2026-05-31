import Foundation

/// Sequences the three maintenance phases (checkpoint+barrier → commit-prefix cleanup
/// → snapshot GC) with explicit, fail-closed gating rules. Each phase owns its own
/// preflight; the coordinator only sequences and dispatches.
///
/// Phase 4: destructive maintenance is now owned exclusively by RepoCompactionService.
/// This coordinator's phases are frozen; checkpoint, commit GC, and snapshot GC are
/// no-op here. The compaction service provides the only active destructive path.
struct RepoMaintenanceCoordinator: Sendable {
    typealias CommitCleanupOverride = @Sendable (LibraryMonthKey) async throws -> RepoRetentionCommitDeleteResult

    let services: BackupV2RuntimeServices
    let nowMs: @Sendable () -> Int64
    let commitCleanupOverride: CommitCleanupOverride?

    init(
        services: BackupV2RuntimeServices,
        nowMs: @escaping @Sendable () -> Int64 = {
            Int64(Date().timeIntervalSince1970 * 1000)
        },
        commitCleanupOverride: CommitCleanupOverride? = nil
    ) {
        self.services = services
        self.nowMs = nowMs
        self.commitCleanupOverride = commitCleanupOverride
    }

    func runForMonth(_ month: LibraryMonthKey) async throws -> RepoMaintenanceMonthResult {
        // Phase 4: destructive maintenance frozen here.
        // Use RepoCompactionService for active checkpoint/GC paths.
        let checkpoint = RepoCheckpointPhaseResult(
            outcome: .skippedEmptyFold,
            checkpoint: RepoCheckpointResult(
                outcome: .skippedEmptyFold,
                month: month,
                snapshotName: nil,
                lamport: nil,
                covered: .empty,
                beforeReport: nil,
                afterReport: nil,
                acceptedSnapshot: nil
            ),
            barrier: nil
        )
        return RepoMaintenanceMonthResult(
            month: month,
            checkpoint: checkpoint,
            commitCleanup: nil,
            snapshotGC: .skipped(.skippedMaintenanceFrozen)
        )
    }
}

/// Phase A — checkpoint write (when recommended) and retention-barrier publish.
/// Frozen under Phase 4; compaction service owns checkpoint writing.
struct RepoCheckpointPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoCheckpointPhaseResult {
        let checkpoint = RepoCheckpointResult(
            outcome: .skippedEmptyFold,
            month: month,
            snapshotName: nil,
            lamport: nil,
            covered: .empty,
            beforeReport: nil,
            afterReport: nil,
            acceptedSnapshot: nil
        )
        return RepoCheckpointPhaseResult(
            outcome: .skippedEmptyFold,
            checkpoint: checkpoint,
            barrier: nil
        )
    }
}

/// Phase B — commit-prefix cleanup. Frozen under Phase 4.
struct RepoCommitPrefixCleanupPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey
    let nowMs: @Sendable () -> Int64

    func run() async throws -> RepoRetentionCommitDeleteResult {
        return .preflightBlocked(blockers: [], report: RepoRetentionDeletePreflightReport(
            month: month,
            repoID: services.repoID,
            mode: .dryRun,
            evaluatedAtMs: nowMs()
        ))
    }
}

/// Phase C — snapshot GC. Frozen under Phase 4.
struct RepoSnapshotGCPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey
    let nowMs: @Sendable () -> Int64

    func run() async throws -> RepoSnapshotGCResult {
        return .preflightBlocked(blockers: [], report: RepoSnapshotDeletePreflightReport(
            month: month,
            repoID: services.repoID,
            evaluatedAtMs: nowMs()
        ))
    }
}

private extension RepoSnapshotDeleteStopReason {
    var containsCancellation: Bool {
        switch self {
        case .cancelled:
            return true
        case .deleteFailed(_, .cancelled):
            return true
        case .deleteFailed, .preDeleteRevalidationFailed:
            return false
        }
    }
}

private extension RepoSnapshotPostDeleteVerificationResult {
    var containsCancellation: Bool {
        if case .inconclusive(reason: .cancelled) = self {
            return true
        }
        return false
    }
}
