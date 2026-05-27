import Foundation

/// Sequences the three maintenance phases (checkpoint+barrier → commit-prefix cleanup
/// → snapshot GC) with explicit, fail-closed gating rules. Each phase owns its own
/// preflight; the coordinator only sequences and dispatches.
///
/// Gating contract:
/// - Phase A (checkpoint + barrier) always runs and is followed by Phase B regardless
///   of outcome (`skipped*` is non-destructive).
/// - Phase B (commit-prefix cleanup) runs unconditionally. Phase C is entered ONLY
///   when Phase B reaches `.preflightBlocked` (no mutation) or `.completed` (verified
///   mutation). For `.stopped`, `.verificationFailed`, `.verificationInconclusive`,
///   Phase C is skipped — the remote state is in a shape Phase B could not vouch
///   for, so deleting more files on top of it would compound the uncertainty.
/// - Phase C runs its own independent preflight; it never trusts Phase B's plan.
struct RepoMaintenanceCoordinator: Sendable {
    typealias CommitCleanupOverride = @Sendable (LibraryMonthKey) async throws -> RepoRetentionCommitDeleteResult

    let services: BackupV2RuntimeServices
    let nowMs: @Sendable () -> Int64
    // Test-only seam for driving the coordinator into Phase-B branches that are hard to reach
    // deterministically from the real executor (verificationFailed / verificationInconclusive).
    // Production must leave this nil so the real Phase B runs.
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
        try Task.checkCancellation()
        let checkpoint = try await RepoCheckpointPhase(services: services, month: month).run()
        try Task.checkCancellation()
        let commitCleanup: RepoRetentionCommitDeleteResult
        if let override = commitCleanupOverride {
            commitCleanup = try await override(month)
        } else {
            commitCleanup = try await RepoCommitPrefixCleanupPhase(
                services: services,
                month: month,
                nowMs: nowMs
            ).run()
        }
        let snapshotGC: RepoMaintenanceSnapshotGCDisposition
        switch commitCleanup {
        case .preflightBlocked, .completed:
            if Task.isCancelled {
                snapshotGC = .skipped(.skippedCancellation)
            } else {
                let gcResult = try await RepoSnapshotGCPhase(
                    services: services,
                    month: month,
                    nowMs: nowMs
                ).run()
                snapshotGC = .ran(gcResult)
            }
        case .stopped:
            snapshotGC = .skipped(.skippedAfterCommitCleanupStopped)
        case .verificationFailed:
            snapshotGC = .skipped(.skippedAfterCommitCleanupVerificationFailed)
        case .verificationInconclusive:
            snapshotGC = .skipped(.skippedAfterCommitCleanupVerificationInconclusive)
        }
        return RepoMaintenanceMonthResult(
            month: month,
            checkpoint: checkpoint,
            commitCleanup: commitCleanup,
            snapshotGC: snapshotGC
        )
    }
}

/// Phase A — checkpoint write (when recommended) and retention-barrier publish.
struct RepoCheckpointPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoCheckpointPhaseResult {
        let checkpoint = try await RepoCheckpointService(
            client: services.metadataClient,
            basePath: services.basePath,
            repoID: services.repoID,
            writerID: services.writerID,
            runID: services.runID,
            clock: services.lamport,
            policy: services.compactionPolicy
        ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true)

        switch checkpoint.outcome {
        case .skippedEmptyFold:
            return RepoCheckpointPhaseResult(
                outcome: .skippedEmptyFold,
                checkpoint: checkpoint,
                barrier: nil
            )
        case .skippedBelowThreshold:
            return RepoCheckpointPhaseResult(
                outcome: .skippedBelowThreshold,
                checkpoint: checkpoint,
                barrier: nil
            )
        case .writtenAccepted:
            let barrier = try await RepoRetentionBarrierService(
                client: services.metadataClient,
                basePath: services.basePath,
                repoID: services.repoID,
                writerID: services.writerID,
                runID: services.runID,
                policy: services.compactionPolicy
            ).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            let outcome: RepoCheckpointPhaseOutcome
            switch barrier.writeOutcome {
            case .wroteVerified:
                outcome = .checkpointWrittenBarrierPublished
            case .alreadyExistedSameBytes:
                outcome = .checkpointWrittenBarrierAlreadyExisted
            }
            return RepoCheckpointPhaseResult(
                outcome: outcome,
                checkpoint: checkpoint,
                barrier: barrier
            )
        }
    }
}

/// Phase B — commit-prefix cleanup. Reuses the existing executor unchanged.
struct RepoCommitPrefixCleanupPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey
    let nowMs: @Sendable () -> Int64

    func run() async throws -> RepoRetentionCommitDeleteResult {
        let result = try await RepoRetentionCommitDeleteExecutor(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy,
            isLocalVolume: services.isLocalVolume,
            peerStatusProvider: {
                try await services.liveness.snapshotRetentionPeerStatuses()
            }
        ).execute(
            month: month,
            expectedRepoID: services.repoID,
            nowMs: nowMs()
        )
        if RetentionMaintenanceOrchestrator.containsCancellation(result) {
            throw CancellationError()
        }
        return result
    }
}

/// Phase C — snapshot GC. Independent preflight; never trusts Phase B's plan.
struct RepoSnapshotGCPhase: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey
    let nowMs: @Sendable () -> Int64

    func run() async throws -> RepoSnapshotGCResult {
        let result = try await RepoSnapshotDeleteExecutor(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy,
            isLocalVolume: services.isLocalVolume,
            peerStatusProvider: {
                try await services.liveness.snapshotRetentionPeerStatuses()
            }
        ).execute(
            month: month,
            expectedRepoID: services.repoID,
            nowMs: nowMs()
        )
        if Self.containsCancellation(result) {
            throw CancellationError()
        }
        return result
    }

    static func containsCancellation(_ result: RepoSnapshotGCResult) -> Bool {
        switch result {
        case .preflightBlocked, .completed:
            return false
        case .stopped(_, let reason, _, let verification):
            return reason.containsCancellation || (verification?.containsCancellation ?? false)
        case .verificationFailed(_, let stopReason, _, let verification):
            return (stopReason?.containsCancellation ?? false) || verification.containsCancellation
        case .verificationInconclusive(_, let stopReason, _, let verification):
            return (stopReason?.containsCancellation ?? false) || verification.containsCancellation
        }
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
