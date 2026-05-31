import Foundation

/// Per-flush maintenance entry point. Returns a typed `RepoMaintenanceMonthResult`
/// covering checkpoint+barrier (Phase A), commit-prefix cleanup (Phase B), and
/// snapshot GC (Phase C). Internals delegate to `RepoMaintenanceCoordinator`.
struct RepoCheckpointBarrierHook: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoMaintenanceMonthResult {
        // Phase 0.5: freeze all destructive maintenance (checkpoint, barrier, commit GC,
        // snapshot GC). No-op until Phase 4 independent compaction restores these paths.
        // Append-only commit writing and per-flush incremental snapshot continue normally.
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

struct RepoRetentionStartupMaintenance: Sendable {
    let services: BackupV2RuntimeServices
    let nowMs: @Sendable () -> Int64

    init(
        services: BackupV2RuntimeServices,
        nowMs: @escaping @Sendable () -> Int64 = {
            Int64(Date().timeIntervalSince1970 * 1000)
        }
    ) {
        self.services = services
        self.nowMs = nowMs
    }

    func run() async throws -> RepoMaintenanceStartupResult {
        // Phase 0.5: freeze startup sweep destructive maintenance (checkpoint, barrier, commit GC,
        // snapshot GC). No-op until Phase 4 independent compaction restores these paths.
        // Liveness heartbeat tick continues; barrier manifests are not written or deleted.
        return RepoMaintenanceStartupResult(monthResults: [:])
    }
}
