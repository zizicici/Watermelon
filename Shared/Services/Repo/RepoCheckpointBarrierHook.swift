import Foundation

/// Per-flush maintenance entry point. Returns a typed `RepoMaintenanceMonthResult`
/// covering checkpoint+barrier (Phase A), commit-prefix cleanup (Phase B), and
/// snapshot GC (Phase C). Internals delegate to `RepoMaintenanceCoordinator`.
struct RepoCheckpointBarrierHook: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoMaintenanceMonthResult {
        try await RepoMaintenanceCoordinator(services: services).runForMonth(month)
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
        try await RetentionMaintenanceOrchestrator(services: services, nowMs: nowMs).runStartupSweep()
    }
}
