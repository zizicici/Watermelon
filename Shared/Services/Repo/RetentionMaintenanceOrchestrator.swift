import Foundation

struct RetentionMaintenanceOrchestrator: Sendable {
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

    /// Phase 4: frozen. Use RepoCompactionService for active commit GC.
    func runMonthCommitPrefixDelete(month: LibraryMonthKey) async throws -> RepoRetentionCommitDeleteResult {
        return .preflightBlocked(blockers: [], report: RepoRetentionDeletePreflightReport(
            month: month,
            repoID: services.repoID,
            mode: .dryRun,
            evaluatedAtMs: nowMs()
        ))
    }

    /// Phase 4: delegates to RepoCompactionService for the active compaction path.
    func runStartupSweep() async throws -> RepoMaintenanceStartupResult {
        try await RepoCompactionService(services: services, nowMs: nowMs)
            .compactStartupMonths()
    }

    /// Phase 4: frozen. Use runStartupSweep for the active compaction path.
    func runStartupCommitPrefixSweep() async throws -> [LibraryMonthKey: RepoRetentionCommitDeleteResult] {
        return [:]
    }

    static func containsCancellation(_ result: RepoRetentionCommitDeleteResult) -> Bool {
        switch result {
        case .preflightBlocked(_, _),
             .completed(_, _, _):
            return false
        case .stopped(_, let reason, _, let verification):
            return reason.containsCancellation || verification?.containsCancellation == true
        case .verificationFailed(_, let stopReason, _, let verification):
            return stopReason?.containsCancellation == true || verification.containsCancellation
        case .verificationInconclusive(_, let stopReason, _, let verification):
            return stopReason?.containsCancellation == true || verification.containsCancellation
        }
    }
}

private extension RepoRetentionCommitDeleteStopReason {
    var containsCancellation: Bool {
        switch self {
        case .cancelled(_):
            return true
        case .deleteFailed(_, .cancelled):
            return true
        case .deleteFailed(_, _),
             .preDeleteRevalidationFailed(_, _):
            return false
        }
    }
}

private extension RepoRetentionPostDeleteVerificationResult {
    var containsCancellation: Bool {
        if case .inconclusive(reason: .cancelled) = self {
            return true
        }
        return false
    }
}
