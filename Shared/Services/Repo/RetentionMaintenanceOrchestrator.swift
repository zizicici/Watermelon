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

    /// Phase B only — preserves the narrow commit-cleanup surface for tests and any
    /// caller that does not want to trigger checkpoint or snapshot GC.
    func runMonthCommitPrefixDelete(month: LibraryMonthKey) async throws -> RepoRetentionCommitDeleteResult {
        let result = try await RepoRetentionCommitDeleteExecutor(
            client: services.metadataClient,
            basePath: services.basePath,
            policy: services.compactionPolicy,
            isLocalVolume: services.isLocalVolume
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

    /// Multi-month sweep at startup. Iterates barrier-bearing months and runs the
    /// full coordinator (Phase A + B + C) per month, returning a typed result that
    /// surfaces snapshot-GC dispositions alongside commit cleanup.
    func runStartupSweep() async throws -> RepoMaintenanceStartupResult {
        let coordinator = RepoMaintenanceCoordinator(services: services, nowMs: nowMs)
        let months = try await candidateMonths(nowMs: nowMs())
        var monthResults: [LibraryMonthKey: RepoMaintenanceMonthResult] = [:]
        for month in months {
            try Task.checkCancellation()
            let result = try await coordinator.runForMonth(month)
            monthResults[month] = result
        }
        return RepoMaintenanceStartupResult(monthResults: monthResults)
    }

    /// Legacy commit-only startup sweep retained for tests / callers that only want
    /// the Phase B dictionary. New callers should prefer `runStartupSweep`.
    func runStartupCommitPrefixSweep() async throws -> [LibraryMonthKey: RepoRetentionCommitDeleteResult] {
        let now = nowMs()
        let months = try await candidateMonths(nowMs: now)
        var results: [LibraryMonthKey: RepoRetentionCommitDeleteResult] = [:]
        for month in months {
            try Task.checkCancellation()
            let result = try await RepoRetentionCommitDeleteExecutor(
                client: services.metadataClient,
                basePath: services.basePath,
                policy: services.compactionPolicy,
                isLocalVolume: services.isLocalVolume
            ).execute(
                month: month,
                expectedRepoID: services.repoID,
                nowMs: now
            )
            if Self.containsCancellation(result) {
                throw CancellationError()
            }
            results[month] = result
        }
        return results
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

    private func candidateMonths(nowMs: Int64) async throws -> [LibraryMonthKey] {
        let load = try await RetentionManifestRemoteStore(
            client: services.metadataClient,
            basePath: services.basePath
        ).loadManifests(expectedRepoID: services.repoID, month: nil)
        let minAgeMs = Int64(services.compactionPolicy.retentionStalenessThresholdSeconds) * 1000
        let months = Set(load.valid.compactMap { manifest -> LibraryMonthKey? in
            guard nowMs - manifest.createdAtMs >= minAgeMs else { return nil }
            return manifest.month
        })
        return months.sorted()
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
