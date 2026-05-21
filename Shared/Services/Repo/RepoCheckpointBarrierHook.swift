import Foundation

struct RepoCheckpointBarrierHookResult: Sendable, Equatable {
    enum Outcome: Sendable, Equatable {
        case skippedEmptyFold
        case skippedBelowThreshold
        case checkpointWrittenBarrierPublished
        case checkpointWrittenBarrierAlreadyExisted
    }

    let outcome: Outcome
    let month: LibraryMonthKey
    let checkpoint: RepoCheckpointResult
    let barrier: RepoRetentionBarrierPublishResult?
    let deleteResult: RepoRetentionCommitDeleteResult?
}

struct RepoCheckpointBarrierHook: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoCheckpointBarrierHookResult {
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
            return RepoCheckpointBarrierHookResult(
                outcome: .skippedEmptyFold,
                month: month,
                checkpoint: checkpoint,
                barrier: nil,
                deleteResult: nil
            )
        case .skippedBelowThreshold:
            let deleteResult = try await runCommitPrefixDeletion()
            return RepoCheckpointBarrierHookResult(
                outcome: .skippedBelowThreshold,
                month: month,
                checkpoint: checkpoint,
                barrier: nil,
                deleteResult: deleteResult
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
            let outcome: RepoCheckpointBarrierHookResult.Outcome
            switch barrier.writeOutcome {
            case .wroteVerified:
                outcome = .checkpointWrittenBarrierPublished
            case .alreadyExistedSameBytes:
                outcome = .checkpointWrittenBarrierAlreadyExisted
            }
            let deleteResult = try await runCommitPrefixDeletion()
            return RepoCheckpointBarrierHookResult(
                outcome: outcome,
                month: month,
                checkpoint: checkpoint,
                barrier: barrier,
                deleteResult: deleteResult
            )
        }
    }

    private func runCommitPrefixDeletion() async throws -> RepoRetentionCommitDeleteResult {
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
            nowMs: Int64(Date().timeIntervalSince1970 * 1000)
        )
        if result.containsCancellation {
            throw CancellationError()
        }
        return result
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

    func run() async throws -> [LibraryMonthKey: RepoRetentionCommitDeleteResult] {
        let now = nowMs()
        let months = try await candidateMonths(nowMs: now)
        var results: [LibraryMonthKey: RepoRetentionCommitDeleteResult] = [:]
        for month in months {
            try Task.checkCancellation()
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
                nowMs: now
            )
            if result.containsCancellation {
                throw CancellationError()
            }
            results[month] = result
        }
        return results
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

private extension RepoRetentionCommitDeleteResult {
    var containsCancellation: Bool {
        switch self {
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
