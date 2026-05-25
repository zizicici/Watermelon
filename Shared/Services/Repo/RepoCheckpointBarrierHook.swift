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
        try await RetentionMaintenanceOrchestrator(services: services).runMonthCommitPrefixDelete(month: month)
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
        try await RetentionMaintenanceOrchestrator(services: services, nowMs: nowMs).runStartupCommitPrefixSweep()
    }
}
