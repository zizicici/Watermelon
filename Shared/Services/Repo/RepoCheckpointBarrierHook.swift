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
}

enum RepoCheckpointBarrierHookError: Error, Equatable {
    case invalidRuntimeMode
}

struct RepoCheckpointBarrierHook: Sendable {
    let services: BackupV2RuntimeServices
    let month: LibraryMonthKey

    func run() async throws -> RepoCheckpointBarrierHookResult {
        let mode = services.retentionRuntimeMode
        guard mode.checkpointBarrierHook, mode.barrierAwareSessionRefresh else {
            throw RepoCheckpointBarrierHookError.invalidRuntimeMode
        }

        let checkpoint = try await RepoCheckpointService(
            client: services.metadataClient,
            basePath: services.basePath,
            repoID: services.repoID,
            writerID: services.writerID,
            runID: services.runID,
            clock: services.lamport,
            policy: mode.compactionPolicy
        ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true)

        switch checkpoint.outcome {
        case .skippedEmptyFold:
            return RepoCheckpointBarrierHookResult(
                outcome: .skippedEmptyFold,
                month: month,
                checkpoint: checkpoint,
                barrier: nil
            )
        case .skippedBelowThreshold:
            return RepoCheckpointBarrierHookResult(
                outcome: .skippedBelowThreshold,
                month: month,
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
                policy: mode.compactionPolicy
            ).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            let outcome: RepoCheckpointBarrierHookResult.Outcome
            switch barrier.writeOutcome {
            case .wroteVerified:
                outcome = .checkpointWrittenBarrierPublished
            case .alreadyExistedSameBytes:
                outcome = .checkpointWrittenBarrierAlreadyExisted
            }
            return RepoCheckpointBarrierHookResult(
                outcome: outcome,
                month: month,
                checkpoint: checkpoint,
                barrier: barrier
            )
        }
    }
}
