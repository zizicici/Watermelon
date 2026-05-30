import Foundation

/// Arch-VII A-II B4: binds the per-month durable-commit side-effect pipeline and the hard-abort
/// rollback into one context. It does not change WHEN/WHETHER anything fires — both methods forward
/// to the unchanged `BackupParallelExecutor` static helpers, preserving call order and fingerprints.
struct MonthCommitTransaction {
    let aggregator: ParallelBackupProgressAggregator
    let assetProcessor: AssetProcessor
    let eventStream: BackupEventStream
    let profile: ServerProfileRecord
    let month: LibraryMonthKey
    let workerID: Int

    /// Fixed pipeline: hash-index drain → partial-drain warning → aggregator.markBatchDurable.
    func applyDurableSideEffects(outcome: V2MonthFlushOutcome) async {
        await BackupParallelExecutor.applyDurableBatchSideEffects(
            aggregator: aggregator,
            assetProcessor: assetProcessor,
            month: month,
            outcome: outcome,
            eventStream: eventStream,
            profile: profile,
            workerID: workerID
        )
    }

    /// Abort → rollback: revert provisional counters, discard matching intents, drop optimistic overlay.
    func abort() async {
        await BackupParallelExecutor.rollBackProvisionalAndIntentsForHardAbort(
            aggregator: aggregator,
            assetProcessor: assetProcessor,
            month: month
        )
    }
}
