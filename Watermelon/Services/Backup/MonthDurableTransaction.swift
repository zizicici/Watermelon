import Foundation

/// W1: the single per-month authority for durable-commit outcome handling, hash-index side-effect
/// drain, committed-view publish, and hard-abort cleanup. Foreground constructs it with an
/// aggregator; background passes `aggregator == nil` (no progress counters). The lifecycle is an
/// explicit state machine, so illegal/missing transitions surface to callers and tests instead of
/// silently mis-ordering drain and publish:
///
///   pending --begin--> commitDurable --drain--> sideEffectsDrained --publish--> published
///
/// `beginCommitDurable` (re)starts a cycle — foreground runs one cycle per interval flush plus one
/// at end-of-month, so it is legal from any state. `abort` is always available and only ever rolls
/// back non-durable state: rows already published into the durable committed view survive it.
final class MonthDurableTransaction {
    enum State: Equatable {
        case pending
        case commitDurable
        case sideEffectsDrained
        case published
    }

    enum LifecycleError: Error, Equatable {
        /// `drainSideEffects` before a durable commit outcome was recorded.
        case drainRequiresDurableCommit
        /// `publishCommittedView` before side effects were drained.
        case publishRequiresDrainedSideEffects
    }

    let aggregator: ParallelBackupProgressAggregator?
    let assetProcessor: AssetProcessor
    let eventStream: BackupEventStream
    let profile: ServerProfileRecord
    let month: LibraryMonthKey
    let workerID: Int

    private(set) var state: State = .pending
    private var durableOutcome: V2MonthFlushOutcome?

    init(
        aggregator: ParallelBackupProgressAggregator?,
        assetProcessor: AssetProcessor,
        eventStream: BackupEventStream,
        profile: ServerProfileRecord,
        month: LibraryMonthKey,
        workerID: Int
    ) {
        self.aggregator = aggregator
        self.assetProcessor = assetProcessor
        self.eventStream = eventStream
        self.profile = profile
        self.month = month
        self.workerID = workerID
    }

    private var remoteIndexService: RemoteIndexSyncService { assetProcessor.remoteIndexService }

    /// Records one flush's durable outcome and (re)starts a cycle. Legal from any state: a new flush
    /// can begin after a prior cycle published, or after a deferred partial left it drained.
    func beginCommitDurable(outcome: V2MonthFlushOutcome) {
        durableOutcome = outcome
        state = .commitDurable
    }

    /// commitDurable -> sideEffectsDrained. Drains queued hash-index intents for the committed delta
    /// and (foreground) marks the aggregator batch durable. Returns the drain outcome so a background
    /// caller can log a partial drain; foreground logs internally and the return is unused.
    @discardableResult
    func drainSideEffects() async throws -> HashIndexDrainOutcome? {
        guard state == .commitDurable, let outcome = durableOutcome else {
            throw LifecycleError.drainRequiresDurableCommit
        }
        let drainOutcome: HashIndexDrainOutcome?
        if let aggregator {
            await BackupParallelExecutor.applyDurableBatchSideEffects(
                aggregator: aggregator,
                assetProcessor: assetProcessor,
                month: month,
                outcome: outcome,
                eventStream: eventStream,
                profile: profile,
                workerID: workerID
            )
            drainOutcome = nil
        } else {
            drainOutcome = await BackupParallelExecutor.drainHashIndexIntentsForDurableFlush(
                assetProcessor: assetProcessor,
                month: month,
                outcome: outcome
            )
        }
        state = .sideEffectsDrained
        return drainOutcome
    }

    /// sideEffectsDrained -> published. Publishes the durable committed snapshot, gated so a month
    /// still carrying uncommitted V2 ops (partial multi-chunk) never publishes non-durable rows.
    /// Returns whether rows were actually published.
    @discardableResult
    func publishCommittedView(monthStore: any BackupMonthStore) throws -> Bool {
        guard state == .sideEffectsDrained, let outcome = durableOutcome else {
            throw LifecycleError.publishRequiresDrainedSideEffects
        }
        let didPublish = BackupParallelExecutor.publishDefensiveFlushSnapshotIfNeeded(
            monthStore: monthStore,
            month: month,
            remoteIndexService: remoteIndexService,
            delta: outcome.delta
        )
        state = .published
        return didPublish
    }

    /// Hard abort: roll back non-durable provisional/intent state and drop the optimistic month
    /// overlay. A no-op for durable rows already published into the committed view (only the session
    /// overlay is dropped). Always legal — including after `published`, where it leaves the durable
    /// rows intact and only discards non-durable work accumulated since.
    func abort() async {
        if let aggregator {
            await BackupParallelExecutor.rollBackProvisionalAndIntentsForHardAbort(
                aggregator: aggregator,
                assetProcessor: assetProcessor,
                month: month
            )
        } else {
            await assetProcessor.clearPendingHashIndexIntents(month: month)
            MonthOverlayCoordinator(remoteIndexService: remoteIndexService).onHardAbort(month: month)
        }
        durableOutcome = nil
        state = .pending
    }
}
