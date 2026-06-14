import XCTest
@testable import Watermelon

// Lease-failure routing classifiers. The loop wiring that consumes these is coupled to the photo library
// (PreparedResource wraps a real PHAsset; BackgroundBackupRunner fetches PHAssets), so the decisions are
// extracted and unit-tested here while the call-site wiring stays small:
// - AssetProcessor+Upload.performUploadWithRetry rethrows before any retry/sleep/name-collision branch.
// - BackgroundBackupRunner.runBackupLoop stops asset processing but still reaches the month-end flush.
final class LiteLeaseFailFastTests: XCTestCase {
    private typealias Disposition = LiteRepoError.Disposition

    func testUploadFailFastForLeaseAndOwnershipErrors() {
        XCTAssertTrue(AssetProcessor.isLeaseFailFast(LiteRepoError.leaseConfidenceLost))
        XCTAssertTrue(AssetProcessor.isLeaseFailFast(LiteRepoError.ownershipLost))
    }

    func testUploadDoesNotFailFastForRetryableTerminalOrCancelled() {
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(RemoteErrorFixtures.retryable))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(RemoteErrorFixtures.terminal))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(CancellationError()))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(LiteRepoError.lockConflict),
                       "only lease/ownership loss is fail-fast, not every Lite error")
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(LiteRepoError.ownLockConflict),
                       "a retry-later self lock is not an in-run lease loss")
    }

    func testBackgroundRunFatalForLeaseAndOwnershipLoss() {
        XCTAssertTrue(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.ownershipLost))
        XCTAssertTrue(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.leaseConfidenceLost))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(RemoteErrorFixtures.retryable))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(CancellationError()))
    }

    func testBackgroundLeaseFatalAssetFaultKeepsMonthEndFlushReachable() {
        XCTAssertTrue(BackgroundBackupRunner.shouldAttemptMonthEndFlushAfterAssetFault(LiteRepoError.ownershipLost))
        XCTAssertTrue(BackgroundBackupRunner.shouldAttemptMonthEndFlushAfterAssetFault(LiteRepoError.leaseConfidenceLost))
        XCTAssertFalse(BackgroundBackupRunner.shouldAttemptMonthEndFlushAfterAssetFault(RemoteErrorFixtures.retryable))
        XCTAssertFalse(BackgroundBackupRunner.shouldAttemptMonthEndFlushAfterAssetFault(CancellationError()))
    }

    func testExecutorContinuesOnlyAfterManifestReadBackVerifyFlushFailure() {
        let readBackFailure = NSError(domain: "MonthManifestStore", code: -36)
        XCTAssertTrue(MonthManifestStore.isReadBackVerificationError(readBackFailure))
        XCTAssertTrue(BackupParallelExecutor.shouldContinueAfterManifestFlushFailure(readBackFailure))
        XCTAssertFalse(BackupParallelExecutor.shouldContinueAfterManifestFlushFailure(LiteRepoError.ownershipLost))
        XCTAssertFalse(BackupParallelExecutor.shouldContinueAfterManifestFlushFailure(RemoteErrorFixtures.retryable))
    }

    func testExecutorRecordsReadBackFailureOnlyWhenMonthWouldFinish() {
        let readBackFailure = NSError(domain: "MonthManifestStore", code: -36)
        XCTAssertTrue(BackupParallelExecutor.shouldRecordManifestReadBackFailure(
            disposition: .finish,
            error: readBackFailure
        ))
        XCTAssertFalse(BackupParallelExecutor.shouldRecordManifestReadBackFailure(
            disposition: .paused,
            error: readBackFailure
        ))
        XCTAssertFalse(BackupParallelExecutor.shouldRecordManifestReadBackFailure(
            disposition: .finish,
            error: LiteRepoError.ownershipLost
        ))
    }

    func testExecutorFlushFailureDispositionKeepsRunFatalOrdering() {
        let readBackFailure = NSError(domain: "MonthManifestStore", code: -36)
        let moveFailure = RemoteErrorFixtures.retryable

        XCTAssertEqual(
            BackupParallelExecutor.manifestFlushFailureDisposition(
                completion: .ownFatal,
                error: moveFailure
            ),
            .deferToOwnFatal
        )
        XCTAssertEqual(
            BackupParallelExecutor.manifestFlushFailureDisposition(
                completion: .stoppedByRunFatal,
                error: moveFailure
            ),
            .continueWithoutFinishing
        )
        XCTAssertEqual(
            BackupParallelExecutor.manifestFlushFailureDisposition(
                completion: .stoppedByRunFatal,
                error: readBackFailure
            ),
            .continueWithoutFinishing
        )
        XCTAssertEqual(
            BackupParallelExecutor.manifestFlushFailureDisposition(
                completion: .finish,
                error: readBackFailure
            ),
            .recordMonthFailure
        )
        XCTAssertEqual(
            BackupParallelExecutor.manifestFlushFailureDisposition(
                completion: .finish,
                error: moveFailure
            ),
            .throwFlushError
        )
    }

    func testExecutorDoesNotFinishMonthAfterSiblingFatalStop() {
        XCTAssertEqual(BackupParallelExecutor.monthCompletionDisposition(
            paused: false,
            hasMonthFatalError: false,
            queueStopped: true
        ), .stoppedByRunFatal)
        XCTAssertEqual(BackupParallelExecutor.monthCompletionDisposition(
            paused: false,
            hasMonthFatalError: false,
            queueStopped: false
        ), .finish)
        XCTAssertEqual(BackupParallelExecutor.monthCompletionDisposition(
            paused: false,
            hasMonthFatalError: true,
            queueStopped: false
        ), .ownFatal)
        XCTAssertEqual(BackupParallelExecutor.monthCompletionDisposition(
            paused: true,
            hasMonthFatalError: false,
            queueStopped: false
        ), .paused)
    }

    func testExecutorDoesNotFinishOrRecordReadBackFailureWhenStopArrivesAfterFlush() {
        let readBackFailure = NSError(domain: "MonthManifestStore", code: -36)
        let beforeFlush = BackupParallelExecutor.monthCompletionDisposition(
            paused: false,
            hasMonthFatalError: false,
            queueStopped: false
        )
        let afterFlush = BackupParallelExecutor.monthCompletionDisposition(
            paused: false,
            hasMonthFatalError: false,
            queueStopped: true
        )

        XCTAssertEqual(beforeFlush, .finish)
        XCTAssertEqual(afterFlush, .stoppedByRunFatal)
        XCTAssertFalse(BackupParallelExecutor.shouldRecordManifestReadBackFailure(
            disposition: afterFlush,
            error: readBackFailure
        ))
    }

    func testMonthWorkQueueStopPreventsNewClaims() async {
        let months = [
            MonthWorkItem(month: MonthKey(year: 2024, month: 1), assetLocalIdentifiers: ["a"], estimatedBytes: 1),
            MonthWorkItem(month: MonthKey(year: 2024, month: 2), assetLocalIdentifiers: ["b"], estimatedBytes: 1)
        ]
        let queue = MonthWorkQueue(months: months)

        let first = await queue.next()
        await queue.stop()
        let second = await queue.next()
        let stopped = await queue.isStopped()

        XCTAssertEqual(first?.month, MonthKey(year: 2024, month: 1))
        XCTAssertNil(second, "lease-fatal stop must prevent sibling workers from claiming more months")
        XCTAssertTrue(stopped)
    }

    func testFinalizationFailureConvertsOnlyCurrentMonthSuccessCounts() async {
        let aggregator = ParallelBackupProgressAggregator(total: 5)
        _ = await aggregator.record(result: Self.result(.success))
        _ = await aggregator.record(result: Self.result(.skipped))
        _ = await aggregator.record(result: Self.result(.success))

        let progress = await aggregator.recordFinalizationFailure(
            BackupMonthProgressCounts(succeeded: 1, skipped: 1, failed: 0)
        )

        XCTAssertEqual(progress.state.total, 5)
        XCTAssertEqual(progress.state.succeeded, 1)
        XCTAssertEqual(progress.state.skipped, 1)
        XCTAssertEqual(progress.state.failed, 1)
        XCTAssertEqual(progress.state.processed, 3)
    }

    func testFinalizationFailureLeavesPrecoveredSkippedAssetsAndAddsMonthFailure() async {
        let aggregator = ParallelBackupProgressAggregator(total: 5)
        _ = await aggregator.recordMonthSkipped(count: 5)

        let progress = await aggregator.recordFinalizationFailure(
            BackupMonthProgressCounts(succeeded: 0, skipped: 5, failed: 0)
        )

        XCTAssertEqual(progress.state.total, 6)
        XCTAssertEqual(progress.state.succeeded, 0)
        XCTAssertEqual(progress.state.skipped, 5)
        XCTAssertEqual(progress.state.failed, 1)
        XCTAssertEqual(progress.state.processed, 6)
    }

    func testLiteRepoCancellationFaultsClassifyAsCancelled() {
        XCTAssertTrue(LiteRepoError.probeFault(.cancelled).isCancellation)
        XCTAssertTrue(LiteRepoError.lockFault(.cancelled).isCancellation)
        XCTAssertEqual(RemoteFaultLite.classify(LiteRepoError.probeFault(.cancelled)), .cancelled)
        XCTAssertEqual(RemoteFaultLite.classify(LiteRepoError.lockFault(.cancelled)), .cancelled)
        XCTAssertEqual(RemoteFaultLite.classify(LiteRepoError.probeFault(.retryable)), .terminal)
    }

    func testLiteRepoErrorDispositionMapsEveryCurrentCase() {
        let abort = Disposition(
            isCancellation: false,
            isLeaseOwnershipLoss: false,
            shouldAbortRemoteIndexSync: true,
            shouldContinueDownloadVerify: false
        )
        let leaseLoss = Disposition(
            isCancellation: false,
            isLeaseOwnershipLoss: true,
            shouldAbortRemoteIndexSync: true,
            shouldContinueDownloadVerify: false
        )
        let retryableProbeOrLock = Disposition(
            isCancellation: false,
            isLeaseOwnershipLoss: false,
            shouldAbortRemoteIndexSync: true,
            shouldContinueDownloadVerify: true
        )
        let cancelledProbeOrLock = Disposition(
            isCancellation: true,
            isLeaseOwnershipLoss: false,
            shouldAbortRemoteIndexSync: true,
            shouldContinueDownloadVerify: false
        )
        let skippableLockConflict = Disposition(
            isCancellation: false,
            isLeaseOwnershipLoss: false,
            shouldAbortRemoteIndexSync: true,
            shouldContinueDownloadVerify: true
        )

        let cases: [(String, LiteRepoError, Disposition)] = [
            ("repoDamaged", .repoDamaged, abort),
            ("repoUnsupported", .repoUnsupported(minAppVersion: nil), abort),
            ("repoMaintenanceUnavailable", .repoMaintenanceUnavailable, abort),
            ("probeFault.notFound", .probeFault(.notFound), abort),
            ("probeFault.retryable", .probeFault(.retryable), retryableProbeOrLock),
            ("probeFault.cancelled", .probeFault(.cancelled), cancelledProbeOrLock),
            ("probeFault.terminal", .probeFault(.terminal), abort),
            ("lockConflict", .lockConflict, skippableLockConflict),
            ("ownLockConflict", .ownLockConflict, skippableLockConflict),
            ("lockFault.notFound", .lockFault(.notFound), abort),
            ("lockFault.retryable", .lockFault(.retryable), retryableProbeOrLock),
            ("lockFault.cancelled", .lockFault(.cancelled), cancelledProbeOrLock),
            ("lockFault.terminal", .lockFault(.terminal), abort),
            ("writerIdentityUnavailable", .writerIdentityUnavailable, abort),
            ("versionCommitFailed", .versionCommitFailed, abort),
            ("leaseConfidenceLost", .leaseConfidenceLost, leaseLoss),
            ("ownershipLost", .ownershipLost, leaseLoss),
            ("existingLiteManifestConflict", .existingLiteManifestConflict(month: "2026-06"), abort),
            ("v1MonthManifestUnreadable", .v1MonthManifestUnreadable(month: "2026-06"), abort),
            ("v1SourceChangedDuringMigration", .v1SourceChangedDuringMigration, abort)
        ]

        for (name, error, expected) in cases {
            XCTAssertEqual(error.disposition, expected, name)
            XCTAssertEqual(error.isCancellation, expected.isCancellation, name)
            XCTAssertEqual(error.isLeaseOwnershipLoss, expected.isLeaseOwnershipLoss, name)
            XCTAssertEqual(error.isUploadFailFast, expected.isUploadFailFast, name)
            XCTAssertEqual(error.isBackgroundRunFatal, expected.isBackgroundRunFatal, name)
            XCTAssertEqual(
                error.preservesOriginalDuringVersionCommit,
                expected.preservesOriginalDuringVersionCommit,
                name
            )
            XCTAssertEqual(error.shouldAbortRemoteIndexSync, expected.shouldAbortRemoteIndexSync, name)
            XCTAssertEqual(error.shouldContinueDownloadVerify, expected.shouldContinueDownloadVerify, name)
        }
    }

    func testLiteRepoLockConflictsAbortRemoteIndexSyncButContinueDownloadVerify() {
        for error in [LiteRepoError.lockConflict, .ownLockConflict] {
            XCTAssertFalse(error.isUploadFailFast)
            XCTAssertFalse(error.isBackgroundRunFatal)
            XCTAssertFalse(error.preservesOriginalDuringVersionCommit)
            XCTAssertTrue(error.shouldAbortRemoteIndexSync)
            XCTAssertTrue(error.shouldContinueDownloadVerify)
        }
    }

    func testDownloadVerifyContinuesAfterSnapshotIndependentFailures() {
        // Only the transient missing-manifest signal (-1) keeps last-known-good cache, so it stays continuable.
        let transientMissingManifest = NSError(domain: "RemoteIndexSyncService", code: -1)

        XCTAssertTrue(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(transientMissingManifest))
        XCTAssertTrue(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.lockConflict))
        XCTAssertTrue(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(RemoteErrorFixtures.retryable))
    }

    // A verify that proved the canonical absent (confirmed -2, evicted) or corrupt (-34/-35, evicted) must fail
    // the month closed: continuing would read the cache the verify just evicted and falsely complete the month
    // with an empty restore, masking remote deletion/corruption.
    func testDownloadVerifyFailsClosedForConfirmedAbsentOrCorruptManifest() {
        let confirmedAbsentManifest = NSError(domain: "RemoteIndexSyncService", code: -2)
        let corruptDownloadedManifest = NSError(domain: "MonthManifestStore", code: -34)
        let incompatibleDownloadedManifest = NSError(domain: "MonthManifestStore", code: -35)

        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(confirmedAbsentManifest))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(corruptDownloadedManifest))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(incompatibleDownloadedManifest))
    }

    // A whole-repo format failure from download-verify (repoDamaged — e.g. a directory-only V1 candidate now
    // routed .damaged — and its siblings) must fail the month closed, never continue to a stale-snapshot
    // download that would mask the damaged control state and falsely complete the month.
    func testDownloadVerifyFailsClosedForDamagedRepoFormat() {
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.repoDamaged))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.repoUnsupported(minAppVersion: nil)))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.repoMaintenanceUnavailable))
    }

    func testDownloadVerifyStillFailsFastForCancellationAndOwnershipLoss() {
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(CancellationError()))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.leaseConfidenceLost))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(LiteRepoError.ownershipLost))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(RemoteErrorFixtures.terminal))
        XCTAssertFalse(HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(NSError(domain: "MonthManifestStore", code: -32)))
    }

    func testWrappedCancellationRunErrorPausesInsteadOfFailing() {
        var state = BackupSessionState()
        state.prepareForStart(mode: .scoped(assetIDs: ["a"]))
        state.completeAcceptedStartLaunch()

        state.applyRunError(
            LiteRepoError.probeFault(.cancelled),
            runMode: .scoped(assetIDs: ["a"]),
            displayMode: .scoped(assetIDs: ["a"]),
            externalUnavailable: false,
            intent: .none,
            phaseBeforeFailure: .idle
        )

        XCTAssertEqual(state.state, .paused)
        XCTAssertEqual(state.controlPhase, .idle)
        XCTAssertNotNil(state.lastPausedRunMode)
    }

    private static func result(_ status: BackupItemStatus) -> AssetProcessResult {
        AssetProcessResult(
            status: status,
            reason: nil,
            displayName: "asset",
            assetFingerprint: nil,
            timing: AssetProcessTiming(),
            totalFileSizeBytes: 0,
            uploadedFileSizeBytes: 0
        )
    }
}
