import XCTest
@testable import Watermelon

// Lease-failure routing classifiers. The loop wiring that consumes these is coupled to the photo library
// (PreparedResource wraps a real PHAsset), so the decisions are extracted and unit-tested here while the
// call-site wiring stays small:
// - AssetProcessor+Upload.performUploadWithRetry rethrows before any retry/sleep/name-collision branch.
// - BackupParallelExecutor stops the month queue and fails closed on in-run lease loss.
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
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(LiteRepoError.ownLockConflict()),
                       "a retry-later self lock is not an in-run lease loss")
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

    func testProbeFaultDescriptionDoesNotExposeTerminalDetail() {
        let error = LiteRepoError.probeFault(
            .terminal,
            detail: "S3 request HEAD failed (403): https://bucket.example/.watermelon/version.json"
        )
        let message = error.localizedDescription

        XCTAssertTrue(message.contains("terminal"))
        XCTAssertFalse(message.contains("https://"))
        XCTAssertFalse(message.contains("bucket.example"))
        XCTAssertFalse(message.contains("version.json"))
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
            ("ownLockConflict", .ownLockConflict(), skippableLockConflict),
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
        for error in [LiteRepoError.lockConflict, .ownLockConflict()] {
            XCTAssertFalse(error.isUploadFailFast)
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

    func testStartWindowPausePreservesRunConfigurationForResume() {
        // A pause that cancels the start command before startRun runs must keep the resolved run configuration,
        // else resume falls back to .disable and skips iCloud-only assets the user opted to back up.
        var state = BackupSessionState()
        let scope: Set<String> = ["a"]
        let configuration = BackupRunConfigurationOverride(workerCountOverride: 1, iCloudPhotoBackupMode: .enable)
        _ = state.prepareForStart(mode: .scoped(assetIDs: scope), configuration: configuration)

        // Pause arrives while the start command is in flight (startRun never populated the driver).
        state.beginPauseRequest()
        state.resolveStartCancellation(mode: .scoped(assetIDs: scope))

        XCTAssertEqual(state.state, .paused)
        XCTAssertEqual(state.lastRunConfiguration?.iCloudPhotoBackupMode, .enable,
                       "resume must reuse the iCloud-originals mode captured at start")
        XCTAssertEqual(state.lastRunConfiguration?.workerCountOverride, 1,
                       "resume must reuse the forced worker override captured at start")
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

    func testErrorInducedPauseDropsUncommittedAssetsFromResumeSet() {
        // A dirty month whose flush throws (sibling lease-fail-fast stop / own fatal / non-read-back error) emits
        // no .uploadFailed; a coincident user pause makes applyRunError reduce the thrown run to .paused. Its
        // uploaded-but-unpublished assets must not stay resume-complete, else BackupResumePlanner skips them.
        var state = BackupSessionState()
        let scope: Set<String> = ["a", "b"]
        _ = state.prepareForStart(mode: .scoped(assetIDs: scope))
        state.completeAcceptedStartLaunch()

        _ = state.reduce(event: Self.successProgress(assetID: "a", succeeded: 1, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        _ = state.reduce(event: Self.successProgress(assetID: "b", succeeded: 2, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        XCTAssertEqual(state.completedAssetIDsForResume, scope)

        state.applyRunError(
            LiteRepoError.ownershipLost,
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope),
            externalUnavailable: false,
            intent: .pause,
            phaseBeforeFailure: .pausing
        )

        XCTAssertEqual(state.state, .paused)
        XCTAssertTrue(state.completedAssetIDsForResume.isEmpty,
                      "an error-induced pause must not leave uncommitted-month assets resume-complete")
        XCTAssertEqual(scope.subtracting(state.completedAssetIDsForResume), scope,
                       "BackupResumePlanner replans the full paused scope")
    }

    func testCancelledFaultPauseDropsUncommittedAssetsFromResumeSet() {
        // Same clearing must happen when the throw is a bare cancellation fault (intent .none, fault .cancelled).
        var state = BackupSessionState()
        let scope: Set<String> = ["a"]
        _ = state.prepareForStart(mode: .scoped(assetIDs: scope))
        state.completeAcceptedStartLaunch()
        _ = state.reduce(event: Self.successProgress(assetID: "a", succeeded: 1, total: 1),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        XCTAssertEqual(state.completedAssetIDsForResume, scope)

        state.applyRunError(
            LiteRepoError.probeFault(.cancelled),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope),
            externalUnavailable: false,
            intent: .none,
            phaseBeforeFailure: .idle
        )

        XCTAssertEqual(state.state, .paused)
        XCTAssertTrue(state.completedAssetIDsForResume.isEmpty)
    }

    func testManifestReadBackFailureDuringPauseUncompletesAssetsForResume() {
        var state = BackupSessionState()
        let pausedScope: Set<String> = ["a", "b"]
        _ = state.prepareForStart(mode: .scoped(assetIDs: pausedScope))
        state.completeAcceptedStartLaunch()

        _ = state.reduce(event: Self.successProgress(assetID: "a", succeeded: 1, total: 2),
                         runMode: .scoped(assetIDs: pausedScope),
                         displayMode: .scoped(assetIDs: pausedScope), terminalIntent: .none)
        _ = state.reduce(event: Self.successProgress(assetID: "b", succeeded: 2, total: 2),
                         runMode: .scoped(assetIDs: pausedScope),
                         displayMode: .scoped(assetIDs: pausedScope), terminalIntent: .none)
        XCTAssertEqual(state.completedAssetIDsForResume, pausedScope)

        // Pause-time month manifest read-back failure: un-complete for resume, no surfaced failure count.
        _ = state.reduce(event: .monthChanged(MonthChangeEvent(
            year: 2024, month: 6,
            action: .uploadFailed(resumableAssetLocalIdentifiers: pausedScope, failedItemCount: 0))),
            runMode: .scoped(assetIDs: pausedScope),
            displayMode: .scoped(assetIDs: pausedScope), terminalIntent: .none)

        _ = state.reduce(event: .finished(BackupExecutionResult(
            total: 2, succeeded: 2, failed: 0, skipped: 0, paused: true)),
            runMode: .scoped(assetIDs: pausedScope),
            displayMode: .scoped(assetIDs: pausedScope), terminalIntent: .none)

        XCTAssertEqual(state.state, .paused)
        XCTAssertTrue(state.completedAssetIDsForResume.isEmpty,
                      "assets whose month manifest never committed must not be skipped on resume")
        XCTAssertTrue(state.failedCountByMonth.isEmpty,
                      "a pause-time read-back failure must not leave a stale partial flag that survives resume")
        // BackupResumePlanner subtracts completedAssetIDs, so all paused-scope assets are replanned.
        XCTAssertEqual(pausedScope.subtracting(state.completedAssetIDsForResume), pausedScope)
    }

    func testManifestReadBackFailureOnFinishReportsMonthFailedNotCompleted() {
        var state = BackupSessionState()
        let scope: Set<String> = ["a", "b"]
        _ = state.prepareForStart(mode: .scoped(assetIDs: scope))
        state.completeAcceptedStartLaunch()

        _ = state.reduce(event: Self.successProgress(assetID: "a", succeeded: 1, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        _ = state.reduce(event: Self.successProgress(assetID: "b", succeeded: 2, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)

        let month = LibraryMonthKey(year: 2024, month: 6)
        _ = state.reduce(event: .monthChanged(MonthChangeEvent(
            year: 2024, month: 6,
            action: .uploadFailed(resumableAssetLocalIdentifiers: scope, failedItemCount: 2))),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope), terminalIntent: .none)

        XCTAssertEqual(state.failedCountByMonth[month], 2)
        XCTAssertFalse(state.completedMonths.contains(month),
                       "a read-back-failed month must never be reported completed")

        _ = state.reduce(event: .finished(BackupExecutionResult(
            total: 2, succeeded: 0, failed: 2, skipped: 0, paused: false)),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope), terminalIntent: .none)

        let snapshot = state.snapshot()
        XCTAssertEqual(state.state, .completed)
        XCTAssertEqual(snapshot.failedCountByMonth[month], 2,
                       "Home consumes failedCountByMonth to mark the month partiallyFailed instead of completed")
        XCTAssertFalse(snapshot.completedMonths.contains(month))
    }

    func testResultDirtiedMonthManifestTracksReusedResourceSkips() {
        // `.success` and reused-resource `.skipped` paths upsert month-manifest rows (AssetProcessor); other
        // skips/failures write none, so only the former are at risk on a failed month flush.
        XCTAssertTrue(BackupParallelExecutor.resultDirtiedMonthManifest(status: .success, reason: nil))
        XCTAssertTrue(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: "resources_reused"))
        XCTAssertTrue(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: "resources_reused_cached"))
        XCTAssertFalse(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: "asset_exists_cached"))
        XCTAssertFalse(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: "icloud_photo_backup_disabled"))
        XCTAssertFalse(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: "asset_gone"))
        XCTAssertFalse(BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: nil))
        XCTAssertFalse(BackupParallelExecutor.resultDirtiedMonthManifest(status: .failed, reason: "boom"))
    }

    func testShouldPauseUncommittedMonthOnCancellation() {
        // A user pause/stop (cancellation) over an uncommitted dirty month must pause-continue (so the
        // un-completion event is reliably reduced), not throw; a non-cancelled failure or a clean month does not.
        XCTAssertTrue(BackupParallelExecutor.shouldPauseUncommittedMonthOnCancellation(cancelled: true, hadDirtyManifest: true))
        XCTAssertFalse(BackupParallelExecutor.shouldPauseUncommittedMonthOnCancellation(cancelled: false, hadDirtyManifest: true))
        XCTAssertFalse(BackupParallelExecutor.shouldPauseUncommittedMonthOnCancellation(cancelled: true, hadDirtyManifest: false))
        XCTAssertFalse(BackupParallelExecutor.shouldPauseUncommittedMonthOnCancellation(cancelled: false, hadDirtyManifest: false))
    }

    func testFlushFailureResumeUnmarkCountDropsEveryNonThrowingUncommittedMonth() {
        // The sibling-stop continue path must un-mark its dirtied assets (count 0, not reported failed), else a
        // `.finished(paused:)` settlement skips them on resume; throwing exits emit nothing (applyRunError clears).
        XCTAssertEqual(
            BackupParallelExecutor.flushFailureResumeUnmarkCount(.continueWithoutFinishing, dirtyAssetCount: 3),
            0
        )
        XCTAssertEqual(
            BackupParallelExecutor.flushFailureResumeUnmarkCount(.recordMonthFailure, dirtyAssetCount: 3),
            3
        )
        XCTAssertNil(BackupParallelExecutor.flushFailureResumeUnmarkCount(.deferToOwnFatal, dirtyAssetCount: 3))
        XCTAssertNil(BackupParallelExecutor.flushFailureResumeUnmarkCount(.throwFlushError, dirtyAssetCount: 3))
    }

    func testCleanPauseDropsSiblingContinueWithoutFinishingMonthFromResumeSet() {
        // Multi-worker clean pause: worker B's rerouted dirty month and worker A's `.continueWithoutFinishing`
        // dirty month both emit `.uploadFailed(…, 0)`, then the run settles `.finished(paused: true)` (no throw
        // → not applyRunError). Both months' uncommitted assets must drop so BackupResumePlanner replans them.
        var state = BackupSessionState()
        let scope: Set<String> = ["x", "y"]
        _ = state.prepareForStart(mode: .scoped(assetIDs: scope))
        state.completeAcceptedStartLaunch()

        _ = state.reduce(event: Self.successProgress(assetID: "x", succeeded: 1, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        _ = state.reduce(event: Self.successProgress(assetID: "y", succeeded: 2, total: 2),
                         runMode: .scoped(assetIDs: scope),
                         displayMode: .scoped(assetIDs: scope), terminalIntent: .none)
        XCTAssertEqual(state.completedAssetIDsForResume, scope)

        // Rerouted sibling month (Y) and the fixed `.continueWithoutFinishing` month (X) each un-mark, count 0.
        _ = state.reduce(event: .monthChanged(MonthChangeEvent(
            year: 2024, month: 6,
            action: .uploadFailed(resumableAssetLocalIdentifiers: ["y"], failedItemCount: 0))),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope), terminalIntent: .pause)
        _ = state.reduce(event: .monthChanged(MonthChangeEvent(
            year: 2024, month: 6,
            action: .uploadFailed(resumableAssetLocalIdentifiers: ["x"], failedItemCount: 0))),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope), terminalIntent: .pause)
        _ = state.reduce(event: .finished(BackupExecutionResult(
            total: 2, succeeded: 2, failed: 0, skipped: 0, paused: true)),
            runMode: .scoped(assetIDs: scope),
            displayMode: .scoped(assetIDs: scope), terminalIntent: .pause)

        XCTAssertEqual(state.state, .paused)
        XCTAssertTrue(state.completedAssetIDsForResume.isEmpty,
                      "every uncommitted dirty month must drop on the clean-pause `.finished(paused:)` settlement")
        XCTAssertTrue(state.failedCountByMonth.isEmpty,
                      "an interrupted (not failed) month must not inflate the failure count")
        XCTAssertEqual(scope.subtracting(state.completedAssetIDsForResume), scope)
    }

    private static func successProgress(assetID: String, succeeded: Int, total: Int) -> BackupEvent {
        .progress(BackupProgress(
            succeeded: succeeded, failed: 0, skipped: 0, total: total,
            message: "", logMessage: nil, logLevel: .info,
            itemEvent: BackupItemEvent(
                assetLocalIdentifier: assetID,
                assetFingerprint: nil,
                displayName: assetID,
                resourceDate: Self.june2024,
                status: .success,
                reason: nil,
                updatedAt: Date()
            ),
            transferState: nil
        ))
    }

    private static let june2024: Date = {
        var comps = DateComponents()
        comps.year = 2024
        comps.month = 6
        comps.day = 15
        return Calendar(identifier: .gregorian).date(from: comps) ?? Date(timeIntervalSince1970: 0)
    }()

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
