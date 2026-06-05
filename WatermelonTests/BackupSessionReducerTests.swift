import XCTest
@testable import Watermelon

final class BackupSessionReducerTests: XCTestCase {

    // MARK: - resolveStartCancellation

    func testResolveStartCancellation_pausePreservesPendingRunConfiguration() {
        var state = BackupSessionState()
        let config = BackupRunConfigurationOverride(workerCountOverride: 4, iCloudPhotoBackupMode: .enable)
        state.controlPhase = .starting
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = config

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertEqual(state.pendingRunConfiguration?.workerCountOverride, 4)
        XCTAssertEqual(state.pendingRunConfiguration?.iCloudPhotoBackupMode, .enable)
    }

    func testResolveStartCancellation_stopClearsPendingRunConfiguration() {
        var state = BackupSessionState()
        let config = BackupRunConfigurationOverride(workerCountOverride: 4, iCloudPhotoBackupMode: .enable)
        state.controlPhase = .stopping
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = config

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.pendingRunConfiguration)
    }

    func testResolveStartCancellation_pausingPhaseResolvesToPause() {
        var state = BackupSessionState()
        state.controlPhase = .pausing
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 2,
            iCloudPhotoBackupMode: .disable
        )

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertNotNil(state.pendingRunConfiguration)
        if case .full = state.lastPausedRunMode {} else {
            XCTFail("expected .full, got \(state.lastPausedRunMode)")
        }
    }

    // MARK: - prepareForResume

    func testPrepareForResumeClearsStaleMonthCounters() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.failedCountByMonth[month] = 3
        state.processedCountByMonth[month] = 10
        state.incompleteSummaryByMonth[month] = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1)
        )
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertEqual(state.failedCountByMonth[month], 3)
        XCTAssertTrue(state.processedCountByMonth.isEmpty)
        XCTAssertNotNil(state.incompleteSummaryByMonth[month])
        XCTAssertEqual(state.state, .running)
        XCTAssertEqual(state.controlPhase, .resuming)
    }

    // MARK: - resume preserves incomplete summaries

    func testResumePreservesIncompleteSummaryAndFinishedRunReportsPartial() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.incompleteSummaryByMonth[month] = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1)
        )
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertNotNil(state.incompleteSummaryByMonth[month])

        let _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 1, succeeded: 1, failed: 0, skipped: 0, paused: false
            )),
            runMode: .full,
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .completed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.backupCompletedPartial"))
    }

    func testResumeStartedMonthClearsIncompleteSummary() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.incompleteSummaryByMonth[month] = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 2)
        )
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertNotNil(state.incompleteSummaryByMonth[month])

        let _ = state.reduce(
            event: .monthChanged(MonthChangeEvent(
                year: month.year, month: month.month, action: .started
            )),
            runMode: .full,
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertNil(state.incompleteSummaryByMonth[month])
    }

    func testResumeFinishedRunWithPreservedFailedCountReportsPartial() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.failedCountByMonth[month] = 1
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertNotNil(state.failedCountByMonth[month])

        let _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 1, succeeded: 1, failed: 0, skipped: 0, paused: false
            )),
            runMode: .full,
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .completed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.backupCompletedPartial"))
    }

    func testResumeStartedMonthClearsFailedCountByMonth() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.failedCountByMonth[month] = 5
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertEqual(state.failedCountByMonth[month], 5)

        reduce(&state, .monthChanged(MonthChangeEvent(
            year: month.year, month: month.month, action: .started
        )))

        XCTAssertNil(state.failedCountByMonth[month])
    }

    func testResumeNoPendingWorkPreservesFailedCountByMonthInSnapshot() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        state.failed = 1
        state.failedCountByMonth[month] = 1
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()
        state.completeResumeWithoutPendingWork()

        XCTAssertEqual(state.state, .completed)
        let snapshot = state.snapshot()
        XCTAssertEqual(snapshot.failedCountByMonth[month], 1)
    }

    // MARK: - cancelResume

    func testCancelResume_pausePreservesPendingRunConfiguration() {
        var state = BackupSessionState()
        state.controlPhase = .pausing
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 3,
            iCloudPhotoBackupMode: .enable
        )

        state.cancelResume(pausedMode: .full, pausedDisplayMode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertNotNil(state.pendingRunConfiguration)
    }

    func testCancelResume_stopClearsPendingRunConfiguration() {
        var state = BackupSessionState()
        state.controlPhase = .stopping
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 3,
            iCloudPhotoBackupMode: .enable
        )

        state.cancelResume(pausedMode: .full, pausedDisplayMode: .full)

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.pendingRunConfiguration)
    }

    // MARK: - incomplete month summaries

    func testIncompleteMonthSummaryMergesByMaxAndPreservesUploadCompletion() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        let first = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 3, fingerprintMismatchCount: 1)
        )
        let second = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1, fingerprintMismatchCount: 4)
        )

        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .completed)))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(first))))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(first))))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(second))))

        XCTAssertTrue(state.uploadCompletedMonths.contains(month))
        XCTAssertEqual(state.incompleteMonths, [month])
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.downloadIssues.skippedIncompleteCount, 3)
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.downloadIssues.fingerprintMismatchCount, 4)
    }

    func testIncompleteMonthEventRecordsUploadCompletionInExecutorOrder() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 2)
        )

        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .started)))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(summary))))

        XCTAssertTrue(state.uploadCompletedMonths.contains(month))
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.downloadIssues.skippedIncompleteCount, 2)
    }

    func testCompletedMonthEventClearsIncompleteSummary() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)

        reduce(&state, .monthChanged(MonthChangeEvent(
            year: month.year,
            month: month.month,
            action: .incomplete(BackupMonthIncompleteSummary(
                downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 2)
            ))
        )))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .completed)))

        XCTAssertTrue(state.uploadCompletedMonths.contains(month))
        XCTAssertNil(state.incompleteSummaryByMonth[month])
    }

    func testFinishedRunWithDurableSnapshotDeferredUsesPartialStatusText() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)

        reduce(&state, .monthChanged(MonthChangeEvent(
            year: month.year,
            month: month.month,
            action: .uploadDurableSnapshotDeferred(message: "snapshot deferred")
        )))
        reduce(&state, .finished(BackupExecutionResult(
            total: 1,
            succeeded: 1,
            failed: 0,
            skipped: 0,
            paused: false
        )))

        XCTAssertEqual(state.state, .completed)
        XCTAssertTrue(state.uploadCompletedMonths.contains(month))
        XCTAssertEqual(state.uploadSnapshotDeferredMessageByMonth[month], "snapshot deferred")
        XCTAssertEqual(state.statusText, String(localized: "backup.session.backupCompletedPartial"))
    }

    // MARK: - Mixed repair-required terminal behavior (R02)

    /// A clean subset that finishes while non-clean months were routed out must stay explicit
    /// (paused, repair-required) and resumable — never ordinary completion that hides blocked work.
    func testMixedRepairRequired_cleanSubsetCompletion_staysPausedAndResumable() {
        var state = BackupSessionState()
        let blocked = LibraryMonthKey(year: 2026, month: 5)
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()
        state.markResumedRunRepairRequired(months: [blocked])

        _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 2, succeeded: 2, failed: 0, skipped: 0, paused: false
            )),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .paused,
                       "mixed repair-required completion must not become ordinary .completed")
        XCTAssertEqual(state.statusText, String(localized: "backup.session.resumeRepairRequired"))
        if case .full = state.lastPausedRunMode {} else {
            XCTFail("original paused mode must be preserved so blocked work stays resumable, got \(String(describing: state.lastPausedRunMode))")
        }
        XCTAssertTrue(state.resumeRepairRequiredMonths.isEmpty, "consumed by the terminal")
    }

    /// Differential: a clean resume with no routed-out months still completes ordinarily.
    func testNoRepairRequired_cleanResumeCompletion_ordinaryCompleted() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 1, succeeded: 1, failed: 0, skipped: 0, paused: false
            )),
            runMode: .full,
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .completed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.backupCompleted"))
        XCTAssertNil(state.lastPausedRunMode)
    }

    /// An explicit user stop still wins over repair-required surfacing.
    func testRepairRequired_userStopTakesPrecedence() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()
        state.markResumedRunRepairRequired(months: [LibraryMonthKey(year: 2026, month: 5)])

        _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 1, succeeded: 1, failed: 0, skipped: 0, paused: false
            )),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            terminalIntent: .stop
        )

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.lastPausedRunMode)
        XCTAssertTrue(state.resumeRepairRequiredMonths.isEmpty)
    }

    func testPrepareForResume_clearsStaleRepairRequiredMonths() {
        var state = BackupSessionState()
        state.resumeRepairRequiredMonths = [LibraryMonthKey(year: 2026, month: 5)]
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full

        _ = state.prepareForResume()

        XCTAssertTrue(state.resumeRepairRequiredMonths.isEmpty)
    }

    // MARK: - Interrupted mixed repair-required terminals (R03)

    /// An interrupted (pause-classified) mixed resume must preserve the original paused scope, not the
    /// clean subset, so the next resume can re-scan and re-route the non-clean months — never deferring
    /// into ordinary completion.
    func testMixedRepairRequired_finishRunInterruptedPause_preservesOriginalScope() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()
        state.markResumedRunRepairRequired(months: [LibraryMonthKey(year: 2026, month: 5)])

        _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 5, succeeded: 3, failed: 0, skipped: 0, paused: true
            )),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .paused)
        if case .full = state.lastPausedRunMode {} else {
            XCTFail("interrupted mixed resume must preserve original .full scope, got \(String(describing: state.lastPausedRunMode))")
        }
        if case .full = state.lastPausedDisplayRunMode {} else {
            XCTFail("expected original .full display mode, got \(String(describing: state.lastPausedDisplayRunMode))")
        }
    }

    /// Same protection on the `applyRunError` pause/cancellation terminal (transient interruption).
    func testMixedRepairRequired_applyRunErrorPause_preservesOriginalScope() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()
        state.markResumedRunRepairRequired(months: [LibraryMonthKey(year: 2026, month: 5)])

        state.applyRunError(
            CancellationError(),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            externalUnavailable: false,
            intent: .pause,
            phaseBeforeFailure: .pausing
        )

        XCTAssertEqual(state.state, .paused)
        if case .full = state.lastPausedRunMode {} else {
            XCTFail("interrupted mixed resume (applyRunError) must preserve original .full scope, got \(String(describing: state.lastPausedRunMode))")
        }
    }

    /// Stop precedence is unchanged: a user stop on an interrupted mixed run still clears the scope.
    func testMixedRepairRequired_applyRunErrorStop_clearsScope() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()
        state.markResumedRunRepairRequired(months: [LibraryMonthKey(year: 2026, month: 5)])

        state.applyRunError(
            CancellationError(),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            externalUnavailable: false,
            intent: .stop,
            phaseBeforeFailure: .stopping
        )

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.lastPausedRunMode)
    }

    /// Differential: a clean resume with no repair-required months keeps the existing pause narrowing.
    func testCleanResume_finishRunInterruptedPause_keepsNarrowing() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()

        _ = state.reduce(
            event: .finished(BackupExecutionResult(
                total: 5, succeeded: 3, failed: 0, skipped: 0, paused: true
            )),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            terminalIntent: .none
        )

        XCTAssertEqual(state.state, .paused)
        if case .scoped(let ids) = state.lastPausedRunMode {
            XCTAssertEqual(ids, ["clean"])
        } else {
            XCTFail("clean resume must keep narrowing to the scoped subset, got \(String(describing: state.lastPausedRunMode))")
        }
    }

    func testCleanResume_applyRunErrorPause_keepsNarrowing() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()

        state.applyRunError(
            CancellationError(),
            runMode: .scoped(assetIDs: ["clean"]),
            displayMode: .full,
            externalUnavailable: false,
            intent: .pause,
            phaseBeforeFailure: .pausing
        )

        if case .scoped(let ids) = state.lastPausedRunMode {
            XCTAssertEqual(ids, ["clean"])
        } else {
            XCTFail("clean resume (applyRunError) must keep narrowing, got \(String(describing: state.lastPausedRunMode))")
        }
    }

    /// All-blocked resume preparation stays explicit (failed), never ordinary completion.
    func testFailResumePreparation_allBlockedStaysExplicitNotCompleted() {
        var state = BackupSessionState()
        state.lastPausedRunMode = .full
        state.lastPausedDisplayRunMode = .full
        _ = state.prepareForResume()

        state.failResumePreparation()

        XCTAssertEqual(state.state, .failed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.resumeFailed"))
    }

    private func reduce(
        _ state: inout BackupSessionState,
        _ event: BackupEvent,
        runMode: BackupRunMode = .full,
        displayMode: BackupRunMode = .full,
        terminalIntent: BackupTerminationIntent = .none
    ) {
        _ = state.reduce(
            event: event,
            runMode: runMode,
            displayMode: displayMode,
            terminalIntent: terminalIntent
        )
    }
}
