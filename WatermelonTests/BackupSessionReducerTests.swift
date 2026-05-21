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

        XCTAssertTrue(state.failedCountByMonth.isEmpty)
        XCTAssertTrue(state.processedCountByMonth.isEmpty)
        XCTAssertTrue(state.incompleteSummaryByMonth.isEmpty)
        XCTAssertEqual(state.state, .running)
        XCTAssertEqual(state.controlPhase, .resuming)
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

    func testIncompleteMonthSummaryMergesByMaxAndRemovesCompletedMonth() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)
        let first = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 3, fingerprintMismatchCount: 1),
            metadataSnapshotDeferredMessage: "old"
        )
        let second = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1, fingerprintMismatchCount: 4),
            metadataSnapshotDeferredMessage: "new"
        )

        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .completed)))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(first))))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(first))))
        reduce(&state, .monthChanged(MonthChangeEvent(year: month.year, month: month.month, action: .incomplete(second))))

        XCTAssertFalse(state.completedMonths.contains(month))
        XCTAssertEqual(state.incompleteMonths, [month])
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.downloadIssues.skippedIncompleteCount, 3)
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.downloadIssues.fingerprintMismatchCount, 4)
        XCTAssertEqual(state.incompleteSummaryByMonth[month]?.metadataSnapshotDeferredMessage, "new")
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

        XCTAssertTrue(state.completedMonths.contains(month))
        XCTAssertNil(state.incompleteSummaryByMonth[month])
    }

    func testFinishedRunWithIncompleteSummaryUsesPartialStatusText() {
        var state = BackupSessionState()
        let month = LibraryMonthKey(year: 2024, month: 6)

        reduce(&state, .monthChanged(MonthChangeEvent(
            year: month.year,
            month: month.month,
            action: .incomplete(BackupMonthIncompleteSummary(
                metadataSnapshotDeferredMessage: "snapshot deferred"
            ))
        )))
        reduce(&state, .finished(BackupExecutionResult(
            total: 1,
            succeeded: 1,
            failed: 0,
            skipped: 0,
            paused: false
        )))

        XCTAssertEqual(state.state, .completed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.backupCompletedPartial"))
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
