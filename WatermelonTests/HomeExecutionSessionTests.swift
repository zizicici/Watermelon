import XCTest
@testable import Watermelon

final class HomeExecutionSessionTests: XCTestCase {

    func testConnectionFailureCanPreserveTransportSpecificReason() {
        var session = HomeExecutionSession()
        let month = LibraryMonthKey(year: 2026, month: 1)
        session.enter(backup: [month], download: [], complement: [], localAssetIDs: { _ in ["a"] })

        let alert = session.failForMissingConnection(message: "Desktop left")

        XCTAssertEqual(alert.message, "Desktop left")
        XCTAssertEqual(session.phase, .failed("Desktop left"))
        XCTAssertEqual(session.monthPlans[month]?.phase, .failed)
    }

    private func startedUploadProgress(_ months: Set<LibraryMonthKey>) -> BackupSessionAsyncBridge.UploadProgress {
        BackupSessionAsyncBridge.UploadProgress(
            newlyStartedMonths: months,
            newlyCompletedMonths: [],
            processedCountByMonth: [:]
        )
    }

    private func markUploading(_ session: inout HomeExecutionSession, _ month: LibraryMonthKey) {
        _ = session.handleUploadProgress(
            startedUploadProgress([month]),
            now: 0,
            syncThrottleInterval: 0
        )
    }

    // settleStop auto-exits a `.fatal`-during-stop because the session stays active at `.failed`
    // (isActive == phase != nil). The R04 fix depends on this: clearing transientControlState at the `.fatal`
    // source would instead strand settleStop's `isActive` guard, so the run would no longer auto-exit on stop.
    func testFailedSessionRemainsActive() {
        var session = HomeExecutionSession()
        session.enter(backup: [LibraryMonthKey(year: 2026, month: 1)], download: [], complement: [], localAssetIDs: { _ in ["a"] })
        _ = session.failExecution(reason: "boom")

        guard case .failed = session.phase else { return XCTFail("expected .failed phase") }
        XCTAssertTrue(session.isActive, "a .failed session must stay active so settleStop's isActive guard can still auto-exit")
    }

    // A complement month whose upload finalize fails read-back (-36) never runs its inline download, so it is
    // still `.uploading` when failedCountByMonth lands. It must fail closed, not be terminalized .partiallyFailed
    // (which drops the download out of the phase) under an overall `.completed`.
    func testComplementReadBackFailure_failsClosed_notMaskedCompleted() {
        var session = HomeExecutionSession()
        let month = LibraryMonthKey(year: 2026, month: 1)
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["a"] })
        markUploading(&session, month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .uploading)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [month: 2]))

        XCTAssertEqual(session.monthPlans[month]?.phase, .failed)
        guard case .finished = outcome else { return XCTFail("expected finished outcome") }
        guard case .failed = session.phase else {
            return XCTFail("dropped complement download must surface as failed, not masked completed")
        }
    }

    // Backup-only months have no download phase, so an uncommitted (read-back-failed) upload is correctly a
    // partial completion: stays .partiallyFailed with the run overall .completed.
    func testBackupOnlyReadBackFailure_staysPartiallyFailedCompleted() {
        var session = HomeExecutionSession()
        let month = LibraryMonthKey(year: 2026, month: 1)
        session.enter(backup: [month], download: [], complement: [], localAssetIDs: { _ in ["a"] })
        markUploading(&session, month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .uploading)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [month: 2]))

        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
        guard case .finished = outcome else { return XCTFail("expected finished outcome") }
        guard case .completed = session.phase else {
            return XCTFail("backup-only partial failure should stay completed overall")
        }
    }

    // A pause that races upload completion (BSC reached .completed before requestPause landed, so the session is
    // already .uploadPaused when the .completed result arrives) must be honored, not overridden into the download
    // phase. The completed upload is absorbed (uploadPhaseCompleted) but the run stays paused.
    func testPauseRacingUploadCompletion_staysPausedNotAdvancedToDownload() {
        var session = HomeExecutionSession()
        let backupMonth = LibraryMonthKey(year: 2026, month: 1)
        let downloadMonth = LibraryMonthKey(year: 2026, month: 2)
        session.enter(backup: [backupMonth], download: [downloadMonth], complement: [], localAssetIDs: { _ in ["a"] })
        markUploading(&session, backupMonth)
        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [],
                newlyCompletedMonths: [backupMonth],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )
        XCTAssertEqual(session.monthPlans[backupMonth]?.phase, .completed)

        // Pause lands while the BSC has already finished — session moves to .uploadPaused.
        XCTAssertEqual(session.pause(), .upload)
        XCTAssertEqual(session.phase, .uploadPaused)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [:]))

        guard case .paused = outcome else { return XCTFail("raced pause must yield .paused, not advance to download") }
        XCTAssertEqual(session.phase, .uploadPaused)
        XCTAssertEqual(session.monthPlans[downloadMonth]?.phase, .pending)
    }

    // The normal (no pause) upload→download transition must still advance: a .completed result with a remaining
    // download month yields .continueToDownload and the session enters the downloading phase.
    func testUploadCompletionWithoutPause_advancesToDownload() {
        var session = HomeExecutionSession()
        let backupMonth = LibraryMonthKey(year: 2026, month: 1)
        let downloadMonth = LibraryMonthKey(year: 2026, month: 2)
        session.enter(backup: [backupMonth], download: [downloadMonth], complement: [], localAssetIDs: { _ in ["a"] })
        markUploading(&session, backupMonth)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [:]))

        guard case .continueToDownload = outcome else { return XCTFail("expected continueToDownload") }
        XCTAssertEqual(session.phase, .downloading)
    }

    // A complement month whose read-back failed is still `.uploading` when a pause races completion, so
    // `pauseUploadPhaseMonths()` clobbers it to `.uploadPaused`. The fail-closed guard must still fire (the
    // dropped inline download must not be masked `.completed`); the run stays paused while a download month remains.
    func testPauseRacingComplementReadBackFailure_failsClosed_notMaskedCompleted() {
        var session = HomeExecutionSession()
        let complementMonth = LibraryMonthKey(year: 2026, month: 1)
        let downloadMonth = LibraryMonthKey(year: 2026, month: 2)
        session.enter(backup: [], download: [downloadMonth], complement: [complementMonth], localAssetIDs: { _ in ["a"] })
        markUploading(&session, complementMonth)
        XCTAssertEqual(session.pause(), .upload)
        XCTAssertEqual(session.monthPlans[complementMonth]?.phase, .uploadPaused)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [complementMonth: 2]))

        XCTAssertEqual(session.monthPlans[complementMonth]?.phase, .failed)
        guard case .paused = outcome else { return XCTFail("expected paused; a download month remains") }
        XCTAssertEqual(session.phase, .uploadPaused)
    }

    // Same race, but the read-back-failed complement is the only download-bearing month: failing it closed empties
    // remainingDownloadMonths, so the run finishes failed (nothing left to pause into) rather than masked completed.
    func testPauseRacingComplementReadBackFailure_onlyMonth_finishesFailed() {
        var session = HomeExecutionSession()
        let complementMonth = LibraryMonthKey(year: 2026, month: 1)
        session.enter(backup: [], download: [], complement: [complementMonth], localAssetIDs: { _ in ["a"] })
        markUploading(&session, complementMonth)
        XCTAssertEqual(session.pause(), .upload)
        XCTAssertEqual(session.monthPlans[complementMonth]?.phase, .uploadPaused)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [complementMonth: 2]))

        XCTAssertEqual(session.monthPlans[complementMonth]?.phase, .failed)
        guard case .finished = outcome else { return XCTFail("expected finished; no download months remain") }
        guard case .failed = session.phase else { return XCTFail("run must surface failed, not masked completed") }
    }

    // The same race-clobber to `.uploadPaused` must not swallow a backup-only month's partial-failure annotation:
    // .partiallyFailed is a no-op on `.uploadPaused`, so without the phase restore it would silently end .completed.
    func testPauseRacingBackupOnlyReadBackFailure_staysPartiallyFailed() {
        var session = HomeExecutionSession()
        let backupMonth = LibraryMonthKey(year: 2026, month: 1)
        let downloadMonth = LibraryMonthKey(year: 2026, month: 2)
        session.enter(backup: [backupMonth], download: [downloadMonth], complement: [], localAssetIDs: { _ in ["a"] })
        markUploading(&session, backupMonth)
        XCTAssertEqual(session.pause(), .upload)
        XCTAssertEqual(session.monthPlans[backupMonth]?.phase, .uploadPaused)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [backupMonth: 2]))

        XCTAssertEqual(session.monthPlans[backupMonth]?.phase, .partiallyFailed)
        guard case .paused = outcome else { return XCTFail("expected paused; a download month remains") }
    }

    // A complement month whose inline download already ran reaches `.completed`; the late per-month failed count
    // is an annotation only (.partiallyFailed), and the run stays overall .completed — the download was not dropped.
    func testComplementInlineDownloaded_partialAnnotation_keepsCompleted() {
        var session = HomeExecutionSession()
        let month = LibraryMonthKey(year: 2026, month: 1)
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["a"] })
        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.completeDownloadMonth(month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .completed)

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [month: 1]))

        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
        guard case .finished = outcome else { return XCTFail("expected finished outcome") }
        guard case .completed = session.phase else {
            return XCTFail("inline-downloaded complement should stay completed overall")
        }
    }
}
