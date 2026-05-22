import XCTest
@testable import Watermelon

final class MonthPlanStateMachineTests: XCTestCase {

    func testUploadAndDownloadFactsMergeInEitherOrder() {
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(
            skippedIncompleteCount: 2,
            fingerprintMismatchCount: 1,
            localFingerprintVerificationIncompleteCount: 3
        ))

        var uploadFirst = MonthPlan(needsUpload: true, needsDownload: true)
        uploadFirst.markUploadStarted()
        uploadFirst.markUploadDurablyCompleted()
        uploadFirst.recordUploadFailures(observedFailedItemCount: 4)
        uploadFirst.recordDownloadIncomplete(summary)

        var downloadFirst = MonthPlan(needsUpload: true, needsDownload: true)
        downloadFirst.markUploadStarted()
        downloadFirst.markUploadDurablyCompleted()
        downloadFirst.recordDownloadIncomplete(summary)
        downloadFirst.recordUploadFailures(observedFailedItemCount: 4)

        XCTAssertEqual(uploadFirst.phase, .partiallyFailed)
        XCTAssertEqual(downloadFirst.phase, .partiallyFailed)
        XCTAssertEqual(uploadFirst.failureFacts, downloadFirst.failureFacts)
        XCTAssertEqual(uploadFirst.failureFacts.uploadFailedItemCount, 4)
        XCTAssertEqual(uploadFirst.failureFacts.incomplete.downloadIssues, summary.downloadIssues)
    }

    func testTerminalFailureAfterNonFatalFactsPreservesFacts() {
        let terminal = MonthTerminalFailure(kind: .downloadRunFailed, message: "download failed")
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 2))
        var plan = MonthPlan(needsUpload: true, needsDownload: true)

        plan.recordUploadFailures(observedFailedItemCount: 3)
        plan.recordDownloadIncomplete(summary)
        plan.recordTerminalFailure(terminal)
        plan.markUploadDurablyCompleted()

        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 2)
        XCTAssertEqual(plan.failureFacts.terminalFailure, terminal)
    }

    func testCompletedAfterNonFatalFactsRemainsPartiallyFailed() {
        var plan = MonthPlan(needsUpload: false, needsDownload: true)
        plan.markDownloadStarted()
        plan.recordDownloadIncomplete(BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(fingerprintMismatchCount: 2)
        ))

        plan.closeDownloadAttemptClean()

        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertTrue(plan.failureFacts.hasUserVisibleFailure)
    }

    func testDuplicateFactsUseMaxAndLatestMessage() {
        let first = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 3,
                fingerprintMismatchCount: 1,
                localFingerprintVerificationIncompleteCount: 2
            )
        )
        let second = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 1,
                fingerprintMismatchCount: 4,
                localFingerprintVerificationIncompleteCount: 2
            )
        )
        var plan = MonthPlan(needsUpload: true, needsDownload: true)

        plan.recordUploadFailures(observedFailedItemCount: 2)
        plan.recordUploadFailures(observedFailedItemCount: 2)
        plan.recordUploadFailures(observedFailedItemCount: 5)
        plan.recordUploadFailures(observedFailedItemCount: -9)
        plan.recordDownloadIncomplete(first)
        plan.recordDownloadIncomplete(first)
        plan.recordDownloadIncomplete(second)

        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 5)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.fingerprintMismatchCount, 4)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.localFingerprintVerificationIncompleteCount, 2)
    }

    func testTerminalFailureLatestWins() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)
        plan.recordTerminalFailure(MonthTerminalFailure(kind: .backupStartFailed, message: "start failed"))
        plan.recordTerminalFailure(MonthTerminalFailure(kind: .uploadRunFailed, message: "upload failed"))

        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failureFacts.terminalFailure?.kind, .uploadRunFailed)
        XCTAssertEqual(plan.failureFacts.terminalFailure?.message, "upload failed")
    }

    func testDurableSnapshotDeferredClosesUploadThroughNamedPath() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)

        plan.recordDurableUploadSnapshotDeferred(message: "snapshot deferred")

        XCTAssertTrue(plan.workFacts.uploadFinished)
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertEqual(plan.failureFacts.durableSnapshotDeferredMessage, "snapshot deferred")
    }

    func testUploadProgressCompletionTriggersSyncAfterIncompleteWarning() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1))
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })
        session.recordMonthIncomplete(month, summary: summary)

        let shouldSync = session.handleUploadProgress(
            uploadProgress(newlyUploadCompletedMonths: [month]),
            now: 100,
            syncThrottleInterval: 1_000
        )

        XCTAssertTrue(shouldSync)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.uploadFinished == true)
        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
    }

    func testUploadProgressCompletionTriggersSyncAfterDurableSnapshotDeferredWarning() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [month], download: [], complement: [], localAssetIDs: { _ in ["asset1"] })
        session.recordDurableUploadSnapshotDeferred(month, message: "snapshot deferred")

        let shouldSync = session.handleUploadProgress(
            uploadProgress(newlyUploadCompletedMonths: [month]),
            now: 100,
            syncThrottleInterval: 1_000
        )

        XCTAssertTrue(shouldSync)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.uploadFinished == true)
        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
    }

    func testUploadFailuresDoNotSuppressPendingComplementDownload() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })
        session.completeComplementMonthUpload(month)

        let outcome = session.handleUploadResult(.completed(
            failedCountByMonth: [month: 3],
            incompleteSummaryByMonth: [:],
            uploadSnapshotDeferredMessageByMonth: [:]
        ))

        if case .continueToDownload = outcome {} else {
            XCTFail("expected .continueToDownload, got \(outcome)")
        }
        XCTAssertEqual(session.pendingDownloadMonths(), [month])
        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
        XCTAssertTrue(session.monthPlans[month]?.hasPendingDownloadWork == true)
    }

    func testUploadDoneComplementDoesNotReceiveUploadRunFailure() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })
        session.completeComplementMonthUpload(month)

        _ = session.handleUploadResult(.failed(
            "upload failed",
            failedCountByMonth: [:],
            incompleteSummaryByMonth: [:],
            uploadSnapshotDeferredMessageByMonth: [:]
        ))

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .uploadDone)
        XCTAssertNil(plan.failureFacts.terminalFailure)
        XCTAssertTrue(plan.hasPendingDownloadWork)
    }

    func testFailedUploadResultAppliesDurableSnapshotDeferredBeforeTerminalStamp() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [month], download: [], complement: [], localAssetIDs: { _ in ["asset1"] })

        _ = session.handleUploadResult(.failed(
            "upload failed",
            failedCountByMonth: [:],
            incompleteSummaryByMonth: [:],
            uploadSnapshotDeferredMessageByMonth: [month: "snapshot deferred"]
        ))

        let plan = session.monthPlans[month]!
        XCTAssertTrue(plan.workFacts.uploadFinished)
        XCTAssertEqual(plan.failureFacts.durableSnapshotDeferredMessage, "snapshot deferred")
        XCTAssertNil(plan.failureFacts.terminalFailure)
        XCTAssertEqual(plan.phase, .partiallyFailed)
    }

    func testComplementPartialWithPendingDownloadReceivesRunAbortFailure() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.markUploadDurablyCompleted()
        plan.recordUploadFailures(observedFailedItemCount: 2)

        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertTrue(plan.shouldReceiveRunAbortFailure)
        XCTAssertFalse(plan.hasClosedUserVisibleOutcome)
    }

    func testClosedPartialDoesNotReceiveRunAbortFailure() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.markUploadDurablyCompleted()
        plan.recordUploadFailures(observedFailedItemCount: 2)
        plan.markDownloadStarted()
        plan.closeDownloadAttemptClean()

        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertFalse(plan.shouldReceiveRunAbortFailure)
        XCTAssertTrue(plan.hasClosedUserVisibleOutcome)
    }

    func testRound18DurableSnapshotDeferredIsNotStampedByMissingConnection() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [month], download: [], complement: [], localAssetIDs: { _ in ["asset1"] })
        session.recordDurableUploadSnapshotDeferred(month, message: "snapshot deferred")

        _ = session.failForMissingConnection()

        let plan = session.monthPlans[month]!
        XCTAssertTrue(plan.workFacts.uploadFinished)
        XCTAssertNil(plan.failureFacts.terminalFailure)
        XCTAssertEqual(plan.phase, .partiallyFailed)
    }

    func testFailForMissingConnectionSessionMatrix() {
        let pendingUpload = LibraryMonthKey(year: 2024, month: 6)
        let closedPartial = LibraryMonthKey(year: 2024, month: 7)
        let completed = LibraryMonthKey(year: 2024, month: 8)
        let existingFailed = LibraryMonthKey(year: 2024, month: 9)
        var session = HomeExecutionSession()
        session.enter(
            backup: [pendingUpload],
            download: [closedPartial, completed, existingFailed],
            complement: [],
            localAssetIDs: { _ in ["asset1"] }
        )

        session.beginDownloadMonth(closedPartial)
        session.finishDownloadAttemptWithIncomplete(
            closedPartial,
            summary: BackupMonthIncompleteSummary(
                downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1)
            )
        )
        session.beginDownloadMonth(completed)
        session.finishDownloadAttempt(completed)
        session.beginDownloadMonth(existingFailed)
        session.finishDownloadAttemptWithFailure(
            existingFailed,
            failure: MonthTerminalFailure(kind: .downloadRunFailed, message: "download failed")
        )

        _ = session.failForMissingConnection()

        XCTAssertEqual(
            session.monthPlans[pendingUpload]?.failureFacts.terminalFailure?.kind,
            .missingConnection
        )
        XCTAssertNil(session.monthPlans[closedPartial]?.failureFacts.terminalFailure)
        XCTAssertNil(session.monthPlans[completed]?.failureFacts.terminalFailure)
        XCTAssertEqual(
            session.monthPlans[existingFailed]?.failureFacts.terminalFailure?.kind,
            .downloadRunFailed
        )
    }

    func testDownloadStartedTransitionsPartiallyFailedToDownloading() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.markUploadDurablyCompleted()
        plan.recordUploadFailures(observedFailedItemCount: 2)

        XCTAssertEqual(plan.phase, .partiallyFailed)
        plan.markDownloadStarted()

        XCTAssertEqual(plan.phase, .downloading)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 2)
        XCTAssertTrue(plan.workFacts.downloadStarted)
        XCTAssertFalse(plan.workFacts.downloadFinished)
    }

    func testDownloadAttemptFinishedWithoutStartDoesNotCloseWork() {
        var plan = MonthPlan(needsUpload: false, needsDownload: true)

        plan.closeDownloadAttemptClean()

        XCTAssertFalse(plan.workFacts.downloadStarted)
        XCTAssertFalse(plan.workFacts.downloadFinished)
        XCTAssertTrue(plan.hasPendingDownloadWork)
    }

    func testInlineDownloadIncompleteClosesAttemptAndIsNotPending() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(
            skippedIncompleteCount: 2,
            fingerprintMismatchCount: 1
        ))
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })

        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.finishDownloadAttemptWithIncomplete(month, summary: summary)

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertTrue(plan.workFacts.downloadFinished)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues, summary.downloadIssues)
        XCTAssertEqual(session.pendingDownloadMonths(), [])
    }

    func testInlineDownloadSuccessClosesAttemptAndIsNotPending() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })

        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.finishDownloadAttempt(month)

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .completed)
        XCTAssertTrue(plan.workFacts.downloadFinished)
        XCTAssertEqual(session.pendingDownloadMonths(), [])
    }

    func testInlineDownloadFailureClosesAttemptAndIsNotPending() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let failure = MonthTerminalFailure(kind: .downloadRunFailed, message: "download failed")
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })

        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.finishDownloadAttemptWithFailure(month, failure: failure)

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .failed)
        XCTAssertTrue(plan.workFacts.downloadFinished)
        XCTAssertEqual(plan.failureFacts.terminalFailure, failure)
        XCTAssertEqual(session.pendingDownloadMonths(), [])
    }

    func testPausedDownloadRemainsPendingForResume() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [month], complement: [], localAssetIDs: { _ in [] })

        session.beginDownloadMonth(month)
        _ = session.pause()

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .downloadPaused)
        XCTAssertTrue(plan.workFacts.hasActiveDownloadAttempt)
        XCTAssertEqual(session.pendingDownloadMonths(), [month])
    }

    func testPartiallyFailedActiveDownloadCanPauseAndResume() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.markUploadDurablyCompleted()
        plan.recordUploadFailures(observedFailedItemCount: 2)
        plan.markDownloadStarted()

        XCTAssertEqual(plan.phase, .downloading)
        plan.markDownloadPaused()

        XCTAssertEqual(plan.phase, .downloadPaused)
        XCTAssertTrue(plan.workFacts.hasActiveDownloadAttempt)
        XCTAssertTrue(plan.hasPendingDownloadWork)

        plan.markDownloadResumed()

        XCTAssertEqual(plan.phase, .downloading)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 2)
        XCTAssertTrue(plan.workFacts.hasActiveDownloadAttempt)
    }

    func testCancelledInlineDownloadIsReattemptedAfterUploadFailureFacts() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })

        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .downloading)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.hasActiveDownloadAttempt == true)

        let outcome = session.handleUploadResult(.completed(
            failedCountByMonth: [month: 3],
            incompleteSummaryByMonth: [:],
            uploadSnapshotDeferredMessageByMonth: [:]
        ))

        if case .continueToDownload = outcome {} else {
            XCTFail("expected .continueToDownload, got \(outcome)")
        }
        XCTAssertEqual(session.pendingDownloadMonths(), [month])
        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)

        session.beginDownloadMonth(month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .downloading)
        XCTAssertEqual(session.monthPlans[month]?.failureFacts.uploadFailedItemCount, 3)
    }

    func testRecordIncompleteAndDownloadAttemptFinishedAreOrderIndependent() {
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(
            skippedIncompleteCount: 2,
            fingerprintMismatchCount: 1
        ))

        var factsFirst = MonthPlan(needsUpload: false, needsDownload: true)
        factsFirst.markDownloadStarted()
        factsFirst.closeDownloadAttemptIncomplete(summary)

        var attemptFirst = MonthPlan(needsUpload: false, needsDownload: true)
        attemptFirst.markDownloadStarted()
        attemptFirst.closeDownloadAttemptClean()
        attemptFirst.recordDownloadIncomplete(summary)

        XCTAssertEqual(factsFirst.phase, .partiallyFailed)
        XCTAssertEqual(attemptFirst.phase, .partiallyFailed)
        XCTAssertEqual(factsFirst.failureFacts, attemptFirst.failureFacts)
        XCTAssertEqual(factsFirst.workFacts, attemptFirst.workFacts)
    }

    func testFinishExecutionUsesFactsForGlobalRollup() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [month], complement: [], localAssetIDs: { _ in [] })

        session.recordMonthIncomplete(month, summary: BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(skippedIncompleteCount: 1)
        ))
        session.finishExecution()

        XCTAssertEqual(session.monthPlans[month]?.phase, .partiallyFailed)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.uploadFinished == true)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.downloadFinished == true)
        if case .failed(let message) = session.phase {
            XCTAssertEqual(message, String(localized: "home.execution.partialFailed"))
        } else {
            XCTFail("expected failed phase, got \(String(describing: session.phase))")
        }
    }

    func testFinishExecutionWithNoFactsCompletes() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [month], complement: [], localAssetIDs: { _ in [] })

        session.finishExecution()

        XCTAssertEqual(session.monthPlans[month]?.phase, .completed)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.uploadFinished == true)
        XCTAssertTrue(session.monthPlans[month]?.workFacts.downloadFinished == true)
        XCTAssertEqual(session.phase, .completed)
    }

    func testFinishExecutionWithTerminalFactsFailsGlobalRollup() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [month], complement: [], localAssetIDs: { _ in [] })

        session.beginDownloadMonth(month)
        session.finishDownloadAttemptWithFailure(
            month,
            failure: MonthTerminalFailure(kind: .downloadRunFailed, message: "download failed")
        )
        session.finishExecution()

        XCTAssertEqual(session.monthPlans[month]?.phase, .failed)
        if case .failed(let message) = session.phase {
            XCTAssertEqual(message, String(localized: "home.execution.partialFailed"))
        } else {
            XCTFail("expected failed phase, got \(String(describing: session.phase))")
        }
    }

    func testHandleUploadResultMergesAlreadyRecordedIncompleteFacts() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(downloadIssues: DownloadIssueSummary(
            skippedIncompleteCount: 2,
            fingerprintMismatchCount: 1
        ))
        var session = HomeExecutionSession()
        session.enter(backup: [], download: [], complement: [month], localAssetIDs: { _ in ["asset1"] })

        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.finishDownloadAttemptWithIncomplete(month, summary: summary)

        let result = BackupSessionAsyncBridge.UploadResult.completed(
            failedCountByMonth: [month: 3],
            incompleteSummaryByMonth: [month: summary],
            uploadSnapshotDeferredMessageByMonth: [:]
        )
        let outcome = session.handleUploadResult(result)
        if case .finished = outcome {} else {
            XCTFail("expected .finished, got \(outcome)")
        }

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertTrue(plan.workFacts.downloadFinished)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 2)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.fingerprintMismatchCount, 1)
        XCTAssertEqual(session.pendingDownloadMonths(), [])
    }

    func testFailedMonthInfosRenderAllFailureFactsInOrder() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 1,
                fingerprintMismatchCount: 2,
                localFingerprintVerificationIncompleteCount: 3
            )
        )
        let facts = MonthFailureFacts(
            uploadFailedItemCount: 4,
            incomplete: summary,
            durableSnapshotDeferredMessage: "snapshot deferred",
            terminalFailure: MonthTerminalFailure(kind: .downloadRunFailed, message: "download failed")
        )
        let state = HomeExecutionState(
            monthPlans: [month: MonthPlan(
                needsUpload: true,
                needsDownload: true,
                phase: .failed,
                failureFacts: facts
            )],
            phase: .failed("error"),
            controlState: .idle,
            statusText: "",
            processedCountByMonth: [:],
            assetCountByMonth: [:],
            backupMonths: [],
            downloadMonths: [],
            complementMonths: [month]
        )

        let message = state.failedMonthInfos.first?.message ?? ""
        XCTAssertTrue(message.contains("4"), "message should include upload failure count")
        XCTAssertTrue(message.contains(String.localizedStringWithFormat(
            String(localized: "restore.log.skippedIncomplete"),
            month.displayText,
            1
        )))
        XCTAssertTrue(message.contains(String.localizedStringWithFormat(
            String(localized: "restore.log.fingerprintMismatch"),
            month.displayText,
            2
        )))
        XCTAssertTrue(message.contains(String.localizedStringWithFormat(
            String(localized: "restore.log.unverifiedFingerprint"),
            month.displayText,
            3
        )))
        XCTAssertTrue(message.contains("snapshot deferred"))
        XCTAssertTrue(message.hasSuffix("download failed"))
    }

    func testDownloadIssueLocalizationKeysRemainDistinct() {
        let unverified = String(localized: "restore.log.unverifiedFingerprint")
        let skipped = String(localized: "restore.log.skippedIncomplete")
        let mismatch = String(localized: "restore.log.fingerprintMismatch")

        XCTAssertNotEqual(unverified, skipped)
        XCTAssertNotEqual(mismatch, skipped)
        XCTAssertNotEqual(mismatch, unverified)
    }

    private func uploadProgress(
        newlyUploadCompletedMonths: Set<LibraryMonthKey>
    ) -> BackupSessionAsyncBridge.UploadProgress {
        BackupSessionAsyncBridge.UploadProgress(
            newlyStartedMonths: [],
            newlyUploadCompletedMonths: newlyUploadCompletedMonths,
            processedCountByMonth: [:]
        )
    }
}
