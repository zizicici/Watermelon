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
        uploadFirst.apply(.uploadStarted)
        uploadFirst.apply(.uploadCompleted)
        uploadFirst.apply(.recordUploadFailures(observedFailedItemCount: 4))
        uploadFirst.apply(.recordIncomplete(summary))

        var downloadFirst = MonthPlan(needsUpload: true, needsDownload: true)
        downloadFirst.apply(.uploadStarted)
        downloadFirst.apply(.uploadCompleted)
        downloadFirst.apply(.recordIncomplete(summary))
        downloadFirst.apply(.recordUploadFailures(observedFailedItemCount: 4))

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

        plan.apply(.recordUploadFailures(observedFailedItemCount: 3))
        plan.apply(.recordIncomplete(summary))
        plan.apply(.recordTerminalFailure(terminal))
        plan.apply(.completed)

        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 2)
        XCTAssertEqual(plan.failureFacts.terminalFailure, terminal)
    }

    func testCompletedAfterNonFatalFactsRemainsPartiallyFailed() {
        var plan = MonthPlan(needsUpload: false, needsDownload: true)
        plan.apply(.downloadStarted)
        plan.apply(.recordIncomplete(BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(fingerprintMismatchCount: 2)
        )))

        plan.apply(.completed)

        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertTrue(plan.failureFacts.hasUserVisibleFailure)
    }

    func testDuplicateFactsUseMaxAndLatestMessage() {
        let first = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 3,
                fingerprintMismatchCount: 1,
                localFingerprintVerificationIncompleteCount: 2
            ),
            metadataSnapshotDeferredMessage: "old snapshot warning"
        )
        let second = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 1,
                fingerprintMismatchCount: 4,
                localFingerprintVerificationIncompleteCount: 2
            ),
            metadataSnapshotDeferredMessage: "new snapshot warning"
        )
        var plan = MonthPlan(needsUpload: true, needsDownload: true)

        plan.apply(.recordUploadFailures(observedFailedItemCount: 2))
        plan.apply(.recordUploadFailures(observedFailedItemCount: 2))
        plan.apply(.recordUploadFailures(observedFailedItemCount: 5))
        plan.apply(.recordUploadFailures(observedFailedItemCount: -9))
        plan.apply(.recordIncomplete(first))
        plan.apply(.recordIncomplete(first))
        plan.apply(.recordIncomplete(second))

        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 5)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.fingerprintMismatchCount, 4)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.localFingerprintVerificationIncompleteCount, 2)
        XCTAssertEqual(plan.failureFacts.incomplete.metadataSnapshotDeferredMessage, "new snapshot warning")
    }

    func testTerminalFailureLatestWins() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)
        plan.apply(.recordTerminalFailure(MonthTerminalFailure(kind: .backupStartFailed, message: "start failed")))
        plan.apply(.recordTerminalFailure(MonthTerminalFailure(kind: .uploadRunFailed, message: "upload failed")))

        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failureFacts.terminalFailure?.kind, .uploadRunFailed)
        XCTAssertEqual(plan.failureFacts.terminalFailure?.message, "upload failed")
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
        XCTAssertEqual(session.phase, .completed)
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
        session.recordMonthIncomplete(month, summary: summary)

        let result = BackupSessionAsyncBridge.UploadResult.completed(
            failedCountByMonth: [month: 3],
            incompleteSummaryByMonth: [month: summary]
        )
        let outcome = session.handleUploadResult(result)
        if case .finished = outcome {} else {
            XCTFail("expected .finished, got \(outcome)")
        }

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertEqual(plan.failureFacts.uploadFailedItemCount, 3)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.skippedIncompleteCount, 2)
        XCTAssertEqual(plan.failureFacts.incomplete.downloadIssues.fingerprintMismatchCount, 1)
    }

    func testFailedMonthInfosRenderAllFailureFactsInOrder() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let summary = BackupMonthIncompleteSummary(
            downloadIssues: DownloadIssueSummary(
                skippedIncompleteCount: 1,
                fingerprintMismatchCount: 2,
                localFingerprintVerificationIncompleteCount: 3
            ),
            metadataSnapshotDeferredMessage: "snapshot deferred"
        )
        let facts = MonthFailureFacts(
            uploadFailedItemCount: 4,
            incomplete: summary,
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
}
