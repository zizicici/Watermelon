import XCTest
@testable import Watermelon

final class MonthPlanStateMachineTests: XCTestCase {

    // MARK: - .partiallyFailed → .failed preserves failedItemCount

    func testPartiallyFailedToFailedPreservesFailedItemCount() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.apply(.uploadStarted)
        XCTAssertEqual(plan.phase, .uploading)

        plan.apply(.uploadCompleted)
        XCTAssertEqual(plan.phase, .uploadDone)

        plan.apply(.partiallyFailed(count: 3))
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertEqual(plan.failedItemCount, 3)

        plan.apply(.failed(reason: "download incomplete"))
        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failedItemCount, 3, "failedItemCount must survive partiallyFailed → failed escalation")
        XCTAssertEqual(plan.failureMessage, "download incomplete")
    }

    func testFailedBlocksPartiallyFailedRegression() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.apply(.uploadStarted)
        plan.apply(.uploadCompleted)
        XCTAssertEqual(plan.phase, .uploadDone)

        plan.apply(.failed(reason: "download incomplete"))
        XCTAssertEqual(plan.phase, .failed)

        plan.apply(.partiallyFailed(count: 5))
        XCTAssertEqual(plan.phase, .failed, "partiallyFailed must not override .failed")
        XCTAssertEqual(plan.failedItemCount, 0, "failedItemCount stays 0 because transition was blocked")
    }

    func testPartiallyFailedToFailedFromUploading() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)
        plan.apply(.uploadStarted)
        XCTAssertEqual(plan.phase, .uploading)

        plan.apply(.partiallyFailed(count: 2))
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertEqual(plan.failedItemCount, 2)

        plan.apply(.failed(reason: "connection lost"))
        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failedItemCount, 2)
        XCTAssertEqual(plan.failureMessage, "connection lost")
    }

    func testTerminalPartiallyFailedBlocksCompleted() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.apply(.uploadStarted)
        plan.apply(.uploadCompleted)
        plan.apply(.partiallyFailed(count: 1))
        XCTAssertEqual(plan.phase, .partiallyFailed)

        plan.apply(.completed)
        XCTAssertEqual(plan.phase, .partiallyFailed, "completed must not override partiallyFailed")
    }

    // MARK: - handleUploadResult preserves failedItemCount on already-.failed months

    func testHandleUploadResultPreservesFailedCountOnAlreadyFailedMonth() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(
            backup: [], download: [], complement: [month],
            localAssetIDs: { _ in ["asset1"] }
        )

        // Simulate inline download marking month .failed during upload phase
        session.completeComplementMonthUpload(month)
        session.beginDownloadMonth(month)
        session.markDownloadIncompleteMonth(month, reason: "download incomplete")
        XCTAssertEqual(session.monthPlans[month]?.phase, .failed)

        let result = BackupSessionAsyncBridge.UploadResult.completed(
            failedCountByMonth: [month: 3],
            downloadIncompleteMonths: [month],
            downloadIncompleteMessagesByMonth: [month: "download incomplete"]
        )
        let outcome = session.handleUploadResult(result)
        if case .finished = outcome { /* expected */ } else {
            XCTFail("expected .finished, got \(outcome)")
        }

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .failed)
        XCTAssertEqual(plan.failedItemCount, 3, "failedItemCount must be set even when month was already .failed")
    }

    func testHandleUploadResultAppliesPartiallyFailedOnNonFailedMonth() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(
            backup: [], download: [], complement: [month],
            localAssetIDs: { _ in ["asset1"] }
        )
        session.completeComplementMonthUpload(month)
        XCTAssertEqual(session.monthPlans[month]?.phase, .uploadDone)

        let result = BackupSessionAsyncBridge.UploadResult.completed(
            failedCountByMonth: [month: 2],
            downloadIncompleteMonths: [],
            downloadIncompleteMessagesByMonth: [:]
        )
        let outcome = session.handleUploadResult(result)
        if case .finished = outcome { /* expected */ } else {
            XCTFail("expected .finished, got \(outcome)")
        }

        let plan = session.monthPlans[month]!
        XCTAssertEqual(plan.phase, .partiallyFailed)
        XCTAssertEqual(plan.failedItemCount, 2)
    }

    // MARK: - failedMonthInfos includes upload count for .failed months

    func testFailedMonthInfosIncludesUploadCountForFailedMonth() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let plan = MonthPlan(
            needsUpload: true, needsDownload: true,
            phase: .failed, failedItemCount: 3,
            failureMessage: "download incomplete"
        )
        let state = HomeExecutionState(
            monthPlans: [month: plan],
            phase: .failed("error"),
            controlState: .idle,
            statusText: "",
            processedCountByMonth: [:],
            assetCountByMonth: [:],
            backupMonths: [],
            downloadMonths: [],
            complementMonths: [month]
        )
        let infos = state.failedMonthInfos
        XCTAssertEqual(infos.count, 1)
        XCTAssertTrue(infos[0].message.contains("3"), "message should include upload failure count")
        XCTAssertTrue(infos[0].message.contains("download incomplete"), "message should include original failure reason")
    }

    func testFailedMonthInfosExcludesCountWhenZeroForFailedMonth() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let plan = MonthPlan(
            needsUpload: true, needsDownload: true,
            phase: .failed, failedItemCount: 0,
            failureMessage: "connection lost"
        )
        let state = HomeExecutionState(
            monthPlans: [month: plan],
            phase: .failed("error"),
            controlState: .idle,
            statusText: "",
            processedCountByMonth: [:],
            assetCountByMonth: [:],
            backupMonths: [],
            downloadMonths: [],
            complementMonths: [month]
        )
        let infos = state.failedMonthInfos
        XCTAssertEqual(infos.count, 1)
        XCTAssertEqual(infos[0].message, "connection lost", "message should not include count when failedItemCount is 0")
    }

    // MARK: - Download incomplete reason strings

    func testUnverifiedFingerprintStringDiffersFromSkippedIncomplete() {
        let unverified = String(localized: "restore.log.unverifiedFingerprint")
        let skipped = String(localized: "restore.log.skippedIncomplete")
        XCTAssertNotEqual(unverified, skipped)
    }

    func testCombinedDownloadIncompleteReasonIncludesBothCounts() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let skippedPart = String.localizedStringWithFormat(
            String(localized: "restore.log.skippedIncomplete"),
            month.displayText,
            3
        )
        let unverifiedPart = String.localizedStringWithFormat(
            String(localized: "restore.log.unverifiedFingerprint"),
            month.displayText,
            2
        )
        let combined = [skippedPart, unverifiedPart].joined(separator: ". ")
        XCTAssertTrue(combined.hasPrefix(month.displayText), "combined reason should start with month name")
        XCTAssertTrue(combined.contains(skippedPart), "combined reason should include skipped part")
        XCTAssertTrue(combined.contains(unverifiedPart), "combined reason should include unverified part")
    }
}
