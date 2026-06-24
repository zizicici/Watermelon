import XCTest
@testable import Watermelon

final class BackupSessionReducerTests: XCTestCase {

    private func itemEvent(_ id: String, status: BackupItemStatus, date: Date) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: id,
            assetFingerprint: nil,
            displayName: id,
            resourceDate: date,
            status: status,
            reason: nil,
            updatedAt: date
        )
    }

    private func reduceProgress(_ state: inout BackupSessionState, _ item: BackupItemEvent) {
        let progress = BackupProgress(
            succeeded: 0, failed: 0, skipped: 0, total: 1,
            message: "",
            logMessage: nil,
            logLevel: .info,
            itemEvent: item,
            transferState: nil
        )
        _ = state.reduce(event: .progress(progress), runMode: .full, displayMode: .full, terminalIntent: .none)
    }

    // F1-R05: a transient per-asset failure that succeeds on retry after resume must NOT leave the month falsely
    // `.partiallyFailed` — the monotonic failed counter is reset on resume and the retry re-establishes it.
    func testTransientFailureSucceedingOnResumeClearsFailedCount() {
        var state = BackupSessionState()
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let month = LibraryMonthKey.from(date: date)

        reduceProgress(&state, itemEvent("A", status: .failed, date: date))
        XCTAssertEqual(state.snapshot().failedCountByMonth[month], 1)
        XCTAssertFalse(state.completedAssetIDsForResume.contains("A"), "a failed asset is dropped from resume-complete")

        _ = state.prepareForResume()
        XCTAssertNil(state.snapshot().failedCountByMonth[month], "resume must reset the monotonic failed counter")

        reduceProgress(&state, itemEvent("A", status: .success, date: date))
        XCTAssertNil(state.snapshot().failedCountByMonth[month], "a retry-success must not re-mark the month failed")
        XCTAssertTrue(state.completedAssetIDsForResume.contains("A"))
    }

    // A genuine failure that recurs on the resume retry is still reported (the retry re-establishes the count).
    func testFailureRecurringOnResumeStaysFailed() {
        var state = BackupSessionState()
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let month = LibraryMonthKey.from(date: date)

        reduceProgress(&state, itemEvent("A", status: .failed, date: date))
        _ = state.prepareForResume()
        reduceProgress(&state, itemEvent("A", status: .failed, date: date))

        XCTAssertEqual(state.snapshot().failedCountByMonth[month], 1, "a failure that recurs on resume must still be reported")
    }
}
