import XCTest
@testable import Watermelon

final class HomeExecutionSessionTests: XCTestCase {

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
