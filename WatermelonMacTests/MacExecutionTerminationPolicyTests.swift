import XCTest
@testable import WatermelonMac

final class MacExecutionTerminationPolicyTests: XCTestCase {
    func testResultPresentationDoesNotBlockTermination() {
        XCTAssertFalse(
            isBlocking(
                .completed(
                    MacBackupExecutionSummary(
                        upload: nil,
                        restoredCount: 0,
                        skippedIncompleteCount: 0,
                        failedDownloadMonths: 0
                    )
                ),
                runtimeExecuting: false
            )
        )
        XCTAssertFalse(
            isBlocking(
                .failed("Failed"),
                runtimeExecuting: false
            )
        )
    }

    func testExternalActivityStillBlocksBehindResultPresentation() {
        XCTAssertTrue(
            isBlocking(
                .completed(
                    MacBackupExecutionSummary(
                        upload: nil,
                        restoredCount: 0,
                        skippedIncompleteCount: 0,
                        failedDownloadMonths: 0
                    )
                ),
                runtimeExecuting: true
            )
        )
        XCTAssertTrue(
            isBlocking(
                .failed("Failed"),
                runtimeExecuting: true
            )
        )
    }

    func testTerminalPresentationDoesNotHoldExecutionLease() {
        let completed = MacBackupExecutionState.completed(
            MacBackupExecutionSummary(
                upload: nil,
                restoredCount: 0,
                skippedIncompleteCount: 0,
                failedDownloadMonths: 0
            )
        )

        XCTAssertTrue(completed.isActive)
        XCTAssertFalse(completed.requiresExecutionLease)
        XCTAssertTrue(
            MacBackupExecutionState.failed("Failed").isActive
        )
        XCTAssertFalse(
            MacBackupExecutionState.failed("Failed")
                .requiresExecutionLease
        )
    }

    func testStopRequestEligibilityRejectsTerminalResults() {
        let completed = MacBackupExecutionState.completed(
            MacBackupExecutionSummary(
                upload: nil,
                restoredCount: 0,
                skippedIncompleteCount: 0,
                failedDownloadMonths: 0
            )
        )

        XCTAssertFalse(
            completed.requiresSafeStopBeforeTermination
        )
        XCTAssertFalse(
            MacBackupExecutionState.failed("Failed")
                .requiresSafeStopBeforeTermination
        )
        XCTAssertTrue(
            MacBackupExecutionState.paused(.upload)
                .requiresSafeStopBeforeTermination
        )
        XCTAssertTrue(
            MacBackupExecutionState.stopping
                .requiresSafeStopBeforeTermination
        )
        XCTAssertFalse(completed.acceptsStopRequest)
        XCTAssertFalse(
            MacBackupExecutionState.failed("Failed")
                .acceptsStopRequest
        )
    }

    func testStopRequestBecomesIdempotentWhileSafeStopFinishes() {
        XCTAssertTrue(
            MacBackupExecutionState.preflighting(
                processed: 0,
                total: 1,
                message: ""
            ).acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.uploading(nil)
                .acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.downloading(
                month: LibraryMonthKey(year: 2026, month: 7),
                itemPosition: 1,
                totalItems: 2
            ).acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.pausing.acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.paused(.upload)
                .acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.resuming(.download)
                .acceptsStopRequest
        )
        XCTAssertFalse(
            MacBackupExecutionState.stopping.acceptsStopRequest
        )
        XCTAssertTrue(
            MacBackupExecutionState.stopping
                .requiresSafeStopBeforeTermination
        )
    }

    func testRunningAndPausedStatesHoldExecutionLease() {
        XCTAssertTrue(
            MacBackupExecutionState.preflighting(
                processed: 0,
                total: 1,
                message: ""
            ).requiresExecutionLease
        )
        XCTAssertTrue(
            MacBackupExecutionState.uploading(nil)
                .requiresExecutionLease
        )
        XCTAssertTrue(
            MacBackupExecutionState.downloading(
                month: LibraryMonthKey(year: 2026, month: 7),
                itemPosition: 1,
                totalItems: 2
            ).requiresExecutionLease
        )
        XCTAssertTrue(
            MacBackupExecutionState.paused(.upload)
                .requiresExecutionLease
        )
        XCTAssertTrue(
            MacBackupExecutionState.resuming(.download)
                .requiresExecutionLease
        )
        XCTAssertTrue(
            MacBackupExecutionState.stopping
                .requiresExecutionLease
        )
    }

    func testRunningOrPausedManualBackupBlocksTermination() {
        XCTAssertTrue(
            isBlocking(
                .preflighting(
                    processed: 0,
                    total: 1,
                    message: ""
                ),
                runtimeExecuting: true
            )
        )
        XCTAssertTrue(
            isBlocking(
                .paused(.download),
                runtimeExecuting: true
            )
        )
    }

    func testExternalActivitiesStillBlockTermination() {
        XCTAssertTrue(
            isBlocking(.idle, runtimeExecuting: true)
        )
        XCTAssertFalse(
            isBlocking(.idle, runtimeExecuting: false)
        )
    }

    func testOnlyTerminalResultsAreDismissedWhenMainWindowCloses() {
        XCTAssertTrue(
            MacBackupExecutionState.completed(
                MacBackupExecutionSummary(
                    upload: nil,
                    restoredCount: 0,
                    skippedIncompleteCount: 0,
                    failedDownloadMonths: 0
                )
            ).isAwaitingResultDismissal
        )
        XCTAssertTrue(
            MacBackupExecutionState.failed("Failed")
                .isAwaitingResultDismissal
        )
        XCTAssertFalse(
            MacBackupExecutionState.paused(.upload)
                .isAwaitingResultDismissal
        )
        XCTAssertFalse(
            MacBackupExecutionState.downloading(
                month: LibraryMonthKey(year: 2026, month: 7),
                itemPosition: 1,
                totalItems: 2
            ).isAwaitingResultDismissal
        )
    }

    private func isBlocking(
        _ state: MacBackupExecutionState,
        runtimeExecuting: Bool
    ) -> Bool {
        MacExecutionTerminationPolicy.isBlocking(
            manualBackupState: state,
            runtimeExecuting: runtimeExecuting
        )
    }
}
