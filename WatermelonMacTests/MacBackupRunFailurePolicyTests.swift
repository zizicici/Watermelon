import XCTest
@testable import WatermelonMac

final class MacBackupRunFailurePolicyTests: XCTestCase {
    func testPauseCommandPausesTaskCancellationWithRemainingWork() {
        XCTAssertEqual(
            disposition(
                intent: .pause,
                source: .taskCancellation,
                allOperationsCommitted: false
            ),
            .pause
        )
    }

    func testPauseCommandCompletesWhenCommitBoundaryFinishedThePlan() {
        XCTAssertEqual(
            disposition(
                intent: .pause,
                source: .taskCancellation,
                allOperationsCommitted: true
            ),
            .complete
        )
    }

    func testUncommandedTaskCancellationCancelsPresentation() {
        XCTAssertEqual(
            disposition(
                intent: .none,
                source: .taskCancellation,
                allOperationsCommitted: false
            ),
            .cancel
        )
    }

    func testRecoverableFailurePausesWithoutACommand() {
        XCTAssertEqual(
            disposition(
                intent: .none,
                source: .recoverable,
                allOperationsCommitted: false
            ),
            .pause
        )
    }

    func testClassifiedCancellationCompletesCommittedPlan() {
        XCTAssertEqual(
            disposition(
                intent: .none,
                source: .classifiedCancellation,
                allOperationsCommitted: true
            ),
            .complete
        )
    }

    func testStopCommandOverridesRecoverableFailure() {
        XCTAssertEqual(
            disposition(
                intent: .stop,
                source: .recoverable,
                allOperationsCommitted: false
            ),
            .cancel
        )
    }

    func testPauseCommandOverridesFatalFailure() {
        XCTAssertEqual(
            disposition(
                intent: .pause,
                source: .fatal,
                allOperationsCommitted: false
            ),
            .pause
        )
    }

    func testFatalFailureWithoutCommandFails() {
        XCTAssertEqual(
            disposition(
                intent: .none,
                source: .fatal,
                allOperationsCommitted: false
            ),
            .fail
        )
    }

    private func disposition(
        intent: ExecutionTerminationIntent,
        source: MacBackupRunFailureSource,
        allOperationsCommitted: Bool
    ) -> MacBackupRunFailureDisposition {
        MacBackupRunFailurePolicy.disposition(
            intent: intent,
            source: source,
            allOperationsCommitted: allOperationsCommitted
        )
    }
}
