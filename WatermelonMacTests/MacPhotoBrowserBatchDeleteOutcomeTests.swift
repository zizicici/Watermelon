import XCTest
@testable import WatermelonMac

final class MacPhotoBrowserBatchDeleteOutcomeTests: XCTestCase {
    func testCancellationPreservesCommittedChanges() {
        let local = MacPhotoBrowserBatchDeleteOutcome.cancelled(
            localChanged: true,
            remoteChanged: false
        )
        XCTAssertTrue(local.localChanged)
        XCTAssertFalse(local.remoteChanged)
        XCTAssertTrue(local.shouldReload)

        let remote = MacPhotoBrowserBatchDeleteOutcome.cancelled(
            localChanged: false,
            remoteChanged: true
        )
        XCTAssertFalse(remote.localChanged)
        XCTAssertTrue(remote.remoteChanged)
        XCTAssertTrue(remote.shouldReload)
    }

    func testCancellationBeforeCommitNeedsNoReload() {
        let outcome = MacPhotoBrowserBatchDeleteOutcome.cancelled(
            localChanged: false,
            remoteChanged: false
        )

        XCTAssertFalse(outcome.localChanged)
        XCTAssertFalse(outcome.remoteChanged)
        XCTAssertEqual(outcome.failedCount, 0)
        XCTAssertFalse(outcome.shouldReload)
    }

    func testCompletedOutcomeCarriesFailuresAndReloads() {
        let outcome = MacPhotoBrowserBatchDeleteOutcome.completed(
            localChanged: true,
            remoteChanged: true,
            failed: 2
        )

        XCTAssertTrue(outcome.localChanged)
        XCTAssertTrue(outcome.remoteChanged)
        XCTAssertEqual(outcome.failedCount, 2)
        XCTAssertTrue(outcome.shouldReload)
    }
}
