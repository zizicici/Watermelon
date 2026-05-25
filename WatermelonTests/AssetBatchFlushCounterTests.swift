import XCTest
@testable import Watermelon

final class AssetBatchFlushCounterTests: XCTestCase {

    func testCounter_InitialStateNoThreshold() {
        // A fresh counter must not report threshold-reached until recordSuccessAndCheckThreshold is called.
        var counter = AssetBatchFlushCounter(threshold: 3)
        // No recordSuccessAndCheckThreshold call yet; we don't have a separate "peek" API, so we
        // verify by recording the first success: at threshold=3, the first call MUST return false.
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
    }

    func testCounter_BelowThresholdNoTrip() {
        var counter = AssetBatchFlushCounter(threshold: 3)
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
    }

    func testCounter_AtThresholdTrips() {
        var counter = AssetBatchFlushCounter(threshold: 3)
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertTrue(counter.recordSuccessAndCheckThreshold(),
                      "Nth call (N == threshold) must return true")
    }

    func testCounter_ResetAfterTrip() {
        var counter = AssetBatchFlushCounter(threshold: 3)
        _ = counter.recordSuccessAndCheckThreshold()
        _ = counter.recordSuccessAndCheckThreshold()
        XCTAssertTrue(counter.recordSuccessAndCheckThreshold())
        counter.reset()
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold(),
                       "After reset, counter must start fresh")
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertTrue(counter.recordSuccessAndCheckThreshold(),
                      "After reset, threshold must be reached on the Nth call again")
    }

    func testCounter_ResetWithoutTrip() {
        // The .continueAssetLoopAndResetCounter dispatch branch resets without tripping; verify the
        // counter behaves correctly: after partial increments + reset, threshold needs N more.
        var counter = AssetBatchFlushCounter(threshold: 3)
        _ = counter.recordSuccessAndCheckThreshold()
        counter.reset()
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertFalse(counter.recordSuccessAndCheckThreshold())
        XCTAssertTrue(counter.recordSuccessAndCheckThreshold())
    }

    func testCounter_CustomThreshold() {
        var counter = AssetBatchFlushCounter(threshold: 1)
        XCTAssertTrue(counter.recordSuccessAndCheckThreshold(),
                      "threshold=1 must trip on the first call")
    }

    func testCounter_DefaultThresholdMatchesBackupV2Constants() {
        // Pins the default initializer to BackupV2Constants.batchFlushInterval. If a future change
        // alters the default, this test fails loudly rather than silently shifting flush cadence.
        let counter = AssetBatchFlushCounter()
        XCTAssertEqual(counter.threshold, BackupV2Constants.batchFlushInterval)
    }
}
