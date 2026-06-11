import XCTest
@testable import Watermelon

// Phase 2 lease-failure routing classifiers. The retry/loop wiring that consumes these is coupled to the
// photo library (PreparedResource wraps a real PHAsset; BackgroundBackupRunner fetches PHAssets), so the
// decision is extracted and unit-tested here while the call-site wiring is verified by inspection:
// - AssetProcessor+Upload.performUploadWithRetry rethrows before any retry/sleep/name-collision branch.
// - BackgroundBackupRunner.runBackupLoop `break monthLoop`s the whole run at each lease-fatal catch.
final class LiteLeaseFailFastTests: XCTestCase {
    func testUploadFailFastForLeaseAndOwnershipErrors() {
        XCTAssertTrue(AssetProcessor.isLeaseFailFast(LiteRepoError.leaseConfidenceLost))
        XCTAssertTrue(AssetProcessor.isLeaseFailFast(LiteRepoError.ownershipLost))
    }

    func testUploadDoesNotFailFastForRetryableTerminalOrCancelled() {
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(RemoteErrorFixtures.retryable))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(RemoteErrorFixtures.terminal))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(CancellationError()))
        XCTAssertFalse(AssetProcessor.isLeaseFailFast(LiteRepoError.lockConflict),
                       "only lease/ownership loss is fail-fast, not every Lite error")
    }

    func testBackgroundRunFatalForLeaseAndOwnershipLoss() {
        XCTAssertTrue(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.ownershipLost))
        XCTAssertTrue(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.leaseConfidenceLost))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(RemoteErrorFixtures.retryable))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(CancellationError()))
    }

    func testFinalizationFailureConvertsOnlyCurrentMonthSuccessAndSkippedCounts() async {
        let aggregator = ParallelBackupProgressAggregator(total: 5)
        _ = await aggregator.record(result: Self.result(.success))
        _ = await aggregator.record(result: Self.result(.skipped))
        _ = await aggregator.record(result: Self.result(.success))

        let progress = await aggregator.recordFinalizationFailure(
            BackupMonthProgressCounts(succeeded: 1, skipped: 1, failed: 0)
        )

        XCTAssertEqual(progress.state.total, 5)
        XCTAssertEqual(progress.state.succeeded, 1)
        XCTAssertEqual(progress.state.skipped, 0)
        XCTAssertEqual(progress.state.failed, 2)
        XCTAssertEqual(progress.state.processed, 3)
    }

    private static func result(_ status: BackupItemStatus) -> AssetProcessResult {
        AssetProcessResult(
            status: status,
            reason: nil,
            displayName: "asset",
            assetFingerprint: nil,
            timing: AssetProcessTiming(),
            totalFileSizeBytes: 0,
            uploadedFileSizeBytes: 0
        )
    }
}
