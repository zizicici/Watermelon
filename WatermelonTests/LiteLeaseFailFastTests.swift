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

    func testBackgroundRunFatalOnlyForOwnershipLost() {
        XCTAssertTrue(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.ownershipLost))
        // A lost-confidence upload error is recoverable by the refresh loop, so background does not
        // terminate the whole run on it; only a true ownership loss is run-fatal.
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(LiteRepoError.leaseConfidenceLost))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(RemoteErrorFixtures.retryable))
        XCTAssertFalse(BackgroundBackupRunner.isLeaseRunFatal(CancellationError()))
    }
}
