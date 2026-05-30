import XCTest
@testable import Watermelon

/// Pins the verify-success stamping policy: only genuine unrepaired damage
/// (partiallyMissing / fingerprintMismatch) withholds the "verified OK" timestamp.
/// Budget-incomplete (verificationIncomplete) is routine and must still stamp.
final class RemoteMaintenanceStampDecisionTests: XCTestCase {

    func testCleanOutcomeStamps() {
        XCTAssertTrue(RemoteMaintenanceController.shouldStampVerifiedAt(for: .clean))
    }

    func testMutatedOutcomeStamps() {
        XCTAssertTrue(RemoteMaintenanceController.shouldStampVerifiedAt(for: .mutated))
    }

    func testPartiallyMissingDamageWithholdsStamp() {
        XCTAssertFalse(
            RemoteMaintenanceController.shouldStampVerifiedAt(for: .damaged(kinds: [.partiallyMissing]))
        )
    }

    func testFingerprintMismatchDamageWithholdsStamp() {
        XCTAssertFalse(
            RemoteMaintenanceController.shouldStampVerifiedAt(for: .damaged(kinds: [.fingerprintMismatch]))
        )
    }

    func testAggregatedDamageAmongCleanMonthsWithholdsStamp() {
        let aggregate = MonthVerifyOutcome.clean
            .combined(with: .clean)
            .combined(with: .damaged(kinds: [.partiallyMissing]))
            .combined(with: .mutated)
        XCTAssertFalse(RemoteMaintenanceController.shouldStampVerifiedAt(for: aggregate))
    }

    func testBudgetIncompleteAggregateStamps() {
        // A run of healthy months that all hit the content-trust budget classify clean → still stamps.
        let aggregate = MonthVerifyOutcome.clean.combined(with: .clean).combined(with: .mutated)
        XCTAssertTrue(RemoteMaintenanceController.shouldStampVerifiedAt(for: aggregate))
    }
}
