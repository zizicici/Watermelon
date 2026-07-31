import XCTest
@testable import WatermelonMac

final class LegacyMigrationTerminalPolicyTests: XCTestCase {
    func testCancellationPreservesPartialTotals() {
        var totals = LegacyImportTotals()
        totals.bundlesImported = 2
        totals.monthsCommitted = 1

        let event = LegacyMigrationTerminalPolicy.event(
            for: CancellationError(),
            totals: totals
        )

        guard case .cancelled(let captured) = event else {
            return XCTFail("Expected cancelled event")
        }
        XCTAssertEqual(captured, totals)
    }

    func testFailurePreservesPartialTotals() {
        var totals = LegacyImportTotals()
        totals.bundlesImported = 3
        let error = NSError(
            domain: "LegacyMigrationTerminalPolicyTests",
            code: 1
        )

        let event = LegacyMigrationTerminalPolicy.event(
            for: error,
            totals: totals
        )

        guard case .failed(_, let captured) = event else {
            return XCTFail("Expected failed event")
        }
        XCTAssertEqual(captured, totals)
    }

    func testRefreshRequiresPossibleRepositoryMutation() {
        XCTAssertFalse(
            LegacyMigrationTerminalPolicy
                .shouldRefreshRemoteSnapshot(
                    after: LegacyImportTotals()
                )
        )

        var imported = LegacyImportTotals()
        imported.bundlesImported = 1
        XCTAssertTrue(
            LegacyMigrationTerminalPolicy
                .shouldRefreshRemoteSnapshot(after: imported)
        )

        var committed = LegacyImportTotals()
        committed.monthsCommitted = 1
        XCTAssertTrue(
            LegacyMigrationTerminalPolicy
                .shouldRefreshRemoteSnapshot(after: committed)
        )
    }
}
