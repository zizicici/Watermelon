import XCTest
@testable import Watermelon

/// `prepareRun` (and the verifyMonthV2 tombstone-lease catch) drop the stale committed view only when
/// `isDeterministicCompatibilityRefusal` returns true. A deterministic damaged-V2 open refusal must
/// fail closed for Home/read-state, matching `syncIndex`/`verifyMonth`; transport/cancellation errors
/// (which arrive as non-compatibility errors) must not drop a still-valid view.
final class BackupRunPreparationRefusalTests: XCTestCase {
    func testDamagedV2RepoIsTreatedAsRefusal() {
        XCTAssertTrue(
            BackupRunPreparationService.isDeterministicCompatibilityRefusal(BackupCompatibilityError.damagedV2Repo),
            "a damaged-V2 open failure must drop the stale committed view, like syncIndex/verifyMonth"
        )
    }

    func testAllCompatibilityRefusalsDropView() {
        let refusals: [BackupCompatibilityError] = [
            .repoIdentityMismatch(stored: "a", observed: "b"),
            .remoteFormatUnsupported(minAppVersion: "9.9.9"),
            .requiresForegroundMigration,
            .repoFormatRegression(repoID: "r"),
            .damagedV2Repo,
        ]
        for refusal in refusals {
            XCTAssertTrue(
                BackupRunPreparationService.isDeterministicCompatibilityRefusal(refusal),
                "\(refusal) must be treated as a fail-closed refusal"
            )
        }
    }

    func testNonCompatibilityErrorsDoNotDropView() {
        XCTAssertFalse(
            BackupRunPreparationService.isDeterministicCompatibilityRefusal(CancellationError()),
            "cancellation must not drop the committed view"
        )
        XCTAssertFalse(
            BackupRunPreparationService.isDeterministicCompatibilityRefusal(
                NSError(domain: "transport", code: -1009)
            ),
            "a raw transport error must not drop the committed view"
        )
    }
}
