import XCTest
@testable import Watermelon

final class BackupV2RepoVerifyPlannerTests: XCTestCase {
    func testUnsupported_carriesMinAppVersion_indifferentToPriorBinding() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .unsupported(minAppVersion: "3.4.5"),
                hasPriorV2Binding: false
            ),
            .throwUnsupported(minAppVersion: "3.4.5")
        )
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .unsupported(minAppVersion: "3.4.5"),
                hasPriorV2Binding: true
            ),
            .throwUnsupported(minAppVersion: "3.4.5")
        )
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .unsupported(minAppVersion: nil),
                hasPriorV2Binding: false
            ),
            .throwUnsupported(minAppVersion: nil)
        )
    }

    func testV2_returnsVerifyMonthV2_indifferentToPriorBinding() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2(formatVersion: 2),
                hasPriorV2Binding: false
            ),
            .verifyMonthV2
        )
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2(formatVersion: 2),
                hasPriorV2Binding: true
            ),
            .verifyMonthV2
        )
    }

    func testV2WithPendingMigrationCleanup_returnsVerifyMonthV2_indifferentToPriorBinding() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: "w"),
                hasPriorV2Binding: false
            ),
            .verifyMonthV2
        )
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: "w"),
                hasPriorV2Binding: true
            ),
            .verifyMonthV2
        )
    }

    func testV2WithV1Manifests_returnsThrowRequiresForegroundMigration_indifferentToPriorBinding() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2WithV1Manifests(formatVersion: 2),
                hasPriorV2Binding: false
            ),
            .throwRequiresForegroundMigration
        )
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v2WithV1Manifests(formatVersion: 2),
                hasPriorV2Binding: true
            ),
            .throwRequiresForegroundMigration
        )
    }

    func testV1_noPriorBinding_returnsVerifyMonthV1() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v1,
                hasPriorV2Binding: false
            ),
            .verifyMonthV1
        )
    }

    func testV1_withPriorBinding_returnsThrowRequiresForegroundMigration() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .v1,
                hasPriorV2Binding: true
            ),
            .throwRequiresForegroundMigration
        )
    }

    func testFresh_noPriorBinding_returnsSkipFreshRepo() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .fresh,
                hasPriorV2Binding: false
            ),
            .skipFreshRepo
        )
    }

    func testFresh_withPriorBinding_returnsThrowDamagedV2Repo() {
        XCTAssertEqual(
            BackupV2RepoVerifyPlanner.plan(
                inspection: .fresh,
                hasPriorV2Binding: true
            ),
            .throwDamagedV2Repo
        )
    }
}
