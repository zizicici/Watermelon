import XCTest
@testable import Watermelon

final class BackupV2RepoOpenPlannerTests: XCTestCase {
    func testFresh_returnsBootstrapFresh() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(inspection: .fresh, allowMigration: false),
            .bootstrapFresh
        )
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(inspection: .fresh, allowMigration: true),
            .bootstrapFresh
        )
    }

    func testV1_allowMigrationTrue_returnsMigrateFromV1() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(inspection: .v1, allowMigration: true),
            .migrateFromV1
        )
    }

    func testV1_allowMigrationFalse_returnsThrowRequiresForegroundMigration() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(inspection: .v1, allowMigration: false),
            .throwRequiresForegroundMigration
        )
    }

    func testV2WithV1Manifests_allowMigrationTrue_returnsMigrateFromV1() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .v2WithV1Manifests(formatVersion: 2),
                allowMigration: true
            ),
            .migrateFromV1
        )
    }

    func testV2WithV1Manifests_allowMigrationFalse_returnsThrowRequiresForegroundMigration() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .v2WithV1Manifests(formatVersion: 2),
                allowMigration: false
            ),
            .throwRequiresForegroundMigration
        )
    }

    func testV2_returnsOpenExistingV2() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .v2(formatVersion: 2),
                allowMigration: false
            ),
            .openExistingV2
        )
    }

    func testV2WithPendingMigrationCleanup_returnsOpenWithCleanupV2_carryingOwnerWriterID() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .v2WithPendingMigrationCleanup(
                    formatVersion: 2,
                    ownerWriterID: "writer-7"
                ),
                allowMigration: false
            ),
            .openWithCleanupV2(ownerWriterID: "writer-7")
        )
    }

    func testUnsupported_returnsThrowUnsupported_carryingMinAppVersion() {
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .unsupported(minAppVersion: "12.3.4"),
                allowMigration: false
            ),
            .throwUnsupported(minAppVersion: "12.3.4")
        )
        XCTAssertEqual(
            BackupV2RepoOpenPlanner.plan(
                inspection: .unsupported(minAppVersion: nil),
                allowMigration: false
            ),
            .throwUnsupported(minAppVersion: nil)
        )
    }
}
