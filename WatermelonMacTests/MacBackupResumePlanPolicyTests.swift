import XCTest
@testable import WatermelonMac

final class MacBackupResumePlanPolicyTests: XCTestCase {
    private let directBackup = LibraryMonthKey(year: 2025, month: 1)
    private let directDownload = LibraryMonthKey(year: 2025, month: 2)
    private let complement = LibraryMonthKey(year: 2025, month: 3)

    func testUntouchedPlanKeepsOriginalOperationKinds() {
        let plan = makePlan()

        let remaining = MacBackupResumePlanPolicy.remainingPlan(
            from: plan,
            completedUploadMonths: [],
            completedDownloadMonths: []
        )

        XCTAssertEqual(remaining.backupMonths, [directBackup])
        XCTAssertEqual(remaining.downloadMonths, [directDownload])
        XCTAssertEqual(remaining.complementMonths, [complement])
        XCTAssertEqual(remaining.localAssetIDsByMonth, plan.localAssetIDsByMonth)
        XCTAssertEqual(
            remaining.monthGroupingTimeZone,
            plan.monthGroupingTimeZone
        )
        XCTAssertEqual(
            remaining.incompleteDownloadPolicy,
            plan.incompleteDownloadPolicy
        )
    }

    func testComplementWithDurableUploadResumesAsDownloadOnly() {
        let remaining = MacBackupResumePlanPolicy.remainingPlan(
            from: makePlan(),
            completedUploadMonths: [complement],
            completedDownloadMonths: []
        )

        XCTAssertFalse(remaining.complementMonths.contains(complement))
        XCTAssertTrue(remaining.downloadMonths.contains(complement))
        XCTAssertFalse(remaining.uploadMonths.contains(complement))
        XCTAssertFalse(
            remaining.uploadAssetIDs.contains("complement-local")
        )
        XCTAssertTrue(
            remaining.requestedLocalAssetIDs.contains("complement-local")
        )
    }

    func testComplementWithCompletedDownloadResumesAsBackupOnly() {
        let remaining = MacBackupResumePlanPolicy.remainingPlan(
            from: makePlan(),
            completedUploadMonths: [],
            completedDownloadMonths: [complement]
        )

        XCTAssertFalse(remaining.complementMonths.contains(complement))
        XCTAssertTrue(remaining.backupMonths.contains(complement))
        XCTAssertFalse(remaining.downloadMonths.contains(complement))
    }

    func testCompletedOperationsAreRemovedFromResumePlan() {
        let remaining = MacBackupResumePlanPolicy.remainingPlan(
            from: makePlan(),
            completedUploadMonths: [directBackup, complement],
            completedDownloadMonths: [directDownload, complement]
        )

        XCTAssertTrue(remaining.backupMonths.isEmpty)
        XCTAssertTrue(remaining.downloadMonths.isEmpty)
        XCTAssertTrue(remaining.complementMonths.isEmpty)
        XCTAssertTrue(remaining.allMonths.isEmpty)
    }

    func testInterruptionFinishesWhenEveryOperationCommitted() {
        let disposition =
            MacBackupResumePlanPolicy.interruptionDisposition(
                for: makePlan(),
                completedUploadMonths: [directBackup, complement],
                completedDownloadMonths: [directDownload, complement]
            )

        XCTAssertEqual(disposition, .finish)
    }

    func testInterruptionPausesWhenComplementDownloadRemains() {
        let disposition =
            MacBackupResumePlanPolicy.interruptionDisposition(
                for: makePlan(),
                completedUploadMonths: [directBackup, complement],
                completedDownloadMonths: [directDownload]
            )

        XCTAssertEqual(disposition, .pause)
    }

    private func makePlan() -> MacBackupExecutionPlan {
        MacBackupExecutionPlan(
            backupMonths: [directBackup],
            downloadMonths: [directDownload],
            complementMonths: [complement],
            localAssetIDsByMonth: [
                directBackup: ["backup-local"],
                directDownload: ["download-local"],
                complement: ["complement-local"],
            ],
            monthGroupingTimeZone: .fixedUTC(),
            incompleteDownloadPolicy: .createNewAsset
        )
    }
}
