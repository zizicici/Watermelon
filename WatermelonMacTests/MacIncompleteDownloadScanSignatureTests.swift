import XCTest
@testable import WatermelonMac

final class MacIncompleteDownloadScanSignatureTests: XCTestCase {
    private let backup = LibraryMonthKey(year: 2025, month: 1)
    private let download = LibraryMonthKey(year: 2025, month: 2)
    private let complement = LibraryMonthKey(year: 2025, month: 3)

    func testSignatureIncludesOnlyMonthsThatCanDownload() {
        let signature = makePlan()
            .incompleteDownloadScanSignature

        XCTAssertEqual(signature.months, [download, complement])
        XCTAssertEqual(
            signature.localAssetIDsByMonth,
            [
                download: ["download-local"],
                complement: ["complement-local"],
            ]
        )
    }

    func testDownloadSideLibraryChangeInvalidatesSignature() {
        let original = makePlan()
            .incompleteDownloadScanSignature
        let changed = makePlan(
            downloadAssetIDs: ["download-local", "new-local"]
        ).incompleteDownloadScanSignature

        XCTAssertNotEqual(original, changed)
    }

    func testBackupOnlyLibraryChangeDoesNotInvalidateSignature() {
        let original = makePlan()
            .incompleteDownloadScanSignature
        let changed = makePlan(
            backupAssetIDs: ["backup-local", "new-local"]
        ).incompleteDownloadScanSignature

        XCTAssertEqual(original, changed)
    }

    private func makePlan(
        backupAssetIDs: Set<String> = ["backup-local"],
        downloadAssetIDs: Set<String> = ["download-local"]
    ) -> MacBackupExecutionPlan {
        MacBackupExecutionPlan(
            backupMonths: [backup],
            downloadMonths: [download],
            complementMonths: [complement],
            localAssetIDsByMonth: [
                backup: backupAssetIDs,
                download: downloadAssetIDs,
                complement: ["complement-local"],
            ],
            monthGroupingTimeZone: .fixedUTC(),
            incompleteDownloadPolicy: .skip
        )
    }
}
