import XCTest
@testable import WatermelonMac

final class MacMonthExecutionTrackerTests: XCTestCase {
    private let backup = LibraryMonthKey(year: 2024, month: 1)
    private let download = LibraryMonthKey(year: 2024, month: 2)
    private let complement = LibraryMonthKey(year: 2024, month: 3)

    func testTracksUploadDownloadAndComplementCompletion() {
        var tracker = makeTracker()

        XCTAssertEqual(tracker.phases[backup], .pending)
        XCTAssertEqual(tracker.phases[download], .pending)
        XCTAssertEqual(tracker.phases[complement], .pending)

        tracker.beginUpload(backup)
        tracker.beginUpload(complement)
        XCTAssertEqual(tracker.phases[backup], .uploading)
        XCTAssertEqual(tracker.phases[complement], .uploading)

        tracker.complete(backup)
        tracker.beginDownload(complement)
        tracker.complete(complement)
        tracker.beginDownload(download)
        tracker.complete(download)

        XCTAssertEqual(tracker.phases[backup], .completed)
        XCTAssertEqual(tracker.phases[download], .completed)
        XCTAssertEqual(tracker.phases[complement], .completed)
    }

    func testPauseAndResumeOnlyAffectCurrentStage() {
        var tracker = makeTracker()
        tracker.beginUpload(backup)
        tracker.beginDownload(download)

        tracker.pause(.upload)
        XCTAssertEqual(tracker.phases[backup], .uploadPaused)
        XCTAssertEqual(tracker.phases[download], .downloading)

        tracker.resume(.upload)
        tracker.pause(.download)
        XCTAssertEqual(tracker.phases[backup], .uploading)
        XCTAssertEqual(tracker.phases[download], .downloadPaused)

        tracker.resume(.download)
        XCTAssertEqual(tracker.phases[download], .downloading)
    }

    func testItemFailureBecomesPartialFailureAtCommit() {
        var tracker = makeTracker()
        tracker.beginUpload(backup)
        tracker.recordItemFailure(backup)
        tracker.complete(backup)

        tracker.beginDownload(download)
        tracker.complete(download, hasIssues: true)

        XCTAssertEqual(
            tracker.phases[backup],
            .partiallyFailed
        )
        XCTAssertEqual(
            tracker.phases[download],
            .partiallyFailed
        )
    }

    func testTerminalProjectionPreservesFailures() {
        var tracker = makeTracker()
        tracker.beginDownload(download)
        tracker.fail(download)
        tracker.completeRemaining()

        XCTAssertEqual(tracker.phases[download], .failed)
        XCTAssertEqual(tracker.phases[backup], .completed)
        XCTAssertEqual(tracker.phases[complement], .completed)

        tracker.failRemaining()
        XCTAssertEqual(tracker.phases[download], .failed)
        XCTAssertEqual(tracker.phases[backup], .completed)
        XCTAssertEqual(tracker.phases[complement], .completed)
    }

    func testUploadCommitFailureMatchesMonthIntent() {
        var tracker = makeTracker()
        tracker.apply(monthEvent(.started, month: backup))
        tracker.apply(monthEvent(.started, month: complement))

        tracker.apply(
            monthEvent(
                .uploadFailed(
                    resumableAssetLocalIdentifiers: ["backup"],
                    failedItemCount: 1
                ),
                month: backup
            )
        )
        tracker.apply(
            monthEvent(
                .uploadFailed(
                    resumableAssetLocalIdentifiers: ["complement"],
                    failedItemCount: 1
                ),
                month: complement
            )
        )

        XCTAssertEqual(tracker.phases[backup], .partiallyFailed)
        XCTAssertEqual(tracker.phases[complement], .failed)
    }

    func testUploadRollbackForPauseRemainsResumable() {
        var tracker = makeTracker()
        tracker.apply(monthEvent(.started, month: backup))
        tracker.apply(
            monthEvent(
                .uploadFailed(
                    resumableAssetLocalIdentifiers: ["backup"],
                    failedItemCount: 0
                ),
                month: backup
            )
        )
        tracker.pause(.upload)

        XCTAssertEqual(tracker.phases[backup], .uploadPaused)
    }

    func testUploadProgressDeduplicatesItemEvents() {
        var tracker = makeTracker()
        tracker.apply(monthEvent(.started, month: backup))
        tracker.apply(progressEvent(assetID: "a", month: backup))
        tracker.apply(progressEvent(assetID: "a", month: backup))
        tracker.apply(progressEvent(assetID: "b", month: backup))

        XCTAssertEqual(
            tracker.progress[backup]?.uploadProcessedCount,
            2
        )
        XCTAssertEqual(
            tracker.progress[backup]?.uploadTotalCount,
            2
        )
        XCTAssertEqual(
            tracker.progress[complement]?.uploadTotalCount,
            0
        )
    }

    func testDownloadProgressUsesAssetAndResourcePosition() {
        var tracker = makeTracker()
        tracker.beginDownload(download)
        tracker.recordDownloadTransfer(
            BackupTransferState(
                kind: .download,
                workerID: 1,
                assetLocalIdentifier: "remote",
                assetDisplayName: "IMG",
                resourceDate: nil,
                assetPosition: 2,
                totalAssets: 4,
                resourceDisplayName: "IMG.HEIC",
                resourcePosition: 2,
                totalResources: 2,
                resourceFraction: 0.5,
                resourceBytesTransferred: nil,
                resourceTotalBytes: nil,
                countsTowardTransferSpeed: true,
                stageDescription: ""
            ),
            month: download
        )

        XCTAssertEqual(
            tracker.progress[download]?.downloadFraction,
            0.437
        )
    }

    private func makeTracker() -> MacMonthExecutionTracker {
        MacMonthExecutionTracker(
            backupMonths: [backup],
            downloadMonths: [download],
            complementMonths: [complement],
            localAssetIDsByMonth: [
                backup: ["a", "b"],
                complement: ["c"],
            ]
        )
    }

    private func monthEvent(
        _ action: MonthChangeEvent.MonthAction,
        month: LibraryMonthKey
    ) -> BackupEvent {
        .monthChanged(
            MonthChangeEvent(
                year: month.year,
                month: month.month,
                action: action
            )
        )
    }

    private func progressEvent(
        assetID: String,
        month: LibraryMonthKey
    ) -> BackupEvent {
        .progress(
            BackupProgress(
                succeeded: 1,
                failed: 0,
                skipped: 0,
                total: 2,
                message: "",
                logMessage: nil,
                logLevel: .info,
                itemEvent: BackupItemEvent(
                    assetLocalIdentifier: assetID,
                    assetFingerprint: nil,
                    month: month,
                    displayName: assetID,
                    resourceDate: nil,
                    status: .success,
                    reason: nil,
                    updatedAt: Date()
                ),
                transferState: nil
            )
        )
    }
}
