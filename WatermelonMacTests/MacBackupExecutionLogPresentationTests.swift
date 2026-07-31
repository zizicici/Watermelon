import XCTest
@testable import WatermelonMac

final class MacBackupExecutionLogPresentationTests:
    XCTestCase {
    func testStatusUsesCurrentUploadProgress() {
        let progress = BackupProgress(
            succeeded: 5,
            failed: 1,
            skipped: 2,
            total: 12,
            message: "",
            logMessage: nil,
            logLevel: .info,
            itemEvent: nil,
            transferState: nil
        )

        XCTAssertEqual(
            MacBackupExecutionLogPresentation.statusText(
                for: .uploading(progress)
            ),
            String.localizedStringWithFormat(
                String(
                    localized: "mac.execution.uploadProgress"
                ),
                Int64(8),
                Int64(12)
            )
        )
    }

    func testSharedTrackerProducesSpeedAndRemainingTime() {
        var tracker = HomeExecutionTransferTracker()
        tracker.updateTotalBytes(1_000)

        _ = tracker.record(
            transferState(bytes: 100, fraction: 0.1),
            now: 100
        )
        let metrics = tracker.record(
            transferState(bytes: 600, fraction: 0.6),
            now: 102
        )

        XCTAssertEqual(metrics.progressFraction, 0.6)
        XCTAssertEqual(metrics.speedBytesPerSecond, 250)
        XCTAssertEqual(metrics.remainingTimeSeconds, 1.6)
    }

    func testSharedFormatterIsAvailableToMacLog() {
        XCTAssertNotNil(
            HomeExecutionTransferFormatter.speed(1_024)
        )
        XCTAssertNotNil(
            HomeExecutionTransferFormatter.remainingTime(90)
        )
    }

    private func transferState(
        bytes: Int64,
        fraction: Float
    ) -> BackupTransferState {
        BackupTransferState(
            kind: .download,
            workerID: 1,
            assetLocalIdentifier: "asset",
            assetDisplayName: "IMG_0001.HEIC",
            resourceDate: nil,
            assetPosition: 1,
            totalAssets: 1,
            resourceDisplayName: "IMG_0001.HEIC",
            resourcePosition: 1,
            totalResources: 1,
            resourceFraction: fraction,
            resourceBytesTransferred: bytes,
            resourceTotalBytes: 1_000,
            countsTowardTransferSpeed: true,
            stageDescription: ""
        )
    }
}
