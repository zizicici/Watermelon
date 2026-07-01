import XCTest
@testable import Watermelon

final class HomeExecutionTransferTrackerTests: XCTestCase {
    private func state(
        transferredBytes: Int64,
        totalBytes: Int64,
        fraction: Float? = nil,
        resourceName: String = "IMG_0001.JPG",
        countsTowardTransferSpeed: Bool = true
    ) -> BackupTransferState {
        BackupTransferState(
            kind: .upload,
            workerID: 1,
            assetLocalIdentifier: "asset-1",
            assetDisplayName: "IMG_0001",
            resourceDate: nil,
            assetPosition: 1,
            totalAssets: 1,
            resourceDisplayName: resourceName,
            resourcePosition: 1,
            totalResources: 1,
            resourceFraction: fraction ?? Float(Double(transferredBytes) / Double(totalBytes)),
            resourceBytesTransferred: transferredBytes,
            resourceTotalBytes: totalBytes,
            countsTowardTransferSpeed: countsTowardTransferSpeed,
            stageDescription: "Uploading"
        )
    }

    private func makeTracker(totalBytes: Int64) -> HomeExecutionTransferTracker {
        var tracker = HomeExecutionTransferTracker()
        tracker.updateTotalBytes(totalBytes)
        return tracker
    }

    func testRateUsesRecentProgressWithoutIdleDilution() {
        let mb: Int64 = 1_000_000
        let totalBytes: Int64 = 121_000 * mb
        var tracker = makeTracker(totalBytes: totalBytes)

        _ = tracker.record(state(transferredBytes: 119_000 * mb, totalBytes: totalBytes), now: 0)
        let metrics = tracker.record(state(transferredBytes: 119_041 * mb, totalBytes: totalBytes), now: 10)

        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, 4_100_000, accuracy: 1)
        XCTAssertLessThan(metrics.remainingTimeSeconds ?? .infinity, 600)

        let staleMetrics = tracker.snapshot(now: 21)
        XCTAssertNil(staleMetrics.speedBytesPerSecond)
        XCTAssertNil(staleMetrics.remainingTimeSeconds)
    }

    func testRateIsSmoothedInsideTenSecondWindow() {
        var tracker = makeTracker(totalBytes: 10_000)

        _ = tracker.record(state(transferredBytes: 1_000, totalBytes: 10_000), now: 0)
        _ = tracker.record(state(transferredBytes: 1_010, totalBytes: 10_000), now: 1)
        let metrics = tracker.record(state(transferredBytes: 6_010, totalBytes: 10_000), now: 10)

        let rawTenSecondAverage = 501.0
        XCTAssertGreaterThan(metrics.speedBytesPerSecond ?? 0, 300)
        XCTAssertLessThan(metrics.speedBytesPerSecond ?? .infinity, rawTenSecondAverage)

        let refreshedMetrics = tracker.snapshot(now: 11)
        XCTAssertEqual(refreshedMetrics.speedBytesPerSecond ?? 0, metrics.speedBytesPerSecond ?? 0, accuracy: 0.001)
    }

    func testRateSmoothingAdvancesOnlyWhenNewProgressArrives() {
        var tracker = makeTracker(totalBytes: 10_000)

        _ = tracker.record(state(transferredBytes: 1_000, totalBytes: 10_000), now: 0)
        _ = tracker.record(state(transferredBytes: 1_010, totalBytes: 10_000), now: 1)
        let previous = tracker.record(state(transferredBytes: 6_010, totalBytes: 10_000), now: 10)
        _ = tracker.snapshot(now: 11)
        let metrics = tracker.record(state(transferredBytes: 7_010, totalBytes: 10_000), now: 11)

        let rawRate = 600.0
        let alpha = 1 - exp(-1.0 / 6.0)
        let expected = (previous.speedBytesPerSecond ?? 0) + (rawRate - (previous.speedBytesPerSecond ?? 0)) * alpha
        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, expected, accuracy: 0.001)
    }

    func testInitialWaitBeforeFirstProgressDoesNotCreateSpeed() {
        var tracker = makeTracker(totalBytes: 1_000)

        let metrics = tracker.record(state(transferredBytes: 500, totalBytes: 1_000), now: 240)

        XCTAssertNil(metrics.speedBytesPerSecond)
        XCTAssertNil(metrics.remainingTimeSeconds)
    }

    func testRetryProgressRollbackCountsActualTransferWithoutReducingCommittedBytes() {
        var tracker = makeTracker(totalBytes: 1_000)

        _ = tracker.record(state(transferredBytes: 900, totalBytes: 1_000), now: 0)
        let metrics = tracker.record(state(transferredBytes: 100, totalBytes: 1_000), now: 10)

        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, 10, accuracy: 0.001)
        XCTAssertEqual(metrics.remainingTimeSeconds ?? 0, 10, accuracy: 0.001)
    }

    func testLogicalOnlyProgressReducesRemainingWithoutInflatingSpeed() {
        var tracker = makeTracker(totalBytes: 1_000)

        _ = tracker.record(
            state(
                transferredBytes: 500,
                totalBytes: 500,
                resourceName: "already-present.jpg",
                countsTowardTransferSpeed: false
            ),
            now: 0
        )
        _ = tracker.record(state(transferredBytes: 100, totalBytes: 500, resourceName: "uploading.mov"), now: 10)
        let metrics = tracker.record(state(transferredBytes: 200, totalBytes: 500, resourceName: "uploading.mov"), now: 20)

        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, 10, accuracy: 0.001)
        XCTAssertEqual(metrics.remainingTimeSeconds ?? 0, 30, accuracy: 0.001)
    }

    func testFailedUploadCreditFinishesRemainingWithoutInflatingSpeed() {
        var tracker = makeTracker(totalBytes: 1_000)

        _ = tracker.record(state(transferredBytes: 100, totalBytes: 1_000), now: 0)
        let beforeFailureCredit = tracker.record(state(transferredBytes: 200, totalBytes: 1_000), now: 10)
        let afterFailureCredit = tracker.record(
            state(
                transferredBytes: 1_000,
                totalBytes: 1_000,
                countsTowardTransferSpeed: false
            ),
            now: 10
        )

        XCTAssertEqual(afterFailureCredit.speedBytesPerSecond ?? 0, beforeFailureCredit.speedBytesPerSecond ?? 0, accuracy: 0.001)
        XCTAssertEqual(afterFailureCredit.remainingTimeSeconds ?? .infinity, 0, accuracy: 0.001)
    }

    func testAggregateAboveEstimatedTotalShowsZeroRemainingTime() {
        var tracker = makeTracker(totalBytes: 1_000)

        _ = tracker.record(state(transferredBytes: 500, totalBytes: 1_500), now: 0)
        let metrics = tracker.record(state(transferredBytes: 1_500, totalBytes: 1_500), now: 10)

        XCTAssertNotNil(metrics.speedBytesPerSecond)
        XCTAssertEqual(metrics.remainingTimeSeconds ?? .infinity, 0, accuracy: 0.001)
    }

    func testSkippedMonthCreditReducesRemainingWithoutInflatingSpeed() throws {
        let skippedMonthState = try XCTUnwrap(BackupParallelExecutor.skippedMonthTransferState(
            monthKey: LibraryMonthKey(year: 2026, month: 5),
            workerID: 2,
            skippedAssetCount: 40,
            estimatedBytes: 700
        ))
        var tracker = makeTracker(totalBytes: 1_000)

        let idleMetrics = tracker.record(skippedMonthState, now: 0)
        XCTAssertNil(idleMetrics.speedBytesPerSecond)
        XCTAssertNil(idleMetrics.remainingTimeSeconds)

        _ = tracker.record(state(transferredBytes: 100, totalBytes: 300, resourceName: "new.mov"), now: 10)
        let metrics = tracker.record(state(transferredBytes: 200, totalBytes: 300, resourceName: "new.mov"), now: 20)

        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, 10, accuracy: 0.001)
        XCTAssertEqual(metrics.remainingTimeSeconds ?? 0, 10, accuracy: 0.001)
    }

    func testEstimatedAssetCreditReducesRemainingWithoutInflatingSpeed() throws {
        let skippedAssetState = try XCTUnwrap(BackupParallelExecutor.estimatedAssetTransferState(
            assetLocalIdentifier: "asset-skipped",
            displayName: "IMG_9999",
            totalBytes: 700,
            workerID: 2,
            assetPosition: 1,
            totalAssets: 2
        ))
        var tracker = makeTracker(totalBytes: 1_000)

        _ = tracker.record(skippedAssetState, now: 0)
        _ = tracker.record(state(transferredBytes: 100, totalBytes: 300, resourceName: "new.mov"), now: 10)
        let metrics = tracker.record(state(transferredBytes: 200, totalBytes: 300, resourceName: "new.mov"), now: 20)

        XCTAssertEqual(metrics.speedBytesPerSecond ?? 0, 10, accuracy: 0.001)
        XCTAssertEqual(metrics.remainingTimeSeconds ?? 0, 10, accuracy: 0.001)
    }

    func testCachedSkippedResultRequiresLogicalCredit() {
        let result = AssetProcessResult(
            status: .skipped,
            reason: "asset_exists_cached",
            displayName: "IMG_0001",
            assetFingerprint: nil,
            timing: AssetProcessTiming(),
            totalFileSizeBytes: 1_000,
            uploadedFileSizeBytes: 0
        )

        XCTAssertTrue(BackupParallelExecutor.shouldEmitResultCredit(result))
    }

    func testItemEventFactoryUsesPlannedMonthInsteadOfCreationDateMonth() throws {
        let plan = MonthWorkItem(
            month: LibraryMonthKey(year: 2026, month: 7),
            assetLocalIdentifiers: ["asset-1"],
            estimatedBytes: 0
        )
        let resourceDate = ISO8601DateFormatter().date(from: "2026-06-15T12:00:00Z")!
        let utcCalendar = LibraryMonthKey.monthCalendar(preference: .fixedUTC())
        let event = BackupParallelExecutor.makeItemEvent(
            assetLocalIdentifier: "asset-1",
            assetFingerprint: nil,
            displayName: "IMG_0001",
            resourceDate: resourceDate,
            status: .success,
            reason: nil,
            in: plan,
            updatedAt: Date(timeIntervalSince1970: 1)
        )

        XCTAssertEqual(event.month, plan.month)
        XCTAssertEqual(event.resourceDate, resourceDate)
        XCTAssertEqual(LibraryMonthKey.from(date: resourceDate, calendar: utcCalendar), LibraryMonthKey(year: 2026, month: 6))
    }
}
