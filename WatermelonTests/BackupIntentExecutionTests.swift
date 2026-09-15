import XCTest
import CryptoKit
import Photos
@testable import Watermelon

final class BackupIntentExecutionTests: XCTestCase {
    func testCancellationBeforeStartDoesNotRunOperation() async {
        let cancellation = BackupCancellationController()
        cancellation.cancel()
        do {
            _ = try await BackupIntentExecution.run(cancellation: cancellation) {
                XCTFail("A cancelled intent must not start a backup")
                return 1
            }
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
    }

    func testSystemCancellationWaitsForCleanupAndExecutionLeaseRelease() async {
        await assertCancellationWaitsForCleanup(cancelParent: false)
    }

    func testParentCancellationReachesBackupAndWaitsForCleanup() async {
        await assertCancellationWaitsForCleanup(cancelParent: true)
    }

    private func assertCancellationWaitsForCleanup(cancelParent: Bool) async {
        let cancellation = BackupCancellationController()
        let flags = AppRuntimeFlags()
        let started = expectation(description: "Backup started")
        let cleaningUp = expectation(description: "Backup is cleaning up")
        let cleanupGate = IntentCleanupGate()
        let task = Task {
            try await BackupIntentExecution.run(cancellation: cancellation) {
                try await flags.withExecutionLease {
                    started.fulfill()
                    do {
                        try await Task.sleep(for: .seconds(60))
                        XCTFail("Backup did not receive cancellation")
                    } catch {
                        cleaningUp.fulfill()
                        await cleanupGate.wait()
                        throw error
                    }
                    return 1
                }
            }
        }
        await fulfillment(of: [started], timeout: 3)
        if cancelParent { task.cancel() } else { cancellation.cancel() }
        await fulfillment(of: [cleaningUp], timeout: 3)
        XCTAssertTrue(flags.isExecuting, "Cleanup must retain the app-wide execution lease")
        await cleanupGate.open()
        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertFalse(flags.isExecuting)
    }

    func testProgressReservesCompletionUntilCleanupFinishes() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 0, totalBytes: nil))
        XCTAssertLessThan(progress.totalUnitCount, 0)
        await reporter.receive(.started(totalAssets: 2, totalBytes: nil))
        await reporter.receive(.progress(BackupProgress(
            succeeded: 2, failed: 0, skipped: 0, total: 2,
            message: "Flushing", logMessage: nil, logLevel: .info,
            itemEvent: nil, transferState: nil
        )))
        await reporter.receive(.finished(BackupExecutionResult(total: 2, succeeded: 2, failed: 0, skipped: 0, paused: false)))
        XCTAssertLessThan(progress.completedUnitCount, progress.totalUnitCount)
        await reporter.complete()
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount)
    }

    func testLargeAssetReportsTransferProgressWithoutCompletingTheRun() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(.transferState(BackupTransferState(
            kind: .upload, workerID: 0, assetLocalIdentifier: "asset", assetDisplayName: "Video",
            resourceDate: nil, assetPosition: 1, totalAssets: 1,
            resourceDisplayName: "Video.mov", resourcePosition: 1, totalResources: 1,
            resourceFraction: 0.5, resourceBytesTransferred: 500, resourceTotalBytes: 1_000,
            countsTowardTransferSpeed: true, stageDescription: "Uploading"
        )))
        XCTAssertGreaterThan(progress.completedUnitCount, 0)
        XCTAssertLessThan(progress.completedUnitCount, progress.totalUnitCount)
        let completed = progress.completedUnitCount
        await reporter.receive(.progress(BackupProgress(
            succeeded: 0, failed: 0, skipped: 0, total: 1,
            message: "Retrying", logMessage: nil, logLevel: .info,
            itemEvent: nil, transferState: nil
        )))
        XCTAssertEqual(progress.completedUnitCount, completed)
    }

    func testEmptyLibraryCanComplete() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.complete()
        XCTAssertEqual(progress.totalUnitCount, 1)
        XCTAssertEqual(progress.completedUnitCount, 1)
    }

    func testSlowLargeUploadKeepsPublishingProgressWithAnUnchangedSubtitle() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 0, size: 100_000_000_000))
        let caption = progress.localizedAdditionalDescription
        var previous = progress.completedUnitCount
        for second in 1...180 {
            await reporter.receive(transferProgress(bytes: Int64(second) * 1_000_000, size: 100_000_000_000))
            XCTAssertGreaterThan(progress.completedUnitCount, previous)
            XCTAssertLessThan(progress.completedUnitCount, progress.totalUnitCount)
            XCTAssertEqual(progress.localizedAdditionalDescription, caption)
            previous = progress.completedUnitCount
        }
    }

    func testUploadUsesBytesWhenFloatFractionsAreIndistinguishable() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        let size: Int64 = 100_000_000_000
        let before: Int64 = 95_000_000_000
        let after = before + 1_000
        XCTAssertEqual(Float(Double(before) / Double(size)), Float(Double(after) / Double(size)))
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: before, size: size))
        let previous = progress.completedUnitCount
        await reporter.receive(transferProgress(bytes: after, size: size))
        XCTAssertGreaterThan(progress.completedUnitCount, previous)
    }

    func testRepeatedUploadAttemptsAdvanceBeforeCatchingUpAndReserveCleanup() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 950, size: 1_000))
        for _ in 1...2 {
            let previousCompleted = progress.completedUnitCount
            let previousTotal = progress.totalUnitCount
            await reporter.receive(transferProgress(bytes: 0, size: 1_000))
            XCTAssertEqual(progress.completedUnitCount, previousCompleted)
            XCTAssertGreaterThan(progress.totalUnitCount, previousTotal)
            await reporter.receive(transferProgress(bytes: 100, size: 1_000))
            XCTAssertGreaterThan(progress.completedUnitCount, previousCompleted)
            XCTAssertLessThan(progress.completedUnitCount, progress.totalUnitCount)
        }
        await reporter.receive(transferProgress(bytes: 1_000, size: 1_000))
        await reporter.receive(itemProgress(processed: 1))
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount - 1)
        await reporter.complete()
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount)
    }

    func testResourceAndPhaseTransitionsDoNotAddRetryWork() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 2, totalBytes: nil))
        let initialTotal = progress.totalUnitCount
        for asset in 1...2 {
            for isUpload in [false, true] {
                for resource in 1...2 {
                    for bytes in [Int64(0), 500, 1_000] {
                        await reporter.receive(transferProgress(
                            bytes: bytes, size: 1_000,
                            resourcePosition: resource, totalResources: 2, isUpload: isUpload
                        ))
                        XCTAssertEqual(progress.totalUnitCount, initialTotal)
                    }
                }
            }
            await reporter.receive(itemProgress(processed: asset))
        }
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount - 1)
    }

    func testAssetRetryReportsRepreparationAndUploadBeforeCatchingUp() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 950, size: 1_000))
        let beforeRetry = progress.completedUnitCount
        let initialTotal = progress.totalUnitCount
        await reporter.receive(transferProgress(bytes: 0, size: 1_000, isUpload: false))
        XCTAssertEqual(progress.completedUnitCount, beforeRetry)
        XCTAssertGreaterThan(progress.totalUnitCount, initialTotal)
        let retryTotal = progress.totalUnitCount
        for isUpload in [false, true] {
            var previous = progress.completedUnitCount
            for percent in 1...100 {
                await reporter.receive(transferProgress(bytes: Int64(percent) * 10, size: 1_000, isUpload: isUpload))
                XCTAssertGreaterThan(progress.completedUnitCount, previous)
                XCTAssertEqual(progress.totalUnitCount, retryTotal)
                previous = progress.completedUnitCount
            }
        }
        await reporter.receive(itemProgress(processed: 1))
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount - 1)
    }

    func testAssetRetryDuringSecondResourcePreparationReportsWorkImmediately() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 500, size: 1_000, resourcePosition: 2, totalResources: 2, isUpload: false))
        let beforeRetry = progress.completedUnitCount
        let initialTotal = progress.totalUnitCount
        await reporter.receive(transferProgress(bytes: 0, size: 1_000, totalResources: 2, isUpload: false))
        XCTAssertEqual(progress.completedUnitCount, beforeRetry)
        XCTAssertGreaterThan(progress.totalUnitCount, initialTotal)
        let retryTotal = progress.totalUnitCount
        await reporter.receive(transferProgress(bytes: 10, size: 1_000, totalResources: 2, isUpload: false))
        XCTAssertGreaterThan(progress.completedUnitCount, beforeRetry)
        XCTAssertEqual(progress.totalUnitCount, retryTotal)
    }

    func testCompletionEstimatesDoNotLookLikeAnAssetRetry() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 950, size: 1_000))
        let completed = progress.completedUnitCount
        let total = progress.totalUnitCount
        await reporter.receive(.transferState(BackupTransferState(
            kind: .upload, workerID: 0, assetLocalIdentifier: "asset", assetDisplayName: "Video",
            resourceDate: nil, assetPosition: 1, totalAssets: 1,
            resourceDisplayName: "Video.mov", resourcePosition: 1, totalResources: 1,
            resourceFraction: 1, resourceBytesTransferred: 1_000, resourceTotalBytes: 1_000,
            countsTowardTransferSpeed: false, stageDescription: "Failed"
        )))
        XCTAssertEqual(progress.completedUnitCount, completed)
        XCTAssertEqual(progress.totalUnitCount, total)
        await reporter.receive(itemProgress(processed: 1))
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount - 1)
    }

    func testDuplicateTransferEventsDoNotInventProgress() async throws {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .milliseconds(20))
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(transferProgress(bytes: 950, size: 1_000))
        await reporter.receive(transferProgress(bytes: 100, size: 1_000))
        try await Task.sleep(for: .milliseconds(80))
        let completed = progress.completedUnitCount
        let total = progress.totalUnitCount
        for _ in 1...10 {
            await reporter.receive(transferProgress(bytes: 100, size: 1_000))
        }
        try await Task.sleep(for: .milliseconds(80))
        XCTAssertEqual(progress.completedUnitCount, completed)
        XCTAssertEqual(progress.totalUnitCount, total)
        await reporter.stop()
    }

    func testPreparationAdvancesProgressBeforeUploadStarts() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        await reporter.receive(.started(totalAssets: 1, totalBytes: nil))
        await reporter.receive(.transferState(BackupTransferState(
            kind: .upload, workerID: 0, assetLocalIdentifier: "asset", assetDisplayName: "Video",
            resourceDate: nil, assetPosition: 1, totalAssets: 1,
            resourceDisplayName: "Video.mov", resourcePosition: 1, totalResources: 1,
            resourceFraction: 1, resourceBytesTransferred: nil, resourceTotalBytes: nil,
            countsTowardTransferSpeed: false, stageDescription: "Preparing"
        )))
        XCTAssertGreaterThan(progress.completedUnitCount, 0)
        XCTAssertLessThan(progress.fractionCompleted, 0.51)
    }

    func testHashProgressReportsRealBytesAndPreservesDigest() throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: url) }
        let bytes = Data(repeating: 42, count: 2_000_000)
        try bytes.write(to: url)
        var observed: [Int64] = []
        let result = try AssetProcessor.contentHashAndSize(of: url, onProgress: { observed.append($0) })
        XCTAssertEqual(result.hash, Data(SHA256.hash(data: bytes)))
        XCTAssertEqual(result.size, Int64(bytes.count))
        XCTAssertEqual(observed.first, 0)
        XCTAssertEqual(observed.last, result.size)
        XCTAssertEqual(observed, observed.sorted())
    }

    func testHashCancellationDoesNotReportFinishedProgress() throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: url) }
        try Data(repeating: 42, count: 1_000).write(to: url)
        let cancellation = BackupCancellationController()
        var observed: [Int64] = []
        XCTAssertThrowsError(try AssetProcessor.contentHashAndSize(of: url, cancellationController: cancellation) { bytes in
            observed.append(bytes)
            cancellation.cancel()
        }) { error in
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertEqual(observed, [0])
    }

    func testPartialFailuresAndUnprocessedItemsAreNotSuccessful() {
        XCTAssertThrowsError(try BackgroundBackupRunner.validateCompletion(
            BackupExecutionResult(total: 2, succeeded: 1, failed: 1, skipped: 0, paused: false)
        ))
        XCTAssertThrowsError(try BackgroundBackupRunner.validateCompletion(
            BackupExecutionResult(total: 2, succeeded: 1, failed: 0, skipped: 0, paused: false)
        ))
    }

    func testPausedRunReportsCancellation() {
        XCTAssertThrowsError(try BackgroundBackupRunner.validateCompletion(
            BackupExecutionResult(total: 2, succeeded: 1, failed: 0, skipped: 0, paused: true)
        )) { error in
            XCTAssertTrue(error is CancellationError)
        }
    }

    func testEmptyAndAlreadyBackedUpLibrariesSucceed() {
        XCTAssertNoThrow(try BackgroundBackupRunner.validateCompletion(
            BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
        ))
        XCTAssertNoThrow(try BackgroundBackupRunner.validateCompletion(
            BackupExecutionResult(total: 2, succeeded: 0, failed: 0, skipped: 2, paused: false)
        ))
    }

    func testIntentDefaultsToRecentTwoMonths() throws {
        guard #available(iOS 27.0, *) else { throw XCTSkip("Requires iOS 27 App Intents") }
        let intent = RunBackupIntent()
        XCTAssertEqual(intent.scope, .recentTwoMonths)
        if case .recentMonths(2) = intent.scope.monthScope {} else {
            XCTFail("Default range must remain two calendar months")
        }
        if case .all = IntentBackupScope.allPhotos.monthScope {} else {
            XCTFail("Entire library must include every month")
        }
    }

    func testProgressTitleNamesNodeAndSubtitleIgnoresDetailedMessages() async {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .zero)
        XCTAssertEqual(progress.localizedDescription, String(format: String(localized: "backupIntent.progress.title"), "Home NAS"))
        await reporter.receive(.preparationProgress(current: 2, total: 5))
        XCTAssertEqual(progress.localizedAdditionalDescription, String(format: String(localized: "backupIntent.progress.preparingCount"), 2, 5))
        await reporter.receive(.started(totalAssets: 100, totalBytes: nil))
        await reporter.receive(itemProgress(processed: 12))
        let caption = String(format: String(localized: "backupIntent.progress.backingUp"), 12, 100)
        XCTAssertEqual(progress.localizedAdditionalDescription, caption)
        await reporter.receive(.log("Worker #1 is flushing manifest.sqlite", level: .warning))
        await reporter.receive(completedTransfer())
        await reporter.receive(.preparationProgress(current: 3, total: 5))
        XCTAssertEqual(progress.localizedAdditionalDescription, caption)
        XCTAssertFalse(progress.localizedAdditionalDescription.contains("·"))
    }

    func testProgressCoalescesAllEventsAndPublishesLatestCounts() async throws {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS")
        let initialCaption = progress.localizedAdditionalDescription
        await reporter.receive(.started(totalAssets: 100, totalBytes: nil))
        for count in 1...12 {
            await reporter.receive(itemProgress(processed: count))
            await reporter.receive(completedTransfer())
        }
        await reporter.receive(itemProgress(processed: 13))
        XCTAssertEqual(progress.localizedAdditionalDescription, initialCaption)
        XCTAssertEqual(progress.completedUnitCount, 0)
        try await Task.sleep(for: .milliseconds(1_100))
        XCTAssertEqual(progress.localizedAdditionalDescription, String(format: String(localized: "backupIntent.progress.backingUp"), 13, 100))
        XCTAssertEqual(progress.completedUnitCount, (progress.totalUnitCount - 1) / 100 * 13)
        await reporter.stop()
    }

    func testCompletionCancelsPendingUpdates() async throws {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .milliseconds(50))
        await reporter.receive(.started(totalAssets: 100, totalBytes: nil))
        await reporter.receive(itemProgress(processed: 12))
        await reporter.complete()
        try await Task.sleep(for: .milliseconds(100))
        XCTAssertEqual(progress.completedUnitCount, progress.totalUnitCount)
        XCTAssertEqual(progress.localizedAdditionalDescription, String(localized: "backupIntent.progress.completed"))
    }

    func testStoppingCancelsPendingUpdates() async throws {
        let progress = Progress()
        let reporter = BackupIntentProgressReporter(progress: progress, nodeName: "Home NAS", updateInterval: .milliseconds(50))
        let caption = progress.localizedAdditionalDescription
        await reporter.receive(.started(totalAssets: 100, totalBytes: nil))
        await reporter.receive(itemProgress(processed: 12))
        await reporter.stop()
        try await Task.sleep(for: .milliseconds(100))
        XCTAssertEqual(progress.localizedAdditionalDescription, caption)
        XCTAssertEqual(progress.completedUnitCount, 0)
    }

    func testMediaFiltersSelectWholePhotoOrVideoAssets() {
        let photo = ["mediaType": PHAssetMediaType.image.rawValue]
        let video = ["mediaType": PHAssetMediaType.video.rawValue]
        XCTAssertNil(PhotoLibraryMediaFilter.all.predicate)
        XCTAssertTrue(PhotoLibraryMediaFilter.photos.predicate!.evaluate(with: photo))
        XCTAssertFalse(PhotoLibraryMediaFilter.photos.predicate!.evaluate(with: video))
        XCTAssertTrue(PhotoLibraryMediaFilter.videos.predicate!.evaluate(with: video))
        XCTAssertFalse(PhotoLibraryMediaFilter.videos.predicate!.evaluate(with: photo))
    }

    private func itemProgress(processed: Int) -> BackupEvent {
        .progress(BackupProgress(
            succeeded: processed, failed: 0, skipped: 0, total: 100,
            message: "Completed IMG_1234.JPG", logMessage: nil, logLevel: .info,
            itemEvent: nil, transferState: nil
        ))
    }

    private func completedTransfer() -> BackupEvent {
        .transferState(BackupTransferState(
            kind: .upload, workerID: 1, assetLocalIdentifier: "asset", assetDisplayName: "Video",
            resourceDate: nil, assetPosition: 1, totalAssets: 100,
            resourceDisplayName: "Video.mov", resourcePosition: 1, totalResources: 1,
            resourceFraction: 1, resourceBytesTransferred: 1_000, resourceTotalBytes: 1_000,
            countsTowardTransferSpeed: true, stageDescription: "Upload complete"
        ))
    }

    private func transferProgress(
        bytes: Int64,
        size: Int64,
        resourcePosition: Int = 1,
        totalResources: Int = 1,
        isUpload: Bool = true
    ) -> BackupEvent {
        .transferState(BackupTransferState(
            kind: .upload, workerID: 0, assetLocalIdentifier: "asset", assetDisplayName: "Video",
            resourceDate: nil, assetPosition: 1, totalAssets: 1,
            resourceDisplayName: "Video.mov", resourcePosition: resourcePosition, totalResources: totalResources,
            resourceFraction: Float(Double(bytes) / Double(size)),
            resourceBytesTransferred: isUpload ? bytes : nil, resourceTotalBytes: isUpload ? size : nil,
            countsTowardTransferSpeed: isUpload, stageDescription: "Transfer"
        ))
    }
}

private actor IntentCleanupGate {
    private var isOpen = false
    private var continuation: CheckedContinuation<Void, Never>?

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { continuation = $0 }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }
}
