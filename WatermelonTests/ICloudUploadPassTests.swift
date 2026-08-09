import XCTest
@testable import Watermelon

final class ICloudUploadPassTests: XCTestCase {

    // MARK: - Month phase

    func testLocalPassCompletionParksMonthInLocalUploadDone() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)
        plan.apply(.uploadStarted)
        plan.apply(.localUploadCompleted)

        XCTAssertEqual(plan.phase, .localUploadDone)
        XCTAssertFalse(plan.isTerminal)
        XCTAssertFalse(plan.isDone)
        XCTAssertFalse(plan.isActive)
    }

    func testICloudPassReentersAndCompletesBackupMonth() {
        var plan = MonthPlan(needsUpload: true, needsDownload: false)
        plan.apply(.uploadStarted)
        plan.apply(.localUploadCompleted)
        plan.apply(.uploadStarted)
        XCTAssertEqual(plan.phase, .uploading)

        plan.apply(.uploadCompleted)
        XCTAssertEqual(plan.phase, .completed)
    }

    func testSameRunICloudStartReactivatesLocalUploadDoneMonth() {
        let month = LibraryMonthKey(year: 2026, month: 7)
        var session = HomeExecutionSession()
        session.enter(
            backup: [month],
            download: [],
            complement: [],
            localAssetIDs: { _ in ["asset-a"] }
        )

        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [month],
                newlyCompletedMonths: [],
                newlyLocalUploadDoneMonths: [month],
                newlyICloudUploadStartedMonths: [],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )
        XCTAssertEqual(session.monthPlans[month]?.phase, .localUploadDone)

        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [],
                newlyCompletedMonths: [],
                newlyLocalUploadDoneMonths: [],
                newlyICloudUploadStartedMonths: [month],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )

        XCTAssertEqual(session.monthPlans[month]?.phase, .uploading)
    }

    // A complement month must reach `.uploadDone` (not `.completed`) so its inline download still runs.
    func testICloudPassCompletionOfComplementMonthAwaitsDownload() {
        var plan = MonthPlan(needsUpload: true, needsDownload: true)
        plan.apply(.uploadStarted)
        plan.apply(.localUploadCompleted)
        plan.apply(.uploadCompleted)

        XCTAssertEqual(plan.phase, .uploadDone)
        XCTAssertFalse(plan.isTerminal)
    }

    func testLocalUploadDoneMonthPausesAndFails() {
        var paused = MonthPlan(needsUpload: true, needsDownload: false)
        paused.apply(.uploadStarted)
        paused.apply(.localUploadCompleted)
        paused.apply(.uploadPaused)
        XCTAssertEqual(paused.phase, .uploadPaused)
        paused.apply(.uploadResumed)
        XCTAssertEqual(paused.phase, .uploading)

        var failed = MonthPlan(needsUpload: true, needsDownload: false)
        failed.apply(.uploadStarted)
        failed.apply(.localUploadCompleted)
        failed.apply(.failed(reason: "boom"))
        XCTAssertEqual(failed.phase, .failed)

        var partial = MonthPlan(needsUpload: true, needsDownload: false)
        partial.apply(.uploadStarted)
        partial.apply(.localUploadCompleted)
        partial.apply(.partiallyFailed(count: 3))
        XCTAssertEqual(partial.phase, .partiallyFailed)
        XCTAssertEqual(partial.failedItemCount, 3)
    }

    func testSessionPauseMovesLocalUploadDoneMonthToUploadPaused() {
        let month = LibraryMonthKey(year: 2026, month: 7)
        var session = HomeExecutionSession()
        session.enter(
            backup: [month],
            download: [],
            complement: [],
            localAssetIDs: { _ in ["asset-a"] }
        )
        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [month],
                newlyCompletedMonths: [],
                newlyLocalUploadDoneMonths: [month],
                newlyICloudUploadStartedMonths: [],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )

        _ = session.pause()

        XCTAssertEqual(session.phase, .uploadPaused)
        XCTAssertEqual(session.monthPlans[month]?.phase, .uploadPaused)
    }

    // The month owes the iCloud pass its inline download, so a pause must re-queue its assets for resume.
    func testLocalUploadDoneComplementMonthIsPendingForResume() {
        let month = LibraryMonthKey(year: 2026, month: 7)
        var session = HomeExecutionSession()
        session.enter(
            backup: [],
            download: [],
            complement: [month],
            localAssetIDs: { _ in ["asset-a", "asset-b"] }
        )
        _ = session.consumePendingUploadScope()

        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [month],
                newlyCompletedMonths: [],
                newlyLocalUploadDoneMonths: [month],
                newlyICloudUploadStartedMonths: [],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )
        XCTAssertEqual(session.monthPlans[month]?.phase, .localUploadDone)
        XCTAssertEqual(session.assetIDsAwaitingInlineComplementResume(), ["asset-a", "asset-b"])
    }

    func testICloudReadBackFailureFailsClosedFromLocalUploadDone() {
        let month = LibraryMonthKey(year: 2026, month: 7)
        var session = HomeExecutionSession()
        session.enter(
            backup: [],
            download: [],
            complement: [month],
            localAssetIDs: { _ in ["asset-a"] }
        )
        _ = session.consumePendingUploadScope()
        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [month],
                newlyCompletedMonths: [],
                newlyLocalUploadDoneMonths: [month],
                newlyICloudUploadStartedMonths: [],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )

        let outcome = session.handleUploadResult(.completed(failedCountByMonth: [month: 1]))

        XCTAssertEqual(session.monthPlans[month]?.phase, .failed)
        guard case .finished = outcome else { return XCTFail("expected finished outcome") }
        guard case .failed = session.phase else {
            return XCTFail("a complement without inline download must fail closed")
        }
    }

    // MARK: - Pass accounting

    func testICloudPassSchedulesOnlyDeferredAssets() {
        let monthAssetIDs = ["a", "b", "c"]

        XCTAssertEqual(
            BackupParallelExecutor.assetIDsForPass(
                monthAssetIDs: monthAssetIDs,
                includedAssetIDs: nil
            ),
            monthAssetIDs
        )
        XCTAssertEqual(
            BackupParallelExecutor.assetIDsForPass(
                monthAssetIDs: monthAssetIDs,
                includedAssetIDs: ["b", "c"]
            ),
            ["b", "c"]
        )
        XCTAssertEqual(
            BackupParallelExecutor.assetIDsForPass(
                monthAssetIDs: monthAssetIDs,
                includedAssetIDs: []
            ),
            []
        )
    }

    func testICloudPassDoesNotReusePreparedSnapshotSeed() {
        let resource = TestFixtures.remoteResource(
            year: 2026,
            month: 7,
            contentHash: Data([0x11])
        )
        let lookup = MonthSeedLookup(snapshot: RemoteLibrarySnapshot(
            resources: [resource],
            assets: []
        ))

        XCTAssertNotNil(BackupParallelExecutor.snapshotSeedLookup(
            for: .localResources,
            preparedLookup: lookup
        ))
        XCTAssertNil(BackupParallelExecutor.snapshotSeedLookup(
            for: .iCloudResources,
            preparedLookup: lookup
        ))
    }

    func testReadBackFailureCountsDeferredAssetsBlockedByTheMonth() async {
        let aggregator = ParallelBackupProgressAggregator(total: 3)
        let success = AssetProcessResult(
            status: .success,
            reason: nil,
            displayName: "asset",
            assetFingerprint: nil,
            timing: AssetProcessTiming(),
            totalFileSizeBytes: 0,
            uploadedFileSizeBytes: 0
        )
        _ = await aggregator.record(result: success)
        _ = await aggregator.record(result: success)

        var monthCounts = BackupMonthProgressCounts()
        monthCounts.succeeded = 2
        _ = await aggregator.recordFinalizationFailure(
            monthCounts,
            additionalFailureCount: 1
        )

        let state = await aggregator.snapshot()
        XCTAssertEqual(state.total, 3)
        XCTAssertEqual(state.succeeded, 0)
        XCTAssertEqual(state.failed, 3)
        XCTAssertEqual(state.skipped, 0)
    }

    func testICloudPassReusesDeferredAssetDispatchSlot() async {
        let aggregator = ParallelBackupProgressAggregator(total: 2)
        let deferredSlot = await aggregator.allocateDispatchSlot()
        await aggregator.retainDispatchSlot(deferredSlot, forDeferredAssetID: "icloud-a")
        let localSlot = await aggregator.allocateDispatchSlot()

        let resumedSlot = await aggregator.allocateDispatchSlot(
            resumingDeferredAssetID: "icloud-a"
        )

        XCTAssertEqual(deferredSlot.position, 1)
        XCTAssertEqual(localSlot.position, 2)
        XCTAssertEqual(resumedSlot.position, deferredSlot.position)
        XCTAssertEqual(resumedSlot.total, deferredSlot.total)
    }

    func testDeferredResultNeitherDirtiesManifestNorEarnsCredit() {
        let reason = AssetProcessor.iCloudDeferredReason
        XCTAssertFalse(
            BackupParallelExecutor.resultDirtiedMonthManifest(status: .skipped, reason: reason)
        )
        XCTAssertFalse(
            BackupParallelExecutor.shouldEmitResultCredit(
                AssetProcessResult(
                    status: .skipped,
                    reason: reason,
                    displayName: "IMG_0001",
                    assetFingerprint: nil,
                    timing: AssetProcessTiming(),
                    totalFileSizeBytes: 0,
                    uploadedFileSizeBytes: 0
                )
            )
        )
    }

    func testICloudPassStaysSerial() {
        XCTAssertEqual(BackupParallelExecutor.iCloudPassWorkerCount, 1)
    }
}
