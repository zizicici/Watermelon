import XCTest
@testable import Watermelon

final class BackupSessionReducerTests: XCTestCase {

    private func itemEvent(
        _ id: String,
        status: BackupItemStatus,
        date: Date,
        month: LibraryMonthKey? = nil
    ) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: id,
            assetFingerprint: nil,
            month: month ?? LibraryMonthKey.from(date: date),
            displayName: id,
            resourceDate: date,
            status: status,
            reason: nil,
            updatedAt: date
        )
    }

    private func reduceProgress(
        _ state: inout BackupSessionState,
        _ item: BackupItemEvent,
        runMode: BackupRunMode = .full
    ) {
        let progress = BackupProgress(
            succeeded: 0, failed: 0, skipped: 0, total: 1,
            message: "",
            logMessage: nil,
            logLevel: .info,
            itemEvent: item,
            transferState: nil
        )
        _ = state.reduce(event: .progress(progress), runMode: runMode, displayMode: runMode, terminalIntent: .none)
    }

    // F1-R05: a transient per-asset failure that succeeds on retry after resume must NOT leave the month falsely
    // `.partiallyFailed` — the monotonic failed counter is reset on resume and the retry re-establishes it.
    func testTransientFailureSucceedingOnResumeClearsFailedCount() {
        var state = BackupSessionState()
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let month = LibraryMonthKey.from(date: date)

        reduceProgress(&state, itemEvent("A", status: .failed, date: date))
        XCTAssertEqual(state.snapshot().failedCountByMonth[month], 1)
        XCTAssertFalse(state.completedAssetIDsForResume.contains("A"), "a failed asset is dropped from resume-complete")

        _ = state.prepareForResume()
        XCTAssertNil(state.snapshot().failedCountByMonth[month], "resume must reset the monotonic failed counter")

        reduceProgress(&state, itemEvent("A", status: .success, date: date))
        XCTAssertNil(state.snapshot().failedCountByMonth[month], "a retry-success must not re-mark the month failed")
        XCTAssertTrue(state.completedAssetIDsForResume.contains("A"))
    }

    // A genuine failure that recurs on the resume retry is still reported (the retry re-establishes the count).
    func testFailureRecurringOnResumeStaysFailed() {
        var state = BackupSessionState()
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let month = LibraryMonthKey.from(date: date)

        reduceProgress(&state, itemEvent("A", status: .failed, date: date))
        _ = state.prepareForResume()
        reduceProgress(&state, itemEvent("A", status: .failed, date: date))

        XCTAssertEqual(state.snapshot().failedCountByMonth[month], 1, "a failure that recurs on resume must still be reported")
    }

    func testProgressEventUsesEventMonthInsteadOfResourceDate() {
        var state = BackupSessionState()
        let resourceDate = Date(timeIntervalSince1970: 0)
        let eventMonth = LibraryMonthKey(year: 2024, month: 6)

        reduceProgress(&state, itemEvent("A", status: .success, date: resourceDate, month: eventMonth))

        let snapshot = state.snapshot()
        XCTAssertEqual(snapshot.processedCountByMonth[eventMonth], 1)
        XCTAssertNil(snapshot.processedCountByMonth[LibraryMonthKey.from(date: resourceDate)])
    }

}

final class ExecutionTerminationControlTests: XCTestCase {
    func testPauseRequestsDrainAndPreservesIntent() {
        let control = ExecutionTerminationControl()

        XCTAssertFalse(control.shouldDrain)
        control.request(.pause)

        XCTAssertTrue(control.shouldDrain)
        guard case .pause = control.terminationIntent else {
            return XCTFail("pause must remain distinguishable from stop")
        }
    }

    func testStopSupersedesPauseAndCannotBeDowngraded() {
        let control = ExecutionTerminationControl()

        control.request(.pause)
        control.request(.stop)
        control.request(.pause)

        guard case .stop = control.terminationIntent else {
            return XCTFail("stop must be monotonic once requested")
        }
    }
}

@MainActor
final class BackupSessionAsyncBridgeTests: XCTestCase {
    func testPausedTerminalRemarkHappensBeforeRunUploadReturns() async {
        let month = LibraryMonthKey(year: 2026, month: 1)
        let assetIDs: Set<String> = ["A"]
        let home = PausedComplementSessionBox(month: month, assetIDs: assetIDs)
        let controller = FakeBackupSessionController(completedAssetIDs: assetIDs)
        let bridge = BackupSessionAsyncBridge(backupSessionController: controller)

        let runTask = Task { @MainActor in
            await bridge.runUpload(
                pendingAssetIDsOnPause: {
                    home.session.assetIDsAwaitingInlineComplementResume()
                },
                onProgress: { progress in
                    _ = home.session.handleUploadProgress(
                        progress,
                        now: 0,
                        syncThrottleInterval: 0
                    )
                }
            )
        }

        for _ in 0..<100 where !controller.hasObserver {
            await Task.yield()
        }
        XCTAssertTrue(controller.hasObserver)

        controller.emit(state: .paused)
        let result = await runTask.value

        guard case .paused = result else {
            return XCTFail("expected paused bridge result")
        }
        XCTAssertEqual(controller.markedAssetIDSets, [assetIDs])
        XCTAssertTrue(controller.completedAssetIDs.isEmpty)
    }
}

@MainActor
private final class PausedComplementSessionBox {
    var session = HomeExecutionSession()

    init(month: LibraryMonthKey, assetIDs: Set<String>) {
        session.enter(
            backup: [],
            download: [],
            complement: [month],
            localAssetIDs: { _ in assetIDs }
        )
        _ = session.handleUploadProgress(
            BackupSessionAsyncBridge.UploadProgress(
                newlyStartedMonths: [month],
                newlyCompletedMonths: [],
                processedCountByMonth: [:]
            ),
            now: 0,
            syncThrottleInterval: 0
        )
        _ = session.pause()
    }
}

@MainActor
private final class FakeBackupSessionController: BackupSessionControlling {
    private var observer: (@MainActor (BackupSessionController.Snapshot) -> Void)?
    private let observerID = UUID()
    private(set) var completedAssetIDs: Set<String>
    private(set) var markedAssetIDSets: [Set<String>] = []
    private var currentState: BackupSessionController.State = .running

    var hasObserver: Bool { observer != nil }

    init(completedAssetIDs: Set<String>) {
        self.completedAssetIDs = completedAssetIDs
    }

    func startBackupWhenReady(
        scope _: BackupScopeSelection?,
        runConfigurationOverride _: BackupRunConfigurationOverride?,
        onMonthUploaded _: BackupMonthFinalizer?
    ) async -> Bool {
        true
    }

    func addObserver(
        _ observer: @escaping @MainActor (BackupSessionController.Snapshot) -> Void
    ) -> UUID {
        self.observer = observer
        observer(makeSnapshot(state: currentState))
        return observerID
    }

    func removeObserver(_ id: UUID) {
        if id == observerID {
            observer = nil
        }
    }

    func pauseBackup() {}
    func stopBackup() {}
    func cancelBackupImmediately() -> Task<Void, Never> { Task {} }

    func markAssetIDsPendingForResume(_ assetIDs: Set<String>) {
        markedAssetIDSets.append(assetIDs)
        completedAssetIDs.subtract(assetIDs)
    }

    func emit(state: BackupSessionController.State) {
        currentState = state
        observer?(makeSnapshot(state: state))
    }

    private func makeSnapshot(
        state: BackupSessionController.State
    ) -> BackupSessionController.Snapshot {
        BackupSessionController.Snapshot(
            state: state,
            controlPhase: .idle,
            statusText: "",
            succeeded: 0,
            failed: 0,
            skipped: 0,
            total: 1,
            startedMonths: [],
            completedMonths: [],
            processedCountByMonth: [:],
            failedCountByMonth: [:]
        )
    }
}
