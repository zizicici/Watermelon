import XCTest
@testable import Watermelon

final class BackupParallelExecutorMonthEventTests: XCTestCase {

    private static let probeAssets: Set<AssetFingerprint> = [
        TestFixtures.assetFingerprint(0xC1),
        TestFixtures.assetFingerprint(0xC2)
    ]
    private static let probeTombstones: Set<AssetFingerprint> = [
        TestFixtures.assetFingerprint(0xD1)
    ]

    private func makePartialOutcome(underlying: Error) -> V2MonthFlushOutcome {
        let flushError = V2MonthSession.FlushError.postCommitFailed(underlying: underlying)
        let delta = BackupMonthFlushDelta(
            didFlush: true,
            committedAssetFingerprints: Self.probeAssets,
            committedTombstoneFingerprints: Self.probeTombstones
        )
        return .commitDurablePartial(delta: delta, flushError: flushError)
    }

    func testForegroundEndOfMonthPartialDispatchEmitsDeferredEventForSoftPartialWithShouldFinishMonth() async {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logErrorAndEmitDeferred)

        // Verify the wrapper-derived message reaches the emitted MonthChangeEvent.
        // userFacingStorageErrorMessage(dispatch.displayError) is the production source of truth;
        // we re-derive it here and assert the emit helper passes it through.
        let expectedMessage = profile.userFacingStorageErrorMessage(dispatch.displayError)
        let month = LibraryMonthKey(year: 2024, month: 6)
        let eventStream = BackupEventStream()
        let emitted = BackupParallelExecutor.emitUploadDurableSnapshotDeferred(
            eventStream: eventStream,
            month: month,
            message: expectedMessage
        )
        eventStream.finish()
        XCTAssertTrue(emitted)
        let events = await collectEvents(from: eventStream)
        XCTAssertEqual(events.deferredEventMessages, [expectedMessage],
                       "Emitted MonthChangeEvent.uploadDurableSnapshotDeferred.message must equal userFacingStorageErrorMessage(dispatch.displayError) — proves the wrapper, not the unwrapped inner error, is the message source.")
    }

    func testForegroundEndOfMonthPartialDispatchEmitsLogOnlyForSoftPartialWithoutShouldFinishMonth() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: false
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logErrorOnly,
                       "shouldFinishMonth=false must NOT emit the durable-snapshot-deferred event — preserves today's shouldEmitUploadDurableSnapshotDeferred returning false.")
    }

    func testForegroundEndOfMonthPartialDispatchPausesForCancellationPartial() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .pauseAndBreakMonthLoop,
                       "Cancellation precedence beats partial — never emit deferred event for cancellation partial.")
    }

    func testForegroundEndOfMonthPartialDispatchAbortsForConnectionUnavailablePartial() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .abortMonthBreakMonthLoop,
                       "Connection-unavailable precedence beats partial — never emit deferred event for connection-unavailable partial.")
    }

    func testDurableSnapshotDeferredEmitterProducesOnlyDeferredEvent() async {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let eventStream = BackupEventStream()

        let emitted = BackupParallelExecutor.emitUploadDurableSnapshotDeferred(
            eventStream: eventStream,
            month: month,
            message: "snapshot deferred"
        )
        eventStream.finish()

        XCTAssertTrue(emitted)
        let events = await collectEvents(from: eventStream)
        XCTAssertEqual(events.deferredEventMessages, ["snapshot deferred"])
        XCTAssertFalse(events.containsIncompleteEvent)
    }

    // Worker-loop outcome-transition precedence: a fatal set during EOM must throw (run records
    // .failed) even when the EOM path also requests .breakMonthLoop. Before the fix the break won and
    // the connection-unavailable EOM abort was swallowed into a .completed run.
    func testMonthLoopContinuation_BreakWithFatal_ThrowsFatal() {
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .breakMonthLoop, hasFatal: true, paused: false
            ),
            .throwFatal,
            "An EOM abort that markFatals AND returns .breakMonthLoop (connection-unavailable) must throw, not break — otherwise the run records .completed for a failed V2 commit."
        )
    }

    func testMonthLoopContinuation_ProceedWithFatal_ThrowsFatal() {
        // EOM-skip path (interval connection loss): returns .proceed with a fatal set — must still throw.
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .proceed, hasFatal: true, paused: false
            ),
            .throwFatal
        )
    }

    func testMonthLoopContinuation_BreakWithoutFatal_Breaks() {
        // Cancellation pause-break / onMonthUploaded cancelled: pause, no fatal → break the month loop.
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .breakMonthLoop, hasFatal: false, paused: true
            ),
            .breakMonthLoop
        )
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .breakMonthLoop, hasFatal: false, paused: false
            ),
            .breakMonthLoop
        )
    }

    func testMonthLoopContinuation_ProceedPausedNoFatal_Breaks() {
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .proceed, hasFatal: false, paused: true
            ),
            .breakMonthLoop
        )
    }

    func testMonthLoopContinuation_ProceedCleanMonth_Proceeds() {
        // Normal completion: no fatal, not paused, not break → continue to the next month.
        XCTAssertEqual(
            BackupParallelExecutor.monthLoopContinuationAfterFinish(
                eomFlow: .proceed, hasFatal: false, paused: false
            ),
            .proceed
        )
    }

    private func collectEvents(from eventStream: BackupEventStream) async -> [BackupEvent] {
        var events: [BackupEvent] = []
        for await event in eventStream.stream {
            events.append(event)
        }
        return events
    }
}

private extension [BackupEvent] {
    var deferredEventMessages: [String] {
        compactMap { event -> String? in
            guard case .monthChanged(let change) = event,
                  case .uploadDurableSnapshotDeferred(let message) = change.action else {
                return nil
            }
            return message
        }
    }

    var containsIncompleteEvent: Bool {
        contains { event in
            guard case .monthChanged(let change) = event,
                  case .incomplete = change.action else {
                return false
            }
            return true
        }
    }
}
