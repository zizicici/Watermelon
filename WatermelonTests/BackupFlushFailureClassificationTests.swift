import XCTest
@testable import Watermelon

final class BackupFlushFailureClassificationTests: XCTestCase {

    // MARK: - Section A: classifier branch coverage (4-case enum)

    func testClassifiesConcurrentFlushRejected() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = V2MonthSession.FlushError.concurrentFlushRejected
        XCTAssertEqual(BackupFlushFailureClassification.classify(error, on: profile),
                       .concurrentFlushRejected)
    }

    func testClassifiesRawCancellationError() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        XCTAssertEqual(BackupFlushFailureClassification.classify(CancellationError(), on: profile),
                       .cancelled)
    }

    func testClassifiesNSURLErrorCancelledWrappedInSnapshotWriteFailed() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let inner = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let wrapped = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [],
            committedTombstones: [],
            underlying: inner
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .cancelled,
                       "FlushError.cancellationCause walks the underlying chain and matches NSURLErrorCancelled.")
    }

    func testClassifiesCancellationErrorWrappedInSnapshotWriteFailed() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let wrapped = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [TestFixtures.fingerprint(0x01)],
            committedTombstones: [],
            underlying: CancellationError()
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .cancelled,
                       "Cancellation precedence wins over snapshot-write-failed even with non-empty committed sets.")
    }

    func testRawNSURLErrorCancelledIsNotCancelledOutsideFlushError() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let raw = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        XCTAssertEqual(BackupFlushFailureClassification.classify(raw, on: profile),
                       .other,
                       "Raw NSURLErrorCancelled outside FlushError must NOT widen to .cancelled and must NOT be treated as .connectionUnavailable under WebDAV.")
    }

    func testClassifiesConnectionUnavailable_NSURLErrorNotConnectedToInternet() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
        XCTAssertEqual(BackupFlushFailureClassification.classify(error, on: profile),
                       .connectionUnavailable)
    }

    func testClassifiesConnectionUnavailable_BuriedInSnapshotWriteFailed() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let inner = NSError(domain: NSURLErrorDomain, code: NSURLErrorNetworkConnectionLost)
        let wrapped = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [TestFixtures.fingerprint(0x02)],
            committedTombstones: [],
            underlying: inner
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .connectionUnavailable,
                       "isConnectionUnavailableErrorIncludingFlushUnderlying walks BackupErrorChain into the underlying.")
    }

    func testClassifiesOtherForGenericError() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: "GenericTest", code: 42)
        XCTAssertEqual(BackupFlushFailureClassification.classify(error, on: profile),
                       .other)
    }

    // MARK: - Section B: per-site catch-arm action mappers (4 inputs × 4 sites = 16 tests)

    func testForegroundIntervalAction_concurrentFlushRejected() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.foregroundIntervalAction,
                       .continueAssetLoopAndResetCounter)
    }
    func testForegroundIntervalAction_cancelled() {
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.foregroundIntervalAction,
                       .pauseAndBreakAssetLoop)
    }
    func testForegroundIntervalAction_connectionUnavailable() {
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.foregroundIntervalAction,
                       .abortMonthBreakAssetLoop)
    }
    func testForegroundIntervalAction_other() {
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundIntervalAction,
                       .logWarningAndContinue)
    }

    func testForegroundEndOfMonthAction_concurrentFlushRejected() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.foregroundEndOfMonthAction,
                       .ignoreConcurrentReject)
    }
    func testForegroundEndOfMonthAction_cancelled() {
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.foregroundEndOfMonthAction,
                       .pauseAndBreakMonthLoop)
    }
    func testForegroundEndOfMonthAction_connectionUnavailable() {
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.foregroundEndOfMonthAction,
                       .abortMonthBreakMonthLoop)
    }
    func testForegroundEndOfMonthAction_otherIsLogErrorAndRethrow() {
        // After unit-006: .other at foreground EOM catch arm is "log + rethrow" (the
        // maybe-emit branch is dead because partial-success is consumed by the boundary
        // helper and never reaches the catch arm).
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundEndOfMonthAction,
                       .logErrorAndRethrow)
    }

    func testBackgroundIntervalAction_concurrentFlushRejected() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.backgroundIntervalAction,
                       .continueAssetLoopAndResetCounter)
    }
    func testBackgroundIntervalAction_cancelled() {
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.backgroundIntervalAction,
                       .ignoreSilently)
    }
    func testBackgroundIntervalAction_connectionUnavailable() {
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.backgroundIntervalAction,
                       .abortProfileLogError)
    }
    func testBackgroundIntervalAction_other() {
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundIntervalAction,
                       .logErrorAndContinue)
    }

    func testBackgroundEndOfMonthAction_concurrentFlushRejected() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.backgroundEndOfMonthAction,
                       .continueMonthLoop)
    }
    func testBackgroundEndOfMonthAction_cancelled() {
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.backgroundEndOfMonthAction,
                       .ignoreSilently)
    }
    func testBackgroundEndOfMonthAction_connectionUnavailable() {
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.backgroundEndOfMonthAction,
                       .abortProfileLogError)
    }
    func testBackgroundEndOfMonthAction_other() {
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundEndOfMonthAction,
                       .recordReasonLogError)
    }

    // MARK: - Section C: per-site partial-dispatch mappers (typed payload assertions)

    private static let probeAssets: Set<Data> = [
        TestFixtures.fingerprint(0xAA),
        TestFixtures.fingerprint(0xAB)
    ]
    private static let probeTombstones: Set<Data> = [
        TestFixtures.fingerprint(0xBB)
    ]

    private func makePartialOutcome(underlying: Error) -> V2MonthFlushOutcome {
        let flushError = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: Self.probeAssets,
            committedTombstones: Self.probeTombstones,
            underlying: underlying
        )
        let delta = BackupMonthFlushDelta(
            didFlush: true,
            committedAssetFingerprints: Self.probeAssets,
            committedTombstoneFingerprints: Self.probeTombstones
        )
        return .commitDurableSnapshotDeferred(delta: delta, flushError: flushError)
    }

    private func assertDisplayErrorRoundTripsPayload(
        _ flushError: V2MonthSession.FlushError,
        file: StaticString = #file,
        line: UInt = #line
    ) {
        guard case .snapshotWriteFailed(let outAssets, let outTombstones, _) = flushError else {
            XCTFail("dispatch.displayError must be FlushError.snapshotWriteFailed (the wrapper)",
                    file: file, line: line)
            return
        }
        XCTAssertEqual(outAssets, Self.probeAssets,
                       "dispatch.displayError must round-trip the constructed committedAssets payload exactly — if this fails, production may be substituting a different value for the wrapper",
                       file: file, line: line)
        XCTAssertEqual(outTombstones, Self.probeTombstones,
                       "dispatch.displayError must round-trip the constructed committedTombstones payload exactly",
                       file: file, line: line)
    }

    // C1–C5: foregroundIntervalPartialDispatch

    func testForegroundIntervalPartialDispatch_CompletedReturnsNil() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ))
    }

    func testForegroundIntervalPartialDispatch_CancellationErrorUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .pauseAndBreakAssetLoop)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundIntervalPartialDispatch_NSURLErrorCancelledUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled))
        guard let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .pauseAndBreakAssetLoop,
                       "NSURLErrorCancelled in the underlying chain must surface as cancellationCause via the FlushError walker.")
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundIntervalPartialDispatch_ConnectionUnavailableUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .abortMonthBreakAssetLoop)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundIntervalPartialDispatch_SoftUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.foregroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logWarningAndContinue)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    // C6–C12: foregroundEndOfMonthPartialDispatch (with shouldFinishMonth axis)

    func testForegroundEndOfMonthPartialDispatch_CompletedReturnsNil_ShouldFinishTrue() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ))
    }

    func testForegroundEndOfMonthPartialDispatch_CompletedReturnsNil_ShouldFinishFalse() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: false
        ))
    }

    func testForegroundEndOfMonthPartialDispatch_CancellationUnderlying_ShouldFinishTrue() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .pauseAndBreakMonthLoop)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundEndOfMonthPartialDispatch_CancellationUnderlying_ShouldFinishFalse() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: false
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .pauseAndBreakMonthLoop,
                       "Cancellation precedence beats shouldFinishMonth — paused regardless.")
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundEndOfMonthPartialDispatch_ConnectionUnavailableUnderlying_ShouldFinishTrue() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .abortMonthBreakMonthLoop)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundEndOfMonthPartialDispatch_SoftUnderlying_ShouldFinishTrue_EmitsDeferred() {
        // Pins the shouldEmit gate: soft underlying AND shouldFinishMonth=true → emit-deferred branch.
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: true
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logErrorAndEmitDeferred)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testForegroundEndOfMonthPartialDispatch_SoftUnderlying_ShouldFinishFalse_NoEmit() {
        // Pins the no-emit gate: soft underlying AND shouldFinishMonth=false → log-only branch.
        // Preserves today's shouldEmitUploadDurableSnapshotDeferred returning false when
        // shouldFinishMonth is false.
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.foregroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile, shouldFinishMonth: false
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logErrorOnly)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    // C13–C17: backgroundIntervalPartialDispatch

    func testBackgroundIntervalPartialDispatch_CompletedReturnsNil() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ))
    }

    func testBackgroundIntervalPartialDispatch_CancellationUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .ignoreSilently)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundIntervalPartialDispatch_NSURLErrorCancelledUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled))
        guard let dispatch = BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .ignoreSilently)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundIntervalPartialDispatch_ConnectionUnavailableUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let dispatch = BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .abortProfileLogError)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundIntervalPartialDispatch_SoftUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .logErrorAndContinue)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    // C18–C22: backgroundEndOfMonthPartialDispatch

    func testBackgroundEndOfMonthPartialDispatch_CompletedReturnsNil() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile
        ))
    }

    func testBackgroundEndOfMonthPartialDispatch_CancellationUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let dispatch = BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .ignoreSilently)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundEndOfMonthPartialDispatch_NSURLErrorCancelledUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled))
        guard let dispatch = BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .ignoreSilently)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundEndOfMonthPartialDispatch_ConnectionUnavailableUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let dispatch = BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .abortProfileLogError)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }

    func testBackgroundEndOfMonthPartialDispatch_SoftUnderlying() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let dispatch = BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
            outcome: outcome, profile: profile
        ) else {
            XCTFail("dispatch must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(dispatch.action, .recordReasonLogError)
        assertDisplayErrorRoundTripsPayload(dispatch.displayError)
    }
}
