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
        // U01: V2 batch commit failures must pause the worker rather than warn-and-continue —
        // continuing past a failed batch commit accumulates orphan resources and provisional
        // progress that cannot be honestly reconciled. The classifier's `.other` branch now maps
        // to `.pauseAndBreakAssetLoop` for the interval-flush path.
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundIntervalAction,
                       .pauseAndBreakAssetLoop)
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
        // U01: V2 batch commit failures in the background must break the asset loop (so orphan
        // resources don't accumulate beyond the 200-op redo bound) rather than warn-and-continue.
        // V1 never reaches this branch — V1's `flushToRemote` returns `.none` and never throws.
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundIntervalAction,
                       .logErrorAndBreakAssetLoop)
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

    private func assertDisplayErrorPreservesUnderlying(
        _ flushError: V2MonthSession.FlushError,
        expectedDomain: String,
        expectedCode: Int,
        file: StaticString = #file,
        line: UInt = #line
    ) {
        guard case .snapshotWriteFailed(_, _, let underlying) = flushError else {
            XCTFail("dispatch.displayError must be FlushError.snapshotWriteFailed (the wrapper)",
                    file: file, line: line)
            return
        }
        let actual = underlying as NSError
        XCTAssertEqual(actual.domain, expectedDomain,
                       "displayError must preserve the original underlying error's domain — if this fails, a refactor may have substituted the wrapper's underlying",
                       file: file, line: line)
        XCTAssertEqual(actual.code, expectedCode,
                       "displayError must preserve the original underlying error's code",
                       file: file, line: line)
    }

    // C0: classifyPartialOutcome (shared classifier consumed by all four PartialDispatch helpers)

    func testClassifyPartialOutcome_CompletedReturnsNil() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile))
    }

    func testClassifyPartialOutcome_CancellationErrorUnderlying_CategoryCancelled() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: CancellationError())
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .cancelled)
        assertDisplayErrorRoundTripsPayload(classified.flushError)
    }

    func testClassifyPartialOutcome_NSURLErrorCancelledUnderlying_CategoryCancelled() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled))
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .cancelled,
                       "NSURLErrorCancelled in the underlying chain must surface as cancellationCause via the FlushError walker.")
        assertDisplayErrorRoundTripsPayload(classified.flushError)
    }

    func testClassifyPartialOutcome_ConnectionUnavailableUnderlying_CategoryConnectionUnavailable() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet))
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .connectionUnavailable)
        assertDisplayErrorRoundTripsPayload(classified.flushError)
    }

    func testClassifyPartialOutcome_SoftUnderlying_CategoryOther() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let outcome = makePartialOutcome(underlying: NSError(domain: "soft", code: 1))
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .other)
        assertDisplayErrorRoundTripsPayload(classified.flushError)
    }

    func testClassifyPartialOutcome_BothCancellationAndConnectionUnavailable_CategoryCancelled() {
        // Cancellation precedence MUST beat connection-unavailable when both predicates fire on the same chain.
        // Inner = NSURLErrorCancelled (matched by FlushError.cancellationCause walker).
        // Outer = NSURLErrorNotConnectedToInternet wrapping inner via NSUnderlyingErrorKey
        //         (matched by isConnectionUnavailableErrorIncludingFlushUnderlying walker).
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let inner = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let outer = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorNotConnectedToInternet,
            userInfo: [NSUnderlyingErrorKey: inner]
        )
        let outcome = makePartialOutcome(underlying: outer)
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .cancelled,
                       "cancellation precedence MUST beat connection-unavailable when both predicates match the same chain; reversed precedence would classify this as .connectionUnavailable")
        assertDisplayErrorRoundTripsPayload(classified.flushError)
    }

    func testClassifyPartialOutcome_DisplayErrorPreservesUnderlyingPayload() {
        // Pins that the original underlying error survives end-to-end through `classified.flushError`.
        // The existing round-trip helper only checks committedAssets/committedTombstones; this asserts
        // the underlying domain+code (the carrier for userFacingStorageErrorMessage / logging) is intact.
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let sentinelDomain = "WatermelonTests.unit033.sentinel"
        let sentinelCode = 0xDEAD
        let sentinel = NSError(domain: sentinelDomain, code: sentinelCode)
        let outcome = makePartialOutcome(underlying: sentinel)
        guard let classified = BackupFlushFailureClassification.classifyPartialOutcome(outcome, on: profile) else {
            XCTFail("classified must be non-nil for partial outcome")
            return
        }
        XCTAssertEqual(classified.category, .other,
                       "sentinel error must not match cancellation or connection-unavailable walkers; if it does, the test premise is broken")
        assertDisplayErrorPreservesUnderlying(classified.flushError,
                                              expectedDomain: sentinelDomain,
                                              expectedCode: sentinelCode)
    }

    // D0: classifyAssetProcessError (shared classifier consumed by both asset-loop catch arms)

    func testClassifyAssetProcessError_CancellationError_CategoryCancelled() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let classified = BackupFlushFailureClassification.classifyAssetProcessError(CancellationError(), on: profile)
        XCTAssertEqual(classified.category, .cancelled)
        XCTAssertTrue(classified.error is CancellationError,
                      "AssetErrorClassification must carry the original error instance")
    }

    func testClassifyAssetProcessError_RawNSURLErrorCancelled_CategoryOther() {
        // Pins NO widening: raw NSURLErrorCancelled outside FlushError is .other, mirroring the
        // existing top-level rule at testRawNSURLErrorCancelledIsNotCancelledOutsideFlushError.
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let raw = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let classified = BackupFlushFailureClassification.classifyAssetProcessError(raw, on: profile)
        XCTAssertEqual(classified.category, .other,
                       "Raw NSURLErrorCancelled outside FlushError must NOT classify as .cancelled at the asset-process layer; only `error is CancellationError` does")
        let actual = classified.error as NSError
        XCTAssertEqual(actual.domain, NSURLErrorDomain)
        XCTAssertEqual(actual.code, NSURLErrorCancelled)
    }

    func testClassifyAssetProcessError_NSURLErrorNotConnectedToInternet_CategoryConnectionUnavailable() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
        let classified = BackupFlushFailureClassification.classifyAssetProcessError(error, on: profile)
        XCTAssertEqual(classified.category, .connectionUnavailable)
        let actual = classified.error as NSError
        XCTAssertEqual(actual.domain, NSURLErrorDomain)
        XCTAssertEqual(actual.code, NSURLErrorNotConnectedToInternet)
    }

    func testClassifyAssetProcessError_Soft_CategoryOther() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: "soft", code: 1)
        let classified = BackupFlushFailureClassification.classifyAssetProcessError(error, on: profile)
        XCTAssertEqual(classified.category, .other)
        let actual = classified.error as NSError
        XCTAssertEqual(actual.domain, "soft")
        XCTAssertEqual(actual.code, 1)
    }

    // D1: foregroundAssetErrorDispatch

    func testForegroundAssetErrorDispatch_CancellationError_PauseAndBreak() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let dispatch = BackupFlushFailureClassification.foregroundAssetErrorDispatch(
            error: CancellationError(),
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .pauseAndBreakAssetLoop)
        XCTAssertTrue(dispatch.error is CancellationError)
    }

    func testForegroundAssetErrorDispatch_ConnectionUnavailable_AbortMonthDataConnectionLoss() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
        let dispatch = BackupFlushFailureClassification.foregroundAssetErrorDispatch(
            error: error,
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .abortMonthDataConnectionLossBreakAssetLoop)
        let actual = dispatch.error as NSError
        XCTAssertEqual(actual.domain, NSURLErrorDomain)
        XCTAssertEqual(actual.code, NSURLErrorNotConnectedToInternet)
    }

    func testForegroundAssetErrorDispatch_Soft_LogGenericFailureAndContinue() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: "soft", code: 1)
        let dispatch = BackupFlushFailureClassification.foregroundAssetErrorDispatch(
            error: error,
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .logGenericFailureAndContinue)
        let actual = dispatch.error as NSError
        XCTAssertEqual(actual.domain, "soft")
        XCTAssertEqual(actual.code, 1)
    }

    // D2: backgroundAssetErrorDispatch

    func testBackgroundAssetErrorDispatch_CancellationError_BreakAssetLoop() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let dispatch = BackupFlushFailureClassification.backgroundAssetErrorDispatch(
            error: CancellationError(),
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .breakAssetLoop)
        XCTAssertTrue(dispatch.error is CancellationError)
    }

    func testBackgroundAssetErrorDispatch_ConnectionUnavailable_AbortMonthConnectionUnavailable() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
        let dispatch = BackupFlushFailureClassification.backgroundAssetErrorDispatch(
            error: error,
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .abortMonthConnectionUnavailableBreakAssetLoop)
        let actual = dispatch.error as NSError
        XCTAssertEqual(actual.domain, NSURLErrorDomain)
        XCTAssertEqual(actual.code, NSURLErrorNotConnectedToInternet)
    }

    func testBackgroundAssetErrorDispatch_Soft_LogGenericFailureAndContinue() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: "soft", code: 1)
        let dispatch = BackupFlushFailureClassification.backgroundAssetErrorDispatch(
            error: error,
            profile: profile
        )
        XCTAssertEqual(dispatch.action, .logGenericFailureAndContinue)
        let actual = dispatch.error as NSError
        XCTAssertEqual(actual.domain, "soft")
        XCTAssertEqual(actual.code, 1)
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
