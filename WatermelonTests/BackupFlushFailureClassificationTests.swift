import XCTest
@testable import Watermelon

final class BackupFlushFailureClassificationTests: XCTestCase {

    // MARK: - Classifier branch coverage

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
            committedAssets: [Data([0x01])],
            committedTombstones: [],
            underlying: CancellationError()
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .cancelled,
                       "Cancellation precedence wins over snapshot-write-failed even with non-empty committed sets.")
    }

    func testRawNSURLErrorCancelledIsNotCancelledOutsideFlushError() {
        // WebDAV classifier does not list NSURLErrorCancelled among its
        // connection-loss codes, so the deterministic result is .other.
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
            committedAssets: [Data([0x02])],
            committedTombstones: [],
            underlying: inner
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .connectionUnavailable,
                       "isConnectionUnavailableErrorIncludingFlushUnderlying walks BackupErrorChain into the underlying.")
    }

    func testClassifiesSnapshotWriteFailedPartial_WhenUnderlyingIsNeitherCancellationNorConnection() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let inner = NSError(domain: "arbitrary-test-domain", code: 1)
        let wrapped = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [Data([0x03])],
            committedTombstones: [],
            underlying: inner
        )
        XCTAssertEqual(BackupFlushFailureClassification.classify(wrapped, on: profile),
                       .snapshotWriteFailedPartial)
    }

    func testClassifiesOtherForGenericError() {
        let profile = TestFixtures.makeServerProfile(storageType: .webdav)
        let error = NSError(domain: "GenericTest", code: 42)
        XCTAssertEqual(BackupFlushFailureClassification.classify(error, on: profile),
                       .other)
    }

    // MARK: - Per-site lumping (pins the .snapshotWriteFailedPartial vs .other policy at each site)

    func testForegroundIntervalLumpsSnapshotPartialAndOther() {
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.foregroundIntervalAction,
                       .logWarningAndContinue)
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundIntervalAction,
                       .logWarningAndContinue)
    }

    func testBackgroundIntervalLumpsSnapshotPartialAndOther() {
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.backgroundIntervalAction,
                       .logErrorAndContinue)
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundIntervalAction,
                       .logErrorAndContinue)
    }

    func testBackgroundEndOfMonthLumpsSnapshotPartialAndOther() {
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.backgroundEndOfMonthAction,
                       .recordReasonLogError)
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundEndOfMonthAction,
                       .recordReasonLogError)
    }

    func testForegroundEndOfMonthDistinguishesSnapshotPartialFromOther() {
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.foregroundEndOfMonthAction,
                       .logErrorTryDeferDurableSnapshotSuppressRethrow)
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundEndOfMonthAction,
                       .logErrorTryDeferDurableSnapshotOrRethrow)
        XCTAssertNotEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.foregroundEndOfMonthAction,
                          BackupFlushFailureClassification.other.foregroundEndOfMonthAction)
    }

    // MARK: - Per-site full-mapping coverage (regression safety for the full policy table)

    func testForegroundIntervalActionMappingComplete() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.foregroundIntervalAction,
                       .continueAssetLoopAndResetCounter)
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.foregroundIntervalAction,
                       .pauseAndBreakAssetLoop)
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.foregroundIntervalAction,
                       .abortMonthBreakAssetLoop)
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.foregroundIntervalAction,
                       .logWarningAndContinue)
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundIntervalAction,
                       .logWarningAndContinue)
    }

    func testForegroundEndOfMonthActionMappingComplete() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.foregroundEndOfMonthAction,
                       .ignoreConcurrentReject)
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.foregroundEndOfMonthAction,
                       .pauseAndBreakMonthLoop)
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.foregroundEndOfMonthAction,
                       .abortMonthBreakMonthLoop)
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.foregroundEndOfMonthAction,
                       .logErrorTryDeferDurableSnapshotSuppressRethrow)
        XCTAssertEqual(BackupFlushFailureClassification.other.foregroundEndOfMonthAction,
                       .logErrorTryDeferDurableSnapshotOrRethrow)
    }

    func testBackgroundIntervalActionMappingComplete() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.backgroundIntervalAction,
                       .continueAssetLoopAndResetCounter)
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.backgroundIntervalAction,
                       .ignoreSilently)
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.backgroundIntervalAction,
                       .abortProfileLogError)
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.backgroundIntervalAction,
                       .logErrorAndContinue)
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundIntervalAction,
                       .logErrorAndContinue)
    }

    func testBackgroundEndOfMonthActionMappingComplete() {
        XCTAssertEqual(BackupFlushFailureClassification.concurrentFlushRejected.backgroundEndOfMonthAction,
                       .continueMonthLoop)
        XCTAssertEqual(BackupFlushFailureClassification.cancelled.backgroundEndOfMonthAction,
                       .ignoreSilently)
        XCTAssertEqual(BackupFlushFailureClassification.connectionUnavailable.backgroundEndOfMonthAction,
                       .abortProfileLogError)
        XCTAssertEqual(BackupFlushFailureClassification.snapshotWriteFailedPartial.backgroundEndOfMonthAction,
                       .recordReasonLogError)
        XCTAssertEqual(BackupFlushFailureClassification.other.backgroundEndOfMonthAction,
                       .recordReasonLogError)
    }
}
