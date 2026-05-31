import XCTest
@testable import Watermelon

final class ExternalStorageUnavailableClassifierTests: XCTestCase {
    func testDirectExternalStorageUnavailable() {
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            RemoteStorageClientError.externalStorageUnavailable
        ))
    }

    func testUnderlyingChainExternalStorageUnavailable() {
        let wrapped = RemoteStorageClientError.underlying(
            RemoteStorageClientError.underlying(
                RemoteStorageClientError.externalStorageUnavailable
            )
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(wrapped))
    }

    func testNSUnderlyingErrorKeyWrappedExternalStorageUnavailable() {
        // BackupParallelExecutor wraps inline-complement finalizer failures as
        // NSError with the storage cause under NSUnderlyingErrorKey.
        let wrapped = NSError(
            domain: "BackupParallelExecutor",
            code: -201,
            userInfo: [
                NSLocalizedDescriptionKey: "onMonthUploaded failed",
                NSUnderlyingErrorKey: RemoteStorageClientError.externalStorageUnavailable
            ]
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(wrapped))
    }

    func testNSUnderlyingErrorKeyWrappedUnderlyingChain() {
        let cause = RemoteStorageClientError.underlying(
            RemoteStorageClientError.externalStorageUnavailable
        )
        let wrapped = NSError(
            domain: "BackupParallelExecutor",
            code: -201,
            userInfo: [NSUnderlyingErrorKey: cause]
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(wrapped))
    }

    func testDeeplyNestedNSUnderlyingErrorKeyChain() {
        var current: Error = RemoteStorageClientError.externalStorageUnavailable
        for depth in 0..<6 {
            current = NSError(
                domain: "wrap",
                code: depth,
                userInfo: [NSUnderlyingErrorKey: current]
            )
        }
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(current))
    }

    func testUnrelatedErrorIsNotMisclassified() {
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            RemoteStorageClientError.notConnected
        ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            RemoteStorageClientError.unavailable
        ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            RemoteStorageClientError.invalidConfiguration
        ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            CancellationError()
        ))
    }

    func testNSErrorWithoutUnderlyingKeyTerminatesCleanly() {
        let wrapped = NSError(
            domain: "BackupParallelExecutor",
            code: -201,
            userInfo: [NSLocalizedDescriptionKey: "onMonthUploaded failed"]
        )
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(wrapped))
    }

    func testV2FlushErrorSnapshotWriteFailedSurfacesExternalStorageUnavailable() {
        // V2MonthSession.flushToRemote rewraps SnapshotWriter failures so loss of
        // the external volume during snapshot finalization arrives at the run-error
        // boundary as FlushError → WriteError → RemoteStorageClientError.
        let cause = RemoteStorageClientError.externalStorageUnavailable
        let writeError = SnapshotWriter.WriteError.finalizationFailed(cause)
        let flushError = V2MonthSession.FlushError.postCommitFailed(underlying: writeError)
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(flushError))
    }

    func testSnapshotWriterIOFailureWrappedExternalStorageUnavailable() {
        let writeError = SnapshotWriter.WriteError.ioFailure(
            RemoteStorageClientError.externalStorageUnavailable
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(writeError))
    }

    func testCommitLogWriterIOFailureWrappedExternalStorageUnavailable() {
        // verifyMonthV2 tombstone apply path: CommitLogWriter.write rewraps
        // storage errors as WriteError.ioFailure(...). Inline complement final
        // failure carries this enum through BackupFinalizationFailure.
        let commitError = CommitLogWriter.WriteError.ioFailure(
            RemoteStorageClientError.externalStorageUnavailable
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(commitError))
    }

    func testMetadataCreateGateStagingVerificationFailedSurfacesExternalStorageUnavailable() {
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(
            remotePath: "/repo/staging/sentinel",
            underlying: RemoteStorageClientError.externalStorageUnavailable
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(gateError))
    }

    func testMetadataCreateGateFinalVerificationFailedSurfacesExternalStorageUnavailable() {
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(
            remotePath: "/repo/final/sentinel",
            underlying: RemoteStorageClientError.externalStorageUnavailable
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(gateError))
    }

    func testNSErrorWrappingV2FlushErrorSurfacesExternalStorageUnavailable() {
        // BackupParallelExecutor's NSError wrap can carry a V2 flush wrapper
        // when an inline-complement finalizer rethrows the wrapper unchanged.
        let cause = V2MonthSession.FlushError.postCommitFailed(underlying: CommitLogWriter.WriteError.ioFailure(
                RemoteStorageClientError.externalStorageUnavailable
            ))
        let wrapped = NSError(
            domain: "BackupParallelExecutor",
            code: -201,
            userInfo: [NSUnderlyingErrorKey: cause]
        )
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(wrapped))
    }

    func testV2WrapperWithoutExternalStorageCauseIsNotMisclassified() {
        let flushError = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.ioFailure(
                NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
            ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(flushError))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            V2MonthSession.FlushError.concurrentFlushRejected
        ))
        XCTAssertFalse(RemoteStorageClientError.isLikelyExternalStorageUnavailable(
            MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/x")
        ))
    }
}
