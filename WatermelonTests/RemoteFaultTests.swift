import Foundation
import XCTest
@testable import Watermelon

/// Arch-VII A-I B1: RemoteFault is the single boundary classifier; verify its buckets and
/// that it agrees with the primitives it is built on (no behavioral regression).
final class RemoteFaultTests: XCTestCase {
    func testClassifyExhaustivelyMapsEachBucket() {
        // Exhaustiveness guard: every case must be reachable and switchable.
        for fault in [RemoteFault.notFound, .retryable, .cancelled, .terminal] {
            switch fault {
            case .notFound: XCTAssertFalse(fault.isRetryable)
            case .retryable: XCTAssertTrue(fault.isRetryable)
            case .cancelled: XCTAssertFalse(fault.isRetryable)
            case .terminal: XCTAssertFalse(fault.isRetryable)
            }
        }
    }

    func testCancellationWinsOverEverything() {
        XCTAssertEqual(RemoteFault.classify(CancellationError()), .cancelled)
        let urlCancel = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        XCTAssertEqual(RemoteFault.classify(urlCancel), .cancelled)
        // A cancelled error must never be read as absence.
        XCTAssertNotEqual(RemoteFault.classify(urlCancel), .notFound)
    }

    func testNotFoundBucketAgreesWithPrimitive() {
        let s3Missing = RemoteStorageClientError.underlying(
            NSError(domain: S3ErrorClassifier.errorDomain, code: 404,
                    userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NoSuchKey"])
        )
        XCTAssertTrue(isStorageNotFoundError(s3Missing))
        XCTAssertEqual(RemoteFault.classify(s3Missing), .notFound)

        let cocoaMissing = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError)
        XCTAssertEqual(RemoteFault.classify(cocoaMissing), .notFound)
    }

    func testRetryableBucketAgreesWithTransientPrimitive() {
        let unavailable = RemoteStorageClientError.unavailable
        XCTAssertTrue(RemoteWriteClassifier.isTransientVerifyFailure(unavailable))
        XCTAssertEqual(RemoteFault.classify(unavailable), .retryable)

        let s3Throttle = NSError(domain: S3ErrorClassifier.errorDomain, code: 503,
                                 userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "SlowDown"])
        XCTAssertEqual(RemoteFault.classify(s3Throttle), .retryable)
    }

    func testTerminalBucketForUnclassifiedAndPermanent() {
        // NoSuchBucket is a config/permanent fault, NOT absence.
        let noBucket = NSError(domain: S3ErrorClassifier.errorDomain, code: 404,
                               userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NoSuchBucket"])
        XCTAssertFalse(isStorageNotFoundError(noBucket))
        XCTAssertEqual(RemoteFault.classify(noBucket), .terminal)

        let unknown = NSError(domain: "totally.unknown", code: 7)
        XCTAssertEqual(RemoteFault.classify(unknown), .terminal)
    }
}

/// Arch-VII A-I B1: the unified NSError collector replaces 4 ad-hoc walkers. Verify it
/// reaches the same nested NSErrors the per-classifier copies relied on, including across
/// RemoteStorageClientError.underlying and V2 wrapper types.
final class UnifiedNSErrorChainTests: XCTestCase {
    func testCollectsAcrossUnderlyingAndStorageWrapper() {
        let leaf = NSError(domain: S3ErrorClassifier.errorDomain, code: 500,
                           userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "InternalError"])
        let wrapped = RemoteStorageClientError.underlying(leaf)
        let chain = BackupErrorChain.nsErrorChain(wrapped)
        XCTAssertTrue(chain.contains { $0.domain == S3ErrorClassifier.errorDomain && $0.code == 500 },
                      "must peel RemoteStorageClientError.underlying to reach the S3 leaf")
    }

    func testCollectsAcrossNSUnderlyingErrorKey() {
        let leaf = NSError(domain: "leaf", code: 1)
        let mid = NSError(domain: "mid", code: 2, userInfo: [NSUnderlyingErrorKey: leaf])
        let chain = BackupErrorChain.nsErrorChain(mid).map(\.domain)
        XCTAssertEqual(chain, ["mid", "leaf"])
    }

    func testRemoteWriteClassifierDelegatesToUnifiedChain() {
        // RemoteWriteClassifier.nsErrorChain now delegates; transient detection still finds the leaf.
        let throttle = RemoteStorageClientError.underlying(
            NSError(domain: WebDAVClient.errorDomain, code: 503)
        )
        XCTAssertTrue(RemoteWriteClassifier.isTransientVerifyFailure(throttle))
    }
}

/// Arch-VII A-I B1: S3 not-found lives in one place; S3Client + RemoteStorageErrorClassifier
/// must agree with S3ErrorClassifier.isNotFound for every shape.
final class S3NotFoundConsolidationTests: XCTestCase {
    private func s3(_ serverCode: String?, code: Int) -> Error {
        var info: [String: Any] = [:]
        if let serverCode { info[S3ErrorClassifier.userInfoServerCodeKey] = serverCode }
        return RemoteStorageClientError.underlying(
            NSError(domain: S3ErrorClassifier.errorDomain, code: code, userInfo: info)
        )
    }

    func testNoSuchKeyIsNotFound() {
        XCTAssertTrue(S3ErrorClassifier.isNotFound(s3("NoSuchKey", code: 404)))
        XCTAssertTrue(isStorageNotFoundError(s3("NoSuchKey", code: 404)))
    }

    func testNotFoundServerCodeIsNotFound() {
        XCTAssertTrue(S3ErrorClassifier.isNotFound(s3("NotFound", code: 404)))
    }

    func testBareCode404IsNotFound() {
        XCTAssertTrue(S3ErrorClassifier.isNotFound(s3(nil, code: 404)))
    }

    func testNoSuchBucketIsNotAbsence() {
        XCTAssertFalse(S3ErrorClassifier.isNotFound(s3("NoSuchBucket", code: 404)))
        XCTAssertFalse(isStorageNotFoundError(s3("NoSuchBucket", code: 404)))
    }

    func testRemoteStorageClassifierAgreesWithS3Classifier() {
        for (sc, code) in [("NoSuchKey", 404), ("NotFound", 404), ("NoSuchBucket", 404), ("SlowDown", 503)] as [(String, Int)] {
            let err = s3(sc, code: code)
            XCTAssertEqual(isStorageNotFoundError(err), S3ErrorClassifier.isNotFound(err),
                           "RemoteStorageErrorClassifier and S3ErrorClassifier disagree on \(sc)")
        }
    }
}
