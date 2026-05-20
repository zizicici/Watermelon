import XCTest
@testable import Watermelon

final class RemoteWriteClassifierTests: XCTestCase {
    func testCancellationShapesNormalize() {
        let url = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let nested = NSError(
            domain: "outer",
            code: 1,
            userInfo: [NSUnderlyingErrorKey: url]
        )
        let cases: [Error] = [
            CancellationError(),
            url,
            RemoteStorageClientError.underlying(url),
            nested
        ]

        for error in cases {
            XCTAssertTrue(RemoteWriteClassifier.isCancellation(error), "\(error)")
            XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(error), .cancelled)
        }
    }

    func testClassifyVerifyFailure_transientStorageAndBackendShapes() {
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(RemoteStorageClientError.notConnected), .transient)
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(RemoteStorageClientError.unavailable), .transient)

        let webDAV = NSError(domain: WebDAVClient.errorDomain, code: 503)
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(webDAV), .transient)

        let s3 = NSError(
            domain: S3ErrorClassifier.errorDomain,
            code: 503,
            userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "SlowDown"]
        )
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(s3), .transient)
    }

    func testClassifyVerifyFailure_notFoundAndPermissionArePermanent() {
        let notFound = RemoteStorageClientError.underlying(NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileNoSuchFileError
        ))
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(notFound), .permanent)

        let permission = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoPermissionError)
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(permission), .permanent)
    }

    func testNSErrorChainUnwrapsStorageAndUnderlyingError() {
        let inner = NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        let outer = NSError(domain: "outer", code: 7, userInfo: [NSUnderlyingErrorKey: inner])
        let chain = RemoteWriteClassifier.nsErrorChain(RemoteStorageClientError.underlying(outer))

        XCTAssertTrue(chain.contains { $0.domain == "outer" && $0.code == 7 })
        XCTAssertTrue(chain.contains { $0.domain == NSURLErrorDomain && $0.code == NSURLErrorTimedOut })
    }

    func testCancellationCauseTerminatesOnDeeplyNestedChain() {
        var error: Error = NSError(domain: "leaf", code: 42)
        for _ in 0..<200 {
            error = NSError(domain: "wrap", code: 1, userInfo: [NSUnderlyingErrorKey: error as NSError])
        }
        XCTAssertFalse(RemoteWriteClassifier.isCancellation(error))
    }

    func testCancellationCauseFindsCancellationInDeeplyNestedChain() {
        let cancellation = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        var error: Error = cancellation
        for _ in 0..<200 {
            error = NSError(domain: "wrap", code: 1, userInfo: [NSUnderlyingErrorKey: error as NSError])
        }
        XCTAssertTrue(RemoteWriteClassifier.isCancellation(error))
    }
}
