import XCTest
@testable import Watermelon

// SFTP no-such-file (SFTPError.errorStatus) and the Citadel/NIO transient shapes are NOT exercised
// here: the test target does not link Citadel/NIO (see SFTPErrorClassifierTests), so those error
// values can't be constructed. RemoteFaultLite still routes them in-module; the Foundation-level
// SFTP branch (POSIX connection codes via SFTPErrorClassifier) is covered below.
final class RemoteFaultLiteTests: XCTestCase {
    private func classify(_ error: Error) -> RemoteFaultLite.Category {
        RemoteFaultLite.classify(error)
    }

    private func s3Error(serverCode: String?, status: Int) -> Error {
        var userInfo: [String: Any] = [S3ErrorClassifier.userInfoStatusCodeKey: status]
        if let serverCode {
            userInfo[S3ErrorClassifier.userInfoServerCodeKey] = serverCode
        }
        let ns = NSError(domain: S3ErrorClassifier.errorDomain, code: status, userInfo: userInfo)
        return RemoteStorageClientError.underlying(ns)
    }

    private func webdavError(status: Int) -> Error {
        let ns = NSError(
            domain: WebDAVClient.errorDomain,
            code: status,
            userInfo: [NSLocalizedDescriptionKey: "WebDAV request failed (\(status))"]
        )
        return RemoteStorageClientError.underlying(ns)
    }

    private func urlError(_ code: Int) -> NSError {
        NSError(domain: NSURLErrorDomain, code: code)
    }

    private func posix(_ code: Int32) -> NSError {
        NSError(domain: NSPOSIXErrorDomain, code: Int(code))
    }

    // MARK: - Cancellation

    func testCancellationError() {
        XCTAssertEqual(classify(CancellationError()), .cancelled)
    }

    func testURLSessionCancelledIsCancelled() {
        XCTAssertEqual(classify(urlError(NSURLErrorCancelled)), .cancelled)
    }

    func testWrappedCancellationIsCancelled() {
        XCTAssertEqual(
            classify(RemoteStorageClientError.underlying(urlError(NSURLErrorCancelled))),
            .cancelled
        )
    }

    // MARK: - WebDAV

    func testWebDAV404IsNotFound() {
        XCTAssertEqual(classify(webdavError(status: 404)), .notFound)
    }

    func testWebDAV401IsTerminal() {
        XCTAssertEqual(classify(webdavError(status: 401)), .terminal)
    }

    func testWebDAV403IsTerminal() {
        XCTAssertEqual(classify(webdavError(status: 403)), .terminal)
    }

    func testWebDAV500IsTerminal() {
        XCTAssertEqual(classify(webdavError(status: 500)), .terminal)
    }

    // MARK: - S3

    func testS3NoSuchKeyIsNotFound() {
        XCTAssertEqual(classify(s3Error(serverCode: "NoSuchKey", status: 404)), .notFound)
    }

    func testS3NotFoundCodeIsNotFound() {
        XCTAssertEqual(classify(s3Error(serverCode: "NotFound", status: 404)), .notFound)
    }

    func testS3ObjectHTTP404IsNotFound() {
        XCTAssertEqual(classify(s3Error(serverCode: nil, status: 404)), .notFound)
    }

    func testS3NoSuchBucketIsTerminalNotNotFound() {
        let category = classify(s3Error(serverCode: "NoSuchBucket", status: 404))
        XCTAssertEqual(category, .terminal)
        XCTAssertNotEqual(category, .notFound)
    }

    func testS3SlowDownIsRetryable() {
        XCTAssertEqual(classify(s3Error(serverCode: "SlowDown", status: 503)), .retryable)
    }

    func testS3ServiceUnavailableIsRetryable() {
        XCTAssertEqual(classify(s3Error(serverCode: "ServiceUnavailable", status: 503)), .retryable)
    }

    func testS3RetryableServerStatusWithoutCodeIsRetryable() {
        XCTAssertEqual(classify(s3Error(serverCode: nil, status: 503)), .retryable)
    }

    func testS3AccessDeniedIsTerminal() {
        XCTAssertEqual(classify(s3Error(serverCode: "AccessDenied", status: 403)), .terminal)
    }

    // MARK: - URL-session transport

    func testURLTimeoutIsRetryable() {
        XCTAssertTrue(S3ErrorClassifier.isConnectionUnavailableURLErrorCode(NSURLErrorTimedOut))
        XCTAssertEqual(classify(urlError(NSURLErrorTimedOut)), .retryable)
    }

    func testURLNotConnectedIsRetryable() {
        XCTAssertEqual(classify(urlError(NSURLErrorNotConnectedToInternet)), .retryable)
    }

    func testWrappedURLNetworkLostIsRetryable() {
        XCTAssertEqual(
            classify(RemoteStorageClientError.underlying(urlError(NSURLErrorNetworkConnectionLost))),
            .retryable
        )
    }

    // MARK: - SMB

    private func smb(_ token: String) -> NSError {
        NSError(domain: "AMSMB2", code: 2, userInfo: [NSLocalizedDescriptionKey: token])
    }

    func testSMBNotFoundTokenIsNotFound() {
        XCTAssertEqual(classify(smb("STATUS_OBJECT_NAME_NOT_FOUND")), .notFound)
    }

    // Clear object/path/file absence tokens must stay `.notFound` after the BAD_NETWORK_NAME reclassify.
    func testSMBObjectPathAbsenceTokensAreNotFound() {
        for token in ["STATUS_NO_SUCH_FILE", "STATUS_OBJECT_PATH_NOT_FOUND", "STATUS_NOT_FOUND"] {
            XCTAssertEqual(classify(smb(token)), .notFound, "\(token) must classify .notFound")
        }
    }

    func testSMBPosixENOENTIsNotFound() {
        XCTAssertEqual(classify(posix(ENOENT)), .notFound)
    }

    func testSMBConnectionTokenIsRetryable() {
        XCTAssertEqual(classify(smb("STATUS_IO_TIMEOUT")), .retryable)
    }

    // A transient share outage must never be read as object absence (the P05 destructive-prune hazard).
    func testSMBBadNetworkNameIsRetryableNotNotFound() {
        let category = classify(smb("STATUS_BAD_NETWORK_NAME"))
        XCTAssertEqual(category, .retryable)
        XCTAssertNotEqual(category, .notFound)
    }

    func testSMBRedirectorNotStartedIsRetryableNotNotFound() {
        let category = classify(smb("STATUS_REDIRECTOR_NOT_STARTED"))
        XCTAssertEqual(category, .retryable)
        XCTAssertNotEqual(category, .notFound)
    }

    // A wrapped chain carrying a retryable backend/session fault must win over an ambiguous not-found
    // token in an outer description — retryable is tested before notFound.
    func testWrappedRetryableWithAmbiguousNotFoundTokenIsNotNotFound() {
        let inner = smb("STATUS_IO_TIMEOUT")
        let outer = NSError(
            domain: "AMSMB2",
            code: 2,
            userInfo: [
                NSLocalizedDescriptionKey: "STATUS_OBJECT_NAME_NOT_FOUND",
                NSUnderlyingErrorKey: inner
            ]
        )
        let category = classify(outer)
        XCTAssertEqual(category, .retryable)
        XCTAssertNotEqual(category, .notFound)
    }

    func testSMBPosixTimedOutIsRetryable() {
        XCTAssertEqual(classify(posix(ETIMEDOUT)), .retryable)
    }

    // MARK: - SFTP (Foundation-level connection shape)

    func testPosixConnectionRefusedIsRetryable() {
        // ECONNREFUSED is recognized by SFTPErrorClassifier's POSIX network set.
        XCTAssertEqual(classify(posix(ECONNREFUSED)), .retryable)
    }

    // MARK: - Local / Foundation

    func testCocoaNoSuchFileIsNotFound() {
        XCTAssertEqual(classify(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError)), .notFound)
    }

    func testCocoaReadNoSuchFileIsNotFound() {
        XCTAssertEqual(classify(NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError)), .notFound)
    }

    func testWrappedCocoaNoSuchFileIsNotFound() {
        let inner = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError)
        XCTAssertEqual(classify(RemoteStorageClientError.underlying(inner)), .notFound)
    }

    // MARK: - RemoteStorageClientError surface

    func testNotConnectedIsRetryable() {
        XCTAssertEqual(classify(RemoteStorageClientError.notConnected), .retryable)
    }

    func testUnavailableIsRetryable() {
        XCTAssertEqual(classify(RemoteStorageClientError.unavailable), .retryable)
    }

    func testInvalidConfigurationIsTerminal() {
        XCTAssertEqual(classify(RemoteStorageClientError.invalidConfiguration), .terminal)
    }

    func testExternalStorageUnavailableIsTerminal() {
        XCTAssertEqual(classify(RemoteStorageClientError.externalStorageUnavailable), .terminal)
    }

    func testUnsupportedStorageTypeIsTerminal() {
        XCTAssertEqual(classify(RemoteStorageClientError.unsupportedStorageType("sftp")), .terminal)
    }

    // MARK: - Wrapper recursion

    func testNSUnderlyingErrorKeyRecursionToNotFound() {
        let inner = NSError(domain: WebDAVClient.errorDomain, code: 404)
        let outer = NSError(domain: "OuterDomain", code: 7, userInfo: [NSUnderlyingErrorKey: inner])
        XCTAssertEqual(classify(outer), .notFound)
    }

    func testNSUnderlyingErrorKeyRecursionToRetryable() {
        let inner = urlError(NSURLErrorTimedOut)
        let outer = NSError(domain: "OuterDomain", code: 7, userInfo: [NSUnderlyingErrorKey: inner])
        XCTAssertEqual(classify(outer), .retryable)
    }

    func testDoubleWrappedNotFound() {
        let inner = NSError(domain: WebDAVClient.errorDomain, code: 404)
        let nested = NSError(domain: "Mid", code: 1, userInfo: [NSUnderlyingErrorKey: inner])
        XCTAssertEqual(classify(RemoteStorageClientError.underlying(nested)), .notFound)
    }

    // MARK: - Terminal fallback

    func testGenericErrorIsTerminal() {
        XCTAssertEqual(classify(NSError(domain: "SomethingElse", code: 42)), .terminal)
    }
}
