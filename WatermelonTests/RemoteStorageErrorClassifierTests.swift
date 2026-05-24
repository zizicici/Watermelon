import XCTest
@testable import Watermelon

final class RemoteStorageErrorClassifierTests: XCTestCase {
    func testNotFoundBackendShapesAndLegacyWrapper() {
        let cases: [Error] = [
            NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError),
            NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError),
            NSError(domain: NSURLErrorDomain, code: NSURLErrorFileDoesNotExist),
            NSError(domain: WebDAVClient.errorDomain, code: 404),
            NSError(domain: S3ErrorClassifier.errorDomain, code: 404),
            NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 200,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NoSuchKey"]
            ),
            NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 200,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NotFound"]
            ),
            NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "STATUS_OBJECT_NAME_NOT_FOUND"]
            ),
            POSIXError(.ENOENT),
            RemoteStorageClientError.underlying(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError))
        ]

        for error in cases {
            XCTAssertTrue(RemoteStorageErrorClassifier.isNotFound(error), "\(error)")
            XCTAssertEqual(RemoteStorageErrorClassifier.isNotFound(error), isStorageNotFoundError(error), "\(error)")
        }
    }

    func testCancellationAndNonNotFoundShapesDoNotMatch() {
        let cases: [Error] = [
            CancellationError(),
            NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled),
            RemoteStorageClientError.underlying(NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)),
            RemoteStorageClientError.notConnected,
            RemoteStorageClientError.unavailable,
            RemoteStorageClientError.externalStorageUnavailable,
            RemoteStorageClientError.invalidConfiguration,
            RemoteStorageClientError.unsupportedStorageType("x"),
            NSError(domain: WebDAVClient.errorDomain, code: 403),
            NSError(domain: WebDAVClient.errorDomain, code: 503),
            NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 503,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "SlowDown"]
            ),
            NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 500,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "InternalError"]
            ),
            RepoJSONLReadError.missingHeader
        ]

        for error in cases {
            XCTAssertFalse(RemoteStorageErrorClassifier.isNotFound(error), "\(error)")
            XCTAssertEqual(RemoteStorageErrorClassifier.isNotFound(error), isStorageNotFoundError(error), "\(error)")
        }
    }
}
