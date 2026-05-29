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
            RepoJSONLReadError.missingHeader,
            // Bug-IX P04 R04 CodexReviewerB F1: bucket-level errors must not be path-level not-found
            NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 404,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NoSuchBucket"]
            ),
            RemoteStorageClientError.underlying(NSError(
                domain: S3ErrorClassifier.errorDomain,
                code: 404,
                userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "NoSuchBucket"]
            )),
            NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "STATUS_BAD_NETWORK_NAME"]
            ),
            NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "STATUS_REDIRECTOR_NOT_STARTED"]
            )
        ]
        for error in cases {
            XCTAssertFalse(RemoteStorageErrorClassifier.isNotFound(error), "\(error)")
            XCTAssertEqual(RemoteStorageErrorClassifier.isNotFound(error), isStorageNotFoundError(error), "\(error)")
        }
    }

    // P04 R24: PROPFIND child-entry 404 is remapped to 424 so
    // isStorageNotFoundError does not misclassify it as directory-not-found.
    func testWebDAVChildPropfind424_isNotNotFound() {
        let error = NSError(domain: WebDAVClient.errorDomain, code: 424)
        XCTAssertFalse(RemoteStorageErrorClassifier.isNotFound(error))
        XCTAssertEqual(RemoteStorageErrorClassifier.isNotFound(error), isStorageNotFoundError(error))
    }

    func testWebDAVWatchdogTimeouts_classifyTransient() {
        let codes = [-1301, -1302, -1303]
        for code in codes {
            let raw = NSError(domain: WebDAVClient.errorDomain, code: code)
            let wrapped = RemoteStorageClientError.underlying(raw)
            for error in [raw as Error, wrapped as Error] {
                XCTAssertTrue(
                    WebDAVErrorClassifier.isConnectionUnavailable(error),
                    "WebDAVErrorClassifier.isConnectionUnavailable expected true for code \(code): \(error)"
                )
                XCTAssertTrue(
                    RemoteWriteClassifier.isTransientVerifyFailure(error),
                    "isTransientVerifyFailure expected true for code \(code): \(error)"
                )
            }
        }
    }

    func testSMBShareErrors_classifyTransient() {
        let cases: [Error] = [
            NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "STATUS_BAD_NETWORK_NAME"]
            ),
            NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "STATUS_REDIRECTOR_NOT_STARTED"]
            )
        ]
        for error in cases {
            XCTAssertTrue(
                RemoteWriteClassifier.isTransientVerifyFailure(error),
                "expected transient: \(error)"
            )
        }
    }
}
