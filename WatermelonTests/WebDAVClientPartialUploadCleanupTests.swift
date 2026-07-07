import XCTest
@testable import Watermelon

// Locks the partial-upload-cleanup classifier: only a mid-body stall proves the body was not fully sent (a genuine
// partial worth deleting). A response-timeout or a bare cancellation can arrive after the object landed COMPLETE, so
// neither may queue the upload target for deletion — most critically a direct-PUT canonical.
final class WebDAVClientPartialUploadCleanupTests: XCTestCase {
    private func webdavError(_ code: Int) -> Error {
        NSError(domain: WebDAVClient.errorDomain, code: code)
    }

    func testOnlyMidBodyStallQueuesPartialUploadCleanup() {
        XCTAssertTrue(
            WebDAVClient.shouldCleanupPartialUpload(webdavError(WebDAVClient.uploadStalledErrorCode)),
            "a mid-body stall leaves a genuine partial → queue cleanup"
        )
        XCTAssertFalse(
            WebDAVClient.shouldCleanupPartialUpload(webdavError(WebDAVClient.uploadResponseTimeoutErrorCode)),
            "a response-timeout means the body was fully sent → must not delete a possibly-complete object"
        )
        XCTAssertFalse(
            WebDAVClient.shouldCleanupPartialUpload(CancellationError()),
            "a bare cancellation can arrive after the body is sent → must not queue cleanup"
        )
        XCTAssertFalse(
            WebDAVClient.shouldCleanupPartialUpload(NSError(domain: "SomeOtherDomain", code: 1)),
            "an unrelated error is not partial-upload evidence"
        )
    }
}
