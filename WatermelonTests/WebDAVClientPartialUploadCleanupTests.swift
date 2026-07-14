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

final class WebDAVClientReadBackRetryTests: XCTestCase {
    private actor Probe {
        enum Outcome: Sendable {
            case notFound
            case failure
            case success
        }

        private var outcomes: [Outcome]
        private(set) var operationCount = 0
        private(set) var delays: [Duration] = []

        init(_ outcomes: [Outcome]) {
            self.outcomes = outcomes
        }

        func run() throws -> Int {
            operationCount += 1
            switch outcomes.removeFirst() {
            case .notFound:
                throw RemoteStorageClientError.underlying(
                    NSError(domain: WebDAVClient.errorDomain, code: 404)
                )
            case .failure:
                throw NSError(domain: "WebDAVClientReadBackRetryTests", code: 1)
            case .success:
                return 42
            }
        }

        func record(delay: Duration) {
            delays.append(delay)
        }
    }

    func testRetriesWrappedNotFoundWithBoundedBackoffThenSucceeds() async throws {
        let probe = Probe([.notFound, .notFound, .notFound, .success])

        let value = try await WebDAVClient.retryReadBackNotFound(
            operation: { try await probe.run() },
            wait: { await probe.record(delay: $0) }
        )

        let operationCount = await probe.operationCount
        let delays = await probe.delays
        XCTAssertEqual(value, 42)
        XCTAssertEqual(operationCount, 4)
        XCTAssertEqual(delays, [.milliseconds(500), .seconds(1), .seconds(2)])
    }

    func testPersistentNotFoundStopsAfterFourAttempts() async {
        let probe = Probe([.notFound, .notFound, .notFound, .notFound])

        do {
            _ = try await WebDAVClient.retryReadBackNotFound(
                operation: { try await probe.run() },
                wait: { await probe.record(delay: $0) }
            )
            XCTFail("persistent 404 must stop after the bounded retries")
        } catch {
            XCTAssertTrue(error is RemoteReadBackRetryExhaustedError)
            XCTAssertTrue(WebDAVClient.isHTTPNotFound(error))
        }

        let operationCount = await probe.operationCount
        let delays = await probe.delays
        XCTAssertEqual(operationCount, 4)
        XCTAssertEqual(delays, [.milliseconds(500), .seconds(1), .seconds(2)])
    }

    func testUnrelatedFailureIsNotRetried() async {
        let probe = Probe([.failure])

        do {
            _ = try await WebDAVClient.retryReadBackNotFound(
                operation: { try await probe.run() },
                wait: { await probe.record(delay: $0) }
            )
            XCTFail("an unrelated failure must not be retried")
        } catch {
            XCTAssertFalse(WebDAVClient.isHTTPNotFound(error))
        }

        let operationCount = await probe.operationCount
        let delays = await probe.delays
        XCTAssertEqual(operationCount, 1)
        XCTAssertEqual(delays, [])
    }

    func testCancellationDuringBackoffStopsRetrying() async {
        let probe = Probe([.notFound, .success])

        do {
            _ = try await WebDAVClient.retryReadBackNotFound(
                operation: { try await probe.run() },
                wait: { _ in throw CancellationError() }
            )
            XCTFail("backoff cancellation must stop retrying")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }

        let operationCount = await probe.operationCount
        XCTAssertEqual(operationCount, 1)
    }
}
