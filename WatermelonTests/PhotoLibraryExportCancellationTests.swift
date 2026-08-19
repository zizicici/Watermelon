import XCTest
@testable import Watermelon

final class PhotoLibraryExportCancellationTests: XCTestCase {
    func testCancellationResumesCallerBeforeWriteCompletesAndDefersCleanup() async {
        let cleanup = CleanupProbe()
        let state = PhotoLibraryService.ExportRequestState {
            cleanup.markCalled()
        }
        let writeStarted = expectation(description: "write started")
        let waiter = Task {
            try await withCheckedThrowingContinuation { continuation in
                XCTAssertTrue(state.bind(continuation: continuation))
                XCTAssertTrue(state.beginWrite())
                writeStarted.fulfill()
            }
        }

        await fulfillment(of: [writeStarted], timeout: 1)
        state.requestCancellation()

        do {
            try await waiter.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertFalse(cleanup.wasCalled)
        XCTAssertFalse(state.callerCanRemoveTemporaryFile)

        state.complete(.success(()))

        XCTAssertTrue(cleanup.wasCalled)
        XCTAssertTrue(state.callerCanRemoveTemporaryFile)
    }

    func testCancellationBeforeWritePreventsWriteAndNeedsNoDeferredCleanup() async {
        let cleanup = CleanupProbe()
        let state = PhotoLibraryService.ExportRequestState {
            cleanup.markCalled()
        }
        state.requestCancellation()

        await XCTAssertThrowsCancellation {
            try await withCheckedThrowingContinuation { continuation in
                guard state.bind(continuation: continuation) else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                XCTFail("A cancelled request must not bind")
            }
        }
        XCTAssertFalse(state.beginWrite())
        XCTAssertTrue(state.callerCanRemoveTemporaryFile)
        XCTAssertFalse(cleanup.wasCalled)
    }
}

private final class CleanupProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var called = false

    var wasCalled: Bool { lock.withLock { called } }

    func markCalled() {
        lock.withLock { called = true }
    }
}

private func XCTAssertThrowsCancellation(
    _ operation: () async throws -> Void,
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        try await operation()
        XCTFail("Expected cancellation", file: file, line: line)
    } catch is CancellationError {
    } catch {
        XCTFail("Unexpected error: \(error)", file: file, line: line)
    }
}
