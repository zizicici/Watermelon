import XCTest
@testable import Watermelon

final class SMBDownloadCancellationTests: XCTestCase {
    func testCancellationStopsDispatchQueueProgressAndRemovesPartialFile() async throws {
        try await checkCancellation(transportThrows: false)
    }

    func testCancellationNormalizesTransportErrorAndRemovesPartialFile() async throws {
        try await checkCancellation(transportThrows: true)
    }

    private func checkCancellation(transportThrows: Bool) async throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: url) }
        let started = expectation(description: "dispatch transfer started")
        let release = DispatchSemaphore(value: 0)
        let task = Task {
            try await AMSMB2Client.performCancellableDownload(localURL: url, onProgress: { _ in
                XCTFail("cancelled transfer must not publish progress")
            }) { progress in
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    DispatchQueue.global().async {
                        do {
                            try Data("partial".utf8).write(to: url)
                            started.fulfill()
                            XCTAssertEqual(release.wait(timeout: .now() + 5), .success)
                            XCTAssertFalse(Task.isCancelled)
                            XCTAssertFalse(progress(7, 100))
                            if transportThrows { throw URLError(.networkConnectionLost) }
                            continuation.resume()
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                }
            }
        }
        await fulfillment(of: [started], timeout: 3)
        task.cancel()
        release.signal()
        do {
            try await task.value
            XCTFail("expected cancellation")
        } catch is CancellationError {}
        XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    }

    func testSuccessfulDownloadKeepsFile() async throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: url) }
        let contents = Data("complete".utf8)
        try await AMSMB2Client.performCancellableDownload(localURL: url, onProgress: nil) { progress in
            try contents.write(to: url)
            XCTAssertTrue(progress(Int64(contents.count), Int64(contents.count)))
        }
        XCTAssertEqual(try Data(contentsOf: url), contents)
    }
}
