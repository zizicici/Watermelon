import XCTest
@testable import Watermelon

// A transfer whose socket accepts but never makes progress must be bounded by the watchdog (not the session's
// multi-day transfer timeout). HangingURLProtocol intercepts the request and never responds, so the only way
// runUpload/runDownload/runData return is the watchdog cancelling the task and surfacing the stall error.
final class URLSessionStallWatchdogTests: XCTestCase {

    private let fastTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: 0.5, uploadResponseStall: 0.5,
        downloadFirstByte: 0.5, downloadStall: 0.5, pollInterval: 0.1
    )

    private func makeSession(delegate: URLSessionStallWatchdog.Delegate?) -> URLSession {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [HangingURLProtocol.self]
        return URLSession(configuration: config, delegate: delegate, delegateQueue: nil)
    }

    private func request(_ method: String) -> URLRequest {
        var request = URLRequest(url: URL(string: "https://stall.invalid/object")!)
        request.httpMethod = method
        return request
    }

    func testUploadStallSurfacesStallError() async throws {
        let delegate = URLSessionStallWatchdog.Delegate()
        let session = makeSession(delegate: delegate)
        defer { session.invalidateAndCancel() }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent("wd-\(UUID().uuidString).bin")
        try Data(repeating: 7, count: 4096).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        do {
            _ = try await URLSessionStallWatchdog.runUpload(
                session: session, delegate: delegate, request: request("PUT"),
                body: .file(fileURL), onProgress: nil, timeouts: fastTimeouts,
                makeStallError: { _, _, _, _ in NSError(domain: "Test", code: 42) }
            )
            XCTFail("a stalled upload must surface the stall error")
        } catch {
            XCTAssertEqual((error as NSError).code, 42, "expected the watchdog's stall error, got \(error)")
        }
    }

    func testDownloadStallSurfacesStallError() async {
        let session = makeSession(delegate: nil)
        defer { session.invalidateAndCancel() }
        do {
            _ = try await URLSessionStallWatchdog.runDownload(
                session: session, request: request("GET"), timeouts: fastTimeouts,
                makeStallError: { _, _, _, _ in NSError(domain: "Test", code: 43) }
            )
            XCTFail("a stalled download must surface the stall error")
        } catch {
            XCTAssertEqual((error as NSError).code, 43, "expected the watchdog's stall error, got \(error)")
        }
    }

    func testDataStallSurfacesStallError() async {
        let session = makeSession(delegate: nil)
        defer { session.invalidateAndCancel() }
        do {
            _ = try await URLSessionStallWatchdog.runData(
                session: session, request: request("PUT"), timeouts: fastTimeouts,
                makeStallError: { _, _, _, _ in NSError(domain: "Test", code: 44) }
            )
            XCTFail("a stalled response-only transfer must surface the stall error")
        } catch {
            XCTAssertEqual((error as NSError).code, 44, "expected the watchdog's stall error, got \(error)")
        }
    }

    func testUploadProgressDoesNotEmitDecreasingFractions() {
        var emitted: [Double] = []
        let progress = URLSessionStallWatchdog.UploadProgress { emitted.append($0) }

        progress.recordProgress(bytesSent: 800, totalBytesExpectedToSend: 1_000)
        progress.recordProgress(bytesSent: 700, totalBytesExpectedToSend: 1_000)

        XCTAssertEqual(emitted.count, 2)
        XCTAssertEqual(emitted[0], 0.8, accuracy: 0.0001)
        XCTAssertEqual(emitted[1], 0.8, accuracy: 0.0001)
    }

    func testDownloadProgressDoesNotEmitDecreasingFractions() {
        var emitted: [Double] = []
        let progress = URLSessionStallWatchdog.DownloadProgress { emitted.append($0) }

        progress.recordProgress(bytesWritten: 800, totalBytesExpectedToWrite: 1_000)
        progress.recordProgress(bytesWritten: 700, totalBytesExpectedToWrite: 1_000)

        XCTAssertEqual(emitted.count, 2)
        XCTAssertEqual(emitted[0], 0.8, accuracy: 0.0001)
        XCTAssertEqual(emitted[1], 0.8, accuracy: 0.0001)
    }
}

private final class HangingURLProtocol: URLProtocol {
    override class func canInit(with _: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {}   // never respond — the watchdog must break the stall
    override func stopLoading() {}
}
