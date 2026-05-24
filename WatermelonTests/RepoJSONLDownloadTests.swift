import Foundation
import XCTest
@testable import Watermelon

final class RepoJSONLDownloadTests: XCTestCase {
    private let remotePath = "/repo/.watermelon/commits/0001.jsonl"

    private func tempJSONL(prefix: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString).jsonl")
    }

    func testDownload_succeedsAndWritesLocalFile() async throws {
        let client = InMemoryRemoteStorageClient()
        let payload = "header\nbody\nend\n"
        await client.injectFile(path: remotePath, contents: payload)
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        try await RepoJSONLDownload.download(
            client: client,
            remotePath: remotePath,
            to: temp,
            notFoundError: CommitLogReader.ReadError.notFound(filename: "should-not-fire.jsonl")
        )

        let bytes = try Data(contentsOf: temp)
        XCTAssertEqual(String(data: bytes, encoding: .utf8), payload)
    }

    func testDownload_translatesRawURLCancelledToCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectRawDownloadError(
            NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled),
            for: remotePath
        )
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: CommitLogReader.ReadError.notFound(filename: "abc.jsonl")
            )
            XCTFail("expected helper to throw CancellationError")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testDownload_translatesWrappedURLCancelledToCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectDownloadWrappedURLCancellation(for: remotePath)
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: CommitLogReader.ReadError.notFound(filename: "abc.jsonl")
            )
            XCTFail("expected helper to throw CancellationError")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testDownload_translatesNotFoundToCallerSuppliedError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectRawDownloadError(
            NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError),
            for: remotePath
        )
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: CommitLogReader.ReadError.notFound(filename: "abc.jsonl")
            )
            XCTFail("expected helper to throw CommitLogReader.ReadError.notFound")
        } catch let CommitLogReader.ReadError.notFound(filename) {
            XCTAssertEqual(filename, "abc.jsonl")
        } catch {
            XCTFail("expected CommitLogReader.ReadError.notFound, got \(error)")
        }
    }

    func testDownload_translatesSnapshotNotFoundToSnapshotReaderTypedError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectRawDownloadError(
            NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError),
            for: remotePath
        )
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: SnapshotReader.ReadError.notFound(filename: "xyz.jsonl")
            )
            XCTFail("expected helper to throw SnapshotReader.ReadError.notFound")
        } catch let SnapshotReader.ReadError.notFound(filename) {
            XCTAssertEqual(filename, "xyz.jsonl")
        } catch {
            XCTFail("expected SnapshotReader.ReadError.notFound, got \(error)")
        }
    }

    func testDownload_rethrowsTransportErrorRaw() async throws {
        let client = InMemoryRemoteStorageClient()
        let injected = NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
        await client.injectRawDownloadError(injected, for: remotePath)
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: CommitLogReader.ReadError.notFound(filename: "abc.jsonl")
            )
            XCTFail("expected helper to throw")
        } catch is CancellationError {
            XCTFail("must not surface a transport error as CancellationError")
        } catch let CommitLogReader.ReadError.notFound {
            XCTFail("must not surface a transport error as notFound")
        } catch {
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, NSURLErrorNotConnectedToInternet)
        }
    }

    func testDownload_cancellationDominatesNotFoundChain() async throws {
        let client = InMemoryRemoteStorageClient()
        let underlying = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoSuchFileError)
        let cancelWrappingNotFound = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorCancelled,
            userInfo: [NSUnderlyingErrorKey: underlying]
        )
        await client.injectRawDownloadError(cancelWrappingNotFound, for: remotePath)
        let temp = tempJSONL(prefix: "test")
        defer { try? FileManager.default.removeItem(at: temp) }

        do {
            try await RepoJSONLDownload.download(
                client: client,
                remotePath: remotePath,
                to: temp,
                notFoundError: CommitLogReader.ReadError.notFound(filename: "abc.jsonl")
            )
            XCTFail("expected helper to throw CancellationError")
        } catch is CancellationError {
        } catch let CommitLogReader.ReadError.notFound {
            XCTFail("notFound branch must not fire when the outer error is a cancellation")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }
}
