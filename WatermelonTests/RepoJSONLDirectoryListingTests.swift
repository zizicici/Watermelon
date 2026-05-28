import Foundation
import XCTest
@testable import Watermelon

final class RepoJSONLDirectoryListingTests: XCTestCase {
    private let dir = "/dir"

    func testListFilenames_filtersJSONLFilesOnly() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectFile(path: "\(dir)/a.jsonl", contents: "a")
        await client.injectFile(path: "\(dir)/b.jsonl", contents: "b")
        await client.injectFile(path: "\(dir)/skip.json", contents: "x")
        await client.injectFile(path: "\(dir)/notes.txt", contents: "y")
        await client.injectFile(path: "\(dir)/sub/inner.jsonl", contents: "z")

        let result = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)

        XCTAssertEqual(result.sorted(), ["a.jsonl", "b.jsonl"])
    }

    func testListFilenames_returnsEmptyWhenDirectoryAbsentViaMetadataNil() async throws {
        let client = InMemoryRemoteStorageClient()

        let result = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)

        XCTAssertEqual(result, [])
    }

    func testListFilenames_rethrowsListErrorWhenMetadataErrorsWithDistinguishableShape() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectFile(path: "\(dir)/anchor.jsonl", contents: "x")
        await client.injectListError(.transport, for: dir)
        await client.injectMetadataError(.permission, for: dir)

        do {
            _ = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)
            XCTFail("expected helper to throw")
        } catch {
            let nsError = Self.unwrapToNSError(error)
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, NSURLErrorNotConnectedToInternet)
            XCTAssertNotEqual(nsError.domain, NSCocoaErrorDomain)
            XCTAssertNotEqual(nsError.code, NSFileReadNoPermissionError)
        }
    }

    func testListFilenames_translatesOuterListCancellationToCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectListWrappedURLCancellation(for: dir)

        do {
            _ = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)
            XCTFail("expected helper to throw CancellationError")
        } catch {
            XCTAssertTrue(error is CancellationError, "expected CancellationError, got \(error)")
        }
    }

    func testListFilenames_translatesMetadataCancellationToCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectListError(.notFound, for: dir)
        await client.injectRawMetadataError(
            RemoteStorageClientError.underlying(NSError(
                domain: NSURLErrorDomain,
                code: NSURLErrorCancelled
            )),
            for: dir
        )

        do {
            _ = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)
            XCTFail("expected helper to throw CancellationError")
        } catch {
            XCTAssertTrue(error is CancellationError, "expected CancellationError, got \(error)")
        }
    }

    func testListFilenames_rethrowsListErrorWhenDirectoryExistsAndListFails() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectFile(path: "\(dir)/sentinel.jsonl", contents: "x")
        await client.injectListError(.transport, for: dir)

        do {
            _ = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)
            XCTFail("expected helper to throw")
        } catch is CancellationError {
            XCTFail("must not surface as CancellationError")
        } catch {
            let nsError = Self.unwrapToNSError(error)
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, NSURLErrorNotConnectedToInternet)
        }
    }

    func testListCommitFilenames_returnsCommitFilenamesAfterUploads() async throws {
        let client = InMemoryRemoteStorageClient()
        let basePath = "/repo"
        let commitsDir = "\(basePath)/.watermelon/commits"
        await client.injectFile(path: "\(commitsDir)/0000.jsonl", contents: "x")
        await client.injectFile(path: "\(commitsDir)/0001.jsonl", contents: "y")
        await client.injectFile(path: "\(commitsDir)/skip.txt", contents: "z")
        let reader = CommitLogReader(client: client, basePath: basePath)

        let result = try await reader.listCommitFilenames()

        XCTAssertEqual(result.sorted(), ["0000.jsonl", "0001.jsonl"])
    }

    func testListSnapshotFilenames_returnsSnapshotFilenamesAfterUploads() async throws {
        let client = InMemoryRemoteStorageClient()
        let basePath = "/repo"
        let snapshotsDir = "\(basePath)/.watermelon/snapshots"
        await client.injectFile(path: "\(snapshotsDir)/0000.jsonl", contents: "x")
        await client.injectFile(path: "\(snapshotsDir)/0001.jsonl", contents: "y")
        await client.injectFile(path: "\(snapshotsDir)/skip.txt", contents: "z")
        let reader = SnapshotReader(client: client, basePath: basePath)

        let result = try await reader.listSnapshotFilenames()

        XCTAssertEqual(result.sorted(), ["0000.jsonl", "0001.jsonl"])
    }

    // Bug-IX P04 R02 ClaudeReviewerC F1: non-not-found list errors must not be
    // silently converted to [] via metadata fallback (e.g. S3 truncated listing
    // throws, S3 HEAD on prefix returns nil → was returning []).
    func testListFilenames_throwsNonNotFoundErrorEvenWhenMetadataReturnsNil() async throws {
        let client = InMemoryRemoteStorageClient()
        // Inject a non-not-found list error (transport) with no files (metadata returns nil).
        await client.injectListError(.transport, for: dir)
        // metadata for absent dir returns nil — before fix this caused return [].

        do {
            _ = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)
            XCTFail("non-not-found list error must propagate, not return []")
        } catch is CancellationError {
            XCTFail("must not surface as CancellationError")
        } catch {
            let nsError = Self.unwrapToNSError(error)
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, NSURLErrorNotConnectedToInternet)
        }
    }

    // Not-found list errors with nil metadata should still return [] (directory confirmed absent).
    func testListFilenames_notFoundWithNilMetadata_returnsEmpty() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.injectListError(.notFound, for: dir)

        let result = try await RepoJSONLDirectoryListing.listFilenames(client: client, directory: dir)

        XCTAssertEqual(result, [])
    }

    private static func unwrapToNSError(_ error: Error) -> NSError {
        if case RemoteStorageClientError.underlying(let underlying) = error {
            return underlying as NSError
        }
        return error as NSError
    }
}
