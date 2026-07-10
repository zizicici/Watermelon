import XCTest
@testable import Watermelon

// Verifies RemoteThumbnailService.recursiveDelete surfaces observed list/delete failures instead of
// swallowing them — the R03 fix so purgeRemoteThumbnails can't report success while remote sidecars remain.
final class RemoteThumbnailPurgeTests: XCTestCase {
    private let root = "/base/.watermelon/thumbs"
    private let shard = "/base/.watermelon/thumbs/de"
    private let sidecar = "/base/.watermelon/thumbs/de/deadbeef.jpg"
    private struct OwnershipDenied: Error {}

    private func seedTree(_ client: InMemoryRemoteStorageClient) async {
        await client.seedDirectory(root)
        await client.seedDirectory(shard)
        await client.seedFile(path: sidecar, data: Data([0x1, 0x2, 0x3]))
    }

    func testRecursiveDeleteReturnsZeroWhenEverythingDeletes() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedTree(client)
        let failures = try await RemoteThumbnailService.recursiveDelete(path: root, client: client)
        XCTAssertEqual(failures, 0)
    }

    func testRecursiveDeleteDoesNotDeleteWhenOwnershipAssertionFails() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedTree(client)

        do {
            _ = try await RemoteThumbnailService.recursiveDelete(
                path: root,
                client: client,
                assertOwnership: { throw OwnershipDenied() }
            )
            XCTFail("thumbnail purge must fail closed when the write lease cannot be proven")
        } catch is OwnershipDenied {
            // expected
        }

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
        let sidecarData = await client.fileData(path: sidecar)
        XCTAssertEqual(sidecarData, Data([0x1, 0x2, 0x3]))
    }

    func testRecursiveDeleteCountsSwallowedDeleteFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedTree(client)
        // The first delete (the sidecar file) throws a terminal transport fault; previously this was
        // swallowed by `try?` and the purge still reported success.
        await client.enqueueDeleteError(RemoteErrorFixtures.terminal)
        let failures = try await RemoteThumbnailService.recursiveDelete(path: root, client: client)
        XCTAssertGreaterThan(failures, 0)
    }

    func testRecursiveDeleteCountsListFailureAsFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedTree(client)
        // Enumerating the shard fails with a non-not-found fault → can't delete its contents → a failure.
        await client.enqueueListResult([RemoteStorageEntry(path: shard, name: "de", isDirectory: true, size: 0, creationDate: nil, modificationDate: nil)])
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        let failures = try await RemoteThumbnailService.recursiveDelete(path: root, client: client)
        XCTAssertGreaterThan(failures, 0)
    }

    func testRecursiveDeleteTreatsMissingRootAsSuccess() async throws {
        // Node never used the feature: listing the absent root throws not-found, which is nothing to delete.
        let client = InMemoryRemoteStorageClient()
        let failures = try await RemoteThumbnailService.recursiveDelete(path: root, client: client)
        XCTAssertEqual(failures, 0)
    }

    func testRecursiveDeletePropagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        await seedTree(client)
        await client.enqueueDeleteError(RemoteErrorFixtures.cancelled)
        do {
            _ = try await RemoteThumbnailService.recursiveDelete(path: root, client: client)
            XCTFail("expected cancellation to propagate")
        } catch is CancellationError {
            // expected
        }
    }
}
