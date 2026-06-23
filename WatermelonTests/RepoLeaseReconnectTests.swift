import XCTest
@testable import Watermelon

final class RepoLeaseReconnectTests: XCTestCase {
    private let basePath = "/photos"
    private let base = Date(timeIntervalSince1970: 1_700_000_000)
    private func newWriterID() -> String { UUID().uuidString.lowercased() }
    private var locksDirectory: String { RepoLayoutLite.locksDirectoryPath(basePath: basePath) }

    private func makeService(writerID: String, client: InMemoryRemoteStorageClient) -> WriteLockService {
        guard let service = WriteLockService(basePath: basePath, writerID: writerID, client: client) else {
            preconditionFailure("canonical writer ID must build a service")
        }
        return service
    }

    // A lock client replaced after a retryable fault must be disconnected before stopAndRelease.
    func testReconnectDisconnectsRetiredLockClientPromptly() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let retiredProbe = ProbeStorageClient()
        let session = RepoLeaseSession(
            lock: service,
            ownedLockClient: retiredProbe,
            reconnectLockClient: { LiteLockClientHandle(client: client) }
        )

        // Retryable ownership fault triggers the reconnect; the retry outcome is not what we assert.
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        _ = try? await session.assertLeaseProvenForWrite(now: base)

        var disconnected = false
        for _ in 0 ..< 200 {
            if await retiredProbe.didDisconnect { disconnected = true; break }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTAssertTrue(disconnected, "retired lock client must be disconnected promptly on reconnect")

        await session.stopAndRelease()
    }
}
