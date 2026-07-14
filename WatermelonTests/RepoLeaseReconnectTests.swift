import XCTest
@testable import Watermelon

private actor GatedReconnectProvider {
    private let handle: LiteLockClientHandle
    private var continuation: CheckedContinuation<Void, Never>?
    private var isOpen = false
    private(set) var callCount = 0

    init(handle: LiteLockClientHandle) {
        self.handle = handle
    }

    func make() async -> LiteLockClientHandle {
        callCount += 1
        if !isOpen {
            await withCheckedContinuation { continuation = $0 }
        }
        return handle
    }

    func waitUntilCalled() async {
        while callCount == 0 {
            await Task.yield()
        }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }
}

private actor ReleaseCompletionRecorder {
    private(set) var count = 0

    func record() {
        count += 1
    }
}

private actor CancellationAwareReconnectProvider {
    private let handle: LiteLockClientHandle
    private(set) var callCount = 0
    private(set) var cancellationCount = 0

    init(handle: LiteLockClientHandle) {
        self.handle = handle
    }

    func make() async throws -> LiteLockClientHandle {
        callCount += 1
        do {
            try await Task.sleep(for: .milliseconds(200))
            return handle
        } catch {
            cancellationCount += 1
            throw error
        }
    }

    func waitUntilCalled() async {
        while callCount == 0 {
            await Task.yield()
        }
    }
}

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

    func testReconnectGateWaitsForRetiredClientDisconnect() async throws {
        let writerID = newWriterID()
        let original = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        await original.seedDirectory(locksDirectory)
        await original.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: original)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let lockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        let storedLockBytes = await original.fileData(path: lockPath)
        let lockBytes = try XCTUnwrap(storedLockBytes)
        await replacement.seedDirectory(locksDirectory)
        await replacement.seedFile(path: lockPath, data: lockBytes, modificationDate: base)
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: original),
            reconnectLockClient: { LiteLockClientHandle(client: replacement) }
        )
        await original.enqueueListError(RemoteErrorFixtures.retryable)
        await original.blockDisconnectUntilOpened()
        let completions = ReleaseCompletionRecorder()

        let gate = Task {
            try await session.assertLeaseProvenForWrite(now: base)
            await completions.record()
        }
        await original.waitUntilDisconnectEntered()
        await Task.yield()
        let completionsBeforeOpen = await completions.count
        XCTAssertEqual(completionsBeforeOpen, 0)

        await original.openDisconnect()
        try await gate.value
        let completionCount = await completions.count
        XCTAssertEqual(completionCount, 1)
        await session.stopAndRelease()
    }

    func testConcurrentOwnershipGatesShareOneReconnect() async throws {
        let writerID = newWriterID()
        let original = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        await original.seedDirectory(locksDirectory)
        await original.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: original)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let lockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        let storedLockBytes = await original.fileData(path: lockPath)
        let lockBytes = try XCTUnwrap(storedLockBytes)
        await replacement.seedDirectory(locksDirectory)
        await replacement.seedFile(path: lockPath, data: lockBytes, modificationDate: base)

        let provider = GatedReconnectProvider(handle: LiteLockClientHandle(client: replacement))
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: original),
            reconnectLockClient: { await provider.make() }
        )
        await original.enqueueListError(RemoteErrorFixtures.retryable)
        await original.enqueueListError(RemoteErrorFixtures.retryable)

        let first = Task { try await session.assertLeaseProvenForWrite(now: base) }
        let second = Task { try await session.assertLeaseProvenForWrite(now: base) }
        await provider.waitUntilCalled()
        await provider.open()
        try await first.value
        try await second.value

        let providerCalls = await provider.callCount
        let originalDisconnects = await original.disconnectCount
        let replacementDisconnectsBeforeRelease = await replacement.disconnectCount
        XCTAssertEqual(providerCalls, 1)
        XCTAssertEqual(originalDisconnects, 1)
        XCTAssertEqual(replacementDisconnectsBeforeRelease, 0)
        await session.stopAndRelease()
        let replacementDisconnectsAfterRelease = await replacement.disconnectCount
        XCTAssertEqual(replacementDisconnectsAfterRelease, 1)
    }

    func testReleaseWaitsForInFlightReconnectAndDisconnectsEveryOwnedClientOnce() async throws {
        let writerID = newWriterID()
        let original = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        await original.seedDirectory(locksDirectory)
        await original.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: original)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let lockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        let storedLockBytes = await original.fileData(path: lockPath)
        let lockBytes = try XCTUnwrap(storedLockBytes)
        await replacement.seedDirectory(locksDirectory)
        await replacement.seedFile(path: lockPath, data: lockBytes, modificationDate: base)

        let provider = GatedReconnectProvider(handle: LiteLockClientHandle(client: replacement))
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: original),
            reconnectLockClient: { await provider.make() }
        )
        await original.enqueueListError(RemoteErrorFixtures.retryable)
        await original.enqueueDeleteError(RemoteErrorFixtures.retryable)

        let gate = Task { try await session.assertLeaseProvenForWrite(now: base) }
        await provider.waitUntilCalled()
        let release = Task { await session.stopAndRelease() }
        await Task.yield()
        await provider.open()
        _ = try? await gate.value
        await release.value

        let providerCalls = await provider.callCount
        let originalDisconnects = await original.disconnectCount
        let replacementDisconnects = await replacement.disconnectCount
        let replacementLockExists = await replacement.lockExists(basePath: basePath, writerID: writerID)
        XCTAssertEqual(providerCalls, 1)
        XCTAssertEqual(originalDisconnects, 1)
        XCTAssertEqual(replacementDisconnects, 1)
        XCTAssertFalse(replacementLockExists)
    }

    func testConcurrentReleaseCallersWaitForTheSameCleanup() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: client)
        )
        await client.blockDisconnectUntilOpened()
        let completions = ReleaseCompletionRecorder()

        let first = Task {
            await session.stopAndRelease()
            await completions.record()
        }
        await client.waitUntilDisconnectEntered()
        let second = Task {
            await session.stopAndRelease()
            await completions.record()
        }
        await Task.yield()
        let completionsBeforeOpen = await completions.count
        XCTAssertEqual(completionsBeforeOpen, 0)

        await client.openDisconnect()
        await first.value
        await second.value
        let completionCount = await completions.count
        let disconnectCount = await client.disconnectCount
        XCTAssertEqual(completionCount, 2)
        XCTAssertEqual(disconnectCount, 1)
    }

    func testCancelledQueuedOwnershipGateDoesNotTouchReplacementClient() async throws {
        let writerID = newWriterID()
        let original = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        await original.seedDirectory(locksDirectory)
        await original.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: original)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let lockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        let storedLockBytes = await original.fileData(path: lockPath)
        let lockBytes = try XCTUnwrap(storedLockBytes)
        await replacement.seedDirectory(locksDirectory)
        await replacement.seedFile(path: lockPath, data: lockBytes, modificationDate: base)
        let provider = GatedReconnectProvider(handle: LiteLockClientHandle(client: replacement))
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: original),
            reconnectLockClient: { await provider.make() }
        )
        await original.enqueueListError(RemoteErrorFixtures.retryable)

        let recoveringGate = Task { try await session.assertLeaseProvenForWrite(now: base) }
        await provider.waitUntilCalled()
        let cancelledGate = Task { try await session.assertLeaseProvenForWrite(now: base) }
        cancelledGate.cancel()
        await provider.open()
        try await recoveringGate.value
        do {
            try await cancelledGate.value
            XCTFail("a cancelled queued gate must surface cancellation")
        } catch is CancellationError {
        }

        let providerCalls = await provider.callCount
        let replacementLists = await replacement.listedPaths
        XCTAssertEqual(providerCalls, 1)
        XCTAssertEqual(replacementLists, [locksDirectory])
        await session.stopAndRelease()
    }

    func testSecondOwnershipGateCannotEnterClientWhileFirstGateIsBlocked() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: client)
        )
        let baselineListCount = await client.listedPaths.count
        await client.blockNextListUntilOpened()

        let first = Task { try await session.assertLeaseProvenForWrite(now: base) }
        await client.waitUntilBlockedListEntered()
        let second = Task { try await session.assertLeaseProvenForWrite(now: base) }
        for _ in 0..<50 {
            await Task.yield()
        }
        let listsWhileBlocked = await client.listedPaths
        XCTAssertEqual(listsWhileBlocked.count, baselineListCount + 1)

        await client.openBlockedList()
        try await first.value
        try await second.value
        let finalLists = await client.listedPaths
        XCTAssertEqual(finalLists.count, baselineListCount + 2)
        await session.stopAndRelease()
    }

    func testCancellingActiveReconnectPropagatesToProvider() async throws {
        let writerID = newWriterID()
        let original = InMemoryRemoteStorageClient()
        let replacement = InMemoryRemoteStorageClient()
        await original.seedDirectory(locksDirectory)
        await original.setPendingUploadModificationDate(base)
        let service = makeService(writerID: writerID, client: original)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)
        let provider = CancellationAwareReconnectProvider(
            handle: LiteLockClientHandle(client: replacement)
        )
        let session = RepoLeaseSession(
            lock: service,
            lockClientHandle: LiteLockClientHandle(client: original),
            reconnectLockClient: { try await provider.make() }
        )
        await original.enqueueListError(RemoteErrorFixtures.retryable)

        let gate = Task { try await session.assertLeaseProvenForWrite(now: base) }
        await provider.waitUntilCalled()
        gate.cancel()
        do {
            try await gate.value
            XCTFail("cancelling a recovery gate must surface cancellation")
        } catch is CancellationError {
        }

        let providerCalls = await provider.callCount
        let providerCancellations = await provider.cancellationCount
        XCTAssertEqual(providerCalls, 1)
        XCTAssertEqual(providerCancellations, 1)
        await session.stopAndRelease()
    }
}
