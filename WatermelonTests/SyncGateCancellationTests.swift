import XCTest
@testable import Watermelon

// Regression for the SyncGate cancellation fix: a sync queued behind a holder that still owns the gate must
// observe cancellation while parked (so a user stop/pause settles promptly), and normal FIFO hand-off after
// release must still work.
final class SyncGateCancellationTests: XCTestCase {

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil, name: "p", storageType: StorageType.s3.rawValue, connectionParams: nil, sortOrder: 0,
            host: "host.local", port: 0, shareName: "share", basePath: "/p", username: "u",
            domain: nil, credentialRef: "ref", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil
        )
    }

    private func pollUntil(timeout: TimeInterval, _ condition: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() { return }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        XCTFail("condition not met within \(timeout)s")
    }

    func testQueuedSyncIsCancellableWhileHolderHoldsGate() async throws {
        let service = RemoteIndexSyncService()
        let holder = GateHoldingClient(shouldBlock: true)

        // Task 1 acquires the gate and blocks inside `list` (scanLiteManifestDigests).
        let holderTask = Task { try? await service.syncIndex(client: holder, profile: profile(), layout: .lite) }
        try await pollUntil(timeout: 5) { holder.listEntered }

        // Task 2 queues behind the gate.
        let queued = Task { () -> Bool in
            do {
                _ = try await service.syncIndex(client: GateHoldingClient(shouldBlock: false), profile: profile(), layout: .lite)
                return false
            } catch is CancellationError {
                return true
            } catch {
                return false
            }
        }
        try await Task.sleep(nanoseconds: 300_000_000)   // let it reach the gate and park

        let start = Date()
        queued.cancel()
        let observedCancellation = await queued.value
        let elapsed = Date().timeIntervalSince(start)

        XCTAssertTrue(observedCancellation, "a queued sync must throw CancellationError when cancelled while parked")
        XCTAssertLessThan(elapsed, 2.0, "cancellation must be observed promptly, not only after the holder releases")

        holder.unblock()
        _ = await holderTask.value
    }

    func testQueuedSyncProceedsAfterHolderReleases() async throws {
        let service = RemoteIndexSyncService()
        let holder = GateHoldingClient(shouldBlock: true)
        let holderTask = Task { try? await service.syncIndex(client: holder, profile: profile(), layout: .lite) }
        try await pollUntil(timeout: 5) { holder.listEntered }

        let second = GateHoldingClient(shouldBlock: false)
        let queued = Task { () -> Bool in
            (try? await service.syncIndex(client: second, profile: profile(), layout: .lite)) != nil
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        XCTAssertFalse(second.listEntered, "queued sync must not run while the holder owns the gate")

        holder.unblock()   // release → the parked waiter should be handed the gate and complete
        let completed = await queued.value
        XCTAssertTrue(completed, "queued sync must proceed and complete after the holder releases the gate")
        _ = await holderTask.value
    }
}

// Minimal RemoteStorageClientProtocol fake whose `list` either blocks until `unblock()` (to hold the gate) or
// returns immediately. Mirrors the method set of the existing ProbeStorageClient support fake.
private final class GateHoldingClient: RemoteStorageClientProtocol, @unchecked Sendable {
    private let lock = NSLock()
    private let shouldBlock: Bool
    private var entered = false
    private var released = false
    private var blockContinuation: CheckedContinuation<Void, Never>?

    init(shouldBlock: Bool) { self.shouldBlock = shouldBlock }

    var listEntered: Bool { lock.lock(); defer { lock.unlock() }; return entered }

    func unblock() {
        lock.lock()
        released = true
        let continuation = blockContinuation
        blockContinuation = nil
        lock.unlock()
        continuation?.resume()
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        lock.lock(); entered = true; let block = shouldBlock; let alreadyReleased = released; lock.unlock()
        guard block, !alreadyReleased else { return [] }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            lock.lock()
            if released {
                lock.unlock()
                continuation.resume()
            } else {
                blockContinuation = continuation
                lock.unlock()
            }
        }
        return []
    }

    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws {}
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}
