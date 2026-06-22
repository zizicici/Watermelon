import Foundation
@testable import Watermelon

// Configurable fake for exercising StorageClientPool's reserved-slot replacement: a connect that succeeds,
// throws, or runs slow (cooperative or not), plus a shared live-session counter so a test can assert the
// pool never holds more live sessions than its cap during recovery.
actor ProbeStorageClient: RemoteStorageClientProtocol {

    // Thread-safe peak/current tracker shared across all clients a pool mints.
    final class LiveCounter: @unchecked Sendable {
        private let lock = NSLock()
        private(set) var current = 0
        private(set) var peak = 0
        func enter() { lock.lock(); current += 1; peak = max(peak, current); lock.unlock() }
        func leave() { lock.lock(); current = max(0, current - 1); lock.unlock() }
    }

    enum ConnectBehavior: Sendable {
        case succeed
        case throwError(Error)
        case delay(TimeInterval, cancellable: Bool)   // cancellable:false simulates an uncooperative backend
    }

    private let behavior: ConnectBehavior
    private let counter: LiveCounter?
    private(set) var connected = false
    private(set) var didDisconnect = false

    init(_ behavior: ConnectBehavior = .succeed, counter: LiveCounter? = nil) {
        self.behavior = behavior
        self.counter = counter
    }

    func connect() async throws {
        switch behavior {
        case .succeed:
            break
        case .throwError(let error):
            throw error
        case .delay(let seconds, let cancellable):
            if cancellable {
                try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            } else {
                await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                    DispatchQueue.global().asyncAfter(deadline: .now() + seconds) { c.resume() }
                }
            }
        }
        connected = true
        counter?.enter()
    }

    func disconnect() async {
        if connected { counter?.leave() }
        connected = false
        didDisconnect = true
    }

    // Unused by the replacement tests.
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { [] }
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

// Collects the clients a pool's makeClient closure produced, so a test can inspect a reaped stray connection.
final class ProbeClientHolder: @unchecked Sendable {
    private let lock = NSLock()
    private(set) var clients: [ProbeStorageClient] = []
    func make(_ behavior: ProbeStorageClient.ConnectBehavior, counter: ProbeStorageClient.LiveCounter? = nil) -> ProbeStorageClient {
        let client = ProbeStorageClient(behavior, counter: counter)
        lock.lock(); clients.append(client); lock.unlock()
        return client
    }
}
