import Foundation

actor StorageClientPool {
    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private var createdConnections = 0
    private var idleClients: [any RemoteStorageClientProtocol] = []
    private var waiters: [(UUID, CheckedContinuation<any RemoteStorageClientProtocol, Error>)] = []
    /// Cancel-arrived-before-register IDs; capped — cancel-after-release orphans never clear.
    private var preCancelledWaiterIDs: Set<UUID> = []
    private static let preCancelledIDsCap = 256
    private var isShutdown = false

    init(
        maxConnections: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) {
        self.maxConnections = max(1, maxConnections)
        self.makeClient = makeClient
    }

    func seedConnectedClient(_ client: any RemoteStorageClientProtocol) async {
        if isShutdown {
            await client.disconnect()
            return
        }
        guard createdConnections < maxConnections else {
            await client.disconnect()
            return
        }
        createdConnections += 1
        if let waiter = popNextWaiter() {
            waiter.resume(returning: client)
        } else {
            idleClients.append(client)
        }
    }

    func acquire() async throws -> any RemoteStorageClientProtocol {
        if isShutdown { throw CancellationError() }
        if let client = idleClients.popLast() {
            return client
        }
        if createdConnections < maxConnections {
            createdConnections += 1
            do {
                let client = try makeClient()
                try await client.connect()
                return client
            } catch {
                createdConnections = max(createdConnections - 1, 0)
                throw error
            }
        }
        let id = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<any RemoteStorageClientProtocol, Error>) in
                Task { await self.registerWaiter(id: id, continuation: continuation) }
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id) }
        }
    }

    private func registerWaiter(id: UUID, continuation: CheckedContinuation<any RemoteStorageClientProtocol, Error>) {
        if isShutdown || preCancelledWaiterIDs.remove(id) != nil {
            continuation.resume(throwing: CancellationError())
            return
        }
        // Wake-up loss guard: a release that ran before us already moved a client
        // into idleClients (no waiter visible at release time). Grab it now.
        if let client = idleClients.popLast() {
            continuation.resume(returning: client)
            return
        }
        waiters.append((id, continuation))
    }

    private func cancelWaiter(id: UUID) {
        if let idx = waiters.firstIndex(where: { $0.0 == id }) {
            let (_, continuation) = waiters.remove(at: idx)
            continuation.resume(throwing: CancellationError())
        } else {
            if preCancelledWaiterIDs.count >= Self.preCancelledIDsCap {
                preCancelledWaiterIDs.removeFirst()
            }
            preCancelledWaiterIDs.insert(id)
        }
    }

    private func popNextWaiter() -> CheckedContinuation<any RemoteStorageClientProtocol, Error>? {
        guard !waiters.isEmpty else { return nil }
        return waiters.removeFirst().1
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        if isShutdown {
            createdConnections = max(createdConnections - 1, 0)
            await client.disconnect()
            return
        }
        if reusable {
            if let waiter = popNextWaiter() {
                waiter.resume(returning: client)
            } else {
                idleClients.append(client)
            }
            return
        }

        // Disconnect first, decrement-only-if-no-waiter; otherwise the slot is "passed
        // directly" to the waiter — preventing a concurrent acquire from seeing 4/5 during
        // our disconnect-await, creating its own client, and us also creating a replacement.
        await client.disconnect()
        if let waiter = popNextWaiter() {
            do {
                let replacement = try makeClient()
                try await replacement.connect()
                waiter.resume(returning: replacement)
            } catch {
                createdConnections = max(createdConnections - 1, 0)
                waiter.resume(throwing: error)
            }
        } else {
            createdConnections = max(createdConnections - 1, 0)
        }
    }

    func shutdown() async {
        isShutdown = true
        let clients = idleClients
        idleClients.removeAll()
        let pendingWaiters = waiters
        waiters.removeAll()
        preCancelledWaiterIDs.removeAll()
        createdConnections = 0

        for (_, waiter) in pendingWaiters {
            waiter.resume(throwing: CancellationError())
        }
        for client in clients {
            await client.disconnect()
        }
    }
}
