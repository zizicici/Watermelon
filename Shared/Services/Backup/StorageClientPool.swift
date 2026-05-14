import Foundation

actor StorageClientPool {
    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private var createdConnections = 0
    private var pendingConnectionStarts = 0
    private var idleClients: [any RemoteStorageClientProtocol] = []
    private var waiters: [(UUID, CheckedContinuation<any RemoteStorageClientProtocol, Error>)] = []
    /// Cancel-arrived-before-register IDs; FIFO-evicted so live markers aren't dropped early.
    private var preCancelledWaiterIDs: Set<UUID> = []
    private var preCancelledOrder: [UUID] = []
    private static let preCancelledIDsCap = 4096
    private var retiredWaiterIDs: Set<UUID> = []
    private var retiredWaiterOrder: [UUID] = []
    private static let retiredWaiterIDsCap = 4096
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
        guard createdConnections + pendingConnectionStarts < maxConnections else {
            await client.disconnect()
            return
        }
        createdConnections += 1
        handReusableClientToLiveWaiter(client)
    }

    func acquire() async throws -> any RemoteStorageClientProtocol {
        if isShutdown { throw CancellationError() }
        if let client = idleClients.popLast() {
            return client
        }
        if createdConnections + pendingConnectionStarts < maxConnections {
            createdConnections += 1
            let client: any RemoteStorageClientProtocol
            do {
                client = try makeClient()
                try await client.connect()
            } catch {
                // Hand slot to any waiter queued during our connect-await; otherwise they'd strand.
                await passSlotToLiveWaiter()
                throw error
            }
            // shutdown may have run during connect; surrender rather than hand a live client to a dead pool.
            if isShutdown {
                createdConnections = max(createdConnections - 1, 0)
                await client.disconnect()
                throw CancellationError()
            }
            do {
                try Task.checkCancellation()
            } catch {
                handReusableClientToLiveWaiter(client)
                throw error
            }
            return client
        }
        let id = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<any RemoteStorageClientProtocol, Error>) in
                self.registerWaiter(id: id, continuation: continuation)
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id) }
        }
    }

    private func registerWaiter(id: UUID, continuation: CheckedContinuation<any RemoteStorageClientProtocol, Error>) {
        if isShutdown || consumePreCancelled(id) {
            recordRetiredWaiter(id)
            continuation.resume(throwing: CancellationError())
            return
        }
        // Wake-up loss guard: a release that ran before us already moved a client
        // into idleClients (no waiter visible at release time). Grab it now.
        if let client = idleClients.popLast() {
            recordRetiredWaiter(id)
            continuation.resume(returning: client)
            return
        }
        waiters.append((id, continuation))
        // A non-reusable release between acquire's slot check and our enqueue would never wake us; claim the freed slot.
        startConnectionForWaiterIfPossible()
    }

    private func cancelWaiter(id: UUID) {
        if let idx = waiters.firstIndex(where: { $0.0 == id }) {
            let (_, continuation) = waiters.remove(at: idx)
            recordRetiredWaiter(id)
            continuation.resume(throwing: CancellationError())
        } else if isShutdown || retiredWaiterIDs.contains(id) {
            return
        } else {
            recordPreCancelled(id)
        }
    }

    private func recordRetiredWaiter(_ id: UUID) {
        if retiredWaiterIDs.contains(id) { return }
        if retiredWaiterIDs.count >= Self.retiredWaiterIDsCap {
            if let oldest = retiredWaiterOrder.first {
                retiredWaiterOrder.removeFirst()
                retiredWaiterIDs.remove(oldest)
            }
        }
        retiredWaiterIDs.insert(id)
        retiredWaiterOrder.append(id)
    }

    private func recordPreCancelled(_ id: UUID) {
        if preCancelledWaiterIDs.contains(id) { return }
        if preCancelledWaiterIDs.count >= Self.preCancelledIDsCap {
            if let oldest = preCancelledOrder.first {
                preCancelledOrder.removeFirst()
                preCancelledWaiterIDs.remove(oldest)
            }
        }
        preCancelledWaiterIDs.insert(id)
        preCancelledOrder.append(id)
    }

    private func consumePreCancelled(_ id: UUID) -> Bool {
        guard preCancelledWaiterIDs.remove(id) != nil else { return false }
        if let idx = preCancelledOrder.firstIndex(of: id) {
            preCancelledOrder.remove(at: idx)
        }
        return true
    }

    private func popNextWaiter() -> (UUID, CheckedContinuation<any RemoteStorageClientProtocol, Error>)? {
        guard !waiters.isEmpty else { return nil }
        let entry = waiters.removeFirst()
        return (entry.0, entry.1)
    }

    private func startConnectionForWaiterIfPossible() {
        guard !isShutdown, !waiters.isEmpty, createdConnections + pendingConnectionStarts < maxConnections else { return }
        pendingConnectionStarts += 1
        Task { await self.claimSlotAndPassToLiveWaiter() }
    }

    private func claimSlotAndPassToLiveWaiter() async {
        guard pendingConnectionStarts > 0 else { return }
        pendingConnectionStarts -= 1
        guard !isShutdown, !waiters.isEmpty, createdConnections < maxConnections else { return }
        createdConnections += 1
        await passSlotToLiveWaiter()
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        if isShutdown {
            createdConnections = max(createdConnections - 1, 0)
            await client.disconnect()
            return
        }
        if reusable {
            handReusableClientToLiveWaiter(client)
            return
        }

        // Disconnect first, decrement-only-if-no-waiter; otherwise the slot is "passed
        // directly" to the waiter — preventing a concurrent acquire from seeing 4/5 during
        // our disconnect-await, creating its own client, and us also creating a replacement.
        await client.disconnect()
        await passSlotToLiveWaiter()
    }

    private func handReusableClientToLiveWaiter(_ client: any RemoteStorageClientProtocol) {
        while let (waiterID, waiter) = popNextWaiter() {
            if consumePreCancelled(waiterID) {
                recordRetiredWaiter(waiterID)
                waiter.resume(throwing: CancellationError())
                continue
            }
            recordRetiredWaiter(waiterID)
            waiter.resume(returning: client)
            return
        }
        idleClients.append(client)
    }

    /// Drives a fresh connect for the next live waiter; retries past pre-cancelled or connect-failing waiters so they don't starve.
    private func passSlotToLiveWaiter() async {
        while let (waiterID, waiter) = popNextWaiter() {
            if consumePreCancelled(waiterID) {
                recordRetiredWaiter(waiterID)
                waiter.resume(throwing: CancellationError())
                continue
            }
            if isShutdown {
                recordRetiredWaiter(waiterID)
                waiter.resume(throwing: CancellationError())
                continue
            }
            do {
                let replacement = try makeClient()
                try await replacement.connect()
                // Popped waiter is no longer in `pendingWaiters`; shutdown won't cancel it for us.
                if isShutdown {
                    await replacement.disconnect()
                    recordRetiredWaiter(waiterID)
                    waiter.resume(throwing: CancellationError())
                    return
                }
                if consumePreCancelled(waiterID) {
                    // Cancel arrived during connect; recycle the live replacement for the next waiter.
                    recordRetiredWaiter(waiterID)
                    waiter.resume(throwing: CancellationError())
                    handReusableClientToLiveWaiter(replacement)
                    return
                }
                recordRetiredWaiter(waiterID)
                waiter.resume(returning: replacement)
                return
            } catch {
                if consumePreCancelled(waiterID) {
                    // Caller cancelled mid-connect; surface cancel rather than the connect error.
                    recordRetiredWaiter(waiterID)
                    waiter.resume(throwing: CancellationError())
                    continue
                }
                recordRetiredWaiter(waiterID)
                waiter.resume(throwing: error)
                continue
            }
        }
        createdConnections = max(createdConnections - 1, 0)
    }

    func shutdown() async {
        isShutdown = true
        let clients = idleClients
        idleClients.removeAll()
        let pendingWaiters = waiters
        waiters.removeAll()
        preCancelledWaiterIDs.removeAll()
        preCancelledOrder.removeAll()
        retiredWaiterIDs.removeAll()
        retiredWaiterOrder.removeAll()
        pendingConnectionStarts = 0
        createdConnections = 0

        for (waiterID, waiter) in pendingWaiters {
            recordRetiredWaiter(waiterID)
            waiter.resume(throwing: CancellationError())
        }
        for client in clients {
            await client.disconnect()
        }
    }
}
