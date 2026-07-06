import Foundation

// The browser's dedicated connection pool: thumbnail/original reads manage their own capped set of
// long-lived sessions, independent of the backup/sync transfer pools (StorageClientPool), whose
// bounded-abandon connect semantics are tuned for workers and break down under the browser's
// high-frequency cancellation (scrolled-away cells):
//   · A connect, once started, is never cancelled or abandoned. A caller that stops waiting leaves the
//     connect to finish in the background and park its session for the next caller. Abandoned connects
//     can't actually be stopped (SMB ignores Swift cancellation), so re-dialing per cell piles up
//     sessions until the server wedges.
//   · Sessions stay open across requests. Only a genuinely dead session (connection-unavailable) is
//     dropped, and its slot redials lazily on demand.
actor MediaBrowserConnectionPool {
    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol

    private var idleClients: [any RemoteStorageClientProtocol] = []
    // Sessions the cap counts: connected (idle or checked out) plus connecting.
    private var liveCount = 0
    private var isShutdown = false
    private var waiters: [Waiter] = []

    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<(any RemoteStorageClientProtocol)?, Never>
    }

    init(
        maxConnections: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) {
        self.maxConnections = max(1, maxConnections)
        self.makeClient = makeClient
    }

    // Returns nil when the pool is shut down, the caller is cancelled while waiting, or the pending
    // connect fails. Waiting is cancellable; the connect itself never is.
    func acquire() async -> (any RemoteStorageClientProtocol)? {
        guard !isShutdown else { return nil }
        if let client = idleClients.popLast() { return client }
        if liveCount < maxConnections { startConnect() }
        return await park()
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        guard !isShutdown else {
            liveCount = max(liveCount - 1, 0)
            await client.disconnect()
            return
        }
        guard reusable else {
            // Dead session: drop it, and redial immediately only if someone is actually waiting —
            // otherwise the slot reconnects lazily on the next acquire.
            liveCount = max(liveCount - 1, 0)
            await client.disconnect()
            if !waiters.isEmpty, liveCount < maxConnections { startConnect() }
            return
        }
        handOver(client)
    }

    func shutdown() async {
        isShutdown = true
        let parked = waiters
        waiters = []
        for waiter in parked { waiter.continuation.resume(returning: nil) }
        let clients = idleClients
        idleClients = []
        liveCount = max(liveCount - clients.count, 0)
        for client in clients { await client.disconnect() }
        // In-flight connects land in connectFinished; checked-out clients in release — both disconnect.
    }

    // MARK: - Shielded connect

    private func startConnect() {
        liveCount += 1
        Task.detached { [makeClient] in
            let client: (any RemoteStorageClientProtocol)?
            do {
                let made = try makeClient()
                try await made.connect()
                client = made
            } catch {
                client = nil
            }
            await self.connectFinished(client)
        }
    }

    private func connectFinished(_ client: (any RemoteStorageClientProtocol)?) async {
        guard let client else {
            liveCount = max(liveCount - 1, 0)
            // The failed connect resolves one waiter (its would-have-been session); any others keep
            // waiting on remaining in-flight connects or releases.
            if !waiters.isEmpty { waiters.removeFirst().continuation.resume(returning: nil) }
            return
        }
        guard !isShutdown else {
            liveCount = max(liveCount - 1, 0)
            await client.disconnect()
            return
        }
        handOver(client)
    }

    private func handOver(_ client: any RemoteStorageClientProtocol) {
        if waiters.isEmpty {
            idleClients.append(client)
        } else {
            waiters.removeFirst().continuation.resume(returning: client)
        }
    }

    // MARK: - Cancellable waiting

    private func park() async -> (any RemoteStorageClientProtocol)? {
        let id = UUID()
        return await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<(any RemoteStorageClientProtocol)?, Never>) in
                // Re-check under actor isolation: onCancel may have fired before this body parked the waiter.
                if Task.isCancelled || isShutdown {
                    continuation.resume(returning: nil)
                } else {
                    waiters.append(Waiter(id: id, continuation: continuation))
                }
            }
        } onCancel: {
            Task { await self.cancelWaiter(id: id) }
        }
    }

    private func cancelWaiter(id: UUID) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        let waiter = waiters.remove(at: index)
        waiter.continuation.resume(returning: nil)
    }
}
