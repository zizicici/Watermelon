import Foundation

actor StorageClientPool {
    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private var createdConnections = 0
    private var idleClients: [any RemoteStorageClientProtocol] = []
    private var waiters: [CheckedContinuation<any RemoteStorageClientProtocol, Error>] = []

    init(
        maxConnections: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) {
        self.maxConnections = max(1, maxConnections)
        self.makeClient = makeClient
    }

    func seedConnectedClient(_ client: any RemoteStorageClientProtocol) {
        guard createdConnections < maxConnections else { return }
        createdConnections += 1
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.resume(returning: client)
        } else {
            idleClients.append(client)
        }
    }

    func acquire() async throws -> any RemoteStorageClientProtocol {
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
        return try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        if reusable {
            if !waiters.isEmpty {
                let waiter = waiters.removeFirst()
                waiter.resume(returning: client)
            } else {
                idleClients.append(client)
            }
            return
        }

        createdConnections = max(createdConnections - 1, 0)
        await client.disconnect()
        guard !waiters.isEmpty else { return }
        let waiter = waiters.removeFirst()
        do {
            let replacement = try makeClient()
            try await replacement.connect()
            createdConnections += 1
            waiter.resume(returning: replacement)
        } catch {
            waiter.resume(throwing: error)
        }
    }

    func shutdown() async {
        let clients = idleClients
        idleClients.removeAll()
        let pendingWaiters = waiters
        waiters.removeAll()
        createdConnections = 0

        for waiter in pendingWaiters {
            waiter.resume(throwing: CancellationError())
        }
        for client in clients {
            await client.disconnect()
        }
    }
}
