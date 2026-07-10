import Foundation

actor StorageClientPool {
    // Outcome of a bounded replacement connect. `.failed` carries the connect error for the caller to classify
    // (terminal vs transient); `.timedOut` means the deadline/cancellation won and the connect was abandoned.
    enum ConnectOutcome {
        case connected(any RemoteStorageClientProtocol)
        case failed(Error)
        case timedOut
    }

    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private var createdConnections = 0
    private var idleClients: [any RemoteStorageClientProtocol] = []
    private var waiters: [CheckedContinuation<any RemoteStorageClientProtocol, Error>] = []
    // Latched by shutdown(): an in-flight task releasing afterwards must not park a live session in a
    // dead pool, and a straggler acquire must not open a fresh connection nobody will ever close.
    private var isShutdown = false

    init(
        maxConnections: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) {
        self.maxConnections = max(1, maxConnections)
        self.makeClient = makeClient
    }

    func seedConnectedClient(_ client: any RemoteStorageClientProtocol) {
        guard !isShutdown else {
            Task { await client.disconnect() }
            return
        }
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
        guard !isShutdown else { throw CancellationError() }
        if let client = idleClients.popLast() {
            return client
        }
        if createdConnections < maxConnections {
            createdConnections += 1
            // Bound the connect so a hung one fails fast (caller retries/degrades) instead of stalling.
            switch await boundedConnect(by: Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout), abortIf: { false }) {
            case .connected(let client):
                return client
            case .failed(let error):
                createdConnections = max(createdConnections - 1, 0)
                throw error
            case .timedOut:
                createdConnections = max(createdConnections - 1, 0)
                throw RemoteStorageClientError.unavailable
            }
        }
        return try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        guard !isShutdown else {
            await client.disconnect()
            return
        }
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
        // Bound the replacement connect too, so a hung reconnect can't strand a parked waiter indefinitely.
        switch await boundedConnect(by: Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout), abortIf: { false }) {
        case .connected(let replacement):
            createdConnections += 1
            waiter.resume(returning: replacement)
        case .failed(let error):
            waiter.resume(throwing: error)
        case .timedOut:
            waiter.resume(throwing: RemoteStorageClientError.unavailable)
        }
    }

    // Reserved-slot replacement: the slot stays counted from retire through reconnect (a non-recovered outcome
    // frees it via the worker's terminal release(reusable: false)), so live sessions never exceed the cap.
    func retireForReplacement(_ broken: any RemoteStorageClientProtocol) async {
        await broken.disconnect()   // drop the broken session before reconnecting so the slot never holds two
    }

    // Race a fresh makeClient()+connect() against the deadline, cancellation, and abortIf (a sibling's
    // queue-stop); a loser still connecting is abandoned and detached-reaped so it can't stall the run or leak a
    // live session. Count-neutral — the caller owns slot accounting.
    private func boundedConnect(
        by deadline: Date,
        abortIf shouldAbort: @escaping @Sendable () async -> Bool
    ) async -> ConnectOutcome {
        let makeClient = self.makeClient
        let clientHandle = NetworkAttemptClientHandle()
        let result = await NetworkRecovery.boundedAttempt(
            deadline: deadline,
            abortIf: shouldAbort,
            onAbandon: { clientHandle.abandon() },
            reap: { (_: ConnectOutcome) in await clientHandle.reap() },
            op: { () async -> ConnectOutcome in
                do {
                    let client = try makeClient()
                    guard clientHandle.install(client) else { throw CancellationError() }
                    try await client.connect()
                    return .connected(client)
                } catch {
                    return .failed(error)
                }
            }
        )
        switch result {
        case .completed(let outcome):
            return outcome   // .connected / .failed (a cancelled connect surfaces as .failed)
        case .timedOut:
            return .timedOut   // deadline/cancel/abort won; caller re-checks stop/cancel at the loop top
        }
    }

    // Reserved-slot reconnect: the slot is already counted, so this is count-neutral.
    func connectReplacement(
        by deadline: Date,
        abortIf shouldAbort: @escaping @Sendable () async -> Bool = { false }
    ) async -> ConnectOutcome {
        guard !isShutdown else { return .timedOut }
        return await boundedConnect(by: deadline, abortIf: shouldAbort)
    }

    // Bounded acquire for the worker's initial client: pop idle, else connect a fresh client into a counted slot
    // under the same deadline/cancel/abort bound, so a single hung connect can't eat the recovery window. Frees
    // the slot if the connect doesn't land.
    func acquire(
        by deadline: Date,
        abortIf shouldAbort: @escaping @Sendable () async -> Bool = { false }
    ) async -> ConnectOutcome {
        guard !isShutdown else { return .timedOut }
        if let client = idleClients.popLast() { return .connected(client) }
        guard createdConnections < maxConnections else {
            // Saturated with no idle client — wait for a release to hand one off (unbounded; not reached when
            // the pool is sized to the worker count).
            do { return .connected(try await withCheckedThrowingContinuation { waiters.append($0) }) }
            catch { return .failed(error) }
        }
        createdConnections += 1
        let outcome = await boundedConnect(by: deadline, abortIf: shouldAbort)
        if case .connected = outcome { return outcome }
        createdConnections = max(createdConnections - 1, 0)   // connect failed/timed out → free the slot
        return outcome
    }

    func shutdown() async {
        isShutdown = true
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
