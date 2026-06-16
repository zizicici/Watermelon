import Foundation

struct LiteLockClientHandle: Sendable {
    let client: any RemoteStorageClientProtocol
    private(set) var ownsClient: Bool

    init(client: any RemoteStorageClientProtocol, ownsClient: Bool = true) {
        self.client = client
        self.ownsClient = ownsClient
    }

    mutating func transferToSession() {
        ownsClient = false
    }

    func disconnectIfOwned() async {
        if ownsClient {
            await client.disconnectSafely()
        }
    }
}

typealias ConnectedLockClientProvider = @Sendable () async throws -> LiteLockClientHandle

// Live foreground/background Lite write lease: owns the WriteLockService lock plus a periodic refresh task.
actor LiteWriteSession {
    let lock: WriteLockService
    private var lockClientHandle: LiteLockClientHandle?
    private var retiredLockClientHandles: [LiteLockClientHandle] = []
    private let reconnectLockClient: ConnectedLockClientProvider?
    private var refreshTask: Task<Void, Never>?
    private var released = false

    init(
        lock: WriteLockService,
        ownedLockClient: (any RemoteStorageClientProtocol)? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil
    ) {
        self.lock = lock
        self.lockClientHandle = ownedLockClient.map { LiteLockClientHandle(client: $0) }
        self.reconnectLockClient = reconnectLockClient
    }

    func startRefresh() {
        guard refreshTask == nil, !released else { return }
        refreshTask = Task { [weak self] in
            let sleepNanos = UInt64(WriteLockService.refreshInterval * 1_000_000_000)
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: sleepNanos)
                } catch {
                    break
                }
                if Task.isCancelled { break }
                guard let self else { break }
                _ = await self.refreshLease()
            }
        }
    }

    // Stops the refresh loop and deletes our lock. Idempotent; safe to call from every run exit.
    // Awaits any in-flight refresh before releasing so the old refresh cannot delete a new
    // same-writer session's lock or fail its cleanup after the caller disconnects.
    func stopAndRelease() async {
        if released { return }
        released = true
        let task = refreshTask
        refreshTask = nil
        task?.cancel()
        _ = await task?.value
        let lock = self.lock
        let lockClientHandle = self.lockClientHandle
        let retiredLockClientHandles = self.retiredLockClientHandles
        self.lockClientHandle = nil
        self.retiredLockClientHandles = []
        await Task {
            await lock.release()
            await lockClientHandle?.disconnectIfOwned()
            for handle in retiredLockClientHandles {
                await handle.disconnectIfOwned()
            }
        }.value
    }

    func hasLeaseConfidence(now: Date = Date()) async -> Bool {
        await lock.hasLeaseConfidence(now: now)
    }

    func refreshLease(now: Date = Date()) async -> WriteLockService.Refresh {
        let first = await lock.refresh(now: now)
        guard case .degraded(.retryable) = first,
              await lock.canRecoverRetryableRefresh(now: now),
              await recoverLockClient() else {
            return first
        }
        return await lock.refresh(now: now)
    }

    func assertStillOwnedForWrite(now: Date = Date()) async throws {
        let first = await lock.assertStillOwned(now: now)
        if case .faulted(.retryable) = first, await recoverLockClient() {
            let retried = await lock.assertStillOwned(now: now)
            try mapOwnershipAssertion(retried)
            return
        }
        try mapOwnershipAssertion(first)
    }

    private func mapOwnershipAssertion(_ assertion: WriteLockService.Assertion) throws {
        switch assertion {
        case .stillOwned:
            return
        case .lost:
            throw LiteRepoError.ownershipLost
        case .faulted(.cancelled):
            // A cancelled ownership LIST means the run is being torn down (pause/stop), not a confidence
            // loss; surface cancellation so the executor and version/migration/flush guards still see it.
            throw CancellationError()
        case .faulted:
            // Teardown can also surface as a retryable fault, or as a cancellation swallowed inside
            // recoverLockClient's reconnect; a torn-down run must still surface cancellation, never a
            // lease-fail-fast confidence loss.
            if Task.isCancelled { throw CancellationError() }
            throw LiteRepoError.leaseConfidenceLost
        }
    }

    func assertLeaseConfidence(now: Date = Date()) async throws {
        if await lock.isUnattendedLease {
            // An unattended lease cannot trust local confidence blindly. While confidence is fresh, the own lock
            // is necessarily fresh too, and the foreign-evidence LIST clears expired/invalid locks or fails on
            // fresh/future/changed locks. Once confidence expires or drops, fall back to full body proof.
            if await lock.hasLeaseConfidence(now: now) {
                try await assertBackgroundForeignAbsence(now: now)
            } else {
                try await assertStillOwnedForWrite(now: now)
            }
            return
        }
        if await lock.hasLeaseConfidence(now: now) { return }
        try await assertStillOwnedForWrite(now: now)
    }

    private func assertBackgroundForeignAbsence(now: Date) async throws {
        let first = await lock.assertForeignAbsentForBackgroundWrite(now: now)
        if case .faulted(.retryable) = first, await recoverLockClient() {
            let retried = await lock.assertForeignAbsentForBackgroundWrite(now: now)
            try mapOwnershipAssertion(retried)
            return
        }
        try mapOwnershipAssertion(first)
    }

    private func recoverLockClient() async -> Bool {
        guard !released, let reconnectLockClient else { return false }
        let newHandle: LiteLockClientHandle
        do {
            newHandle = try await reconnectLockClient()
        } catch {
            return false
        }
        guard !released else {
            await newHandle.disconnectIfOwned()
            return false
        }
        let previousHandle = lockClientHandle
        lockClientHandle = newHandle
        if let previousHandle {
            retiredLockClientHandles.append(previousHandle)
        }
        await lock.replaceClient(newHandle.client)
        guard !released else {
            return false
        }
        return true
    }
}

// Two fail-closed gates the Lite write path consults. Both are no-ops when there is no write session.
enum LiteWriteGuard {
    static func ownershipAssertion(_ session: LiteWriteSession?) -> MonthManifestOwnershipAssertion? {
        guard let session else { return nil }
        return { try await session.assertStillOwnedForWrite() }
    }

    static func ownershipAssertion(_ mode: RepoWriteMode) -> MonthManifestOwnershipAssertion? {
        mode.ownershipAssertion
    }

    // Before writing remote *data* bytes: the lease must still be confidently held.
    static func assertLeaseConfidence(_ session: LiteWriteSession?, now: Date = Date()) async throws {
        guard let session else { return }
        try await session.assertLeaseConfidence(now: now)
    }

    static func assertLeaseConfidence(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.liteSession.assertLeaseConfidence(now: now)
    }

    // Before pushing a *dirty manifest*: re-assert ownership against the backend.
    static func assertOwnedBeforeFlush(_ session: LiteWriteSession?, now: Date = Date()) async throws {
        guard let session else { return }
        try await session.assertStillOwnedForWrite(now: now)
    }
}
