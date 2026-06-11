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

// Live foreground/background Lite write lease: owns the WriteLockService lock plus a periodic refresh task.
actor LiteWriteSession {
    let lock: WriteLockService
    private let ownedLockClient: (any RemoteStorageClientProtocol)?
    private var refreshTask: Task<Void, Never>?
    private var released = false

    init(lock: WriteLockService, ownedLockClient: (any RemoteStorageClientProtocol)? = nil) {
        self.lock = lock
        self.ownedLockClient = ownedLockClient
    }

    func startRefresh() {
        guard refreshTask == nil, !released else { return }
        let lock = self.lock
        refreshTask = Task {
            let sleepNanos = UInt64(WriteLockService.refreshInterval * 1_000_000_000)
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: sleepNanos)
                } catch {
                    break
                }
                if Task.isCancelled { break }
                _ = await lock.refresh()
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
        let ownedLockClient = self.ownedLockClient
        await Task {
            await lock.release()
            await ownedLockClient?.disconnectSafely()
        }.value
    }

    func hasLeaseConfidence(now: Date = Date()) async -> Bool {
        await lock.hasLeaseConfidence(now: now)
    }

    func assertStillOwnedForWrite(now: Date = Date()) async throws {
        switch await lock.assertStillOwned(now: now) {
        case .stillOwned:
            return
        case .lost:
            throw LiteRepoError.ownershipLost
        case .faulted:
            throw LiteRepoError.leaseConfidenceLost
        }
    }

    func assertLeaseConfidence(now: Date = Date()) async throws {
        if await lock.hasLeaseConfidence(now: now) { return }
        try await assertStillOwnedForWrite(now: now)
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
        switch mode {
        case .lite(let session):
            try await session.assertLeaseConfidence(now: now)
        }
    }

    // Before pushing a *dirty manifest*: re-assert ownership against the backend.
    static func assertOwnedBeforeFlush(_ session: LiteWriteSession?, now: Date = Date()) async throws {
        guard let session else { return }
        try await session.assertStillOwnedForWrite(now: now)
    }
}
