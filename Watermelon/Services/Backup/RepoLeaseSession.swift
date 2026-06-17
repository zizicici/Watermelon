import Foundation
import os.log

// Diagnostic: surfaces the ownership-loss reason on the console. Category "WriteLock".
private let writeLockSessionLog = Logger(subsystem: "com.zizicici.watermelon", category: "WriteLock")

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
actor RepoLeaseSession {
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

    // Reclaiming assertion: REWRITES the own lock (writeOwnLock). It must never be wired into a per-month
    // worker/flush/verify gate — concurrent reclaims corrupt the lock file (the bug this model fixed).
    // The lock is written only by `acquire` and the single refresh task; gates use `assertLeaseProvenForWrite`.
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
        case .lost(.ownLockStale):
            // Still ours, just stale: the refresh task can reclaim it — surface as a confidence loss.
            writeLockSessionLog.error("[WriteLock] lease confidence lost: own lock stale (refresh lagging)")
            throw LiteRepoError.leaseConfidenceLost
        case .lost(let reason):
            writeLockSessionLog.error("[WriteLock] ownership assertion LOST: reason=\(String(describing: reason), privacy: .public)")
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

    // Read tier (per-month load, data-upload gate): the lease must be confidently held. Read-only — never
    // reclaims/writes the lock; recovery is the refresh task's job, so a lapse just fails closed. For an
    // attended lease, in-memory confidence is sufficient: confidenceMaxAge (2.5m) < expiry+skew (6m), so
    // "confident" implies our lock is still fresh and a foreign acquire cannot have taken over. An unattended
    // lease's local clock is untrustworthy (device sleep), so it LISTs for foreign evidence — that path reads
    // and may delete *foreign* stale locks, but never writes our own lock.
    func assertLeaseConfidence(now: Date = Date()) async throws {
        if await lock.isUnattendedLease {
            // Unattended: local confidence is not trustable blindly (device sleep). While confident, a
            // foreign-evidence LIST suffices; once it lapses, fall back to the read-only ownership proof
            // (recovers if still owned, surfaces a successor/foreign as ownershipLost) — never reclaims.
            if await lock.hasLeaseConfidence(now: now) {
                try await assertBackgroundForeignAbsence(now: now)
            } else {
                try await assertLeaseProvenForWrite(now: now)
            }
            return
        }
        if await lock.hasLeaseConfidence(now: now) { return }
        // Lapsed: prove ownership read-only (recovers a still-owned lease without writing the lock; the
        // refresh task owns the actual mtime refresh). Fails closed on a real loss or sustained fault.
        try await assertLeaseProvenForWrite(now: now)
    }

    // Write tier (manifest flush, canonical delete/restore, V1 prune, verify, version commit): proves
    // ownership against the backend WITHOUT reclaiming — LISTs for a foreign writer and reads the own-lock
    // body, but never writes the lock, so concurrent gates can't corrupt it (the refresh task stays the
    // sole writer). Same loss/fault/cancellation mapping as the strong path: a definitive loss fails closed
    // (`ownershipLost`), a transient fault recovers the client and retries once then fails closed
    // (`leaseConfidenceLost`), cancellation surfaces as cancellation.
    func assertLeaseProvenForWrite(now: Date = Date()) async throws {
        let first = await lock.assertOwnedReadOnly(now: now)
        if case .faulted(.retryable) = first, await recoverLockClient() {
            let retried = await lock.assertOwnedReadOnly(now: now)
            try mapOwnershipAssertion(retried)
            return
        }
        try mapOwnershipAssertion(first)
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

// Fail-closed lease gates the Lite run consults. All no-ops when there is no write session, and none
// write the lock — the refresh task is the sole writer (see assertStillOwnedForWrite's warning).
enum RepoLeaseGuard {
    // Before writing remote *data* bytes: the lease must still be confidently held.
    static func assertLeaseConfidence(_ session: RepoLeaseSession?, now: Date = Date()) async throws {
        guard let session else { return }
        try await session.assertLeaseConfidence(now: now)
    }

    static func assertLeaseConfidence(_ mode: RepoWriteMode, now: Date = Date()) async throws {
        try await mode.leaseSession.assertLeaseConfidence(now: now)
    }

    // Write-tier lease gate (manifest flush / verify / migration / cleanup): read-only ownership proof,
    // never reclaims (never writes the own lock), so concurrent gates can't corrupt the lock file.
    static func leaseProvenAssertion(_ session: RepoLeaseSession?) -> MonthManifestOwnershipAssertion? {
        guard let session else { return nil }
        return { try await session.assertLeaseProvenForWrite() }
    }

    // Before pushing a *dirty manifest*: prove ownership (read-only) against the backend.
    static func assertOwnedBeforeFlush(_ session: RepoLeaseSession?, now: Date = Date()) async throws {
        guard let session else { return }
        try await session.assertLeaseProvenForWrite(now: now)
    }
}
