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
typealias RepoLeaseDiagnosticLogger = @Sendable (String, ExecutionLogLevel) async -> Void

// Live foreground/background Lite write lease: owns the WriteLockService lock plus a periodic refresh task.
actor RepoLeaseSession {
    let lock: WriteLockService
    private var lockClientHandle: LiteLockClientHandle?
    private var retiredLockClientHandles: [LiteLockClientHandle] = []
    private let reconnectLockClient: ConnectedLockClientProvider?
    private let diagnosticLogger: RepoLeaseDiagnosticLogger?
    private var refreshTask: Task<Void, Never>?
    private var released = false

    init(
        lock: WriteLockService,
        ownedLockClient: (any RemoteStorageClientProtocol)? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        diagnosticLogger: RepoLeaseDiagnosticLogger? = nil
    ) {
        self.lock = lock
        self.lockClientHandle = ownedLockClient.map { LiteLockClientHandle(client: $0) }
        self.reconnectLockClient = reconnectLockClient
        self.diagnosticLogger = diagnosticLogger
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
        await logRefreshResult(first, attempt: "first")
        guard case .degraded(.retryable) = first,
              await lock.canRecoverRetryableRefresh(now: now) else {
            return first
        }
        await emitDiagnostic("[WriteLock] refresh retrying after retryable fault", level: .warning)
        guard await recoverLockClient(reason: "refresh retryable fault") else {
            await emitDiagnostic("[WriteLock] refresh retry skipped: lock-client reconnect unavailable", level: .warning)
            return first
        }
        let retried = await lock.refresh(now: now)
        await logRefreshResult(retried, attempt: "retry")
        return retried
    }

    // Reclaiming assertion: REWRITES the own lock (writeOwnLock). It must never be wired into a per-month
    // worker/flush/verify gate — concurrent reclaims corrupt the lock file (the bug this model fixed).
    // The lock is written only by `acquire` and the single refresh task; gates use `assertLeaseProvenForWrite`.
    func assertStillOwnedForWrite(now: Date = Date()) async throws {
        let first = await lock.assertStillOwned(now: now)
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertStillOwnedForWrite retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertStillOwnedForWrite retryable fault") else {
                try await mapOwnershipAssertion(first, operation: "assertStillOwnedForWrite")
                return
            }
            let retried = await lock.assertStillOwned(now: now)
            try await mapOwnershipAssertion(retried, operation: "assertStillOwnedForWrite.retry")
            return
        }
        try await mapOwnershipAssertion(first, operation: "assertStillOwnedForWrite")
    }

    private func mapOwnershipAssertion(_ assertion: WriteLockService.Assertion, operation: String) async throws {
        switch assertion {
        case .stillOwned:
            return
        case .lost(.ownLockStale):
            // Still ours, just stale: the refresh task can reclaim it — surface as a confidence loss.
            await emitDiagnostic("[WriteLock] \(operation) failed: assertion=lost(ownLockStale), mapped=leaseConfidenceLost", level: .error)
            writeLockSessionLog.error("[WriteLock] lease confidence lost: own lock stale (refresh lagging)")
            throw LiteRepoError.leaseConfidenceLost
        case .lost(let reason):
            await emitDiagnostic("[WriteLock] \(operation) failed: assertion=lost(\(String(describing: reason))), mapped=ownershipLost", level: .error)
            writeLockSessionLog.error("[WriteLock] ownership assertion LOST: reason=\(String(describing: reason), privacy: .public)")
            throw LiteRepoError.ownershipLost
        case .faulted(.cancelled):
            // A cancelled ownership LIST means the run is being torn down (pause/stop), not a confidence
            // loss; surface cancellation so the executor and version/migration/flush guards still see it.
            throw CancellationError()
        case .faulted(let category):
            // Teardown can also surface as a retryable fault, or as a cancellation swallowed inside
            // recoverLockClient's reconnect; a torn-down run must still surface cancellation, never a
            // lease-fail-fast confidence loss.
            if Task.isCancelled { throw CancellationError() }
            await emitDiagnostic("[WriteLock] \(operation) faulted: category=\(String(describing: category)), mapped=leaseConfidenceLost", level: .error)
            throw LiteRepoError.leaseConfidenceLost
        }
    }

    // Data-upload gate. An attended lease trusts in-memory confidence: the refresh task is the remote
    // watchdog, and confidence can only hold (≤ confidenceMaxAge) while our lock is far from the takeover
    // threshold (expiry + skew), so no foreign writer/successor can legitimately have reclaimed it. A
    // confident attended gate therefore makes ZERO remote calls; a lapse falls back to the read-only proof.
    // The control-state writes that a silently-lost lease could corrupt (manifest flush / version commit /
    // cleanup) keep their own strong `assertLeaseProvenForWrite` gate, so an undetected lapse here can only
    // waste idempotent byte uploads (recovered as orphans), never break the single-writer invariant.
    // An unattended lease still LISTs for foreign evidence while confident — its local clock is untrustworthy
    // after device sleep — and proves the own-lock body on lapse.
    func assertLeaseConfidence(now: Date = Date()) async throws {
        if await lock.isUnattendedLease {
            if await lock.hasLeaseConfidence(now: now) {
                try await assertBackgroundForeignAbsence(now: now)
            } else {
                try await assertLeaseProvenForWrite(now: now)
            }
            return
        }
        if await lock.hasLeaseConfidence(now: now) { return }
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
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertLeaseProvenForWrite retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertLeaseProvenForWrite retryable fault") else {
                try await mapOwnershipAssertion(first, operation: "assertLeaseProvenForWrite")
                return
            }
            let retried = await lock.assertOwnedReadOnly(now: now)
            try await mapOwnershipAssertion(retried, operation: "assertLeaseProvenForWrite.retry")
            return
        }
        try await mapOwnershipAssertion(first, operation: "assertLeaseProvenForWrite")
    }

    private func assertBackgroundForeignAbsence(now: Date) async throws {
        let first = await lock.assertForeignAbsentForBackgroundWrite(now: now)
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertBackgroundForeignAbsence retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertBackgroundForeignAbsence retryable fault") else {
                try await mapOwnershipAssertion(first, operation: "assertBackgroundForeignAbsence")
                return
            }
            let retried = await lock.assertForeignAbsentForBackgroundWrite(now: now)
            try await mapOwnershipAssertion(retried, operation: "assertBackgroundForeignAbsence.retry")
            return
        }
        try await mapOwnershipAssertion(first, operation: "assertBackgroundForeignAbsence")
    }

    private func recoverLockClient(reason: String) async -> Bool {
        guard !released, let reconnectLockClient else { return false }
        let newHandle: LiteLockClientHandle
        do {
            newHandle = try await reconnectLockClient()
        } catch {
            await emitDiagnostic("[WriteLock] lock-client reconnect failed: reason=\(reason), error=\(error.localizedDescription)", level: .warning)
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
        // Disconnect replaced clients now so reconnects can't accumulate live connections until run end.
        let handlesToDisconnect = retiredLockClientHandles
        retiredLockClientHandles = []
        if !handlesToDisconnect.isEmpty {
            Task {
                for handle in handlesToDisconnect {
                    await handle.disconnectIfOwned()
                }
            }
        }
        await emitDiagnostic("[WriteLock] lock-client reconnect succeeded: reason=\(reason)", level: .debug)
        return true
    }

    private func logRefreshResult(_ result: WriteLockService.Refresh, attempt: String) async {
        switch result {
        case .refreshed:
            await emitDiagnostic("[WriteLock] refresh \(attempt) succeeded", level: .debug)
        case .degraded(let category):
            await emitDiagnostic("[WriteLock] refresh \(attempt) degraded: category=\(String(describing: category))", level: .warning)
        }
    }

    private func emitDiagnostic(_ message: String, level: ExecutionLogLevel) async {
        await diagnosticLogger?(message, level)
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
