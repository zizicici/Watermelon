import Foundation
import os.log

// Diagnostic: surfaces the ownership-loss reason on the console. Category "WriteLock".
private let writeLockSessionLog = Logger(subsystem: "com.zizicici.watermelon", category: "WriteLock")

struct LiteLockClientHandle: Sendable {
    private final class Ownership: @unchecked Sendable {
        enum Owner: Equatable {
            case caller
            case coordinator
            case session
            case borrowed
            case disconnected
        }

        private let lock = NSLock()
        private var owner: Owner

        init(ownsClient: Bool) {
            owner = ownsClient ? .caller : .borrowed
        }

        func transferToCoordinator() {
            lock.withLock {
                if owner == .caller { owner = .coordinator }
            }
        }

        func transferToSession() {
            lock.withLock {
                if owner == .caller || owner == .coordinator { owner = .session }
            }
        }

        func claimDisconnect(from expectedOwner: Owner) -> Bool {
            lock.withLock {
                guard owner == expectedOwner else { return false }
                owner = .disconnected
                return true
            }
        }
    }

    let client: any RemoteStorageClientProtocol
    private let ownership: Ownership

    init(client: any RemoteStorageClientProtocol, ownsClient: Bool = true) {
        self.client = client
        self.ownership = Ownership(ownsClient: ownsClient)
    }

    func transferToCoordinator() {
        ownership.transferToCoordinator()
    }

    func transferToSession() {
        ownership.transferToSession()
    }

    func disconnectIfOwned() async {
        if ownership.claimDisconnect(from: .caller) {
            await client.disconnectSafely()
        }
    }

    func disconnectIfCoordinatorOwned() async {
        if ownership.claimDisconnect(from: .coordinator) {
            await client.disconnectSafely()
        }
    }

    func disconnectIfSessionOwned() async {
        if ownership.claimDisconnect(from: .session) {
            await client.disconnectSafely()
        }
    }
}

typealias ConnectedLockClientProvider = @Sendable () async throws -> LiteLockClientHandle
typealias RepoLeaseDiagnosticLogger = @Sendable (String, ExecutionLogLevel) async -> Void

// Live foreground/background Lite write lease: owns the WriteLockService lock plus a periodic refresh task.
actor RepoLeaseSession: RepoWriteSession {
    let lock: WriteLockService
    private var lockClientHandle: LiteLockClientHandle?
    private var retiredLockClientHandles: [LiteLockClientHandle] = []
    private let reconnectLockClient: ConnectedLockClientProvider?
    private let diagnosticLogger: RepoLeaseDiagnosticLogger?
    private var refreshTask: Task<Void, Never>?
    private var lockOperationInProgress = false
    private var lockOperationWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseTask: Task<Void, Never>?
    private var released = false

    init(
        lock: WriteLockService,
        lockClientHandle: LiteLockClientHandle? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        diagnosticLogger: RepoLeaseDiagnosticLogger? = nil
    ) {
        self.lock = lock
        if let lockClientHandle {
            lockClientHandle.transferToSession()
            self.lockClientHandle = lockClientHandle
        } else {
            self.lockClientHandle = nil
        }
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
        if let releaseTask {
            await releaseTask.value
            return
        }
        released = true
        let refreshTask = self.refreshTask
        self.refreshTask = nil
        refreshTask?.cancel()
        let task = Task<Void, Never> { [weak self] in
            guard let self else { return }
            await self.finishRelease(after: refreshTask)
        }
        releaseTask = task
        await task.value
    }

    private func finishRelease(after refreshTask: Task<Void, Never>?) async {
        _ = await refreshTask?.value
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        let lockClientHandle = self.lockClientHandle
        let retiredLockClientHandles = self.retiredLockClientHandles
        self.lockClientHandle = nil
        self.retiredLockClientHandles = []
        let lock = self.lock
        await Task {
            await lock.release()
            await lockClientHandle?.disconnectIfSessionOwned()
            for handle in retiredLockClientHandles {
                await handle.disconnectIfSessionOwned()
            }
        }.value
    }

    private func acquireLockOperationPermit() async {
        if !lockOperationInProgress {
            lockOperationInProgress = true
            return
        }
        await withCheckedContinuation { continuation in
            lockOperationWaiters.append(continuation)
        }
    }

    private func releaseLockOperationPermit() {
        guard !lockOperationWaiters.isEmpty else {
            lockOperationInProgress = false
            return
        }
        lockOperationWaiters.removeFirst().resume()
    }

    private func requireActiveOperation() throws {
        try Task.checkCancellation()
        guard !released else { throw CancellationError() }
    }

    func hasLeaseConfidence(now: Date = Date()) async -> Bool {
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        guard !released, !Task.isCancelled else { return false }
        let hasConfidence = await lock.hasLeaseConfidence(now: now)
        return !released && !Task.isCancelled && hasConfidence
    }

    func refreshLease(now: Date = Date()) async -> WriteLockService.Refresh {
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        guard !released, !Task.isCancelled else { return .degraded(.cancelled) }
        let first = await lock.refresh(now: now)
        await logRefreshResult(first, attempt: "first")
        guard !released, !Task.isCancelled else { return .degraded(.cancelled) }
        guard case .degraded(.retryable) = first,
              await lock.canRecoverRetryableRefresh(now: now) else {
            return first
        }
        await emitDiagnostic("[WriteLock] refresh retrying after retryable fault", level: .warning)
        guard await recoverLockClient(reason: "refresh retryable fault") else {
            await emitDiagnostic("[WriteLock] refresh retry skipped: lock-client reconnect unavailable", level: .warning)
            return released || Task.isCancelled ? .degraded(.cancelled) : first
        }
        guard !released, !Task.isCancelled else { return .degraded(.cancelled) }
        let retried = await lock.refresh(now: now)
        await logRefreshResult(retried, attempt: "retry")
        return released || Task.isCancelled ? .degraded(.cancelled) : retried
    }

    // Reclaiming assertion: REWRITES the own lock (writeOwnLock). It must never be wired into a per-month
    // worker/flush/verify gate — concurrent reclaims corrupt the lock file (the bug this model fixed).
    // The lock is written only by `acquire` and the single refresh task; gates use `assertLeaseProvenForWrite`.
    func assertStillOwnedForWrite(now: Date = Date()) async throws {
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        try requireActiveOperation()
        let first = await lock.assertStillOwned(now: now)
        try requireActiveOperation()
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertStillOwnedForWrite retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertStillOwnedForWrite retryable fault") else {
                try requireActiveOperation()
                try await mapOwnershipAssertion(first, operation: "assertStillOwnedForWrite")
                return
            }
            try requireActiveOperation()
            let retried = await lock.assertStillOwned(now: now)
            try requireActiveOperation()
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
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        try requireActiveOperation()
        if await lock.isUnattendedLease {
            try requireActiveOperation()
            if await lock.hasLeaseConfidence(now: now) {
                try requireActiveOperation()
                try await assertBackgroundForeignAbsenceLocked(now: now)
            } else {
                try requireActiveOperation()
                try await assertLeaseProvenForWriteLocked(now: now)
            }
            return
        }
        let hasConfidence = await lock.hasLeaseConfidence(now: now)
        try requireActiveOperation()
        if hasConfidence { return }
        try await assertLeaseProvenForWriteLocked(now: now)
    }

    // Write tier (manifest flush, canonical delete/restore, V1 prune, verify, version commit): proves
    // ownership against the backend WITHOUT reclaiming — LISTs for a foreign writer and reads the own-lock
    // body, but never writes the lock, so concurrent gates can't corrupt it (the refresh task stays the
    // sole writer). Same loss/fault/cancellation mapping as the strong path: a definitive loss fails closed
    // (`ownershipLost`), a transient fault recovers the client and retries once then fails closed
    // (`leaseConfidenceLost`), cancellation surfaces as cancellation.
    func assertLeaseProvenForWrite(now: Date = Date()) async throws {
        await acquireLockOperationPermit()
        defer { releaseLockOperationPermit() }
        try requireActiveOperation()
        try await assertLeaseProvenForWriteLocked(now: now)
    }

    private func assertLeaseProvenForWriteLocked(now: Date) async throws {
        let first = await lock.assertOwnedReadOnly(now: now)
        try requireActiveOperation()
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertLeaseProvenForWrite retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertLeaseProvenForWrite retryable fault") else {
                try requireActiveOperation()
                try await mapOwnershipAssertion(first, operation: "assertLeaseProvenForWrite")
                return
            }
            try requireActiveOperation()
            let retried = await lock.assertOwnedReadOnly(now: now)
            try requireActiveOperation()
            try await mapOwnershipAssertion(retried, operation: "assertLeaseProvenForWrite.retry")
            return
        }
        try await mapOwnershipAssertion(first, operation: "assertLeaseProvenForWrite")
    }

    func begin() {
        startRefresh()
    }

    func release() async {
        await stopAndRelease()
    }

    func assertDataWriteAllowed(now: Date) async throws {
        try await assertLeaseConfidence(now: now)
    }

    func assertControlWriteAllowed(now: Date) async throws {
        try await assertLeaseProvenForWrite(now: now)
    }

    private func assertBackgroundForeignAbsenceLocked(now: Date) async throws {
        let first = await lock.assertForeignAbsentForBackgroundWrite(now: now)
        try requireActiveOperation()
        if case .faulted(.retryable) = first {
            await emitDiagnostic("[WriteLock] assertBackgroundForeignAbsence retrying after retryable fault", level: .warning)
            guard await recoverLockClient(reason: "assertBackgroundForeignAbsence retryable fault") else {
                try requireActiveOperation()
                try await mapOwnershipAssertion(first, operation: "assertBackgroundForeignAbsence")
                return
            }
            try requireActiveOperation()
            let retried = await lock.assertForeignAbsentForBackgroundWrite(now: now)
            try requireActiveOperation()
            try await mapOwnershipAssertion(retried, operation: "assertBackgroundForeignAbsence.retry")
            return
        }
        try await mapOwnershipAssertion(first, operation: "assertBackgroundForeignAbsence")
    }

    private func recoverLockClient(reason: String) async -> Bool {
        guard !released, !Task.isCancelled, let reconnectLockClient else { return false }
        let newHandle: LiteLockClientHandle
        do {
            newHandle = try await reconnectLockClient()
        } catch {
            await emitDiagnostic("[WriteLock] lock-client reconnect failed: reason=\(reason), error=\(error.localizedDescription)", level: .warning)
            return false
        }
        guard !Task.isCancelled || released else {
            await newHandle.disconnectIfOwned()
            return false
        }
        newHandle.transferToSession()
        let previousHandle = lockClientHandle
        lockClientHandle = newHandle
        if let previousHandle {
            retiredLockClientHandles.append(previousHandle)
        }
        await lock.replaceClient(newHandle.client)
        let handlesToDisconnect = retiredLockClientHandles
        retiredLockClientHandles = []
        for handle in handlesToDisconnect {
            await handle.disconnectIfSessionOwned()
        }
        guard !released, !Task.isCancelled else { return false }
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
