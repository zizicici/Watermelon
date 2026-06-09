import Foundation

// Live foreground/background Lite write lease: owns the WriteLockService lock plus a periodic refresh
// task. A `nil` session means V1 or a read-only path, so every gate treats absence as "no Lite gating".
// Nothing constructs this unless the internal `liteRepoEnabled` flag is on.
actor LiteWriteSession {
    let lock: WriteLockService
    private var refreshTask: Task<Void, Never>?
    private var released = false

    init(lock: WriteLockService) {
        self.lock = lock
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
        await lock.release()
    }

    func hasLeaseConfidence(now: Date = Date()) async -> Bool {
        await lock.hasLeaseConfidence(now: now)
    }

    // Re-asserts ownership against the backend (re-LIST + reclaim). True only when still safely owned.
    func assertStillOwned(now: Date = Date()) async -> Bool {
        switch await lock.assertStillOwned(mode: .foreground, now: now) {
        case .stillOwned:
            return true
        case .lost, .faulted:
            return false
        }
    }
}

// Two fail-closed gates the Lite write path consults. Both are no-ops when the session is nil (V1 /
// read path), so call sites stay uniform across layouts.
enum LiteWriteGuard {
    // Before writing remote *data* bytes: the lease must still be confidently held.
    static func assertLeaseConfidence(_ session: LiteWriteSession?, now: Date = Date()) async throws {
        guard let session else { return }
        guard await session.hasLeaseConfidence(now: now) else {
            throw LiteRepoError.leaseConfidenceLost
        }
    }

    // Before pushing a *dirty manifest*: re-assert ownership against the backend.
    static func assertOwnedBeforeFlush(_ session: LiteWriteSession?, now: Date = Date()) async throws {
        guard let session else { return }
        guard await session.assertStillOwned(now: now) else {
            throw LiteRepoError.ownershipLost
        }
    }
}
