import Foundation

/// Per-month "I uploaded this but haven't confirmed it's in the commit log" tracker.
///
/// Lifetime is profile-scoped: `BackupV2RuntimeServices` owns one and discards
/// it on shutdown. Cross-profile leak (Round 6 / Round 8) is impossible by
/// construction — the tracker simply doesn't outlive the profile session.
///
/// `RepoCommittedView` shows the union of (committed + optimistic). The resume
/// planner subtracts the inflight set so it sees pure committed truth — that's
/// the only consumer that requires durability semantics, the rest of the UI is
/// fine with optimism.
final class OptimisticInflightTracker: @unchecked Sendable {
    private let lock = NSLock()
    private var assetFingerprintsByMonth = PerMonth<Set<Data>>()

    init() {}

    func markUncommittedAssets(month: LibraryMonthKey, fingerprints: Set<Data>) {
        guard !fingerprints.isEmpty else { return }
        lock.withLock {
            assetFingerprintsByMonth.formUnion(fingerprints, for: month)
        }
    }

    /// Callers pass the asset/tombstone union from `FlushDelta`.
    func markCommitted(month: LibraryMonthKey, fingerprints: Set<Data>) {
        lock.withLock {
            assetFingerprintsByMonth.subtract(fingerprints, from: month)
        }
    }

    /// Returns a snapshot copy — convenience for callers that need a free-standing
    /// value. Hot paths (resume planner, Home sync) should prefer `readUncommittedAssets`
    /// to avoid the full PerMonth + Set<Data> copy under the lock.
    func uncommittedAssets() -> PerMonth<Set<Data>> {
        lock.withLock { assetFingerprintsByMonth }
    }

    /// Executes `block` under the lock with direct access — no copy. Block must NOT
    /// escape the snapshot; that's the price for skipping the dict/set deep copy.
    func readUncommittedAssets<T>(_ block: (PerMonth<Set<Data>>) -> T) -> T {
        lock.withLock { block(assetFingerprintsByMonth) }
    }

    func reset() {
        lock.withLock {
            assetFingerprintsByMonth.removeAll()
        }
    }
}
