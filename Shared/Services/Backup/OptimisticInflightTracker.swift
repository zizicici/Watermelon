import Foundation

/// Per-month uploaded-but-not-yet-committed fingerprints; resume planner subtracts this to see committed truth.
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
    func markCommitted(month: LibraryMonthKey, fingerprints: Set<Data>) -> Bool {
        lock.withLock {
            let previous = assetFingerprintsByMonth[month] ?? []
            assetFingerprintsByMonth.subtract(fingerprints, from: month)
            return previous != (assetFingerprintsByMonth[month] ?? [])
        }
    }

    /// Snapshot under lock then run block outside; re-entry can't deadlock.
    func readUncommittedAssets<T>(_ block: (PerMonth<Set<Data>>) -> T) -> T {
        let snapshot = lock.withLock { assetFingerprintsByMonth }
        return block(snapshot)
    }

    func reset() -> Set<LibraryMonthKey> {
        lock.withLock {
            let months = assetFingerprintsByMonth.months
            assetFingerprintsByMonth.removeAll()
            return months
        }
    }
}
