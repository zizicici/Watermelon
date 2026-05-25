import Foundation

struct RemotePresenceSnapshot: Sendable, Equatable {
    struct Month: Sendable, Equatable {
        let missingHashes: Set<Data>
        let isAuthoritative: Bool

        static let absent = Month(missingHashes: [], isAuthoritative: false)
    }

    struct Entry: Sendable, Equatable {
        let month: LibraryMonthKey
        let value: Month
    }

    private let monthsByKey: [LibraryMonthKey: Month]

    init(monthsByKey: [LibraryMonthKey: Month] = [:]) {
        self.monthsByKey = monthsByKey
    }

    func month(_ key: LibraryMonthKey) -> Month {
        monthsByKey[key] ?? .absent
    }

    /// Apply paths MUST iterate this so explicit empty entries still clear stale overlays.
    var entries: [Entry] {
        monthsByKey.map { Entry(month: $0.key, value: $0.value) }
    }

    var freshMonths: Set<LibraryMonthKey> {
        var out: Set<LibraryMonthKey> = []
        for (k, v) in monthsByKey where v.isAuthoritative { out.insert(k) }
        return out
    }

    /// Cache-subtraction adapter: only months with a non-empty missing-hash set.
    /// Drops authoritative-empty entries so the cache fast paths trip exactly as before.
    var missingHashesByMonth: [LibraryMonthKey: Set<Data>] {
        var dict: [LibraryMonthKey: Set<Data>] = [:]
        for (month, value) in monthsByKey where !value.missingHashes.isEmpty {
            dict[month] = value.missingHashes
        }
        return dict
    }

    struct Builder {
        private var monthsByKey: [LibraryMonthKey: Month] = [:]

        mutating func set(_ key: LibraryMonthKey, missingHashes: Set<Data>, isAuthoritative: Bool) {
            monthsByKey[key] = Month(missingHashes: missingHashes, isAuthoritative: isAuthoritative)
        }

        func build() -> RemotePresenceSnapshot {
            RemotePresenceSnapshot(monthsByKey: monthsByKey)
        }
    }

    /// Wraps a fail-closed raw missing-hash dictionary into a non-authoritative snapshot.
    static func failClosed(missingByMonth: [LibraryMonthKey: Set<Data>]) -> RemotePresenceSnapshot {
        var builder = Builder()
        for (month, hashes) in missingByMonth {
            builder.set(month, missingHashes: hashes, isAuthoritative: false)
        }
        return builder.build()
    }
}
