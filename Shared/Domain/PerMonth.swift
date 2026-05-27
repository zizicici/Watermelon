import Foundation

/// Prevents missing-month optional chaining from silently dropping per-month set mutations.
struct PerMonth<Value: Sendable & Equatable>: Sendable, Equatable {
    private var byMonth: [LibraryMonthKey: Value]

    init() {
        self.byMonth = [:]
    }

    init(_ byMonth: [LibraryMonthKey: Value]) {
        self.byMonth = byMonth
    }

    static func == (lhs: PerMonth<Value>, rhs: PerMonth<Value>) -> Bool {
        lhs.byMonth == rhs.byMonth
    }

    subscript(_ month: LibraryMonthKey) -> Value? {
        get { byMonth[month] }
        set { byMonth[month] = newValue }
    }

    var months: Set<LibraryMonthKey> {
        Set(byMonth.keys)
    }

    var isEmpty: Bool { byMonth.isEmpty }

    mutating func set(_ value: Value, for month: LibraryMonthKey) {
        byMonth[month] = value
    }

    mutating func remove(_ month: LibraryMonthKey) {
        byMonth.removeValue(forKey: month)
    }

    mutating func removeAll() {
        byMonth.removeAll()
    }

    func map<U>(_ transform: (LibraryMonthKey, Value) throws -> U) rethrows -> PerMonth<U> where U: Sendable & Equatable {
        var result: [LibraryMonthKey: U] = [:]
        result.reserveCapacity(byMonth.count)
        for (month, value) in byMonth {
            result[month] = try transform(month, value)
        }
        return PerMonth<U>(result)
    }

    func contains(_ month: LibraryMonthKey) -> Bool {
        byMonth[month] != nil
    }

    /// Flatten only for month-agnostic work; it discards the month consistency boundary.
    func flattened<U>(combining: ([(LibraryMonthKey, Value)]) -> U) -> U {
        combining(byMonth.map { ($0.key, $0.value) })
    }

    /// Transitional escape hatch; production code should keep month operations explicit.
    var asDictionary: [LibraryMonthKey: Value] { byMonth }
}

extension PerMonth where Value == Set<AssetFingerprint> {
    mutating func insert(_ fingerprint: AssetFingerprint, for month: LibraryMonthKey) {
        byMonth[month, default: []].insert(fingerprint)
    }

    mutating func formUnion(_ fingerprints: Set<AssetFingerprint>, for month: LibraryMonthKey) {
        guard !fingerprints.isEmpty else { return }
        byMonth[month, default: []].formUnion(fingerprints)
    }

    mutating func subtract(_ fingerprints: Set<AssetFingerprint>, from month: LibraryMonthKey) {
        guard byMonth[month] != nil else { return }
        byMonth[month]?.subtract(fingerprints)
        if byMonth[month]?.isEmpty == true {
            byMonth.removeValue(forKey: month)
        }
    }

    func contains(_ fingerprint: AssetFingerprint, in month: LibraryMonthKey) -> Bool {
        byMonth[month]?.contains(fingerprint) ?? false
    }
}

extension PerMonth where Value == Set<Data> {
    // Resource content-hash domain (RepoCommittedView.physicallyMissingByMonth); distinct from
    // the asset-fingerprint overload above.
    mutating func subtract(_ hashes: Set<Data>, from month: LibraryMonthKey) {
        guard byMonth[month] != nil else { return }
        byMonth[month]?.subtract(hashes)
        if byMonth[month]?.isEmpty == true {
            byMonth.removeValue(forKey: month)
        }
    }
}
