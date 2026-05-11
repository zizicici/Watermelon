import Foundation

/// Type-level guard against the flat-set bug class. The reviews surfaced this
/// pattern three separate times: `[LibraryMonthKey: Set<Data>]` looks innocuous,
/// but every time we wrote `byMonth[m]?.subtract(...)` on a missing month the
/// optional-chain silently swallowed the operation, and every time we wrote
/// `Set<Data>` flat we lost the per-month consistency boundary entirely.
///
/// `PerMonth<Value>` requires explicit per-month operations and forces flatten
/// to be a documented decision rather than the default code-shape.
///
/// Asymmetry: subscript-set / `set(_:for:)` preserve empty values, while
/// `subtract(_:from:)` / `remove(_:)` prune on empty. Current callers always go
/// through `formUnion` + `subtract`, so the asymmetry isn't exercised.
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

    /// Flatten to a single Set/Dictionary across all months. ONLY use when the
    /// operation is genuinely month-agnostic (e.g. cross-month deduplication
    /// logging) — flatten loses month-as-consistency-unit, which is exactly the
    /// bug class this type was built to prevent. Document the reason at the
    /// callsite.
    func flattened<U>(combining: ([(LibraryMonthKey, Value)]) -> U) -> U {
        combining(byMonth.map { ($0.key, $0.value) })
    }

    /// Underlying dictionary view. Avoid in production code; prefer subscript
    /// + months. Provided for transitional API compat with consumers that still
    /// expect `[LibraryMonthKey: Value]`.
    var asDictionary: [LibraryMonthKey: Value] { byMonth }
}

extension PerMonth where Value == Set<Data> {
    /// Insert into the per-month set, creating the entry if missing.
    mutating func insert(_ fingerprint: Data, for month: LibraryMonthKey) {
        byMonth[month, default: []].insert(fingerprint)
    }

    /// Union into the per-month set, creating the entry if missing.
    mutating func formUnion(_ fingerprints: Set<Data>, for month: LibraryMonthKey) {
        guard !fingerprints.isEmpty else { return }
        byMonth[month, default: []].formUnion(fingerprints)
    }

    /// Subtract from the per-month set; no-op when the month isn't tracked.
    /// Empties the entry's value rather than removing the key by default,
    /// because absence-of-key vs empty-set is meaningless for fingerprints.
    /// Use `remove(_:)` if you also want to drop the key.
    mutating func subtract(_ fingerprints: Set<Data>, from month: LibraryMonthKey) {
        guard byMonth[month] != nil else { return }
        byMonth[month]?.subtract(fingerprints)
        if byMonth[month]?.isEmpty == true {
            byMonth.removeValue(forKey: month)
        }
    }

    /// Convenience: does this month's set contain the fingerprint?
    func contains(_ fingerprint: Data, in month: LibraryMonthKey) -> Bool {
        byMonth[month]?.contains(fingerprint) ?? false
    }
}
