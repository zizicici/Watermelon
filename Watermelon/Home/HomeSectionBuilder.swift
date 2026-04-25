import Foundation

@MainActor
final class HomeSectionBuilder {
    struct Hooks {
        var allMonthRows: () -> [LibraryMonthKey: HomeMonthRow]
        var monthRow: (LibraryMonthKey) -> HomeMonthRow
    }

    private(set) var sections: [HomeMergedYearSection] = []
    private(set) var rowLookup: [LibraryMonthKey: HomeMonthRow] = [:]

    private let hooks: Hooks

    init(hooks: Hooks) {
        self.hooks = hooks
    }

    /// Refresh both the row lookup and the sections from the data source. Used for
    /// full structural rebuilds (initial load, scope change, post-execution refresh).
    func rebuildAll() {
        rowLookup = hooks.allMonthRows()
        rebuildSections()
    }

    /// In-place row refresh for the given months. Skips months whose updated row has
    /// neither a local nor a remote summary (e.g., a month that emptied during a scan).
    /// Returns the months whose row actually changed.
    @discardableResult
    func refreshFileSizeRows(for months: Set<LibraryMonthKey>) -> Set<LibraryMonthKey> {
        var changed = Set<LibraryMonthKey>()
        for month in months where rowLookup[month] != nil {
            let row = hooks.monthRow(month)
            guard row.local != nil || row.remote != nil else { continue }
            rowLookup[month] = row
            changed.insert(month)
        }
        guard !changed.isEmpty else { return changed }
        sections = sections.map { section in
            let updatedRows = section.rows.map { row -> HomeMonthRow in
                guard changed.contains(row.month), let updated = rowLookup[row.month] else { return row }
                return updated
            }
            return HomeMergedYearSection(year: section.year, rows: updatedRows)
        }
        return changed
    }

    /// Refresh row lookup for the given months and rebuild sections from scratch.
    /// Used by the execution change handler where multiple month rows may have flipped
    /// phase (and we want a single section rebuild instead of per-month patches).
    func updateRowsAndRebuild(for months: Set<LibraryMonthKey>) {
        for month in months {
            rowLookup[month] = hooks.monthRow(month)
        }
        rebuildSections()
    }

    private func rebuildSections() {
        var rowsByYear: [Int: [HomeMonthRow]] = [:]
        for (_, row) in rowLookup {
            rowsByYear[row.month.year, default: []].append(row)
        }
        sections = rowsByYear
            .map { HomeMergedYearSection(year: $0.key, rows: $0.value.sorted { $0.month > $1.month }) }
            .sorted { $0.year > $1.year }
    }
}
