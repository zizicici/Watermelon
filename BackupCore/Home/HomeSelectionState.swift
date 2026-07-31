import Foundation

enum SelectionSide {
    case local
    case remote
}

struct SelectionState {
    var localMonths = Set<LibraryMonthKey>()
    var remoteMonths = Set<LibraryMonthKey>()

    var isEmpty: Bool {
        localMonths.isEmpty && remoteMonths.isEmpty
    }

    mutating func clear() {
        localMonths.removeAll()
        remoteMonths.removeAll()
    }

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        switch (localMonths.contains(month), remoteMonths.contains(month)) {
        case (true, false):
            return .backup
        case (false, true):
            return .download
        case (true, true):
            return .complement
        case (false, false):
            return nil
        }
    }

    func selectionState(
        for months: Set<LibraryMonthKey>,
        side: SelectionSide
    ) -> HomeSelectionState {
        let selected = side == .local ? localMonths : remoteMonths
        guard !months.isEmpty else { return .none }
        if months.isSubset(of: selected) { return .all }
        if !months.isDisjoint(with: selected) { return .partial }
        return .none
    }

    static func selectableMonths(
        in rows: [HomeMonthRow],
        side: SelectionSide
    ) -> Set<LibraryMonthKey> {
        switch side {
        case .local:
            return Set(rows.lazy.filter { $0.local != nil }.map(\.month))
        case .remote:
            return Set(rows.lazy.filter { $0.remote != nil }.map(\.month))
        }
    }

    func selectionState(
        forRows rows: [HomeMonthRow],
        side: SelectionSide
    ) -> HomeSelectionState {
        selectionState(
            for: Self.selectableMonths(in: rows, side: side),
            side: side
        )
    }

    func counts() -> (backup: Int, download: Int, complement: Int) {
        let allSelected = localMonths.union(remoteMonths)
        var backup = 0
        var download = 0
        var complement = 0
        for month in allSelected {
            switch intent(for: month) {
            case .backup:
                backup += 1
            case .download:
                download += 1
            case .complement:
                complement += 1
            case nil:
                break
            }
        }
        return (backup, download, complement)
    }

    func months(for targetIntent: MonthIntent) -> [LibraryMonthKey] {
        localMonths.union(remoteMonths)
            .filter { intent(for: $0) == targetIntent }
            .sorted()
    }

    func revalidated(
        backup: [LibraryMonthKey],
        download: [LibraryMonthKey],
        complement: [LibraryMonthKey]
    ) -> (
        backup: [LibraryMonthKey],
        download: [LibraryMonthKey],
        complement: [LibraryMonthKey]
    ) {
        (
            backup.filter { intent(for: $0) == .backup },
            download.filter { intent(for: $0) == .download },
            complement.filter { intent(for: $0) == .complement }
        )
    }
}
