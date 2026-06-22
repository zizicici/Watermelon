import Foundation

@MainActor
final class HomeSelectionController {
    struct Hooks {
        var isSelectable: () -> Bool
        var isRemoteSelectionAllowed: () -> Bool
        var sections: () -> [HomeMergedYearSection]
    }

    private(set) var state = SelectionState()
    private let hooks: Hooks

    init(hooks: Hooks) {
        self.hooks = hooks
    }

    @discardableResult
    func toggleMonth(_ month: LibraryMonthKey, side: SelectionSide) -> Bool {
        guard hooks.isSelectable() else { return false }
        switch side {
        case .local:
            if state.localMonths.contains(month) {
                state.localMonths.remove(month)
            } else {
                guard rowHasSide(month, .local) else { return false }
                state.localMonths.insert(month)
            }
        case .remote:
            guard hooks.isRemoteSelectionAllowed() else { return false }
            if state.remoteMonths.contains(month) {
                state.remoteMonths.remove(month)
            } else {
                guard rowHasSide(month, .remote) else { return false }
                state.remoteMonths.insert(month)
            }
        }
        return true
    }

    @discardableResult
    func toggleYear(sectionIndex: Int, side: SelectionSide) -> Bool {
        guard hooks.isSelectable() else { return false }
        let sections = hooks.sections()
        guard sectionIndex < sections.count else { return false }
        let allMonths = SelectionState.selectableMonths(in: sections[sectionIndex].rows, side: side)
        switch side {
        case .local:
            if allMonths.isSubset(of: state.localMonths) {
                state.localMonths.subtract(allMonths)
            } else {
                state.localMonths.formUnion(allMonths)
            }
        case .remote:
            guard hooks.isRemoteSelectionAllowed() else { return false }
            if allMonths.isSubset(of: state.remoteMonths) {
                state.remoteMonths.subtract(allMonths)
            } else {
                state.remoteMonths.formUnion(allMonths)
            }
        }
        return true
    }

    @discardableResult
    func toggleAll(side: SelectionSide) -> Bool {
        guard hooks.isSelectable() else { return false }
        let allMonths = SelectionState.selectableMonths(in: hooks.sections().flatMap(\.rows), side: side)
        switch side {
        case .local:
            if allMonths.isSubset(of: state.localMonths) {
                state.localMonths.removeAll()
            } else {
                state.localMonths = allMonths
            }
        case .remote:
            guard hooks.isRemoteSelectionAllowed() else { return false }
            if allMonths.isSubset(of: state.remoteMonths) {
                state.remoteMonths.removeAll()
            } else {
                state.remoteMonths = allMonths
            }
        }
        return true
    }

    func clear() {
        state.clear()
    }

    // Side-aware: a month surviving via the opposite side must not keep a stale selection on the evicted side.
    func intersect(localMonths: Set<LibraryMonthKey>, remoteMonths: Set<LibraryMonthKey>) {
        state.localMonths.formIntersection(localMonths)
        state.remoteMonths.formIntersection(remoteMonths)
    }

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        state.intent(for: month)
    }

    // A side may only be selected where that side has current truth, else it executes as a no-op backup/download.
    private func rowHasSide(_ month: LibraryMonthKey, _ side: SelectionSide) -> Bool {
        guard let row = hooks.sections().lazy.flatMap(\.rows).first(where: { $0.month == month }) else {
            return false
        }
        switch side {
        case .local:  return row.local != nil
        case .remote: return row.remote != nil
        }
    }
}
