import Foundation

@MainActor
final class HomeSelectionController {
    struct Dependencies {
        var isSelectable: () -> Bool
        var isRemoteSelectionAllowed: () -> Bool
        var sections: () -> [HomeMergedYearSection]
    }

    private(set) var state = SelectionState()
    private let dependencies: Dependencies

    init(dependencies: Dependencies) {
        self.dependencies = dependencies
    }

    @discardableResult
    func toggleMonth(_ month: LibraryMonthKey, side: SelectionSide) -> Bool {
        guard dependencies.isSelectable() else { return false }
        switch side {
        case .local:
            if state.localMonths.contains(month) {
                state.localMonths.remove(month)
            } else {
                state.localMonths.insert(month)
            }
        case .remote:
            guard dependencies.isRemoteSelectionAllowed() else { return false }
            if state.remoteMonths.contains(month) {
                state.remoteMonths.remove(month)
            } else {
                state.remoteMonths.insert(month)
            }
        }
        return true
    }

    @discardableResult
    func toggleYear(sectionIndex: Int, side: SelectionSide) -> Bool {
        guard dependencies.isSelectable() else { return false }
        let sections = dependencies.sections()
        guard sectionIndex < sections.count else { return false }
        let allMonths = Set(sections[sectionIndex].rows.map(\.month))
        switch side {
        case .local:
            if allMonths.isSubset(of: state.localMonths) {
                state.localMonths.subtract(allMonths)
            } else {
                state.localMonths.formUnion(allMonths)
            }
        case .remote:
            guard dependencies.isRemoteSelectionAllowed() else { return false }
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
        guard dependencies.isSelectable() else { return false }
        let allMonths = Set(dependencies.sections().flatMap { $0.rows.map(\.month) })
        switch side {
        case .local:
            if allMonths.isSubset(of: state.localMonths) {
                state.localMonths.removeAll()
            } else {
                state.localMonths = allMonths
            }
        case .remote:
            guard dependencies.isRemoteSelectionAllowed() else { return false }
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

    func intersect(with months: Set<LibraryMonthKey>) {
        state.localMonths.formIntersection(months)
        state.remoteMonths.formIntersection(months)
    }

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        state.intent(for: month)
    }
}
