import XCTest
@testable import Watermelon

@MainActor
final class SelectionControllerTests: XCTestCase {
    // MARK: - Helpers

    private final class GateState {
        var selectable = true
        var remoteAllowed = true
        var sections: [HomeMergedYearSection] = []
    }

    private func makeController(state: GateState) -> HomeSelectionController {
        HomeSelectionController(hooks: HomeSelectionController.Hooks(
            isSelectable: { state.selectable },
            isRemoteSelectionAllowed: { state.remoteAllowed },
            sections: { state.sections }
        ))
    }

    private func row(_ year: Int, _ month: Int) -> HomeMonthRow {
        HomeMonthRow(month: LibraryMonthKey(year: year, month: month), local: nil, remote: nil)
    }

    private func section(year: Int, months: [Int]) -> HomeMergedYearSection {
        HomeMergedYearSection(year: year, rows: months.map { row(year, $0) })
    }

    // MARK: - toggleMonth

    func testToggleMonth_local_addsAndRemoves() {
        let state = GateState()
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertEqual(controller.state.localMonths, [key])

        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }

    func testToggleMonth_remote_addsAndRemoves() {
        let state = GateState()
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertTrue(controller.toggleMonth(key, side: .remote))
        XCTAssertEqual(controller.state.remoteMonths, [key])

        XCTAssertTrue(controller.toggleMonth(key, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    func testToggleMonth_blockedWhenNotSelectable() {
        let state = GateState()
        state.selectable = false
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleMonth(LibraryMonthKey(year: 2024, month: 1), side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }

    func testToggleMonth_remote_blockedWhenRemoteSelectionDisallowed_localStillWorks() {
        let state = GateState()
        state.remoteAllowed = false
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertFalse(controller.toggleMonth(key, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)

        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertEqual(controller.state.localMonths, [key])
    }

    // MARK: - toggleYear

    func testToggleYear_local_unionThenSubtract() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2, 3])]
        let controller = makeController(state: state)
        let allMonths: Set<LibraryMonthKey> = Set((1...3).map { LibraryMonthKey(year: 2024, month: $0) })

        XCTAssertTrue(controller.toggleYear(sectionIndex: 0, side: .local))
        XCTAssertEqual(controller.state.localMonths, allMonths, "first toggle: not a subset → union")

        XCTAssertTrue(controller.toggleYear(sectionIndex: 0, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty, "second toggle: subset → subtract")
    }

    func testToggleYear_outOfBounds_isNoOp() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleYear(sectionIndex: 5, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }

    func testToggleYear_remote_blockedWhenRemoteSelectionDisallowed() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2])]
        state.remoteAllowed = false
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleYear(sectionIndex: 0, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    // MARK: - toggleAll

    func testToggleAll_local_unionThenSubtract() {
        let state = GateState()
        state.sections = [
            section(year: 2024, months: [1, 2]),
            section(year: 2023, months: [11, 12])
        ]
        let controller = makeController(state: state)
        let allMonths: Set<LibraryMonthKey> = [
            LibraryMonthKey(year: 2024, month: 1),
            LibraryMonthKey(year: 2024, month: 2),
            LibraryMonthKey(year: 2023, month: 11),
            LibraryMonthKey(year: 2023, month: 12)
        ]

        XCTAssertTrue(controller.toggleAll(side: .local))
        XCTAssertEqual(controller.state.localMonths, allMonths)

        XCTAssertTrue(controller.toggleAll(side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }

    // MARK: - clear / intersect

    func testClear_emptiesBothSides() {
        let state = GateState()
        let controller = makeController(state: state)
        let k1 = LibraryMonthKey(year: 2024, month: 1)
        let k2 = LibraryMonthKey(year: 2024, month: 2)

        _ = controller.toggleMonth(k1, side: .local)
        _ = controller.toggleMonth(k2, side: .remote)
        controller.clear()

        XCTAssertTrue(controller.state.localMonths.isEmpty)
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    func testIntersect_trimsToVisibleMonths() {
        let state = GateState()
        let controller = makeController(state: state)
        let visible = LibraryMonthKey(year: 2024, month: 1)
        let goneLocal = LibraryMonthKey(year: 2024, month: 2)
        let goneRemote = LibraryMonthKey(year: 2024, month: 3)

        _ = controller.toggleMonth(visible, side: .local)
        _ = controller.toggleMonth(goneLocal, side: .local)
        _ = controller.toggleMonth(visible, side: .remote)
        _ = controller.toggleMonth(goneRemote, side: .remote)

        controller.intersect(with: [visible])
        XCTAssertEqual(controller.state.localMonths, [visible])
        XCTAssertEqual(controller.state.remoteMonths, [visible])
    }

    // MARK: - intent

    func testIntent_returnsBackupForLocal_complementForBoth() {
        let state = GateState()
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertNil(controller.intent(for: key))

        _ = controller.toggleMonth(key, side: .local)
        XCTAssertEqual(controller.intent(for: key), .backup)

        _ = controller.toggleMonth(key, side: .remote)
        XCTAssertEqual(controller.intent(for: key), .complement)
    }

    func testToggleAll_remote_blockedWhenRemoteSelectionDisallowed() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2])]
        state.remoteAllowed = false
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleAll(side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }
}
