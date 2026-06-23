import XCTest
@testable import Watermelon

@MainActor
final class SelectionControllerTests: XCTestCase {
    // MARK: - Helpers

    private final class GateState {
        var selectable = true
        var remoteAllowed = true
        var remoteReady = true
        var sections: [HomeMergedYearSection] = []
    }

    private func makeController(state: GateState) -> HomeSelectionController {
        HomeSelectionController(hooks: HomeSelectionController.Hooks(
            isSelectable: { state.selectable },
            isRemoteSelectionAllowed: { state.remoteAllowed },
            isRemoteReady: { state.remoteReady },
            sections: { state.sections }
        ))
    }

    private func summary(_ year: Int, _ month: Int) -> HomeMonthSummary {
        HomeMonthSummary(
            month: LibraryMonthKey(year: year, month: month),
            assetCount: 1, photoCount: 1, videoCount: 0, backedUpCount: nil, totalSizeBytes: nil
        )
    }

    private func row(_ year: Int, _ month: Int, local: Bool = true, remote: Bool = true) -> HomeMonthRow {
        HomeMonthRow(
            month: LibraryMonthKey(year: year, month: month),
            local: local ? summary(year, month) : nil,
            remote: remote ? summary(year, month) : nil
        )
    }

    private func section(year: Int, months: [Int], local: Bool = true, remote: Bool = true) -> HomeMergedYearSection {
        HomeMergedYearSection(year: year, rows: months.map { row(year, $0, local: local, remote: remote) })
    }

    // MARK: - toggleMonth

    func testToggleMonth_local_addsAndRemoves() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertEqual(controller.state.localMonths, [key])

        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }

    func testToggleMonth_remote_addsAndRemoves() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
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
        state.sections = [section(year: 2024, months: [1])]
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
        state.sections = [section(year: 2024, months: [1, 2])]
        let controller = makeController(state: state)
        let k1 = LibraryMonthKey(year: 2024, month: 1)
        let k2 = LibraryMonthKey(year: 2024, month: 2)

        _ = controller.toggleMonth(k1, side: .local)
        _ = controller.toggleMonth(k2, side: .remote)
        controller.clear()

        XCTAssertTrue(controller.state.localMonths.isEmpty)
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    func testIntersect_trimsEachSideToItsVisibleMonths() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2, 3])]
        let controller = makeController(state: state)
        let visible = LibraryMonthKey(year: 2024, month: 1)
        let goneLocal = LibraryMonthKey(year: 2024, month: 2)
        let goneRemote = LibraryMonthKey(year: 2024, month: 3)

        _ = controller.toggleMonth(visible, side: .local)
        _ = controller.toggleMonth(goneLocal, side: .local)
        _ = controller.toggleMonth(visible, side: .remote)
        _ = controller.toggleMonth(goneRemote, side: .remote)

        controller.intersect(localMonths: [visible], remoteMonths: [visible])
        XCTAssertEqual(controller.state.localMonths, [visible])
        XCTAssertEqual(controller.state.remoteMonths, [visible])
    }

    // Month survives via its local side; its remote summary is evicted → only the remote selection drops.
    func testIntersect_remoteSideEvicted_localSurvives_dropsOnlyRemote() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        let controller = makeController(state: state)
        let m = LibraryMonthKey(year: 2024, month: 1)

        _ = controller.toggleMonth(m, side: .local)
        _ = controller.toggleMonth(m, side: .remote)
        XCTAssertEqual(controller.intent(for: m), .complement)

        controller.intersect(localMonths: [m], remoteMonths: [])
        XCTAssertEqual(controller.state.localMonths, [m])
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
        XCTAssertEqual(controller.intent(for: m), .backup)
    }

    // intersect reports whether it changed the selection, so handleDataChange can emit .structural (which
    // re-renders the action panel + top toggles) instead of .data (which refreshes neither) after a trim.
    func testIntersect_returnsTrueWhenItTrims_falseWhenNoOp() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        let controller = makeController(state: state)
        let m = LibraryMonthKey(year: 2024, month: 1)

        _ = controller.toggleMonth(m, side: .local)
        _ = controller.toggleMonth(m, side: .remote)

        // No-op: both sides still present.
        XCTAssertFalse(controller.intersect(localMonths: [m], remoteMonths: [m]))
        // Trim: local side evicted while the month survives via remote.
        XCTAssertTrue(controller.intersect(localMonths: [], remoteMonths: [m]))
        // Idempotent: already trimmed → no further change.
        XCTAssertFalse(controller.intersect(localMonths: [], remoteMonths: [m]))
    }

    // Symmetric: month survives via its remote side; its local summary is evicted → only the local selection drops.
    func testIntersect_localSideEvicted_remoteSurvives_dropsOnlyLocal() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        let controller = makeController(state: state)
        let m = LibraryMonthKey(year: 2024, month: 1)

        _ = controller.toggleMonth(m, side: .local)
        _ = controller.toggleMonth(m, side: .remote)

        controller.intersect(localMonths: [], remoteMonths: [m])
        XCTAssertTrue(controller.state.localMonths.isEmpty)
        XCTAssertEqual(controller.state.remoteMonths, [m])
        XCTAssertEqual(controller.intent(for: m), .download)
    }

    // MARK: - revalidated (confirm-dialog capture vs live truth)

    // A backup month whose local side was reconciled away while the confirm dialog was open must be dropped,
    // so startExecution can't run it as a no-op that completes having uploaded nothing.
    func testRevalidated_dropsBackupMonthWhoseLocalSideGone() {
        var live = SelectionState()
        let kept = LibraryMonthKey(year: 2024, month: 1)
        live.localMonths = [kept]   // still a live backup month
        // `stale` was captured as backup but its local side is gone from the live selection.
        let stale = LibraryMonthKey(year: 2024, month: 2)

        let result = live.revalidated(backup: [kept, stale], download: [], complement: [])
        XCTAssertEqual(result.backup, [kept])
        XCTAssertTrue(result.download.isEmpty)
        XCTAssertTrue(result.complement.isEmpty)
    }

    // A complement month that lost its local side mid-dialog is no longer .complement (now .download), so it is
    // dropped from the captured complement list rather than executed against absent truth.
    func testRevalidated_dropsCapturedMonthWhoseLiveIntentChanged() {
        var live = SelectionState()
        let m = LibraryMonthKey(year: 2024, month: 3)
        live.remoteMonths = [m]   // live intent is now .download, not the captured .complement

        let result = live.revalidated(backup: [], download: [], complement: [m])
        XCTAssertTrue(result.complement.isEmpty)
        // It is not silently promoted into another category either.
        XCTAssertTrue(result.backup.isEmpty)
        XCTAssertTrue(result.download.isEmpty)
    }

    // A fully cleared live selection (e.g. connection dropped) drops every captured month → no execution.
    func testRevalidated_clearedSelectionDropsEverything() {
        let live = SelectionState()
        let result = live.revalidated(
            backup: [LibraryMonthKey(year: 2024, month: 1)],
            download: [LibraryMonthKey(year: 2024, month: 2)],
            complement: [LibraryMonthKey(year: 2024, month: 3)]
        )
        XCTAssertTrue(result.backup.isEmpty && result.download.isEmpty && result.complement.isEmpty)
    }

    // MARK: - intent

    func testIntent_returnsBackupForLocal_complementForBoth() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
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

    // MARK: - Side-presence gate (input cannot create a side without current truth)

    func testToggleMonth_remote_absentRemoteSide_isRejected() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1], local: true, remote: false)]
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertFalse(controller.toggleMonth(key, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
        // Local side has truth, so it remains selectable.
        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertEqual(controller.state.localMonths, [key])
    }

    func testToggleMonth_local_absentLocalSide_isRejected() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1], local: false, remote: true)]
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertFalse(controller.toggleMonth(key, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
        XCTAssertTrue(controller.toggleMonth(key, side: .remote))
        XCTAssertEqual(controller.state.remoteMonths, [key])
    }

    // MARK: - Remote readiness gate (no remote selection before the current connection's snapshot applies)

    // While connected but not yet remote-ready the grid still holds the prior connection's rows, so every remote
    // entry point (cell tap, year header, select-all) must refuse — local selection stays unaffected.
    func testToggleMonth_remote_blockedWhenRemoteNotReady_localStillWorks() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1])]
        state.remoteReady = false
        let controller = makeController(state: state)
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertFalse(controller.toggleMonth(key, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
        // Local is independent of remote readiness.
        XCTAssertTrue(controller.toggleMonth(key, side: .local))
        XCTAssertEqual(controller.state.localMonths, [key])
    }

    func testToggleYear_remote_blockedWhenRemoteNotReady() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2])]
        state.remoteReady = false
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleYear(sectionIndex: 0, side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    func testToggleAll_remote_blockedWhenRemoteNotReady() {
        let state = GateState()
        state.sections = [section(year: 2024, months: [1, 2])]
        state.remoteReady = false
        let controller = makeController(state: state)

        XCTAssertFalse(controller.toggleAll(side: .remote))
        XCTAssertTrue(controller.state.remoteMonths.isEmpty)
    }

    func testToggleMonth_unknownMonth_isRejected() {
        let state = GateState()
        let controller = makeController(state: state) // no sections → no row truth
        let key = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertFalse(controller.toggleMonth(key, side: .local))
        XCTAssertFalse(controller.toggleMonth(key, side: .remote))
        XCTAssertTrue(controller.state.isEmpty)
    }

    func testToggleYear_local_selectsOnlyLocalPresentMonths() {
        let state = GateState()
        state.sections = [HomeMergedYearSection(year: 2024, rows: [
            row(2024, 1, local: true, remote: true),
            row(2024, 2, local: false, remote: true)   // remote-only: must be skipped for a local toggle
        ])]
        let controller = makeController(state: state)

        XCTAssertTrue(controller.toggleYear(sectionIndex: 0, side: .local))
        XCTAssertEqual(controller.state.localMonths, [LibraryMonthKey(year: 2024, month: 1)])
    }

    func testToggleAll_remote_selectsOnlyRemotePresentMonths() {
        let state = GateState()
        state.sections = [HomeMergedYearSection(year: 2024, rows: [
            row(2024, 1, local: true, remote: false),   // local-only: must be skipped for a remote toggle
            row(2024, 2, local: true, remote: true)
        ])]
        let controller = makeController(state: state)

        XCTAssertTrue(controller.toggleAll(side: .remote))
        XCTAssertEqual(controller.state.remoteMonths, [LibraryMonthKey(year: 2024, month: 2)])
    }

    // MARK: - Side-aware indicator projection (denominator must match the side-eligible candidate set)

    func testSelectionState_forRows_local_ignoresRemoteOnlyRows_reportsAll() {
        var state = SelectionState()
        state.localMonths = [LibraryMonthKey(year: 2024, month: 1)]
        let rows = [
            row(2024, 1, local: true, remote: true),
            row(2024, 2, local: false, remote: true)   // remote-only: not local-eligible, excluded from the local denominator
        ]
        XCTAssertEqual(state.selectionState(forRows: rows, side: .local), .all)
    }

    func testSelectionState_forRows_remote_ignoresLocalOnlyRows_reportsAll() {
        var state = SelectionState()
        state.remoteMonths = [LibraryMonthKey(year: 2024, month: 1)]
        let rows = [
            row(2024, 1, local: true, remote: true),
            row(2024, 2, local: true, remote: false)   // local-only: not remote-eligible
        ]
        XCTAssertEqual(state.selectionState(forRows: rows, side: .remote), .all)
    }

    func testSelectionState_forRows_local_partialWhenSomeEligibleUnselected() {
        var state = SelectionState()
        state.localMonths = [LibraryMonthKey(year: 2024, month: 1)]
        let rows = [row(2024, 1), row(2024, 2)]   // both local-eligible, only one selected
        XCTAssertEqual(state.selectionState(forRows: rows, side: .local), .partial)
    }

    func testSelectionState_forRows_local_noEligibleRows_reportsNone() {
        let state = SelectionState()
        let rows = [row(2024, 1, local: false, remote: true)]
        XCTAssertEqual(state.selectionState(forRows: rows, side: .local), .none)
    }

    // A .data-path side-presence flip can change the global toggle's denominator without changing the selection
    // set: a month surviving via its opposite side is no-op for reconcile/intersect, yet its eviction shrinks the
    // local-eligible set so the only selected month becomes "all" — the render path must refresh the toggle on .data.
    func testSelectionState_forRows_local_denominatorShrinksWhenSiblingSideEvicted_flipsPartialToAll() {
        var state = SelectionState()
        state.localMonths = [LibraryMonthKey(year: 2024, month: 1)]   // only M1 selected; set is constant throughout

        let before = [row(2024, 1, local: true, remote: true), row(2024, 2, local: true, remote: true)]
        XCTAssertEqual(state.selectionState(forRows: before, side: .local), .partial)

        // M2's local side evicted (survives via remote). Selection set unchanged, denominator drops to {M1}.
        let after = [row(2024, 1, local: true, remote: true), row(2024, 2, local: false, remote: true)]
        XCTAssertEqual(state.selectionState(forRows: after, side: .local), .all)
    }

    // Symmetric grow case: a remote-only month gains a local side, so a select-all-local toggle must drop to partial.
    func testSelectionState_forRows_local_denominatorGrowsWhenSiblingSideAdded_flipsAllToPartial() {
        var state = SelectionState()
        state.localMonths = [LibraryMonthKey(year: 2024, month: 1)]

        let before = [row(2024, 1, local: true, remote: true), row(2024, 2, local: false, remote: true)]
        XCTAssertEqual(state.selectionState(forRows: before, side: .local), .all)

        let after = [row(2024, 1, local: true, remote: true), row(2024, 2, local: true, remote: true)]
        XCTAssertEqual(state.selectionState(forRows: after, side: .local), .partial)
    }

    // Mutation and projection agree: after a side toggle selects every eligible month, the indicator reads .all,
    // so the next tap is understood as "clear", not a trap that discards a complete selection.
    func testToggleYear_local_mixedSection_indicatorReportsAllThenClears() {
        let state = GateState()
        let mixed = HomeMergedYearSection(year: 2024, rows: [
            row(2024, 1, local: true, remote: true),
            row(2024, 2, local: false, remote: true)   // remote-only
        ])
        state.sections = [mixed]
        let controller = makeController(state: state)

        XCTAssertTrue(controller.toggleYear(sectionIndex: 0, side: .local))
        XCTAssertEqual(controller.state.localMonths, [LibraryMonthKey(year: 2024, month: 1)])
        XCTAssertEqual(controller.state.selectionState(forRows: mixed.rows, side: .local), .all)

        XCTAssertTrue(controller.toggleYear(sectionIndex: 0, side: .local))
        XCTAssertTrue(controller.state.localMonths.isEmpty)
    }
}
