import XCTest
@testable import Watermelon

@MainActor
final class SectionBuilderTests: XCTestCase {
    private final class State {
        var rows: [LibraryMonthKey: HomeMonthRow] = [:]
    }

    private func makeBuilder(state: State) -> HomeSectionBuilder {
        HomeSectionBuilder(hooks: HomeSectionBuilder.Hooks(
            allMonthRows: { state.rows },
            monthRow: { state.rows[$0] ?? HomeMonthRow(month: $0, local: nil, remote: nil) }
        ))
    }

    private func row(_ year: Int, _ month: Int, localCount: Int = 1, size: Int64? = nil) -> HomeMonthRow {
        HomeMonthRow(
            month: LibraryMonthKey(year: year, month: month),
            local: HomeMonthSummary(
                month: LibraryMonthKey(year: year, month: month),
                assetCount: localCount,
                photoCount: localCount,
                videoCount: 0,
                backedUpCount: nil,
                totalSizeBytes: size
            ),
            remote: nil
        )
    }

    func testRebuildAll_groupsByYear_descending() {
        let state = State()
        state.rows = [
            LibraryMonthKey(year: 2023, month: 12): row(2023, 12),
            LibraryMonthKey(year: 2024, month: 1): row(2024, 1),
            LibraryMonthKey(year: 2024, month: 3): row(2024, 3)
        ]
        let builder = makeBuilder(state: state)

        builder.rebuildAll()

        XCTAssertEqual(builder.sections.map(\.year), [2024, 2023])
        XCTAssertEqual(builder.sections[0].rows.map(\.month), [
            LibraryMonthKey(year: 2024, month: 3),
            LibraryMonthKey(year: 2024, month: 1)
        ])
        XCTAssertEqual(builder.sections[1].rows.map(\.month), [LibraryMonthKey(year: 2023, month: 12)])
    }

    func testRebuildAll_emptySource_clearsSections() {
        let state = State()
        state.rows = [LibraryMonthKey(year: 2024, month: 1): row(2024, 1)]
        let builder = makeBuilder(state: state)
        builder.rebuildAll()
        XCTAssertEqual(builder.sections.count, 1)

        state.rows = [:]
        builder.rebuildAll()
        XCTAssertTrue(builder.sections.isEmpty)
        XCTAssertTrue(builder.rowLookup.isEmpty)
    }

    func testRefreshFileSizeRows_skipsMonthsNotInLookup() {
        let state = State()
        let visible = LibraryMonthKey(year: 2024, month: 1)
        let invisible = LibraryMonthKey(year: 2024, month: 2)
        state.rows = [visible: row(2024, 1, size: 100)]
        let builder = makeBuilder(state: state)
        builder.rebuildAll()

        state.rows[visible] = row(2024, 1, size: 200)
        let changed = builder.refreshFileSizeRows(for: [visible, invisible])

        XCTAssertEqual(changed, [visible])
        XCTAssertEqual(builder.rowLookup[visible]?.local?.totalSizeBytes, 200)
    }

    func testRefreshFileSizeRows_dropsMonthsThatBecameEmpty() {
        let state = State()
        let key = LibraryMonthKey(year: 2024, month: 1)
        state.rows = [key: row(2024, 1, size: 100)]
        let builder = makeBuilder(state: state)
        builder.rebuildAll()

        state.rows[key] = HomeMonthRow(month: key, local: nil, remote: nil)
        let changed = builder.refreshFileSizeRows(for: [key])

        XCTAssertTrue(changed.isEmpty, "row with no local+remote summary is not propagated as a change")
    }

    func testUpdateRowsAndRebuild_refreshesLookupAndSections() {
        let state = State()
        let key = LibraryMonthKey(year: 2024, month: 1)
        state.rows = [key: row(2024, 1, localCount: 1)]
        let builder = makeBuilder(state: state)
        builder.rebuildAll()
        XCTAssertEqual(builder.rowLookup[key]?.local?.assetCount, 1)

        state.rows[key] = row(2024, 1, localCount: 5)
        builder.updateRowsAndRebuild(for: [key])

        XCTAssertEqual(builder.rowLookup[key]?.local?.assetCount, 5)
        XCTAssertEqual(builder.sections.count, 1)
    }
}
