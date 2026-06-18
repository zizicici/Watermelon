import XCTest
@testable import Watermelon

final class BackupMonthScopeTests: XCTestCase {
    // Gregorian + current tz, matching LibraryMonthKey.from(date:). Using Calendar.current here would mask a
    // non-Gregorian-locale divergence between the scope's month keys and the repo's Gregorian keys.
    private let calendar: Calendar = {
        var c = Calendar(identifier: .gregorian)
        c.timeZone = .current
        return c
    }()

    private func date(_ year: Int, _ month: Int, _ day: Int) -> Date {
        var comps = DateComponents()
        comps.year = year
        comps.month = month
        comps.day = day
        return calendar.date(from: comps)!
    }

    func testAllScopeReturnsNil() {
        XCTAssertNil(BackupRunPreparationService.resolveMonthScope(.all))
    }

    func testRecentTwoMonthsMidYear() {
        let resolved = BackupRunPreparationService.resolveMonthScope(.recentMonths(2), now: date(2026, 6, 18))
        let scope = try? XCTUnwrap(resolved)
        XCTAssertEqual(scope?.months, [LibraryMonthKey(year: 2026, month: 5), LibraryMonthKey(year: 2026, month: 6)])
        // Cutoff is the first instant of the earliest in-scope month.
        XCTAssertEqual(scope?.cutoff, date(2026, 5, 1))
    }

    func testRecentTwoMonthsCrossesYearBoundary() {
        let resolved = BackupRunPreparationService.resolveMonthScope(.recentMonths(2), now: date(2026, 1, 15))
        XCTAssertEqual(resolved?.months, [LibraryMonthKey(year: 2025, month: 12), LibraryMonthKey(year: 2026, month: 1)])
        XCTAssertEqual(resolved?.cutoff, date(2025, 12, 1))
    }

    func testRecentSingleMonthIsCurrentMonthOnly() {
        let resolved = BackupRunPreparationService.resolveMonthScope(.recentMonths(1), now: date(2026, 6, 18))
        XCTAssertEqual(resolved?.months, [LibraryMonthKey(year: 2026, month: 6)])
        XCTAssertEqual(resolved?.cutoff, date(2026, 6, 1))
    }

    func testNonPositiveCountClampsToSingleMonth() {
        let resolved = BackupRunPreparationService.resolveMonthScope(.recentMonths(0), now: date(2026, 6, 18))
        XCTAssertEqual(resolved?.months, [LibraryMonthKey(year: 2026, month: 6)])
    }

    // Retry (explicit asset IDs) targets specific assets regardless of month: month scope must be ignored so
    // requested targets are never dropped. This is the release-safe contract, not a debug-only assert.
    func testExplicitAssetTargetingIgnoresMonthScope() {
        XCTAssertNil(
            BackupRunPreparationService.resolveMonthScope(.recentMonths(2), targetsExplicitAssets: true, now: date(2026, 6, 18)),
            "explicit asset targeting must ignore month scope"
        )
        // Sanity: same scope without explicit-asset targeting resolves normally.
        XCTAssertEqual(
            BackupRunPreparationService.resolveMonthScope(.recentMonths(2), targetsExplicitAssets: false, now: date(2026, 6, 18))?.months,
            [LibraryMonthKey(year: 2026, month: 5), LibraryMonthKey(year: 2026, month: 6)]
        )
    }

    // Background runs single-worker under BG-task expiration: the newest month must run first so an older,
    // larger month can't starve it. .balanced would order the larger month first.
    func testNewestMonthFirstOrderingIgnoresSizeAndAssetCount() {
        let older = LibraryMonthKey(year: 2026, month: 5)
        let newer = LibraryMonthKey(year: 2026, month: 6)
        let byMonth: [MonthKey: [String]] = [older: ["a", "b", "c"], newer: ["d"]]
        let bytes: [MonthKey: Int64] = [older: 9_000, newer: 1]

        let newestFirst = BackupMonthScheduler.buildMonthPlans(
            assetLocalIdentifiersByMonth: byMonth, estimatedBytesByMonth: bytes, ordering: .newestMonthFirst
        )
        XCTAssertEqual(newestFirst.map(\.month), [newer, older])

        // .balanced keeps the throughput-oriented largest-first order — the older/larger month leads.
        let balanced = BackupMonthScheduler.buildMonthPlans(
            assetLocalIdentifiersByMonth: byMonth, estimatedBytesByMonth: bytes, ordering: .balanced
        )
        XCTAssertEqual(balanced.map(\.month), [older, newer])
    }
}
