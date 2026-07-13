import XCTest
@testable import Watermelon

private final class MonthAssetLoadCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    func bump() {
        lock.lock()
        value += 1
        lock.unlock()
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

final class BackupMonthScopeTests: XCTestCase {
    private var savedMonthGroupingTimeZoneRaw: String?

    // Gregorian calendar avoids masking non-Gregorian locale drift in repo month keys.
    private let calendar: Calendar = {
        var c = Calendar(identifier: .gregorian)
        c.timeZone = .current
        return c
    }()

    override func setUp() {
        super.setUp()
        savedMonthGroupingTimeZoneRaw = UserDefaults.standard.string(forKey: MonthGroupingTimeZonePreference.storageKey)
        UserDefaults.standard.removeObject(forKey: MonthGroupingTimeZonePreference.storageKey)
    }

    override func tearDown() {
        if let savedMonthGroupingTimeZoneRaw {
            UserDefaults.standard.set(savedMonthGroupingTimeZoneRaw, forKey: MonthGroupingTimeZonePreference.storageKey)
        } else {
            UserDefaults.standard.removeObject(forKey: MonthGroupingTimeZonePreference.storageKey)
        }
        savedMonthGroupingTimeZoneRaw = nil
        super.tearDown()
    }

    private func date(_ year: Int, _ month: Int, _ day: Int) -> Date {
        var comps = DateComponents()
        comps.year = year
        comps.month = month
        comps.day = day
        return calendar.date(from: comps)!
    }

    private func isoDate(_ text: String) -> Date {
        ISO8601DateFormatter().date(from: text)!
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

    func testMonthKeyUsesConfiguredGroupingTimeZone() {
        let date = isoDate("2026-06-30T20:30:00Z")

        MonthGroupingTimeZonePreference.setCurrent(MonthGroupingTimeZonePreference.fixedUTC())
        XCTAssertEqual(LibraryMonthKey.from(date: date), LibraryMonthKey(year: 2026, month: 6))

        MonthGroupingTimeZonePreference.setCurrent(MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "Indian/Maldives",
            fallbackOffsetSeconds: 18_000
        ))
        XCTAssertEqual(LibraryMonthKey.from(date: date), LibraryMonthKey(year: 2026, month: 7))
    }

    func testInvalidCreationDatesUseScannableEpochMonth() {
        var utc = Calendar(identifier: .gregorian)
        utc.timeZone = TimeZone(secondsFromGMT: 0)!
        let invalid: [Date?] = [
            nil,
            Date(timeIntervalSince1970: .nan),
            Date(timeIntervalSince1970: .infinity),
            Date(timeIntervalSince1970: -.infinity),
            Date(timeIntervalSince1970: -1_000_000_000_000),
            Date(timeIntervalSince1970: 1_000_000_000_000)
        ]

        for candidate in invalid {
            let month = AssetProcessor.monthKey(for: candidate, calendar: utc)
            XCTAssertEqual(month, LibraryMonthKey(year: 1970, month: 1))
            XCTAssertEqual(
                RepoLayoutLite.month(fromFilename: RepoLayoutLite.monthFilename(month: month)),
                month
            )
        }
    }

    func testMonthGroupingTimeZonePreferenceAcceptsMissingVersion() {
        UserDefaults.standard.set(
            #"{"mode":"fixedOffset","offsetSeconds":0}"#,
            forKey: MonthGroupingTimeZonePreference.storageKey
        )

        XCTAssertEqual(MonthGroupingTimeZonePreference.current, .fixedUTC())
    }

    func testInvalidFixedIanaWithoutFallbackDefaultsToSystem() {
        UserDefaults.standard.set(
            #"{"mode":"fixedIana","identifier":"Not/AZone"}"#,
            forKey: MonthGroupingTimeZonePreference.storageKey
        )

        XCTAssertEqual(MonthGroupingTimeZonePreference.current, .defaultPreference)
    }

    func testFixedIanaEqualityIgnoresFallbackWhenIdentifierIsValid() {
        let summer = MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "America/New_York",
            fallbackOffsetSeconds: -14_400
        ).normalized()
        let winter = MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "America/New_York",
            fallbackOffsetSeconds: -18_000
        ).normalized()
        let pacific = MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "America/Los_Angeles",
            fallbackOffsetSeconds: -28_800
        ).normalized()

        XCTAssertEqual(summer, winter)
        XCTAssertEqual(Set([summer, winter]).count, 1)
        XCTAssertNotEqual(summer, pacific)

        let legacySummer = MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "Legacy/New_York",
            fallbackOffsetSeconds: -14_400
        ).normalized()
        let legacyWinter = MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "Legacy/New_York",
            fallbackOffsetSeconds: -18_000
        ).normalized()
        XCTAssertNotEqual(legacySummer, legacyWinter)
    }

    func testSystemMonthGroupingTimeZoneFreezesToCurrentIanaZone() {
        let frozen = MonthGroupingTimeZonePreference.defaultPreference.frozen(at: date(2026, 6, 18))

        XCTAssertEqual(frozen.mode, .fixedIana)
        XCTAssertEqual(frozen.identifier, TimeZone.current.identifier)
    }

    func testRecentMonthScopeUsesConfiguredGroupingTimeZone() {
        let now = isoDate("2026-06-30T20:30:00Z")

        MonthGroupingTimeZonePreference.setCurrent(MonthGroupingTimeZonePreference.fixedUTC())
        let utcScope = BackupRunPreparationService.resolveMonthScope(.recentMonths(1), now: now)
        XCTAssertEqual(utcScope?.months, [LibraryMonthKey(year: 2026, month: 6)])
        XCTAssertEqual(utcScope?.cutoff, isoDate("2026-06-01T00:00:00Z"))

        MonthGroupingTimeZonePreference.setCurrent(MonthGroupingTimeZonePreference(
            mode: .fixedIana,
            identifier: "Indian/Maldives",
            fallbackOffsetSeconds: 18_000
        ))
        let maldivesScope = BackupRunPreparationService.resolveMonthScope(.recentMonths(1), now: now)
        XCTAssertEqual(maldivesScope?.months, [LibraryMonthKey(year: 2026, month: 7)])
        XCTAssertEqual(maldivesScope?.cutoff, isoDate("2026-06-30T19:00:00Z"))
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

    func testMonthAssetCacheDefersAndReusesPhotoFetch() async {
        let month = LibraryMonthKey(year: 2026, month: 7)
        let expected = [month: ["asset-a", "asset-b"]]
        let counter = MonthAssetLoadCounter()
        let cache = BackupMonthAssetIDsCache {
            counter.bump()
            return expected
        }

        XCTAssertEqual(counter.count, 0)
        let first = await cache.load()
        let second = await cache.load()

        XCTAssertEqual(first, expected)
        XCTAssertEqual(second, expected)
        XCTAssertEqual(counter.count, 1)
    }

    func testMissingMonthAssetProviderFetchesOnce() async {
        let month = LibraryMonthKey(year: 2026, month: 7)
        let expected = [month: ["asset-a"]]
        var fetchCount = 0

        let resolved = await BackupRunPreparationService.resolveMonthAssetIDsByMonth(
            provider: nil
        ) {
            fetchCount += 1
            return expected
        }

        XCTAssertEqual(resolved, expected)
        XCTAssertEqual(fetchCount, 1)
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
