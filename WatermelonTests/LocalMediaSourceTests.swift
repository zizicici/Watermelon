import Foundation
import XCTest
@testable import Watermelon

final class LocalMediaSourceTests: XCTestCase {
    func testHomeSeedProjectionGroupsSortsAndAppliesPresence() throws {
        let january = LibraryMonthKey(year: 2026, month: 1)
        let february = LibraryMonthKey(year: 2026, month: 2)
        let backedUp = Data([0x01])
        let seed = HomeBrowserLocalSeed(
            localIDByFingerprint: [backedUp: "jan-new"],
            assets: [
                HomeBrowserLocalAsset(
                    localIdentifier: "jan-old",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "feb",
                    month: february,
                    kind: .video,
                    creationDateMs: 3_000,
                    fingerprint: Data([0x02])
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "jan-new",
                    month: january,
                    kind: .livePhoto,
                    creationDateMs: 2_000,
                    fingerprint: backedUp
                ),
            ],
            monthGroupingTimeZone: .frozenCurrent()
        )

        let sections = try XCTUnwrap(LocalMediaSource.sections(
            from: seed,
            backedUpFingerprints: [backedUp]
        ))

        XCTAssertEqual(sections.map(\.month), [february, january])
        XCTAssertEqual(sections[1].items.map(\.id), ["jan-new", "jan-old"])
        XCTAssertEqual(sections[1].items[0].presence, .both)
        XCTAssertEqual(sections[1].items[1].presence, .localOnly)
        XCTAssertEqual(sections[0].items[0].presence, .localOnly)
        if case .livePhoto = sections[1].items[0].kind {
        } else {
            XCTFail("Expected live photo kind")
        }
    }

    func testHomeSeedProjectionUsesStableIdentifierTieBreak() throws {
        let january = LibraryMonthKey(year: 2026, month: 1)
        let seed = HomeBrowserLocalSeed(
            localIDByFingerprint: [:],
            assets: [
                HomeBrowserLocalAsset(
                    localIdentifier: "z",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
                HomeBrowserLocalAsset(
                    localIdentifier: "a",
                    month: january,
                    kind: .photo,
                    creationDateMs: 1_000,
                    fingerprint: nil
                ),
            ],
            monthGroupingTimeZone: .frozenCurrent()
        )

        let sections = try XCTUnwrap(LocalMediaSource.sections(
            from: seed,
            backedUpFingerprints: []
        ))

        XCTAssertEqual(sections[0].items.map(\.id), ["a", "z"])
    }

    func testMonthMemoMatchesCanonicalGroupingAcrossBoundaries() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "Asia/Kolkata")!
        let dates = [
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 3, day: 31))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 3, day: 1))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 2, day: 28))),
            try XCTUnwrap(calendar.date(from: DateComponents(year: 2026, month: 2, day: 1))),
        ]
        var memo = MediaBrowserMonthMemo(calendar: calendar)

        for date in dates {
            XCTAssertEqual(
                memo.month(for: date),
                LibraryMonthKey.from(date: date, calendar: calendar)
            )
        }
        XCTAssertEqual(memo.calculationCount, 2)
    }

    func testMissingCreationDateRemainsAbsentForManifests() {
        XCTAssertNil(LibraryCreationDate.optionalMilliseconds(nil))
    }
    func testCreationDateNormalizationPreservesValidDate() {
        let date = Date(timeIntervalSince1970: 1.25)

        let normalized = LibraryCreationDate.normalized(date)

        XCTAssertEqual(normalized.date, date)
        XCTAssertEqual(normalized.milliseconds, 1_250)
    }

    func testCreationDateNormalizationPreservesHistoricalDate() {
        let date = Date(timeIntervalSince1970: -3_786_825_600)

        let normalized = LibraryCreationDate.normalized(date)

        XCTAssertEqual(normalized.date, date)
        XCTAssertEqual(normalized.milliseconds, -3_786_825_600_000)
    }

    func testCreationDateNormalizationRejectsInvalidValues() {
        let candidates: [Date?] = [
            nil,
            Date(timeIntervalSince1970: .nan),
            Date(timeIntervalSince1970: .infinity),
            Date(timeIntervalSince1970: -.infinity),
            Date(timeIntervalSince1970: -1_000_000_000_000),
            Date(timeIntervalSince1970: 1_000_000_000_000)
        ]

        for candidate in candidates {
            let normalized = LibraryCreationDate.normalized(candidate)
            XCTAssertEqual(normalized.date.timeIntervalSince1970, 0)
            XCTAssertEqual(normalized.milliseconds, 0)
        }
    }

    func testCreationDateMillisecondsDoNotCrossMonthBoundary() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let marchStart = try XCTUnwrap(calendar.date(from: DateComponents(
            year: 2024,
            month: 3,
            day: 1
        )))
        let candidate = marchStart.addingTimeInterval(-0.0004)

        let normalized = LibraryCreationDate.normalized(candidate)
        let reconstructed = Date(timeIntervalSince1970: Double(normalized.milliseconds) / 1000)

        XCTAssertEqual(LibraryMonthKey.from(date: candidate, calendar: calendar), LibraryMonthKey(year: 2024, month: 2))
        XCTAssertEqual(LibraryMonthKey.from(date: reconstructed, calendar: calendar), LibraryMonthKey(year: 2024, month: 2))
    }

    func testHistoricalCreationDateUsesSharedFlooringRule() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let januaryStart = try XCTUnwrap(calendar.date(from: DateComponents(
            year: 1960,
            month: 1,
            day: 1
        )))
        let candidate = januaryStart.addingTimeInterval(-0.0004)

        let normalized = LibraryCreationDate.normalized(candidate)
        let reconstructed = Date(millisecondsSinceEpoch: normalized.milliseconds)

        XCTAssertEqual(normalized.milliseconds, candidate.millisecondsSinceEpoch)
        XCTAssertEqual(LibraryMonthKey.from(date: candidate, calendar: calendar), LibraryMonthKey(year: 1959, month: 12))
        XCTAssertEqual(LibraryMonthKey.from(date: reconstructed, calendar: calendar), LibraryMonthKey(year: 1959, month: 12))
    }
}
