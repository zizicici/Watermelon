import Foundation
import XCTest
@testable import Watermelon

final class LocalMediaSourceTests: XCTestCase {
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
