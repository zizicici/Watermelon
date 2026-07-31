import XCTest
@testable import WatermelonMac

final class RatingPromptEligibilityTests: XCTestCase {
    func testMissingDatabaseCreationDateIsNotEligible() {
        XCTAssertFalse(
            RatingPromptEligibility.isEligible(
                databaseCreationDate: nil,
                now: Date(timeIntervalSince1970: 1_000_000)
            )
        )
    }

    func testDatabaseYoungerThanSevenDaysIsNotEligible() {
        let now = Date(timeIntervalSince1970: 1_000_000)

        XCTAssertFalse(
            RatingPromptEligibility.isEligible(
                databaseCreationDate: now.addingTimeInterval(
                    -RatingPromptEligibility.minimumDatabaseAge + 1
                ),
                now: now
            )
        )
    }

    func testDatabaseBecomesEligibleAtSevenDays() {
        let now = Date(timeIntervalSince1970: 1_000_000)

        XCTAssertTrue(
            RatingPromptEligibility.isEligible(
                databaseCreationDate: now.addingTimeInterval(
                    -RatingPromptEligibility.minimumDatabaseAge
                ),
                now: now
            )
        )
    }
}
