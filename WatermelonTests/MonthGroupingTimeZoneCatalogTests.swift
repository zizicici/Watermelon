import Foundation
import XCTest
@testable import Watermelon

final class MonthGroupingTimeZoneCatalogTests: XCTestCase {
    func testPrimaryIdentifiersAreRecognizedByFoundation() {
        let invalid = MonthGroupingTimeZoneCatalog.primaryIdentifiers.filter {
            TimeZone(identifier: $0) == nil
        }

        XCTAssertEqual(invalid, [])
    }

    func testPrimaryIdentifiersPreferMainGeographicIanaNames() {
        XCTAssertTrue(MonthGroupingTimeZoneCatalog.primaryIdentifiers.contains("Asia/Shanghai"))
        XCTAssertTrue(MonthGroupingTimeZoneCatalog.primaryIdentifiers.contains("Asia/Kolkata"))
        XCTAssertTrue(MonthGroupingTimeZoneCatalog.primaryIdentifiers.contains("Indian/Maldives"))
        XCTAssertFalse(MonthGroupingTimeZoneCatalog.primaryIdentifiers.contains("Asia/Calcutta"))
        XCTAssertFalse(MonthGroupingTimeZoneCatalog.primaryIdentifiers.contains("GMT"))
    }

    func testSelectableIdentifiersKeepRecognizedExtrasWithoutDuplicates() {
        let identifiers = MonthGroupingTimeZoneCatalog.selectableIdentifiers(adding: [
            "Asia/Calcutta",
            "Not/AZone",
            "Asia/Shanghai"
        ])

        XCTAssertTrue(identifiers.contains("Asia/Calcutta"))
        XCTAssertFalse(identifiers.contains("Not/AZone"))
        XCTAssertEqual(identifiers.filter { $0 == "Asia/Shanghai" }.count, 1)
    }
}
