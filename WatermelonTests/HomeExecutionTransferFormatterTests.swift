import XCTest
@testable import Watermelon

final class HomeExecutionTransferFormatterTests: XCTestCase {
    func testInvalidSpeedValuesAreRejected() {
        XCTAssertNil(HomeExecutionTransferFormatter.speed(.nan))
        XCTAssertNil(HomeExecutionTransferFormatter.speed(.infinity))
        XCTAssertNil(HomeExecutionTransferFormatter.speed(.greatestFiniteMagnitude))
    }

    func testInvalidRemainingTimeValuesAreRejected() {
        XCTAssertNil(HomeExecutionTransferFormatter.remainingTime(.nan))
        XCTAssertNil(HomeExecutionTransferFormatter.remainingTime(.infinity))
    }
}
