import XCTest
@testable import WatermelonMac

final class MacExecutionLogSessionPolicyTests: XCTestCase {
    func testPreferredSessionSelectsExactURLOrNewestFallback() {
        let first = URL(fileURLWithPath: "/tmp/first.log")
        let second = URL(fileURLWithPath: "/tmp/second.log")

        XCTAssertEqual(
            MacExecutionLogSessionPolicy.preferredIndex(
                sessionURLs: [first, second],
                preferredURL: second
            ),
            1
        )
        XCTAssertEqual(
            MacExecutionLogSessionPolicy.preferredIndex(
                sessionURLs: [first, second],
                preferredURL: URL(
                    fileURLWithPath: "/tmp/missing.log"
                )
            ),
            0
        )
    }

    func testActiveSessionCannotBeDeleted() {
        let active = URL(fileURLWithPath: "/tmp/active.log")

        XCTAssertFalse(
            MacExecutionLogSessionPolicy.canDelete(
                sessionURL: active,
                activeSessionURL: active
            )
        )
        XCTAssertTrue(
            MacExecutionLogSessionPolicy.canDelete(
                sessionURL: URL(
                    fileURLWithPath: "/tmp/completed.log"
                ),
                activeSessionURL: active
            )
        )
    }
}
