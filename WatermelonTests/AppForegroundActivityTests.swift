import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class AppForegroundActivityTests: XCTestCase {
    func testStartReflectsApplicationStateWithoutAResumeCallbackAndIsIdempotent() {
        var changes: [Bool] = []
        var foregroundCount = 0
        let activity = AppForegroundActivity(onForeground: { foregroundCount += 1 }) { changes.append($0) }
        XCTAssertFalse(activity.isActive)
        activity.start()
        activity.start()
        XCTAssertTrue(activity.isActive)
        XCTAssertEqual(changes, [true])
        XCTAssertEqual(foregroundCount, 0)
    }

    func testLifecycleTransitionsCoalesceAndInvalidateBeforeResuming() {
        var events: [String] = []
        let activity = AppForegroundActivity(onForeground: { events.append("invalidate") }) {
            events.append($0 ? "active" : "inactive")
        }
        activity.start()
        for _ in 0..<3 {
            NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        }
        XCTAssertEqual(events, ["active"])
        for _ in 0..<3 {
            NotificationCenter.default.post(name: UIApplication.willResignActiveNotification, object: nil)
        }
        XCTAssertFalse(activity.isActive)
        XCTAssertEqual(events, ["active", "inactive"])
        for _ in 0..<3 {
            NotificationCenter.default.post(name: UIApplication.didBecomeActiveNotification, object: nil)
        }
        XCTAssertTrue(activity.isActive)
        XCTAssertEqual(events, ["active", "inactive", "invalidate", "active"])
    }
}
