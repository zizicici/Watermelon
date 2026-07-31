import Foundation
import XCTest
@testable import WatermelonMac

@MainActor
final class MacExecutionPowerActivityTests: XCTestCase {
    private final class ActivityToken: NSObject {}

    override func setUp() {
        super.setUp()
        AppRuntimeFlags._testReset()
    }

    override func tearDown() {
        AppRuntimeFlags._testReset()
        super.tearDown()
    }

    func testExecutionLeaseBeginsAndEndsPowerActivity() {
        let flags = AppRuntimeFlags()
        let token = ActivityToken()
        var begins: [(ProcessInfo.ActivityOptions, String)] = []
        var endedTokens: [NSObjectProtocol] = []
        let activity = makeActivity(
            flags: flags,
            token: token,
            begins: { begins.append($0) },
            ends: { endedTokens.append($0) }
        )

        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertEqual(begins.count, 1)
        XCTAssertTrue(
            begins[0].0.contains(.idleSystemSleepDisabled)
        )
        XCTAssertTrue(begins[0].0.contains(.userInitiated))
        XCTAssertFalse(begins[0].1.isEmpty)

        flags.exitExecution()
        XCTAssertEqual(endedTokens.count, 1)
        XCTAssertTrue(endedTokens[0] === token)
        activity.invalidate()
    }

    func testDuplicateLifecycleNotificationsAreIdempotent() {
        let flags = AppRuntimeFlags()
        let token = ActivityToken()
        var beginCount = 0
        var endCount = 0
        let activity = makeActivity(
            flags: flags,
            token: token,
            begins: { _ in beginCount += 1 },
            ends: { _ in endCount += 1 }
        )

        XCTAssertTrue(flags.tryEnterExecution())
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: flags
        )
        XCTAssertEqual(beginCount, 1)

        flags.exitExecution()
        NotificationCenter.default.post(
            name: .ExecutionLifecycleDidChange,
            object: flags
        )
        XCTAssertEqual(endCount, 1)
        activity.invalidate()
    }

    func testInvalidateEndsActivityAndStopsObserving() {
        let flags = AppRuntimeFlags()
        let token = ActivityToken()
        var beginCount = 0
        var endCount = 0
        let activity = makeActivity(
            flags: flags,
            token: token,
            begins: { _ in beginCount += 1 },
            ends: { _ in endCount += 1 }
        )

        XCTAssertTrue(flags.tryEnterExecution())
        activity.invalidate()
        activity.invalidate()
        XCTAssertEqual(beginCount, 1)
        XCTAssertEqual(endCount, 1)

        flags.exitExecution()
        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertEqual(beginCount, 1)
        flags.exitExecution()
    }

    private func makeActivity(
        flags: AppRuntimeFlags,
        token: ActivityToken,
        begins: @escaping (
            (ProcessInfo.ActivityOptions, String)
        ) -> Void,
        ends: @escaping (NSObjectProtocol) -> Void
    ) -> MacExecutionPowerActivity {
        MacExecutionPowerActivity(
            appRuntimeFlags: flags,
            beginActivity: { options, reason in
                begins((options, reason))
                return token
            },
            endActivity: ends
        )
    }
}
