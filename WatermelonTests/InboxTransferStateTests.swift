import XCTest
@testable import Watermelon

final class InboxTransferStateTests: XCTestCase {
    func testPauseGateReportsPauseAndResumesAtFileBoundary() async throws {
        let gate = InboxTransferPauseGate()
        let probe = PauseProbe()
        await gate.setPaused(true)

        let waiter = Task {
            try await gate.waitUntilResumed {
                await probe.markReached()
            }
        }
        for _ in 0 ..< 50 {
            if await probe.wasReached { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let reached = await probe.wasReached
        XCTAssertTrue(reached)
        await gate.setPaused(false)
        let didResumeFromPause = try await waiter.value
        let pausedAgain = try await gate.waitUntilResumed {}
        XCTAssertTrue(didResumeFromPause)
        XCTAssertFalse(pausedAgain)
    }
}

private actor PauseProbe {
    private(set) var wasReached = false

    func markReached() {
        wasReached = true
    }
}
