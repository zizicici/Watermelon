import XCTest
@testable import WatermelonMac

final class MacRemoteThumbnailRequestGateTests: XCTestCase {
    func testBoundsConcurrentRequests() async {
        let gate = MacRemoteThumbnailRequestGate(limit: 2)
        guard let first = await gate.wait(),
              let second = await gate.wait() else {
            XCTFail("Expected initial permits")
            return
        }
        let thirdTask = Task {
            await gate.wait()
        }
        guard await waitForPendingCount(1, gate: gate) else {
            XCTFail("Expected third request to wait")
            return
        }

        await gate.release(first)
        guard let third = await thirdTask.value else {
            XCTFail("Expected queued permit")
            return
        }
        await gate.release(second)
        await gate.release(third)
    }

    func testCancellationRemovesQueuedRequest() async {
        let gate = MacRemoteThumbnailRequestGate(limit: 1)
        guard let first = await gate.wait() else {
            XCTFail("Expected initial permit")
            return
        }
        let queuedTask = Task {
            await gate.wait()
        }
        guard await waitForPendingCount(1, gate: gate) else {
            XCTFail("Expected queued request")
            return
        }

        queuedTask.cancel()
        let cancelledPermit = await queuedTask.value
        let pendingCount = await gate.pendingCount
        XCTAssertNil(cancelledPermit)
        XCTAssertEqual(pendingCount, 0)
        await gate.release(first)

        let next = await gate.wait()
        XCTAssertNotNil(next)
        if let next {
            await gate.release(next)
        }
    }

    func testShutdownRejectsQueuedAndFutureRequests() async {
        let gate = MacRemoteThumbnailRequestGate(limit: 1)
        guard let first = await gate.wait() else {
            XCTFail("Expected initial permit")
            return
        }
        let queuedTask = Task {
            await gate.wait()
        }
        guard await waitForPendingCount(1, gate: gate) else {
            XCTFail("Expected queued request")
            return
        }

        await gate.shutdown()
        let queuedPermit = await queuedTask.value
        let futurePermit = await gate.wait()
        XCTAssertNil(queuedPermit)
        XCTAssertNil(futurePermit)
        await gate.release(first)
    }

    private func waitForPendingCount(
        _ expected: Int,
        gate: MacRemoteThumbnailRequestGate
    ) async -> Bool {
        for _ in 0..<1_000 {
            if await gate.pendingCount == expected {
                return true
            }
            await Task.yield()
        }
        return false
    }
}
