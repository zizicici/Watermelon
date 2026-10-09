import XCTest
@testable import Watermelon

@MainActor
final class DownloadMonthGateTests: XCTestCase {
    func testMonthsWaitAcrossSuspensionsAndEnterInOrder() async throws {
        let gate = DownloadMonthGate()
        try await gate.acquire(shouldDrain: { false })
        var order: [Int] = []
        let firstQueued = expectation(description: "first queued")
        let secondQueued = expectation(description: "second queued")
        let first = Task {
            firstQueued.fulfill()
            try await gate.acquire(shouldDrain: { false })
            defer { gate.release() }
            order.append(1)
            await Task.yield()
            XCTAssertEqual(order, [1])
        }
        await fulfillment(of: [firstQueued], timeout: 3)
        let second = Task {
            secondQueued.fulfill()
            try await gate.acquire(shouldDrain: { false })
            defer { gate.release() }
            order.append(2)
        }
        await fulfillment(of: [secondQueued], timeout: 3)
        XCTAssertTrue(order.isEmpty)
        gate.release()
        try await first.value
        try await second.value
        XCTAssertEqual(order, [1, 2])
    }

    func testPausedQueuedMonthExitsBeforeHolderFinishes() async throws {
        let gate = DownloadMonthGate()
        let drain = ExecutionTerminationControl()
        try await gate.acquire(shouldDrain: { false })
        let queued = expectation(description: "queued")
        let finished = expectation(description: "paused waiter finished")
        let task = Task {
            defer { finished.fulfill() }
            queued.fulfill()
            do {
                try await gate.acquire(shouldDrain: { drain.shouldDrain })
                gate.release()
                XCTFail("paused month entered")
            } catch is CancellationError {}
        }
        await fulfillment(of: [queued], timeout: 3)
        drain.request(.pause)
        await fulfillment(of: [finished], timeout: 3)
        gate.release()
        try await task.value
        try await gate.acquire(shouldDrain: { false })
        gate.release()
    }

    func testCancelledWaiterDoesNotConsumeNextPermit() async throws {
        let gate = DownloadMonthGate()
        try await gate.acquire(shouldDrain: { false })
        let queued = expectation(description: "queued")
        let finished = expectation(description: "cancelled waiter finished")
        let task = Task {
            defer { finished.fulfill() }
            queued.fulfill()
            do {
                try await gate.acquire(shouldDrain: { false })
                gate.release()
                XCTFail("cancelled month entered")
            } catch is CancellationError {}
        }
        await fulfillment(of: [queued], timeout: 3)
        task.cancel()
        await fulfillment(of: [finished], timeout: 3)
        gate.release()
        try await task.value
        try await gate.acquire(shouldDrain: { false })
        gate.release()
    }
}
