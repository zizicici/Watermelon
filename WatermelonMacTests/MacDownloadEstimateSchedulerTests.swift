import XCTest
@testable import WatermelonMac

private actor DownloadEstimateGate {
    private var continuation: CheckedContinuation<Int64?, Never>?
    private var pendingValue: Int64??
    private(set) var isWaiting = false

    func wait() async -> Int64? {
        isWaiting = true
        if let pendingValue {
            self.pendingValue = nil
            return pendingValue
        }
        return await withCheckedContinuation {
            continuation = $0
        }
    }

    func release(_ value: Int64?) {
        if let continuation {
            self.continuation = nil
            continuation.resume(returning: value)
        } else {
            pendingValue = value
        }
    }
}

@MainActor
final class MacDownloadEstimateSchedulerTests: XCTestCase {
    func testNewScheduleRejectsOlderResult() async {
        let scheduler = MacDownloadEstimateScheduler()
        let olderGate = DownloadEstimateGate()
        var values: [Int64?] = []

        scheduler.schedule(
            operation: {
                await olderGate.wait()
            },
            onValue: {
                values.append($0)
            }
        )
        for _ in 0 ..< 2_000 {
            if await olderGate.isWaiting {
                break
            }
            await Task.yield()
        }

        scheduler.schedule(
            operation: { 2 },
            onValue: {
                values.append($0)
            }
        )
        for _ in 0 ..< 2_000 where values.isEmpty {
            await Task.yield()
        }
        await olderGate.release(1)
        for _ in 0 ..< 100 {
            await Task.yield()
        }

        XCTAssertEqual(values.count, 1)
        XCTAssertEqual(values.map { $0 ?? -1 }, [2])
    }

    func testCancelRejectsOperationThatIgnoresCancellation() async {
        let scheduler = MacDownloadEstimateScheduler()
        let gate = DownloadEstimateGate()
        var values: [Int64?] = []

        scheduler.schedule(
            operation: {
                await gate.wait()
            },
            onValue: {
                values.append($0)
            }
        )
        for _ in 0 ..< 2_000 {
            if await gate.isWaiting {
                break
            }
            await Task.yield()
        }

        scheduler.cancel()
        await gate.release(1)
        for _ in 0 ..< 100 {
            await Task.yield()
        }

        XCTAssertTrue(values.isEmpty)
    }
}
