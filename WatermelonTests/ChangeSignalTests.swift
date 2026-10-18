import XCTest
@testable import Watermelon

@MainActor
final class ChangeSignalTests: XCTestCase {
    func testBackgroundBurstDoesNotWaitForMainThreadAndCoalescesDelivery() async {
        let signal = ChangeSignal<Void>()
        let counter = Counter()
        let delivered = expectation(description: "One UI update")
        let token = signal.addCoalescedMainActorObserver {
            XCTAssertTrue(Thread.isMainThread)
            counter.value += 1
            delivered.fulfill()
        }
        defer { signal.removeObserver(token) }
        let finished = DispatchSemaphore(value: 0)
        DispatchQueue.global().async {
            for _ in 0..<1_000 { signal.publish() }
            finished.signal()
        }
        XCTAssertEqual(finished.wait(timeout: .now() + 2), .success)
        XCTAssertEqual(counter.value, 0)
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(counter.value, 1)
    }

    func testRemovingObserverCancelsQueuedDelivery() async {
        let signal = ChangeSignal<Void>()
        let counter = Counter()
        let token = signal.addCoalescedMainActorObserver { counter.value += 1 }
        signal.publish()
        signal.removeObserver(token)
        await withCheckedContinuation { continuation in
            DispatchQueue.main.async { continuation.resume() }
        }
        XCTAssertEqual(counter.value, 0)
    }

    func testMutationDuringDeliverySchedulesAnotherPass() async {
        let signal = ChangeSignal<Void>()
        let counter = Counter()
        let delivered = expectation(description: "Both passes")
        delivered.expectedFulfillmentCount = 2
        let token = signal.addCoalescedMainActorObserver {
            counter.value += 1
            if counter.value == 1 { signal.publish() }
            delivered.fulfill()
        }
        defer { signal.removeObserver(token) }
        signal.publish()
        await fulfillment(of: [delivered], timeout: 3)
        XCTAssertEqual(counter.value, 2)
    }

    private final class Counter {
        var value = 0
    }
}
