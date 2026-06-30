import XCTest
@testable import Watermelon

final class AsyncSemaphoreTests: XCTestCase {
    func testAcquiresUpToValueImmediately() async {
        let sem = AsyncSemaphore(value: 2)
        let first = await sem.wait()
        let second = await sem.wait()
        XCTAssertTrue(first)
        XCTAssertTrue(second)
    }

    func testParkedWaiterResumesAfterSignal() async {
        let sem = AsyncSemaphore(value: 1)
        let acquired = await sem.wait() // count -> 0
        XCTAssertTrue(acquired)

        let parked = Task { await sem.wait() }
        try? await Task.sleep(nanoseconds: 50_000_000) // let it park
        sem.signal() // hand the slot to the parked waiter

        let resumed = await parked.value
        XCTAssertTrue(resumed)
    }

    func testCancelledParkedWaiterReturnsFalse() async {
        let sem = AsyncSemaphore(value: 0) // always parks
        let parked = Task { await sem.wait() }
        try? await Task.sleep(nanoseconds: 50_000_000)
        parked.cancel()

        let resumed = await parked.value
        XCTAssertFalse(resumed) // cancelled, not deadlocked
    }

    func testSignalWithoutWaitersRaisesCount() async {
        let sem = AsyncSemaphore(value: 0)
        sem.signal() // no waiter -> count becomes 1
        let acquired = await sem.wait() // the raised slot is available
        XCTAssertTrue(acquired)
    }

    // Cancel lands at varying points relative to parking across iterations, exercising the
    // park-after-cancel race. Asserts no hang (the test would time out) and no slot leak: after each
    // cancelled wait, a signal + fresh wait must still succeed.
    func testCancelStormDoesNotHangOrLeakSlots() async {
        for _ in 0..<200 {
            let sem = AsyncSemaphore(value: 0)
            let waiter = Task { await sem.wait() }
            waiter.cancel()
            let cancelled = await waiter.value
            XCTAssertFalse(cancelled)

            sem.signal()
            let acquired = await sem.wait()
            XCTAssertTrue(acquired)
        }
    }
}
