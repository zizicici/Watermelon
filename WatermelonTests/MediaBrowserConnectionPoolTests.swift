import XCTest
@testable import Watermelon

// The browser pool's contract: connects are never abandoned (a cancelled waiter's session parks for the
// next caller), sessions are long-lived with a hard cap, and shutdown latches everything closed.
final class MediaBrowserConnectionPoolTests: XCTestCase {

    private func pollUntil(timeout: TimeInterval, _ condition: () async -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await condition() { return }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTFail("condition not met within \(timeout)s")
    }

    // Scrolled-away cell: the waiter cancels, the in-flight (uncancellable) connect must complete in the
    // background and park — the next caller reuses that session instead of dialing a new one.
    func testCancelledWaiterDoesNotAbandonConnect() async throws {
        let counter = ProbeStorageClient.LiveCounter()
        let holder = ProbeClientHolder()
        let pool = MediaBrowserConnectionPool(maxConnections: 1) {
            holder.make(.delay(0.4, cancellable: false), counter: counter)
        }

        let waiter = Task { await pool.acquire() }
        try await Task.sleep(nanoseconds: 50_000_000)
        waiter.cancel()
        let bailed = await waiter.value
        XCTAssertNil(bailed, "a cancelled waiter must bail out with nil")

        try await pollUntil(timeout: 5) { counter.current == 1 }
        let reused = await pool.acquire()
        XCTAssertNotNil(reused, "the parked session must be handed to the next caller")
        XCTAssertEqual(holder.clients.count, 1, "no second connect may be dialed for the same slot")
        XCTAssertEqual(counter.peak, 1, "live sessions must never exceed the cap")
        await pool.shutdown()
    }

    // A failed connect must resolve the waiter (nil) and free the slot for a lazy redial on next demand.
    func testFailedConnectResumesWaiterAndRedialsLazily() async throws {
        let script = BehaviorScript([.throwError(RemoteStorageClientError.unavailable), .succeed])
        let holder = ProbeClientHolder()
        let pool = MediaBrowserConnectionPool(maxConnections: 1) {
            holder.make(script.next(), counter: nil)
        }

        let first = await pool.acquire()
        XCTAssertNil(first, "a failed connect resolves the waiter with nil")

        let second = await pool.acquire()
        XCTAssertNotNil(second, "the freed slot must redial on the next acquire")
        XCTAssertEqual(holder.clients.count, 2)
        await pool.shutdown()
    }

    // Shutdown latch: idle sessions disconnect, a late release disconnects instead of parking, and a
    // straggler acquire fails without dialing.
    func testShutdownLatchesLateReleaseAndAcquire() async throws {
        let counter = ProbeStorageClient.LiveCounter()
        let holder = ProbeClientHolder()
        let pool = MediaBrowserConnectionPool(maxConnections: 2) {
            holder.make(.succeed, counter: counter)
        }

        let acquired = await pool.acquire()
        let inFlight = try XCTUnwrap(acquired)
        await pool.shutdown()

        await pool.release(inFlight, reusable: true)
        let probe = try XCTUnwrap(inFlight as? ProbeStorageClient)
        let disconnected = await probe.didDisconnect
        XCTAssertTrue(disconnected, "a release after shutdown must disconnect, not park")
        XCTAssertEqual(counter.current, 0)

        let straggler = await pool.acquire()
        XCTAssertNil(straggler, "a straggler acquire after shutdown must fail")
        XCTAssertEqual(holder.clients.count, 1, "no new session may be dialed after shutdown")
    }

    // A dead session (reusable: false) redials immediately only when someone is waiting on it.
    func testNonReusableReleaseRedialsForWaiter() async throws {
        let holder = ProbeClientHolder()
        let pool = MediaBrowserConnectionPool(maxConnections: 1) {
            holder.make(.succeed, counter: nil)
        }

        let acquired = await pool.acquire()
        let client = try XCTUnwrap(acquired)
        let waiter = Task { await pool.acquire() }
        try await Task.sleep(nanoseconds: 100_000_000)

        await pool.release(client, reusable: false)
        let replacement = await waiter.value
        XCTAssertNotNil(replacement, "the waiter must receive the redialed session")
        XCTAssertEqual(holder.clients.count, 2)
        await pool.shutdown()
    }
}

// Hands out scripted connect behaviors one per make() call.
private final class BehaviorScript: @unchecked Sendable {
    private let lock = NSLock()
    private var queue: [ProbeStorageClient.ConnectBehavior]
    init(_ behaviors: [ProbeStorageClient.ConnectBehavior]) { queue = behaviors }
    func next() -> ProbeStorageClient.ConnectBehavior {
        lock.withLock { queue.isEmpty ? .succeed : queue.removeFirst() }
    }
}
