import XCTest
@testable import Watermelon

// The pool-owned reserved-slot replacement: disconnect-before-connect (strict live-session cap), a
// deadline/cancellation-bounded connect, and reaping of a connect that outlasts the deadline.
final class StorageClientPoolReplacementTests: XCTestCase {

    private func future(_ seconds: TimeInterval = 5) -> Date { Date().addingTimeInterval(seconds) }

    // Polls a condition instead of sleeping a fixed wall-clock duration, so a slow/loaded CI box can't flake.
    private func pollUntil(timeout: TimeInterval, _ condition: () async -> Bool) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await condition() { return }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        XCTFail("condition not met within \(timeout)s")
    }

    // Strict cap: retireForReplacement must drop the broken client's live session BEFORE the replacement
    // connects, so a slot never holds two live sessions (the old connectReplacement() peaked at 2).
    func testRetireDropsBrokenBeforeReplacementConnects() async throws {
        let counter = ProbeStorageClient.LiveCounter()
        let pool = StorageClientPool(maxConnections: 1) { ProbeStorageClient(.succeed, counter: counter) }

        let broken = try await pool.acquire()                 // live == 1
        await pool.retireForReplacement(broken)               // live == 0 (disconnected first)
        let outcome = await pool.connectReplacement(by: future())   // live == 1 again

        guard case .connected = outcome else { return XCTFail("expected .connected, got \(outcome)") }
        XCTAssertEqual(counter.peak, 1, "a slot must never hold two live sessions during replacement")
        await pool.shutdown()
    }

    // The reserved-slot invariant under the worst case: a timed-out replacement leaves an uncooperative connect
    // running in the background (a stray that will connect ~later). Because retire drops the broken session
    // first and the stray is reaped, live sessions must stay <= cap throughout — a peak of 2 here would mean
    // the broken wasn't dropped before the stray came up. (LiveCounter would catch that; a fixed peak==N test
    // cannot, since per-slot leave-then-enter can't arithmetically exceed N.)
    func testReservedSlotStrayNeverExceedsCap() async throws {
        let counter = ProbeStorageClient.LiveCounter()
        let holder = ProbeClientHolder()
        let pool = StorageClientPool(maxConnections: 1) { holder.make(.delay(1, cancellable: false), counter: counter) }

        let broken = try await pool.acquire()                 // live 1
        await pool.retireForReplacement(broken)               // dropped before reconnect → live 0
        let outcome = await pool.connectReplacement(by: future(0.2))   // times out; stray connects ~1s later
        guard case .timedOut = outcome else { return XCTFail("expected .timedOut, got \(outcome)") }

        let stray = try XCTUnwrap(holder.clients.last)         // the abandoned replacement connect
        try await pollUntil(timeout: 5) { await stray.didDisconnect }   // wait out the stray + detached reaper
        XCTAssertLessThanOrEqual(counter.peak, 1, "a reserved-slot stray must never push live sessions over the cap")
        await pool.shutdown()
    }

    // Shutdown is a latch: an in-flight client released afterwards is disconnected instead of parked in
    // the dead pool, and a straggler acquire fails instead of opening a fresh connection nobody will close.
    func testShutdownLatchesLateReleaseAndAcquire() async throws {
        let counter = ProbeStorageClient.LiveCounter()
        let pool = StorageClientPool(maxConnections: 2) { ProbeStorageClient(.succeed, counter: counter) }

        let inFlight = try await pool.acquire()
        await pool.shutdown()

        await pool.release(inFlight, reusable: true)
        let probe = try XCTUnwrap(inFlight as? ProbeStorageClient)
        let disconnected = await probe.didDisconnect
        XCTAssertTrue(disconnected, "a release after shutdown must disconnect, not park a live session")
        XCTAssertEqual(counter.current, 0, "no live session may survive a post-shutdown release")

        do {
            _ = try await pool.acquire()
            XCTFail("a straggler acquire after shutdown must fail")
        } catch {}
        XCTAssertEqual(counter.current, 0, "a straggler acquire must not open a fresh connection")
    }

    // The bounded acquire must abandon a hung connect at the deadline (return .timedOut promptly), so a single
    // stuck initial connect can't eat the worker's whole recovery window. Slot is freed for a later retry.
    func testBoundedAcquireTimesOutOnHungConnect() async {
        let pool = StorageClientPool(maxConnections: 1) { ProbeStorageClient(.delay(5, cancellable: true)) }

        let start = Date()
        let outcome = await pool.acquire(by: future(0.3))
        let elapsed = Date().timeIntervalSince(start)

        guard case .timedOut = outcome else { return XCTFail("expected .timedOut, got \(outcome)") }
        XCTAssertLessThan(elapsed, 3.0, "a hung connect must time out at ~deadline, not block the window")
        await pool.shutdown()
    }

    func testConnectReplacementSurfacesConnectError() async {
        let failure = RemoteStorageClientError.invalidConfiguration
        let pool = StorageClientPool(maxConnections: 1) { ProbeStorageClient(.throwError(failure)) }

        let outcome = await pool.connectReplacement(by: future())

        guard case .failed(let error) = outcome else { return XCTFail("expected .failed, got \(outcome)") }
        XCTAssertTrue(error is RemoteStorageClientError)
        await pool.shutdown()
    }

    // A connect that outruns the deadline must yield .timedOut promptly (not block for the full connect),
    // so a slow backend connect can neither overrun the recovery window nor stall teardown.
    func testConnectReplacementTimesOutOnSlowConnect() async {
        let pool = StorageClientPool(maxConnections: 1) { ProbeStorageClient(.delay(3, cancellable: false)) }

        let start = Date()
        let outcome = await pool.connectReplacement(by: future(0.3))
        let elapsed = Date().timeIntervalSince(start)

        guard case .timedOut = outcome else { return XCTFail("expected .timedOut, got \(outcome)") }
        XCTAssertLessThan(elapsed, 3.0, "timed-out connect must return at ~deadline, not at connect completion")
        await pool.shutdown()
    }

    // The abortIf predicate (the worker wires the queue-stopped check) must abandon an in-flight connect and
    // return .timedOut promptly — well before the deadline or the slow connect completes — so a sibling's fatal
    // preempts an in-flight reconnect instead of stalling teardown.
    func testConnectReplacementAbortsOnPredicate() async {
        let pool = StorageClientPool(maxConnections: 1) { ProbeStorageClient(.delay(5, cancellable: true)) }
        let abort = AbortFlag()

        let task = Task { await pool.connectReplacement(by: self.future(30), abortIf: { abort.value }) }
        try? await Task.sleep(nanoseconds: 200_000_000)   // let the connect get in flight
        let start = Date()
        abort.value = true
        let outcome = await task.value
        let elapsed = Date().timeIntervalSince(start)

        guard case .timedOut = outcome else { return XCTFail("expected .timedOut, got \(outcome)") }
        XCTAssertLessThan(elapsed, 3.0, "abort must preempt the in-flight connect, not wait the deadline/connect")
        await pool.shutdown()
    }

    // A connect abandoned at the deadline that nevertheless completes later must be reaped (disconnected),
    // so an uncooperative backend connect can't leak a live session.
    func testTimedOutConnectIsReaped() async throws {
        let holder = ProbeClientHolder()
        let pool = StorageClientPool(maxConnections: 1) { holder.make(.delay(1, cancellable: false)) }

        let outcome = await pool.connectReplacement(by: future(0.2))
        guard case .timedOut = outcome else { return XCTFail("expected .timedOut, got \(outcome)") }

        let stray = try XCTUnwrap(holder.clients.first)
        // The abandoned connect (1s) must finish and the detached reaper must disconnect it.
        try await pollUntil(timeout: 5) { await stray.didDisconnect }
        await pool.shutdown()
    }
}

private final class AbortFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = false
    var value: Bool {
        get { lock.lock(); defer { lock.unlock() }; return _value }
        set { lock.lock(); _value = newValue; lock.unlock() }
    }
}
