import XCTest
@testable import Watermelon

// Adversarial probes for recoverWorkerConnection classification + termination edges that the shipped
// RecoverWorkerConnectionTests do not cover. Goal: break the .failed/.exhausted/.cancelled/.stopped routing.
final class AdversarialRecoveryProbeTests: XCTestCase {

    private func profile(_ type: StorageType = .s3) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil, name: "p", storageType: type.rawValue, connectionParams: nil, sortOrder: 0,
            host: "host.local", port: 0, shareName: "share", basePath: "/p", username: "u",
            domain: nil, credentialRef: "ref", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil
        )
    }

    private var retryableError: Error { NSError(domain: NSURLErrorDomain, code: NSURLErrorNetworkConnectionLost) }

    private func recover(
        broken: any RemoteStorageClientProtocol,
        pool: StorageClientPool,
        queue: MonthWorkQueue,
        window: TimeInterval,
        profileType: StorageType = .s3
    ) async -> BackupParallelExecutor.RecoveryOutcome {
        await BackupParallelExecutor.recoverWorkerConnection(
            broken: broken, monthStore: nil, deadline: Date().addingTimeInterval(window),
            clientPool: pool, monthQueue: queue, profile: profile(profileType),
            eventStream: BackupEventStream(), workerID: 0, monthText: "2024-05", error: retryableError
        )
    }

    // PROBE 1 (Bug B fixed): a CancellationError surfacing from connect while Task.isCancelled is FALSE must
    // classify as .cancelled (pause), not a terminal .failed. The helper now keys on RemoteFaultLite.classify.
    func testCancellationErrorFromConnectClassifiesAsCancelled() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { throw CancellationError() }
        let queue = MonthWorkQueue(months: [])

        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 5)

        guard case .cancelled = outcome else {
            return XCTFail("a CancellationError from connect must classify .cancelled, got \(outcome)")
        }
    }

    // PROBE 2: a reconnect probe that classifies as .notFound (e.g. WebDAV 404 on the base path) is a wrong
    // endpoint/config, not a transient blip — it must fail fast (.failed), not back off until .exhausted/pause.
    func testNotFoundReconnectFaultFailsFast() async {
        let broken = InMemoryRemoteStorageClient()
        let notFound = NSError(domain: WebDAVClient.errorDomain, code: 404)
        let pool = StorageClientPool(maxConnections: 2) { throw notFound }
        let queue = MonthWorkQueue(months: [])

        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 5)
        guard case .failed = outcome else {
            return XCTFail("a .notFound reconnect must fail fast (.failed), got \(outcome)")
        }
    }

    // PROBE 3: cancellation arriving during the backoff sleep must abort promptly as .cancelled (not loop,
    // not misread as a network fault). The window is long; only the cancellation should end it.
    func testCancellationDuringSleepAbortsPromptly() async {
        let broken = InMemoryRemoteStorageClient()
        // makeClient throws recoverable so, absent cancellation, this would keep retrying for the full window.
        let pool = StorageClientPool(maxConnections: 2) { throw RemoteStorageClientError.unavailable }
        let queue = MonthWorkQueue(months: [])

        let task = Task { () -> BackupParallelExecutor.RecoveryOutcome in
            await self.recover(broken: broken, pool: pool, queue: queue, window: 60)
        }
        // Let the helper enter its first backoff sleep, then cancel.
        try? await Task.sleep(nanoseconds: 150_000_000)
        let start = Date()
        task.cancel()
        let outcome = await task.value
        let elapsedAfterCancel = Date().timeIntervalSince(start)

        guard case .cancelled = outcome else {
            return XCTFail("expected .cancelled, got \(outcome)")
        }
        XCTAssertLessThan(elapsedAfterCancel, 2.0, "cancellation during sleep must abort promptly")
    }

    // PROBE 4 (cancellation wins over a stopped queue when both are set): if a worker is cancelled (user
    // pause) AND a sibling stopped the queue, the loop-top order checks cancel first, so the outcome must be
    // .cancelled, not .stopped — a user pause must never be downgraded to a sibling-stop settle.
    func testCancelOutranksStopAtLoopTop() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { InMemoryRemoteStorageClient() }
        let queue = MonthWorkQueue(months: [])
        await queue.stop()

        let task = Task { () -> BackupParallelExecutor.RecoveryOutcome in
            await self.recover(broken: broken, pool: pool, queue: queue, window: 10)
        }
        task.cancel()
        let outcome = await task.value
        // Both cancel and stop are true at loop top; line 310 checks cancel before line 311 checks stop.
        guard case .cancelled = outcome else {
            return XCTFail("expected .cancelled (cancel checked before stop), got \(outcome)")
        }
    }

    // PROBE 5 (near-hard cap, invariant 8): with a recoverable connect failure that never recovers, a tight
    // window must bound wall-clock to ~window + at most one in-flight connect. The fake's connect is instant,
    // so elapsed must stay well under the 1s first-backoff that an unclamped sleep would impose.
    func testTightWindowDoesNotOvershootBackoff() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { throw RemoteStorageClientError.unavailable }
        let queue = MonthWorkQueue(months: [])

        let start = Date()
        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 0.3)
        let elapsed = Date().timeIntervalSince(start)

        guard case .exhausted = outcome else { return XCTFail("expected .exhausted, got \(outcome)") }
        XCTAssertLessThan(elapsed, 1.0, "0.3s window overshot to a full backoff sleep")
        XCTAssertGreaterThan(elapsed, 0.25, "window ended far too early")
    }

    // PROBE 6 (pool accounting, invariant 6): the full worker contract on exhaustion, on ONE pool (the
    // production path: acquire, recover, and release are the same pool). maxConnections==1, seeded so the broken
    // client is checked out without a makeClient call; reconnects then fail recoverably until exhaustion (broken
    // kept checked out); the worker releases it non-reusably. The reserved slot must drain so a fresh acquire on
    // that same pool mints a new live connection — proving no same-pool leak/underflow.
    func testExhaustionThenNonReusableReleaseDrainsSamePoolSlot() async throws {
        let gate = ConnectFailureGate()
        let pool = StorageClientPool(maxConnections: 1) {
            if gate.failing { throw RemoteStorageClientError.unavailable }
            return InMemoryRemoteStorageClient()
        }
        let broken = InMemoryRemoteStorageClient()
        await pool.seedConnectedClient(broken)        // count == 1 == max, no makeClient call
        let acquired = try await pool.acquire()        // pops the seeded broken client, count stays 1
        let queue = MonthWorkQueue(months: [])

        gate.failing = true                            // every reconnect on this pool now fails recoverably
        let outcome = await BackupParallelExecutor.recoverWorkerConnection(
            broken: acquired, monthStore: nil, deadline: Date().addingTimeInterval(0.25),
            clientPool: pool, monthQueue: queue, profile: profile(),
            eventStream: BackupEventStream(), workerID: 0, monthText: "m", error: retryableError
        )
        guard case .exhausted = outcome else { return XCTFail("expected .exhausted, got \(outcome)") }

        await pool.release(acquired, reusable: false)  // worker's terminal release on the exhausted path
        gate.failing = false
        // Slot drained: a fresh acquire on the SAME pool mints a new live connection (no leak/underflow).
        let reacquired = try await pool.acquire()
        let connected = await (reacquired as? InMemoryRemoteStorageClient)?.connected
        XCTAssertEqual(connected, true)
        await pool.shutdown()
    }

    // PROBE 7 (P1: stop preempts an in-flight reconnect): a sibling's fatal stops the queue while this worker is
    // blocked inside a slow replacement connect. The connect must be abandoned and the helper return .stopped
    // promptly — not wait out the recovery window (or the 30s connect) for the eventual task-group cancellation.
    func testStopDuringInFlightConnectReturnsStoppedPromptly() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { ProbeStorageClient(.delay(30, cancellable: true)) }
        let queue = MonthWorkQueue(months: [])

        let task = Task { () -> BackupParallelExecutor.RecoveryOutcome in
            await self.recover(broken: broken, pool: pool, queue: queue, window: 60)
        }
        // Let the helper clear its first backoff (<=1.5s) and enter the hanging connectReplacement, then stop.
        try? await Task.sleep(nanoseconds: 1_700_000_000)
        let start = Date()
        await queue.stop()
        let outcome = await task.value
        let elapsedAfterStop = Date().timeIntervalSince(start)

        guard case .stopped = outcome else { return XCTFail("expected .stopped, got \(outcome)") }
        XCTAssertLessThan(elapsedAfterStop, 3.0, "stop must preempt the in-flight connect, not wait the window/connect")
    }
}

// Stateful makeClient switch: seed a broken client, then flip `failing` so reconnects fail recoverably, then
// flip back so a post-release acquire succeeds — all on a single pool.
private final class ConnectFailureGate: @unchecked Sendable {
    private let lock = NSLock()
    private var _failing = false
    var failing: Bool {
        get { lock.lock(); defer { lock.unlock() }; return _failing }
        set { lock.lock(); _failing = newValue; lock.unlock() }
    }
}
