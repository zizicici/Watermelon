import XCTest
@testable import Watermelon

// Direct coverage for the bounded-reconnect helper: the .recovered / .stopped / .exhausted / .failed
// outcomes. The broken client is retired (disconnected) up front for every outcome — the worker frees the
// slot's count via its terminal release(reusable:false). The full worker loop is PHAsset-dependent.
final class RecoverWorkerConnectionTests: XCTestCase {

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil, name: "p", storageType: StorageType.s3.rawValue, connectionParams: nil, sortOrder: 0,
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
        window: TimeInterval
    ) async -> BackupParallelExecutor.RecoveryOutcome {
        await BackupParallelExecutor.recoverWorkerConnection(
            broken: broken,
            monthStore: nil,
            deadline: Date().addingTimeInterval(window),
            clientPool: pool,
            monthQueue: queue,
            profile: profile(),
            eventStream: BackupEventStream(),
            workerID: 0,
            monthText: "2024-05",
            error: retryableError
        )
    }

    func testRecoveredReturnsConnectedClientAndDropsBroken() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { InMemoryRemoteStorageClient() }
        let queue = MonthWorkQueue(months: [])

        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 10)

        switch outcome {
        case .recovered(let fresh):
            let freshConnected = await (fresh as? InMemoryRemoteStorageClient)?.connected
            XCTAssertEqual(freshConnected, true)
            let brokenConnected = await broken.connected
            XCTAssertFalse(brokenConnected, "broken client must be disconnected on success")
        default:
            XCTFail("expected .recovered, got \(outcome)")
        }
    }

    func testExhaustedWhenReconnectKeepsFailing() async {
        let broken = InMemoryRemoteStorageClient()
        // makeClient throws a RECOVERABLE fault → keeps retrying → window expires → .exhausted.
        let pool = StorageClientPool(maxConnections: 2) { throw RemoteStorageClientError.unavailable }
        let queue = MonthWorkQueue(months: [])

        let start = Date()
        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 0.2)
        let elapsed = Date().timeIntervalSince(start)

        guard case .exhausted = outcome else {
            return XCTFail("expected .exhausted, got \(outcome)")
        }
        // The window is a near-hard cap: a 0.2s window must not overshoot to a full ~1s backoff sleep.
        XCTAssertLessThan(elapsed, 1.0, "recovery window overshot its bound")
        let brokenConnected = await broken.connected
        XCTAssertFalse(brokenConnected, "broken is retired up front (disconnect-before-connect)")
    }

    func testTerminalFaultDuringReconnectSurfacesAsFailedNotExhausted() async {
        let broken = InMemoryRemoteStorageClient()
        // A non-recoverable fault during reconnect (here: invalid configuration) must surface as .failed so
        // the run fails, not as .exhausted (which the reducer would mask as a resumable network pause).
        let pool = StorageClientPool(maxConnections: 2) { throw RemoteStorageClientError.invalidConfiguration }
        let queue = MonthWorkQueue(months: [])

        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 10)

        guard case .failed(let terminal) = outcome else {
            return XCTFail("expected .failed, got \(outcome)")
        }
        XCTAssertFalse(AssetProcessor.isRecoverableNetworkFault(terminal, profile: profile()))
    }

    func testHonorsAlreadyElapsedDeadline() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { InMemoryRemoteStorageClient() }   // would connect fine
        let queue = MonthWorkQueue(months: [])

        // A cumulative deadline already in the past must exhaust immediately rather than start a fresh window —
        // this is what bounds a flapping network across the worker's recover→retry cycles (anti-livelock).
        let outcome = await BackupParallelExecutor.recoverWorkerConnection(
            broken: broken, monthStore: nil, deadline: Date().addingTimeInterval(-1),
            clientPool: pool, monthQueue: queue, profile: profile(),
            eventStream: BackupEventStream(), workerID: 0, monthText: "2024-05", error: retryableError
        )

        guard case .exhausted = outcome else {
            return XCTFail("an elapsed deadline must exhaust immediately, got \(outcome)")
        }
    }

    func testStoppedQueueShortCircuitsToStoppedNotCancelled() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { InMemoryRemoteStorageClient() }
        let queue = MonthWorkQueue(months: [])
        await queue.stop()

        let outcome = await recover(broken: broken, pool: pool, queue: queue, window: 10)

        guard case .stopped = outcome else {
            return XCTFail("expected .stopped (defer to sibling), got \(outcome)")
        }
    }

    // A queue stopped exactly when the deadline has already elapsed must defer to the sibling's fatal (.stopped),
    // not mask it as a resumable .exhausted pause. The past deadline skips the loop, so only the final re-check
    // catches the stop.
    func testStoppedAtDeadlineExpiryReturnsStoppedNotExhausted() async {
        let broken = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 2) { InMemoryRemoteStorageClient() }
        let queue = MonthWorkQueue(months: [])
        await queue.stop()

        let outcome = await BackupParallelExecutor.recoverWorkerConnection(
            broken: broken, monthStore: nil, deadline: Date().addingTimeInterval(-1),
            clientPool: pool, monthQueue: queue, profile: profile(),
            eventStream: BackupEventStream(), workerID: 0, monthText: "2024-05", error: retryableError
        )

        guard case .stopped = outcome else {
            return XCTFail("a stop at deadline expiry must return .stopped, not .exhausted, got \(outcome)")
        }
    }

    // MARK: - Initial acquire (acquireWorkerClient)

    private func acquireWorker(pool: StorageClientPool, queue: MonthWorkQueue, window: TimeInterval) async throws -> (any RemoteStorageClientProtocol)? {
        try await BackupParallelExecutor.acquireWorkerClient(
            clientPool: pool, deadline: Date().addingTimeInterval(window),
            monthQueue: queue, profile: profile(), eventStream: BackupEventStream(), workerID: 0
        )
    }

    func testAcquireReturnsClientWhenPoolHealthy() async throws {
        let pool = StorageClientPool(maxConnections: 1) { InMemoryRemoteStorageClient() }
        let acquired = try await acquireWorker(pool: pool, queue: MonthWorkQueue(months: []), window: 5)
        let client = try XCTUnwrap(acquired)
        let connected = await (client as? InMemoryRemoteStorageClient)?.connected
        XCTAssertEqual(connected, true)
    }

    // A sibling stopping the queue during the initial acquire must defer (return nil), not throw.
    func testAcquireDefersWhenQueueStopped() async throws {
        let pool = StorageClientPool(maxConnections: 1) { InMemoryRemoteStorageClient() }
        let queue = MonthWorkQueue(months: [])
        await queue.stop()
        let client = try await acquireWorker(pool: pool, queue: queue, window: 5)
        XCTAssertNil(client, "a stopped queue must defer to the sibling (nil), not throw or return a client")
    }

    // A transient fault establishing the initial connection must be ridden out, not fail the run.
    func testAcquireRidesOutTransientThenConnects() async throws {
        let gate = AcquireFailureGate(failFirst: 1)
        let pool = StorageClientPool(maxConnections: 1) {
            if gate.shouldFail() { throw RemoteStorageClientError.unavailable }
            return InMemoryRemoteStorageClient()
        }
        let acquired = try await acquireWorker(pool: pool, queue: MonthWorkQueue(months: []), window: 30)
        let client = try XCTUnwrap(acquired)
        let connected = await (client as? InMemoryRemoteStorageClient)?.connected
        XCTAssertEqual(connected, true)
    }

    // A sustained outage on the initial connect must pause (resumable), not fail the run.
    func testAcquireSustainedTransientExhaustsToResumablePause() async {
        let pool = StorageClientPool(maxConnections: 1) { throw RemoteStorageClientError.unavailable }
        do {
            _ = try await acquireWorker(pool: pool, queue: MonthWorkQueue(months: []), window: 0.25)
            XCTFail("expected a throw")
        } catch {
            XCTAssertTrue(error is BackupNetworkRecoveryExhausted, "a sustained transient must pause (resumable), got \(error)")
        }
    }

    // A terminal fault on the initial connect must fail fast, not pause.
    func testAcquireTerminalFaultFailsFast() async {
        let pool = StorageClientPool(maxConnections: 1) { throw RemoteStorageClientError.invalidConfiguration }
        do {
            _ = try await acquireWorker(pool: pool, queue: MonthWorkQueue(months: []), window: 30)
            XCTFail("expected a throw")
        } catch {
            XCTAssertFalse(error is BackupNetworkRecoveryExhausted, "a terminal fault must fail, not pause")
        }
    }
}

private final class AcquireFailureGate: @unchecked Sendable {
    private let lock = NSLock()
    private var remaining: Int
    init(failFirst: Int) { remaining = failFirst }
    func shouldFail() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if remaining > 0 { remaining -= 1; return true }
        return false
    }
}
