import XCTest
@testable import Watermelon

final class OrderedRestorePipelineTests: XCTestCase {
    func testOutOfOrderDownloadsCommitInOrderAndBoundLookahead() async throws {
        let head = RestoreTestLatch()
        let laterReady = expectation(description: "second download ready")
        let probe = RestorePipelineProbe()
        let task = Task {
            try await OrderedRestorePipeline<Int>(policy: .init()).run(
                estimatedBytes: [1, 1, 1, 1], shouldDrain: { false }, availableCapacity: { nil }, byteCount: { _ in 1 },
                prepare: { index, worker in
                    await probe.start(index, worker: worker)
                    if index == 0 { await head.wait() }
                    if index == 1 { laterReady.fulfill() }
                    return index
                },
                commit: { _, result in await probe.commit(try result.get()) }
            )
        }
        await fulfillment(of: [laterReady], timeout: 3)
        let before = await probe.snapshot()
        XCTAssertEqual(before.started, [0, 1])
        XCTAssertEqual(before.committed, [])
        await head.open()
        try await task.value
        let after = await probe.snapshot()
        XCTAssertEqual(after.committed, [0, 1, 2, 3])
        XCTAssertEqual(Set(after.workers), [0, 1])
    }

    func testLaterFailureIsDeliveredAfterEarlierCommit() async throws {
        let head = RestoreTestLatch()
        let laterFailed = expectation(description: "second download failed")
        let probe = RestorePipelineProbe()
        let task = Task {
            try await OrderedRestorePipeline<Int>(policy: .init()).run(
                estimatedBytes: [1, 1, 1], shouldDrain: { false }, availableCapacity: { nil }, byteCount: { _ in 1 },
                prepare: { index, _ in
                    if index == 0 { await head.wait() }
                    if index == 1 {
                        laterFailed.fulfill()
                        throw RestorePipelineTestError.failed
                    }
                    return index
                },
                commit: { _, result in await probe.commit(try result.get()) }
            )
        }
        await fulfillment(of: [laterFailed], timeout: 3)
        let before = await probe.snapshot()
        XCTAssertEqual(before.committed, [])
        await head.open()
        do { try await task.value; XCTFail("expected download failure") } catch RestorePipelineTestError.failed {}
        let after = await probe.snapshot()
        XCTAssertEqual(after.committed, [0])
    }

    func testDrainCancelsPrefetchWhileFinishingHead() async throws {
        let head = RestoreTestLatch()
        let started = expectation(description: "both downloads started")
        started.expectedFulfillmentCount = 2
        let prefetchCancelled = expectation(description: "prefetch cancelled")
        let drain = ExecutionTerminationControl()
        let probe = RestorePipelineProbe()
        let task = Task {
            try await OrderedRestorePipeline<Int>(policy: .init()).run(
                estimatedBytes: [1, 1, 1], shouldDrain: { drain.shouldDrain }, availableCapacity: { nil }, byteCount: { _ in 1 },
                prepare: { index, _ in
                    started.fulfill()
                    if index == 0 {
                        await head.wait()
                        XCTAssertFalse(Task.isCancelled)
                    } else {
                        do { try await Task.sleep(for: .seconds(30)) }
                        catch { prefetchCancelled.fulfill(); throw error }
                    }
                    return index
                },
                commit: { _, result in await probe.commit(try result.get()) }
            )
        }
        await fulfillment(of: [started], timeout: 3)
        drain.request(.pause)
        await fulfillment(of: [prefetchCancelled], timeout: 3)
        await head.open()
        do { try await task.value; XCTFail("expected drain") } catch is CancellationError {}
        let state = await probe.snapshot()
        XCTAssertEqual(state.committed, [0])
    }

    func testHardCancellationCancelsEveryDownload() async throws {
        let started = expectation(description: "both downloads started")
        started.expectedFulfillmentCount = 2
        let cancelled = expectation(description: "both downloads cancelled")
        cancelled.expectedFulfillmentCount = 2
        let task = Task {
            try await OrderedRestorePipeline<Int>(policy: .init()).run(
                estimatedBytes: [1, 1], shouldDrain: { false }, availableCapacity: { nil }, byteCount: { _ in 1 },
                prepare: { index, _ in
                    started.fulfill()
                    do { try await Task.sleep(for: .seconds(30)) }
                    catch { cancelled.fulfill(); throw error }
                    return index
                },
                commit: { _, result in _ = try result.get(); XCTFail("must not commit") }
            )
        }
        await fulfillment(of: [started], timeout: 3)
        task.cancel()
        do { try await task.value; XCTFail("expected cancellation") } catch is CancellationError {}
        await fulfillment(of: [cancelled], timeout: 3)
    }

    func testFailureWaitsForCancelledPrefetchToSettle() async throws {
        let failHead = RestoreTestLatch()
        let releasePrefetch = RestoreTestLatch()
        let started = expectation(description: "prefetch started")
        let cancelled = expectation(description: "prefetch cancellation requested")
        let returned = ExecutionTerminationControl()
        let task = Task {
            defer { returned.request(.stop) }
            try await OrderedRestorePipeline<Int>(policy: .init()).run(
                estimatedBytes: [1, 1], shouldDrain: { false }, availableCapacity: { nil }, byteCount: { _ in 1 },
                prepare: { index, _ in
                    if index == 0 { await failHead.wait(); throw RestorePipelineTestError.failed }
                    started.fulfill()
                    await withTaskCancellationHandler { await releasePrefetch.wait() } onCancel: { cancelled.fulfill() }
                    return index
                },
                commit: { _, result in _ = try result.get() }
            )
        }
        await fulfillment(of: [started], timeout: 3)
        await failHead.open()
        await fulfillment(of: [cancelled], timeout: 3)
        XCTAssertFalse(returned.shouldDrain)
        await releasePrefetch.open()
        do { try await task.value; XCTFail("expected failure") } catch RestorePipelineTestError.failed {}
        XCTAssertTrue(returned.shouldDrain)
    }

    func testOversizedAssetRunsAloneAndDoesNotDeadlock() async throws {
        let probe = RestorePipelineProbe()
        let policy = RestoreDownloadPolicy(workerCount: 2, bufferedByteLimit: 10, freeSpaceReserve: 0)
        try await OrderedRestorePipeline<Int>(policy: policy).run(
            estimatedBytes: [100, 2], shouldDrain: { false }, availableCapacity: { nil }, byteCount: { _ in 100 },
            prepare: { index, worker in
                if index == 1 {
                    let state = await probe.snapshot()
                    XCTAssertEqual(state.committed, [0])
                }
                await probe.start(index, worker: worker)
                return index
            },
            commit: { _, result in await probe.commit(try result.get()) }
        )
        let state = await probe.snapshot()
        XCTAssertEqual(state.committed, [0, 1])
    }

    func testCompletedActualSizePreventsFurtherPrefetch() async throws {
        let head = RestoreTestLatch()
        let prepared = expectation(description: "large second item prepared")
        let probe = RestorePipelineProbe()
        let task = Task {
            try await OrderedRestorePipeline<Int>(policy: .init(workerCount: 2, bufferedByteLimit: 10)).run(
                estimatedBytes: [1, 1, 1], shouldDrain: { false }, availableCapacity: { nil },
                byteCount: { index in
                    if index == 1 { prepared.fulfill() }
                    return index == 1 ? 100 : 1
                },
                prepare: { index, _ in
                    if index == 0 { await head.wait() }
                    if index == 2 {
                        let state = await probe.snapshot()
                        XCTAssertEqual(state.committed, [0, 1])
                    }
                    return index
                },
                commit: { _, result in await probe.commit(try result.get()) }
            )
        }
        await fulfillment(of: [prepared], timeout: 3)
        await head.open()
        try await task.value
    }

    func testSpaceAdmissionReservesImportRoomAndHandlesOverflow() {
        let policy = RestoreDownloadPolicy(workerCount: 2, bufferedByteLimit: 100, freeSpaceReserve: 10)
        XCTAssertTrue(policy.admits(bytes: 200, reserved: [], available: 610))
        XCTAssertFalse(policy.admits(bytes: 200, reserved: [], available: 609))
        XCTAssertFalse(policy.admits(bytes: 60, reserved: [60], available: nil))
        XCTAssertFalse(policy.admits(bytes: 0, reserved: [1], available: nil))
        XCTAssertFalse(policy.admits(bytes: 1, reserved: [0], available: nil))
        XCTAssertFalse(policy.admits(bytes: .max, reserved: [], available: 1_000))
        XCTAssertEqual(RestoreDownloadPolicy.adding(.max, 1), .max)
    }
}

private enum RestorePipelineTestError: Error { case failed }

actor RestoreTestLatch {
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !opened else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        opened = true
        let pending = waiters
        waiters.removeAll()
        pending.forEach { $0.resume() }
    }
}

private actor RestorePipelineProbe {
    private var started: Set<Int> = []
    private var workers: [Int] = []
    private var committed: [Int] = []

    func start(_ index: Int, worker: Int) { started.insert(index); workers.append(worker) }
    func commit(_ index: Int) { committed.append(index) }
    func snapshot() -> (started: Set<Int>, workers: [Int], committed: [Int]) { (started, workers, committed) }
}
