import XCTest
@testable import Watermelon

@MainActor
final class RefreshSchedulerTests: XCTestCase {
    final class HookRecorder {
        struct AfterReload {
            let scopeChanged: Bool
            let accessChanged: Bool
            let hasMoreReloadPending: Bool
        }
        struct Iteration {
            let work: HomeRefreshScheduler.Work
            let scopeChanged: Bool
            let accessChanged: Bool
        }
        var normalizeReturns: [Bool] = []
        var reloadCount = 0
        var refreshAccessReturns: [Bool] = []
        var afterReloads: [AfterReload] = []
        var syncRemoteCount = 0
        var postProcessCount = 0
        var iterations: [Iteration] = []
        // Hook to inject extra work mid-iteration for testing coalescing.
        var midReloadHook: () -> Void = {}
    }

    private func makeScheduler(
        recorder: HookRecorder,
        normalizeReturn: Bool = false,
        accessChangedReturn: Bool = false
    ) -> HomeRefreshScheduler {
        HomeRefreshScheduler(hooks: HomeRefreshScheduler.Hooks(
            normalizeBeforeReload: {
                recorder.normalizeReturns.append(normalizeReturn)
                return normalizeReturn
            },
            reloadLocal: {
                recorder.reloadCount += 1
                recorder.midReloadHook()
            },
            refreshAccessState: {
                recorder.refreshAccessReturns.append(accessChangedReturn)
                return accessChangedReturn
            },
            afterReload: { sc, ac, hasMore in
                recorder.afterReloads.append(.init(scopeChanged: sc, accessChanged: ac, hasMoreReloadPending: hasMore))
            },
            syncRemote: { recorder.syncRemoteCount += 1 },
            postProcess: { recorder.postProcessCount += 1 },
            onIterationComplete: { work, sc, ac in
                recorder.iterations.append(.init(work: work, scopeChanged: sc, accessChanged: ac))
            }
        ))
    }

    // MARK: - Single-iteration paths

    func testEnqueue_reloadLocal_runsFullReloadPath() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.normalizeReturns.count, 1)
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.refreshAccessReturns.count, 1)
        XCTAssertEqual(recorder.afterReloads.count, 1)
        XCTAssertEqual(recorder.afterReloads.first?.hasMoreReloadPending, false)
        XCTAssertEqual(recorder.postProcessCount, 1)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertEqual(recorder.iterations.first?.work, .reloadLocal)
    }

    func testEnqueue_syncRemote_skipsReloadHooks() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.syncRemote)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.normalizeReturns.count, 0, "normalize is reloadLocal-only")
        XCTAssertEqual(recorder.reloadCount, 0)
        XCTAssertEqual(recorder.afterReloads.count, 0)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.postProcessCount, 1, "postProcess always runs each iteration")
        XCTAssertEqual(recorder.iterations.count, 1)
    }

    func testEnqueue_notifyOnly_skipsBothPhases() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.notifyConnection)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.reloadCount, 0)
        XCTAssertEqual(recorder.syncRemoteCount, 0)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertEqual(recorder.iterations.first?.work, .notifyConnection)
    }

    func testEnqueue_propagatesScopeChangedToOnIterationComplete() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, normalizeReturn: true)

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.iterations.first?.scopeChanged, true)
    }

    func testEnqueue_propagatesAccessChangedToOnIterationComplete() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, accessChangedReturn: true)

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.iterations.first?.accessChanged, true)
    }

    // MARK: - Coalescing

    func testCoalescing_enqueueDuringReload_signalsHasMorePending() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        // One-shot: enqueue another reload from inside the first one's hook. This
        // simulates "another reload landed while the current one was running".
        var didReenqueue = false
        recorder.midReloadHook = { [weak scheduler] in
            guard !didReenqueue else { return }
            didReenqueue = true
            scheduler?.enqueue(.reloadLocal)
        }

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()

        // First iteration sees hasMoreReloadPending=true (because the hook re-enqueued);
        // second iteration sees false (no more pending).
        XCTAssertEqual(recorder.afterReloads.count, 2)
        XCTAssertEqual(recorder.afterReloads[0].hasMoreReloadPending, true)
        XCTAssertEqual(recorder.afterReloads[1].hasMoreReloadPending, false)
        XCTAssertEqual(recorder.reloadCount, 2)
        XCTAssertEqual(recorder.iterations.count, 2)
    }

    func testCoalescing_multipleEnqueuesBeforeRun_mergeIntoOneIteration() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.reloadLocal)
        scheduler.enqueue(.syncRemote)
        scheduler.enqueue(.notifyStructural)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.iterations.count, 1, "all flags fold into a single iteration")
        let work = recorder.iterations.first?.work ?? []
        XCTAssertTrue(work.contains(.reloadLocal))
        XCTAssertTrue(work.contains(.syncRemote))
        XCTAssertTrue(work.contains(.notifyStructural))
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
    }

    // MARK: - Lifecycle

    func testReenqueueAfterDrain_startsFreshIteration() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.iterations.count, 1)

        scheduler.enqueue(.syncRemote)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.iterations.count, 2)
    }

    func testCancel_clearsPendingAndStopsRunningTask() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue(.reloadLocal)
        scheduler.cancel()
        // After cancel, the task is torn down; pending is cleared.
        await scheduler._testWaitUntilIdle()

        // A subsequent enqueue should still work — cancel resets the scheduler.
        scheduler.enqueue(.notifyConnection)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.iterations.last?.work, .notifyConnection)
    }
}
