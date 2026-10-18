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
            let localMonths: Set<LibraryMonthKey>
        }
        var normalizeReturns: [Bool] = []
        var reloadCount = 0
        var reconcileCount = 0
        var reconcileMonths = Set<LibraryMonthKey>()
        var midReconcileAsyncHook: () async -> Void = {}
        var refreshAccessReturns: [Bool] = []
        var afterReloads: [AfterReload] = []
        var shouldDeferWork = false
        var syncRemoteCount = 0
        var presentationCount = 0
        var presentedReconcileCounts: [Int] = []
        var iterations: [Iteration] = []
        var midReloadHook: () -> Void = {}
        var midSyncHook: () -> Void = {}
        var midSyncAsyncHook: () async -> Void = {}
    }

    private func makeScheduler(
        recorder: HookRecorder,
        normalizeReturn: Bool = false,
        accessChangedReturn: Bool = false,
        initiallyActive: Bool = true,
        coalescingInterval: Duration = .milliseconds(10)
    ) -> HomeRefreshScheduler {
        HomeRefreshScheduler(initiallyActive: initiallyActive, coalescingInterval: coalescingInterval, hooks: HomeRefreshScheduler.Hooks(
            normalizeBeforeReload: {
                recorder.normalizeReturns.append(normalizeReturn)
                return normalizeReturn
            },
            isReloadBlocked: { recorder.shouldDeferWork },
            reloadLocal: {
                recorder.reloadCount += 1
                recorder.midReloadHook()
            },
            reconcileLocal: {
                recorder.reconcileCount += 1
                let months = recorder.reconcileMonths
                await recorder.midReconcileAsyncHook()
                return months
            },
            refreshAccessState: {
                recorder.refreshAccessReturns.append(accessChangedReturn)
                return accessChangedReturn
            },
            afterReload: { sc, ac, hasMore in
                recorder.afterReloads.append(.init(scopeChanged: sc, accessChanged: ac, hasMoreReloadPending: hasMore))
            },
            syncRemote: {
                recorder.syncRemoteCount += 1
                recorder.midSyncHook()
                await recorder.midSyncAsyncHook()
            },
            onIterationComplete: { work, sc, ac, months in
                recorder.presentationCount += 1
                recorder.presentedReconcileCounts.append(recorder.reconcileCount)
                recorder.iterations.append(.init(work: work, scopeChanged: sc, accessChanged: ac, localMonths: months))
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
        XCTAssertEqual(recorder.presentationCount, 1)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertEqual(recorder.iterations.first?.work, .reloadLocal)
        XCTAssertFalse(scheduler.hasQueuedOrRunningReloadLocal)
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
        XCTAssertEqual(recorder.presentationCount, 1, "presentation runs after the data phases")
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

    func testQueuedReloadDefersIfBlockedBeforeExecution() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)

        scheduler.enqueue([.reloadLocal, .syncRemote, .notifyStructural])
        recorder.shouldDeferWork = true
        XCTAssertTrue(scheduler.hasQueuedOrRunningReloadLocal)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.reloadCount, 0)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.iterations.first?.work, [.syncRemote, .notifyStructural])
        XCTAssertTrue(scheduler.hasQueuedOrRunningReloadLocal)
        XCTAssertFalse(scheduler.isReloadUnderway)

        recorder.shouldDeferWork = false
        scheduler.resumeIfPossible()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertFalse(scheduler.hasQueuedOrRunningReloadLocal)
    }

    func testBlockedReloadStillCoalescesIncrementalCommitsAndResumesOnce() async throws {
        let recorder = HookRecorder()
        recorder.shouldDeferWork = true
        let scheduler = makeScheduler(recorder: recorder, coalescingInterval: .milliseconds(100))
        scheduler.enqueue(.reloadLocal)
        for _ in 0..<5 {
            scheduler.enqueue(.reconcileLocal, coalescing: true)
            try await Task.sleep(for: .milliseconds(10))
        }
        XCTAssertEqual(recorder.reconcileCount, 0)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.reloadCount, 0)
        XCTAssertTrue(scheduler.hasQueuedOrRunningReloadLocal)
        recorder.shouldDeferWork = false
        scheduler.resumeIfPossible()
        scheduler.resumeIfPossible()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
    }

    func testBlockedReloadSurvivesInactivityAndNewWorkAfterUnblocking() async {
        let recorder = HookRecorder()
        recorder.shouldDeferWork = true
        let scheduler = makeScheduler(recorder: recorder, initiallyActive: false)
        scheduler.enqueue([.reloadLocal, .notifyStructural])
        scheduler.enqueue(.reloadLocal)
        scheduler.resumeIfPossible()
        await scheduler._testWaitUntilIdle()
        XCTAssertTrue(recorder.iterations.isEmpty)
        scheduler.setActive(true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.iterations.first?.work, .notifyStructural)
        XCTAssertEqual(recorder.reloadCount, 0)
        recorder.shouldDeferWork = false
        scheduler.enqueue(.syncRemote)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.iterations.last?.work, [.reloadLocal, .syncRemote])
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertFalse(scheduler.hasQueuedOrRunningReloadLocal)
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
        // Simulate a reload arriving while one is already in flight. The first
        // iteration must see hasMoreReloadPending=true; the second false.
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        var didReenqueue = false
        recorder.midReloadHook = { [weak scheduler] in
            guard !didReenqueue else { return }
            didReenqueue = true
            scheduler?.enqueue(.reloadLocal)
        }

        scheduler.enqueue(.reloadLocal)
        await scheduler._testWaitUntilIdle()

        XCTAssertEqual(recorder.afterReloads.count, 2)
        XCTAssertEqual(recorder.afterReloads[0].hasMoreReloadPending, true)
        XCTAssertEqual(recorder.afterReloads[1].hasMoreReloadPending, false)
        XCTAssertEqual(recorder.reloadCount, 2)
        XCTAssertEqual(recorder.iterations.count, 1)
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

    func testCommittedChangeBurstUsesOneCoalescingWindow() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let month = LibraryMonthKey(year: 2026, month: 1)
        recorder.reconcileMonths = [month]
        for _ in 0..<100 { scheduler.enqueue(.reconcileLocal, coalescing: true) }
        XCTAssertEqual(recorder.reconcileCount, 0)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertEqual(recorder.iterations.first?.localMonths, [month])
        XCTAssertEqual(recorder.iterations.first?.work, .reconcileLocal)
    }

    func testFullReloadAbsorbsPendingIncrementalWorkWithoutWaitingForTimer() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, coalescingInterval: .seconds(60))
        let completed = expectation(description: "full reload bypasses coalescing")
        recorder.midReloadHook = { completed.fulfill() }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        scheduler.enqueue([.reloadLocal, .notifyStructural])
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await fulfillment(of: [completed], timeout: 3)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.reconcileCount, 0)
        XCTAssertEqual(recorder.iterations.count, 1)
    }

    func testArrivalsDuringReconcileAreSerializedAndPreserveEarlierChanges() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "incremental started")
        let gate = RestoreTestLatch()
        var active = false
        recorder.reconcileMonths = [.init(year: 2026, month: 1)]
        recorder.midReconcileAsyncHook = {
            XCTAssertFalse(active)
            active = true
            if recorder.reconcileCount == 1 {
                started.fulfill()
                await gate.wait()
            }
            active = false
        }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await fulfillment(of: [started], timeout: 3)
        recorder.reconcileMonths = []
        for _ in 0..<100 { scheduler.enqueue(.reconcileLocal, coalescing: true) }
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 2)
        XCTAssertEqual(recorder.presentedReconcileCounts, [1, 2])
        XCTAssertEqual(recorder.iterations.first?.localMonths, [.init(year: 2026, month: 1)])
        XCTAssertEqual(recorder.iterations.first?.work, .reconcileLocal)
        XCTAssertEqual(recorder.iterations.last?.localMonths, [])
        XCTAssertFalse(active)
    }

    func testCommitDuringFullReloadPublishesReloadBeforeIncrementalFollowUp() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        recorder.midReloadHook = { [weak scheduler] in
            for _ in 0..<100 { scheduler?.enqueue(.reconcileLocal, coalescing: true) }
        }
        recorder.midReconcileAsyncHook = {
            XCTAssertEqual(recorder.presentationCount, 1)
        }
        scheduler.enqueue([.reloadLocal, .syncRemote, .notifyStructural])
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentedReconcileCounts, [0, 1])
        XCTAssertTrue(recorder.iterations.first?.work.contains(.notifyStructural) == true)
        XCTAssertEqual(recorder.iterations.last?.work, .reconcileLocal)
    }

    func testContinuousCommitsPublishEveryCompletedReconcile() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, coalescingInterval: .milliseconds(1))
        let passCount = 20
        recorder.reconcileMonths = [.init(year: 2026, month: 1)]
        recorder.midReconcileAsyncHook = { [weak scheduler] in
            if recorder.reconcileCount < passCount {
                scheduler?.enqueue(.reconcileLocal, coalescing: true)
            }
            await Task.yield()
        }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, passCount)
        XCTAssertEqual(recorder.presentedReconcileCounts, Array(1...passCount))
        XCTAssertTrue(recorder.iterations.allSatisfy { $0.work == .reconcileLocal && $0.localMonths == recorder.reconcileMonths })
    }

    func testForegroundCatchUpPublishesWhileCommitsAndRemoteUpdatesContinue() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, initiallyActive: false)
        let passCount = 20
        recorder.midReconcileAsyncHook = { [weak scheduler] in
            if recorder.reconcileCount < passCount {
                scheduler?.enqueue(.reconcileLocal, coalescing: true)
            }
            await Task.yield()
        }
        recorder.midSyncAsyncHook = { [weak scheduler] in
            if recorder.syncRemoteCount < passCount {
                scheduler?.enqueue([.syncRemote, .notifyStructural])
            }
            await Task.yield()
        }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        scheduler.setActive(true)
        scheduler.enqueue([.reconcileLocal, .syncRemote, .notifyStructural])
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, passCount)
        XCTAssertEqual(recorder.syncRemoteCount, passCount)
        XCTAssertEqual(recorder.presentedReconcileCounts, Array(1...passCount))
        XCTAssertTrue(recorder.iterations.allSatisfy { $0.work == [.reconcileLocal, .syncRemote, .notifyStructural] })
    }

    func testBackgroundCommittedChangesCatchUpImmediatelyOnForeground() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, coalescingInterval: .seconds(60))
        let reconciled = expectation(description: "foreground catch-up bypasses delay")
        recorder.midReconcileAsyncHook = { reconciled.fulfill() }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        scheduler.setActive(false)
        for _ in 0..<100 { scheduler.enqueue(.reconcileLocal, coalescing: true) }
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 0)
        scheduler.setActive(true)
        await fulfillment(of: [reconciled], timeout: 3)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
    }

    func testBackgroundingDuringIncrementalRetainsFollowUpAndPresentation() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "incremental started")
        let gate = RestoreTestLatch()
        recorder.midReconcileAsyncHook = {
            if recorder.reconcileCount == 1 {
                started.fulfill()
                await gate.wait()
            }
        }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await fulfillment(of: [started], timeout: 3)
        scheduler.setActive(false)
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.iterations.count, 0)
        scheduler.setActive(true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 2)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertTrue(recorder.iterations.first?.work.contains(.notifyStructural) == true)
    }

    func testReleaseCancelsCoalescingTimer() async throws {
        let recorder = HookRecorder()
        var scheduler: HomeRefreshScheduler? = makeScheduler(recorder: recorder)
        weak var released = scheduler
        scheduler?.enqueue(.reconcileLocal, coalescing: true)
        scheduler = nil
        XCTAssertNil(released)
        try await Task.sleep(for: .milliseconds(30))
        XCTAssertEqual(recorder.reconcileCount, 0)
    }

    func testCancelledCallerStillEnqueuesCommittedChanges() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        recorder.midReconcileAsyncHook = { XCTAssertFalse(Task.isCancelled) }
        await Task {
            withUnsafeCurrentTask { $0?.cancel() }
            scheduler.enqueue(.reconcileLocal, coalescing: true)
        }.value
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
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

    func testBackgroundWorkCoalescesAndCatchesUpOnceOnForeground() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder, initiallyActive: false)
        for _ in 0 ..< 100 {
            scheduler.enqueue([.reconcileLocal, .syncRemote, .notifyStructural])
        }
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 0)
        XCTAssertEqual(recorder.syncRemoteCount, 0)
        XCTAssertEqual(recorder.presentationCount, 0)
        scheduler.setActive(true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentationCount, 1)
    }

    func testBackgroundAfterReloadDefersRemoteAndPresentationWithoutRepeatingReload() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        recorder.midReloadHook = { [weak scheduler] in scheduler?.setActive(false) }
        scheduler.enqueue([.reloadLocal, .reconcileLocal, .syncRemote, .notifyConnection])
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.reconcileCount, 0)
        XCTAssertEqual(recorder.syncRemoteCount, 0)
        XCTAssertEqual(recorder.presentationCount, 0)
        scheduler.setActive(true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentationCount, 1)
        XCTAssertTrue(recorder.iterations.last?.work.contains(.notifyConnection) == true)
    }

    func testBackgroundDuringRemoteSyncDefersOnlyPresentation() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        recorder.midSyncHook = { [weak scheduler] in scheduler?.setActive(false) }
        scheduler.enqueue([.syncRemote, .notifyStructural])
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentationCount, 0)
        scheduler.setActive(true)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentationCount, 1)
    }

    func testForegroundDuringOldSyncWaitsForNewCatchUpBeforePresentation() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "old sync started")
        let gate = RestoreTestLatch()
        recorder.midSyncAsyncHook = {
            if recorder.syncRemoteCount == 1 {
                started.fulfill()
                await gate.wait()
            } else {
                XCTAssertEqual(recorder.reconcileCount, 1)
                XCTAssertEqual(recorder.presentationCount, 0)
                XCTAssertEqual(recorder.iterations.count, 0)
            }
        }
        scheduler.enqueue([.syncRemote, .notifyConnection])
        await fulfillment(of: [started], timeout: 3)
        scheduler.setActive(false)
        scheduler.setActive(true)
        scheduler.enqueue([.reconcileLocal, .syncRemote, .notifyStructural])
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.syncRemoteCount, 2)
        XCTAssertEqual(recorder.presentationCount, 1)
        XCTAssertEqual(recorder.iterations.count, 1)
        XCTAssertTrue(recorder.iterations.last?.work.contains(.notifyConnection) == true)
    }

    func testForegroundDuringReconcileWaitsForFreshLocalAndRemotePass() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "old reconcile started")
        let gate = RestoreTestLatch()
        recorder.midReconcileAsyncHook = {
            if recorder.reconcileCount == 1 {
                started.fulfill()
                await gate.wait()
            } else {
                XCTAssertEqual(recorder.presentationCount, 0)
            }
        }
        recorder.midSyncHook = {
            XCTAssertEqual(recorder.reconcileCount, 2)
            XCTAssertEqual(recorder.presentationCount, 0)
        }
        scheduler.enqueue(.reconcileLocal, coalescing: true)
        await fulfillment(of: [started], timeout: 3)
        scheduler.setActive(false)
        for _ in 0..<100 { scheduler.enqueue(.reconcileLocal, coalescing: true) }
        scheduler.setActive(true)
        scheduler.enqueue([.reconcileLocal, .syncRemote, .notifyStructural])
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 2)
        XCTAssertEqual(recorder.syncRemoteCount, 1)
        XCTAssertEqual(recorder.presentedReconcileCounts, [2])
        XCTAssertTrue(recorder.iterations.last?.work.contains(.notifyStructural) == true)
    }

    func testScopeChangeDuringReconcileWaitsForReloadBeforePresentation() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "old scope reconcile started")
        let gate = RestoreTestLatch()
        recorder.reconcileMonths = [.init(year: 2026, month: 1)]
        recorder.midReconcileAsyncHook = {
            started.fulfill()
            await gate.wait()
        }
        recorder.midReloadHook = {
            XCTAssertEqual(recorder.presentationCount, 0)
        }
        scheduler.enqueue([.reconcileLocal, .notifyConnection])
        await fulfillment(of: [started], timeout: 3)
        scheduler.enqueue([.reloadLocal, .notifyStructural])
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.reloadCount, 1)
        XCTAssertEqual(recorder.presentationCount, 1)
        XCTAssertEqual(recorder.iterations.first?.work, [.reloadLocal, .notifyStructural, .notifyConnection])
    }

    func testConnectionChangeDuringSyncWaitsForNewConnectionBeforePresentation() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        let started = expectation(description: "old connection sync started")
        let gate = RestoreTestLatch()
        recorder.midSyncAsyncHook = {
            if recorder.syncRemoteCount == 1 {
                started.fulfill()
                await gate.wait()
            } else {
                XCTAssertEqual(recorder.presentationCount, 0)
            }
        }
        scheduler.enqueue([.syncRemote, .notifyStructural])
        await fulfillment(of: [started], timeout: 3)
        scheduler.enqueue([.syncRemote, .notifyConnection])
        await gate.open()
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.syncRemoteCount, 2)
        XCTAssertEqual(recorder.presentationCount, 1)
        XCTAssertEqual(recorder.iterations.first?.work, [.syncRemote, .notifyConnection, .notifyStructural])
    }

    func testRepeatedActiveStateDoesNotInvalidateCompletedWork() async {
        let recorder = HookRecorder()
        let scheduler = makeScheduler(recorder: recorder)
        recorder.midReconcileAsyncHook = { [weak scheduler] in
            scheduler?.setActive(true)
            scheduler?.setActive(true)
        }
        scheduler.enqueue(.reconcileLocal)
        await scheduler._testWaitUntilIdle()
        XCTAssertEqual(recorder.reconcileCount, 1)
        XCTAssertEqual(recorder.iterations.first?.work, .reconcileLocal)
        XCTAssertEqual(recorder.presentationCount, 1)
    }
}
