import XCTest
@testable import Watermelon

@MainActor
final class HomeLocalIndexReloadCoordinatorTests: XCTestCase {
    private final class State {
        var isBlocked = false
        var executionBlocked = false
        var maintenanceBlocked = false
        var hasQueuedOrRunningReload = false
        var enqueued: [HomeRefreshScheduler.Work] = []
        var availabilityChangeCount = 0
        var events: [String] = []
    }

    private func makeCoordinator(state: State) -> HomeLocalIndexReloadCoordinator {
        HomeLocalIndexReloadCoordinator(hooks: .init(
            isBlocked: { state.isBlocked },
            hasQueuedOrRunningReload: { state.hasQueuedOrRunningReload },
            enqueue: { work in
                state.events.append("enqueue")
                state.enqueued.append(work)
            },
            notifyAvailabilityChanged: {
                state.availabilityChangeCount += 1
            }
        ))
    }

    func testSchedulePassesThroughNonReloadWorkWhileBlocked() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule([.syncRemote, .notifyStructural])

        XCTAssertEqual(state.enqueued, [[.syncRemote, .notifyStructural]])
        XCTAssertEqual(state.availabilityChangeCount, 0)
        XCTAssertFalse(coordinator.isReloading)
    }

    func testScheduleDoesNotDrainPendingReloadWhileStillBlocked() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule(.reloadLocal) {
            state.events.append("pending")
        }
        coordinator.schedule(.syncRemote)

        XCTAssertEqual(state.enqueued, [.syncRemote])
        XCTAssertEqual(state.events, ["enqueue"])
        XCTAssertTrue(coordinator.isReloading)
    }

    func testScheduleDefersReloadWorkWhileBlockedAndReplaysWhenUnblocked() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule([.reloadLocal, .syncRemote, .notifyStructural])

        XCTAssertTrue(coordinator.isReloading)
        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertEqual(state.availabilityChangeCount, 1)

        state.isBlocked = false
        coordinator.replayIfPossible()

        XCTAssertEqual(state.enqueued, [[.reloadLocal, .syncRemote, .notifyStructural]])
        XCTAssertEqual(state.availabilityChangeCount, 1)
        XCTAssertFalse(coordinator.isReloading)
    }

    func testScheduleRunsCallbackAfterEnqueueWhenUnblocked() {
        let state = State()
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule(.reloadLocal) {
            state.events.append("onEnqueued")
        }

        XCTAssertEqual(state.events, ["enqueue", "onEnqueued"])
        XCTAssertEqual(state.enqueued, [.reloadLocal])
    }

    func testScheduleDefersCallbackUntilReplay() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule([.reloadLocal, .notifyStructural]) {
            state.events.append("onEnqueued")
        }

        XCTAssertTrue(state.events.isEmpty)
        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertTrue(coordinator.isReloading)

        state.isBlocked = false
        coordinator.replayIfPossible()

        XCTAssertEqual(state.events, ["enqueue", "onEnqueued"])
        XCTAssertEqual(state.enqueued, [[.reloadLocal, .notifyStructural]])
    }

    func testMultipleDeferredCallbacksReplayInOrderOnce() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule(.reloadLocal) {
            state.events.append("first")
        }
        coordinator.schedule([.reloadLocal, .syncRemote]) {
            state.events.append("second")
        }

        XCTAssertTrue(state.events.isEmpty)
        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertEqual(state.availabilityChangeCount, 2)

        state.isBlocked = false
        coordinator.replayIfPossible()
        coordinator.replayIfPossible()

        XCTAssertEqual(state.enqueued, [[.reloadLocal, .syncRemote]])
        XCTAssertEqual(state.events, ["enqueue", "first", "second"])
    }

    func testUnblockedScheduleDrainsStalePendingWork() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        coordinator.schedule(.reloadLocal) {
            state.events.append("pending")
        }
        state.isBlocked = false
        coordinator.schedule(.syncRemote) {
            state.events.append("fresh")
        }

        XCTAssertEqual(state.enqueued, [[.reloadLocal, .syncRemote]])
        XCTAssertEqual(state.events, ["enqueue", "pending", "fresh"])
        XCTAssertFalse(coordinator.isReloading)
    }

    func testReplayWaitsForBothIndependentBlockers() {
        let state = State()
        state.executionBlocked = true
        state.maintenanceBlocked = true
        let coordinator = HomeLocalIndexReloadCoordinator(hooks: .init(
            isBlocked: { state.executionBlocked || state.maintenanceBlocked },
            hasQueuedOrRunningReload: { state.hasQueuedOrRunningReload },
            enqueue: { work in
                state.events.append("enqueue")
                state.enqueued.append(work)
            },
            notifyAvailabilityChanged: {
                state.availabilityChangeCount += 1
            }
        ))

        coordinator.schedule(.reloadLocal)
        state.executionBlocked = false
        coordinator.replayIfPossible()
        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertTrue(coordinator.isReloading)

        state.maintenanceBlocked = false
        coordinator.replayIfPossible()
        XCTAssertEqual(state.enqueued, [.reloadLocal])
        XCTAssertFalse(coordinator.isReloading)
    }

    func testReplayKeepsPendingWorkWhileStillBlocked() {
        let state = State()
        state.isBlocked = true
        let coordinator = makeCoordinator(state: state)

        XCTAssertTrue(coordinator.deferIfBlocked(.reloadLocal))
        coordinator.replayIfPossible()

        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertTrue(coordinator.isReloading)
        XCTAssertEqual(state.availabilityChangeCount, 1)
    }

    func testReplayWithNoPendingWorkIsNoOp() {
        let state = State()
        let coordinator = makeCoordinator(state: state)

        coordinator.replayIfPossible()

        XCTAssertTrue(state.enqueued.isEmpty)
        XCTAssertEqual(state.availabilityChangeCount, 0)
        XCTAssertFalse(coordinator.isReloading)
    }

    func testIsReloadingIncludesSchedulerQueuedOrRunningWork() {
        let state = State()
        let coordinator = makeCoordinator(state: state)

        XCTAssertFalse(coordinator.isReloading)

        state.hasQueuedOrRunningReload = true
        XCTAssertTrue(coordinator.isReloading)
    }
}
