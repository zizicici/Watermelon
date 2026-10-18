import XCTest
@testable import Watermelon

@MainActor
final class MediaBrowserReloadSchedulerTests: XCTestCase {
    private typealias Request = MediaBrowserReloadScheduler.Request

    private func request(_ trigger: String, attempt: Int = 0) -> Request {
        Request(trigger: trigger, staleAttempt: attempt)
    }

    func testBackgroundRequestsMergeAndReplayAsynchronouslyWhenForegrounded() async {
        let reloaded = expectation(description: "foreground browser reloads")
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 60) {
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }

        scheduler.request(request("first"), delay: 0)
        scheduler.request(request("latest", attempt: 2))
        await Task.yield()
        XCTAssertTrue(received.isEmpty)

        scheduler.update(isActive: true, isInteractionBlocked: false)
        scheduler.update(isActive: true, isInteractionBlocked: false)
        XCTAssertTrue(received.isEmpty)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.count, 1)
        XCTAssertEqual(received.first?.trigger, "latest")
        XCTAssertEqual(received.first?.staleAttempt, 2)
    }

    func testAutomaticBurstUsesOneDelayedWindow() async {
        let reloaded = expectation(description: "burst reloads once")
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 0.01) {
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false)

        for index in 0..<100 {
            scheduler.request(request("change-\(index)", attempt: index))
        }
        XCTAssertTrue(received.isEmpty)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.count, 1)
        XCTAssertEqual(received.first?.trigger, "change-99")
        XCTAssertEqual(received.first?.staleAttempt, 99)
    }

    func testImmediateRequestAdvancesTimerAndAutomaticRequestCannotDelayIt() async {
        let reloaded = expectation(description: "immediate request reloads")
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 60) {
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false)

        scheduler.request(request("initial"))
        scheduler.request(request("user"), delay: 0)
        scheduler.request(request("automatic", attempt: 3))
        XCTAssertTrue(received.isEmpty)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.count, 1)
        XCTAssertEqual(received.first?.trigger, "automatic")
        XCTAssertEqual(received.first?.staleAttempt, 3)
    }

    func testContinuousChangesDoNotRestartCoalescingWindow() async throws {
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 0.03) { received.append($0) }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false)

        for index in 0..<30 {
            scheduler.request(request("change", attempt: index))
            try await Task.sleep(for: .milliseconds(5))
        }
        XCTAssertFalse(received.isEmpty)
        XCTAssertLessThan(received.first?.staleAttempt ?? 30, 29)
        XCTAssertLessThan(received.count, 15)
    }

    func testBackgroundingCancelsScheduledWorkAndPreservesPendingRequest() async {
        let reloaded = expectation(description: "foreground replays pending work")
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 60) {
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false)
        scheduler.request(request("pending"), delay: 0)
        scheduler.update(isActive: false, isInteractionBlocked: false)
        await Task.yield()
        XCTAssertTrue(received.isEmpty)

        scheduler.update(isActive: true, isInteractionBlocked: false)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.count, 1)
        XCTAssertEqual(received.first?.trigger, "pending")
    }

    func testInteractionBlocksExplicitRequestsUntilAllGatesAllowReload() async {
        let reloaded = expectation(description: "all gates restored")
        var received: [Request] = []
        let scheduler = MediaBrowserReloadScheduler(delay: 60) {
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false)
        scheduler.request(request("explicit"), delay: 0)
        scheduler.update(isActive: true, isInteractionBlocked: true)
        scheduler.request(request("latest"))
        await Task.yield()
        XCTAssertTrue(received.isEmpty)

        scheduler.update(isActive: false, isInteractionBlocked: false)
        await Task.yield()
        XCTAssertTrue(received.isEmpty)
        scheduler.update(isActive: true, isInteractionBlocked: false)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.count, 1)
        XCTAssertEqual(received.first?.trigger, "latest")
    }

    func testDrainClearsPendingBeforeCallbackCanRequestAnotherReload() async {
        let reloaded = expectation(description: "two distinct reloads")
        reloaded.expectedFulfillmentCount = 2
        var received: [Request] = []
        var scheduler: MediaBrowserReloadScheduler!
        scheduler = MediaBrowserReloadScheduler(delay: 60) {
            received.append($0)
            if received.count == 1 {
                scheduler.request(self.request("follow-up"), delay: 0)
                XCTAssertEqual(received.count, 1)
            }
            reloaded.fulfill()
        }
        defer { scheduler = nil }
        scheduler.update(isActive: true, isInteractionBlocked: false)
        scheduler.request(request("first"), delay: 0)

        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.map(\.trigger), ["first", "follow-up"])
    }

    func testReleaseCancelsPendingTimer() async {
        let reloaded = expectation(description: "released scheduler does not reload")
        reloaded.isInverted = true
        var scheduler: MediaBrowserReloadScheduler? = MediaBrowserReloadScheduler(delay: 0.01) { _ in
            reloaded.fulfill()
        }
        weak var released = scheduler
        scheduler?.update(isActive: true, isInteractionBlocked: false)
        scheduler?.request(request("pending"))
        scheduler = nil

        XCTAssertNil(released)
        await fulfillment(of: [reloaded], timeout: 0.05)
    }

    func testLoadCompletionPreservesCoalescingDelayForTrailingChanges() async {
        let reloaded = expectation(description: "one delayed trailing reload")
        var received: [Request] = []
        var resumedAt = Date.distantPast
        let scheduler = MediaBrowserReloadScheduler(delay: 0.05) {
            XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(resumedAt), 0.04)
            received.append($0)
            reloaded.fulfill()
        }
        defer { withExtendedLifetime(scheduler) {} }
        scheduler.update(isActive: true, isInteractionBlocked: false, isLoading: true)
        for index in 0..<100 { scheduler.request(request("change-\(index)")) }
        await Task.yield()
        XCTAssertTrue(received.isEmpty)

        resumedAt = Date()
        scheduler.update(isActive: true, isInteractionBlocked: false, isLoading: false)
        await fulfillment(of: [reloaded], timeout: 3)
        XCTAssertEqual(received.map(\.trigger), ["change-99"])
    }
}
