import XCTest
@testable import Watermelon

@MainActor
final class HomeExecutionDataRefresherTests: XCTestCase {

    // A run-1 remote-sync task that is cancelled (cancel() nils the slot without awaiting it) and then
    // resumes after run 2 has spawned a fresh task + registered its waiter must NOT drain run 2's waiter
    // or clear its task slot. The new run's barrier must resolve with the live task's result.
    func testStaleCancelledTaskDoesNotResolveNewRunWaiter() async {
        let probe = RefresherProbe()
        let refresher = HomeExecutionDataRefresher(
            syncRemoteData: { await probe.sync() },
            refreshLocalIndex: { _ in [] }
        )

        // T1 (no waiter, modelling an upload-phase scheduleRemoteSync) suspends inside syncRemoteData.
        refresher.scheduleRemoteSync()
        await pollUntil { probe.t1Entered }

        // Stop / connection drop: cancel orphans T1 while it is still suspended and nils the task slot.
        refresher.cancel()

        // New run: registers a waiter; ensureRemoteSyncTask spawns T2 because the slot is nil.
        let waiter = Task { @MainActor in await refresher.syncRemoteDataAndWait() }
        await pollUntil { probe.t2Entered }

        // Release T1: its terminal cleanup runs now, while T2's waiter is still registered.
        probe.releaseT1()
        for _ in 0..<20 { await Task.yield() }

        // Release T2: it must be the one that resolves the waiter, with its own fresh result.
        probe.releaseT2()
        let result = await waiter.value

        XCTAssertEqual(
            result,
            [probe.monthX],
            "the new run's sync barrier must resolve with the live task's result, not a cancelled task's empty set"
        )
    }

    private func pollUntil(_ condition: @MainActor () -> Bool) async {
        for _ in 0..<600 {
            if condition() { return }
            try? await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTFail("condition not met within timeout")
    }
}

@MainActor
private final class RefresherProbe {
    let monthX = LibraryMonthKey(year: 2024, month: 7)
    private(set) var t1Entered = false
    private(set) var t2Entered = false
    private var call = 0
    private var gateA: CheckedContinuation<Void, Never>?
    private var gateB: CheckedContinuation<Void, Never>?

    func sync() async -> Set<LibraryMonthKey> {
        call += 1
        if call == 1 {
            t1Entered = true
            await withCheckedContinuation { gateA = $0 }
            return []
        } else {
            t2Entered = true
            await withCheckedContinuation { gateB = $0 }
            return [monthX]
        }
    }

    func releaseT1() {
        gateA?.resume()
        gateA = nil
    }

    func releaseT2() {
        gateB?.resume()
        gateB = nil
    }
}
