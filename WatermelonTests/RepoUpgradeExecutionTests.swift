import XCTest
@testable import Watermelon

final class RepoUpgradeExecutionTests: XCTestCase {
    private func months(_ count: Int) -> [LibraryMonthKey] {
        (1...count).map { LibraryMonthKey(year: 2026, month: $0) }
    }

    func testConfiguredConcurrencyBoundsWorkAndReusesConnections() async throws {
        for configured in [1, 3, 24] {
            let jobs = months(5)
            let expected = min(configured, jobs.count)
            let started = expectation(description: "first wave \(configured)")
            started.expectedFulfillmentCount = expected
            let gate = UpgradeTestGate()
            let state = UpgradeExecutionRecorder()
            let holder = ProbeClientHolder()
            let primary = ProbeStorageClient()
            let execution = RepoUpgradeExecution(workerCount: configured, makeClient: { holder.make(.succeed) })
            let task = Task {
                try await execution.run(months: jobs, client: primary, operation: { month, _ in
                    await state.begin(month)
                    if month.month <= expected {
                        started.fulfill()
                        await gate.wait()
                    }
                    await state.end()
                }, onMonthCompleted: { await state.progress($0) })
            }
            await fulfillment(of: [started], timeout: 5)
            await gate.open()
            try await task.value
            let result = await state.snapshot()
            XCTAssertEqual(result.peak, expected)
            XCTAssertEqual(Set(result.started), Set(jobs))
            XCTAssertEqual(result.started.count, jobs.count)
            XCTAssertEqual(result.progress, Array(1...jobs.count))
            XCTAssertEqual(holder.clients.count, expected - 1)
            for client in holder.clients {
                let disconnected = await client.didDisconnect
                XCTAssertTrue(disconnected)
            }
            let primaryDisconnected = await primary.didDisconnect
            XCTAssertFalse(primaryDisconnected)
        }
    }

    func testCompletionSchedulesNextMonthWithoutWaitingForSlowMonth() async throws {
        let slowStarted = expectation(description: "slow month started")
        let nextStarted = expectation(description: "next month started")
        let slowGate = UpgradeTestGate()
        let primary = ProbeStorageClient()
        let execution = RepoUpgradeExecution(workerCount: 2, makeClient: { ProbeStorageClient() })
        let task = Task {
            try await execution.run(months: months(3), client: primary, operation: { month, _ in
                if month.month == 1 {
                    slowStarted.fulfill()
                    await slowGate.wait()
                } else if month.month == 3 {
                    nextStarted.fulfill()
                }
            }, onMonthCompleted: { _ in })
        }
        await fulfillment(of: [slowStarted, nextStarted], timeout: 5)
        await slowGate.open()
        try await task.value
    }

    func testCancellationDrainsInFlightWorkBeforeReturningAndDisconnecting() async throws {
        let started = expectation(description: "workers started")
        started.expectedFulfillmentCount = 2
        let cancelled = expectation(description: "worker cancelled")
        let gate = UpgradeTestGate()
        let state = UpgradeExecutionRecorder()
        let holder = ProbeClientHolder()
        let primary = ProbeStorageClient()
        let execution = RepoUpgradeExecution(workerCount: 2, makeClient: { holder.make(.succeed) })
        let task = Task {
            do {
                try await execution.run(months: months(4), client: primary, operation: { month, _ in
                    await state.begin(month)
                    started.fulfill()
                    if month.month == 1 {
                        await gate.wait()
                    } else {
                        do { try await Task.sleep(for: .seconds(60)) }
                        catch { cancelled.fulfill(); throw error }
                    }
                }, onMonthCompleted: { _ in })
            } catch {
                await state.markReturned()
                throw error
            }
            await state.markReturned()
        }
        await fulfillment(of: [started], timeout: 5)
        task.cancel()
        await fulfillment(of: [cancelled], timeout: 5)
        let returnedEarly = await state.returned
        XCTAssertFalse(returnedEarly)
        await gate.open()
        do { try await task.value; XCTFail("Cancellation must prevent completion") }
        catch { XCTAssertTrue(error is CancellationError) }
        let result = await state.snapshot()
        XCTAssertEqual(result.started.count, 2)
        for client in holder.clients {
            let disconnected = await client.didDisconnect
            XCTAssertTrue(disconnected)
        }
        let primaryDisconnected = await primary.didDisconnect
        XCTAssertFalse(primaryDisconnected)
    }

    func testConnectionFailurePreventsSchedulingRemainingMonths() async throws {
        let primary = ProbeStorageClient()
        let execution = RepoUpgradeExecution(workerCount: 2, makeClient: {
            ProbeStorageClient(.throwError(RemoteStorageClientError.invalidConfiguration))
        })
        do {
            try await execution.run(months: months(4), client: primary, operation: { _, _ in
                try await Task.sleep(for: .seconds(60))
            }, onMonthCompleted: { _ in XCTFail("No month should complete") })
            XCTFail("A failed worker must fail the upgrade")
        } catch {
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
        let disconnected = await primary.didDisconnect
        XCTAssertFalse(disconnected)
    }

    func testMissingClientFactoryRunsSeriallyOnPrimary() async throws {
        let primary = ProbeStorageClient()
        let state = UpgradeExecutionRecorder()
        let execution = RepoUpgradeExecution(workerCount: 3)
        try await execution.run(months: months(4), client: primary, operation: { month, client in
            XCTAssertTrue((client as? ProbeStorageClient) === primary)
            await state.begin(month)
            await state.end()
        }, onMonthCompleted: { await state.progress($0) })
        let result = await state.snapshot()
        XCTAssertEqual(result.peak, 1)
        XCTAssertEqual(result.started, months(4))
        XCTAssertEqual(result.progress, [1, 2, 3, 4])
        let disconnected = await primary.didDisconnect
        XCTAssertFalse(disconnected)
    }
}

private actor UpgradeExecutionRecorder {
    private var active = 0
    private var peak = 0
    private var started: [LibraryMonthKey] = []
    private var counts: [Int] = []
    private(set) var returned = false

    func begin(_ month: LibraryMonthKey) {
        active += 1
        peak = max(peak, active)
        started.append(month)
    }

    func end() { active -= 1 }
    func progress(_ count: Int) { counts.append(count) }
    func markReturned() { returned = true }
    func snapshot() -> (peak: Int, started: [LibraryMonthKey], progress: [Int]) { (peak, started, counts) }
}
