import XCTest
@testable import Watermelon

@MainActor
final class HomeExecutionCoordinatorLifecycleTests: XCTestCase {
    func testImmediateStopPreventsDownloadPreflightFromStarting() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 3)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        harness.coordinator.stop()

        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.builder.didEnter)
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testImmediatePauseSettlesWithoutStartingDownloadPreflight() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 4)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        harness.coordinator.pause()

        await waitUntil {
            harness.coordinator.currentState?.phase == .downloadPaused
                && harness.coordinator.currentState?.controlState == .idle
        }
        XCTAssertFalse(harness.builder.didEnter)
        XCTAssertTrue(harness.dependencies.appRuntimeFlags.isExecuting)

        harness.coordinator.stop()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testDirectStopCancelsDownloadPreflightButKeepsClaimUntilSettlement() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 1)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }
        XCTAssertTrue(harness.builder.didEnter)

        harness.coordinator.stop()
        await waitUntil { harness.builder.didObserveCancellation }
        XCTAssertTrue(harness.builder.didObserveCancellation)
        assertExecutionClaimIsHeld(harness.dependencies.appRuntimeFlags)

        harness.builder.release()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testPauseThenStopCancelsDownloadPreflightAndWaitsForSettlement() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 2)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }
        XCTAssertTrue(harness.builder.didEnter)

        harness.coordinator.pause()
        await Task.yield()
        XCTAssertFalse(harness.builder.didObserveCancellation)

        harness.coordinator.stop()
        await waitUntil { harness.builder.didObserveCancellation }
        XCTAssertTrue(harness.builder.didObserveCancellation)
        assertExecutionClaimIsHeld(harness.dependencies.appRuntimeFlags)

        harness.builder.release()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testStopPreflightDomainErrorStillReleasesExecutionClaim() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 5)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }
        XCTAssertTrue(harness.builder.didEnter)

        harness.coordinator.stop()
        await waitUntil { harness.builder.didObserveCancellation }
        XCTAssertTrue(harness.builder.didObserveCancellation)
        assertExecutionClaimIsHeld(harness.dependencies.appRuntimeFlags)

        harness.builder.fail()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testPauseSettlesAfterDownloadPreflightReturns() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 6)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }

        harness.coordinator.pause()
        harness.builder.release()

        await waitUntil {
            harness.coordinator.currentState?.phase == .downloadPaused
                && harness.coordinator.currentState?.controlState == .idle
        }
        XCTAssertTrue(harness.dependencies.appRuntimeFlags.isExecuting)

        harness.coordinator.stop()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testPausePreflightDomainErrorStillSettlesPaused() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 7)

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }

        harness.coordinator.pause()
        harness.builder.fail()

        await waitUntil {
            harness.coordinator.currentState?.phase == .downloadPaused
                && harness.coordinator.currentState?.controlState == .idle
        }
        XCTAssertTrue(harness.dependencies.appRuntimeFlags.isExecuting)

        harness.coordinator.stop()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    private func makeHarness() throws -> (
        coordinator: HomeExecutionCoordinator,
        dependencies: DependencyContainer,
        builder: BlockingLocalHashIndexBuilder,
        directory: URL
    ) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("home-execution-lifecycle-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let dependencies = DependencyContainer(
            databaseManager: database,
            startProfileReachability: false,
            reconcileOneDriveAccounts: false
        )
        let builder = BlockingLocalHashIndexBuilder()
        let dataAccess = HomeExecutionCoordinator.DataAccess(
            localAssetIDs: { _ in ["asset"] },
            localMonthGroupingTimeZone: { .fixedUTC() },
            remoteOnlyItems: { _ in [] },
            syncRemoteData: { [] },
            refreshLocalIndex: { _ in [] }
        )
        let coordinator = HomeExecutionCoordinator(
            dependencies: dependencies,
            dataAccess: dataAccess,
            localHashIndexBuildService: builder
        )
        return (coordinator, dependencies, builder, directory)
    }

    private func waitUntil(
        attempts: Int = 500,
        _ condition: () -> Bool
    ) async {
        for _ in 0..<attempts {
            if condition() { return }
            try? await Task.sleep(for: .milliseconds(2))
        }
    }

    private func assertExecutionClaimIsHeld(
        _ flags: AppRuntimeFlags,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        if let unexpectedClaim = flags.tryEnterExecution() {
            flags.exitExecution(unexpectedClaim)
            XCTFail("execution claim was released before the preflight task settled", file: file, line: line)
        }
    }
}

private final class BlockingLocalHashIndexBuilder: LocalHashIndexBuilding, @unchecked Sendable {
    private let lock = NSLock()
    private var entered = false
    private var cancellationObserved = false
    private var released = false
    private var continuation: CheckedContinuation<LocalHashIndexBuildResult, Error>?

    var didEnter: Bool {
        lock.withLock { entered }
    }

    var didObserveCancellation: Bool {
        lock.withLock { cancellationObserved }
    }

    func buildIndex(
        for assetIDs: Set<String>,
        workerCount _: Int,
        allowNetworkAccess _: Bool,
        progressHandler _: LocalHashIndexProgressHandler?,
        tickHandler _: LocalHashIndexProgressTickHandler?
    ) async throws -> LocalHashIndexBuildResult {
        lock.withLock { entered = true }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let shouldResume = lock.withLock { () -> Bool in
                    if released { return true }
                    self.continuation = continuation
                    return false
                }
                if shouldResume {
                    continuation.resume(returning: Self.emptyResult(for: assetIDs))
                }
            }
        } onCancel: {
            self.lock.withLock { self.cancellationObserved = true }
        }
    }

    func release() {
        let continuation = lock.withLock { () -> CheckedContinuation<LocalHashIndexBuildResult, Error>? in
            released = true
            let continuation = self.continuation
            self.continuation = nil
            return continuation
        }
        continuation?.resume(returning: Self.emptyResult(for: ["asset"]))
    }

    func fail() {
        let continuation = lock.withLock { () -> CheckedContinuation<LocalHashIndexBuildResult, Error>? in
            released = true
            let continuation = self.continuation
            self.continuation = nil
            return continuation
        }
        continuation?.resume(throwing: LocalIndexBuilderFailure())
    }

    private static func emptyResult(for assetIDs: Set<String>) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: assetIDs,
            readyAssetIDs: [],
            unavailableAssetIDs: [],
            failedAssetIDs: [],
            missingAssetIDs: [],
            networkPendingAssetIDs: []
        )
    }
}

private struct LocalIndexBuilderFailure: Error {}
