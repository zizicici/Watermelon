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

    func testPreflightFailureResumedOffMainDeliversCallbacksOnMainThread() async throws {
        let harness = try makeHarness()
        defer {
            try? harness.dependencies.databaseManager.dbQueue.close()
            try? FileManager.default.removeItem(at: harness.directory)
        }
        let month = LibraryMonthKey(year: 2024, month: 8)
        var stateNotificationCount = 0
        var receivedAlert = false

        harness.coordinator.onStateChanged = {
            XCTAssertTrue(Thread.isMainThread)
            stateNotificationCount += 1
        }
        harness.coordinator.onAlert = { _, _ in
            XCTAssertTrue(Thread.isMainThread)
            receivedAlert = true
        }

        XCTAssertTrue(harness.coordinator.enter(backup: [], download: [month], complement: []))
        await waitUntil { harness.builder.didEnter }
        let notificationsBeforeRelease = stateNotificationCount
        let builder = harness.builder

        await Task.detached(priority: .userInitiated) {
            builder.releaseIncomplete()
        }.value

        await waitUntil { receivedAlert }
        XCTAssertTrue(receivedAlert)
        XCTAssertGreaterThan(stateNotificationCount, notificationsBeforeRelease)

        harness.coordinator.exit()
        await waitUntil { !harness.dependencies.appRuntimeFlags.isExecuting }
        XCTAssertFalse(harness.dependencies.appRuntimeFlags.isExecuting)
    }

    func testManualLocalIndexOwnsExecutionClaimBeforeWorkStarts() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("local-index-execution-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        var dependencies: DependencyContainer? = DependencyContainer(
            databaseManager: database,
            startProfileReachability: false,
            reconcileOneDriveAccounts: false
        )
        let flags = try XCTUnwrap(dependencies?.appRuntimeFlags)
        var coordinator: LocalIndexBuildCoordinator? = dependencies?.localIndexBuildCoordinator

        XCTAssertTrue(coordinator?.start(mode: .incremental, initialIndexed: 0) == true)
        XCTAssertTrue(flags.isExecuting)
        if let unexpectedClaim = flags.tryEnterExecution() {
            flags.exitExecution(unexpectedClaim)
            XCTFail("manual local index did not retain the execution claim")
        }

        coordinator?.cancel()
        coordinator = nil
        dependencies = nil
        XCTAssertFalse(flags.isExecuting)
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
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let shouldResume = lock.withLock { () -> Bool in
                    if released {
                        entered = true
                        return true
                    }
                    self.continuation = continuation
                    entered = true
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

    func releaseIncomplete() {
        let continuation = lock.withLock { () -> CheckedContinuation<LocalHashIndexBuildResult, Error>? in
            released = true
            let continuation = self.continuation
            self.continuation = nil
            return continuation
        }
        continuation?.resume(returning: Self.incompleteResult(for: ["asset"]))
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

    private static func incompleteResult(for assetIDs: Set<String>) -> LocalHashIndexBuildResult {
        LocalHashIndexBuildResult(
            requestedAssetIDs: assetIDs,
            readyAssetIDs: [],
            unavailableAssetIDs: [],
            failedAssetIDs: assetIDs,
            missingAssetIDs: [],
            networkPendingAssetIDs: []
        )
    }
}

private struct LocalIndexBuilderFailure: Error {}
