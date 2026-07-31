import XCTest
@testable import Watermelon

@MainActor
final class BackupSessionHardCancelTests: XCTestCase {
    func testRunDriverRejectsAlreadyDrainedControl() {
        let runDriver = BackupRunDriver { _, _ in }
        let control = ExecutionTerminationControl()
        control.request(.pause)
        let configuration = BackupRunConfigurationOverride(
            workerCountOverride: 1,
            iCloudPhotoBackupMode: .disable,
            monthGroupingTimeZone: .fixedUTC()
        )

        let runToken = runDriver.startRun(
            profile: makeProfile(),
            password: "password",
            mode: .full,
            displayMode: .full,
            configuration: configuration,
            terminationControl: control,
            onEvent: { _, _, _, _ in false },
            onError: { _, _, _, _, _ in }
        )

        XCTAssertNil(runToken)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testRunTaskRemainsTrackedUntilOperationReturnsAfterTerminalEvent() async throws {
        let returnLatch = RunReturnLatch()
        let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: true)
        let runDriver = BackupRunDriver { _, eventStream in
            eventStream.emit(.finished(result))
            await returnLatch.wait()
        }
        let configuration = BackupRunConfigurationOverride(
            workerCountOverride: 1,
            iCloudPhotoBackupMode: .disable,
            monthGroupingTimeZone: .fixedUTC()
        )
        var handledTerminalEvent = false

        let runToken = runDriver.startRun(
            profile: makeProfile(),
            password: "password",
            mode: .full,
            displayMode: .full,
            configuration: configuration,
            onEvent: { event, _, _, _ in
                if case .finished = event {
                    handledTerminalEvent = true
                    runDriver.clearActiveRunState()
                    return true
                }
                return false
            },
            onError: { _, _, _, _, _ in }
        )
        XCTAssertNotNil(runToken)

        for _ in 0..<100 where !handledTerminalEvent {
            await Task.yield()
        }
        XCTAssertTrue(handledTerminalEvent)
        XCTAssertTrue(runDriver.hasActiveRunTask)

        let overlappingRun = runDriver.startRun(
            profile: makeProfile(),
            password: "password",
            mode: .full,
            displayMode: .full,
            configuration: configuration,
            onEvent: { _, _, _, _ in false },
            onError: { _, _, _, _, _ in }
        )
        XCTAssertNil(overlappingRun)

        let settlement = try XCTUnwrap(runDriver.cancelActiveRunImmediately())
        XCTAssertTrue(runDriver.hasActiveRunTask)
        await returnLatch.release()
        _ = await settlement.value
        for _ in 0..<100 where runDriver.hasActiveRunTask {
            await Task.yield()
        }
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testFinishedEventObserverPauseUsesLatestControlIntent() async throws {
        try await assertFinishedEventObserverIntent(
            expectedState: .paused,
            requestIntent: { $0.pauseBackup() }
        )
    }

    func testFinishedEventObserverStopUsesLatestControlIntent() async throws {
        try await assertFinishedEventObserverIntent(
            expectedState: .stopped,
            requestIntent: { $0.stopBackup() }
        )
    }

    func testFinishedEventObserverPauseThenStopUsesUpgradedControlIntent() async throws {
        try await assertFinishedEventObserverIntent(
            expectedState: .stopped,
            requestIntent: {
                $0.pauseBackup()
                $0.stopBackup()
            }
        )
    }

    func testStopAfterOperationReturnStillReachesPendingTerminalControl() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-terminal-pending-stop-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = TerminalPendingRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where runDriver.startRunCount == 0 {
            await Task.yield()
        }
        XCTAssertEqual(runDriver.startRunCount, 1)

        runDriver.markOperationReturned()
        controller.stopBackup()
        runDriver.deliverFinished()

        XCTAssertEqual(controller.snapshot().state, .stopped)
        XCTAssertEqual(runDriver.terminationIntent, .stop)
    }

    func testHardCancelSettlesBlockedStartCommandWithoutLaunchingRun() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-hard-cancel-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = BlockingStartRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where !runDriver.didEnterWait {
            await Task.yield()
        }
        XCTAssertTrue(runDriver.didEnterWait)

        let settlement = controller.cancelBackupImmediately()
        runDriver.releaseWait(simulatingRacedRun: true)
        _ = await settlement.value

        XCTAssertEqual(runDriver.startRunCount, 0)
        XCTAssertEqual(runDriver.cancelCallCount, 2)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testHardCancelFromStartingObserverCapturesQueuedStart() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-start-observer-cancel-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = BlockingStartRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )
        var settlement: Task<Void, Never>?
        let observerID = controller.addObserver { snapshot in
            guard snapshot.controlPhase == .starting, settlement == nil else { return }
            settlement = controller.cancelBackupImmediately()
        }
        defer { controller.removeObserver(observerID) }

        XCTAssertTrue(controller.startBackup())
        let cancellationSettlement = try XCTUnwrap(settlement)
        _ = await cancellationSettlement.value

        XCTAssertEqual(runDriver.startRunCount, 0)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testHardCancelRejectsObserverReentrantStartUntilSettlement() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-hard-cancel-reentrant-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = BlockingStartRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )
        var reentrantStartResult: Bool?
        let observerID = controller.addObserver { snapshot in
            guard snapshot.state == .paused,
                  snapshot.controlPhase == .idle,
                  reentrantStartResult == nil else { return }
            reentrantStartResult = controller.startBackup()
        }
        defer { controller.removeObserver(observerID) }

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where !runDriver.didEnterWait {
            await Task.yield()
        }

        let settlement = controller.cancelBackupImmediately()
        runDriver.releaseWait()
        _ = await settlement.value

        XCTAssertEqual(reentrantStartResult, false)
        XCTAssertEqual(runDriver.startRunCount, 0)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testHardCancelInvalidatesStartWaitingForPreviousTransition() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-hard-cancel-waiter-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = BlockingStartRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where !runDriver.didEnterWait {
            await Task.yield()
        }
        let waitingStart = Task { @MainActor in
            await controller.startBackupWhenReady()
        }
        await Task.yield()

        let settlement = controller.cancelBackupImmediately()
        runDriver.releaseWait()
        _ = await settlement.value

        let waitingStartResult = await waitingStart.value
        XCTAssertFalse(waitingStartResult)
        XCTAssertEqual(runDriver.startRunCount, 0)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    func testStopCancelsStartQueuedBehindReturningRun() async throws {
        try await assertIntentCancelsStartQueuedBehindReturningRun(
            expectedState: .stopped,
            requestIntent: { $0.stopBackup() }
        )
    }

    func testPauseCancelsStartQueuedBehindReturningRun() async throws {
        try await assertIntentCancelsStartQueuedBehindReturningRun(
            expectedState: .paused,
            requestIntent: { $0.pauseBackup() }
        )
    }

    func testStopCancelsResumeQueuedBehindReturningRun() async throws {
        try await assertIntentCancelsResumeQueuedBehindReturningRun(
            expectedState: .stopped,
            requestIntent: { $0.stopBackup() }
        )
    }

    func testPauseCancelsResumeQueuedBehindReturningRun() async throws {
        try await assertIntentCancelsResumeQueuedBehindReturningRun(
            expectedState: .paused,
            requestIntent: { $0.pauseBackup() }
        )
    }

    func testPauseWinsWhenResumePlannerFailsAfterCancellation() async throws {
        try await assertIntentWinsResumePlannerFailure(
            expectedState: .paused,
            requestIntent: { $0.pauseBackup() }
        )
    }

    func testStopWinsWhenResumePlannerFailsAfterCancellation() async throws {
        try await assertIntentWinsResumePlannerFailure(
            expectedState: .stopped,
            requestIntent: { $0.stopBackup() }
        )
    }

    func testHardCancelFromResumingObserverCapturesQueuedResume() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-resume-observer-cancel-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = ReturningPausedRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )
        let assetIDs: Set<String> = ["asset"]
        XCTAssertTrue(controller.updateScopeSelection(BackupScopeSelection(
            selectedAssetIDs: assetIDs,
            selectedAssetCount: assetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: assetIDs.count,
            totalEstimatedBytes: nil
        )))
        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where controller.snapshot().state != .paused {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, .paused)

        var settlement: Task<Void, Never>?
        let observerID = controller.addObserver { snapshot in
            guard snapshot.controlPhase == .resuming, settlement == nil else { return }
            settlement = controller.cancelBackupImmediately()
        }
        defer { controller.removeObserver(observerID) }

        XCTAssertTrue(controller.startBackup())
        let cancellationSettlement = try XCTUnwrap(settlement)
        _ = await cancellationSettlement.value

        XCTAssertEqual(runDriver.startRunCount, 1)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    private func assertIntentCancelsStartQueuedBehindReturningRun(
        expectedState: BackupSessionController.State,
        requestIntent: (BackupSessionController) -> Void
    ) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-queued-start-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = BlockingStartRunDriver(hasActiveRunTask: true)
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where !runDriver.didEnterWait {
            await Task.yield()
        }
        XCTAssertTrue(runDriver.didEnterWait)

        requestIntent(controller)
        runDriver.releaseWait()

        for _ in 0..<100 where controller.snapshot().controlPhase != .idle {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, expectedState)
        XCTAssertEqual(runDriver.startRunCount, 0)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    private func assertFinishedEventObserverIntent(
        expectedState: BackupSessionController.State,
        requestIntent: @escaping (BackupSessionController) -> Void
    ) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-terminal-observer-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let result = BackupExecutionResult(
            total: 1,
            succeeded: 1,
            failed: 0,
            skipped: 0,
            paused: false
        )
        let runDriver = BackupRunDriver { _, eventStream in
            eventStream.emit(.finished(result))
        }
        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )
        var didRequestIntent = false
        let observerID = controller.addEventObserver { event in
            guard case .finished = event, !didRequestIntent else { return }
            didRequestIntent = true
            requestIntent(controller)
        }
        defer { controller.removeEventObserver(observerID) }

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where controller.snapshot().state == .running {
            await Task.yield()
        }

        XCTAssertTrue(didRequestIntent)
        XCTAssertEqual(controller.snapshot().state, expectedState)
    }

    private func assertIntentCancelsResumeQueuedBehindReturningRun(
        expectedState: BackupSessionController.State,
        requestIntent: (BackupSessionController) -> Void
    ) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-queued-resume-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = ReturningPausedRunDriver()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService()
        )
        let assetIDs: Set<String> = ["asset"]
        XCTAssertTrue(controller.updateScopeSelection(BackupScopeSelection(
            selectedAssetIDs: assetIDs,
            selectedAssetCount: assetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: assetIDs.count,
            totalEstimatedBytes: nil
        )))

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where controller.snapshot().state != .paused {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, .paused)
        XCTAssertTrue(runDriver.hasActiveRunTask)
        XCTAssertEqual(runDriver.startRunCount, 1)

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where runDriver.waitCallCount < 2 {
            await Task.yield()
        }
        XCTAssertEqual(runDriver.waitCallCount, 2)

        requestIntent(controller)
        runDriver.releaseReturningRun()

        for _ in 0..<100 where controller.snapshot().controlPhase != .idle {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, expectedState)
        XCTAssertEqual(runDriver.startRunCount, 1)
        XCTAssertFalse(runDriver.hasActiveRunTask)
    }

    private func assertIntentWinsResumePlannerFailure(
        expectedState: BackupSessionController.State,
        requestIntent: (BackupSessionController) -> Void
    ) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("backup-resume-planner-failure-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }

        let appSession = AppSession()
        appSession.activate(profile: makeProfile(), password: "password")
        let runDriver = ReturningPausedRunDriver()
        let resumePlanner = FailingResumePlanner()
        let controller = BackupSessionController(
            runDriver: runDriver,
            appSession: appSession,
            databaseManager: database,
            photoLibraryService: PhotoLibraryService(),
            resumePlanner: resumePlanner
        )

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where controller.snapshot().state != .paused {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, .paused)

        XCTAssertTrue(controller.startBackup())
        for _ in 0..<100 where !resumePlanner.didEnter {
            await Task.yield()
        }
        XCTAssertTrue(resumePlanner.didEnter)

        requestIntent(controller)
        resumePlanner.fail()

        for _ in 0..<100 where controller.snapshot().controlPhase != .idle {
            await Task.yield()
        }
        XCTAssertEqual(controller.snapshot().state, expectedState)
        XCTAssertEqual(runDriver.startRunCount, 1)
        runDriver.releaseReturningRun()
    }

    private func makeProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: 1,
            name: "Test",
            storageType: StorageType.webdav.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "example.invalid",
            port: 443,
            shareName: "",
            basePath: "/Photos",
            username: "user",
            domain: nil,
            credentialRef: "test",
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}

private actor RunReturnLatch {
    private var released = false
    private var continuation: CheckedContinuation<Void, Never>?

    func wait() async {
        if released { return }
        await withCheckedContinuation { continuation in
            if released {
                continuation.resume()
            } else {
                self.continuation = continuation
            }
        }
    }

    func release() {
        released = true
        continuation?.resume()
        continuation = nil
    }
}

@MainActor
private final class FailingResumePlanner: BackupResumePlanning {
    private var continuation: CheckedContinuation<BackupResumePlan, Error>?
    private(set) var didEnter = false

    func makePlan(
        pausedMode _: BackupRunMode,
        completedAssetIDs _: Set<String>
    ) async throws -> BackupResumePlan {
        didEnter = true
        return try await withCheckedThrowingContinuation { continuation in
            self.continuation = continuation
        }
    }

    func fail() {
        continuation?.resume(throwing: ResumePlannerFailure())
        continuation = nil
    }
}

private struct ResumePlannerFailure: Error {}

@MainActor
private final class BlockingStartRunDriver: BackupRunDriving {
    private var waitContinuation: CheckedContinuation<Void, Never>?
    private(set) var didEnterWait = false
    private(set) var startRunCount = 0
    private(set) var cancelCallCount = 0
    private(set) var hasActiveRunTask: Bool
    private(set) var activeRunConfiguration: BackupRunConfigurationOverride?

    init(hasActiveRunTask: Bool = false) {
        self.hasActiveRunTask = hasActiveRunTask
    }

    func matchesActiveRunToken(_: UInt64) -> Bool {
        false
    }

    func waitForPreviousRunToClear() async throws {
        didEnterWait = true
        await withCheckedContinuation { continuation in
            waitContinuation = continuation
        }
    }

    func releaseWait(simulatingRacedRun: Bool = false) {
        hasActiveRunTask = simulatingRacedRun
        waitContinuation?.resume()
        waitContinuation = nil
    }

    func startRun(
        profile _: ServerProfileRecord,
        password _: String,
        mode _: BackupRunMode,
        displayMode _: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded _: BackupMonthFinalizer?,
        terminationControl _: ExecutionTerminationControl,
        onEvent _: @escaping BackupRunDriver.EventHandler,
        onError _: @escaping BackupRunDriver.ErrorHandler
    ) -> UInt64? {
        startRunCount += 1
        hasActiveRunTask = true
        activeRunConfiguration = configuration
        return 1
    }

    func requestTermination(_: ExecutionTerminationIntent) {}

    func clearActiveRunState() {
        hasActiveRunTask = false
        activeRunConfiguration = nil
    }

    func cancelActiveRunImmediately() -> Task<Void, Never>? {
        cancelCallCount += 1
        let hadActiveRun = hasActiveRunTask
        hasActiveRunTask = false
        activeRunConfiguration = nil
        return hadActiveRun ? Task {} : nil
    }
}

@MainActor
private final class ReturningPausedRunDriver: BackupRunDriving {
    private var waitContinuation: CheckedContinuation<Void, Never>?
    private var activeRunToken: UInt64 = 0
    private(set) var waitCallCount = 0
    private(set) var startRunCount = 0
    private(set) var hasActiveRunTask = false
    private(set) var activeRunConfiguration: BackupRunConfigurationOverride?

    func matchesActiveRunToken(_ runToken: UInt64) -> Bool {
        runToken == activeRunToken
    }

    func waitForPreviousRunToClear() async throws {
        waitCallCount += 1
        guard hasActiveRunTask else { return }
        await withCheckedContinuation { continuation in
            waitContinuation = continuation
        }
    }

    func startRun(
        profile _: ServerProfileRecord,
        password _: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded _: BackupMonthFinalizer?,
        terminationControl: ExecutionTerminationControl,
        onEvent: @escaping BackupRunDriver.EventHandler,
        onError _: @escaping BackupRunDriver.ErrorHandler
    ) -> UInt64? {
        guard !hasActiveRunTask else { return nil }
        activeRunToken &+= 1
        let runToken = activeRunToken
        startRunCount += 1
        hasActiveRunTask = true
        activeRunConfiguration = configuration
        let result = BackupExecutionResult(
            total: 1,
            succeeded: 0,
            failed: 0,
            skipped: 0,
            paused: true
        )
        Task { @MainActor in
            _ = onEvent(.finished(result), mode, displayMode, terminationControl)
        }
        return runToken
    }

    func requestTermination(_: ExecutionTerminationIntent) {}

    func clearActiveRunState() {
        activeRunConfiguration = nil
    }

    func cancelActiveRunImmediately() -> Task<Void, Never>? {
        let hadActiveRun = hasActiveRunTask
        releaseReturningRun()
        return hadActiveRun ? Task {} : nil
    }

    func releaseReturningRun() {
        hasActiveRunTask = false
        waitContinuation?.resume()
        waitContinuation = nil
    }
}

@MainActor
private final class TerminalPendingRunDriver: BackupRunDriving {
    private var activeRunToken: UInt64 = 0
    private var terminationControl: ExecutionTerminationControl?
    private var eventHandler: BackupRunDriver.EventHandler?
    private var runMode: BackupRunMode = .full
    private var displayMode: BackupRunMode = .full
    private(set) var startRunCount = 0
    private(set) var hasActiveRunTask = false
    private(set) var activeRunConfiguration: BackupRunConfigurationOverride?

    var terminationIntent: ExecutionTerminationIntent {
        terminationControl?.terminationIntent ?? .none
    }

    func matchesActiveRunToken(_ runToken: UInt64) -> Bool {
        runToken == activeRunToken
    }

    func waitForPreviousRunToClear() async throws {}

    func startRun(
        profile _: ServerProfileRecord,
        password _: String,
        mode: BackupRunMode,
        displayMode: BackupRunMode,
        configuration: BackupRunConfigurationOverride,
        onMonthUploaded _: BackupMonthFinalizer?,
        terminationControl: ExecutionTerminationControl,
        onEvent: @escaping BackupRunDriver.EventHandler,
        onError _: @escaping BackupRunDriver.ErrorHandler
    ) -> UInt64? {
        activeRunToken &+= 1
        startRunCount += 1
        hasActiveRunTask = true
        activeRunConfiguration = configuration
        self.terminationControl = terminationControl
        eventHandler = onEvent
        runMode = mode
        self.displayMode = displayMode
        return activeRunToken
    }

    func requestTermination(_ intent: ExecutionTerminationIntent) {
        terminationControl?.request(intent)
    }

    func clearActiveRunState() {
        activeRunConfiguration = nil
        eventHandler = nil
    }

    func cancelActiveRunImmediately() -> Task<Void, Never>? {
        let hadActiveRun = hasActiveRunTask
        hasActiveRunTask = false
        activeRunConfiguration = nil
        terminationControl = nil
        eventHandler = nil
        return hadActiveRun ? Task {} : nil
    }

    func markOperationReturned() {
        hasActiveRunTask = false
    }

    func deliverFinished() {
        let result = BackupExecutionResult(
            total: 1,
            succeeded: 1,
            failed: 0,
            skipped: 0,
            paused: false
        )
        guard let terminationControl, let eventHandler else { return }
        _ = eventHandler(.finished(result), runMode, displayMode, terminationControl)
    }
}
