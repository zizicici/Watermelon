import XCTest
@testable import Watermelon

final class AppRuntimeFlagsTests: XCTestCase {
    override func setUp() {
        super.setUp()
        AppRuntimeFlags._testReset()
    }

    override func tearDown() {
        AppRuntimeFlags._testReset()
        super.tearDown()
    }

    func testExecutionFlagIsSharedAndExclusiveAcrossInstances() throws {
        let foreground = AppRuntimeFlags()
        let background = AppRuntimeFlags()

        let foregroundClaim = try XCTUnwrap(foreground.tryEnterExecution())
        XCTAssertTrue(background.isExecuting)

        XCTAssertNil(background.tryEnterExecution())
        XCTAssertTrue(foreground.isExecuting)

        foreground.exitExecution(foregroundClaim)
        XCTAssertFalse(background.isExecuting)

        let backgroundClaim = try XCTUnwrap(background.tryEnterExecution())
        XCTAssertTrue(foreground.isExecuting)

        background.exitExecution(backgroundClaim)
        XCTAssertFalse(foreground.isExecuting)
    }

    func testTestResetReleasesExecutionOwner() throws {
        let flags = AppRuntimeFlags()
        _ = try XCTUnwrap(flags.tryEnterExecution())
        XCTAssertTrue(flags.isExecuting)

        AppRuntimeFlags._testReset()
        XCTAssertFalse(flags.isExecuting)

        let claim = try XCTUnwrap(flags.tryEnterExecution())
        XCTAssertTrue(flags.isExecuting)

        flags.exitExecution(claim)
        XCTAssertFalse(flags.isExecuting)
    }

    func testDeinitReleasesExecutionOwner() {
        var flags: AppRuntimeFlags? = AppRuntimeFlags()
        weak var weakFlags: AppRuntimeFlags?
        weakFlags = flags

        XCTAssertNotNil(flags?.tryEnterExecution())
        XCTAssertTrue(flags?.isExecuting == true)

        flags = nil

        XCTAssertNil(weakFlags)
        XCTAssertFalse(AppRuntimeFlags().isExecuting)
    }

    func testDeinitExecutionNotificationCannotReenterDeinitializingOwner() {
        var flags: AppRuntimeFlags? = AppRuntimeFlags()
        XCTAssertNotNil(flags?.tryEnterExecution())
        var notificationCount = 0
        var exposedDeinitializingOwner = false
        let observer = NotificationCenter.default.addObserver(
            forName: .ExecutionLifecycleDidChange,
            object: nil,
            queue: nil
        ) { notification in
            notificationCount += 1
            guard !exposedDeinitializingOwner,
                  let source = notification.object as? AppRuntimeFlags else { return }
            exposedDeinitializingOwner = true
            _ = source.tryEnterExecution()
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
        }

        flags = nil

        XCTAssertEqual(notificationCount, 1)
        XCTAssertFalse(exposedDeinitializingOwner)
        XCTAssertFalse(AppRuntimeFlags().isExecuting)
    }

    func testDeinitConnectionNotificationCannotReenterDeinitializingOwner() {
        var flags: AppRuntimeFlags? = AppRuntimeFlags()
        XCTAssertTrue(flags?.tryBeginConnecting(profileID: 7) == true)
        var notificationCount = 0
        var exposedDeinitializingOwner = false
        let observer = NotificationCenter.default.addObserver(
            forName: .ConnectionLifecycleDidChange,
            object: nil,
            queue: nil
        ) { notification in
            notificationCount += 1
            guard !exposedDeinitializingOwner,
                  let source = notification.object as? AppRuntimeFlags else { return }
            exposedDeinitializingOwner = true
            _ = source.tryBeginConnecting(profileID: 8)
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
        }

        flags = nil

        XCTAssertEqual(notificationCount, 1)
        XCTAssertFalse(exposedDeinitializingOwner)
        NotificationCenter.default.removeObserver(observer)
        let successor = AppRuntimeFlags()
        XCTAssertTrue(successor.tryBeginConnecting(profileID: 8))
        successor.endConnecting(profileID: 8)
    }

    @MainActor
    func testExecutionLeasePreservesActorAcrossSuspension() async {
        let flags = AppRuntimeFlags()

        let ranOnMainActor = await flags.withExecutionLease {
            MainActor.preconditionIsolated()
            try? await Task.sleep(nanoseconds: 1_000_000)
            MainActor.preconditionIsolated()
            return true
        }

        XCTAssertEqual(ranOnMainActor, true)
        XCTAssertFalse(flags.isExecuting)
    }

    func testExecutionLifecycleNotificationPostsOnlyWhenGlobalStateChanges() throws {
        let flags = AppRuntimeFlags()
        let otherFlags = AppRuntimeFlags()
        let lifecycleChanged = expectation(description: "execution lifecycle changed")
        lifecycleChanged.expectedFulfillmentCount = 2
        var observedObjects: [ObjectIdentifier] = []
        var observedExecutionStates: [Bool] = []
        let observer = NotificationCenter.default.addObserver(
            forName: .ExecutionLifecycleDidChange,
            object: nil,
            queue: nil
        ) { notification in
            if let source = notification.object as? AppRuntimeFlags {
                observedObjects.append(ObjectIdentifier(source))
            }
            observedExecutionStates.append(AppRuntimeFlags().isExecuting)
            lifecycleChanged.fulfill()
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
        }

        let claim = try XCTUnwrap(flags.tryEnterExecution())
        XCTAssertNil(flags.tryEnterExecution())
        XCTAssertNil(otherFlags.tryEnterExecution())
        XCTAssertTrue(flags.isExecuting)

        flags.exitExecution(claim)
        flags.exitExecution(claim)

        wait(for: [lifecycleChanged], timeout: 1)
        XCTAssertEqual(observedObjects, [ObjectIdentifier(flags), ObjectIdentifier(flags)])
        XCTAssertEqual(observedExecutionStates, [true, false])
        XCTAssertFalse(flags.isExecuting)
    }

    func testStaleClaimCannotReleaseSuccessorOnSameFlags() throws {
        let flags = AppRuntimeFlags()
        let staleClaim = try XCTUnwrap(flags.tryEnterExecution())
        flags.exitExecution(staleClaim)

        let successorClaim = try XCTUnwrap(flags.tryEnterExecution())
        flags.exitExecution(staleClaim)

        XCTAssertTrue(flags.isExecuting)
        flags.exitExecution(successorClaim)
        XCTAssertFalse(flags.isExecuting)
    }
}
