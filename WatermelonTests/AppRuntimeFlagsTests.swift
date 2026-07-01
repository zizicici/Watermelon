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

    func testExecutionFlagIsSharedAndExclusiveAcrossInstances() {
        let foreground = AppRuntimeFlags()
        let background = AppRuntimeFlags()

        XCTAssertTrue(foreground.tryEnterExecution())
        XCTAssertTrue(background.isExecuting)

        XCTAssertFalse(background.tryEnterExecution())
        XCTAssertTrue(foreground.isExecuting)

        foreground.exitExecution()
        XCTAssertFalse(background.isExecuting)

        XCTAssertTrue(background.tryEnterExecution())
        XCTAssertTrue(foreground.isExecuting)

        background.exitExecution()
        XCTAssertFalse(foreground.isExecuting)
    }

    func testTestResetReleasesExecutionOwner() {
        let flags = AppRuntimeFlags()
        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertTrue(flags.isExecuting)

        AppRuntimeFlags._testReset()
        XCTAssertFalse(flags.isExecuting)

        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertTrue(flags.isExecuting)

        flags.exitExecution()
        XCTAssertFalse(flags.isExecuting)
    }

    func testDeinitReleasesExecutionOwner() {
        var flags: AppRuntimeFlags? = AppRuntimeFlags()
        weak var weakFlags: AppRuntimeFlags?
        weakFlags = flags

        XCTAssertTrue(flags?.tryEnterExecution() == true)
        XCTAssertTrue(flags?.isExecuting == true)

        flags = nil

        XCTAssertNil(weakFlags)
        XCTAssertFalse(AppRuntimeFlags().isExecuting)
    }

    func testExecutionLifecycleNotificationPostsOnlyWhenGlobalStateChanges() {
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

        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertFalse(flags.tryEnterExecution())
        XCTAssertFalse(otherFlags.tryEnterExecution())
        otherFlags.exitExecution()
        XCTAssertTrue(flags.isExecuting)

        flags.exitExecution()
        flags.exitExecution()

        wait(for: [lifecycleChanged], timeout: 1)
        XCTAssertEqual(observedObjects, [ObjectIdentifier(flags), ObjectIdentifier(flags)])
        XCTAssertEqual(observedExecutionStates, [true, false])
        XCTAssertFalse(flags.isExecuting)
    }
}
