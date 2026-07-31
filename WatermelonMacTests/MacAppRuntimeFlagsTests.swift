import XCTest
@testable import WatermelonMac

private final class MacExecutionCancellationProbe:
    @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    var count: Int {
        lock.withLock { value }
    }

    func record() {
        lock.withLock {
            value += 1
        }
    }
}

final class MacAppRuntimeFlagsTests: XCTestCase {
    override func setUp() {
        super.setUp()
        AppRuntimeFlags._testReset()
    }

    override func tearDown() {
        AppRuntimeFlags._testReset()
        super.tearDown()
    }

    func testOnlyExecutionOwnerCanRequestCancellation() {
        let owner = AppRuntimeFlags()
        let observer = AppRuntimeFlags()
        let probe = MacExecutionCancellationProbe()

        XCTAssertTrue(owner.tryEnterExecution())
        owner.setExecutionCancellationHandler {
            probe.record()
        }

        XCTAssertFalse(observer.requestExecutionCancellation())
        XCTAssertEqual(probe.count, 0)
        XCTAssertTrue(owner.requestExecutionCancellation())
        XCTAssertEqual(probe.count, 1)
    }

    func testCancellationHandlerDoesNotLeakAcrossLeases() {
        let flags = AppRuntimeFlags()
        let probe = MacExecutionCancellationProbe()

        XCTAssertTrue(flags.tryEnterExecution())
        flags.setExecutionCancellationHandler {
            probe.record()
        }
        flags.exitExecution()

        XCTAssertTrue(flags.tryEnterExecution())
        XCTAssertFalse(flags.requestExecutionCancellation())
        XCTAssertEqual(probe.count, 0)
    }
}
