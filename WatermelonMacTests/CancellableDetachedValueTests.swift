import Foundation
import XCTest
@testable import WatermelonMac

private final class DetachedCancellationProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var didStartValue = false
    private var didObserveCancellationValue = false

    var didStart: Bool {
        lock.withLock { didStartValue }
    }

    var didObserveCancellation: Bool {
        lock.withLock { didObserveCancellationValue }
    }

    func markStarted() {
        lock.withLock {
            didStartValue = true
        }
    }

    func markCancellationObserved() {
        lock.withLock {
            didObserveCancellationValue = true
        }
    }
}

final class CancellableDetachedValueTests: XCTestCase {
    func testParentCancellationReachesDetachedOperation() async {
        let probe = DetachedCancellationProbe()
        let task = Task {
            await withCancellableDetachedValue(priority: .utility) {
                probe.markStarted()
                let deadline =
                    ProcessInfo.processInfo.systemUptime + 2
                while !Task.isCancelled,
                      ProcessInfo.processInfo.systemUptime < deadline {
                    Thread.sleep(forTimeInterval: 0.001)
                }
                if Task.isCancelled {
                    probe.markCancellationObserved()
                }
                return Task.isCancelled
            }
        }

        for _ in 0 ..< 2_000 where !probe.didStart {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertTrue(probe.didStart)

        task.cancel()
        let didObserveCancellation = await task.value
        XCTAssertTrue(didObserveCancellation)
        XCTAssertTrue(probe.didObserveCancellation)
    }
}
