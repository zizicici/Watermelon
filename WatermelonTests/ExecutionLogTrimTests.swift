import XCTest
@testable import Watermelon

final class ExecutionLogTrimTests: XCTestCase {
    private func entry(_ i: Int) -> ExecutionLogEntry {
        ExecutionLogEntry(timestamp: Date(timeIntervalSince1970: TimeInterval(i)), message: "entry-\(i)", level: .info)
    }

    func testLiveLogStaysBoundedUnderPerAssetAppend() {
        let ceiling = HomeExecutionCoordinator.maxLiveLogEntries + HomeExecutionCoordinator.liveLogTrimChunk
        var entries: [ExecutionLogEntry] = []
        let total = 50_000
        var observedMax = 0
        for i in 0..<total {
            entries.append(entry(i))
            HomeExecutionCoordinator.trimLiveLogEntries(&entries)
            observedMax = max(observedMax, entries.count)
        }

        XCTAssertLessThanOrEqual(observedMax, ceiling, "live log buffer exceeded its bound")
        XCTAssertGreaterThanOrEqual(entries.count, HomeExecutionCoordinator.maxLiveLogEntries)
        // Newest retained, oldest dropped.
        XCTAssertEqual(entries.last?.message, "entry-\(total - 1)")
        XCTAssertEqual(entries.first?.message, "entry-\(total - entries.count)")
    }

    func testNoTrimAtOrBelowThreshold() {
        let n = HomeExecutionCoordinator.maxLiveLogEntries + HomeExecutionCoordinator.liveLogTrimChunk
        var entries: [ExecutionLogEntry] = []
        for i in 0..<n {
            entries.append(entry(i))
            HomeExecutionCoordinator.trimLiveLogEntries(&entries)
        }

        XCTAssertEqual(entries.count, n)
        XCTAssertEqual(entries.first?.message, "entry-0")
    }
}
