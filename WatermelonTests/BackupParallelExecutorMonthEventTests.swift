import XCTest
@testable import Watermelon

final class BackupParallelExecutorMonthEventTests: XCTestCase {

    func testSnapshotWriteFailureEmissionGateRequiresFinishingMonth() {
        let error = snapshotWriteFailedError()

        XCTAssertFalse(BackupParallelExecutor.shouldEmitUploadDurableSnapshotDeferred(
            error: error,
            shouldFinishMonth: false
        ))
        XCTAssertTrue(BackupParallelExecutor.shouldEmitUploadDurableSnapshotDeferred(
            error: error,
            shouldFinishMonth: true
        ))
    }

    func testNonSnapshotFlushFailureDoesNotQualifyForDurableSnapshotDeferredEvent() {
        XCTAssertFalse(BackupParallelExecutor.shouldEmitUploadDurableSnapshotDeferred(
            error: V2MonthSession.FlushError.concurrentFlushRejected,
            shouldFinishMonth: true
        ))
        XCTAssertFalse(BackupParallelExecutor.shouldEmitUploadDurableSnapshotDeferred(
            error: NSError(domain: "test", code: 1),
            shouldFinishMonth: true
        ))
    }

    func testSnapshotWriteFailureWithCancellationCauseDoesNotQualifyForDurableSnapshotDeferredEvent() {
        let error = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [Data([0x01])],
            committedTombstones: [],
            underlying: CancellationError()
        )

        XCTAssertFalse(BackupParallelExecutor.shouldEmitUploadDurableSnapshotDeferred(
            error: error,
            shouldFinishMonth: true
        ))
    }

    func testDurableSnapshotDeferredEmitterProducesOnlyDeferredEvent() async {
        let month = LibraryMonthKey(year: 2024, month: 6)
        let eventStream = BackupEventStream()

        let emitted = BackupParallelExecutor.emitUploadDurableSnapshotDeferred(
            eventStream: eventStream,
            month: month,
            message: "snapshot deferred"
        )
        eventStream.finish()

        XCTAssertTrue(emitted)
        let events = await collectEvents(from: eventStream)
        XCTAssertTrue(events.containsDurableSnapshotDeferredEvent)
        XCTAssertFalse(events.containsIncompleteEvent)
    }

    private func collectEvents(from eventStream: BackupEventStream) async -> [BackupEvent] {
        var events: [BackupEvent] = []
        for await event in eventStream.stream {
            events.append(event)
        }
        return events
    }

    private func snapshotWriteFailedError() -> V2MonthSession.FlushError {
        V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: [Data([0x01])],
            committedTombstones: [Data([0x02])],
            underlying: NSError(domain: "test", code: 1)
        )
    }
}

private extension [BackupEvent] {
    var containsDurableSnapshotDeferredEvent: Bool {
        contains { event in
            guard case .monthChanged(let change) = event,
                  case .uploadDurableSnapshotDeferred = change.action else {
                return false
            }
            return true
        }
    }

    var containsIncompleteEvent: Bool {
        contains { event in
            guard case .monthChanged(let change) = event,
                  case .incomplete = change.action else {
                return false
            }
            return true
        }
    }
}
