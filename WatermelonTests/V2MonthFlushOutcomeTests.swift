import XCTest
@testable import Watermelon

final class V2MonthFlushOutcomeTests: XCTestCase {
    private let assets: Set<Data> = [TestFixtures.fingerprint(0x11)]
    private let tombstones: Set<Data> = [TestFixtures.fingerprint(0x22)]

    private func makeDelta() -> BackupMonthFlushDelta {
        BackupMonthFlushDelta(
            didFlush: true,
            committedAssetFingerprints: assets,
            committedTombstoneFingerprints: tombstones
        )
    }

    func testCompletedDelta() {
        let delta = makeDelta()
        let outcome: V2MonthFlushOutcome = .completed(delta)
        XCTAssertEqual(outcome.delta.committedAssetFingerprints, assets)
        XCTAssertEqual(outcome.delta.committedTombstoneFingerprints, tombstones)
    }

    func testPartialDelta() {
        let delta = makeDelta()
        let underlying = NSError(domain: "soft", code: 1)
        let flushError = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: assets,
            committedTombstones: tombstones,
            underlying: underlying
        )
        let outcome: V2MonthFlushOutcome = .commitDurableSnapshotDeferred(delta: delta, flushError: flushError)
        XCTAssertEqual(outcome.delta.committedAssetFingerprints, assets)
        XCTAssertEqual(outcome.delta.committedTombstoneFingerprints, tombstones)
    }

    func testCompletedHasNilDisplayError() {
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(outcome.displayError)
    }

    func testCompletedHasNilCancellationCause() {
        let outcome: V2MonthFlushOutcome = .completed(.none)
        XCTAssertNil(outcome.cancellationCause)
    }

    func testPartialDisplayErrorIsConstructedFlushError() {
        let delta = makeDelta()
        let underlying = NSError(domain: "soft", code: 1)
        let flushError = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: assets,
            committedTombstones: tombstones,
            underlying: underlying
        )
        let outcome: V2MonthFlushOutcome = .commitDurableSnapshotDeferred(delta: delta, flushError: flushError)
        guard let displayError = outcome.displayError else {
            XCTFail("displayError must be non-nil for commitDurableSnapshotDeferred")
            return
        }
        if case .snapshotWriteFailed(let outAssets, let outTombstones, let outUnderlying) = displayError {
            XCTAssertEqual(outAssets, assets, "displayError must round-trip the constructed committedAssets payload")
            XCTAssertEqual(outTombstones, tombstones, "displayError must round-trip the constructed committedTombstones payload")
            XCTAssertEqual((outUnderlying as NSError).domain, "soft")
            XCTAssertEqual((outUnderlying as NSError).code, 1)
        } else {
            XCTFail("displayError must be FlushError.snapshotWriteFailed wrapper")
        }
    }

    func testPartialCancellationCauseDelegatesToFlushError() {
        let delta = makeDelta()
        let flushError = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: assets,
            committedTombstones: tombstones,
            underlying: CancellationError()
        )
        let outcome: V2MonthFlushOutcome = .commitDurableSnapshotDeferred(delta: delta, flushError: flushError)
        XCTAssertNotNil(outcome.cancellationCause, "outcome.cancellationCause must delegate to FlushError.cancellationCause and match CancellationError underlying")
        XCTAssertNotNil(flushError.cancellationCause, "FlushError.cancellationCause must match the same underlying — delegation pins this equivalence")
    }

    func testPartialCancellationCauseWalksNSURLErrorCancelled() {
        let delta = makeDelta()
        let underlying = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        let flushError = V2MonthSession.FlushError.snapshotWriteFailed(
            committedAssets: assets,
            committedTombstones: tombstones,
            underlying: underlying
        )
        let outcome: V2MonthFlushOutcome = .commitDurableSnapshotDeferred(delta: delta, flushError: flushError)
        XCTAssertNotNil(outcome.cancellationCause, "NSURLErrorCancelled in the underlying chain must surface as cancellationCause; unit-005 walker preserved")
    }
}
