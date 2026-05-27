import XCTest
@testable import Watermelon

final class BackupMonthFlushDeltaTests: XCTestCase {
    func testNoneHasEmptyCommittedSetsAndDidFlushFalse() {
        let none = BackupMonthFlushDelta.none
        XCTAssertFalse(none.didFlush)
        XCTAssertTrue(none.committedAssetFingerprints.isEmpty)
        XCTAssertTrue(none.committedTombstoneFingerprints.isEmpty)
    }

    func testDirectConstructionPopulatesFields() {
        let assetA = TestFixtures.assetFingerprint(0xAA)
        let assetB = TestFixtures.assetFingerprint(0xBB)
        let tombstoneA = TestFixtures.assetFingerprint(0xCC)

        let delta = BackupMonthFlushDelta(
            didFlush: true,
            committedAssetFingerprints: [assetA, assetB],
            committedTombstoneFingerprints: [tombstoneA]
        )

        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedAssetFingerprints, [assetA, assetB])
        XCTAssertEqual(delta.committedTombstoneFingerprints, [tombstoneA])
    }

    func testDirectConstructionAllowsEmptyCommittedSets() {
        let delta = BackupMonthFlushDelta(
            didFlush: true,
            committedAssetFingerprints: [],
            committedTombstoneFingerprints: []
        )
        XCTAssertTrue(delta.didFlush)
        XCTAssertTrue(delta.committedAssetFingerprints.isEmpty)
        XCTAssertTrue(delta.committedTombstoneFingerprints.isEmpty)
    }

    func testTypeConformsToSendable() {
        // Compile-time check: assignment to `any Sendable` requires Sendable conformance.
        let value: any Sendable = BackupMonthFlushDelta.none
        XCTAssertNotNil(value)
    }
}
