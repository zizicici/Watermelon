import XCTest
@testable import Watermelon

/// Direct representation tests for `SnapshotTrustPolicy` and `SnapshotCoveredMaxSelector`, the focused
/// helpers S2a carves out of the materializer, checkpoint lightweight acceptance, snapshot-delete scanner,
/// and post-delete authority checker. These exercise the helpers in isolation; the consumer behavioural
/// suites (materializer round-trip, checkpoint, post-delete, compaction) provide the differential coverage.
final class SnapshotTrustPolicyTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"

    private func fp(_ byte: UInt8) -> AssetFingerprint { TestFixtures.assetFingerprint(byte) }
    private func hash(_ byte: UInt8) -> Data { TestFixtures.fingerprint(byte) }
    private func stamp(seq: UInt64, clock: UInt64, writer: String? = nil) -> OpStamp {
        OpStamp(writerID: writer ?? writerA, seq: seq, clock: clock)
    }

    private func covered(_ ranges: [(UInt64, UInt64)], writer: String? = nil) -> CoveredRanges {
        var c = CoveredRanges()
        for (low, high) in ranges {
            c.add(writerID: writer ?? writerA, range: ClosedSeqRange(low: low, high: high))
        }
        return c
    }

    // MARK: - resourcePathBelongsToMonth

    func testResourcePathInMonthAccepted() {
        XCTAssertTrue(SnapshotTrustPolicy.resourcePathBelongsToMonth("2026/01/photo.jpg", month: month))
    }

    func testResourcePathWrongMonthRejected() {
        XCTAssertFalse(SnapshotTrustPolicy.resourcePathBelongsToMonth("2026/02/photo.jpg", month: month))
        XCTAssertFalse(SnapshotTrustPolicy.resourcePathBelongsToMonth("2025/01/photo.jpg", month: month))
    }

    func testResourcePathWrongShapeRejected() {
        XCTAssertFalse(SnapshotTrustPolicy.resourcePathBelongsToMonth("2026/01", month: month))
        XCTAssertFalse(SnapshotTrustPolicy.resourcePathBelongsToMonth("2026/01/", month: month))
        XCTAssertFalse(SnapshotTrustPolicy.resourcePathBelongsToMonth("2026/01/sub/photo.jpg", month: month))
    }

    // MARK: - rowStampIsWorkable

    func testRowStampWorkableWhenInBounds() {
        XCTAssertTrue(SnapshotTrustPolicy.rowStampIsWorkable(
            stamp(seq: 1, clock: 5), covered: covered([(1, 1)]), filenameLamport: 10
        ))
    }

    func testRowStampRejectedWhenClockAboveCeiling() {
        XCTAssertFalse(SnapshotTrustPolicy.rowStampIsWorkable(
            stamp(seq: 1, clock: LamportClock.maxAdoptableValue),
            covered: covered([(1, 1)]),
            filenameLamport: LamportClock.maxAdoptableValue
        ))
    }

    func testRowStampRejectedWhenClockAboveFilenameLamport() {
        XCTAssertFalse(SnapshotTrustPolicy.rowStampIsWorkable(
            stamp(seq: 1, clock: 11), covered: covered([(1, 1)]), filenameLamport: 10
        ))
    }

    func testRowStampRejectedWhenSeqUncovered() {
        XCTAssertFalse(SnapshotTrustPolicy.rowStampIsWorkable(
            stamp(seq: 5, clock: 5), covered: covered([(1, 1)]), filenameLamport: 10
        ))
    }

    // MARK: - snapshotBodyIsMaterializerTrusted

    private func snapshotFile(
        assets: [SnapshotAssetRow],
        resources: [SnapshotResourceRow],
        assetResources: [SnapshotAssetResourceRow],
        deletedKeys: [SnapshotDeletedKeyRow] = [],
        covered coveredRanges: CoveredRanges
    ) -> SnapshotFile {
        SnapshotFile(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: "repo", covered: coveredRanges, createdAtMs: nil
            ),
            assets: assets,
            resources: resources,
            assetResources: assetResources,
            deletedKeys: deletedKeys,
            sha256Hex: "",
            rowCount: 0
        )
    }

    private func assetRow(_ fingerprint: AssetFingerprint, seq: UInt64 = 1, clock: UInt64 = 1) -> SnapshotAssetRow {
        SnapshotAssetRow(
            assetFingerprint: fingerprint, creationDateMs: nil, backedUpAtMs: 1,
            resourceCount: 1, totalFileSizeBytes: 10, stamp: stamp(seq: seq, clock: clock)
        )
    }

    private func resourceRow(path: String, hash contentHash: Data, seq: UInt64 = 1, clock: UInt64 = 1) -> SnapshotResourceRow {
        SnapshotResourceRow(
            physicalRemotePath: path, contentHash: contentHash, fileSize: 10,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
            stamp: stamp(seq: seq, clock: clock)
        )
    }

    private func linkRow(_ fingerprint: AssetFingerprint, hash resourceHash: Data, slot: Int = 0) -> SnapshotAssetResourceRow {
        SnapshotAssetResourceRow(
            assetFingerprint: fingerprint, role: ResourceTypeCode.photo, slot: slot,
            resourceHash: resourceHash, logicalName: "photo.jpg"
        )
    }

    private func validBody() -> SnapshotFile {
        let f = fp(0x01)
        let h = hash(0x02)
        return snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            covered: covered([(1, 1)])
        )
    }

    func testValidBodyTrusted() {
        XCTAssertTrue(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(validBody(), month: month, filenameLamport: 5))
    }

    func testFilenameLamportAboveCeilingRejected() {
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(
            validBody(), month: month, filenameLamport: LamportClock.maxAdoptableValue
        ))
    }

    func testOutOfMonthResourceRejected() {
        let f = fp(0x03)
        let h = hash(0x04)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/02/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testDuplicateResourcePathRejected() {
        let f = fp(0x05)
        let h1 = hash(0x06)
        let h2 = hash(0x07)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [
                resourceRow(path: "2026/01/dup.jpg", hash: h1),
                resourceRow(path: "2026/01/dup.jpg", hash: h2)
            ],
            assetResources: [linkRow(f, hash: h1)],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testUnworkableRowStampRejected() {
        let f = fp(0x08)
        let h = hash(0x09)
        let file = snapshotFile(
            assets: [assetRow(f, seq: 1, clock: 6)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            covered: covered([(1, 1)])
        )
        // asset-row clock 6 exceeds the filename lamport 5.
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testLinkToAbsentAssetRejected() {
        let present = fp(0x0A)
        let orphan = fp(0x0B)
        let h = hash(0x0C)
        let file = snapshotFile(
            assets: [assetRow(present)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(present, hash: h), linkRow(orphan, hash: h, slot: 1)],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testLinkToAbsentResourceHashRejected() {
        let f = fp(0x0D)
        let present = hash(0x0E)
        let absent = hash(0x0F)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: present)],
            assetResources: [linkRow(f, hash: absent)],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testZeroLinkAssetRejected() {
        let linked = fp(0x10)
        let phantom = fp(0x11)
        let h = hash(0x12)
        let file = snapshotFile(
            assets: [assetRow(linked), assetRow(phantom)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(linked, hash: h)],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testNonAssetDeletedKeyRejected() {
        let f = fp(0x13)
        let h = hash(0x14)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .resource, keyValue: fp(0x15).rawValue.hexString, stamp: stamp(seq: 1, clock: 1))],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    func testDeletedKeyCollidingWithLiveAssetRejected() {
        let f = fp(0x16)
        let h = hash(0x17)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .asset, keyValue: f.rawValue.hexString, stamp: stamp(seq: 1, clock: 1))],
            covered: covered([(1, 1)])
        )
        XCTAssertFalse(SnapshotTrustPolicy.snapshotBodyIsMaterializerTrusted(file, month: month, filenameLamport: 5))
    }

    // MARK: - SnapshotCoveredMaxSelector

    private func candidate(
        _ ranges: [(UInt64, UInt64)],
        lamport: UInt64 = 1,
        writer: String = "w",
        runIDPrefix: String = "r"
    ) -> SnapshotCoveredMaxSelector.Candidate {
        SnapshotCoveredMaxSelector.Candidate(
            covered: covered(ranges, writer: writer),
            lamport: lamport,
            writerID: writer,
            runIDPrefix: runIDPrefix
        )
    }

    func testSelectEmptyReturnsNil() {
        XCTAssertNil(SnapshotCoveredMaxSelector.selectIndex([]))
    }

    func testSelectSingleReturnsZero() {
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([candidate([(1, 1)])]), 0)
    }

    func testSelectSupersetWinsRegardlessOfOrder() {
        let superset = candidate([(1, 5)], lamport: 2)
        let subset = candidate([(1, 2)], lamport: 9)
        // The dominating coverage wins even though the subset carries a higher lamport.
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([superset, subset]), 0)
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([subset, superset]), 1)
    }

    func testSelectIncomparableReturnsNil() {
        let a = candidate([(1, 1)])
        let b = candidate([(2, 2)])
        XCTAssertNil(SnapshotCoveredMaxSelector.selectIndex([a, b]))
    }

    func testEqualCoverageTiebreaksByLamport() {
        let lo = candidate([(1, 2)], lamport: 3, writer: "w", runIDPrefix: "r")
        let hi = candidate([(1, 2)], lamport: 5, writer: "w", runIDPrefix: "r")
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([lo, hi]), 1)
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([hi, lo]), 0)
    }

    func testEqualCoverageAndLamportTiebreaksByWriterID() {
        // The candidates share one covered key so coverage ties and the writerID tiebreak engages.
        let lo = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "aaaa", runIDPrefix: "r")
        let hi = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "bbbb", runIDPrefix: "r")
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([lo, hi]), 1)
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([hi, lo]), 0)
    }

    func testEqualCoverageLamportWriterTiebreaksByRunIDPrefix() {
        let lo = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "w", runIDPrefix: "a")
        let hi = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "w", runIDPrefix: "z")
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([lo, hi]), 1)
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([hi, lo]), 0)
    }

    func testExactTripleTieFavoursLaterIndex() {
        let a = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "w", runIDPrefix: "r")
        let b = SnapshotCoveredMaxSelector.Candidate(covered: covered([(1, 2)], writer: "shared"), lamport: 5, writerID: "w", runIDPrefix: "r")
        // `>=` on the final runIDPrefix tiebreak hands an exact triple tie to the later index.
        XCTAssertEqual(SnapshotCoveredMaxSelector.selectIndex([a, b]), 1)
    }
}
