import XCTest
@testable import Watermelon

/// Direct representation tests for `RepoMonthStateValidator`, the single authority that S1b carves out of
/// `RepoMaterializer`'s snapshot-body and replay-state validation. These exercise the validator in
/// isolation (no remote round-trip) and assert the exact `Violation` each defect produces, including the
/// cases the snapshot-body and replay paths share.
final class RepoMonthStateValidatorTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"

    private func fp(_ byte: UInt8) -> AssetFingerprint { TestFixtures.assetFingerprint(byte) }
    private func hash(_ byte: UInt8) -> Data { TestFixtures.fingerprint(byte) }
    private func stamp(seq: UInt64, clock: UInt64) -> OpStamp { OpStamp(writerID: writerA, seq: seq, clock: clock) }

    private func snapshotFile(
        assets: [SnapshotAssetRow] = [],
        resources: [SnapshotResourceRow] = [],
        assetResources: [SnapshotAssetResourceRow] = [],
        deletedKeys: [SnapshotDeletedKeyRow] = []
    ) -> SnapshotFile {
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        return SnapshotFile(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: "repo", covered: covered, createdAtMs: nil
            ),
            assets: assets,
            resources: resources,
            assetResources: assetResources,
            deletedKeys: deletedKeys,
            sha256Hex: "",
            rowCount: 0
        )
    }

    private func assetRow(_ fingerprint: AssetFingerprint, resourceCount: Int = 1) -> SnapshotAssetRow {
        SnapshotAssetRow(
            assetFingerprint: fingerprint, creationDateMs: nil, backedUpAtMs: 1,
            resourceCount: resourceCount, totalFileSizeBytes: 10, stamp: stamp(seq: 1, clock: 1)
        )
    }

    private func resourceRow(path: String, hash contentHash: Data) -> SnapshotResourceRow {
        SnapshotResourceRow(
            physicalRemotePath: path, contentHash: contentHash, fileSize: 10,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
            stamp: stamp(seq: 1, clock: 1)
        )
    }

    private func linkRow(_ fingerprint: AssetFingerprint, hash resourceHash: Data, slot: Int = 0) -> SnapshotAssetResourceRow {
        SnapshotAssetResourceRow(
            assetFingerprint: fingerprint, role: ResourceTypeCode.photo, slot: slot,
            resourceHash: resourceHash, logicalName: "photo.jpg"
        )
    }

    // MARK: - validateSnapshotBody

    func testValidSnapshotBodyBuildsState() {
        let f = fp(0x01)
        let h = hash(0x02)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)]
        )
        guard case .success(let body) = RepoMonthStateValidator.validateSnapshotBody(file, month: month) else {
            return XCTFail("clean body must validate")
        }
        XCTAssertNotNil(body.state.assets[f])
        XCTAssertNotNil(body.state.resources[RemotePhysicalPathKey("2026/01/photo.jpg")])
        XCTAssertNotNil(body.state.assetResources[AssetResourceKey(assetFingerprint: f, role: ResourceTypeCode.photo, slot: 0)])
        XCTAssertEqual(body.baselineStamps[f], stamp(seq: 1, clock: 1))
    }

    func testOutOfMonthResourcePathRejected() {
        let f = fp(0x03)
        let h = hash(0x04)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/02/wrong.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .outOfMonthResourcePath(path: "2026/02/wrong.jpg")
        )
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
            assetResources: [linkRow(f, hash: h1)]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .duplicateResourcePath(path: "2026/01/dup.jpg")
        )
    }

    func testLinkToAbsentResourceRejected() {
        let f = fp(0x08)
        let present = hash(0x09)
        let absent = hash(0x0A)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: present)],
            assetResources: [linkRow(f, hash: absent)]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .linkToAbsentResource
        )
    }

    func testLinkToAbsentAssetRejected() {
        let present = fp(0x0B)
        let orphan = fp(0x0C)
        let h = hash(0x0D)
        let file = snapshotFile(
            assets: [assetRow(present)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(present, hash: h), linkRow(orphan, hash: h, slot: 1)]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .linkToAbsentAsset
        )
    }

    func testZeroLinkAssetRowRejected() {
        let linked = fp(0x0E)
        let phantom = fp(0x0F)
        let h = hash(0x10)
        let file = snapshotFile(
            assets: [assetRow(linked), assetRow(phantom, resourceCount: 0)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(linked, hash: h)]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .assetRowMissingLinks
        )
    }

    func testUnsupportedDeletedKeyTypeRejected() {
        let file = snapshotFile(
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .resource, keyValue: fp(0x11).rawValue.hexString, stamp: stamp(seq: 1, clock: 1))]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .unsupportedDeletedKeyType
        )
    }

    func testMalformedDeletedKeyHashRejected() {
        let file = snapshotFile(
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .asset, keyValue: "not-a-hash", stamp: stamp(seq: 1, clock: 1))]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .malformedDeletedKeyHash
        )
    }

    func testLiveAssetWithTombstoneRejected() {
        let f = fp(0x12)
        let h = hash(0x13)
        let file = snapshotFile(
            assets: [assetRow(f)],
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: h)],
            assetResources: [linkRow(f, hash: h)],
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .asset, keyValue: f.rawValue.hexString, stamp: stamp(seq: 2, clock: 2))]
        )
        XCTAssertEqual(
            RepoMonthStateValidator.validateSnapshotBody(file, month: month).failure,
            .liveAssetWithTombstone
        )
    }

    func testDeletedKeyWithoutLiveAssetIsAccepted() {
        let f = fp(0x14)
        let file = snapshotFile(
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .asset, keyValue: f.rawValue.hexString, stamp: stamp(seq: 2, clock: 2))]
        )
        guard case .success(let body) = RepoMonthStateValidator.validateSnapshotBody(file, month: month) else {
            return XCTFail("a tombstone without a live asset row is valid")
        }
        XCTAssertEqual(body.state.deletedAssetStamps[f], stamp(seq: 2, clock: 2))
    }

    // MARK: - replayAddAssetOutOfMonth

    func testReplayInMonthResourcesHaveNoViolation() {
        let resources = [
            CommitResourceEntry(
                physicalRemotePath: "2026/01/a.jpg", logicalName: "a.jpg", contentHash: hash(0x20),
                fileSize: 1, resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )
        ]
        XCTAssertNil(RepoMonthStateValidator.replayAddAssetOutOfMonth(resources: resources, month: month))
    }

    func testReplayEmptyResourcesHaveNoViolation() {
        XCTAssertNil(RepoMonthStateValidator.replayAddAssetOutOfMonth(resources: [], month: month))
    }

    func testReplayOutOfMonthResourceProducesViolation() {
        let resources = [
            CommitResourceEntry(
                physicalRemotePath: "2026/02/wrong.jpg", logicalName: "wrong.jpg", contentHash: hash(0x21),
                fileSize: 1, resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )
        ]
        XCTAssertEqual(
            RepoMonthStateValidator.replayAddAssetOutOfMonth(resources: resources, month: month),
            .outOfMonthResourcePath(path: "2026/02/wrong.jpg")
        )
    }

    // MARK: - dangling replay link

    private func stateWith(resources: [SnapshotResourceRow], links: [SnapshotAssetResourceRow]) -> RepoMonthState {
        var resourceMap: [RemotePhysicalPathKey: SnapshotResourceRow] = [:]
        for r in resources { resourceMap[RemotePhysicalPathKey(r.physicalRemotePath)] = r }
        var linkMap: [AssetResourceKey: SnapshotAssetResourceRow] = [:]
        for l in links { linkMap[AssetResourceKey(assetFingerprint: l.assetFingerprint, role: l.role, slot: l.slot)] = l }
        return RepoMonthState(assets: [:], resources: resourceMap, assetResources: linkMap, deletedAssetStamps: [:])
    }

    func testDanglingReplayLinkDetectedWhenHashUnbacked() {
        let f = fp(0x30)
        let backed = hash(0x31)
        let unbacked = hash(0x32)
        let state = stateWith(
            resources: [resourceRow(path: "2026/01/photo.jpg", hash: backed)],
            links: [linkRow(f, hash: unbacked)]
        )
        XCTAssertEqual(RepoMonthStateValidator.danglingReplayLinkViolation(in: state), .linkToAbsentResource)
        XCTAssertEqual(RepoMonthStateValidator.danglingReplayLinkMonths(in: [month: state]), [month])
    }

    func testNoDanglingWhenHashBackedAtDifferentPath() {
        let f = fp(0x33)
        let h = hash(0x34)
        let state = stateWith(
            resources: [resourceRow(path: "2026/01/other.jpg", hash: h)],
            links: [linkRow(f, hash: h)]
        )
        XCTAssertNil(RepoMonthStateValidator.danglingReplayLinkViolation(in: state))
        XCTAssertEqual(RepoMonthStateValidator.danglingReplayLinkMonths(in: [month: state]), [])
    }

    func testNoDanglingWhenNoLinks() {
        let state = stateWith(resources: [resourceRow(path: "2026/01/photo.jpg", hash: hash(0x35))], links: [])
        XCTAssertNil(RepoMonthStateValidator.danglingReplayLinkViolation(in: state))
    }

    // MARK: - shared authority

    /// The same out-of-month resource path is reported by both the snapshot-body and the replay path as the
    /// identical `Violation.outOfMonthResourcePath`, proving one validation authority backs both.
    func testOutOfMonthResourcePathSharedBetweenSnapshotAndReplay() {
        let f = fp(0x40)
        let h = hash(0x41)
        let path = "2026/02/wrong.jpg"
        let snapshotViolation = RepoMonthStateValidator.validateSnapshotBody(
            snapshotFile(assets: [assetRow(f)], resources: [resourceRow(path: path, hash: h)], assetResources: [linkRow(f, hash: h)]),
            month: month
        ).failure
        let replayViolation = RepoMonthStateValidator.replayAddAssetOutOfMonth(
            resources: [CommitResourceEntry(
                physicalRemotePath: path, logicalName: "wrong.jpg", contentHash: h,
                fileSize: 1, resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )],
            month: month
        )
        XCTAssertEqual(snapshotViolation, .outOfMonthResourcePath(path: path))
        XCTAssertEqual(snapshotViolation, replayViolation)
    }

    /// An unbacked link is reported as the identical `Violation.linkToAbsentResource` by both paths.
    func testLinkToAbsentResourceSharedBetweenSnapshotAndReplay() {
        let f = fp(0x42)
        let present = hash(0x43)
        let absent = hash(0x44)
        let snapshotViolation = RepoMonthStateValidator.validateSnapshotBody(
            snapshotFile(assets: [assetRow(f)], resources: [resourceRow(path: "2026/01/photo.jpg", hash: present)], assetResources: [linkRow(f, hash: absent)]),
            month: month
        ).failure
        let replayViolation = RepoMonthStateValidator.danglingReplayLinkViolation(
            in: stateWith(resources: [resourceRow(path: "2026/01/photo.jpg", hash: present)], links: [linkRow(f, hash: absent)])
        )
        XCTAssertEqual(snapshotViolation, .linkToAbsentResource)
        XCTAssertEqual(snapshotViolation, replayViolation)
    }
}

private extension Result {
    /// Convenience for asserting the error of a `Result` in tests.
    var failure: Failure? {
        if case .failure(let error) = self { return error }
        return nil
    }
}
