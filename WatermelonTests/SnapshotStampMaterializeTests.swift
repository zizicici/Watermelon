import XCTest
@testable import Watermelon

/// Per-asset LWW stamps on the snapshot baseline — both add side and deleted
/// side. Without them, three failure modes manifest:
/// 1. Observation-tombstone false-delete: heal in baseline → tombstone basis
///    predates heal → without stamp seed the basis-skip never fires.
/// 2. Cross-writer add-vs-stale-add LWW: newer add in baseline, stale add
///    replays → without add-stamp gate, stale wins.
/// 3. Cross-writer add-vs-stale-tombstone LWW: newer tombstone in baseline,
///    stale add replays → without deletedAssetStamps gate, stale add resurrects.
final class SnapshotStampMaterializeTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "repo-test-id"
    private let runID = "run-stamp-001"
    private let month = LibraryMonthKey(year: 2026, month: 3)

    // MARK: - Test 1: Stamp persists round-trip

    func testStampRoundTripsThroughCommitToSnapshotToReMaterialize() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.fingerprint(0xA1)
        let hash = TestFixtures.fingerprint(0xB1)
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 100, clockMax: 100, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 100, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [resourceEntry(path: "2026/03/a.jpg", hash: hash)]
            )))],
            month: month, respectTaskCancellation: false
        )

        let materializer1 = RepoMaterializer(client: client, basePath: basePath)
        let output1 = try await materializer1.materialize(expectedRepoID: repoID)
        let assetAfterReplay = try XCTUnwrap(output1.state.months[month]?.assets[fp])
        let stamp1 = try XCTUnwrap(assetAfterReplay.stamp, "replay must stamp the asset row")
        XCTAssertEqual(stamp1, OpStamp(writerID: writerA, seq: 1, clock: 100))

        // Write a snapshot containing the materialized state, then re-materialize from
        // a fresh client (only the snapshot — commits not transferred). Stamp must
        // survive the wire round-trip.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let parts = RepoSnapshotBuilder.build(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered
            ),
            state: output1.state.months[month] ?? .empty
        )
        let snapHeader = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA, repoID: repoID, covered: covered
        )
        _ = try await snapshotWriter.write(
            header: snapHeader,
            assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: month, lamport: 100, runID: runID,
            respectTaskCancellation: false
        )

        let snapshotOnlyClient = try await transplantSnapshotsOnly(from: client)
        let materializer2 = RepoMaterializer(client: snapshotOnlyClient, basePath: basePath)
        let output2 = try await materializer2.materialize(expectedRepoID: repoID)
        let assetFromSnapshot = try XCTUnwrap(output2.state.months[month]?.assets[fp])
        XCTAssertEqual(assetFromSnapshot.stamp, stamp1, "stamp must persist through encode→decode→re-materialize")
    }

    // MARK: - Test 2: Cross-writer add-vs-stale-add LWW

    func testStaleAddDoesNotOverwriteNewerStampedBaseline() async throws {
        // Writer B writes the most recent add at clock=200. Snapshot bakes B's add.
        // Writer A's older commit at clock=100 sits in commit log uncovered (e.g.,
        // B's snapshot covered B's seqs only). Replay must NOT overwrite B's row.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.fingerprint(0xA2)

        // A's old commit (uncovered).
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 100, clockMax: 100, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 100, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [resourceEntry(path: "2026/03/a-from-A.jpg", hash: TestFixtures.fingerprint(0xC1))]
            )))],
            month: month, respectTaskCancellation: false
        )
        // B's newer commit (covered).
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 200, clockMax: 200, writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 200, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 2,
                resources: [resourceEntry(path: "2026/03/a-from-B.jpg", hash: TestFixtures.fingerprint(0xC2))]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Snapshot covers ONLY B's seq, so A's commit gets replayed every time.
        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let snapState = RepoMonthState(
            assets: [fp: SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 2,
                resourceCount: 1, totalFileSizeBytes: 100,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 200)
            )],
            resources: [
                "2026/03/a-from-B.jpg": SnapshotResourceRow(
                    physicalRemotePath: "2026/03/a-from-B.jpg",
                    contentHash: TestFixtures.fingerprint(0xC2),
                    fileSize: 100, resourceType: ResourceTypeCode.photo,
                    creationDateMs: nil, backedUpAtMs: 2, crypto: nil
                )
            ],
            assetResources: [:],
            deletedAssetFingerprints: []
        )
        let parts = RepoSnapshotBuilder.build(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            state: snapState
        )
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: month, lamport: 200, runID: "run-snap-B",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let asset = try XCTUnwrap(output.state.months[month]?.assets[fp])
        XCTAssertEqual(asset.stamp?.clock, 200, "stale add at clock=100 must NOT overwrite newer baseline")
        XCTAssertEqual(asset.stamp?.writerID, writerB)
        XCTAssertEqual(asset.backedUpAtMs, 2, "B's row data must survive replay")
    }

    // MARK: - Test 3: Observation-tombstone with stamped baseline (heal baked in)

    func testObservationTombstoneSkippedWhenHealStampedInBaseline() async throws {
        // Snapshot baseline contains heal at stamp clock=300 (writer B). Observation
        // tombstone arrives via commit replay with basis at clock=200 — the basis is
        // strictly less than the heal stamp, so the tombstone must be skipped.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.fingerprint(0xA3)

        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let snapState = RepoMonthState(
            assets: [fp: SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 300)
            )],
            resources: [:], assetResources: [:],
            deletedAssetFingerprints: []
        )
        let parts = RepoSnapshotBuilder.build(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            state: snapState
        )
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: month, lamport: 300, runID: "run-snap-heal",
            respectTaskCancellation: false
        )

        // Writer A issues observation tombstone with basis at clock=200 — older than
        // the heal stamp clock=300 in baseline. Tombstone must be skipped.
        let basis = TombstoneObservationBasis(perWriterMaxSeq: [:], lamportWatermark: 200)
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 350, clockMax: 350, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 350, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp, reason: .verifyFailed, observedBasis: basis
            )))],
            month: month, respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fp], "heal in baseline must survive observation tombstone whose basis predates it")
        XCTAssertFalse(monthState.deletedAssetFingerprints.contains(fp))
    }

    // MARK: - Test 4: tombstone-baseline-vs-stale-add LWW (deletedAssetStamps)

    func testStaleAddReplayedAgainstTombstoneBaseline_doesNotResurrect() async throws {
        // Snapshot baseline: fp X tombstoned at clock=200 by writer B.
        // Writer A's uncovered stale add at clock=100 replays. Add-stamp gate alone
        // doesn't fire (state.assets[fp] is nil for tombstoned); deletedAssetStamps
        // gate must catch this and skip the add.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.fingerprint(0xA4)

        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 100, clockMax: 100, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 100, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [resourceEntry(path: "2026/03/a.jpg", hash: TestFixtures.fingerprint(0xC4))]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Build a baseline snapshot that has X tombstoned with stamp clock=200,
        // covered = writerB only (so A's seq=1 commit will be REPLAYED, not skipped).
        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let snapState = RepoMonthState(
            assets: [:], resources: [:], assetResources: [:],
            deletedAssetFingerprints: [fp],
            deletedAssetStamps: [fp: OpStamp(writerID: writerB, seq: 1, clock: 200)]
        )
        let parts = RepoSnapshotBuilder.build(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            state: snapState
        )
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: covered
            ),
            assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: month, lamport: 200, runID: "run-snap-tombstone",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNil(monthState.assets[fp], "stale add at clock=100 must NOT resurrect a clock=200 tombstone")
        XCTAssertTrue(monthState.deletedAssetFingerprints.contains(fp))
    }

    func testTombstoneStampSurvivesSnapshotRoundTrip() throws {
        let raw = "{\"t\":\"deleted_key\",\"r\":{\"keyType\":\"asset\",\"keyValue\":\"\(String(repeating: "ab", count: 32))\",\"lastWriterID\":\"writer-A\",\"lastSeq\":7,\"lastClock\":42}}"
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .deletedKey(let parsed) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(parsed.stamp, OpStamp(writerID: "writer-A", seq: 7, clock: 42))
    }

    func testLegacyTombstoneWithoutStampDecodes() throws {
        let raw = "{\"t\":\"deleted_key\",\"r\":{\"keyType\":\"asset\",\"keyValue\":\"\(String(repeating: "ab", count: 32))\"}}"
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .deletedKey(let parsed) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertNil(parsed.stamp, "legacy deletedKey rows must decode with stamp=nil")
    }

    // MARK: - Test 5: Legacy snapshot without stamp field

    func testLegacySnapshotWithoutStampDecodes() throws {
        // Encoder writes legacy-shape (no stamp triple). Decoder must accept,
        // returning stamp=nil — the materializer falls back to replay-only behavior.
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"creationDateMs":null,"resourceCount":0,"totalFileSizeBytes":0}}"#
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .asset(let parsed) = decoded else { XCTFail("asset"); return }
        XCTAssertNil(parsed.stamp, "legacy snapshot rows must decode with stamp=nil")
    }

    func testPartialStampTripleRejected() {
        // Atomic null-or-present: lastWriterID without lastSeq/lastClock is malformed.
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"creationDateMs":null,"resourceCount":0,"totalFileSizeBytes":0,"lastWriterID":"writer-A"}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    // MARK: - Helpers

    private func makeHeader(seq: UInt64, clockMin: UInt64, clockMax: UInt64, writerID: String) -> CommitHeader {
        TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: seq, runID: runID,
            month: month, clockMin: clockMin, clockMax: clockMax
        )
    }

    private func resourceEntry(path: String, hash: Data) -> CommitResourceEntry {
        CommitResourceEntry(
            physicalRemotePath: path,
            logicalName: (path as NSString).lastPathComponent,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0,
            crypto: nil
        )
    }

    /// Copy snapshot files only (drop commit log) so re-materialize must rely on
    /// the snapshot — proves the stamp survived the wire format, not the in-memory
    /// replay path.
    private func transplantSnapshotsOnly(from source: InMemoryRemoteStorageClient) async throws -> InMemoryRemoteStorageClient {
        let dest = InMemoryRemoteStorageClient()
        try await dest.connect()
        try await TestFixtures.injectRepoJSON(dest, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(dest, basePath: basePath, writerID: writerA)
        try await dest.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let allFiles = await source.snapshotFiles()
        let snapshotsPrefix = "\(basePath)/.watermelon/snapshots/"
        for (path, data) in allFiles where path.hasPrefix(snapshotsPrefix) {
            await dest.injectFile(path: path, data: data)
        }
        return dest
    }
}
