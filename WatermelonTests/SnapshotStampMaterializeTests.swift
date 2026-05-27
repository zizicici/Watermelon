import XCTest
@testable import Watermelon

final class SnapshotStampMaterializeTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-stamp-001"
    private let month = LibraryMonthKey(year: 2026, month: 3)


    func testStampRoundTripsThroughCommitToSnapshotToReMaterialize() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xA1)
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


    func testStaleAddDoesNotOverwriteNewerStampedBaseline() async throws {
        // Writer B writes the most recent add at clock=200. Snapshot bakes B's add.
        // Writer A's older commit at clock=100 sits in commit log uncovered (e.g.,
        // B's snapshot covered B's seqs only). Replay must NOT overwrite B's row.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xA2)

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
            deletedAssetStamps: [:]
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
        XCTAssertEqual(asset.stamp.clock, 200, "stale add at clock=100 must NOT overwrite newer baseline")
        XCTAssertEqual(asset.stamp.writerID, writerB)
        XCTAssertEqual(asset.backedUpAtMs, 2, "B's row data must survive replay")
    }


    func testObservationTombstoneSkippedWhenHealStampedInBaseline() async throws {
        // Snapshot baseline contains heal at stamp clock=300 (writer B). Observation
        // tombstone arrives via commit replay with basis at clock=200 — the basis is
        // strictly less than the heal stamp, so the tombstone must be skipped.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xA3)

        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let snapState = RepoMonthState(
            assets: [fp: SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 300)
            )],
            resources: [:], assetResources: [:],
            deletedAssetStamps: [:]
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
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp))
    }


    func testStaleAddReplayedAgainstTombstoneBaseline_doesNotResurrect() async throws {
        // Snapshot baseline: fp X tombstoned at clock=200 by writer B.
        // Writer A's uncovered stale add at clock=100 replays. Add-stamp gate alone
        // doesn't fire (state.assets[fp] is nil for tombstoned); deletedAssetStamps
        // gate must catch this and skip the add.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xA4)

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
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
    }

    func testTombstoneStampSurvivesSnapshotRoundTrip() throws {
        let raw = "{\"t\":\"deleted_key\",\"r\":{\"keyType\":\"asset\",\"keyValue\":\"\(String(repeating: "ab", count: 32))\",\"lastWriterID\":\"11111111-1111-1111-1111-aaaaaaaaaaaa\",\"lastSeq\":7,\"lastClock\":42}}"
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .deletedKey(let parsed) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(parsed.stamp, OpStamp(writerID: "11111111-1111-1111-1111-aaaaaaaaaaaa", seq: 7, clock: 42))
    }

    func testSnapshotWithoutStampRejects() {
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"creationDateMs":null,"resourceCount":0,"totalFileSizeBytes":0}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw))
    }

    func testPartialStampTripleRejected() {
        // Atomic null-or-present: lastWriterID without lastSeq/lastClock is malformed.
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"creationDateMs":null,"resourceCount":0,"totalFileSizeBytes":0,"lastWriterID":"11111111-1111-1111-1111-aaaaaaaaaaaa"}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.missingField("stamp") = err else {
                XCTFail("expected missing stamp, got \(err)"); return
            }
        }
    }


    func testStaleUncoveredAddDoesNotOverwriteNewerResourceRowAtSamePath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fpA = TestFixtures.assetFingerprint(0xA5)
        let fpB = TestFixtures.assetFingerprint(0xA6)
        let hashH1 = TestFixtures.fingerprint(0xC5)
        let hashH2 = TestFixtures.fingerprint(0xC6)
        let sharedPath = "2026/03/shared.jpg"

        // A's older uncovered commit writes path -> H1 at clock 100.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 100, clockMax: 100, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 100, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resources: [resourceEntry(path: sharedPath, hash: hashH1)]
            )))],
            month: month, respectTaskCancellation: false
        )
        // B's newer covered commit writes path -> H2 at clock 200.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 200, clockMax: 200, writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 200, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                resources: [resourceEntry(path: sharedPath, hash: hashH2)]
            )))],
            month: month, respectTaskCancellation: false
        )

        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let bStamp = OpStamp(writerID: writerB, seq: 1, clock: 200)
        let snapState = RepoMonthState(
            assets: [fpB: SnapshotAssetRow(
                assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                resourceCount: 1, totalFileSizeBytes: 100, stamp: bStamp
            )],
            resources: [
                sharedPath: SnapshotResourceRow(
                    physicalRemotePath: sharedPath,
                    contentHash: hashH2,
                    fileSize: 100, resourceType: ResourceTypeCode.photo,
                    creationDateMs: nil, backedUpAtMs: 2, crypto: nil,
                    stamp: bStamp
                )
            ],
            assetResources: [
                AssetResourceKey(assetFingerprint: fpB, role: ResourceTypeCode.photo, slot: 0):
                    SnapshotAssetResourceRow(
                        assetFingerprint: fpB, role: ResourceTypeCode.photo, slot: 0,
                        resourceHash: hashH2, logicalName: "shared.jpg"
                    )
            ],
            deletedAssetStamps: [:]
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
            month: month, lamport: 200, runID: "run-snap-pathLWW",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        let row = try XCTUnwrap(monthState.resources[sharedPath])
        XCTAssertEqual(row.contentHash, hashH2,
                       "stale uncovered add at clock=100 must NOT replace baseline H2 row at the shared path")
        XCTAssertEqual(row.stamp.clock, 200, "winning row's stamp must be B's")
        // Both assets still recorded — neither's add is dropped, only the per-path
        // resource overwrite is gated.
        XCTAssertNotNil(monthState.assets[fpA])
        XCTAssertNotNil(monthState.assets[fpB])
    }

    func testResourceRowStampSurvivesWireRoundTrip() throws {
        let row = SnapshotResourceRow(
            physicalRemotePath: "2026/05/IMG.HEIC",
            contentHash: Data(repeating: 0x12, count: 32),
            fileSize: 1234,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: 99,
            backedUpAtMs: 100,
            crypto: nil,
            stamp: OpStamp(writerID: "11111111-1111-1111-1111-aaaaaaaaaaaa", seq: 9, clock: 77)
        )
        let line = try SnapshotRowMapper.encodeResourceLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .resource(let parsed) = decoded else { XCTFail("resource"); return }
        XCTAssertEqual(parsed, row)
        XCTAssertEqual(parsed.stamp.clock, 77)
    }

    func testResourceRowWithoutStampRejects() {
        let raw = #"{"t":"resource","r":{"physicalRemotePath":"2026/05/IMG.HEIC","contentHash":"\#(String(repeating: "ab", count: 32))","fileSize":100,"resourceType":1,"creationDateMs":null,"backedUpAtMs":100,"crypto":null}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw))
    }


    func testSnapshotWithStampOutsideCoveredRanges_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xD1)

        // Snapshot covers only writerB seq 1, but the row stamp claims writerA seq=5,
        // outside coverage. The snapshot must be rejected so replay isn't poisoned.
        var covered = CoveredRanges()
        covered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let snapState = RepoMonthState(
            assets: [fp: SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 5, clock: 100)
            )],
            resources: [:], assetResources: [:],
            deletedAssetStamps: [:]
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
            month: month, lamport: 100, runID: "run-uncovered-stamp",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertNil(
            output.state.months[month]?.assets[fp],
            "snapshot whose row stamp falls outside covered ranges must be skipped"
        )
    }

    func testSnapshotWithStampedRowAndEmptyCoveredRanges_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let fp = TestFixtures.assetFingerprint(0xD2)
        let snapState = RepoMonthState(
            assets: [fp: SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            resources: [:], assetResources: [:],
            deletedAssetStamps: [:]
        )
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA, repoID: repoID, covered: .empty
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: snapState)
        _ = try await snapshotWriter.write(
            header: header,
            assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: month, lamport: 10, runID: "run-empty-covered-stamp",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)

        XCTAssertNil(
            output.state.months[month]?.assets[fp],
            "empty covered ranges must not make stamped snapshot rows authoritative"
        )
    }

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
