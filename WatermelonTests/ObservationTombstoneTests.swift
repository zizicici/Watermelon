import XCTest
@testable import Watermelon

/// Tombstone observation basis (wire format v3). Verifies the materializer's
/// "skip tombstone if heal happened after observation" rule that closes the
/// verify→tombstone TOCTOU window.
final class ObservationTombstoneTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let monthKey = LibraryMonthKey(year: 2026, month: 5)
    private let runID = "run-test"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    private func computedFP(role: Int = ResourceTypeCode.photo, slot: Int = 0, hash: Data) -> AssetFingerprint {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: hash)]
        )
    }

    private func writeAddAsset(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seq: UInt64,
        clock: UInt64,
        fp: AssetFingerprint,
        hash: Data
    ) async throws {
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let body = CommitAddAssetBody(
            assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
            resources: [CommitResourceEntry(
                physicalRemotePath: "2026/05/p.jpg",
                logicalName: "p.jpg",
                contentHash: hash, fileSize: 100,
                resourceType: ResourceTypeCode.photo,
                role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )]
        )
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: seq, runID: runID, month: monthKey,
            clockMin: clock, clockMax: clock
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
            month: monthKey, respectTaskCancellation: false
        )
    }

    private func writeTombstone(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seq: UInt64,
        clock: UInt64,
        fp: AssetFingerprint,
        basis: TombstoneObservationBasis
    ) async throws {
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: seq, runID: runID, month: monthKey,
            clockMin: clock, clockMax: clock
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: clock, body: .tombstoneAsset(
                CommitTombstoneBody(assetFingerprint: fp, reason: .verifyFailed, observedBasis: basis)
            ))],
            month: monthKey, respectTaskCancellation: false
        )
    }

    /// Baseline: tombstone with no concurrent activity → asset is tombstoned.
    func testTombstoneApplies_whenNoHealHappenedAfterObservation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = TestFixtures.fingerprint(0xAA)
        let fp = computedFP(hash: hash)
        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: [writerA: 1],
            lamportWatermark: 1
        )
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 2, fp: fp, basis: basis)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNil(monthState.assets[fp], "tombstone must apply when no heal arrived")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
    }

    /// A writer always observes its own earlier adds, so a same-writer heal whose basis does not even
    /// cover that add's seq is an understated/forged basis. The materializer must NOT let it nullify the
    /// tombstone (which would resurrect a committed-deleted asset while folding clean). Clocks stay
    /// monotonic (add 1 < tombstone 5) so the per-writer monotonicity guard keeps both commits — the
    /// only defense is the heal's basis-trust check.
    func testTombstoneApplies_whenSameWriterAddHealsViaUnderstatedBasis() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = TestFixtures.fingerprint(0xAC)
        let fp = computedFP(hash: hash)
        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)
        // Understated basis: empty perWriterMaxSeq + watermark 0 makes A's own seq-1/clock-1 add look
        // "after basis" under the raw heal check.
        let basis = TombstoneObservationBasis(perWriterMaxSeq: [:], lamportWatermark: 0)
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 5, fp: fp, basis: basis)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNil(monthState.assets[fp],
                     "same-writer understated-basis heal must not resurrect a committed-deleted asset")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
    }

    /// TOCTOU close: writer B heals fp AFTER writer A's verify-observation,
    /// before writer A's tombstone lands. Materializer must skip the tombstone.
    func testTombstoneSkipped_whenWriterBHealsAfterObservation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = TestFixtures.fingerprint(0xBB)
        let fp = computedFP(hash: hash)

        // writer A's original add at clock=1, seq=1
        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)

        // Writer A observes (basis: A=1, B not yet seen, lamport=1).
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: [writerA: 1],
            lamportWatermark: 1
        )

        // Concurrently, writer B heals at clock=5, seq=10 (post-observation).
        try await writeAddAsset(client: client, writerID: writerB, seq: 10, clock: 5, fp: fp, hash: hash)

        // Writer A's tombstone lands at clock=10.
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 10, fp: fp, basis: basis)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[fp], "concurrent heal must survive — tombstone skipped")
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp))
    }

    /// A checkpoint that folds an applied observation tombstone into a snapshot baseline must preserve the
    /// tombstone's observedBasis, so a later concurrent lower-clock heal-add still survives across the
    /// snapshot boundary — exactly as in a single raw replay
    /// (`testTombstoneSkipped_whenWriterBHealsAfterObservation`). Without the persisted basis the baked
    /// deletedKey degrades to pure LWW and silently drops the re-added asset.
    func testHealSurvivesAcrossCheckpointBaselineBoundary() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        let hash = TestFixtures.fingerprint(0xBD)
        let fp = computedFP(hash: hash)

        // Writer A adds X, then verify-observes it (basis covers only A's own add) and tombstones it
        // before any concurrent heal arrives, so the tombstone applies and X materializes absent.
        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)
        let basis = TombstoneObservationBasis(perWriterMaxSeq: [writerA: 1], lamportWatermark: 1)
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 10, fp: fp, basis: basis)

        let m1 = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertNil(m1.state.months[monthKey]?.assets[fp], "tombstone applies before any heal")
        XCTAssertTrue(m1.state.months[monthKey]?.deletedAssetStamps.keys.contains(fp) ?? false)

        // Fold A's add + tombstone into a covered-max baseline via the same builder the checkpoint uses
        // (RepoSnapshotBuilder.build); the baked deletedKey must persist the observation basis.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: writerA,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: try XCTUnwrap(m1.state.months[monthKey]))
        XCTAssertEqual(parts.deletedKeys.first(where: { $0.keyValue == fp.rawValue.hexString })?.observedBasis, basis,
                       "checkpoint baseline must carry the tombstone basis")
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: 11,
            runID: runID,
            respectTaskCancellation: false
        )

        // Writer B, which never observed A's tombstone, concurrently re-adds X at a clock below the
        // tombstone's. Its commit is uncovered by the baseline, so it replays against the baked deletedKey.
        try await writeAddAsset(client: client, writerID: writerB, seq: 10, clock: 5, fp: fp, hash: hash)

        let m2 = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(m2.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[fp], "concurrent heal must survive the checkpoint baseline boundary")
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp))
        XCTAssertEqual(m2.outcomeByMonth[monthKey], .clean)
        // The baseline (covered {A:1-2}) is the authority that carried the tombstone, so the heal came from
        // the persisted basis rather than a raw replay of A's now-covered commits.
        XCTAssertEqual(m2.acceptedSnapshotBaselinesByMonth[monthKey]?.covered, covered)
    }

    /// Wire round-trip: a snapshot deletedKey carrying an observation basis decodes to exact equality.
    func testSnapshotDeletedKeyRoundTrip_withObservedBasis() throws {
        let basis = TombstoneObservationBasis(perWriterMaxSeq: [writerA: 1, writerB: 7], lamportWatermark: 9)
        let row = SnapshotDeletedKeyRow(
            keyType: .asset,
            keyValue: TestFixtures.assetFingerprint(0xDE).rawValue.hexString,
            stamp: OpStamp(writerID: writerA, seq: 2, clock: 10),
            observedBasis: basis
        )
        let line = try SnapshotRowMapper.encodeDeletedKeyLine(row)
        guard case .deletedKey(let decoded) = try SnapshotRowMapper.decodeLine(line) else {
            XCTFail("expected deletedKey row")
            return
        }
        XCTAssertEqual(decoded, row)
        XCTAssertEqual(decoded.observedBasis, basis)
    }

    /// Heal happened BEFORE observation (already seen at observation time) →
    /// the basis encompasses the heal → tombstone applies normally. The basis
    /// is only meant to skip POST-observation heals.
    func testTombstoneApplies_whenHealHappenedBeforeObservation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = TestFixtures.fingerprint(0xCC)
        let fp = computedFP(hash: hash)

        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)
        try await writeAddAsset(client: client, writerID: writerB, seq: 5, clock: 3, fp: fp, hash: hash)

        // Observation AFTER both adds — basis covers both writers.
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: [writerA: 1, writerB: 5],
            lamportWatermark: 3
        )
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 10, fp: fp, basis: basis)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNil(monthState.assets[fp], "all heals were observed; tombstone valid")
    }

    /// Heal from a writer NOT in basis (joined post-observation) is treated as
    /// post-basis — perWriterMaxSeq lookup misses → defaults to 0 → seq>0 wins.
    func testTombstoneSkipped_whenHealFromUnknownWriter() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = TestFixtures.fingerprint(0xEE)
        let fp = computedFP(hash: hash)
        try await writeAddAsset(client: client, writerID: writerA, seq: 1, clock: 1, fp: fp, hash: hash)
        // basis only mentions writerA — writerB is unknown to it.
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: [writerA: 1],
            lamportWatermark: 1
        )
        // writerB heals with seq=1 (would be < basis if it were writerA's seq), clock < watermark+1.
        // Watermark=1, B's clock=1 → not strictly > watermark. seq=1 > basis[B]=nil(=0) → after-basis.
        try await writeAddAsset(client: client, writerID: writerB, seq: 1, clock: 1, fp: fp, hash: hash)
        try await writeTombstone(client: client, writerID: writerA, seq: 2, clock: 5, fp: fp, basis: basis)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[fp],
                        "writer not in basis → all their ops are post-observation → heal wins")
    }

    /// Wire round-trip: encode tombstone with basis → decode → exact equality.
    func testWireRoundTrip_tombstoneWithBasis() throws {
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: [writerA: 100, writerB: 200],
            lamportWatermark: 500
        )
        let body = CommitTombstoneBody(
            assetFingerprint: TestFixtures.assetFingerprint(0xFA),
            reason: .verifyFailed,
            observedBasis: basis
        )
        let op = CommitOp(opSeq: 0, clock: 600, body: .tombstoneAsset(body))
        let line = try CommitOpMapper.encodeOpLine(op)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .op(let parsed) = decoded,
              case .tombstoneAsset(let parsedBody) = parsed.body else {
            XCTFail("expected tombstone op, got \(decoded)")
            return
        }
        XCTAssertEqual(parsedBody.assetFingerprint, body.assetFingerprint)
        XCTAssertEqual(parsedBody.reason, .verifyFailed)
        XCTAssertEqual(parsedBody.observedBasis, basis)
    }

    /// A snapshot deletedKey whose `observedBasis.perWriterMaxSeq` is present but not an object must fail
    /// closed — decoding it to an empty (weaker) basis would let a stale add resurrect a tombstoned asset.
    func testSnapshotDeletedKey_malformedPerWriterMaxSeq_failsClosed() throws {
        func line(perWriterMaxSeq: Any) throws -> String {
            let inner: [String: Any] = [
                "keyType": "asset",
                "keyValue": TestFixtures.assetFingerprint(0xDF).rawValue.hexString,
                "lastWriterID": writerA, "lastSeq": 2, "lastClock": 10,
                "observedBasis": ["lamportWatermark": 50, "perWriterMaxSeq": perWriterMaxSeq]
            ]
            return try CommitOpMapper.jsonLine(dict: ["t": "deleted_key", "r": inner])
        }
        for bad in ["nope" as Any, 7 as Any, [1, 2] as Any, NSNull() as Any] {
            XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(line(perWriterMaxSeq: bad))) { error in
                XCTAssertTrue(error is SnapshotWireError, "expected SnapshotWireError, got \(error)")
            }
        }
        guard case .deletedKey(let row) = try SnapshotRowMapper.decodeLine(line(perWriterMaxSeq: [writerA: 1])) else {
            return XCTFail("well-formed basis must decode")
        }
        XCTAssertEqual(row.observedBasis?.perWriterMaxSeq[writerA], 1)
    }

    /// Same fail-closed rule for a commit tombstone op's `observedBasis.perWriterMaxSeq` (raw-replay path).
    func testCommitTombstone_malformedPerWriterMaxSeq_failsClosed() throws {
        func line(perWriterMaxSeq: Any) throws -> String {
            let body: [String: Any] = [
                "assetFingerprint": TestFixtures.assetFingerprint(0xDF).rawValue.hexString,
                "reason": "verifyFailed",
                "observedBasis": ["lamportWatermark": 50, "perWriterMaxSeq": perWriterMaxSeq]
            ]
            return try CommitOpMapper.jsonLine(
                dict: ["t": "op", "opSeq": 0, "clock": 60, "kind": "tombstoneAsset", "body": body]
            )
        }
        for bad in ["nope" as Any, 7 as Any, [1, 2] as Any, NSNull() as Any] {
            XCTAssertThrowsError(try CommitOpMapper.decodeLine(line(perWriterMaxSeq: bad))) { error in
                XCTAssertTrue(error is CommitWireError, "expected CommitWireError, got \(error)")
            }
        }
        guard case .op(let op) = try CommitOpMapper.decodeLine(line(perWriterMaxSeq: [writerA: 3])),
              case .tombstoneAsset(let tb) = op.body else {
            return XCTFail("well-formed basis must decode")
        }
        XCTAssertEqual(tb.observedBasis.perWriterMaxSeq[writerA], 3)
    }
}
