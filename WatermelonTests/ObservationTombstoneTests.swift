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
}
