import XCTest
@testable import Watermelon

/// End-to-end materializer round-trips: write commits + snapshots through real
/// CommitLogWriter / SnapshotWriter, then materialize and assert reconstructed
/// state. Backstop tests for the V2 cutover — filename mismatch, foreign-repo
/// filter, snapshot fallback, covered ranges suppression.
final class RepoMaterializerRoundTripTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "repo-test-id"
    private let runID = "run-001"
    private let month = LibraryMonthKey(year: 2026, month: 1)

    func testSingleAddAssetRoundTrip() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let fp = Self.fingerprint(0xAA)
        let hash = Self.fingerprint(0xBB)
        let commit = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg",
                    logicalName: "photo.jpg",
                    contentHash: hash,
                    fileSize: 1024,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )))
        let header = makeHeader(seq: 1, clockMin: 1, clockMax: 1)
        _ = try await writer.write(header: header, ops: [commit], month: month, respectTaskCancellation: false)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertEqual(monthState.assets[fp]?.assetFingerprint, fp)
        XCTAssertEqual(monthState.resources["2026/01/photo.jpg"]?.contentHash, hash)
        XCTAssertEqual(output.observedSeqByWriter[writerA], 1)
        XCTAssertEqual(output.state.observedClock, 1)
    }

    func testTombstoneAfterAddRemovesAsset() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0xCC)

        let addOp = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: []
        )))
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [addOp],
            month: month,
            respectTaskCancellation: false
        )

        let tombstoneOp = CommitOp(opSeq: 0, clock: 2, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: fp,
            reason: .userDeleted
        )))
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 2, clockMax: 2),
            ops: [tombstoneOp],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNil(monthState.assets[fp])
        XCTAssertTrue(monthState.deletedAssetFingerprints.contains(fp))
    }

    func testTombstoneBeforeAddByClockOrderResurrectsAsset() async throws {
        // Out-of-order seqs but the materializer sorts by clock — so a higher-clock
        // addAsset after a lower-clock tombstone should resurrect the asset.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0xDD)

        let tombstone = CommitOp(opSeq: 0, clock: 5, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: fp,
            reason: .verifyFailed
        )))
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [tombstone],
            month: month,
            respectTaskCancellation: false
        )

        let add = CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: []
        )))
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 10, clockMax: 10),
            ops: [add],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fp])
        XCTAssertFalse(monthState.deletedAssetFingerprints.contains(fp))
    }

    func testForeignRepoCommitsFilteredOut() async throws {
        // Real cross-writer scenario: a foreign writerID writing under a foreign repoID
        // (e.g. user re-pointed profile at a different bucket, or a peer writing the
        // wrong repo). The materializer must filter the foreign commit *and* track
        // the foreign writerID's seq so the allocator can never collide with it.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let ourFP = Self.fingerprint(0x01)
        let foreignFP = Self.fingerprint(0xFF)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: ourFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: makeHeader(seq: 5, clockMin: 5, clockMax: 5, repoID: "foreign-repo-id", writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: foreignFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[ourFP])
        XCTAssertNil(monthState.assets[foreignFP], "foreign-repo commit must be filtered out")
        // Both writerIDs' seqs must advance from filenames so the allocator
        // doesn't collide on either side.
        XCTAssertEqual(output.observedSeqByWriter[writerA], 1)
        XCTAssertEqual(output.observedSeqByWriter[writerB], 5,
                       "foreign writerID's seq must still be tracked from filename")
    }

    func testForeignRepoSnapshotAtTopFallsBackToNextCandidate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let ourFP = Self.fingerprint(0x10)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        // Write our snapshot at lamport 5
        var ourCovered = CoveredRanges()
        ourCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let ourHeader = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: ourCovered
        )
        _ = try await snapshotWriter.write(
            header: ourHeader,
            assets: [SnapshotAssetRow(
                assetFingerprint: ourFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: "run-our",
            respectTaskCancellation: false
        )

        // Foreign-repo snapshot at higher lamport 10 — should be skipped, our snapshot used
        var foreignCovered = CoveredRanges()
        foreignCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        let foreignHeader = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerB,
            repoID: "foreign-repo-id",
            covered: foreignCovered
        )
        _ = try await snapshotWriter.write(
            header: foreignHeader,
            assets: [SnapshotAssetRow(
                assetFingerprint: Self.fingerprint(0xEE),
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-foreign",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[ourFP], "should fall back to our-repo snapshot")
        XCTAssertNil(monthState.assets[Self.fingerprint(0xEE)], "foreign snapshot's asset must not be loaded")
    }

    func testCorruptSnapshotAtTopFallsBackToNextCandidate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let ourFP = Self.fingerprint(0x20)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered
        )
        _ = try await snapshotWriter.write(
            header: header,
            assets: [SnapshotAssetRow(
                assetFingerprint: ourFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: "run-good",
            respectTaskCancellation: false
        )
        // Write a "newer" snapshot then truncate it to corrupt.
        _ = try await snapshotWriter.write(
            header: header,
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-bad-uuid",
            respectTaskCancellation: false
        )
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 10,
            writerID: writerA,
            runID: "run-bad-uuid"
        )
        await client.truncateInHalf(path: badPath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[ourFP], "should fall back to good snapshot at lower lamport")
    }

    func testCoveredRangesSuppressAlreadyBakedCommits() async throws {
        // Realistic scenario: writer baked commits 1-5 into a snapshot, then continued
        // writing commit 6. After cold-start materialize, the snapshot contributes
        // baseFP via its assets section, the covered range tells the materializer to
        // skip replay of seq 3 (the original commit), and seq 6 (uncovered) is replayed
        // to add laterFP.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let baseFP = Self.fingerprint(0x30)
        let laterFP = Self.fingerprint(0x60)

        // Commit at seq 3 adds baseFP. This same commit's effects are baked into the
        // snapshot below (covered=[1..5]), so materializer must NOT replay it.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 3, clockMin: 3, clockMax: 3),
            ops: [CommitOp(opSeq: 0, clock: 3, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: baseFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        // Snapshot at lamport 5 covering seqs 1-5; baseFP is in its assets section
        // (faithful summary, not a lie).
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 5))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: baseFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: "run-snap",
            respectTaskCancellation: false
        )

        // Commit at seq 6 — outside covered range, must be replayed.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 6, clockMin: 6, clockMax: 6),
            ops: [CommitOp(opSeq: 0, clock: 6, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: laterFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[baseFP], "baseFP comes in via the snapshot")
        XCTAssertNotNil(monthState.assets[laterFP], "uncovered seq 6 must be replayed")
        // covered must include baseline range + the replayed seq 6, so the next
        // snapshot writer can claim a superset.
        let resultCovered = output.coveredByMonth[month] ?? .empty
        XCTAssertTrue(resultCovered.contains(writerID: writerA, seq: 3))
        XCTAssertTrue(resultCovered.contains(writerID: writerA, seq: 6))
    }

    func testSkippedCommitFilenameAdvancesObservedSeq() async throws {
        // Cold-start scenario: corrupt commit at seq 7 — even if we can't replay
        // it, observedSeqByWriter[writerA] should be ≥ 7 so the allocator doesn't
        // try to write seq 1, 2, 3, ... and collide on filename.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        _ = try await writer.write(
            header: makeHeader(seq: 7, clockMin: 7, clockMax: 7),
            ops: [CommitOp(opSeq: 0, clock: 7, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: Self.fingerprint(0x50),
                creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        // Truncate so the materializer skips it.
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 7)
        await client.truncateInHalf(path: path)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertEqual(
            output.observedSeqByWriter[writerA],
            7,
            "observedSeqByWriter must advance from filename even when commit body is corrupt"
        )
    }

    /// Materializer guards against a misnamed commit being assigned to the wrong
    /// (month, writer, seq) — pin the skip behavior so a manual file rename or partial
    /// move can't pollute state under a key that doesn't match the header.
    func testCommitWithFilenameMismatchingHeader_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x70)

        // Write a legit commit at seq=5 for writerA.
        _ = try await writer.write(
            header: makeHeader(seq: 5, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        // Move it to a DIFFERENT seq slot (filename), header still claims seq=5.
        let originalPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 5)
        let renamedPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 99)
        try await client.move(from: originalPath, to: renamedPath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        XCTAssertNil(output.state.months[month]?.assets[fp],
                     "filename-vs-header mismatch must be skipped, not replayed under wrong seq")
    }

    func testSnapshotWithFilenameMismatchingHeader_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x80)
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered
        )
        _ = try await writer.write(
            header: header,
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: "run-001",
            respectTaskCancellation: false
        )
        let originalPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerA, runID: "run-001"
        )
        // Rename into a writerB slot — header still says writerA.
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerB, runID: "run-001"
        )
        try await client.move(from: originalPath, to: badPath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        XCTAssertNil(output.state.months[month]?.assets[fp],
                     "snapshot whose filename writer disagrees with header must be skipped")
    }

    private func makeHeader(
        seq: UInt64,
        clockMin: UInt64,
        clockMax: UInt64,
        repoID: String? = nil,
        writerID: String? = nil
    ) -> CommitHeader {
        TestFixtures.makeCommitHeader(
            repoID: repoID ?? self.repoID,
            writerID: writerID ?? writerA,
            seq: seq,
            runID: runID,
            month: month,
            clockMin: clockMin,
            clockMax: clockMax
        )
    }

    private static func fingerprint(_ byte: UInt8) -> Data {
        TestFixtures.fingerprint(byte)
    }
}
