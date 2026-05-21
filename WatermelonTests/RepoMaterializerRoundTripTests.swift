import XCTest
@testable import Watermelon

final class RepoMaterializerRoundTripTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
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

    func testReplayTieBreaksByWriterSeqAndOpSeq() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let writerFP = Self.fingerprint(0xD1)
        let seqFP = Self.fingerprint(0xD2)
        let opSeqFP = Self.fingerprint(0xD3)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 10, clockMax: 10, writerID: writerA),
            ops: [
                CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: writerFP, creationDateMs: nil, backedUpAtMs: 1, resources: []))),
                CommitOp(opSeq: 1, clock: 10, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: seqFP, creationDateMs: nil, backedUpAtMs: 11, resources: [])))
            ],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 10, clockMax: 10, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: seqFP, creationDateMs: nil, backedUpAtMs: 12, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: makeHeader(seq: 3, clockMin: 10, clockMax: 10, writerID: writerA),
            ops: [
                CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: opSeqFP, creationDateMs: nil, backedUpAtMs: 21, resources: []))),
                CommitOp(opSeq: 1, clock: 10, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: opSeqFP, creationDateMs: nil, backedUpAtMs: 22, resources: [])))
            ],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 10, clockMax: 10, writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: writerFP, creationDateMs: nil, backedUpAtMs: 2, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])

        XCTAssertEqual(monthState.assets[writerFP]?.backedUpAtMs, 2)
        XCTAssertEqual(monthState.assets[seqFP]?.backedUpAtMs, 12)
        XCTAssertEqual(monthState.assets[opSeqFP]?.backedUpAtMs, 22)
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
            header: makeHeader(seq: 5, clockMin: 5, clockMax: 5, repoID: "99999999-9999-9999-9999-999999999999", writerID: writerB),
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
        // Accepted commit advances writerA; filename-derived seq advances writerB for
        // allocator collision avoidance even though the commit was rejected.
        XCTAssertEqual(output.observedSeqByWriter[writerA], 1)
        XCTAssertEqual(output.observedSeqByWriter[writerB], 5,
                       "foreign writerID's seq must be tracked from filename for allocator collision avoidance")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerB, seq: 5))
    }

    func testUnparseableMetadataFilenamesAreIgnoredByMaterializer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x02)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        await client.injectFile(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, RepoLayout.commitsDirectory, "junk-commit.jsonl"]),
            contents: "not a commit"
        )
        await client.injectFile(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, RepoLayout.snapshotsDirectory, "junk-snapshot.jsonl"]),
            contents: "not a snapshot"
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertEqual(output.observedSeqByWriter[writerA], 1)
        XCTAssertEqual(output.state.observedClock, 1)
        XCTAssertTrue(output.corruptedSnapshotMonths.isEmpty)
    }

    func testNonCanonicalCommitFilenameIsIgnoredByMaterializer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x12)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let canonicalPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let commitFiles = await client.snapshotFiles()
        let bytes = try XCTUnwrap(commitFiles[canonicalPath])
        let nonCanonicalPath = RepoLayout.normalize(joining: [
            basePath,
            RepoLayout.watermelonDirectory,
            RepoLayout.commitsDirectory,
            "\(month.text)--\(writerA)--1.jsonl"
        ])
        await client.injectFile(path: nonCanonicalPath, data: bytes)
        try await client.delete(path: canonicalPath)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month]?.assets[fp])
        XCTAssertNil(output.observedSeqByWriter[writerA])
        XCTAssertEqual(output.state.observedClock, 0)
    }

    func testNonCanonicalSnapshotFilenameIsIgnoredByMaterializer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x13)

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: .empty
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 2,
            runID: "abc123",
            respectTaskCancellation: false
        )
        let canonicalPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 2, writerID: writerA, runID: "abc123")
        let snapshotFiles = await client.snapshotFiles()
        let bytes = try XCTUnwrap(snapshotFiles[canonicalPath])
        let nonCanonicalPath = RepoLayout.normalize(joining: [
            basePath,
            RepoLayout.watermelonDirectory,
            RepoLayout.snapshotsDirectory,
            "\(month.text)--2--\(writerA)--abc123.jsonl"
        ])
        await client.injectFile(path: nonCanonicalPath, data: bytes)
        try await client.delete(path: canonicalPath)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month]?.assets[fp])
        XCTAssertNil(output.acceptedSnapshotBaselinesByMonth[month])
        XCTAssertEqual(output.state.observedClock, 0)
    }

    func testLegacyUnstampedSnapshotRejectedWhenExpectedRepoIDProvided() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let legacyFP = Self.fingerprint(0x03)

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: "",
                covered: .empty
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: legacyFP,
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
            runID: "legacy",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month]?.assets[legacyFP])
        XCTAssertEqual(output.state.observedClock, 0)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testSnapshotRowLevelSkipsKeepUsableRows() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let assetFP = Self.fingerprint(0x04)
        let goodHash = Self.fingerprint(0x05)
        let badHash = Self.fingerprint(0x06)
        let deletedFP = Self.fingerprint(0x07)
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: assetFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 2,
                totalFileSizeBytes: 2,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: "2026/01/good.jpg",
                    contentHash: goodHash,
                    fileSize: 1,
                    resourceType: ResourceTypeCode.photo,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotResourceRow(
                    physicalRemotePath: "2026/02/wrong-month.jpg",
                    contentHash: badHash,
                    fileSize: 1,
                    resourceType: ResourceTypeCode.photo,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                )
            ],
            assetResources: [
                SnapshotAssetResourceRow(
                    assetFingerprint: assetFP,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    resourceHash: goodHash,
                    logicalName: "good.jpg"
                )
            ],
            deletedKeys: [
                SnapshotDeletedKeyRow(keyType: .asset, keyValue: deletedFP.hexString, stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)),
                SnapshotDeletedKeyRow(keyType: .resource, keyValue: "resource-key")
            ],
            month: month,
            lamport: 3,
            runID: "rows",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let state = try XCTUnwrap(output.state.months[month])

        XCTAssertTrue(state.assets.isEmpty)
        XCTAssertTrue(state.resources.isEmpty)
        XCTAssertTrue(state.deletedAssetFingerprints.isEmpty)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testSnapshotMalformedAssetDeletedKeyCandidateFallsBack() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0x08)
        let badFP = Self.fingerprint(0x09)

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: .empty
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 2,
            runID: "good-deleted-key",
            respectTaskCancellation: false
        )

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: .empty
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: badFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [SnapshotDeletedKeyRow(keyType: .asset, keyValue: "not-a-hex")],
            month: month,
            lamport: 5,
            runID: "bad-deleted-key",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let state = try XCTUnwrap(output.state.months[month])

        XCTAssertNotNil(state.assets[goodFP])
        XCTAssertNil(state.assets[badFP])
        XCTAssertEqual(output.state.observedClock, 2)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
    }

    func testOutOfMonthResourceInTopSnapshotFallsBackToOlderCandidate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let olderFP = Self.fingerprint(0x20)
        let newerFP = Self.fingerprint(0x21)
        let goodHash = Self.fingerprint(0x22)

        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))

        // Older valid snapshot at lamport 2
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredA
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: olderFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 1,
                totalFileSizeBytes: 1,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/valid.jpg",
                contentHash: goodHash,
                fileSize: 1,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 1,
                crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 2,
            runID: "older-valid",
            respectTaskCancellation: false
        )

        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))

        // Newer snapshot at lamport 5 with an out-of-month resource
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: coveredB
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: newerFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 1,
                totalFileSizeBytes: 1,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 2)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/02/wrong-month.jpg",
                contentHash: Self.fingerprint(0x23),
                fileSize: 1,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 1,
                crypto: nil,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 2)
            )],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: "newer-bad-resource",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let state = try XCTUnwrap(output.state.months[month])

        XCTAssertNotNil(state.assets[olderFP], "older snapshot's asset should survive")
        XCTAssertNil(state.assets[newerFP], "newer rejected snapshot's asset should not appear")
        XCTAssertNotNil(state.resources["2026/01/valid.jpg"], "older snapshot's resource should survive")
        XCTAssertEqual(output.state.observedClock, 2)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month), "month should not be marked corrupted when a valid older snapshot exists")
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
            repoID: "99999999-9999-9999-9999-999999999999",
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
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
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
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
    }

    func testCorruptOnlySnapshotFlagsMonthForRebaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: 5,
            writerID: writerA,
            runID: "corrupt"
        )
        await client.injectFile(path: corruptPath, contents: "not-a-snapshot-body")

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(output.state.months[month], .empty)
        XCTAssertEqual(output.state.observedClock, 0)
    }

    func testForeignOnlySnapshotFlagsMonthForRebaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let foreignFP = Self.fingerprint(0x21)

        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: "99999999-9999-9999-9999-999999999999",
                covered: .empty
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: foreignFP,
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
            runID: "foreign",
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
        XCTAssertNil(output.state.months[month]?.assets[foreignFP])
        XCTAssertEqual(output.state.observedClock, 0)
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
        // Corrupt commit at seq 7 still occupies the filename path — advance
        // observedSeq so the allocator avoids colliding with it.
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
            "corrupt commit must advance observedSeqByWriter from filename for allocator collision avoidance"
        )
        XCTAssertNil(output.state.months[month]?.assets[Self.fingerprint(0x50)])
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 7))
        XCTAssertEqual(output.state.observedClock, 0)
    }

    func testCorruptMaxCommitFilenameDoesNotAdvanceSeq() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let corruptPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: UInt64.max)
        await client.injectFile(path: corruptPath, contents: "corrupt filename-only high-water\n")

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.observedSeqByWriter[writerA],
                     "corrupt filename must not poison observedSeq even at max seq")
        XCTAssertNil(output.state.months[month]?.assets[Self.fingerprint(0x51)])
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: UInt64.max))
        XCTAssertEqual(output.state.observedClock, 0)
    }

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
        XCTAssertEqual(output.observedSeqByWriter[writerA], 99,
                       "filename-derived seq must advance for allocator collision avoidance")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 99))
    }

    func testAddAssetWithResourceOutsideMonthSkipsWholeOpButCoversFile() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x71)

        _ = try await writer.write(
            header: makeHeader(seq: 8, clockMin: 8, clockMax: 8),
            ops: [CommitOp(opSeq: 0, clock: 8, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/02/wrong-month.jpg",
                    logicalName: "wrong-month.jpg",
                    contentHash: Self.fingerprint(0x72),
                    fileSize: 1,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )]
            )))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month]?.assets[fp])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 8))
        XCTAssertEqual(output.observedSeqByWriter[writerA], 8)
        XCTAssertEqual(output.state.observedClock, 8)
    }

    func testAcceptedCommitWithRejectedOpStillContributesCoveredSeq() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0x73)
        let poisonedFP = Self.fingerprint(0x74)

        _ = try await writer.write(
            header: makeHeader(seq: 9, clockMin: 9, clockMax: LamportClock.maxAdoptableValue),
            ops: [
                CommitOp(opSeq: 0, clock: 9, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: goodFP,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    resources: []
                ))),
                CommitOp(opSeq: 1, clock: LamportClock.maxAdoptableValue, body: .addAsset(CommitAddAssetBody(
                    assetFingerprint: poisonedFP,
                    creationDateMs: nil,
                    backedUpAtMs: 1,
                    resources: []
                )))
            ],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[poisonedFP])
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 9))
        XCTAssertEqual(output.state.observedClock, 0)
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

    func testPoisonedSnapshotLamportIsSkipped_observedClockStaysBounded() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xA1)

        // Legit baseline snapshot at lamport=10.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        // Plant a peer-style snapshot filename with poisoned lamport just below
        // UInt64.max. We don't bother writing valid body bytes — even an empty
        // stub at this filename used to elevate observedClock and force a crash.
        let poisonedPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: UInt64.max - 1,
            writerID: writerB,
            runID: "deadbeef"
        )
        await client.injectFile(path: poisonedPath, contents: "{}")

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThanOrEqual(
            output.state.observedClock,
            LamportClock.maxAdvanceableValue,
            "poisoned filename lamport must not advance observedClock past the safe ceiling"
        )
        XCTAssertNotNil(
            output.state.months[month]?.assets[goodFP],
            "legit baseline must still load; only the poisoned snapshot is skipped"
        )
    }

    func testPoisonedCommitOpClockIsSkipped_observedClockStaysBounded() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xA2)
        let poisonedFP = Self.fingerprint(0xA3)

        // A legit commit so we have a real observedClock baseline.
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        // Peer-like commit whose op claims clock=UInt64.max - 1 (the header has
        // matching clockMin/clockMax so the body decodes cleanly). Without the
        // materializer guard, this would set observedClock above the safe
        // ceiling and crash the next writer.
        let poisonedClock = UInt64.max - 1
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerB, seq: 1, runID: runID,
                month: month, clockMin: poisonedClock, clockMax: poisonedClock
            ),
            ops: [CommitOp(opSeq: 0, clock: poisonedClock, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThanOrEqual(
            output.state.observedClock,
            LamportClock.maxAdvanceableValue,
            "poisoned op clock must not advance observedClock past the safe ceiling"
        )
        XCTAssertNotNil(
            output.state.months[month]?.assets[goodFP],
            "legit commit must still apply"
        )
        XCTAssertNil(
            output.state.months[month]?.assets[poisonedFP],
            "poisoned-clock op must be skipped at replay"
        )
    }

    func testExactCeilingSnapshotLamportIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xC1)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        let poisonedPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: LamportClock.maxAdvanceableValue,
            writerID: writerB,
            runID: "deadbeef"
        )
        await client.injectFile(path: poisonedPath, contents: "{}")

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdvanceableValue,
            "exact-ceiling filename lamport must not advance observedClock to the ceiling"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
    }

    func testExactCeilingCommitOpClockIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xC2)
        let poisonedFP = Self.fingerprint(0xC3)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let ceiling = LamportClock.maxAdvanceableValue
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerB, seq: 1, runID: runID,
                month: month, clockMin: ceiling, clockMax: ceiling
            ),
            ops: [CommitOp(opSeq: 0, clock: ceiling, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdvanceableValue,
            "exact-ceiling op clock must not advance observedClock to the ceiling"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[poisonedFP])
    }

    func testSnapshotWithPoisonedRowStampClockIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xD1)
        let poisonedFP = Self.fingerprint(0xD2)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 5)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        var poisonCovered = CoveredRanges()
        poisonCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: UInt64.max - 1)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 12,
            runID: "run-poison",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdvanceableValue,
            "row stamp at/above ceiling must not advance observedClock past the safe ceiling"
        )
        XCTAssertNotNil(
            output.state.months[month]?.assets[goodFP],
            "legit snapshot (with sub-ceiling stamp) must still load as baseline"
        )
        XCTAssertNil(
            output.state.months[month]?.assets[poisonedFP],
            "snapshot with poisoned row stamp must be skipped — its peer asset must not enter baseline"
        )
    }

    func testSnapshotWithRowStampAboveFilenameLamportIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xE1)
        let poisonedFP = Self.fingerprint(0xE2)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 4)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 8,
            runID: "run-good",
            respectTaskCancellation: false
        )

        // Peer snapshot at filename lamport 9 but with a row stamp clock at 5000
        // — well below ceiling, but far above 9. Legitimate writers would tick
        // the filename at-or-after every row's clock.
        var poisonCovered = CoveredRanges()
        poisonCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 5000)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 9,
            runID: "run-future",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertNotNil(
            output.state.months[month]?.assets[goodFP],
            "legit baseline must still load"
        )
        XCTAssertNil(
            output.state.months[month]?.assets[poisonedFP],
            "peer snapshot whose row stamp.clock > filename lamport must be skipped"
        )
    }

    func testCorruptSnapshotAtMaxAdoptableMinusOneDoesNotPoisonObservedClock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xC1)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        // Plant a syntactically valid filename at the boundary lamport with a
        // body the snapshot reader cannot decode — falls through the candidate
        // loop with .integrityMismatch/.decodeFailure.
        let highLamport = LamportClock.maxAdoptableValue - 1
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: highLamport,
            writerID: writerB,
            runID: "deadbeef"
        )
        await client.injectFile(path: corruptPath, contents: "not-a-snapshot-body")

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue - 1,
            "corrupt snapshot at the highest-accepted lamport must not advance observedClock to that value"
        )
        XCTAssertEqual(
            output.state.observedClock,
            10,
            "observedClock should reflect only the accepted (good) baseline lamport"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        // Subsequent tickRange must still allocate — the failure mode this test
        // guards against is `advanceExhausted` after observe(observedClock).
        let lamport = LamportClock(initial: output.state.observedClock)
        let range = try await lamport.tickRange(count: 1)
        XCTAssertEqual(range.high, 11)
        XCTAssertLessThan(range.high, LamportClock.maxAdoptableValue)
    }

    func testRowStampRejectedSnapshotAtMaxAdoptableMinusOneDoesNotPoisonObservedClock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xC2)
        let poisonedFP = Self.fingerprint(0xC3)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 5)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        // Boundary filename lamport, but row stamp at the ceiling so the
        // row-stamp quarantine rejects the candidate even after read succeeds.
        let highLamport = LamportClock.maxAdoptableValue - 1
        var poisonCovered = CoveredRanges()
        poisonCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: LamportClock.maxAdoptableValue)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: highLamport,
            runID: "run-poison",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue - 1,
            "row-stamp-rejected snapshot at the highest-accepted lamport must not advance observedClock to that value"
        )
        XCTAssertEqual(
            output.state.observedClock,
            10,
            "observedClock should reflect only the accepted (good) baseline lamport"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[poisonedFP])
        let lamport = LamportClock(initial: output.state.observedClock)
        let range = try await lamport.tickRange(count: 1)
        XCTAssertEqual(range.high, 11)
        XCTAssertLessThan(range.high, LamportClock.maxAdoptableValue)
    }

    func testForeignRepoSnapshotAtMaxAdoptableMinusOneDoesNotPoisonObservedClock() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xC4)
        let foreignFP = Self.fingerprint(0xC5)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        let highLamport = LamportClock.maxAdoptableValue - 1
        var foreignCovered = CoveredRanges()
        foreignCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: "99999999-9999-9999-9999-999999999999",
                covered: foreignCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: foreignFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: highLamport,
            runID: "run-foreign",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue - 1,
            "foreign-repo snapshot at the highest-accepted lamport must not advance observedClock"
        )
        XCTAssertEqual(output.state.observedClock, 10)
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[foreignFP])
    }

    func testMaxObservableValueSnapshotLamportIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xF1)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        let poisonedPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: LamportClock.maxAdoptableValue,
            writerID: writerB,
            runID: "deadbeef"
        )
        await client.injectFile(path: poisonedPath, contents: "{}")

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue,
            "maxAdoptableValue filename lamport must not advance observedClock to the new ceiling"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
    }

    func testValidSnapshotAtMaxAdoptableValueDoesNotDeadEndTickRange() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xD1)
        let boundaryFP = Self.fingerprint(0xD2)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        var boundaryCovered = CoveredRanges()
        boundaryCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: boundaryCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: LamportClock.maxAdoptableValue,
            runID: "run-boundary",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue,
            "valid snapshot at maxAdoptableValue must NOT advance observedClock — would dead-end the next tickRange"
        )
        XCTAssertEqual(
            output.state.observedClock,
            10,
            "observedClock should reflect only the accepted (good) baseline"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[boundaryFP])
        // The dead-end vector: observe(observedClock) into a persisted clock,
        // then attempt one tick. The fix ensures this succeeds.
        let lamport = LamportClock(initial: 0)
        await lamport.observe(output.state.observedClock)
        let range = try await lamport.tickRange(count: 1)
        XCTAssertEqual(range.high, 11)
    }

    func testValidCommitOpAtMaxAdoptableValueDoesNotDeadEndTickRange() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xD3)
        let boundaryFP = Self.fingerprint(0xD4)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let boundary = LamportClock.maxAdoptableValue
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerB, seq: 1, runID: runID,
                month: month, clockMin: boundary, clockMax: boundary
            ),
            ops: [CommitOp(opSeq: 0, clock: boundary, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue,
            "op clock at maxAdoptableValue must NOT advance observedClock — would dead-end the next tickRange"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[boundaryFP])
        let lamport = LamportClock(initial: 0)
        await lamport.observe(output.state.observedClock)
        let range = try await lamport.tickRange(count: 1)
        XCTAssertEqual(range.high, max(6, output.state.observedClock + 1))
    }

    func testFilenameRejectedSnapshotForcesMonthRebaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let poisonedPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: LamportClock.maxAdoptableValue,
            writerID: writerB,
            runID: "deadbeef"
        )
        await client.injectFile(path: poisonedPath, contents: "{}")

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertTrue(
            output.corruptedSnapshotMonths.contains(month),
            "month with only filename-quarantined snapshot must be flagged so the next flush rebaselines"
        )
        // Also exercise the mixed case: a good baseline at lamport=10 plus a
        // filename-quarantined snapshot — the good baseline is accepted so no
        // rebaseline needed.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        let materializer2 = RepoMaterializer(client: client, basePath: basePath)
        let output2 = try await materializer2.materialize(expectedRepoID: repoID)
        XCTAssertFalse(
            output2.corruptedSnapshotMonths.contains(month),
            "month with usable baseline plus a filename-quarantined snapshot does not need rebaseline"
        )
    }

    func testValidSnapshotAtAdoptCeilingMinusOneAllowsNextTick() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xE1)
        let boundaryFP = Self.fingerprint(0xE2)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: "run-good",
            respectTaskCancellation: false
        )

        let boundaryLamport = LamportClock.maxAdoptableValue - 1
        var boundaryCovered = CoveredRanges()
        boundaryCovered.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: boundaryCovered
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: boundaryLamport,
            runID: "run-boundary",
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertEqual(
            output.state.observedClock,
            boundaryLamport,
            "valid snapshot at the highest adopt-ceiling lamport must advance observedClock so peer ordering is preserved"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[boundaryFP])

        // After the boundary observation, tickRange must throw because the
        // only emittable value would be `maxAdoptableValue` which readers reject.
        let lamport = LamportClock(initial: 0)
        await lamport.observe(output.state.observedClock)
        do {
            _ = try await lamport.tickRange(count: 1)
            XCTFail("expected advanceExhausted because emitting maxAdoptableValue would be rejected by readers")
        } catch LamportClockError.advanceExhausted {
            // Expected: emission ceiling equals reader acceptance ceiling.
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testValidCommitOpAtAdoptCeilingMinusOneAllowsNextTick() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xE3)
        let boundaryFP = Self.fingerprint(0xE4)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let boundaryClock = LamportClock.maxAdoptableValue - 1
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerB, seq: 1, runID: runID,
                month: month, clockMin: boundaryClock, clockMax: boundaryClock
            ),
            ops: [CommitOp(opSeq: 0, clock: boundaryClock, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertEqual(
            output.state.observedClock,
            boundaryClock,
            "valid op clock at the highest adopt-ceiling boundary must advance observedClock"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[boundaryFP])

        let lamport = LamportClock(initial: 0)
        await lamport.observe(output.state.observedClock)
        do {
            _ = try await lamport.tickRange(count: 1)
            XCTFail("expected advanceExhausted because emitting maxAdoptableValue would be rejected by readers")
        } catch LamportClockError.advanceExhausted {
            // Expected: emission ceiling equals reader acceptance ceiling.
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testMaxAdoptableValueCommitOpClockIsSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0xF2)
        let poisonedFP = Self.fingerprint(0xF3)

        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: goodFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let ceiling = LamportClock.maxAdoptableValue
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerB, seq: 1, runID: runID,
                month: month, clockMin: ceiling, clockMax: ceiling
            ),
            ops: [CommitOp(opSeq: 0, clock: ceiling, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: poisonedFP, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)

        XCTAssertLessThan(
            output.state.observedClock,
            LamportClock.maxAdoptableValue,
            "maxAdoptableValue op clock must not advance observedClock to the new ceiling"
        )
        XCTAssertNotNil(output.state.months[month]?.assets[goodFP])
        XCTAssertNil(output.state.months[month]?.assets[poisonedFP])
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
