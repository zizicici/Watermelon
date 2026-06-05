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
        let hash = Self.contentHash(0xBB)
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
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey("2026/01/photo.jpg")]?.contentHash, hash)
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
            reason: .userDeleted,
            observedBasis: TombstoneObservationBasis(
                perWriterMaxSeq: [writerA: 1],
                lamportWatermark: 1
            )
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
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
    }

    func testCommitWithDuplicateOpSeqIsRejectedAsCorrupt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x4A)

        // One integrity-valid commit with two ops for the same asset at the SAME clock and DUPLICATE
        // opSeq — an add and a tombstone. The replay sort key (clock, writerID, seq, opSeq) would then
        // be non-unique, so the final add/tombstone state would resolve by arbitrary sort order. The
        // commit must be rejected fail-closed instead.
        let addOp = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: []
        )))
        let tombstoneOp = CommitOp(opSeq: 0, clock: 1, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: fp, reason: .userDeleted,
            observedBasis: TombstoneObservationBasis(perWriterMaxSeq: [writerA: 1], lamportWatermark: 1)
        )))
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [addOp, tombstoneOp],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a commit with duplicate opSeq must be rejected fail-closed, leaving the month non-clean")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1),
            "the rejected duplicate-opSeq commit must not be covered")
        XCTAssertNil(output.state.months[month]?.assets[fp],
            "the ambiguous add/tombstone asset must not materialize from a rejected commit")
    }

    func testSnapshotLinkWithoutResourceRowIsRejectedSoCoveredCommitReplays() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x40)
        let hash = Self.contentHash(0x41)

        // Good commit seq 1: addAsset with a real resource for `hash`.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Inconsistent snapshot covering seq 1: asset row + link to `hash`, but NO resource row for it.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hash, logicalName: "photo.jpg"
            )],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        // The inconsistent snapshot is rejected, so the covered good commit replays and restores the resource.
        XCTAssertNotNil(monthState.assets[fp], "asset survives via covered-commit replay after baseline rejection")
        XCTAssertNotNil(monthState.resources[RemotePhysicalPathKey("2026/01/photo.jpg")],
            "the linked resource is restored from the replayed commit, not lost to the omitting snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "the omitting snapshot must not be trusted as a clean covered-max baseline")
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month),
            "with no trusted baseline the month fails closed to corrupt-snapshot")
    }

    func testSnapshotAssetRowWithoutLinksIsRejectedSoCoveredCommitReplays() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x50)
        let hash = Self.contentHash(0x51)

        // Good commit seq 1: addAsset with a real resource + role/slot link for `hash`.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Inconsistent snapshot covering seq 1: asset row (resourceCount 1) + resource row, but NO link row.
        // A zero-link asset would materialize as a cleanup-eligible phantom while suppressing the good commit.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/photo.jpg", contentHash: hash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            assetResources: [],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fp], "asset survives via covered-commit replay after baseline rejection")
        XCTAssertNotNil(monthState.assetResources[AssetResourceKey(assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0)],
            "the link is restored from the replayed commit, so the asset is not a phantom")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "the link-omitting snapshot must not be trusted as a clean covered-max baseline")
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testSnapshotLinkWithoutAssetRowIsRejectedSoCoveredCommitReplays() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x52)
        let hash = Self.contentHash(0x53)

        // Good commit seq 1: addAsset with its resource + link.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Inconsistent snapshot covering seq 1: resource row + link, but NO asset row — an orphan link.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/photo.jpg", contentHash: hash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hash, logicalName: "photo.jpg"
            )],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fp], "asset survives via covered-commit replay after the orphan-link baseline is rejected")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "the orphan-link snapshot must not be trusted as a clean covered-max baseline")
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testSnapshotDuplicateResourcePathIsRejectedSoCoveredCommitReplays() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x54)
        let hash1 = Self.contentHash(0x55)
        let hash2 = Self.contentHash(0x56)

        // Good commit seq 1: addAsset with the real resource (hash1) + link to hash1.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash1, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Snapshot covering seq 1: two resource rows at the SAME path (hash1 then hash2 clobbers it) plus a
        // link to the clobbered hash1. Without the duplicate-path guard the link passes against the stale
        // membership set while the surviving resource row carries only hash2 — a false-missing asset.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: "2026/01/photo.jpg", contentHash: hash1, fileSize: 10,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotResourceRow(
                    physicalRemotePath: "2026/01/photo.jpg", contentHash: hash2, fileSize: 10,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                )
            ],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hash1, logicalName: "photo.jpg"
            )],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fp], "asset survives via covered-commit replay after the duplicate-path baseline is rejected")
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey("2026/01/photo.jpg")]?.contentHash, hash1,
            "the replayed commit restores the real resource hash, not the clobbering duplicate")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testCommitOpClockOutsideHeaderRangeIsRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x57)
        let hash = Self.contentHash(0x58)

        // C_add (seq 2): header declares clockMin=clockMax=10 but the op carries clock=30. The lying op clock
        // would sort the add AFTER a lower-clock tombstone, resurrecting a deleted asset. Must be rejected.
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 10, clockMax: 10, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 30, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // C_tomb (seq 3): in-range op (clock=20) tombstoning the asset, observing the add's seq.
        _ = try await writer.write(
            header: makeHeader(seq: 3, clockMin: 20, clockMax: 20, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 20, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp, reason: .userDeleted,
                observedBasis: TombstoneObservationBasis(perWriterMaxSeq: [writerA: 2], lamportWatermark: 10)
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2),
            "the out-of-range-clock commit must be rejected and stay uncovered")
        XCTAssertNil(output.state.months[month]?.assets[fp],
            "with the lying add rejected, the tombstone keeps the asset absent — committed truth is preserved")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a rejected uncovered commit folds the month non-clean")
    }

    // R08 only rejected a link-less asset when resourceCount >= 1; a malformed snapshot can set
    // resourceCount == 0 for a real fingerprint to slip the same cleanup-eligible phantom past the guard.
    func testSnapshotZeroResourceCountAssetRowWithoutLinksIsRejectedSoCoveredCommitReplays() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x5A)
        let hash = Self.contentHash(0x5B)

        // Good commit seq 1: addAsset with a real resource + role/slot link.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        // Malformed snapshot covering seq 1: the asset row lies with resourceCount 0 and carries no
        // resource/link rows, even though fp came from a covered commit with a real link.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-zero-phantom", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assetResources[AssetResourceKey(assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0)],
            "the link is restored from the replayed commit, so the asset is not a cleanup-eligible phantom")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a resourceCount==0 zero-link baseline must not be trusted as a clean covered-max")
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    // A baseline carrying both a live asset row and a deletedKey for one fingerprint publishes the asset
    // as present/healthy while consumers drop the tombstone — replay never produces this coexistence.
    func testSnapshotWithAssetRowAndDeletedKeyForSameFingerprintIsRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x5C)
        let hash = Self.contentHash(0x5D)

        // Good commits: seq 1 adds fp (resource+link), seq 2 tombstones it.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )
        _ = try await commitWriter.write(
            header: makeHeader(seq: 2, clockMin: 2, clockMax: 2, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 2, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp, reason: .userDeleted,
                observedBasis: TombstoneObservationBasis(perWriterMaxSeq: [writerA: 1], lamportWatermark: 1)
            )))],
            month: month, respectTaskCancellation: false
        )

        // Malformed snapshot covering seq 1..2 that carries BOTH the live asset (resource+link) and a
        // deletedKey for the same fp.
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/photo.jpg", contentHash: hash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hash, logicalName: "photo.jpg"
            )],
            deletedKeys: [SnapshotDeletedKeyRow(
                keyType: .asset, keyValue: fp.rawValue.hexString,
                stamp: OpStamp(writerID: writerA, seq: 2, clock: 2)
            )],
            month: month, lamport: 5, runID: "run-conflict", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNil(monthState.assets[fp],
            "the committed tombstone wins after the contradictory baseline is rejected and commits replay")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    // A readable-but-body-rejected sibling is bad metadata, not authority over a trusted baseline.
    func testMakeBaselineRejectedHigherCoverageSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x5E)
        let fpB = Self.fingerprint(0x5F)
        let hashA = Self.contentHash(0x60)

        // S1: valid, covers writerA:[1..1], holds only fpA (writeBaseline gives it a link), lamport 2.
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: coveredA, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 2, runID: "run-good", respectTaskCancellation: false
        )

        // S2: covers writerA:[1..2] (a strict superset of S1), holds fpA + fpB, lamport 5 — but fpB is a
        // link-less asset row, so makeBaseline rejects S2 (readable, not a read-error).
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: coveredB, createdAtMs: nil
            ),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 1, totalFileSizeBytes: 1,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotAssetRow(
                    assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 2)
                )
            ],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/a.jpg", contentHash: hashA, fileSize: 1,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: fpA, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hashA, logicalName: "a.jpg"
            )],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpA], "surviving smaller baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpB], "fpB lived only in the rejected higher-coverage snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
            "a makeBaseline-rejected sibling must not demote an accepted trusted baseline")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
            "the survivor is trusted, so the month is not repair-eligible corrupt")
    }

    // The R08 op-clock-range guard is intra-commit only; a same-writer higher-seq commit can carry a
    // header-consistent clock that still dips below a lower-seq commit, inverting the clock-sorted replay.
    func testCommitOpClockInsideHeaderButNonMonotonicAcrossSeqIsRejected() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x6B)
        let hash = Self.contentHash(0x6C)

        // C_add seq 2: header [30,30], op clock 30 — in range, so the R08 op-clock-range guard passes.
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 30, clockMax: 30, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 30, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/photo.jpg", logicalName: "photo.jpg",
                    contentHash: hash, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )
        // C_tomb seq 3 (higher seq): header [20,20], op clock 20 — also in range, so it passes the R08
        // guard. But its clock dips below the lower-seq commit's, which an honest flusher never does; the
        // clock-sorted replay would apply the tombstone before the add, resurrecting fp.
        _ = try await writer.write(
            header: makeHeader(seq: 3, clockMin: 20, clockMax: 20, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 20, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fp, reason: .userDeleted,
                observedBasis: TombstoneObservationBasis(perWriterMaxSeq: [writerA: 2], lamportWatermark: 30)
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 3),
            "the non-monotonic higher-seq commit must be rejected and stay uncovered")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a rejected uncovered commit folds the month non-clean rather than attesting the resurrection as clean")
    }

    // The flusher allocates a commit's clock range (tickRange) and seq (allocate) on two independent
    // actors, so one writer flushing two months concurrently can honestly land a higher seq on a lower
    // clock range. The per-writer monotonicity guard must scope per (writer, month) — grouping by writer
    // across months would falsely drop the higher-seq/lower-clock commit and erase its committed asset.
    func testCrossMonthSameWriterSeqClockInversionKeepsBothCommitsClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let month2 = LibraryMonthKey(year: 2026, month: 2)
        let fpA = Self.fingerprint(0x71)
        let fpB = Self.fingerprint(0x72)
        let hashA = Self.contentHash(0x73)
        let hashB = Self.contentHash(0x74)

        // Month 2026/01: higher seq 6 but LOWER clock range [10,12].
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerA, seq: 6, runID: runID, month: month,
                clockMin: 10, clockMax: 12),
            ops: [CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/a.jpg", logicalName: "a.jpg",
                    contentHash: hashA, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil)])))],
            month: month, respectTaskCancellation: false
        )
        // Month 2026/02: lower seq 5 but HIGHER clock range [13,15] — the honest cross-month inversion.
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerA, seq: 5, runID: runID, month: month2,
                clockMin: 13, clockMax: 15),
            ops: [CommitOp(opSeq: 0, clock: 13, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/02/b.jpg", logicalName: "b.jpg",
                    contentHash: hashB, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil)])))],
            month: month2, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
            "the higher-seq/lower-clock month is honestly written and must stay clean")
        XCTAssertEqual(output.outcomeByMonth[month2], .clean)
        XCTAssertNotNil(output.state.months[month]?.assets[fpA],
            "fpA must not vanish from a falsely-rejected cross-month commit")
        XCTAssertNotNil(output.state.months[month2]?.assets[fpB])
    }

    // An accepted baseline holds fpA stamped (writerA, seq 1, clock 100). An uncovered same-writer
    // tombstone commit at seq 2 carries a LOWER clock (50) — a forged inversion: clock-LWW would sort
    // the tombstone before the baseline add, skip it as stale, and fold the month clean while silently
    // dropping the committed deletion. The monotonicity guard, seeded from the baseline's row clock,
    // must reject the commit so the month folds non-clean instead of attesting a false-present asset.
    func testBaselineBoundarySameWriterTombstoneClockInversionIsNotClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x91)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 100))],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 100, runID: "run-base", respectTaskCancellation: false
        )

        // Uncovered same-writer tombstone: seq 2 but clock 50 < the baseline row clock 100.
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 50, clockMax: 50),
            ops: [CommitOp(opSeq: 0, clock: 50, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fpA, reason: .userDeleted,
                observedBasis: TombstoneObservationBasis(
                    perWriterMaxSeq: [writerA: 1], lamportWatermark: 100))))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a same-writer commit whose clock regressed below its own baseline row must not fold the month clean")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2),
            "the forged-inversion tombstone commit must stay uncovered")
    }

    // The baseline-boundary monotonicity seed must not reject an HONEST later commit. fpA is stamped
    // (writerA, seq 1, clock 100) in the baseline; a real seq-2 tombstone with a forward clock (101)
    // applies normally and removes the asset, leaving the month clean.
    func testBaselineBoundaryHonestForwardClockTombstoneApplies() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x92)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 100))],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 100, runID: "run-base", respectTaskCancellation: false
        )

        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 101, clockMax: 101),
            ops: [CommitOp(opSeq: 0, clock: 101, body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: fpA, reason: .userDeleted,
                observedBasis: TombstoneObservationBasis(
                    perWriterMaxSeq: [writerA: 1], lamportWatermark: 100))))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
            "an honest forward-clock tombstone must not be falsely rejected by the baseline seed")
        XCTAssertNil(output.state.months[month]?.assets[fpA],
            "the honest tombstone must remove the asset")
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2))
    }

    // An accepted baseline has NON-CONTIGUOUS per-writer coverage W:{[1...1],[3...3]} (an interior gap at
    // seq 2, e.g. a grace-backend LIST that absorbed seq 3 before seq 2). Its surviving rows are stamped
    // (W,1,100) and (W,3,300). An honest uncovered commit fills the gap at seq 2 with clock 200 — below the
    // higher-seq baseline row's clock 300 but above the lower-seq row's 100. The seq-aware guard compares
    // seq 2 only against strictly-lower baseline seqs (seq 1, clock 100), so the gap-filler is kept and the
    // month stays clean. A per-writer-max seed would falsely reject it (200 < 300) and drop the asset.
    func testBaselineGapFillHonestLowerSeqCommitStaysClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp1 = Self.fingerprint(0x93)
        let fp3 = Self.fingerprint(0x94)
        let fpF = Self.fingerprint(0x95)
        let hashF = Self.contentHash(0x96)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 3, high: 3))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fp1, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 100)),
                SnapshotAssetRow(
                    assetFingerprint: fp3, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 3, clock: 300))
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 300, runID: "run-base", respectTaskCancellation: false
        )

        // Honest gap-filler at the uncovered interior seq 2; clock 200 sits between the baseline seqs.
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 200, clockMax: 200),
            ops: [CommitOp(opSeq: 0, clock: 200, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpF, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/f.jpg", logicalName: "f.jpg",
                    contentHash: hashF, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil)])))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
            "an honest gap-filling commit below a higher-seq baseline row must not fold the month corrupt")
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpF], "the gap-filler's asset must remain in materialized truth")
        XCTAssertNotNil(monthState.assets[fp1])
        XCTAssertNotNil(monthState.assets[fp3])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2))
    }

    // The seq-aware seed must still fold ALL strictly-lower baseline seqs, including a higher covered seq
    // reached across a gap. A forged uncovered commit at seq 4 with clock 250 (below the seq-3 row's 300)
    // is a genuine inversion and must stay rejected, folding the month non-clean — the gap fix must not
    // open a hole for inversions above the gap.
    func testBaselineGapStillRejectsHigherSeqClockInversion() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp1 = Self.fingerprint(0x97)
        let fp3 = Self.fingerprint(0x98)
        let fpF = Self.fingerprint(0x99)
        let hashF = Self.contentHash(0x9A)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 3, high: 3))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fp1, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 100)),
                SnapshotAssetRow(
                    assetFingerprint: fp3, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 3, clock: 300))
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 300, runID: "run-base", respectTaskCancellation: false
        )

        // Forged inversion above the gap: seq 4 but clock 250 < the seq-3 baseline row's clock 300.
        _ = try await writer.write(
            header: makeHeader(seq: 4, clockMin: 250, clockMax: 250),
            ops: [CommitOp(opSeq: 0, clock: 250, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fpF, creationDateMs: nil, backedUpAtMs: 1,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/01/g.jpg", logicalName: "g.jpg",
                    contentHash: hashF, fileSize: 10, resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo, slot: 0, crypto: nil)])))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
            "a forged seq-4 commit whose clock regressed below the seq-3 baseline row must stay rejected")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 4))
    }

    // Sibling of testMakeBaselineRejectedHigherCoverageSiblingDoesNotDemoteTrustedBaseline, but the higher-coverage
    // covered-max is rejected by the pre-makeBaseline poisoned-row-stamp guard instead of a body defect.
    func testPoisonedRowStampHigherCoverageSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x7A)
        let fpB = Self.fingerprint(0x7B)
        let hashA = Self.contentHash(0x7C)
        let hashB = Self.contentHash(0x7D)

        // S1: valid, covers writerA:[1..1], holds only fpA (writeBaseline gives it a link), lamport 2.
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: coveredA, createdAtMs: nil),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1))],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 2, runID: "run-good", respectTaskCancellation: false
        )

        // S2: covers writerA:[1..2] (a strict superset of S1), lamport 5, holds fpA + fpB both linked —
        // but fpB's asset-row stamp clock 6 exceeds the filename lamport 5, so snapshotHasUnworkableRowStamp
        // skips S2 BEFORE makeBaseline.
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: coveredB, createdAtMs: nil),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 1, totalFileSizeBytes: 1,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)),
                SnapshotAssetRow(
                    assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 1, totalFileSizeBytes: 1,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 6))
            ],
            resources: [
                SnapshotResourceRow(
                    physicalRemotePath: "2026/01/a.jpg", contentHash: hashA, fileSize: 1,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 1, crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)),
                SnapshotResourceRow(
                    physicalRemotePath: "2026/01/b.jpg", contentHash: hashB, fileSize: 1,
                    resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 2, crypto: nil,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 2))
            ],
            assetResources: [
                SnapshotAssetResourceRow(
                    assetFingerprint: fpA, role: ResourceTypeCode.photo, slot: 0,
                    resourceHash: hashA, logicalName: "a.jpg"),
                SnapshotAssetResourceRow(
                    assetFingerprint: fpB, role: ResourceTypeCode.photo, slot: 0,
                    resourceHash: hashB, logicalName: "b.jpg")
            ],
            deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpA], "surviving smaller baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpB], "fpB lived only in the poison-skipped higher-coverage snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
            "a poisoned-row-stamp sibling must not demote an accepted trusted baseline")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
            "the survivor is trusted, so the month is not repair-eligible corrupt")
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
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp))
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

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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

    func testSnapshotWithEmptyRepoIDRejectedWhenExpectedRepoIDProvided() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let legacyFP = Self.fingerprint(0x03)

        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: "",
                covered: .empty, createdAtMs: nil
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
            runID: "empty-repo-id",
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
        let goodHash = Self.contentHash(0x05)
        let badHash = Self.contentHash(0x06)
        let deletedFP = Self.fingerprint(0x07)
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))

        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
                SnapshotDeletedKeyRow(keyType: .asset, keyValue: deletedFP.rawValue.hexString, stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)),
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
        XCTAssertTrue(state.deletedAssetStamps.isEmpty)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
    }

    func testSnapshotMalformedAssetDeletedKeyCandidateFallsBack() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let goodFP = Self.fingerprint(0x08)
        let badFP = Self.fingerprint(0x09)

        var goodCovered = CoveredRanges()
        goodCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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

        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: .empty, createdAtMs: nil
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
        let goodHash = Self.contentHash(0x22)

        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))

        // Older valid snapshot at lamport 2
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredA, createdAtMs: nil
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
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: olderFP, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: goodHash, logicalName: "valid.jpg"
            )],
            deletedKeys: [],
            month: month,
            lamport: 2,
            runID: "older-valid",
            respectTaskCancellation: false
        )

        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))

        // Newer snapshot at lamport 5 with an out-of-month resource
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: coveredB, createdAtMs: nil
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
                contentHash: Self.contentHash(0x23),
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
        XCTAssertNotNil(state.resources[RemotePhysicalPathKey("2026/01/valid.jpg")], "older snapshot's resource should survive")
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
            covered: ourCovered, createdAtMs: nil
        )
        _ = try await snapshotWriter.writeBaseline(
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
            covered: foreignCovered, createdAtMs: nil
        )
        _ = try await snapshotWriter.writeBaseline(
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
            covered: covered, createdAtMs: nil
        )
        _ = try await snapshotWriter.writeBaseline(
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
        _ = try await snapshotWriter.writeBaseline(
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
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "corrupt higher-lamport sibling must not demote an accepted trusted baseline")
    }

    func testCorruptSameWriterHigherLamportSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x21)
        let fpB = Self.fingerprint(0x22)

        // S1: valid, covers writerA:[1..1], holds only fpA, lamport 2.
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredA, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 2, runID: "run-good", respectTaskCancellation: false
        )

        // S2: covers writerA:[1..2], holds fpA AND fpB, lamport 5, then corrupts.
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredB, createdAtMs: nil
            ),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotAssetRow(
                    assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 2)
                )
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerA, runID: "run-bad"
        )
        await client.truncateInHalf(path: badPath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpA], "surviving baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpB], "fpB lived only in the corrupt higher snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "same-writer corrupt sibling above the survivor must not demote clean authority")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "accepted trusted baseline means this is not a corrupt-only repair case")
    }

    func testCorruptSameWriterSameLamportSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x23)
        let fpB = Self.fingerprint(0x24)

        // S1: valid, covers writerA:[1..1], holds only fpA, lamport 5, lower run tuple.
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredA, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 5, runID: "run-aaa", respectTaskCancellation: false
        )

        // S2: SAME lamport 5, higher run tuple, covers writerA:[1..2], holds fpA AND fpB, then corrupts.
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredB, createdAtMs: nil
            ),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotAssetRow(
                    assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 2)
                )
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 5, runID: "run-zzz", respectTaskCancellation: false
        )
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerA, runID: "run-zzz"
        )
        await client.truncateInHalf(path: badPath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpA], "surviving baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpB], "fpB lived only in the corrupt same-lamport snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "same-writer corrupt sibling at the survivor lamport must not demote clean authority")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "accepted trusted baseline means this is not a corrupt-only repair case")
    }

    func testCorruptSameWriterLowerLamportSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA = Self.fingerprint(0x25)
        let fpB = Self.fingerprint(0x26)

        var coveredGood = CoveredRanges()
        coveredGood.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredGood, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
            )],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 10, runID: "run-good", respectTaskCancellation: false
        )

        var coveredBad = CoveredRanges()
        coveredBad.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredBad, createdAtMs: nil
            ),
            assets: [
                SnapshotAssetRow(
                    assetFingerprint: fpA, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
                ),
                SnapshotAssetRow(
                    assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 0, totalFileSizeBytes: 0,
                    stamp: OpStamp(writerID: writerA, seq: 2, clock: 2)
                )
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 5, runID: "run-bad", respectTaskCancellation: false
        )
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerA, runID: "run-bad"
        )
        await client.truncateInHalf(path: badPath)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpA], "surviving baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpB], "fpB lived only in the corrupt lower-lamport snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "same-writer corrupt sibling below the survivor must not demote clean authority")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
    }

    func testCorruptCrossWriterLowerLamportSiblingDoesNotDemoteTrustedBaseline() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let fpA1 = Self.fingerprint(0x31)
        let fpA2 = Self.fingerprint(0x32)
        let fpB = Self.fingerprint(0x33)

        // Survivor S_B: writerB, covers writerB:[1..1], holds only fpB, lamport 12 (writerB's high global
        // clock from heavy work in OTHER months — independent of how little of THIS month it covers).
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: coveredB, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: 1)
            )],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 12, runID: "run-b", respectTaskCancellation: false
        )

        // S_A: writerA, covers writerA:[1..2] AND writerB:[1..1] (a strict superset of S_B),
        // holds fpA1, fpA2, fpB, lamport 7 (writerA's LOWER global clock). It was the accepted covered-max
        // before its bytes corrupt.
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 2))
        coveredA.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredA, createdAtMs: nil
            ),
            assets: [
                SnapshotAssetRow(assetFingerprint: fpA1, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0, stamp: OpStamp(writerID: writerA, seq: 1, clock: 3)),
                SnapshotAssetRow(assetFingerprint: fpA2, creationDateMs: nil, backedUpAtMs: 2,
                    resourceCount: 0, totalFileSizeBytes: 0, stamp: OpStamp(writerID: writerA, seq: 2, clock: 4)),
                SnapshotAssetRow(assetFingerprint: fpB, creationDateMs: nil, backedUpAtMs: 1,
                    resourceCount: 0, totalFileSizeBytes: 0, stamp: OpStamp(writerID: writerB, seq: 1, clock: 1))
            ],
            resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 7, runID: "run-a", respectTaskCancellation: false
        )
        let badPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 7, writerID: writerA, runID: "run-a"
        )
        await client.truncateInHalf(path: badPath)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNotNil(monthState.assets[fpB], "surviving writerB baseline still serves its own asset")
        XCTAssertNil(monthState.assets[fpA1], "fpA1 lived only in the corrupt cross-writer snapshot")
        XCTAssertNil(monthState.assets[fpA2], "fpA2 lived only in the corrupt cross-writer snapshot")
        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "cross-writer corrupt sibling below the survivor must not demote clean authority")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "accepted trusted baseline means this is not a corrupt-only repair case")
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

        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: "99999999-9999-9999-9999-999999999999",
                covered: .empty, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
                    contentHash: Self.contentHash(0x72),
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
        // The op is accepted/covered but its asset is silently dropped (resource out of month). The
        // month must fail closed to `.corrupt`, never attest clean over the dropped asset, so commit GC
        // cannot delete the only record that the asset existed.
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "an out-of-month addAsset op must demote the month from clean")
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
            covered: covered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
        let lamport = InMemoryLamportClock(initial: output.state.observedClock)
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: poisonCovered, createdAtMs: nil
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
        let lamport = InMemoryLamportClock(initial: output.state.observedClock)
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: "99999999-9999-9999-9999-999999999999",
                covered: foreignCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: boundaryCovered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: LamportClock.maxAdoptableValue)
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
        let lamport = InMemoryLamportClock(initial: 0)
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
        let lamport = InMemoryLamportClock(initial: 0)
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: goodCovered, createdAtMs: nil
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
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB,
                repoID: repoID,
                covered: boundaryCovered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: boundaryFP, creationDateMs: nil, backedUpAtMs: 1,
                resourceCount: 0, totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerB, seq: 1, clock: boundaryLamport)
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
        let lamport = InMemoryLamportClock(initial: 0)
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

        let lamport = InMemoryLamportClock(initial: 0)
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

    // MARK: - Byte-exact (NFC vs NFD) physical-path keying
    func testSnapshotBaselinePreservesNFCAndNFDPaths() async throws {
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        XCTAssertNotEqual(Array(nfcLeaf.utf8), Array(nfdLeaf.utf8))
        XCTAssertEqual(nfcLeaf, nfdLeaf)

        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        let nfcPath = "2026/01/\(nfcLeaf)"
        let nfdPath = "2026/01/\(nfdLeaf)"
        let baseStamp = OpStamp(writerID: writerA, seq: 1, clock: 5)
        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered, createdAtMs: nil
        )
        func row(_ path: String, _ hashByte: UInt8) -> SnapshotResourceRow {
            SnapshotResourceRow(
                physicalRemotePath: path,
                contentHash: Self.contentHash(hashByte),
                fileSize: 10,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 1,
                crypto: nil,
                stamp: baseStamp
            )
        }
        _ = try await snapshotWriter.writeBaseline(
            header: header,
            assets: [],
            resources: [row(nfcPath, 0xA1), row(nfdPath, 0xB2)],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 5,
            runID: runID,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertEqual(monthState.resources.count, 2, "snapshot baseline must keep both NFC and NFD rows")
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(nfcPath)]?.contentHash, Self.contentHash(0xA1))
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(nfdPath)]?.contentHash, Self.contentHash(0xB2))
    }


    // "café.jpg": U+00E9 (NFC) vs e + U+0301 (NFD). Byte-distinct, Swift-String-equal.
    // An exact-name backend stores both as distinct objects; a [String: ...]-keyed state
    // would collapse them to one row.
    func testCommitReplayPreservesNFCAndNFDPaths() async throws {
        let nfcLeaf = "caf\u{00E9}.jpg"
        let nfdLeaf = "cafe\u{0301}.jpg"
        // Guard the premise: byte-distinct yet String-equal.
        XCTAssertNotEqual(Array(nfcLeaf.utf8), Array(nfdLeaf.utf8))
        XCTAssertEqual(nfcLeaf, nfdLeaf)

        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let nfcPath = "2026/01/\(nfcLeaf)"
        let nfdPath = "2026/01/\(nfdLeaf)"
        let fpNFC = Self.fingerprint(0xA1)
        let fpNFD = Self.fingerprint(0xB2)
        let hashNFC = Self.contentHash(0xA1)
        let hashNFD = Self.contentHash(0xB2)

        let commit = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fpNFC,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: nfcPath,
                    logicalName: nfcLeaf,
                    contentHash: hashNFC,
                    fileSize: 10,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )))
        let commit2 = CommitOp(opSeq: 0, clock: 2, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fpNFD,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: nfdPath,
                    logicalName: nfdLeaf,
                    contentHash: hashNFD,
                    fileSize: 10,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )))
        _ = try await writer.write(header: makeHeader(seq: 1, clockMin: 1, clockMax: 1), ops: [commit], month: month, respectTaskCancellation: false)
        _ = try await writer.write(header: makeHeader(seq: 2, clockMin: 2, clockMax: 2), ops: [commit2], month: month, respectTaskCancellation: false)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertEqual(monthState.resources.count, 2, "commit replay must keep both NFC and NFD rows")
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(nfcPath)]?.contentHash, hashNFC)
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(nfdPath)]?.contentHash, hashNFD)
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

    private static func fingerprint(_ byte: UInt8) -> AssetFingerprint {
        TestFixtures.assetFingerprint(byte)
    }

    // MARK: - Corrupt-commit month outcome

    func testCorruptOnlyCommitMarksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0xD0)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.corrupt(path: commitPath, with: Data("not-jsonl".utf8))

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month], "corrupt commit must not produce state")
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "month with only corrupt commits must be marked non-clean")
    }

    func testCorruptCommitWithValidSnapshotIsClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0xD1)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.corrupt(path: commitPath, with: Data("not-jsonl".utf8))

        // Write a valid snapshot that covers writerA seq 1 so the snapshot itself passes
        // stamp validation. The corrupt commit at seq 1 is covered by the snapshot so
        // it won't be re-read; the outcome should be clean.
        let snapshotFP = Self.fingerprint(0xD2)
        var snapshotCovered = CoveredRanges()
        snapshotCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: snapshotCovered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: snapshotFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 5)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertEqual(output.outcomeByMonth[month], .clean,
                       "month with valid snapshot covering the corrupt commit stays clean")
        XCTAssertNotNil(output.state.months[month]?.assets[snapshotFP])
    }

    func testHeaderMismatchOnlyCommitMarksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0xD3)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let originalPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let renamedPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        try await client.move(from: originalPath, to: renamedPath)

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNil(output.state.months[month])
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "month with only header-mismatched commits must be marked non-clean")
    }

    // MARK: - Mixed accepted/rejected commit month outcome

    func testMixedAcceptedAndCorruptCommits_marksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Write a valid commit at seq 1 (accepted).
        let fp1 = Self.fingerprint(0xE0)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp1, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        // Write a valid commit at seq 2, then corrupt it (rejected).
        let fp2 = Self.fingerprint(0xE1)
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 2, clockMax: 2),
            ops: [CommitOp(opSeq: 0, clock: 2, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp2, creationDateMs: nil, backedUpAtMs: 2, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let corruptPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        await client.corrupt(path: corruptPath, with: Data("not-jsonl".utf8))

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        // Seq 1 accepted, seq 2 rejected. State has fp1 but not fp2.
        XCTAssertNotNil(output.state.months[month]?.assets[fp1],
                        "accepted commit seq 1 must produce state")
        XCTAssertNil(output.state.months[month]?.assets[fp2],
                     "corrupt commit seq 2 must not produce state")
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertFalse((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "month with any rejected uncovered commit must be non-clean")
    }

    func testSnapshotPlusCorruptUncoveredCommit_marksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Write a corrupt commit at seq 2 (not covered by snapshot).
        let fp2 = Self.fingerprint(0xE3)
        _ = try await writer.write(
            header: makeHeader(seq: 2, clockMin: 2, clockMax: 2),
            ops: [CommitOp(opSeq: 0, clock: 2, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp2, creationDateMs: nil, backedUpAtMs: 2, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let corruptPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        await client.corrupt(path: corruptPath, with: Data("not-jsonl".utf8))

        // Write a trusted snapshot covering seq 1 only (NOT seq 2), plus a bad sibling.
        let snapshotFP = Self.fingerprint(0xE2)
        var snapshotCovered = CoveredRanges()
        snapshotCovered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: snapshotCovered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: snapshotFP,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 5)
            )],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )
        let badSnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 20, writerID: writerB, runID: "bad-sibling"
        )
        await client.injectFile(path: badSnapshotPath, data: Data("not-jsonl\n".utf8))

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[snapshotFP],
                        "snapshot baseline state must be present")
        XCTAssertNil(output.state.months[month]?.assets[fp2],
                     "corrupt uncovered commit must not produce state")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "bad snapshot siblings must not wash out a corrupt uncovered commit")
    }

    func testSnapshotPlusCorruptSiblingAndDanglingReplayLink_marksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let baselineFP = Self.fingerprint(0xE4)
        let replayFP = Self.fingerprint(0xE5)
        let baselineHash = Self.contentHash(0xE6)
        let replayHash = Self.contentHash(0xE7)
        let sharedPath = "2026/01/shared-name.jpg"

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: baselineFP,
                creationDateMs: nil,
                backedUpAtMs: 10,
                resourceCount: 1,
                totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: sharedPath,
                contentHash: baselineHash,
                fileSize: 10,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 10,
                crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: baselineFP,
                role: ResourceTypeCode.photo,
                slot: 0,
                resourceHash: baselineHash,
                logicalName: "shared-name.jpg"
            )],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )
        let badSnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 20, writerID: writerB, runID: "bad-sibling"
        )
        await client.injectFile(path: badSnapshotPath, data: Data("not-jsonl\n".utf8))

        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5, writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: replayFP,
                creationDateMs: nil,
                backedUpAtMs: 5,
                resources: [CommitResourceEntry(
                    physicalRemotePath: sharedPath,
                    logicalName: "shared-name.jpg",
                    contentHash: replayHash,
                    fileSize: 10,
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

        XCTAssertNotNil(output.state.months[month]?.assets[baselineFP])
        XCTAssertNotNil(output.state.months[month]?.assets[replayFP],
                        "accepted replay commit should materialize before the dangling-link guard marks non-clean")
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerB, seq: 1))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "bad snapshot siblings must not wash out a dangling replay link")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "the corrupt outcome comes from replay inconsistency, not corrupt-only snapshot repair")
    }

    func testSnapshotPlusCorruptSiblingAndOutOfMonthAddAsset_marksMonthNonClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let baselineFP = Self.fingerprint(0xE8)
        let baselineHash = Self.contentHash(0xE9)
        let outOfMonthFP = Self.fingerprint(0xEA)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: baselineFP,
                creationDateMs: nil,
                backedUpAtMs: 10,
                resourceCount: 1,
                totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/good-baseline.jpg",
                contentHash: baselineHash,
                fileSize: 10,
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 10,
                crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: baselineFP,
                role: ResourceTypeCode.photo,
                slot: 0,
                resourceHash: baselineHash,
                logicalName: "good-baseline.jpg"
            )],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )
        let badSnapshotPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 20, writerID: writerB, runID: "bad-sibling"
        )
        await client.injectFile(path: badSnapshotPath, data: Data("not-jsonl\n".utf8))

        _ = try await commitWriter.write(
            header: makeHeader(seq: 2, clockMin: 20, clockMax: 20, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 20, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: outOfMonthFP,
                creationDateMs: nil,
                backedUpAtMs: 20,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/02/wrong-month.jpg",
                    logicalName: "wrong-month.jpg",
                    contentHash: Self.contentHash(0xEB),
                    fileSize: 10,
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

        XCTAssertNotNil(output.state.months[month]?.assets[baselineFP])
        XCTAssertNil(output.state.months[month]?.assets[outOfMonthFP],
                     "out-of-month addAsset op must not produce state")
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 2))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt,
                       "bad snapshot siblings must not wash out an out-of-month replay op")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "the corrupt outcome comes from accepted replay content, not corrupt-only snapshot repair")
    }

    // MARK: - MonthTrust representation (S1a)

    private func reasonKinds(_ trust: RepoMaterializer.MonthTrust?) -> Set<RepoMaterializer.MonthTrustReason.Kind> {
        Set((trust?.reasons ?? []).map(\.kind))
    }

    private func reasonCategories(_ trust: RepoMaterializer.MonthTrust?) -> Set<RepoMaterializer.MonthTrustReason.Category> {
        Set((trust?.reasons ?? []).map(\.category))
    }

    /// Non-clean months as RepoCommittedView.loadFromMaterialize derives them.
    private func committedViewNonCleanMonths(_ output: RepoMaterializer.MaterializeOutput) -> Set<LibraryMonthKey> {
        Set(output.outcomeByMonth.filter { $0.value != .clean }.keys)
    }

    /// Corrupt-snapshot repair candidates as RepoCompactionService.repairCorruptSnapshotBaselines selects.
    private func compactionRepairCandidates(_ output: RepoMaterializer.MaterializeOutput) -> Set<LibraryMonthKey> {
        Set(output.corruptedSnapshotMonths.filter { output.outcomeByMonth[$0] == .corrupt })
    }

    func testCleanMonthTrustHasCleanOutcomeAndEmptyReasons() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x4A)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month], "every materialized month must have a trust entry")
        XCTAssertEqual(trust.outcome, .clean)
        XCTAssertTrue(trust.reasons.isEmpty, "clean months must carry an empty reason list")
        XCTAssertEqual(output.outcomeByMonth[month], .clean)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(committedViewNonCleanMonths(output), [])
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testAmbiguousSnapshotCoverageMapsToAmbiguousReason() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)

        // Two trusted snapshots with incomparable coverage → no covered-max winner → ambiguous.
        for (writer, lamport, high, run) in [(writerA, UInt64(20), UInt64(2), "snap-a"), (writerB, UInt64(25), UInt64(4), "snap-b")] {
            var covered = CoveredRanges()
            covered.add(writerID: writer, range: ClosedSeqRange(low: 1, high: high))
            _ = try await snapshotWriter.writeBaseline(
                header: SnapshotHeader(
                    version: SnapshotHeader.currentVersion,
                    scope: CommitHeader.monthScope(month),
                    writerID: writer,
                    repoID: repoID,
                    covered: covered, createdAtMs: nil
                ),
                assets: [], resources: [], assetResources: [], deletedKeys: [],
                month: month, lamport: lamport, runID: run, respectTaskCancellation: false
            )
        }

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .ambiguous)
        XCTAssertEqual(reasonKinds(trust), [.ambiguousSnapshotCoverage])
        XCTAssertEqual(reasonCategories(trust), [.ambiguous])
        XCTAssertEqual(output.outcomeByMonth[month], .ambiguous)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair)
        XCTAssertEqual(committedViewNonCleanMonths(output), [month])
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testCorruptOnlySnapshotMapsToCorruptReasonAndStaysRepairEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: 5, writerID: writerA, runID: "corrupt"
        )
        await client.injectFile(path: corruptPath, contents: "not-a-snapshot-body")

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertEqual(reasonKinds(trust), [.corruptedSnapshot])
        XCTAssertTrue(trust.allowsCorruptSnapshotRepair)
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(committedViewNonCleanMonths(output), [month])
        XCTAssertEqual(compactionRepairCandidates(output), [month])
    }

    func testFilenameRejectedSnapshotMapsToCorruptedSnapshotRepairPath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let poisonedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: LamportClock.maxAdoptableValue, writerID: writerB, runID: "deadbeef"
        )
        await client.injectFile(path: poisonedPath, contents: "{}")

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertEqual(reasonKinds(trust), [.filenameRejectedSnapshot])
        XCTAssertTrue(trust.allowsCorruptSnapshotRepair)
        XCTAssertTrue(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(compactionRepairCandidates(output), [month])
    }

    func testRejectedCommitMapsToCorruptReasonButNotRepairCandidate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = Self.fingerprint(0x4B)
        _ = try await writer.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month,
            respectTaskCancellation: false
        )
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.corrupt(path: commitPath, with: Data("not-jsonl".utf8))

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertEqual(reasonKinds(trust), [.rejectedUncoveredCommit])
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair, "a pure rejected-commit month has no repairable snapshot cause")
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(committedViewNonCleanMonths(output), [month])
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testDanglingReplayLinkMapsToCorruptPlusRepairConstraint() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let baselineFP = Self.fingerprint(0x4C)
        let replayFP = Self.fingerprint(0x4D)
        let baselineHash = Self.contentHash(0x4E)
        let replayHash = Self.contentHash(0x4F)
        let sharedPath = "2026/01/shared-dangle.jpg"

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: baselineFP, creationDateMs: nil, backedUpAtMs: 10,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: sharedPath, contentHash: baselineHash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 10, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: baselineFP, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: baselineHash, logicalName: "shared-dangle.jpg"
            )],
            deletedKeys: [], month: month, lamport: 10, runID: runID, respectTaskCancellation: false
        )
        await client.injectFile(
            path: RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 20, writerID: writerB, runID: "bad-sibling"),
            data: Data("not-jsonl\n".utf8)
        )
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5, writerID: writerB),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: replayFP, creationDateMs: nil, backedUpAtMs: 5,
                resources: [CommitResourceEntry(
                    physicalRemotePath: sharedPath, logicalName: "shared-dangle.jpg",
                    contentHash: replayHash, fileSize: 10,
                    resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertTrue(reasonKinds(trust).contains(.danglingReplayLink))
        XCTAssertTrue(reasonCategories(trust).contains(.corrupt))
        XCTAssertTrue(reasonCategories(trust).contains(.repairConstraint))
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair, "a replay-defect constraint keeps the month out of repair eligibility")
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testOutOfMonthReplayOpMapsToCorruptPlusRepairConstraint() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let baselineFP = Self.fingerprint(0x5A)
        let baselineHash = Self.contentHash(0x5B)
        let outOfMonthFP = Self.fingerprint(0x5C)

        var covered = CoveredRanges()
        covered.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: covered, createdAtMs: nil
            ),
            assets: [SnapshotAssetRow(
                assetFingerprint: baselineFP, creationDateMs: nil, backedUpAtMs: 10,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            resources: [SnapshotResourceRow(
                physicalRemotePath: "2026/01/good-oom.jpg", contentHash: baselineHash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 10, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            assetResources: [SnapshotAssetResourceRow(
                assetFingerprint: baselineFP, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: baselineHash, logicalName: "good-oom.jpg"
            )],
            deletedKeys: [], month: month, lamport: 10, runID: runID, respectTaskCancellation: false
        )
        await client.injectFile(
            path: RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 20, writerID: writerB, runID: "bad-sibling"),
            data: Data("not-jsonl\n".utf8)
        )
        _ = try await commitWriter.write(
            header: makeHeader(seq: 2, clockMin: 20, clockMax: 20, writerID: writerA),
            ops: [CommitOp(opSeq: 0, clock: 20, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: outOfMonthFP, creationDateMs: nil, backedUpAtMs: 20,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/02/wrong-month.jpg", logicalName: "wrong-month.jpg",
                    contentHash: Self.contentHash(0x5D), fileSize: 10,
                    resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertTrue(reasonKinds(trust).contains(.outOfMonthReplayOp))
        XCTAssertTrue(reasonCategories(trust).contains(.corrupt))
        XCTAssertTrue(reasonCategories(trust).contains(.repairConstraint))
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    // MARK: - MonthTrust mixed ambiguous + corrupt states (R02)

    /// Two trusted snapshots with incomparable coverage; the higher-lamport one (`writerA`) wins best-effort
    /// so it carries the replay baseline, while the month still folds ambiguous.
    private func writeAmbiguousPair(snapshotWriter: SnapshotWriter, aAssets: [SnapshotAssetRow] = [], aResources: [SnapshotResourceRow] = [], aLinks: [SnapshotAssetResourceRow] = []) async throws {
        var coveredA = CoveredRanges()
        coveredA.add(writerID: writerA, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA, repoID: repoID, covered: coveredA, createdAtMs: nil
            ),
            assets: aAssets, resources: aResources, assetResources: aLinks, deletedKeys: [],
            month: month, lamport: 20, runID: "snap-a", respectTaskCancellation: false
        )
        var coveredB = CoveredRanges()
        coveredB.add(writerID: writerB, range: ClosedSeqRange(low: 1, high: 1))
        _ = try await snapshotWriter.writeBaseline(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerB, repoID: repoID, covered: coveredB, createdAtMs: nil
            ),
            assets: [], resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: 10, runID: "snap-b", respectTaskCancellation: false
        )
    }

    func testAmbiguousPlusRejectedCommitKeepsBothReasonsAndFoldsCorrupt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let writerC = "33333333-3333-3333-3333-cccccccccccc"

        try await writeAmbiguousPair(snapshotWriter: snapshotWriter)

        // A valid commit uncovered by either baseline, then corrupted → rejected uncovered commit.
        let fp = Self.fingerprint(0x6A)
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 1, clockMax: 1, writerID: writerC),
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 1, resources: [])))],
            month: month, respectTaskCancellation: false
        )
        await client.corrupt(
            path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerC, seq: 1),
            with: Data("not-jsonl".utf8)
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt, "corrupt beats ambiguous")
        XCTAssertTrue(reasonKinds(trust).contains(.ambiguousSnapshotCoverage),
                      "ambiguous cause stays diagnostically visible in a mixed month")
        XCTAssertTrue(reasonKinds(trust).contains(.rejectedUncoveredCommit))
        XCTAssertTrue(reasonCategories(trust).isSuperset(of: [.ambiguous, .corrupt]))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month),
                       "a rejected commit is not a repairable snapshot cause")
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair)
        XCTAssertEqual(committedViewNonCleanMonths(output), [month])
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testAmbiguousPlusOutOfMonthReplayOpKeepsBothReasonsAndFoldsCorrupt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let writerC = "33333333-3333-3333-3333-cccccccccccc"

        try await writeAmbiguousPair(snapshotWriter: snapshotWriter)

        // An accepted commit whose addAsset resource lies outside the month → out-of-month replay op.
        let outOfMonthFP = Self.fingerprint(0x6B)
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5, writerID: writerC),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: outOfMonthFP, creationDateMs: nil, backedUpAtMs: 5,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2026/02/wrong-month.jpg", logicalName: "wrong-month.jpg",
                    contentHash: Self.contentHash(0x6C), fileSize: 10,
                    resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertTrue(reasonKinds(trust).isSuperset(of: [.ambiguousSnapshotCoverage, .outOfMonthReplayOp]))
        XCTAssertTrue(reasonCategories(trust).isSuperset(of: [.ambiguous, .corrupt, .repairConstraint]))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair,
                       "an out-of-month replay defect is not snapshot-repairable, even alongside ambiguity")
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testAmbiguousPlusDanglingReplayLinkKeepsBothReasonsAndFoldsCorrupt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let writerC = "33333333-3333-3333-3333-cccccccccccc"
        let baselineFP = Self.fingerprint(0x6D)
        let replayFP = Self.fingerprint(0x6E)
        let baselineHash = Self.contentHash(0x6F)
        let replayHash = Self.contentHash(0x70)
        let sharedPath = "2026/01/shared-mixed.jpg"

        // writerA baseline carries the shared-path resource and wins best-effort (equal seqs, higher lamport).
        try await writeAmbiguousPair(
            snapshotWriter: snapshotWriter,
            aAssets: [SnapshotAssetRow(
                assetFingerprint: baselineFP, creationDateMs: nil, backedUpAtMs: 10,
                resourceCount: 1, totalFileSizeBytes: 10,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            aResources: [SnapshotResourceRow(
                physicalRemotePath: sharedPath, contentHash: baselineHash, fileSize: 10,
                resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 10, crypto: nil,
                stamp: OpStamp(writerID: writerA, seq: 1, clock: 10)
            )],
            aLinks: [SnapshotAssetResourceRow(
                assetFingerprint: baselineFP, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: baselineHash, logicalName: "shared-mixed.jpg"
            )]
        )

        // Accepted commit re-adds the shared path with a different hash at a LOWER clock, so the baseline
        // resource row survives (LWW) while the new asset-resource link points to a now-missing hash.
        _ = try await commitWriter.write(
            header: makeHeader(seq: 1, clockMin: 5, clockMax: 5, writerID: writerC),
            ops: [CommitOp(opSeq: 0, clock: 5, body: .addAsset(CommitAddAssetBody(
                assetFingerprint: replayFP, creationDateMs: nil, backedUpAtMs: 5,
                resources: [CommitResourceEntry(
                    physicalRemotePath: sharedPath, logicalName: "shared-mixed.jpg",
                    contentHash: replayHash, fileSize: 10,
                    resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
                )]
            )))],
            month: month, respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)

        let trust = try XCTUnwrap(output.trustByMonth[month])
        XCTAssertEqual(trust.outcome, .corrupt)
        XCTAssertTrue(reasonKinds(trust).isSuperset(of: [.ambiguousSnapshotCoverage, .danglingReplayLink]))
        XCTAssertTrue(reasonCategories(trust).isSuperset(of: [.ambiguous, .corrupt, .repairConstraint]))
        XCTAssertEqual(output.outcomeByMonth[month], .corrupt)
        XCTAssertFalse(output.corruptedSnapshotMonths.contains(month))
        XCTAssertFalse(trust.allowsCorruptSnapshotRepair)
        XCTAssertEqual(compactionRepairCandidates(output), [])
    }

    func testMonthTrustOutcomeIsDerivedFromReasonCategories() {
        typealias Trust = RepoMaterializer.MonthTrust
        typealias Reason = RepoMaterializer.MonthTrustReason

        XCTAssertEqual(Trust.clean.outcome, .clean)
        XCTAssertTrue(Trust.clean.reasons.isEmpty)
        XCTAssertEqual(Trust(reasons: []).outcome, .clean)

        XCTAssertEqual(
            Trust(reasons: [Reason(kind: .ambiguousSnapshotCoverage, category: .ambiguous)]).outcome,
            .ambiguous
        )
        XCTAssertEqual(
            Trust(reasons: [Reason(kind: .rejectedUncoveredCommit, category: .corrupt)]).outcome,
            .corrupt
        )
        // corrupt beats ambiguous regardless of reason order.
        XCTAssertEqual(
            Trust(reasons: [
                Reason(kind: .ambiguousSnapshotCoverage, category: .ambiguous),
                Reason(kind: .danglingReplayLink, category: .corrupt)
            ]).outcome,
            .corrupt
        )
        XCTAssertEqual(
            Trust(reasons: [
                Reason(kind: .danglingReplayLink, category: .corrupt),
                Reason(kind: .ambiguousSnapshotCoverage, category: .ambiguous)
            ]).outcome,
            .corrupt
        )
        // A repair constraint alone never makes a clean month non-clean.
        XCTAssertEqual(
            Trust(reasons: [Reason(kind: .outOfMonthReplayOp, category: .repairConstraint)]).outcome,
            .clean
        )
    }

    private static func contentHash(_ byte: UInt8) -> Data {
        TestFixtures.fingerprint(byte)
    }
}
