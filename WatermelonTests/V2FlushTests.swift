import XCTest
@testable import Watermelon

final class V2FlushTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "repo-test-uuid"
    private let runID = "run-test-uuid"
    private let year = 2026
    private let month = 1
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    func testFlushV2WritesCommitAndSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let store = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.fingerprint(0xBB),
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/photo.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])

        let delta = try await store.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedV2AssetFingerprints, [asset.assetFingerprint])
        XCTAssertTrue(delta.committedV2TombstoneFingerprints.isEmpty)

        // Commit file should exist at expected path.
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 1
        )
        let commitExists = await client.hasFile(commitPath)
        XCTAssertTrue(commitExists, "commit file must be written at seq 1")

        // Snapshot file should exist (lamport advanced past 0).
        let snapshotsList = try await client.list(path: "\(basePath)/.watermelon/snapshots")
        XCTAssertEqual(snapshotsList.filter { !$0.isDirectory }.count, 1)

        // Cross-validate via materialize so we pin actual bytes, not just paths.
        // The existence + delta checks above would pass even if commit body was
        // empty, used the wrong fingerprint, or pointed at a wrong physical path.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])

        let materializedAsset = try XCTUnwrap(monthState.assets[asset.assetFingerprint],
            "asset must round-trip through commit + snapshot")
        XCTAssertEqual(materializedAsset.totalFileSizeBytes, asset.totalFileSizeBytes)
        XCTAssertEqual(materializedAsset.creationDateMs, asset.creationDateMs)
        XCTAssertEqual(materializedAsset.backedUpAtMs, asset.backedUpAtMs)
        XCTAssertEqual(materializedAsset.resourceCount, asset.resourceCount)

        let materializedResource = try XCTUnwrap(monthState.resources["2026/01/photo.jpg"],
            "resource must be at the physical path we wrote")
        XCTAssertEqual(materializedResource.contentHash, hash,
            "content hash must round-trip exactly — a swap or truncation here means the commit body is wrong")
        XCTAssertEqual(materializedResource.fileSize, resource.fileSize)
        XCTAssertEqual(materializedResource.resourceType, ResourceTypeCode.photo)

        let arKey = AssetResourceKey(
            assetFingerprint: asset.assetFingerprint,
            role: ResourceTypeCode.photo,
            slot: 0
        )
        let materializedLink = try XCTUnwrap(monthState.assetResources[arKey],
            "asset → resource link must round-trip — a missing link here means commit's resources[] was empty")
        XCTAssertEqual(materializedLink.resourceHash, hash)
        XCTAssertEqual(materializedLink.logicalName, "photo.jpg")
    }

    func testFlushV2ReturnsTombstoneFingerprintsInDelta() async throws {
        // Tombstone-only flush: applyDeletions adds to pending tombstones; flush
        // reports them in committedV2TombstoneFingerprints.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Materialize an existing asset so reconcile can tombstone it
        let fp = TestFixtures.fingerprint(0xCC)
        let hash = TestFixtures.fingerprint(0xDD)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: fp,
            resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0,
            logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link], replacingSubsetFingerprints: [])
        // Initial flush — asset committed
        _ = try await store.flushToRemote(ignoreCancellation: false)

        let newFP = TestFixtures.fingerprint(0xEE)
        let newAsset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [fp])

        let delta = try await store.flushToRemote(ignoreCancellation: false)
        XCTAssertTrue(delta.committedV2TombstoneFingerprints.contains(fp),
                      "tombstone fingerprint must be reported in FlushDelta so caller can clear uncommittedV2")
        XCTAssertTrue(delta.committedV2AssetFingerprints.contains(newFP),
                      "superseding asset must be reported in FlushDelta")

        // Both ops must land in a single commit file (seq=2; seq=1 was the first flush).
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let secondCommitExists = await client.hasFile(commitPath)
        XCTAssertTrue(secondCommitExists, "second flush must be at seq 2")

        // Materialize and verify the tombstoned-then-superseded shape lands:
        // newFP present, fp absent, fp recorded as tombstoned with our stamp.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.assets[newFP], "superseding asset must be present after replay")
        XCTAssertNil(monthState.assets[fp], "subset-replaced asset must not be present")
        XCTAssertTrue(monthState.deletedAssetFingerprints.contains(fp),
                      "tombstone must survive snapshot baseline so LWW gate against stale adds keeps working")
        XCTAssertNotNil(monthState.deletedAssetStamps[fp],
                        "tombstone stamp must persist for cross-writer LWW comparison")
    }

    func testTombstone_coveredBySnapshot_carriesStampForLWW() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let fp = TestFixtures.fingerprint(0xCC)
        let hash = TestFixtures.fingerprint(0xDD)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        // Tombstone via subset replacement, then flush again — the tombstone now
        // lives in a commit AND its effect is in the snapshot baseline.
        let supersedingFP = TestFixtures.fingerprint(0xEE)
        let superseding = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: supersedingFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        try store.upsertAsset(superseding, links: [link], replacingSubsetFingerprints: [fp])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])

        XCTAssertNil(monthState.assets[fp])
        XCTAssertTrue(monthState.deletedAssetFingerprints.contains(fp))
        let stamp = try XCTUnwrap(monthState.deletedAssetStamps[fp])
        XCTAssertEqual(stamp.writerID, writerID)
        XCTAssertGreaterThan(stamp.clock, 0)
    }

    func testFlushV2_subsetReplacementTombstone_carriesObservedBasis() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldFP = TestFixtures.fingerprint(0xC1)
        let newFP = TestFixtures.fingerprint(0xC2)
        let hash = TestFixtures.fingerprint(0xD1)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let oldAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: oldFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: oldFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(oldAsset, links: [link])
        _ = try await store.flushToRemote()

        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [oldFP])
        _ = try await store.flushToRemote()

        // Read the second commit (seq=2) and assert tombstone has observedBasis.
        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try await client.download(remotePath: commitPath, localURL: downloadURL)
        let parsed = try CommitLogReader.parse(localURL: downloadURL)
        let tombstone = parsed.ops.first { op in
            if case .tombstoneAsset = op.body { return true }
            return false
        }
        guard let op = tombstone, case let .tombstoneAsset(body) = op.body else {
            XCTFail("expected tombstone op in subset-replacement commit"); return
        }
        XCTAssertNotNil(body.observedBasis, "subset-replacement tombstone must carry observedBasis")
    }

    func testFlushV2_observedBasis_rollsForwardAcrossFlushes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldFP = TestFixtures.fingerprint(0xC1)
        let newFP = TestFixtures.fingerprint(0xC2)
        let hash = TestFixtures.fingerprint(0xD1)
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash, fileSize: 1, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let oldAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: oldFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: oldFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )

        _ = try store.upsertResource(resource)
        try store.upsertAsset(oldAsset, links: [link])
        _ = try await store.flushToRemote()
        let lamportAfterFlush1 = await v2.lamport.value()
        XCTAssertGreaterThan(lamportAfterFlush1, 0)

        try store.upsertAsset(newAsset, links: [link], replacingSubsetFingerprints: [oldFP])
        _ = try await store.flushToRemote()

        let commitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try await client.download(remotePath: commitPath, localURL: downloadURL)
        let parsed = try CommitLogReader.parse(localURL: downloadURL)
        let tombstone = parsed.ops.first { op in
            if case .tombstoneAsset = op.body { return true }
            return false
        }
        guard let op = tombstone, case let .tombstoneAsset(body) = op.body,
              let basis = body.observedBasis else {
            XCTFail("expected tombstone with basis"); return
        }
        XCTAssertGreaterThanOrEqual(basis.lamportWatermark, lamportAfterFlush1,
                                     "basis must reflect lamport AFTER flush 1, not load-time only")
        XCTAssertGreaterThanOrEqual(basis.perWriterMaxSeq[writerID] ?? 0, 1,
                                    "basis perWriterMaxSeq must include flush 1's seq")
    }

    func testUpsertAsset_rejectsLinkToPhysicallyMissingHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xCC)
        let assetFP = TestFixtures.fingerprint(0xDD)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(resource)
        try store1.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Physical file disappears; reload sees the resource row but flags the hash missing.
        try await client.delete(path: "\(basePath)/\(physicalPath)")
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let newAssetFP = TestFixtures.fingerprint(0xEE)
        let newAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: newAssetFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100
        )
        let newLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: newAssetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        XCTAssertThrowsError(try store2.upsertAsset(newAsset, links: [newLink])) { err in
            let nsError = err as NSError
            XCTAssertEqual(nsError.domain, "V2MonthSession")
            XCTAssertEqual(nsError.code, -11, "must throw fail-fast on physicallyMissing link")
        }
    }

    func testFlushV2SnapshotEmitsOnlyCommittedPathsPerHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let pathA = "2026/01/photo.jpg"
        let pathB = "2026/01/photo~widB.jpg"

        let resA = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: pathA, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let resB = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: pathB, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store.upsertResource(resA)
        _ = try store.upsertResource(resB)

        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.fingerprint(0xBB),
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0,
            logicalName: "photo.jpg"
        )
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        // pathA is in the addAsset commit body (lex-min present path for hash).
        XCTAssertNotNil(monthState.resources[pathA], "committed path must survive")
        XCTAssertEqual(monthState.resources[pathA]?.contentHash, hash)
        // pathB was upserted but never linked through any committed asset → orphan.
        XCTAssertNil(monthState.resources[pathB],
                     "orphan path (no commit body references it) must not be in snapshot — would break state == fold(commits)")
    }

    func testReconcileDropsResourcesWhosePhysicalFileIsMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: stage a real upload + flush so the snapshot has a resource row
        // with a corresponding file on remote.
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xAA)
        let assetFP = TestFixtures.fingerprint(0xBB)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        // Stage the physical bytes too so the directory listing sees the file.
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store.flushToRemote()

        // Sanity: a fresh session would see this resource as live.
        let baseline = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertNotNil(baseline.findResourceByHash(hash), "baseline session must see the resource")

        // Round 2: simulate someone deleting the physical file out-of-band (manual rm,
        // peer cleanup, anything). Snapshot still has the row; remote dir doesn't.
        try await client.delete(path: "\(basePath)/\(physicalPath)")

        let reloaded = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertNil(
            reloaded.findResourceByHash(hash),
            "missing physical file must drop the resource so re-upload isn't skipped as hash_exists"
        )
        XCTAssertTrue(
            reloaded.isAssetIncomplete(assetFP),
            "asset becomes incomplete after its only resource is filtered out → triggers full re-processing"
        )
    }

    func testSnapshotPreservesAssetsEvenWhenPhysicalFilesMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: set up an asset whose only resource gets uploaded, flush to bake into snapshot.
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xCC)
        let assetFP = TestFixtures.fingerprint(0xDD)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store.flushToRemote()

        // Round 2: physical file gets removed (manual rm or peer cleanup).
        try await client.delete(path: "\(basePath)/\(physicalPath)")

        // Round 3: a fresh session sees the orphan via materialize+filter, then writes
        // a new snapshot for some unrelated reason (different asset upserted + flushed).
        let session2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertTrue(session2.isAssetIncomplete(assetFP),
                      "asset must be flagged incomplete after its only resource is missing")

        // Trigger a flush by upserting an unrelated asset+resource.
        let otherHash = TestFixtures.fingerprint(0x11)
        let otherFP = TestFixtures.fingerprint(0x22)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: otherPath, contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try session2.upsertResource(otherResource)
        try session2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await session2.flushToRemote()

        // Re-materialize and verify: the asset row AND its link MUST appear in the
        // new snapshot — that's the covered-range invariant. Verify-month is the
        // path that surfaces it as `partiallyMissing`/`fullyMissing` to the user;
        // snapshot writer doesn't take action on its own.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let arKey = AssetResourceKey(
            assetFingerprint: assetFP, role: ResourceTypeCode.photo, slot: 0
        )
        XCTAssertNotNil(
            monthState.assetResources[arKey],
            "Step 5 contract: snapshot is faithful to commit log; orphan link survives"
        )
        XCTAssertNotNil(
            monthState.assets[assetFP],
            "Step 5 contract: asset row stays even when its resources are physically missing"
        )
        // Session view: still flags it incomplete so consumers don't act on it.
        let session3 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        XCTAssertTrue(session3.isAssetIncomplete(assetFP),
                      "session view filters via physicallyMissingHashes — incomplete remains observable")
    }

    func testFlushV2_orphanUpsertResourceWithoutUpsertAsset_isFilteredFromSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let orphanHash = TestFixtures.fingerprint(0xA1)
        let orphanPath = "2026/01/orphan.jpg"
        let orphanResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: orphanPath,
            contentHash: orphanHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        // Simulates "first resource of a multi-resource asset uploaded, then second
        // upload permanently failed → AssetProcessor.process returns .failed without
        // upsertAsset". The orphan resource is left dangling in indexes.
        _ = try store.upsertResource(orphanResource)

        // A legitimate asset with its resource succeeds and gets committed.
        let legitHash = TestFixtures.fingerprint(0xA2)
        let legitPath = "2026/01/legit.jpg"
        let legitFP = TestFixtures.fingerprint(0xB2)
        let legitResource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: legitPath,
            contentHash: legitHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let legitAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: legitFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 50
        )
        let legitLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: legitFP, resourceHash: legitHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "legit.jpg"
        )
        _ = try store.upsertResource(legitResource)
        try store.upsertAsset(legitAsset, links: [legitLink])
        _ = try await store.flushToRemote()

        // Materialize: the legit resource is in the snapshot/commit body; the orphan
        // path appears in no commit body anywhere, so it must not show up either.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.resources[legitPath],
                        "committed resource must survive the materialize round-trip")
        XCTAssertNil(monthState.resources[orphanPath],
                     "orphan resource (upserted but never linked to a committed asset) must not be in snapshot — snapshot ≠ fold(commits)")
    }

    func testFlushV2_committedPathOverwrittenByUpsert_snapshotRetainsCommittedHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Round 1: commit asset A at (path, oldHash).
        let path = "2026/01/photo.jpg"
        let oldHash = TestFixtures.fingerprint(0x10)
        let assetAFP = TestFixtures.fingerprint(0x20)
        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let oldResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: oldHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let assetA = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetAFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100
        )
        let linkA = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetAFP, resourceHash: oldHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(oldResource)
        try store1.upsertAsset(assetA, links: [linkA])
        await client.injectFile(path: "\(basePath)/\(path)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Round 2: new session loads the snapshot. Simulate physical file deletion
        // followed by an upsertResource that repurposes the same path with a
        // different hash — but the corresponding upsertAsset never lands (asset
        // commit fails). Then commit a legitimate, unrelated asset to force a
        // snapshot flush.
        try await client.delete(path: "\(basePath)/\(path)")
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let newHash = TestFixtures.fingerprint(0x11)
        let newResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: newHash, fileSize: 200,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        _ = try store2.upsertResource(newResource)

        let otherHash = TestFixtures.fingerprint(0x30)
        let otherFP = TestFixtures.fingerprint(0x40)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: otherPath,
            contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try store2.upsertResource(otherResource)
        try store2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await store2.flushToRemote()

        // Materialize and verify: the path retains oldHash from the original commit,
        // NOT newHash from the orphan upsertResource.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let retainedRow = try XCTUnwrap(monthState.resources[path],
            "originally committed path must remain in snapshot — covered range includes its addAsset commit")
        XCTAssertEqual(retainedRow.contentHash, oldHash,
            "snapshot resource row at path must reflect the COMMITTED hash, not the in-session upsert overwrite")
        XCTAssertEqual(retainedRow.fileSize, 100,
            "the full row (size, type, etc) must match the committed row, not the overwritten one")
    }

    func testV2MonthIndexes_seed_isFaithfulToMaterializedResources() throws {
        let tombstonedHash = TestFixtures.fingerprint(0x77)
        let livingHash = TestFixtures.fingerprint(0x88)
        let livingFP = TestFixtures.fingerprint(0xC1)
        let tombstonedFP = TestFixtures.fingerprint(0xC2)
        let tombstonedPath = "2026/01/tombstoned.jpg"
        let livingPath = "2026/01/living.jpg"

        // Mirror a post-tombstone fold(covered):
        //   resources = {tombstonedPath (orphan after tombstone), livingPath}
        //   assets = {livingFP only}; links reference only livingHash
        //   deletedAssetStamps contains tombstonedFP
        var materialized = RepoMonthState.empty
        materialized.assets[livingFP] = SnapshotAssetRow(
            assetFingerprint: livingFP,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 50,
            stamp: OpStamp(writerID: writerID, seq: 1, clock: 1)
        )
        materialized.resources[livingPath] = SnapshotResourceRow(
            physicalRemotePath: livingPath,
            contentHash: livingHash,
            fileSize: 50,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            crypto: nil
        )
        materialized.resources[tombstonedPath] = SnapshotResourceRow(
            physicalRemotePath: tombstonedPath,
            contentHash: tombstonedHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0,
            crypto: nil
        )
        let linkKey = AssetResourceKey(assetFingerprint: livingFP, role: ResourceTypeCode.photo, slot: 0)
        materialized.assetResources[linkKey] = SnapshotAssetResourceRow(
            assetFingerprint: livingFP,
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: livingHash,
            logicalName: "living.jpg"
        )
        materialized.deletedAssetFingerprints.insert(tombstonedFP)
        materialized.deletedAssetStamps[tombstonedFP] = OpStamp(writerID: writerID, seq: 2, clock: 2)

        let indexes = V2MonthIndexes(
            year: year, month: month,
            materializedState: materialized,
            remoteFilesByName: [:],
            verifiedMissingHashes: nil,
            nameCase: .caseSensitive
        )
        let state = indexes.currentMaterializedState()
        XCTAssertNotNil(state.resources[livingPath],
                        "linked resource row must survive seed")
        XCTAssertNotNil(state.resources[tombstonedPath],
                        "post-tombstone orphan row must survive seed — RepoMaterializer leaves it in fold(covered), so dropping it here would break state == fold(covered)")
        XCTAssertEqual(state.resources[tombstonedPath]?.contentHash, tombstonedHash,
                        "the orphan row's content hash must round-trip exactly — drift would corrupt the snapshot baseline")
    }

    func testFlushV2_postTombstoneOrphanResource_survivesAcrossFlushes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        let assetFP = TestFixtures.fingerprint(0xA1)
        let hash = TestFixtures.fingerprint(0xB1)
        let physicalPath = "2026/01/photo.jpg"

        // Round 1: commit asset A with one resource, with physical bytes on remote.
        let store1 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: physicalPath,
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000, backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: assetFP, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "photo.jpg"
        )
        _ = try store1.upsertResource(resource)
        try store1.upsertAsset(asset, links: [link])
        await client.injectFile(path: "\(basePath)/\(physicalPath)", data: Data(repeating: 0, count: 100))
        _ = try await store1.flushToRemote()

        // Round 2: subset-replace asset A with asset B (different fp, same resource).
        // This emits a tombstone for assetFP whose resource row remains in fold(covered).
        let supersedingFP = TestFixtures.fingerprint(0xA2)
        let superseding = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: supersedingFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100
        )
        try store1.upsertAsset(superseding, links: [link], replacingSubsetFingerprints: [assetFP])
        _ = try await store1.flushToRemote()

        // Reload + flush an unrelated asset; the resulting snapshot must still emit
        // the resource row for `physicalPath` because fold(covered) includes both
        // the addAsset(A) and the tombstone(A) commits, and the materializer's
        // tombstone handling preserves `state.resources[physicalPath]`.
        let store2 = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let otherFP = TestFixtures.fingerprint(0xC0)
        let otherHash = TestFixtures.fingerprint(0xD0)
        let otherPath = "2026/01/other.jpg"
        let otherResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: otherPath,
            contentHash: otherHash, fileSize: 50,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let otherAsset = RemoteManifestAsset(
            year: year, month: month, assetFingerprint: otherFP,
            creationDateMs: nil, backedUpAtMs: 3, resourceCount: 1, totalFileSizeBytes: 50
        )
        let otherLink = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: otherFP, resourceHash: otherHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "other.jpg"
        )
        _ = try store2.upsertResource(otherResource)
        try store2.upsertAsset(otherAsset, links: [otherLink])
        await client.injectFile(path: "\(basePath)/\(otherPath)", data: Data(repeating: 1, count: 50))
        _ = try await store2.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.resources[physicalPath],
                        "post-tombstone orphan resource row must survive reload+flush — fold(covered) preserves it")
        XCTAssertEqual(monthState.resources[physicalPath]?.contentHash, hash)
    }

    func testFlushV2_committedRowDates_matchAssetBodyNotResource() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xAA)
        let path = "2026/01/photo.jpg"
        // Resource has DIFFERENT dates than the asset, to surface any projection bug.
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: hash, fileSize: 100, resourceType: ResourceTypeCode.photo,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_005_000
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.fingerprint(0xBB),
            creationDateMs: 1_700_000_100_000,
            backedUpAtMs: 1_700_000_999_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        // Materialize: the resource row must carry the asset body's dates, matching
        // what RepoMaterializer would derive on replay.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let row = try XCTUnwrap(monthState.resources[path])
        XCTAssertEqual(row.creationDateMs, asset.creationDateMs,
                       "resource row's creationDateMs must come from asset body, not the live resource — replay derives it from body")
        XCTAssertEqual(row.backedUpAtMs, asset.backedUpAtMs,
                       "resource row's backedUpAtMs must come from asset body, not the live resource — replay derives it from body")
    }

    func testFlushV2_resourceRowStampPropagatesThroughProductionFlushPath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let hash = TestFixtures.fingerprint(0xCC)
        let path = "2026/01/stamped.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: hash, fileSize: 200,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.fingerprint(0xDD),
            creationDateMs: 1_700_000_100_000,
            backedUpAtMs: 1_700_000_200_000,
            resourceCount: 1, totalFileSizeBytes: 200
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month, assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "stamped.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])
        _ = try await store.flushToRemote()

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        let resourceRow = try XCTUnwrap(monthState.resources[path],
                                         "production flush must publish a resource row at the path")
        let resourceStamp = try XCTUnwrap(resourceRow.stamp,
                                           "production flush must stamp resource rows for path-level LWW")
        let assetRow = try XCTUnwrap(monthState.assets[asset.assetFingerprint])
        let assetStamp = try XCTUnwrap(assetRow.stamp,
                                        "production flush must stamp asset rows")
        XCTAssertEqual(resourceStamp.writerID, writerID,
                       "stamp.writerID must be the flusher's writerID")
        XCTAssertEqual(resourceStamp.writerID, assetStamp.writerID,
                       "resource and asset stamps share the producing op's writerID")
        XCTAssertEqual(resourceStamp.seq, assetStamp.seq,
                       "resource and asset stamps share the producing op's allocator seq")
        XCTAssertEqual(resourceStamp.clock, assetStamp.clock,
                       "resource stamp clock must match the producing addAsset op's clock")
        XCTAssertGreaterThan(resourceStamp.clock, 0)
        XCTAssertGreaterThan(resourceStamp.seq, 0)
    }

    func testUpsertResource_repurposingPath_dropsOldHashMapping() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        let oldHash = TestFixtures.fingerprint(0xAA)
        let newHash = TestFixtures.fingerprint(0xBB)
        let path = "2026/01/photo.jpg"

        let oldResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: oldHash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let newResource = RemoteManifestResource(
            year: year, month: month, physicalRemotePath: path,
            contentHash: newHash, fileSize: 200,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )

        _ = try store.upsertResource(oldResource)
        XCTAssertNotNil(store.findResourceByHash(oldHash), "baseline: old hash present after first upsert")

        _ = try store.upsertResource(newResource)
        XCTAssertNil(
            store.findResourceByHash(oldHash),
            "old hash must be unmapped after path is repurposed — otherwise lookup would serve new content under the old key"
        )
        XCTAssertEqual(store.findResourceByHash(newHash)?.contentHash, newHash)
    }

    func testFlushV2_retryOnAlreadyExists_reTicksLamportClockForFreshOrdering() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Default moveIfAbsent guarantee = .exclusive so the gate's staging path
        // surfaces destination collisions as .alreadyExists (matching production SMB/SFTP).
        let v2 = try await makeV2Services(client: client)

        // Pre-occupy seq=1 with arbitrary bytes — the gate's exclusive moveIfAbsent
        // will refuse to overwrite, throwing .alreadyExists.
        let preExistingCommitPath = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 1
        )
        await client.injectFile(path: preExistingCommitPath, data: Data("pre-existing".utf8))

        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: TestFixtures.fingerprint(0xFE),
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1, totalFileSizeBytes: 100
        )
        let hash = TestFixtures.fingerprint(0xFD)
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/retry-photo.jpg",
            contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint,
            resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
            logicalName: "retry-photo.jpg"
        )
        _ = try store.upsertResource(resource)
        try store.upsertAsset(asset, links: [link])

        let delta = try await store.flushToRemote()
        XCTAssertTrue(delta.didFlush)
        XCTAssertEqual(delta.committedV2AssetFingerprints, [asset.assetFingerprint])

        // The successful commit lands at seq=2 (seq=1 was pre-occupied).
        let seq2Path = RepoLayout.commitFilePath(
            base: basePath, month: monthKey, writerID: writerID, seq: 2
        )
        let seq2Exists = await client.hasFile(seq2Path)
        XCTAssertTrue(seq2Exists, "retry must succeed at seq=2 after seq=1 collision")

        // Parse the successful commit's header to verify clock advanced past the
        // first attempt's tick. Without re-tick, clockMin would stay at 1 even though
        // seq advanced to 2; with re-tick, clockMin is 2 (or higher).
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("retry-commit-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        try await client.download(remotePath: seq2Path, localURL: tempURL)
        let parsed = try CommitLogReader.parse(localURL: tempURL)
        XCTAssertGreaterThanOrEqual(parsed.header.clockMin, 2,
                                    "retry must re-tick Lamport so clockMin advances past the failed-attempt tick of 1")
    }


    private func makeV2Services(client: InMemoryRemoteStorageClient) async throws -> BackupV2RuntimeServices {
        let profileID = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let liveness = LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            liveness: liveness,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private func insertProfile() throws -> Int64 {
        try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
    }
}
