import XCTest
@testable import Watermelon

/// End-to-end V2 flush: upsert into V2MonthSession, flush, verify produced commit /
/// snapshot files and FlushDelta round-trip back through materialize.
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

        // Now write a superseding asset that subset-replaces the old one → tombstone
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

    /// Tombstone stamp must persist past snapshot baseline — LWW gate against
    /// stale adds reads it.
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

    /// Subset-replacement tombstones must carry observedBasis. Without it, a peer's
    /// concurrent add of the same fp at clock > our basis would be silently
    /// resurrected by replay; with it, the tombstone is observation-style and the
    /// materializer's basis-comparison gate skips it.
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

    /// observedBasis must roll forward across flushes. With a session-constant
    /// basis, our own first flush's addAsset (clock allocated AFTER load) would
    /// look like "after-observation" to a tombstone written in a later flush,
    /// so replay would suppress that tombstone. The basis captured at flush 2
    /// must include flush 1's clocks.
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

    /// upsertAsset must reject a link whose hash is in physicallyMissingHashes.
    /// Otherwise flush would emit a commit body with an empty resources[] (the
    /// link gets dropped at flush) and the snapshot covering that seq would
    /// break `state == fold(covered)`.
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
            XCTAssertEqual(nsError.code, -12, "must throw fail-fast on physicallyMissing link")
        }
    }

    /// Snapshot must emit one resource row per known physical path per hash. If it
    /// only emits the deduped one, the next materialize uses the snapshot as
    /// baseline + covered suppresses the alternate paths' commits → data loss.
    func testFlushV2SnapshotEmitsAllPhysicalPathsPerHash() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        // Same content hash, two different physical paths (multi-writer collision).
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

        // Re-materialize and verify both physical paths survive the snapshot round-trip.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[monthKey])
        XCTAssertNotNil(monthState.resources[pathA], "primary physical path must survive")
        XCTAssertNotNil(monthState.resources[pathB], "alternate physical path must survive — \"deduped\" snapshot loses data")
        XCTAssertEqual(monthState.resources[pathA]?.contentHash, hash)
        XCTAssertEqual(monthState.resources[pathB]?.contentHash, hash)
    }

    /// Resources whose physical files are missing from the directory listing must be
    /// dropped at session-load time. Otherwise `findResourceByHash` keeps returning the
    /// stale path → `AssetProcessor.uploadResource` short-circuits as `hash_exists` and
    /// `monthAlreadyFullyBackedUp` skips the whole month, leaving the gap unrepaired.
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

    /// Snapshot is **faithful to the commit log** (post-Step-5 contract): even when
    /// physical files are missing for some resources, the next snapshot must include
    /// the asset row + link unchanged. This is the covered-range invariant —
    /// `state == fold(commit ops in covered)`. Earlier rounds suppressed orphan
    /// links/assets here, which broke the invariant: snapshot dropped state but
    /// still claimed to cover the commits that produced it, so materializer
    /// silently lost historical evidence forever.
    ///
    /// The session-view layer (`findResourceByHash`, `isAssetIncomplete`) is what
    /// gates actionability now; the snapshot writer never filters.
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

    /// Re-upserting at the same physicalRemotePath with a different content hash must
    /// not leave the old (oldHash → path) mapping dangling. A stale entry would make
    /// `findResourceByHash(oldHash)` return the slot — now containing the new content —
    /// and downstream callers would fetch wrong-content bytes.
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

    // MARK: - V2 services manual construction

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
