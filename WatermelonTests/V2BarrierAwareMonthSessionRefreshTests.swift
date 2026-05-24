import XCTest
@testable import Watermelon

final class V2BarrierAwareMonthSessionRefreshTests: XCTestCase {
    private let basePath = "/unit6-refresh"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let foreignRepoID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let runA = "33333333-3333-3333-3333-333333333333"
    private let runB = "44444444-4444-4444-4444-444444444444"
    private let month = LibraryMonthKey(year: 2026, month: 5)
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

    func testPeerAddSurvivesStaleTombstoneAndFreshSnapshotIncludesPeer() async throws {
        let scenario = try await flushPeerHealScenario()

        let materialized = try await RepoMaterializer(client: scenario.client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)
        let state = try XCTUnwrap(materialized.state.months[month])
        XCTAssertNotNil(state.assets[scenario.oldFP])
        XCTAssertNotNil(state.assets[scenario.replacementFP])

        let snapshot = try await latestSnapshot(client: scenario.client)
        XCTAssertTrue(snapshot.header.covered.contains(writerID: writerB, seq: 7))
        XCTAssertTrue(snapshot.header.covered.contains(writerID: writerA, seq: 2))
        XCTAssertTrue(snapshot.assets.contains { $0.assetFingerprint == scenario.oldFP })
    }

    func testBarrierCommitClockStillTicksAboveFreshObservedClock() async throws {
        let scenario = try await flushPeerHealScenario()

        XCTAssertGreaterThan(scenario.commit.header.clockMin, 500)
        XCTAssertGreaterThan(scenario.commit.header.clockMax, 500)
    }

    func testPendingTombstoneBasisIncludesPriorLocalSessionCommitsButNotPeerBarrier() async throws {
        let scenario = try await flushPeerHealScenario()

        XCTAssertLessThan(scenario.tombstoneBasis.lamportWatermark, 500)
        XCTAssertLessThan(scenario.tombstoneBasis.perWriterMaxSeq[writerB] ?? 0, 7)
        XCTAssertGreaterThanOrEqual(scenario.tombstoneBasis.perWriterMaxSeq[writerA] ?? 0, 1)
    }

    func testEnabledInvalidBarrierFailsClosedBeforeCommitWrite() async throws {
        let client = try await makeClient()
        try await injectBarrier(client: client, repoID: foreignRepoID, writerID: writerB, runID: runB, seq: 1, lamport: 10)
        let services = try await makeV2Services(client: client)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xC1, name: "blocked.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected invalid barrier to fail closed")
        } catch V2RetentionBarrierRefreshError.invalidBarrierSet(let invalid) {
            XCTAssertEqual(invalid.map(\.reason), [.foreignRepoID(foreignRepoID)])
        }
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 0)
    }

    func testEnabledInvalidBarrierFailsClosedBeforeSnapshotWrite() async throws {
        let client = try await makeClient()
        try await injectBarrier(client: client, repoID: foreignRepoID, writerID: writerB, runID: runB, seq: 1, lamport: 10)
        let services = try await makeV2Services(client: client)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        session.requestSnapshotRebaseline()

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected invalid barrier to fail snapshot refresh")
        } catch V2MonthSession.FlushError.snapshotWriteFailed(let assets, let tombstones, let underlying) {
            XCTAssertTrue(assets.isEmpty)
            XCTAssertTrue(tombstones.isEmpty)
            guard case V2RetentionBarrierRefreshError.invalidBarrierSet(let invalid) = underlying else {
                return XCTFail("expected invalidBarrierSet, got \(underlying)")
            }
            XCTAssertEqual(invalid.map(\.reason), [.foreignRepoID(foreignRepoID)])
        }
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.commits, 0)
        XCTAssertEqual(counts.snapshots, 0)
    }

    func testInvalidBarrierAfterCommitSurfacesSnapshotWriteFailedWithCommittedFingerprints() async throws {
        let client = try await makeClient()
        let hooked = HookedRemoteStorageClient(inner: client)
        let services = try await makeV2Services(client: hooked)
        let session = try await V2MonthSession.loadOrCreate(
            client: hooked,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xE1, name: "post-commit.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])
        hooked.setPostAtomicCreateHook(containing: "/commits/") { _, _, inner in
            try await self.injectBarrier(
                client: inner,
                repoID: self.foreignRepoID,
                writerID: self.writerB,
                runID: self.runB,
                seq: 1,
                lamport: 10
            )
        }

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected snapshotWriteFailed")
        } catch V2MonthSession.FlushError.snapshotWriteFailed(let assets, let tombstones, let underlying) {
            XCTAssertEqual(assets, [rows.asset.assetFingerprint])
            XCTAssertTrue(tombstones.isEmpty)
            guard case V2RetentionBarrierRefreshError.invalidBarrierSet(let invalid) = underlying else {
                return XCTFail("expected invalidBarrierSet, got \(underlying)")
            }
            XCTAssertEqual(invalid.map(\.reason), [.foreignRepoID(foreignRepoID)])
        }
        let commitExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1))
        XCTAssertTrue(commitExists)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.snapshots, 0)
    }

    func testSameWriterSeqObservedBeforeCommitAllocation() async throws {
        let client = try await makeClient()
        let oldHash = TestFixtures.fingerprint(0xE2)
        try await writeAddAsset(
            client: client,
            writerID: writerA,
            runID: runA,
            seq: 9,
            clock: 90,
            fp: assetFingerprint(hash: oldHash),
            hash: oldHash,
            path: "2026/05/old-same-writer.jpg"
        )
        try await injectBarrier(client: client, repoID: repoID, writerID: writerA, runID: runA, seq: 9, lamport: 100)
        let services = try await makeV2Services(client: client, initialSeq: 1)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xE3, name: "after-same-writer.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])

        _ = try await session.flushToRemote(ignoreCancellation: false)

        let highSeqExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 10))
        let staleSeqExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2))
        XCTAssertTrue(highSeqExists)
        XCTAssertFalse(staleSeqExists)
    }

    func testMetadataReadRaceFromBarrierMaterializePropagatesWithoutCommitWrite() async throws {
        let client = try await makeClient()
        let peerHash = TestFixtures.fingerprint(0xE4)
        let peerPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerB, seq: 4)
        try await writeAddAsset(
            client: client,
            writerID: writerB,
            runID: runB,
            seq: 4,
            clock: 40,
            fp: assetFingerprint(hash: peerHash),
            hash: peerHash,
            path: "2026/05/raced-peer.jpg"
        )
        try await injectBarrier(client: client, repoID: repoID, writerID: writerB, runID: runB, seq: 4, lamport: 80)
        let hooked = HookedRemoteStorageClient(inner: client)
        let services = try await makeV2Services(client: hooked)
        let session = try await V2MonthSession.loadOrCreate(
            client: hooked,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xE5, name: "blocked-by-race.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])
        hooked.setDownloadHook(path: peerPath) { _, _, inner in
            try await inner.delete(path: peerPath)
            throw Self.notFoundError()
        }

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected metadata read race")
        } catch RepoMaterializer.MetadataReadRaceError.requiredCommitVanished(let filename, let month, let writerID, let seq) {
            XCTAssertEqual(filename, RepoLayout.commitFileName(month: self.month, writerID: writerB, seq: 4))
            XCTAssertEqual(month, self.month)
            XCTAssertEqual(writerID, writerB)
            XCTAssertEqual(seq, 4)
        }
        let localCommitExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1))
        XCTAssertFalse(localCommitExists)
    }

    func testCancellationPropagatesFromBarrierLoad() async throws {
        let client = try await makeClient()
        let hooked = HookedRemoteStorageClient(inner: client)
        hooked.setListHook(path: RepoLayout.retentionDirectoryPath(base: basePath)) { _, _ in
            throw CancellationError()
        }
        let services = try await makeV2Services(client: hooked)
        let session = try await V2MonthSession.loadOrCreate(
            client: hooked,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xE6, name: "cancel-load.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
        let localCommitExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1))
        XCTAssertFalse(localCommitExists)
    }

    func testCancellationPropagatesFromBarrierFreshMaterialize() async throws {
        let client = try await makeClient()
        let peerHash = TestFixtures.fingerprint(0xE7)
        let peerPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerB, seq: 3)
        try await writeAddAsset(
            client: client,
            writerID: writerB,
            runID: runB,
            seq: 3,
            clock: 30,
            fp: assetFingerprint(hash: peerHash),
            hash: peerHash,
            path: "2026/05/cancel-peer.jpg"
        )
        try await injectBarrier(client: client, repoID: repoID, writerID: writerB, runID: runB, seq: 3, lamport: 70)
        let hooked = HookedRemoteStorageClient(inner: client)
        let services = try await makeV2Services(client: hooked)
        let session = try await V2MonthSession.loadOrCreate(
            client: hooked,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xE8, name: "cancel-materialize.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])
        hooked.setDownloadHook(path: peerPath) { _, _, _ in
            throw CancellationError()
        }

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
        let localCommitExists = await client.hasFile(RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1))
        XCTAssertFalse(localCommitExists)
    }

    func testSnapshotFreshCoverageMissingSessionWritesFailsAfterDurableCommit() async throws {
        let client = try await makeClient()
        let peerHash = TestFixtures.fingerprint(0xE9)
        try await writeAddAsset(
            client: client,
            writerID: writerB,
            runID: runB,
            seq: 5,
            clock: 50,
            fp: assetFingerprint(hash: peerHash),
            hash: peerHash,
            path: "2026/05/session-coverage-peer.jpg"
        )
        try await injectBarrier(client: client, repoID: repoID, writerID: writerB, runID: runB, seq: 5, lamport: 90)
        let hooked = HookedRemoteStorageClient(inner: client)
        let services = try await makeV2Services(client: hooked)
        let session = try await V2MonthSession.loadOrCreate(
            client: hooked,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        let rows = makeAssetRows(hashByte: 0xEA, name: "session-coverage-local.jpg")
        _ = try session.upsertResource(rows.resource)
        try session.upsertAsset(rows.asset, links: [rows.link])
        let ourCommitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        hooked.setPostAtomicCreateHook(containing: ourCommitPath) { _, _, _ in
            hooked.setListHook(path: RepoLayout.commitsDirectoryPath(base: self.basePath), once: false) { _, inner in
                let entries = try await inner.list(path: RepoLayout.commitsDirectoryPath(base: self.basePath))
                return entries.filter { $0.path != ourCommitPath }
            }
        }

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected freshCoverageMissingSessionWrites")
        } catch V2MonthSession.FlushError.snapshotWriteFailed(let assets, _, let underlying) {
            XCTAssertEqual(assets, [rows.asset.assetFingerprint])
            guard case V2RetentionBarrierRefreshError.freshCoverageMissingSessionWrites(let month, _, let sessionWritten) = underlying else {
                return XCTFail("expected freshCoverageMissingSessionWrites, got \(underlying)")
            }
            XCTAssertEqual(month, self.month)
            XCTAssertTrue(sessionWritten.contains(writerID: writerA, seq: 1))
        }
        let localCommitExists = await client.hasFile(ourCommitPath)
        XCTAssertTrue(localCommitExists)
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.snapshots, 0)
    }

    func testSnapshotFreshCoverageMissingBarrierFailsBeforeSnapshotWrite() async throws {
        let client = try await makeClient()
        try await injectBarrier(client: client, repoID: repoID, writerID: writerB, runID: runB, seq: 44, lamport: 120)
        let services = try await makeV2Services(client: client)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )
        session.requestSnapshotRebaseline()

        do {
            _ = try await session.flushToRemote(ignoreCancellation: false)
            XCTFail("expected freshCoverageMissingBarrier")
        } catch V2MonthSession.FlushError.snapshotWriteFailed(_, _, let underlying) {
            guard case V2RetentionBarrierRefreshError.freshCoverageMissingBarrier(let month, let fresh, let barrier) = underlying else {
                return XCTFail("expected freshCoverageMissingBarrier, got \(underlying)")
            }
            XCTAssertEqual(month, self.month)
            XCTAssertFalse(fresh.contains(writerID: writerB, seq: 44))
            XCTAssertTrue(barrier.contains(writerID: writerB, seq: 44))
        }
        let counts = await repoMetadataCounts(client)
        XCTAssertEqual(counts.snapshots, 0)
    }

    private func flushPeerHealScenario() async throws -> (
        client: InMemoryRemoteStorageClient,
        commit: CommitFile,
        tombstoneBasis: TombstoneObservationBasis,
        oldFP: Data,
        replacementFP: Data
    ) {
        let client = try await makeClient()
        let oldHash = TestFixtures.fingerprint(0xA1)
        let oldFP = assetFingerprint(hash: oldHash)
        try await writeAddAsset(
            client: client,
            writerID: writerA,
            runID: runA,
            seq: 1,
            clock: 100,
            fp: oldFP,
            hash: oldHash,
            path: "2026/05/original.jpg"
        )

        let services = try await makeV2Services(client: client, initialSeq: 1)
        let session = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: services
        )

        let replacement = makeAssetRows(hashByte: 0xB1, name: "replacement.jpg")
        _ = try session.upsertResource(replacement.resource)
        try session.upsertAsset(
            replacement.asset,
            links: [replacement.link],
            replacingSubsetFingerprints: [oldFP]
        )

        try await writeAddAsset(
            client: client,
            writerID: writerB,
            runID: runB,
            seq: 7,
            clock: 500,
            fp: oldFP,
            hash: oldHash,
            path: "2026/05/peer-heal.jpg"
        )
        try await injectBarrier(client: client, repoID: repoID, writerID: writerB, runID: runB, seq: 7, lamport: 600)

        let delta = try await session.flushToRemote(ignoreCancellation: false)
        XCTAssertEqual(delta.committedAssetFingerprints, [replacement.asset.assetFingerprint])
        XCTAssertEqual(delta.committedTombstoneFingerprints, [oldFP])

        let commit = try await readCommit(client: client, writerID: writerA, seq: 2)
        let tombstone = try XCTUnwrap(commit.ops.compactMap { op -> CommitTombstoneBody? in
            if case .tombstoneAsset(let body) = op.body { return body }
            return nil
        }.first)
        let basis = try XCTUnwrap(tombstone.observedBasis)
        return (client, commit, basis, oldFP, replacement.asset.assetFingerprint)
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        return client
    }

    private func makeV2Services(
        client: any RemoteStorageClientProtocol,
        initialSeq: UInt64 = 0
    ) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager,
            writerID: writerA,
            basePath: basePath,
            storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerA)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: initialSeq)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        return BackupV2RuntimeServices(
            writerID: writerA,
            repoID: repoID,
            runID: runA,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: CommitLogWriter(client: client, basePath: basePath),
            snapshotWriter: SnapshotWriter(client: client, basePath: basePath),
            liveness: LivenessTracker(client: client, basePath: basePath, writerID: writerA, isLocalVolume: true),
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private func writeAddAsset(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        runID: String,
        seq: UInt64,
        clock: UInt64,
        fp: Data,
        hash: Data,
        path: String
    ) async throws {
        let body = CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: path,
                    logicalName: (path as NSString).lastPathComponent,
                    contentHash: hash,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: month,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: true
        )
    }

    private func injectBarrier(
        client: InMemoryRemoteStorageClient,
        repoID: String,
        writerID: String,
        runID: String,
        seq: UInt64,
        lamport: UInt64
    ) async throws {
        let covered = CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: seq, high: seq)]])
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerID,
            runID: UUID(uuidString: runID)!,
            createdAtMs: 1_800_000_000_000,
            barrierLamport: lamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID),
            checkpointSHA256Hex: String(repeating: "a", count: 64),
            coveredRanges: covered,
            deletePrefixByWriter: [:],
            observedSeqHighByWriter: [writerID: seq],
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: 2
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 604_800_000
            )
        )
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref),
            data: try RetentionManifestStore.encode(manifest)
        )
    }

    private func makeAssetRows(hashByte: UInt8, name: String) -> (
        asset: RemoteManifestAsset,
        resource: RemoteManifestResource,
        link: RemoteAssetResourceLink
    ) {
        let hash = TestFixtures.fingerprint(hashByte)
        let fp = assetFingerprint(hash: hash)
        let path = "2026/05/\(name)"
        let asset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: path,
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: fp,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: name
        )
        return (asset, resource, link)
    }

    private func assetFingerprint(hash: Data) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
    }

    private func readCommit(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seq: UInt64
    ) async throws -> CommitFile {
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: seq)
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: path, localURL: temp)
        return try CommitLogReader.parse(localURL: temp)
    }

    private func latestSnapshot(client: InMemoryRemoteStorageClient) async throws -> SnapshotFile {
        let reader = SnapshotReader(client: client, basePath: basePath)
        let snapshots = try await reader.listSnapshotFilenames()
            .compactMap { filename -> (filename: String, parsed: RepoLayout.ParsedSnapshotFilename)? in
                guard let parsed = RepoLayout.parseSnapshotFilename(filename), parsed.month == month else { return nil }
                return (filename, parsed)
            }
        let latest = try XCTUnwrap(snapshots.max { $0.parsed.lamport < $1.parsed.lamport })
        return try await reader.read(filename: latest.filename)
    }

    private func repoMetadataCounts(_ client: InMemoryRemoteStorageClient) async -> (commits: Int, snapshots: Int) {
        let files = await client.snapshotFiles().keys
        return (
            files.filter { $0.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/") }.count,
            files.filter { $0.hasPrefix(RepoLayout.snapshotsDirectoryPath(base: basePath) + "/") }.count
        )
    }

    private static func notFoundError() -> Error {
        RemoteStorageClientError.underlying(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError))
    }
}

private final class HookedRemoteStorageClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias ListHook = (_ path: String, _ inner: InMemoryRemoteStorageClient) async throws -> [RemoteStorageEntry]
    typealias DownloadHook = (_ path: String, _ localURL: URL, _ inner: InMemoryRemoteStorageClient) async throws -> Void
    typealias AtomicCreateHook = (_ path: String, _ result: AtomicCreateResult, _ inner: InMemoryRemoteStorageClient) async throws -> Void

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        .exclusive
    }

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var listHooks: [String: (once: Bool, hook: ListHook)] = [:]
    private var downloadHooks: [String: (once: Bool, hook: DownloadHook)] = [:]
    private var atomicCreateHooks: [(substring: String, once: Bool, hook: AtomicCreateHook)] = []
    private var listCounts: [String: Int] = [:]

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func setListHook(path: String, once: Bool = true, hook: @escaping ListHook) {
        lock.withLock {
            listHooks[Self.normalize(path)] = (once, hook)
        }
    }

    func setDownloadHook(path: String, once: Bool = true, hook: @escaping DownloadHook) {
        lock.withLock {
            downloadHooks[Self.normalize(path)] = (once, hook)
        }
    }

    func setPostAtomicCreateHook(containing substring: String, once: Bool = true, hook: @escaping AtomicCreateHook) {
        lock.withLock {
            atomicCreateHooks.append((substring, once, hook))
        }
    }

    func listCount(path: String) -> Int {
        lock.withLock { listCounts[Self.normalize(path)] ?? 0 }
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let key = Self.normalize(path)
        let hook: ListHook? = lock.withLock {
            listCounts[key, default: 0] += 1
            guard let entry = listHooks[key] else { return nil }
            if entry.once { listHooks.removeValue(forKey: key) }
            return entry.hook
        }
        if let hook {
            return try await hook(key, inner)
        }
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        try await inner.metadata(path: path)
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        let key = Self.normalize(remotePath)
        let result = try await inner.atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
        let hooks: [AtomicCreateHook] = lock.withLock {
            var matched: [AtomicCreateHook] = []
            var remaining: [(substring: String, once: Bool, hook: AtomicCreateHook)] = []
            for entry in atomicCreateHooks {
                if key.contains(entry.substring) {
                    matched.append(entry.hook)
                    if !entry.once { remaining.append(entry) }
                } else {
                    remaining.append(entry)
                }
            }
            atomicCreateHooks = remaining
            return matched
        }
        for hook in hooks {
            try await hook(key, result, inner)
        }
        return result
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let hook: DownloadHook? = lock.withLock {
            guard let entry = downloadHooks[key] else { return nil }
            if entry.once { downloadHooks.removeValue(forKey: key) }
            return entry.hook
        }
        if let hook {
            try await hook(key, localURL, inner)
            return
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}
