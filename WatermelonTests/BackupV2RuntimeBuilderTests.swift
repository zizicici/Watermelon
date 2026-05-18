import XCTest
@testable import Watermelon

final class BackupV2RuntimeBuilderTests: XCTestCase {
    private let basePath = "/repo"
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

    func testFreshRepo_bootstrapsAndInvokesCallback() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        var bootstrapCalled = false
        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false,
            onBootstrap: { bootstrapCalled = true }
        )
        XCTAssertTrue(bootstrapCalled, "fresh path must invoke onBootstrap")
        XCTAssertFalse(services.repoID.isEmpty)
        XCTAssertFalse(services.writerID.isEmpty)
        let repoExists = await client.hasFile(RepoLayout.repoFilePath(base: basePath))
        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(repoExists)
        XCTAssertTrue(versionExists)
        await services.shutdown()
    }

    func testV1Repo_allowMigrationFalse_throwsRequiresForegroundMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let metadataClient = InMemoryRemoteStorageClient()
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected requiresForegroundMigration")
        } catch BackupV2RuntimeBuildError.requiresForegroundMigration {
            // expected — BG runner refuses to migrate
        }
    }

    func testUnsupportedRemote_throwsUnsupportedRemoteFormat() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "future-id")
        let metadataClient = InMemoryRemoteStorageClient()
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected unsupportedRemoteFormat")
        } catch BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minApp) {
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testV2Repo_localIDMatchesRemote_succeeds() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let canonicalRepoID = "canonical-repo-id"
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: canonicalRepoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: canonicalRepoID, writerID: writerID)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        XCTAssertEqual(services.repoID, canonicalRepoID)
        await services.shutdown()
    }

    func testV2Repo_localIDDiffersFromRemote_throwsIdentityMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "remote-canonical")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: "stale-local", writerID: writerID)

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
            XCTAssertEqual(stored, "stale-local")
            XCTAssertEqual(observed, "remote-canonical")
        }
    }

    func testV2WithV1Manifests_corruptRepoJSON_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // Malformed bytes at the repo identity path force BootstrapError.ioFailure
        // from RepoIdentitySources.collect → bootstrap.loadRepoID → loadRepoJSONStrict
        // before V1MigrationService is invoked.
        await client.injectFile(path: RepoLayout.repoFilePath(base: basePath), contents: "{not-json")
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: true
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected
        }
    }

    func testFreshArm_corruptRepoJSON_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        // Marker directory present forces inspect past the "no marker → .fresh"
        // shortcut so the malformed file is exercised in the bootstrap path.
        try await client.createDirectory(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory])
        )
        await client.injectFile(path: RepoLayout.repoFilePath(base: basePath), contents: "{not-json")
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected — the `.fresh` arm now mirrors sibling damaged-repo mapping
        }
    }

    func testV2Repo_halfBootstrap_repoMissing_isHealedByEnsureRepoJSON() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let firstRun = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        let canonicalID = firstRun.repoID
        await firstRun.shutdown()
        try await client.delete(path: RepoLayout.repoFilePath(base: basePath))

        let metadataClient2 = InMemoryRemoteStorageClient()
        metadataClient2.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient2.connect()
        let secondRun = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient2,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        XCTAssertEqual(secondRun.repoID, canonicalID,
                       "self-heal must reuse local DB's repoID, not generate a fresh UUID")
        let repoExists = await client.hasFile(RepoLayout.repoFilePath(base: basePath))
        XCTAssertTrue(repoExists, "ensureRepoJSON must re-create the missing file")
        await secondRun.shutdown()
    }

    func testV2WithPendingMigrationCleanup_corruptRepoJSON_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        // `RepoLayout.parseMigrationMarkerFilename` only accepts a 36-char lowercase
        // UUID writerID; anything else (e.g. "peer") is silently dropped by the marker
        // store and inspect would route through the `.v2` arm instead — the test
        // would pass for the wrong reason. Use a real UUID and pin the route below.
        let cleanupWriterID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: cleanupWriterID)
        let markerDict: [String: Any] = [
            "v": 2,
            "writer_id": cleanupWriterID,
            "run_id": "stale-run",
            "phase": 1,
            "started_at_ms": Int64(0),
            "last_step_at_ms": Int64(0)
        ]
        let markerData = try JSONSerialization.data(withJSONObject: markerDict)
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: cleanupWriterID),
            data: markerData
        )

        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        // Pin the inspect route BEFORE injecting the malformed repo.json — inspect
        // doesn't read repo.json, so this stays clean. If a future refactor changes
        // the marker filename rules or stops mapping phase1 residue to the cleanup
        // arm, this assertion fails loud before we exercise the catch-arm under test.
        let inspection = try await RemoteFormatCompatibilityService().inspectRemoteFormat(
            client: client, profile: profile
        )
        switch inspection {
        case .v2WithPendingMigrationCleanup(_, let ownerWriterID):
            XCTAssertEqual(ownerWriterID, cleanupWriterID,
                           "marker writerID must round-trip through parseEntries")
        default:
            XCTFail("expected .v2WithPendingMigrationCleanup route, got \(inspection)")
            return
        }

        // Malformed repo.json forces BootstrapError.ioFailure from the arm's
        // RepoIdentitySources.collect → bootstrap.loadRepoID path.
        await client.injectFile(path: RepoLayout.repoFilePath(base: basePath), contents: "{not-json")

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: true
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected
        }
    }

    func testInspectSide_corruptVersionJSON_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory])
        )
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), contents: "{not-json")
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected damagedV2Repo from inspect-side remap")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected
        }
    }

    func testBuild_doesNotBumpAllocatorToForeignWriterSeq() async throws {
        let canonicalRepoID = "shared-repo-id"
        let foreignWriterID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: canonicalRepoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)

        // Stage a foreign-writer commit at seq=1000 so the materializer reports
        // `observedSeqByWriter[foreignWriter] = 1000`.
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let header = TestFixtures.makeCommitHeader(
            repoID: canonicalRepoID,
            writerID: foreignWriterID,
            seq: 1000,
            runID: "foreign-run",
            month: LibraryMonthKey(year: 2026, month: 1),
            clockMin: 1,
            clockMax: 1
        )
        let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: Data(repeating: 0xAB, count: 32),
            creationDateMs: nil, backedUpAtMs: 1, resources: []
        )))
        _ = try await commitWriter.write(
            header: header,
            ops: [op],
            month: LibraryMonthKey(year: 2026, month: 1),
            respectTaskCancellation: false
        )

        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: canonicalRepoID, writerID: ourWriterID)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        let allocatorValue = await services.seqAllocator.value()
        XCTAssertLessThan(allocatorValue, 1000,
                          "allocator must not bump to a foreign writer's seq — namespacing is per (writerID, seq)")
        await services.shutdown()
    }

    func testBuild_observesSameWriterRemoteSeq() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)

        let month = LibraryMonthKey(year: 2026, month: 1)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false,
            onBootstrap: {
                do {
                    let bootstrap = RepoBootstrap(client: client, basePath: self.basePath)
                    guard let repoID = try await bootstrap.loadRepoID() else {
                        XCTFail("expected bootstrapped repoID")
                        return
                    }
                    let commitWriter = CommitLogWriter(client: client, basePath: self.basePath)
                    let header = TestFixtures.makeCommitHeader(
                        repoID: repoID,
                        writerID: ourWriterID,
                        seq: 7,
                        runID: "same-writer-run",
                        month: month,
                        clockMin: 1,
                        clockMax: 1
                    )
                    let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                        assetFingerprint: Data(repeating: 0xAC, count: 32),
                        creationDateMs: nil, backedUpAtMs: 1, resources: []
                    )))
                    _ = try await commitWriter.write(header: header, ops: [op], month: month, respectTaskCancellation: false)
                } catch {
                    XCTFail("failed to stage same-writer commit: \(error)")
                }
            }
        )
        let allocatorValue = await services.seqAllocator.value()
        XCTAssertEqual(allocatorValue, 7)
        let reloaded = try await identity.loadRepoState(profileID: profile.id!, repoID: services.repoID)
        XCTAssertEqual(reloaded?.lastSeq, 7)
        await services.shutdown()
    }

    func testBuild_ignoresAboveCeilingSameWriterRemoteSeq() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)

        let month = LibraryMonthKey(year: 2026, month: 1)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false,
            onBootstrap: {
                do {
                    let bootstrap = RepoBootstrap(client: client, basePath: self.basePath)
                    guard let repoID = try await bootstrap.loadRepoID() else {
                        XCTFail("expected bootstrapped repoID")
                        return
                    }
                    let commitWriter = CommitLogWriter(client: client, basePath: self.basePath)
                    let header = TestFixtures.makeCommitHeader(
                        repoID: repoID,
                        writerID: ourWriterID,
                        seq: RepoStateAuthority.maxPersistableSeq + 1,
                        runID: "same-writer-poison-run",
                        month: month,
                        clockMin: 1,
                        clockMax: 1
                    )
                    let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
                        assetFingerprint: Data(repeating: 0xAD, count: 32),
                        creationDateMs: nil, backedUpAtMs: 1, resources: []
                    )))
                    _ = try await commitWriter.write(header: header, ops: [op], month: month, respectTaskCancellation: false)
                } catch {
                    XCTFail("failed to stage above-ceiling same-writer commit: \(error)")
                }
            }
        )
        let allocatorValue = await services.seqAllocator.value()
        XCTAssertEqual(allocatorValue, 0)
        let next = try await services.seqAllocator.allocate()
        XCTAssertEqual(next, 1)
        let reloaded = try await identity.loadRepoState(profileID: profile.id!, repoID: services.repoID)
        XCTAssertEqual(reloaded?.lastSeq, 1)
        await services.shutdown()
    }

    func testBuild_sanitizesNegativeLastSeqBeforeAllocatorInit() async throws {
        let canonicalRepoID = "negative-seq-repo-id"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: canonicalRepoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: canonicalRepoID, writerID: ourWriterID)
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastSeq = -1 WHERE profileID = ? AND repoID = ?",
                arguments: [profile.id!, canonicalRepoID]
            )
        }

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )

        let allocatorValue = await services.seqAllocator.value()
        XCTAssertEqual(allocatorValue, 0)
        let next = try await services.seqAllocator.allocate()
        XCTAssertEqual(next, 1)
        let reloaded = try await identity.loadRepoState(profileID: profile.id!, repoID: canonicalRepoID)
        XCTAssertEqual(reloaded?.lastSeq, 1)
        await services.shutdown()
    }

    func testSelfLivenessSweep_runsEvenWhenRenewalUnsafe() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        // Simulate SMB / SFTP: neither renewal atom is safe → supportsLivenessSafeRenewal == false.
        metadataClient.setSupportsLivenessSafeOverwriteMove(false)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)

        // Plant aged own-writer liveness staging residue on the metadata client (where
        // liveness ticks land), older than the 1h sweep threshold.
        let ownStagingName = "\(ourWriterID).json.staging-\(UUID().uuidString).tmp"
        let ownStagingPath = "\(basePath)/.watermelon/liveness/\(ownStagingName)"
        await metadataClient.injectFile(path: ownStagingPath, contents: "stranded heartbeat")
        await metadataClient.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: ownStagingPath)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false,
            onBootstrap: { }
        )
        // shutdown to stop liveness ticks before assertion (would otherwise race with new tick writes).
        await services.shutdown()

        let stillThere = await metadataClient.hasFile(ownStagingPath)
        XCTAssertFalse(stillThere,
                       "self-sweep must reclaim aged own liveness staging even on renewal-unsafe backends (SMB/SFTP) — otherwise own crash residue is immortal")
    }

    func testBuild_repairsPoisonedRepoStateRow() async throws {
        let canonicalRepoID = "poison-repair-repo"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: canonicalRepoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let ourWriterID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: canonicalRepoID, writerID: ourWriterID)

        // Plant exact-ceiling poison into repo_state (the case the legacy
        // `WHERE lastClock < ?` predicate cannot rewrite).
        let poison = LamportClock.maxAdvanceableValue
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profile.id!, canonicalRepoID]
            )
        }

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        defer { Task { await services.shutdown() } }

        let reloaded = try await identity.loadRepoState(profileID: profile.id!, repoID: canonicalRepoID)
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertNotNil(recovered)
        XCTAssertLessThan(recovered!, LamportClock.maxAdvanceableValue,
                          "builder must heal poisoned repo_state.lastClock before returning the runtime — otherwise a session with no tick activity leaves the poison in place forever")
        let lamportValue = await services.lamport.value()
        XCTAssertLessThan(lamportValue, LamportClock.maxObservableValue,
                          "actor-local mirror must also reflect a sane value after builder recovery")
    }

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }
}
