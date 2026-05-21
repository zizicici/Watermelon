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
        XCTAssertEqual(services.compactionPolicy, .default)
        XCTAssertFalse(services.isLocalVolume)
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-1111-2222-3333-444444444444")
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

    func testFutureIdentityWithoutVersion_throwsUnsupportedRemoteFormat() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            data: try RepoIdentityFinalizationWire(
                repoID: "aaaaaaaa-1111-2222-3333-444444444444",
                formatVersion: RepoLayout.currentSupportedFormatVersion + 1,
                createdAtMs: 0,
                createdByWriter: "peer"
            ).encode()
        )
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
            XCTAssertNil(minApp)
        }
    }

    func testV2Repo_localIDMatchesRemote_succeeds() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let canonicalRepoID = "bbbbbbbb-1111-2222-3333-444444444444"
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-1111-2222-3333-444444444444")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: "dddddddd-1111-2222-3333-444444444444", writerID: writerID)

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
            XCTAssertEqual(stored, "dddddddd-1111-2222-3333-444444444444")
            XCTAssertEqual(observed, "cccccccc-1111-2222-3333-444444444444")
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

    func testV2Repo_nonUUIDRepoJSON_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "not-a-uuid")
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
        }
    }

    func testV2Repo_identityReadCancellationPropagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-1111-2222-3333-444444444444")
        await client.injectDownloadCancellation(for: RepoLayout.repoFilePath(base: basePath))
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
            XCTFail("expected cancellation")
        } catch is CancellationError {
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
        let canonicalRepoID = "eeeeeeee-1111-2222-3333-444444444444"
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
        let canonicalRepoID = "11111111-aaaa-bbbb-cccc-dddddddddddd"
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

    func testFreshArm_existingLocalRepoState_throwsRepoFormatRegression() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        let priorRepoID = "22222222-aaaa-bbbb-cccc-dddddddddddd"
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: priorRepoID, writerID: writerID
        )

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected repoFormatRegression")
        } catch BackupV2RuntimeBuildError.repoFormatRegression(let repoID) {
            XCTAssertEqual(repoID, priorRepoID,
                           "regression guard must report the locally-bound repoID")
        }
    }

    func testFreshArm_migratedLocalRepoState_throwsRepoFormatRegression() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        let migratedRepoID = "33333333-aaaa-bbbb-cccc-dddddddddddd"
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: migratedRepoID, writerID: writerID
        )
        try await identity.setMigrationCompleted(profileID: profile.id!, repoID: migratedRepoID)

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected repoFormatRegression")
        } catch BackupV2RuntimeBuildError.repoFormatRegression(let repoID) {
            XCTAssertEqual(repoID, migratedRepoID)
        }
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

    func testRepoOpenServiceDoesNotStartMaintenance() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        let ownStagingPath = "\(basePath)/.watermelon/liveness/\(writerID).json.staging-\(UUID().uuidString).tmp"
        await metadataClient.injectFile(path: ownStagingPath, contents: "stranded heartbeat")
        await metadataClient.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: ownStagingPath)

        let opened = try await BackupV2RepoOpenService(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            format: RemoteFormatCompatibilityService(),
            allowMigration: false
        ).open()
        try await Task.sleep(for: .milliseconds(150))

        XCTAssertEqual(opened.writerID, writerID)
        let heartbeatExists = await metadataClient.hasFile(RepoLayout.livenessFilePath(base: basePath, writerID: writerID))
        let stagingExists = await metadataClient.hasFile(ownStagingPath)
        XCTAssertFalse(heartbeatExists)
        XCTAssertTrue(stagingExists)
    }

    func testDisabledMaintenanceModeIsQuietAndShutdownStillDisconnects() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        let ownStagingPath = "\(basePath)/.watermelon/liveness/\(writerID).json.staging-\(UUID().uuidString).tmp"
        await metadataClient.injectFile(path: ownStagingPath, contents: "stranded heartbeat")
        await metadataClient.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: ownStagingPath)

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            maintenanceStartupMode: .disabled(.test),
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        try await Task.sleep(for: .milliseconds(150))

        let heartbeatExists = await metadataClient.hasFile(RepoLayout.livenessFilePath(base: basePath, writerID: writerID))
        let stagingExists = await metadataClient.hasFile(ownStagingPath)
        XCTAssertFalse(heartbeatExists)
        XCTAssertTrue(stagingExists)
        await services.shutdown()
        let disconnectCount = await metadataClient.disconnectCount
        XCTAssertEqual(disconnectCount, 1)
    }

    func testStartupRetentionCancellationShutsDownOwnedMetadataClient() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        await metadataClient.injectListWrappedURLCancellation(for: RepoLayout.retentionDirectoryPath(base: basePath))
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            XCTFail("expected startup retention cancellation")
        } catch is CancellationError {
        }

        let disconnectCount = await metadataClient.disconnectCount
        XCTAssertEqual(disconnectCount, 1)
    }

    func testMaintenanceSelfSweepCancellationAbortsBeforeLivenessStart() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        await metadataClient.injectListWrappedURLCancellation(
            for: RepoLayout.livenessDirectoryPath(base: basePath)
        )

        do {
            let services = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            await services.shutdown()
            XCTFail("expected maintenance self-sweep cancellation")
        } catch is CancellationError {
        }
        try await Task.sleep(for: .milliseconds(150))

        let heartbeatExists = await metadataClient.hasFile(RepoLayout.livenessFilePath(base: basePath, writerID: writerID))
        XCTAssertFalse(heartbeatExists)
    }

    func testMaintenancePeerStatusCancellationStopsLivenessAndAborts() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()
        await metadataClient.injectListWrappedURLCancellation(
            for: RepoLayout.livenessDirectoryPath(base: basePath),
            onAttempt: 2
        )

        do {
            let services = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
            await services.shutdown()
            XCTFail("expected maintenance peer-status cancellation")
        } catch is CancellationError {
        }
    }

    func testRetentionCapabilityHeartbeatRequiresSafeRenewal() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        defer { Task { await services.shutdown() } }

        let body = try await waitForHeartbeat(client: metadataClient, writerID: services.writerID)
        let heartbeat = try LivenessHeartbeat.decode(body)
        XCTAssertEqual(
            heartbeat.retention,
            RetentionPeerCapability(barrierAwareSessionRefresh: true, checkpointBarrierHook: true)
        )
    }

    func testRetentionCapabilityHeartbeatWithheldWhenRenewalUnsafe() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        metadataClient.setSupportsLivenessSafeOverwriteMove(false)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        defer { Task { await services.shutdown() } }

        let body = try await waitForHeartbeat(client: metadataClient, writerID: services.writerID)
        let heartbeat = try LivenessHeartbeat.decode(body)
        let text = String(decoding: body, as: UTF8.self)
        XCTAssertNil(heartbeat.retention)
        XCTAssertFalse(text.contains("retention"))
    }

    func testBuild_repairsPoisonedRepoStateRow() async throws {
        let canonicalRepoID = "44444444-aaaa-bbbb-cccc-dddddddddddd"
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
        XCTAssertLessThan(lamportValue, LamportClock.maxAdoptableValue,
                          "actor-local mirror must also reflect a sane value after builder recovery")
    }

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }

    private func waitForHeartbeat(
        client: InMemoryRemoteStorageClient,
        writerID: String
    ) async throws -> Data {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        for _ in 0..<40 {
            if let body = await client.snapshotFiles()[path] {
                return body
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTFail("heartbeat was not written")
        return Data()
    }

    // MARK: - verifyMonthV2 identity guard

    func testVerifyMonthV2_throwsIdentityMismatchWhenLocalRepoIDDiffers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "55555555-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let profile = try insertProfile()

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: "66666666-aaaa-bbbb-cccc-dddddddddddd", writerID: writerID
        )

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )

        do {
            _ = try await service.verifyMonthV2(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupCompatibilityError.repoIdentityMismatch {
            // expected
        }
    }

    func testVerifyMonthV2_matchingLocalRepoIDSucceeds() async throws {
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let profile = try insertProfile()

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: repoID, writerID: writerID
        )

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )

        let report = try await service.verifyMonthV2(
            client: client,
            basePath: basePath,
            month: LibraryMonthKey(year: 2026, month: 5),
            profile: profile
        )
        XCTAssertTrue(report.reportOnly.isEmpty)
    }

    func testVerifyMonthV2_nilProfileSkipsIdentityGuard() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "77777777-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )

        let report = try await service.verifyMonthV2(
            client: client,
            basePath: basePath,
            month: LibraryMonthKey(year: 2026, month: 5)
        )
        XCTAssertTrue(report.reportOnly.isEmpty)
    }

    func testVerifyMonthV2_cleanupRuntimeProfileMissingIDMapsAndDoesNotDisconnectBorrowedClient() async throws {
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let writerID = "11111111-1111-1111-1111-111111111111"
        let month = LibraryMonthKey(year: 2026, month: 5)
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.fingerprint(0x31)
        let hash = TestFixtures.fingerprint(0xA1)
        let body = CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: "2026/05/missing.jpg",
                    logicalName: "missing.jpg",
                    contentHash: hash,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID,
            writerID: writerID,
            seq: 1,
            runID: "run",
            month: month,
            clockMin: 1,
            clockMax: 1
        )
        _ = try await commitWriter.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: false
        )
        try await client.createDirectory(path: "\(basePath)/2026/05")
        let profile = TestFixtures.makeServerProfile(id: nil, storageType: .webdav, basePath: basePath)
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )

        do {
            _ = try await service.verifyMonthV2(
                client: client,
                basePath: basePath,
                month: month,
                profile: profile
            )
            XCTFail("expected profileMissingID compatibility mapping")
        } catch {
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, "BackupRunPreparation")
            XCTAssertEqual(nsError.code, -90)
        }
        let disconnectCount = await client.disconnectCount
        XCTAssertEqual(disconnectCount, 0)
    }

    func testVerifyMonthV2_nilBinding_cachedRepoIDMismatch_throwsIdentityMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "88888888-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = try insertProfile()
        // No repo_state — nil-binding profile

        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(client: client, profile: profile, localRepoID: nil)
        let cachedID = await remoteIndexService.materializedRepoID()
        XCTAssertEqual(cachedID, "88888888-aaaa-bbbb-cccc-dddddddddddd", "precondition: materializedRepoID should be set")

        // Swap remote repo.json to a different repo ID (simulates peer swap or corruption)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "99999999-aaaa-bbbb-cccc-dddddddddddd")

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )
        do {
            _ = try await service.verifyMonthV2(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected repoIdentityMismatch — nil-binding with cached materializedRepoID must reject swapped remote")
        } catch BackupCompatibilityError.repoIdentityMismatch {
            // expected
        }
    }

    // MARK: - verifyMonth identity guard (non-V2 remote with local V2 binding)

    func testVerifyMonth_localV2Binding_freshRemote_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: "aaaaaaaa-cccc-dddd-eeee-ffffffffffff", writerID: writerID
        )
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )
        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testVerifyMonth_localV2Binding_v1Remote_throwsRequiresForegroundMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2026, month: 5)
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(
            profileID: profile.id!, repoID: "aaaaaaaa-cccc-dddd-eeee-ffffffffffff", writerID: writerID
        )
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )
        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected requiresForegroundMigration")
        } catch BackupCompatibilityError.requiresForegroundMigration {
            // expected
        }
    }

    func testVerifyMonth_noLocalV2Binding_freshRemote_succeeds() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let profile = try insertProfile()
        // No repo_state seeded — no local V2 binding
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )
        let result = try await service.verifyMonth(
            client: client,
            basePath: basePath,
            month: LibraryMonthKey(year: 2026, month: 5),
            profile: profile
        )
        XCTAssertFalse(result, "fresh remote with no local binding should return false")
    }

    func testVerifyMonth_nilBinding_materializedRepoID_freshRemote_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = try insertProfile()
        // No repo_state — nil-binding profile

        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(client: client, profile: profile, localRepoID: nil)
        let storedID = await remoteIndexService.materializedRepoID()
        XCTAssertEqual(storedID, "aaaaaaaa-aaaa-bbbb-cccc-dddddddddddd", "precondition: materializedRepoID should be set")

        // Wipe V2 structure so inspection returns .fresh
        try await client.delete(path: "\(basePath)/.watermelon")
        try await client.delete(path: "\(basePath)/version.json")

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )
        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected damagedV2Repo — nil-binding profile with cached materializedRepoID must reject fresh remote")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testVerifyMonth_nilBinding_materializedRepoID_v1Remote_throwsRequiresForegroundMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = try insertProfile()
        // No repo_state — nil-binding profile

        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(client: client, profile: profile, localRepoID: nil)

        // Wipe V2 structure and seed V1 manifest so inspection returns .v1
        try await client.delete(path: "\(basePath)/.watermelon")
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2026, month: 5)

        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )
        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: profile
            )
            XCTFail("expected requiresForegroundMigration — nil-binding profile with cached materializedRepoID must reject V1 remote")
        } catch BackupCompatibilityError.requiresForegroundMigration {
            // expected
        }
    }

    // MARK: - verifyMonth V2 always signals refresh

    func testVerifyMonth_v2Repo_alwaysReturnsTrue() async throws {
        let repoID = "11111111-2222-3333-4444-555555555555"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)

        let profile = try insertProfile()
        let remoteIndexService = RemoteIndexSyncService()
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )

        let needsRefresh = try await service.verifyMonth(
            client: client, basePath: basePath,
            month: LibraryMonthKey(year: 2026, month: 5),
            profile: profile
        )
        XCTAssertTrue(needsRefresh, "V2 verify must always signal refresh so Home re-projects the committed view")
    }

    // MARK: - createDirectory URL cancellation normalization

    func testBuild_basePathCreateDirectoryURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        await client.injectCreateDirectoryURLErrorCancelled(for: normalizedBase)
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
            XCTFail("expected CancellationError from URL-shaped basePath createDirectory cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testReloadRemoteIndex_basePathCreateDirectoryURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        await client.injectCreateDirectoryURLErrorCancelled(for: normalizedBase)
        let profile = try insertProfile()

        let remoteIndexService = RemoteIndexSyncService()
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService,
            databaseManager: databaseManager
        )

        do {
            _ = try await service.reloadRemoteIndex(
                client: client,
                profile: profile
            )
            XCTFail("expected CancellationError from URL-shaped basePath createDirectory cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }
}
