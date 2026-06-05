import XCTest
@testable import Watermelon

final class BackupV2RuntimeBuilderTests: XCTestCase {
    private let basePath = "/repo"
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    // Tracked so tearDown awaits shutdown AND releases the services reference
    // before unlinking the DB file — otherwise SQLite warns "vnode unlinked while in use".
    private var activeServices: BackupV2RuntimeServices?

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDown() async throws {
        if let services = activeServices {
            await services.shutdown()
            activeServices = nil
        }
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
        let identityExists = await client.hasFile(RepoLayout.identityFinalizationFilePath(base: basePath))
        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(identityExists)
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: canonicalRepoID)
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "cccccccc-1111-2222-3333-444444444444")
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

    func testV2WithV1Manifests_corruptCanonicalIdentity_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // Malformed canonical identity forces BootstrapError.ioFailure before migration starts.
        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), contents: "{not-json")
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

    func testV2Repo_nonUUIDCanonicalIdentity_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            contents: #"{"v":1,"repo_id":"not-a-uuid","format_version":2}"#
        )
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

    func testV2Repo_versionOnlyBootstrapsFreshCanonicalIdentity() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let preReleaseRepoID = "aaaaaaaa-1111-2222-3333-444444444444"
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
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

        XCTAssertNotEqual(services.repoID, preReleaseRepoID)
        let finalizedExists = await client.hasFile(RepoLayout.identityFinalizationFilePath(base: basePath))
        XCTAssertTrue(finalizedExists)
        await services.shutdown()
    }

    func testV2Repo_identityReadCancellationPropagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        try await TestFixtures.injectIdentityFinalization(
            client,
            basePath: basePath,
            repoID: "cccccccc-1111-2222-3333-444444444444"
        )
        await client.injectDownloadCancellation(for: RepoLayout.identityFinalizationFilePath(base: basePath))
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

    func testFreshArm_emptyWatermelonDir_freshBootstrapSucceeds() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory])
        )
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
        XCTAssertFalse(services.repoID.isEmpty)
        await services.shutdown()
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
        await secondRun.shutdown()
    }

    func testV2WithPendingMigrationCleanup_corruptCanonicalIdentity_throwsDamagedV2Repo() async throws {
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

        // Pin the inspect route BEFORE injecting the malformed marker — inspect
        // doesn't read the marker file, so this stays clean. If a future refactor changes
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

        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), contents: "{not-json")

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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: canonicalRepoID)
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
            assetFingerprint: AssetFingerprint(decoding: Data(repeating: 0xAB, count: 32))!,
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
                        assetFingerprint: AssetFingerprint(decoding: Data(repeating: 0xAC, count: 32))!,
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
                        assetFingerprint: AssetFingerprint(decoding: Data(repeating: 0xAD, count: 32))!,
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: canonicalRepoID)
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

    func testBuild_repairsPoisonedRepoStateRow() async throws {
        let canonicalRepoID = "44444444-aaaa-bbbb-cccc-dddddddddddd"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: canonicalRepoID)
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
        activeServices = services

        let reloaded = try await identity.loadRepoState(profileID: profile.id!, repoID: canonicalRepoID)
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertNotNil(recovered)
        XCTAssertLessThan(recovered!, LamportClock.maxAdvanceableValue,
                          "builder must heal poisoned repo_state.lastClock before returning the runtime — otherwise a session with no tick activity leaves the poison in place forever")
        let lamportValue = await services.lamport.value()
        XCTAssertLessThan(lamportValue, LamportClock.maxAdoptableValue,
                          "actor-local mirror must also reflect a sane value after builder recovery")
    }

    // MARK: - Startup maintenance diagnostics

    func testBuild_exposesStartupMaintenanceDiagnostic() async throws {
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
        activeServices = services
        let diagnostic = try XCTUnwrap(services.startupMaintenanceDiagnostic,
            "build must expose the startup maintenance diagnostic")
        XCTAssertTrue(diagnostic.ran, "default enabled mode runs startup maintenance")
        XCTAssertNil(diagnostic.failureStage, "a clean fresh repo open must record no failure")
    }

    func testBuild_disabledMaintenance_recordsNoOpDiagnostic() async throws {
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
            maintenanceStartupMode: .disabled(.test),
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        activeServices = services
        let diagnostic = try XCTUnwrap(services.startupMaintenanceDiagnostic)
        XCTAssertEqual(diagnostic.mode, .disabled(.test))
        XCTAssertFalse(diagnostic.ran, "disabled startup maintenance must record a no-op diagnostic")
    }

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }

    // MARK: - verifyMonthV2 identity guard

    func testVerifyMonthV2_throwsIdentityMismatchWhenLocalRepoIDDiffers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "55555555-aaaa-bbbb-cccc-dddddddddddd")
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID)
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "77777777-aaaa-bbbb-cccc-dddddddddddd")
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x31)
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "88888888-aaaa-bbbb-cccc-dddddddddddd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = try insertProfile()
        // No repo_state — nil-binding profile

        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(client: client, profile: profile, localRepoID: nil)
        let cachedID = await remoteIndexService.materializedRepoID()
        XCTAssertEqual(cachedID, "88888888-aaaa-bbbb-cccc-dddddddddddd", "precondition: materializedRepoID should be set")

        // Swap canonical identity to a different repo ID (simulates peer swap or corruption)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "99999999-aaaa-bbbb-cccc-dddddddddddd")

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
        XCTAssertEqual(result, .clean, "fresh remote with no local binding is skipped → clean")
    }

    func testVerifyMonth_nilBinding_materializedRepoID_freshRemote_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "aaaaaaaa-aaaa-bbbb-cccc-dddddddddddd")
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: "aaaaaaaa-aaaa-bbbb-cccc-dddddddddddd")
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

    // MARK: - verifyMonth V2 outcome

    func testVerifyMonth_v2Repo_healthyMonthIsClean() async throws {
        let repoID = "11111111-2222-3333-4444-555555555555"
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID)
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

        let outcome = try await service.verifyMonth(
            client: client, basePath: basePath,
            month: LibraryMonthKey(year: 2026, month: 5),
            profile: profile
        )
        // A clean V2 month with no damage and no applied cleanup classifies .clean.
        XCTAssertEqual(outcome, .clean, "healthy V2 verify with no cleanup → clean")
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
