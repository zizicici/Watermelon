import XCTest
@testable import Watermelon

/// `BackupV2RuntimeBuilder.build` is the single entry point routing the 4 remote
/// formats (.fresh / .v1 / .v2 / .unsupported) and ensuring repo identity is
/// canonical before any commits land. Tests pin the routing so regressions can't
/// silently bypass identity-mismatch detection or migration gates.
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

    // The old "V1 after migrationCompleted → throws repoFormatRegression" path was
    // removed: lingering V1 manifests now trigger idempotent phase1+2+3 re-migration
    // instead, because the same condition is hit by an older V1-only peer writing
    // into a V2 repo. End-to-end coverage of the re-migration path lives in
    // V1MigrationServiceTests (phase1 idempotency, phase3 scoped to its scan).

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

    /// User re-pointed the profile at a different remote; local DB still has the
    /// old repo's id. Builder must throw rather than write commits under our local
    /// id (foreign to remote).
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

    /// `.v2WithV1Manifests` arm: a V2-shaped repo with V1 manifest residue and a
    /// malformed `.watermelon/repo.json` must surface as `damagedV2Repo` — same
    /// actionable diagnosis as the `.v2` arm. Locks in the joint-case catch at
    /// BackupV2RuntimeBuilder.swift:164-169 so a future refactor that drops the
    /// catch (or narrows the joint case) is caught by tests.
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

    /// `.fresh` arm: marker directory present, no version.json, no V1/V2 data,
    /// but `.watermelon/repo.json` is malformed. Inspect classifies `.fresh`
    /// (absent version.json + no V1/V2 data); `initializeFreshRepo →
    /// ensureRepoJSON → loadRepoJSONStrict` then throws
    /// `BootstrapError.ioFailure`. Pins parity with the `.v2` and
    /// `.v2WithV1Manifests` arms so a future refactor that drops the catch is
    /// caught by tests.
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

    /// V2 path runs ensureRepoJSON to repair a half-bootstrap state where
    /// version.json exists but repo.json was lost.
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

    /// `.v2WithPendingMigrationCleanup` arm: V2-shaped repo with a stale migration
    /// marker AND a malformed `.watermelon/repo.json`. RepoIdentitySources.collect
    /// → bootstrap.loadRepoID → loadRepoJSONStrict throws `BootstrapError.ioFailure`.
    /// The arm's catch must remap to `damagedV2Repo` so the user sees the actionable
    /// compatibility diagnosis instead of the raw enum render. Pins parity with the
    /// `.v2` / `.v2WithV1Manifests` / `.fresh` arms whose mappings already have
    /// direct tests.
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

    /// Inspect-side malformed `version.json` (marker dir present) throws
    /// `BackupCompatibilityError.damagedV2Repo` from `inspectRemoteFormat`
    /// directly. The builder's wrap at `BackupV2RuntimeBuilder.swift:32-40`
    /// must remap to `BackupV2RuntimeBuildError.damagedV2Repo` so caller
    /// catch-arms (BackupRunPreparation / BackgroundBackupRunner) route
    /// through their typed-error handlers instead of the generic catch.
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

    /// Regression: SeqAllocator must observe our writer's remote max only.
    /// Pre-fix, builder took `observedSeqByWriter.values.max()`, which would
    /// bump our local seq to a peer's high-water mark — wasting seq density
    /// and producing non-contiguous commit filenames for our writer (the
    /// `(writerID, seq)` path is per-writer, never shared).
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

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }
}
