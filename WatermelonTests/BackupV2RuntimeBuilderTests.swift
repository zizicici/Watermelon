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
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let local, let remote) {
            XCTAssertEqual(local, "stale-local")
            XCTAssertEqual(remote, "remote-canonical")
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

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }
}
