import XCTest
@testable import Watermelon

/// `RepoIdentitySources` consolidates the three identity-triangulation paths the
/// builder/migration used to fragment across. Tests pin the precedence rules,
/// damaged-V2 detection, mismatch semantics, and publish idempotence so future
/// refactors can't silently drop a check.
final class RepoIdentitySourcesTests: XCTestCase {
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

    // MARK: - collect

    func testCollect_storedOnly_suggestionIsStored() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "stored-id", writerID: "w")

        let sources = try await RepoIdentitySources.collect(
            profileID: profileID,
            identity: identity,
            client: client,
            basePath: basePath,
            format: RemoteFormatCompatibilityService()
        )

        XCTAssertEqual(sources.stored, "stored-id")
        XCTAssertNil(sources.remote)
        XCTAssertNil(sources.data)
        XCTAssertEqual(sources.suggested, "stored-id")
    }

    /// Files in `.watermelon/commits` that can't be parsed back to a repoID mean
    /// V2 data exists but identity is unrecoverable — must fail loud, not mint fresh.
    func testCollect_unparseableV2CommitData_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(
            path: RepoLayout.normalize(joining: [RepoLayout.commitsDirectoryPath(base: basePath), "stray.jsonl"]),
            data: Data("not a commit".utf8)
        )

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected
        }
    }

    /// Two commits carrying different repoIDs means the data dir was written by
    /// two distinct repos — minting either would orphan the other. Must fail loud.
    func testCollect_multipleDistinctRepoIDsInV2Data_throwsDamagedV2Repo() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))

        let month = LibraryMonthKey(year: 2025, month: 6)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let writerA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        let writerB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        let opA = CommitOp(opSeq: 0, clock: 1, body: .tombstoneAsset(
            CommitTombstoneBody(assetFingerprint: TestFixtures.fingerprint(0x01), reason: .verifyFailed, observedBasis: nil)
        ))
        let opB = CommitOp(opSeq: 0, clock: 1, body: .tombstoneAsset(
            CommitTombstoneBody(assetFingerprint: TestFixtures.fingerprint(0x02), reason: .verifyFailed, observedBasis: nil)
        ))
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(repoID: "id-A", writerID: writerA, seq: 1, runID: "r", month: month),
            ops: [opA],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(repoID: "id-B", writerID: writerB, seq: 1, runID: "r", month: month),
            ops: [opB],
            month: month,
            respectTaskCancellation: false
        )

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected damagedV2Repo")
        } catch BackupV2RuntimeBuildError.damagedV2Repo {
            // expected
        }
    }

    /// Stored DB repoID disagreeing with remote `repo.json` claim means the profile
    /// was re-pointed at a foreign remote — must throw, not silently write commits.
    func testCollect_storedDisagreesWithRemote_throwsMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "remote-id")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "stored-id", writerID: "w")

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let local, let remote) {
            XCTAssertEqual(local, "stored-id")
            XCTAssertEqual(remote, "remote-id")
        }
    }

    // MARK: - publish

    /// Once finalized, `ensureRepoJSON` reads the finalized id back — publish must
    /// return that id rather than the caller's `suggested`.
    func testPublish_existingFinalization_returnsFinalizedID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        _ = try await bootstrap.initializeFreshRepo(writerID: "w")
        let loaded = try await bootstrap.loadRepoID()
        let finalizedID = try XCTUnwrap(loaded)

        // Caller hands in a different suggestion; publish must adopt the finalized id.
        let sources = RepoIdentitySources(stored: nil, remote: nil, data: nil, suggested: "ignored-suggestion")
        let resolved = try await sources.publish(bootstrap: bootstrap, writerID: "w")

        XCTAssertEqual(resolved, finalizedID, "publish must read finalized id, not adopt suggested")
    }

    /// If `sources.stored` disagrees with the id `ensureRepoJSON` returns (e.g.,
    /// remote pre-finalized to a different id), publish must throw rather than
    /// silently bind to a stale local id.
    func testPublish_storedDisagreesWithResolvedAfterEnsureRepoJSON_throwsMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        _ = try await bootstrap.initializeFreshRepo(writerID: "peer")
        let loaded = try await bootstrap.loadRepoID()
        let finalizedID = try XCTUnwrap(loaded)

        let sources = RepoIdentitySources(stored: "stale-local", remote: nil, data: nil, suggested: "stale-local")
        do {
            _ = try await sources.publish(bootstrap: bootstrap, writerID: "w")
            XCTFail("expected repoIdentityMismatch")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let local, let remote) {
            XCTAssertEqual(local, "stale-local")
            XCTAssertEqual(remote, finalizedID)
        }
    }
}
