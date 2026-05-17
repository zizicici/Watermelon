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
            writerID: "w",
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
                writerID: "w",
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
                writerID: "w",
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

    /// Wiped-and-reused remote: profile previously backed up to repo A, the user
    /// wiped the remote, and a later `.fresh` init created a row for repo B.
    /// `collect` must pick the exact (profileID, remoteRepoID) row instead of the
    /// stale higher-`lastSeq` row for repo A, otherwise the user gets stuck in a
    /// false `repoIdentityMismatch` loop every run.
    func testCollect_wipedAndReusedRemote_prefersExactRowOverStalePerProfileFallback() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "fresh-repo-B")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-wiped-A",
                writerID: "w", lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
            try RepoStateRecord(
                profileID: profileID, repoID: "fresh-repo-B",
                writerID: "w", lastClock: 0, lastSeq: 0,
                migrationCompleted: 0
            ).insert(db)
        }

        let sources = try await RepoIdentitySources.collect(
            profileID: profileID,
            writerID: "w",
            identity: identity,
            client: client,
            basePath: basePath,
            format: RemoteFormatCompatibilityService()
        )

        XCTAssertEqual(sources.stored, "fresh-repo-B")
        XCTAssertEqual(sources.remote, "fresh-repo-B")
        XCTAssertEqual(sources.suggested, "fresh-repo-B")
    }

    /// Partial-die wipe-and-reuse: the stale per-profile fallback for repo A is the
    /// only DB row, but our own writer's claim file at the current remote already
    /// names repo B. `collect` must treat `stored` as nil so `lazyEnsureRepoState`
    /// can write the missing (profile, B) row on the next call, rather than
    /// throwing a false mismatch the user can't recover from without re-wiping.
    func testCollect_wipedAndReusedRemote_ownClaimPresent_recoversWithoutMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "fresh-repo-B")
        // Our own claim file proves we already participated in repo B (would have
        // been written by RepoBootstrap.initializeFreshRepo before the partial die).
        try await injectOwnClaim(client: client, writerID: ownWriterID, repoID: "fresh-repo-B")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-wiped-A",
                writerID: ownWriterID, lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
        }

        let sources = try await RepoIdentitySources.collect(
            profileID: profileID,
            writerID: ownWriterID,
            identity: identity,
            client: client,
            basePath: basePath,
            format: RemoteFormatCompatibilityService()
        )

        XCTAssertNil(sources.stored, "own claim authorizes ignoring the stale per-profile fallback")
        XCTAssertEqual(sources.remote, "fresh-repo-B")
        XCTAssertEqual(sources.suggested, "fresh-repo-B")
    }

    /// Same wipe-and-reuse setup but without our own claim — must still throw mismatch.
    /// Guards against weakening the foreign-repo guard when narrowing the recovery rule.
    func testCollect_wipedAndReusedRemote_noOwnClaim_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "fresh-repo-B")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-wiped-A",
                writerID: ownWriterID, lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
        }

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                writerID: ownWriterID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected repoIdentityMismatch without own claim")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
            XCTAssertEqual(stored, "old-wiped-A")
            XCTAssertEqual(observed, "fresh-repo-B")
        }
    }

    /// Wipe-and-reuse setup with a foreign writer's claim only — must still throw
    /// mismatch. The recovery rule must consult OUR claim file, not any claim.
    func testCollect_wipedAndReusedRemote_foreignClaimOnly_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let foreignWriterID = "22222222-2222-2222-2222-bbbbbbbbbbbb"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "fresh-repo-B")
        // Foreign peer participated in repo B but we did not.
        try await injectOwnClaim(client: client, writerID: foreignWriterID, repoID: "fresh-repo-B")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-wiped-A",
                writerID: ownWriterID, lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
        }

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                writerID: ownWriterID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected repoIdentityMismatch — foreign claim must not authorize own recovery")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch {
            // expected
        }
    }

    /// Wipe-and-reuse setup where our claim points to a different repoID than the
    /// canonical remote. Realistic shape: a peer's lex-min-earlier claim wins
    /// canonical election (remote = "B"), but our writer's stale claim still says
    /// "A". The recovery branch must reject this — only an own claim that matches
    /// the canonical remote authorizes ignoring the stale per-profile fallback.
    func testCollect_wipedAndReusedRemote_ownClaimNamesWrongRepo_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let peerWriterID = "00000000-0000-0000-0000-000000000001"  // lex-min < own → wins election
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "fresh-repo-B")
        // Peer's claim wins canonical (lower writerID under equal createdAtMs).
        try await injectOwnClaim(client: client, writerID: peerWriterID, repoID: "fresh-repo-B")
        // Our own writer's claim still names the stale repo.
        try await injectOwnClaim(client: client, writerID: ownWriterID, repoID: "old-wiped-A")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-wiped-A",
                writerID: ownWriterID, lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
        }

        do {
            _ = try await RepoIdentitySources.collect(
                profileID: profileID,
                writerID: ownWriterID,
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected repoIdentityMismatch — own claim for a different repo must not authorize recovery")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch {
            // expected
        }
    }

    private func injectOwnClaim(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        repoID: String
    ) async throws {
        let body: [String: Any] = [
            "v": 1,
            "repo_id": repoID,
            "created_at_ms": Int64(0),
            "writer_id": writerID
        ]
        let data = try JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: RepoLayout.identityClaimPath(base: basePath, writerID: writerID), data: data)
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
                writerID: "w",
                identity: identity,
                client: client,
                basePath: basePath,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
            XCTAssertEqual(stored, "stored-id")
            XCTAssertEqual(observed, "remote-id")
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
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
            XCTAssertEqual(stored, "stale-local")
            XCTAssertEqual(observed, finalizedID)
        }
    }
}
