import XCTest
@testable import Watermelon

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


    func testCollect_storedOnly_suggestionIsStored() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let sources = try await RepoIdentitySources.collect(
            profileID: profileID,
            writerID: "w",
            identity: identity,
            client: client,
            basePath: basePath,
            format: RemoteFormatCompatibilityService()
        )

        XCTAssertEqual(sources.stored, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        XCTAssertNil(sources.remote)
        XCTAssertNil(sources.data)
        XCTAssertEqual(sources.suggested, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    }

    func testAuthority_finalizationPrecedenceOverStaleClaimAndCache() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let finalizedRepoID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        await client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            data: try RepoIdentityFinalizationWire(
                repoID: finalizedRepoID,
                formatVersion: RepoLayout.formatVersion,
                createdAtMs: 0,
                createdByWriter: "peer"
            ).encode()
        )
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        try await injectOwnClaim(client: client, writerID: writerID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager,
            writerID: writerID,
            basePath: basePath,
            storageType: .webdav
        )
        let authority = RepoIdentityAuthority(context: RepoIdentityAuthorityContext(
            profileID: profileID,
            writerID: writerID,
            basePath: basePath,
            dataClient: client,
            identity: identity,
            format: RemoteFormatCompatibilityService()
        ))

        let resolution = try await authority.resolve()
        XCTAssertNil(resolution.stored)
        XCTAssertEqual(resolution.remote, finalizedRepoID)
        XCTAssertNil(resolution.data)
        XCTAssertEqual(resolution.suggested, finalizedRepoID)

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let resolved = try await authority.publish(resolution, using: bootstrap)

        XCTAssertEqual(resolved, finalizedRepoID)
        let loaded = try await bootstrap.loadRepoID()
        XCTAssertEqual(loaded, finalizedRepoID)
    }

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
            header: TestFixtures.makeCommitHeader(repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: writerA, seq: 1, runID: "r", month: month),
            ops: [opA],
            month: month,
            respectTaskCancellation: false
        )
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(repoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: writerB, seq: 1, runID: "r", month: month),
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

    func testCollect_wipedAndReusedRemote_prefersExactRowOverStalePerProfileFallback() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                writerID: "w", lastClock: 500, lastSeq: 200,
                migrationCompleted: 1
            ).insert(db)
            try RepoStateRecord(
                profileID: profileID, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff",
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

        XCTAssertEqual(sources.stored, "cccccccc-cccc-dddd-eeee-ffffffffffff")
        XCTAssertEqual(sources.remote, "cccccccc-cccc-dddd-eeee-ffffffffffff")
        XCTAssertEqual(sources.suggested, "cccccccc-cccc-dddd-eeee-ffffffffffff")
    }

    func testCollect_wipedAndReusedRemote_ownClaimPresent_recoversWithoutMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
        // Our own claim file proves we already participated in repo B (would have
        // been written by RepoBootstrap.initializeFreshRepo before the partial die).
        try await injectOwnClaim(client: client, writerID: ownWriterID, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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
        XCTAssertEqual(sources.remote, "cccccccc-cccc-dddd-eeee-ffffffffffff")
        XCTAssertEqual(sources.suggested, "cccccccc-cccc-dddd-eeee-ffffffffffff")
    }

    func testCollect_wipedAndReusedRemote_noOwnClaim_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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
            XCTAssertEqual(stored, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            XCTAssertEqual(observed, "cccccccc-cccc-dddd-eeee-ffffffffffff")
        }
    }

    func testCollect_wipedAndReusedRemote_foreignClaimOnly_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let foreignWriterID = "22222222-2222-2222-2222-bbbbbbbbbbbb"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
        // Foreign peer participated in repo B but we did not.
        try await injectOwnClaim(client: client, writerID: foreignWriterID, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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

    func testCollect_wipedAndReusedRemote_ownClaimNamesWrongRepo_preservesMismatch() async throws {
        let ownWriterID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let peerWriterID = "00000000-0000-0000-0000-000000000001"  // lex-min < own → wins election
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
        // Peer's claim wins canonical (lower writerID under equal createdAtMs).
        try await injectOwnClaim(client: client, writerID: peerWriterID, repoID: "cccccccc-cccc-dddd-eeee-ffffffffffff")
        // Our own writer's claim still names the stale repo.
        try await injectOwnClaim(client: client, writerID: ownWriterID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: ownWriterID, basePath: basePath, storageType: .webdav)
        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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

    func testCollect_storedDisagreesWithRemote_throwsMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee")

        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

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
            XCTAssertEqual(stored, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            XCTAssertEqual(observed, "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee")
        }
    }


    func testLoadFinalizedRepoID_futureFormatVersionThrowsTypedBootstrapError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)

        await client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            data: try RepoIdentityFinalizationWire(
                repoID: "dddddddd-dddd-dddd-dddd-dddddddddddd",
                formatVersion: RepoLayout.currentSupportedFormatVersion + 1,
                createdAtMs: 0,
                createdByWriter: "peer"
            ).encode()
        )

        do {
            _ = try await bootstrap.loadFinalizedRepoID()
            XCTFail("expected BootstrapError for future format")
        } catch let error as RepoBootstrap.BootstrapError {
            guard case .futureFormatVersion(let minAppVersion) = error else {
                return XCTFail("expected futureFormatVersion, got \(error)")
            }
            XCTAssertNil(minAppVersion)
        }
    }

    func testPublish_existingFinalization_returnsFinalizedID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        _ = try await bootstrap.initializeFreshRepo(writerID: "w")
        let loaded = try await bootstrap.loadRepoID()
        let finalizedID = try XCTUnwrap(loaded)

        // Caller hands in a different suggestion; publish must adopt the finalized id.
        let sources = RepoIdentitySources(stored: nil, remote: nil, data: nil, suggested: "aaaaaaaa-1111-2222-3333-444444444444")
        let resolved = try await sources.publish(bootstrap: bootstrap, writerID: "w")

        XCTAssertEqual(resolved, finalizedID, "publish must read finalized id, not adopt suggested")
    }

    func testPublish_storedDisagreesWithResolvedAfterEnsureRepoJSON_throwsMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        _ = try await bootstrap.initializeFreshRepo(writerID: "peer")
        let loaded = try await bootstrap.loadRepoID()
        let finalizedID = try XCTUnwrap(loaded)

        let sources = RepoIdentitySources(stored: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", remote: nil, data: nil, suggested: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        do {
            _ = try await sources.publish(bootstrap: bootstrap, writerID: "w")
            XCTFail("expected repoIdentityMismatch")
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch(let stored, let observed) {
            XCTAssertEqual(stored, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            XCTAssertEqual(observed, finalizedID)
        }
    }
}
