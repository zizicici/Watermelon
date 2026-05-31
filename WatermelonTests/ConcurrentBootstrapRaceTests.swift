import XCTest
@testable import Watermelon

/// Tests for fresh repo identity finalization: direct write/read consistency,
/// conflict fail-closed behavior, and existing identity adoption.
final class ConcurrentBootstrapRaceTests: XCTestCase {
    private let basePath = "/repo"

    // MARK: - Fresh direct finalization

    func testInitializeFreshRepo_writesRepoIdentityFileAndReadbackMatchesRepoID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let generatedID = try await bootstrap.initializeFreshRepo(writerID: "writer-A")

        XCTAssertFalse(generatedID.isEmpty)
        XCTAssertNotNil(UUID(uuidString: generatedID), "returned repoID must be a valid UUID")

        let loaded = try await bootstrap.loadRepoID()
        XCTAssertEqual(loaded, generatedID,
                       "loadRepoID must return the same repoID that initializeFreshRepo generated")

        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(versionExists, "initializeFreshRepo must write version.json")
    }

    func testInitializeFreshRepo_repoIDIsLowercaseUUID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let repoID = try await bootstrap.initializeFreshRepo(writerID: "writer-A")

        XCTAssertEqual(repoID, repoID.lowercased(),
                       "repoID must be lowercase UUID")
    }

    // MARK: - Existing identity adopt

    func testSecondFreshBootstrapOnFinalizedRemote_adoptsExistingRepoID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let firstID = try await bootstrap.initializeFreshRepo(writerID: "writer-A")

        let secondID = try await bootstrap.initializeFreshRepo(writerID: "writer-B")
        XCTAssertEqual(secondID, firstID,
                       "second fresh bootstrap must adopt existing finalized repoID, not generate a new one")
    }

    func testEnsureIdentityFinalization_onRemoteWithExistingFinalization_returnsExistingID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let existingID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: existingID, writerID: "peer")

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let resolved = try await bootstrap.ensureIdentityFinalization(repoID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", writerID: "writer-A")

        XCTAssertEqual(resolved, existingID,
                       "ensureIdentityFinalization must return existing finalized ID, not the suggested one")
    }

    // MARK: - Create-time conflict (through bootstrap boundary)

    func testConflictingFinalization_createTimeRace_adoptsPeerRepoID() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()

        let peerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        let peerBytes = try RepoIdentityFinalizationWire(
            repoID: peerID,
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 1000,
            createdByWriter: "peer-writer"
        ).encode()

        // Pre-read returns nil (no file), then atomicCreate hits .alwaysAlreadyExists
        // and injects peer bytes — simulating a peer winning the create race.
        await client.setAtomicCreateMode(.alwaysAlreadyExists)
        await client.setAlwaysAlreadyExistsBaselineBytes(peerBytes)

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let suggestedID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        let resolved = try await bootstrap.ensureIdentityFinalization(
            repoID: suggestedID,
            writerID: "writer-A"
        )
        XCTAssertEqual(resolved, peerID,
                       "bootstrap must adopt peer's repoID after losing create race")
        XCTAssertNotEqual(resolved, suggestedID,
                          "bootstrap must never return local suggested ID after losing create race")

        let loaded = try await bootstrap.loadRepoID()
        XCTAssertEqual(loaded, peerID)
    }

    func testConflictingFinalization_damagedPeerMarker_failsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()

        await client.injectFile(
            path: RepoLayout.identityFinalizationFilePath(base: basePath),
            data: Data("}}corrupt{{".utf8)
        )

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let suggestedID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        do {
            let resolved = try await bootstrap.ensureIdentityFinalization(
                repoID: suggestedID,
                writerID: "writer-A"
            )
            XCTFail("bootstrap should fail-closed with damaged peer marker, but returned: \(resolved)")
        } catch is RepoBootstrap.BootstrapError {
            // Expected: ioFailure from unreadable finalized marker.
        } catch {
            XCTFail("expected RepoBootstrap.BootstrapError.ioFailure, got: \(error)")
        }

        // loadRepoID must not silently return the suggested ID.
        // It either throws (malformed marker) or returns nil — never the local suggested ID.
        do {
            let loaded = try await bootstrap.loadRepoID()
            XCTAssertNotEqual(loaded, suggestedID,
                              "damaged marker must not cause loadRepoID to return the local suggested ID")
        } catch {
            // Acceptable: malformed marker causes loadRepoID to throw.
        }
    }

    // MARK: - Fresh repo directories

    func testInitializeFreshRepo_createsWatermelonSubdirectories() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        _ = try await bootstrap.initializeFreshRepo(writerID: "writer-A")

        _ = try await client.list(path: RepoLayout.commitsDirectoryPath(base: basePath))
        _ = try await client.list(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        _ = try await client.list(path: RepoLayout.identityDirectoryPath(base: basePath))
    }
}
