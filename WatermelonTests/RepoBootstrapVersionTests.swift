import XCTest
@testable import Watermelon

/// `ensureVersionJSON`'s collision-recovery path verifies the remote version is
/// compatible. Higher format → back off (`unsupportedRemoteFormat`); read failure
/// → unreadable (never silently treated as compatible).
final class RepoBootstrapVersionTests: XCTestCase {
    private let basePath = "/repo"

    func testFreshRepo_writesBothJSONFiles() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)

        let resolvedID = try await bootstrap.initializeFreshRepo(writerID: "writer-A")
        XCTAssertFalse(resolvedID.isEmpty)
        let repoExists = await client.hasFile(RepoLayout.repoFilePath(base: basePath))
        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(repoExists)
        XCTAssertTrue(versionExists)
    }

    func testEnsureVersion_alreadyExistsHigherFormat_throwsHigherFormatVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9", writerID: "future"
        )

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            try await bootstrap.ensureVersionJSON(writerID: "us")
            XCTFail("expected higherFormatVersion")
        } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, let local, let minApp) {
            XCTAssertEqual(remote, 99)
            XCTAssertEqual(local, RepoLayout.formatVersion)
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testEnsureVersion_alreadyExistsUnreadable_throwsUnreadable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: Data("not json at all".utf8))

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            try await bootstrap.ensureVersionJSON(writerID: "us")
            XCTFail("expected unreadable")
        } catch RepoBootstrap.VersionConflict.unreadable {
            // expected
        }
    }

    func testLoadRepoIDStrict_absent_returnsAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let result = try await bootstrap.loadRepoIDStrict()
        guard case .absent = result else {
            XCTFail("expected .absent, got \(result)")
            return
        }
    }

    func testLoadRepoIDStrict_malformed_throws() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bad: [String: Any] = ["v": 1]
        let data = try JSONSerialization.data(withJSONObject: bad)
        await client.injectFile(path: RepoLayout.repoFilePath(base: basePath), data: data)

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            _ = try await bootstrap.loadRepoIDStrict()
            XCTFail("expected throw on malformed repo.json")
        } catch is RepoBootstrap.BootstrapError {
            // expected
        }
    }

    func testEnsureSubdirectories_createDirectoryURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectCreateDirectoryURLErrorCancelled(for: RepoLayout.commitsDirectoryPath(base: basePath))

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            try await bootstrap.ensureSubdirectories()
            XCTFail("expected CancellationError from URL-shaped createDirectory cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }
}
