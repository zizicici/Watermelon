import XCTest
@testable import Watermelon

final class RepoCanonicalIdentityReaderTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let finalizedRepoID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let claimRepoID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    private let legacyRepoID = "cccccccc-cccc-cccc-cccc-cccccccccccc"

    private func makeClient() async -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        return client
    }

    private func installFinalized(_ client: InMemoryRemoteStorageClient, repoID: String) async throws {
        let wire = RepoIdentityFinalizationWire(
            repoID: repoID,
            formatVersion: 1,
            createdAtMs: 1_000,
            createdByWriter: writerID
        )
        let data = try wire.encode()
        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), data: data)
    }

    private func installClaim(_ client: InMemoryRemoteStorageClient, repoID: String) async throws {
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": repoID,
            "created_at_ms": 1_000,
            "writer_id": writerID
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: RepoLayout.identityClaimPath(base: basePath, writerID: writerID), data: data)
    }

    private func installLegacyCache(_ client: InMemoryRemoteStorageClient, repoID: String) async throws {
        let wire = RepoCacheWire(repoID: repoID, createdAtMs: 1_000, createdByWriter: writerID)
        let data = try wire.encode()
        await client.injectFile(path: RepoLayout.repoFilePath(base: basePath), data: data)
    }

    func testLoadCanonical_FinalizedPresent_ReturnsFinalized() async throws {
        let client = await makeClient()
        try await installFinalized(client, repoID: finalizedRepoID)
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonical()
        XCTAssertEqual(load, .found(finalizedRepoID))
    }

    func testLoadCanonical_FinalizedAbsent_ClaimPresent_ReturnsClaimCanonical() async throws {
        let client = await makeClient()
        try await installClaim(client, repoID: claimRepoID)
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonical()
        XCTAssertEqual(load, .found(claimRepoID))
    }

    func testLoadCanonical_FinalizedAbsent_ClaimAbsent_LegacyCachePresent_ReturnsAbsent() async throws {
        let client = await makeClient()
        try await installLegacyCache(client, repoID: legacyRepoID)
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonical()
        XCTAssertEqual(load, .absent)
    }

    func testLoadCanonical_AllAbsent_ReturnsAbsent() async throws {
        let client = await makeClient()
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonical()
        XCTAssertEqual(load, .absent)
    }

    func testRequireCanonical_Found_ReturnsID() async throws {
        let client = await makeClient()
        try await installFinalized(client, repoID: finalizedRepoID)
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let resolved = try await reader.requireCanonical(absentError: {
            NSError(domain: "Test", code: 0)
        })
        XCTAssertEqual(resolved, finalizedRepoID)
    }

    func testRequireCanonical_Absent_ThrowsClosureError() async throws {
        let client = await makeClient()
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        do {
            _ = try await reader.requireCanonical(absentError: {
                NSError(domain: "TestAbsent", code: -123, userInfo: [NSLocalizedDescriptionKey: "test msg"])
            })
            XCTFail("expected absent to throw the closure's NSError")
        } catch let nsError as NSError {
            XCTAssertEqual(nsError.domain, "TestAbsent")
            XCTAssertEqual(nsError.code, -123)
        }
    }

    func testRequireCanonical_FinalizedMarkerMalformed_PropagatesBootstrapError() async throws {
        let client = await makeClient()
        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), data: Data("malformed".utf8))
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        do {
            _ = try await reader.requireCanonical(absentError: {
                NSError(domain: "ShouldNotFire", code: 0)
            })
            XCTFail("expected BootstrapError to propagate")
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            if case .ioFailure(let underlying) = bootstrap {
                let nsError = underlying as NSError
                XCTAssertEqual(nsError.domain, "RepoBootstrap")
                XCTAssertEqual(nsError.code, 12)
            } else {
                XCTFail("expected BootstrapError.ioFailure, got \(bootstrap)")
            }
        }
    }
}
