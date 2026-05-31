import XCTest
@testable import Watermelon

final class RepoCanonicalIdentityReaderTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let finalizedRepoID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let claimRepoID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

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

    func testLoadCanonicalProvenV2_GraceBackend_IdentityMetadataLag_RetriesUntilFinalizedAppears() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(5)
        try await installFinalized(client, repoID: finalizedRepoID)
        // Finalization marker's metadata is lagging behind the already-visible V2 format marker on
        // the first read, so the strict load reports `.absent`; it clears (single-shot) and the
        // marker becomes visible on the next read.
        await client.injectMetadataError(.notFound, for: RepoLayout.identityFinalizationFilePath(base: basePath))
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonicalProvenV2()
        XCTAssertEqual(load, .found(finalizedRepoID))
    }

    func testLoadCanonicalProvenV2_GraceBackend_IdentityMetadataLag_ClaimAppears() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(5)
        try await installClaim(client, repoID: claimRepoID)
        // No finalized marker; the identity claim directory listing 404s first (visibility lag) then
        // resolves — proven-V2 retry must elect the claim rather than report deterministic absence.
        await client.injectListError(.notFound, for: RepoLayout.identityDirectoryPath(base: basePath))
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonicalProvenV2()
        XCTAssertEqual(load, .found(claimRepoID))
    }

    func testLoadCanonicalProvenV2_ZeroGraceBackend_GenuineAbsence_ReturnsAbsentWithoutPolling() async throws {
        let client = await makeClient()
        // grace defaults to 0; proven-V2 must not introduce any delay on a zero-grace backend.
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let start = Date()
        let load = try await reader.loadCanonicalProvenV2()
        XCTAssertEqual(load, .absent)
        XCTAssertLessThan(Date().timeIntervalSince(start), 0.5)
    }

    func testLoadCanonicalProvenV2_GraceBackend_PersistentAbsence_ReturnsAbsentAfterDeadline() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(1)
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonicalProvenV2()
        XCTAssertEqual(load, .absent)
    }

    // Bug-IX P06 R25 Finding B: a malformed claim must not abort the finalized-marker grace retry.
    func testLoadCanonicalProvenV2_GraceBackend_MalformedClaim_FinalizedHidden_RetriesAndSucceeds() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(5)
        try await installFinalized(client, repoID: finalizedRepoID)
        await client.injectMetadataError(.notFound, for: RepoLayout.identityFinalizationFilePath(base: basePath))
        // Directory-shaped .json in identity dir triggers malformedMetadataDirectoryError from
        // claim election. The retry must swallow this and keep retrying the finalized marker.
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        let childPath = (claimPath as NSString).appendingPathComponent("subfile")
        await client.injectFile(path: childPath, contents: "garbage")
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        let load = try await reader.loadCanonicalProvenV2()
        XCTAssertEqual(load, .found(finalizedRepoID))
    }

    // Bug-IX P06 R25 Finding B: malformed claim with no finalized marker still throws after grace.
    func testLoadCanonicalProvenV2_GraceBackend_MalformedClaim_NoFinalized_ThrowsAfterGrace() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(1)
        // Directory-shaped .json in identity dir triggers malformedMetadataDirectoryError.
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        let childPath = (claimPath as NSString).appendingPathComponent("subfile")
        await client.injectFile(path: childPath, contents: "garbage")
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        do {
            _ = try await reader.loadCanonicalProvenV2()
            XCTFail("expected malformed claim to throw after grace deadline")
        } catch {
            // Expected: claim error surfaces after grace budget is spent on finalized marker.
        }
    }

    func testLoadCanonicalProvenV2_MalformedMarker_StaysFailClosed() async throws {
        let client = await makeClient()
        await client.setReadAfterWriteGrace(5)
        await client.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), data: Data("malformed".utf8))
        let reader = RepoCanonicalIdentityReader(client: client, basePath: basePath)
        do {
            _ = try await reader.loadCanonicalProvenV2()
            XCTFail("expected malformed marker to throw rather than be retried to absence")
        } catch let bootstrap as RepoBootstrap.BootstrapError {
            if case .ioFailure(let underlying) = bootstrap {
                XCTAssertEqual((underlying as NSError).code, 12)
            } else {
                XCTFail("expected BootstrapError.ioFailure, got \(bootstrap)")
            }
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
