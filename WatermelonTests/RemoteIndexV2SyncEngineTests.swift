import CryptoKit
import XCTest
@testable import Watermelon

final class RemoteIndexV2SyncEngineTests: XCTestCase {
    private let basePath = "/repo"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    func testPreMaterializedOutputBypassesRepoIdentityRead() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let bytes = Data(repeating: 0, count: 100)
        let hash = Data(SHA256.hash(data: bytes))
        let fingerprint = TestFixtures.computedFingerprint(for: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: hash)
        ])
        _ = try await builder.addAsset(month: month, fingerprint: fingerprint, contentHash: hash, fileSize: Int64(bytes.count))
        let preMaterialized = try await builder.materialize()

        let absentIdentityClient = InMemoryRemoteStorageClient()
        try await absentIdentityClient.connect()

        let output = try await RemoteIndexV2SyncEngine().materialize(
            client: absentIdentityClient,
            basePath: basePath,
            preMaterialized: preMaterialized
        )

        XCTAssertEqual(output.state.months.keys, preMaterialized.state.months.keys)
    }

    func testNilPreMaterializedWithAbsentIdentityThrowsSyncError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: client,
                basePath: basePath,
                preMaterialized: nil
            )
            XCTFail("expected absent identity to throw")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "RemoteIndexSyncService")
            XCTAssertEqual(error.code, -50)
        }
    }

    func testNilPreMaterializedLoadsIdentityAndRunsMaterializeWithoutThrowing() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee")
        let bytes = Data(repeating: 0, count: 100)
        let hash = Data(SHA256.hash(data: bytes))
        let fingerprint = TestFixtures.computedFingerprint(for: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: hash)
        ])
        _ = try await builder.addAsset(month: month, fingerprint: fingerprint, contentHash: hash, fileSize: Int64(bytes.count))

        let output = try await RemoteIndexV2SyncEngine().materialize(
            client: builder.client,
            basePath: basePath,
            preMaterialized: nil
        )

        XCTAssertTrue(output.state.months.isEmpty)
    }

    func testMaterializeThrowsIdentityMismatchWhenLocalRepoIDDiffers() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builder.client,
                basePath: basePath,
                preMaterialized: nil,
                localRepoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupCompatibilityError.repoIdentityMismatch {
            // expected
        }
    }

    func testMaterializeMatchingLocalRepoIDDoesNotThrow() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        _ = try await RemoteIndexV2SyncEngine().materialize(
            client: builder.client,
            basePath: basePath,
            preMaterialized: nil,
            localRepoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
    }

    func testMaterializeNilLocalRepoIDSkipsGuard() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        _ = try await RemoteIndexV2SyncEngine().materialize(
            client: builder.client,
            basePath: basePath,
            preMaterialized: nil,
            localRepoID: nil
        )
    }

    func testMaterializePreMaterialized_mismatchedRepoID_throwsIdentityMismatch() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let preMaterialized = try await builder.materialize()

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builder.client,
                basePath: basePath,
                preMaterialized: preMaterialized,
                localRepoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            XCTFail("expected repoIdentityMismatch")
        } catch BackupCompatibilityError.repoIdentityMismatch {
            // expected
        }
    }

    func testMaterializePreMaterialized_matchingRepoIDSucceeds() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let preMaterialized = try await builder.materialize()

        _ = try await RemoteIndexV2SyncEngine().materialize(
            client: builder.client,
            basePath: basePath,
            preMaterialized: preMaterialized,
            localRepoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
    }

    func testMaterializePreMaterialized_remoteSwapped_throwsIdentityMismatch() async throws {
        let builderA = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let preMaterializedA = try await builderA.materialize()
        // Simulate remote swap: overwrite repo.json with repo-b
        try await TestFixtures.injectRepoJSON(builderA.client, basePath: basePath, repoID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee")

        do {
            _ = try await RemoteIndexV2SyncEngine().materialize(
                client: builderA.client,
                basePath: basePath,
                preMaterialized: preMaterializedA,
                localRepoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            XCTFail("expected repoIdentityMismatch after remote swap")
        } catch BackupCompatibilityError.repoIdentityMismatch {
            // expected
        }
    }

    func testMaterializePreMaterialized_nilLocalRepoID_skipsGuard() async throws {
        let builder = try await RepoTestBuilder.freshRepo(basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let preMaterialized = try await builder.materialize()

        _ = try await RemoteIndexV2SyncEngine().materialize(
            client: builder.client,
            basePath: basePath,
            preMaterialized: preMaterialized,
            localRepoID: nil
        )
    }
}
