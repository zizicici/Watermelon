import XCTest
@testable import Watermelon

final class VersionManifestStoreTests: XCTestCase {
    private let basePath = "/repo"


    func testWriteIfAbsent_thenLoad_roundTripsAllFields() async throws {
        let (client, store) = await makeStore()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await store.writeIfAbsent(writerID: "writer-A")

        guard case .found(let manifest) = try await store.load() else {
            XCTFail("expected .found after writeIfAbsent")
            return
        }
        XCTAssertEqual(manifest.formatVersion, RepoLayout.formatVersion)
        XCTAssertEqual(manifest.minAppVersion, RepoLayout.minAppVersionPlaceholder)
        XCTAssertEqual(manifest.createdByWriter, "writer-A")
        XCTAssertNotNil(manifest.createdAtMs, "writer key `created_at_ms` must decode into createdAtMs")
        XCTAssertGreaterThan(manifest.createdAtMs ?? 0, 0)
    }

    func testWriteIfAbsent_writerKeysMatchTestFixtures() async throws {
        // Schema lock: TestFixtures.injectVersionJSON and VersionManifestStore must
        // emit the same dictionary keys so round-trip tests don't diverge silently.
        let (clientReal, store) = await makeStore()
        clientReal.setMoveIfAbsentGuarantee(.exclusive)
        try await store.writeIfAbsent(writerID: "writer-A")
        let realBytes = await clientReal.snapshotFiles()[RepoLayout.versionFilePath(base: basePath)]
        let realDict = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: try XCTUnwrap(realBytes)) as? [String: Any]
        )

        let clientFixture = InMemoryRemoteStorageClient()
        try await clientFixture.connect()
        try await TestFixtures.injectVersionJSON(clientFixture, basePath: basePath, writerID: "writer-A")
        let fixtureBytes = await clientFixture.snapshotFiles()[RepoLayout.versionFilePath(base: basePath)]
        let fixtureDict = try XCTUnwrap(
            try JSONSerialization.jsonObject(with: try XCTUnwrap(fixtureBytes)) as? [String: Any]
        )

        XCTAssertEqual(Set(realDict.keys), Set(fixtureDict.keys),
                       "writer and test fixture must agree on `version.json` key set")
    }


    func testLoad_absent_returnsAbsent() async throws {
        let (_, store) = await makeStore()
        guard case .absent = try await store.load() else {
            XCTFail("expected .absent for never-written manifest")
            return
        }
    }

    func testLoad_transportError_propagates() async throws {
        let (client, store) = await makeStore()
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: validVersionData())
        await client.injectDownloadError(.transport, for: RepoLayout.versionFilePath(base: basePath))
        do {
            _ = try await store.load()
            XCTFail("transport error must propagate, not collapse to .absent")
        } catch is RepoBootstrap.VersionConflict {
            XCTFail("transport error must surface as raw error, not VersionConflict on strict load")
        } catch {
            // expected — raw transport error
        }
    }

    func testLoad_directoryAtPath_throwsBootstrapError() async throws {
        let (client, store) = await makeStore()
        // Stash a directory at the version.json path to model damaged remote layout.
        try await client.createDirectory(path: RepoLayout.versionFilePath(base: basePath) + "/child")
        do {
            _ = try await store.load()
            XCTFail("expected damaged-shaped throw for directory at version.json")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 18)
        }
    }

    func testLoad_malformedJSON_throwsUnreadable() async throws {
        let (client, store) = await makeStore()
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: Data("not json at all".utf8))
        do {
            _ = try await store.load()
            XCTFail("expected .unreadable for malformed JSON")
        } catch RepoBootstrap.VersionConflict.unreadable {
            // expected
        }
    }

    func testLoad_formatVersionBoolean_throwsUnreadable() async throws {
        // CFBoolean bridges to NSNumber that successfully `as? Int`-casts to 1/0; strict parser must reject it.
        let (client, store) = await makeStore()
        await client.injectFile(
            path: RepoLayout.versionFilePath(base: basePath),
            data: try booleanFormatVersionData()
        )
        do {
            _ = try await store.load()
            XCTFail("expected .unreadable for `format_version: true`")
        } catch RepoBootstrap.VersionConflict.unreadable {
            // expected
        }
    }

    func testVerifyCompatible_formatVersionBoolean_throwsUnreadable() async throws {
        let (client, store) = await makeStore()
        await client.injectFile(
            path: RepoLayout.versionFilePath(base: basePath),
            data: try booleanFormatVersionData()
        )
        do {
            try await store.verifyCompatible()
            XCTFail("expected .unreadable for `format_version: true`")
        } catch RepoBootstrap.VersionConflict.unreadable {
            // expected
        }
    }

    func testLoad_createdAtMsBoolean_returnsNilCreatedAtMs() async throws {
        let (client, store) = await makeStore()
        let dict: [String: Any] = [
            "format_version": RepoLayout.formatVersion,
            "min_app_version": RepoLayout.minAppVersionPlaceholder,
            "created_at_ms": true,
            "created_by_writer": "boolean-createdAt"
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: data)

        guard case .found(let manifest) = try await store.load() else {
            XCTFail("expected .found — CFBoolean rejection applies to createdAtMs, not format_version")
            return
        }
        XCTAssertNil(manifest.createdAtMs,
                     "strictInt64 must reject CFBoolean — must NOT bridge to Int64=1")
    }


    func testVerifyCompatible_higherFormat_throwsHigherFormatVersion() async throws {
        let (client, store) = await makeStore()
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9", writerID: "future"
        )
        do {
            try await store.verifyCompatible()
            XCTFail("expected higherFormatVersion")
        } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, let local, let minApp) {
            XCTAssertEqual(remote, 99)
            XCTAssertEqual(local, RepoLayout.currentSupportedFormatVersion)
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testVerifyCompatible_formatZero_throwsMismatchedFormatVersion() async throws {
        let (client, store) = await makeStore()
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 0, minAppVersion: "0.0.0", writerID: "stale"
        )
        do {
            try await store.verifyCompatible()
            XCTFail("expected mismatchedFormatVersion for format=0")
        } catch RepoBootstrap.VersionConflict.mismatchedFormatVersion(let remote, let local, _) {
            XCTAssertEqual(remote, 0)
            XCTAssertEqual(local, RepoLayout.formatVersion)
        }
    }

    func testVerifyCompatible_formatOne_throwsMismatchedFormatVersion() async throws {
        let (client, store) = await makeStore()
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 1, minAppVersion: "1.0.0", writerID: "v1"
        )
        do {
            try await store.verifyCompatible()
            XCTFail("expected mismatchedFormatVersion for format=1")
        } catch RepoBootstrap.VersionConflict.mismatchedFormatVersion(let remote, _, _) {
            XCTAssertEqual(remote, 1)
        }
    }

    func testClassify_currentFormat_doesNotThrow() throws {
        try VersionManifestStore.classify(remoteFormat: RepoLayout.formatVersion, minAppVersion: nil)
    }


    func testWriteIfAbsent_existingCompatibleManifest_isNoOpVerified() async throws {
        let (client, store) = await makeStore()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "preexisting")
        let before = await client.snapshotFiles()[RepoLayout.versionFilePath(base: basePath)]

        try await store.writeIfAbsent(writerID: "different-writer")

        let after = await client.snapshotFiles()[RepoLayout.versionFilePath(base: basePath)]
        XCTAssertEqual(before, after, "existing compatible manifest must NOT be overwritten")
    }

    func testWriteIfAbsent_directoryAtVersionPath_throwsBootstrapError() async throws {
        // Pre-extraction: bootstrap's pre-check refused to publish over a directory at version.json
        // and threw `BootstrapError.ioFailure` immediately. Must not route through readback /
        // `.unreadable` after the consolidation.
        let (client, store) = await makeStore()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.createDirectory(path: RepoLayout.versionFilePath(base: basePath) + "/child")
        do {
            try await store.writeIfAbsent(writerID: "writer-A")
            XCTFail("expected damaged-shaped throw for directory at version.json")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 18)
        }
    }

    func testWriteIfAbsent_existingHigherFormat_throwsHigherFormatVersion() async throws {
        let (client, store) = await makeStore()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9", writerID: "future"
        )
        do {
            try await store.writeIfAbsent(writerID: "us")
            XCTFail("expected higherFormatVersion")
        } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, _, let minApp) {
            XCTAssertEqual(remote, 99)
            XCTAssertEqual(minApp, "9.9.9")
        }
    }


    private func makeStore() async -> (InMemoryRemoteStorageClient, VersionManifestStore) {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let store = VersionManifestStore(client: client, basePath: basePath)
        return (client, store)
    }

    private func validVersionData() -> Data {
        let dict: [String: Any] = [
            "format_version": RepoLayout.formatVersion,
            "min_app_version": RepoLayout.minAppVersionPlaceholder,
            "created_at_ms": 0,
            "created_by_writer": "test"
        ]
        return (try? JSONSerialization.data(withJSONObject: dict)) ?? Data()
    }

    private func booleanFormatVersionData() throws -> Data {
        let dict: [String: Any] = [
            "format_version": true,
            "min_app_version": "9.9.9",
            "created_at_ms": 0,
            "created_by_writer": "boolean"
        ]
        return try JSONSerialization.data(withJSONObject: dict)
    }
}
