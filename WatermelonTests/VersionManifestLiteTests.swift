import XCTest
@testable import Watermelon

final class VersionManifestLiteTests: XCTestCase {
    private let basePath = "/photos"
    private let createdAt = "2026-06-08T00:00:00Z"
    private let createdBy = "writer-1b4e28ba"

    private var versionPath: String { RepoLayoutLite.versionPath(basePath: basePath) }

    // MARK: - Canonical schema

    func testMakeManifestUsesCanonicalConstants() {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        XCTAssertEqual(manifest.formatVersion, 2)
        XCTAssertEqual(manifest.layout, "lite-month-sqlite")
        XCTAssertEqual(manifest.minAppVersion, "1.5.0")
        XCTAssertEqual(manifest.createdAt, createdAt)
        XCTAssertEqual(manifest.createdBy, createdBy)
    }

    func testEncodeProducesCanonicalJSONKeysAndValues() throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let data = try VersionManifestLite.encode(manifest)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(object["format_version"] as? Int, 2)
        XCTAssertEqual(object["layout"] as? String, "lite-month-sqlite")
        XCTAssertEqual(object["min_app_version"] as? String, "1.5.0")
        XCTAssertEqual(object["created_at"] as? String, createdAt)
        XCTAssertEqual(object["created_by"] as? String, createdBy)
        XCTAssertEqual(
            Set(object.keys),
            ["format_version", "layout", "min_app_version", "created_at", "created_by"]
        )
    }

    func testEncodeDecodeRoundTrips() throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let decoded = try VersionManifestLite.decode(VersionManifestLite.encode(manifest))
        XCTAssertEqual(decoded, manifest)
    }

    func testIsCurrentOnlyForFormat2LiteLayout() {
        XCTAssertTrue(VersionManifestLite.isCurrent(
            VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        ))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2, layout: "something-else",
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 3, layout: "lite-month-sqlite",
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: nil, layout: "lite-month-sqlite",
            minAppVersion: nil, createdAt: nil, createdBy: nil
        )))
    }

    // MARK: - Writer (upload + read-back verify)

    func testWriterUploadsToVersionPathAndPersistsCanonicalBytes() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        let committed = try await writer.commit(createdAt: createdAt, createdBy: createdBy)

        let uploaded = await client.uploadedPaths
        let created = await client.createdDirectories
        XCTAssertTrue(uploaded.contains(versionPath))
        XCTAssertTrue(created.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)))

        let storedBytes = await client.fileData(path: versionPath)
        let persisted = try VersionManifestLite.decode(try XCTUnwrap(storedBytes))
        XCTAssertEqual(persisted, committed)
        XCTAssertEqual(persisted.formatVersion, 2)
        XCTAssertEqual(persisted.layout, "lite-month-sqlite")
    }

    func testWriterThrowsWhenReadBackDivergesFromWrite() async {
        let client = InMemoryRemoteStorageClient()
        // Read-back returns a valid but different manifest (e.g. a concurrent overwrite).
        let divergent = VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "intruder")
        if let bytes = try? VersionManifestLite.encode(divergent) {
            await client.enqueueDownloadData(bytes)
        } else {
            return XCTFail("failed to encode divergent manifest")
        }
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("expected readBackMismatch")
        } catch let error as VersionManifestWriter.WriteError {
            XCTAssertEqual(error, .readBackMismatch)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testWriterThrowsWhenReadBackBytesDifferDespiteSameDecodedManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        // Same logical manifest, but compact, reordered keys, plus an ignored extra field: decode-equal
        // to what commit writes, yet not byte-equal. A decode-only check would have accepted this.
        let divergentBytes = try JSONSerialization.data(withJSONObject: [
            "created_by": createdBy,
            "created_at": createdAt,
            "min_app_version": "1.5.0",
            "layout": "lite-month-sqlite",
            "format_version": 2,
            "server_note": "reserialized"
        ])
        let canonical = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        XCTAssertEqual(
            try VersionManifestLite.decode(divergentBytes), canonical,
            "premise: divergent bytes must decode to the same manifest"
        )
        XCTAssertNotEqual(
            divergentBytes, try VersionManifestLite.encode(canonical),
            "premise: divergent bytes must not be byte-equal to the canonical encoding"
        )

        await client.enqueueDownloadData(divergentBytes)
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("byte-divergent read-back must not report success")
        } catch let error as VersionManifestWriter.WriteError {
            XCTAssertEqual(error, .readBackMismatch)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testWriterThrowsWhenReadBackIsCorrupt() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(Data("not json".utf8))
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("expected a decode failure to abort the commit")
        } catch {
            // Any thrown error is acceptable; the contract is "do not report success".
        }
    }

    func testWriterPropagatesUploadFault() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("expected upload fault to propagate")
        } catch {
            let uploaded = await client.uploadedPaths
            XCTAssertFalse(uploaded.contains(versionPath))
        }
    }
}
