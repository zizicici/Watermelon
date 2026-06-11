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

    func testIsCurrentRejectsFutureMinAppVersion() {
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2, layout: "lite-month-sqlite",
            minAppVersion: "99.0.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentRejectsAbsentMinAppVersion() {
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2, layout: "lite-month-sqlite",
            minAppVersion: nil, createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentAcceptsOwnMinAppVersion() {
        XCTAssertTrue(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2, layout: "lite-month-sqlite",
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentAcceptsOlderMinAppVersion() {
        XCTAssertTrue(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2, layout: "lite-month-sqlite",
            minAppVersion: "1.4.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    // MARK: - Writer (upload + read-back verify)

    func testWriterUploadsToTempThenPublishesToVersionPath() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        let committed = try await writer.commit(createdAt: createdAt, createdBy: createdBy)

        let uploaded = await client.uploadedPaths
        let created = await client.createdDirectories
        let moved = await client.movedPaths

        // The upload lands on a temp sibling under .watermelon, never directly on version.json.
        XCTAssertEqual(uploaded.count, 1)
        let tempPath = try XCTUnwrap(uploaded.first)
        XCTAssertTrue(tempPath.hasPrefix(RepoLayoutLite.repoDirectoryPath(basePath: basePath) + "/"))
        XCTAssertTrue(tempPath.hasSuffix(".json.tmp"))
        XCTAssertFalse(uploaded.contains(versionPath), "version.json is published by move, never uploaded directly")

        // Published onto the canonical version path by a move from the temp.
        XCTAssertTrue(moved.contains { $0.to == versionPath && $0.from.hasSuffix(".json.tmp") })
        XCTAssertTrue(created.contains(RepoLayoutLite.repoDirectoryPath(basePath: basePath)))

        let storedBytes = await client.fileData(path: versionPath)
        let persisted = try VersionManifestLite.decode(try XCTUnwrap(storedBytes))
        XCTAssertEqual(persisted, committed)
        XCTAssertEqual(persisted.formatVersion, 2)
        XCTAssertEqual(persisted.layout, "lite-month-sqlite")
    }

    func testWriterPublishFailureCleansTempAndDoesNotReportSuccess() async throws {
        let client = InMemoryRemoteStorageClient()
        // Publish move fails terminally with no existing final to fall back to: temp must be cleaned and
        // no half-committed version.json may be left behind.
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a failed publish must not report committed success")
        } catch {
            // expected
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertNil(storedBytes, "no half-committed version.json after a failed publish")
        let uploaded = await client.uploadedPaths
        let tempPath = try XCTUnwrap(uploaded.first)
        let tempData = await client.fileData(path: tempPath)
        XCTAssertNil(tempData, "the temp upload must be cleaned best-effort after a failed publish")
    }

    func testWriterRecoveryOverwritesMalformedFinalWithCanonicalBytes() async throws {
        let client = InMemoryRemoteStorageClient()
        // A malformed version.json already sits at the canonical path (the repair-route scenario).
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        let committed = try await writer.commit(createdAt: createdAt, createdBy: createdBy)

        let storedBytes = await client.fileData(path: versionPath)
        let persisted = try VersionManifestLite.decode(try XCTUnwrap(storedBytes))
        XCTAssertEqual(persisted, committed, "recovery must leave canonical bytes at the final path")

        // No version temp/backup scratch is left under .watermelon on success.
        let repoChildren = try await client.list(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
        XCTAssertFalse(
            repoChildren.contains { $0.name.hasSuffix(".tmp") || $0.name.hasSuffix(".bak") },
            "a successful publish must not leave version scratch behind"
        )
    }

    func testWriterReassertsOwnershipBeforePublishingVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        let gate = BooleanGate([false])
        let writer = VersionManifestWriter(
            client: client,
            basePath: basePath,
            assertOwnership: {
                if await gate.next() == false { throw LiteRepoError.ownershipLost }
            }
        )

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("lost ownership before version publish must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertNil(storedBytes, "version.json must not be published after ownership loss")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "ownership loss before commit must prevent the temp upload")
        let created = await client.createdDirectories
        XCTAssertTrue(created.isEmpty, "ownership loss before commit must prevent marker directory creation")
        let moves = await client.movedPaths
        XCTAssertFalse(moves.contains { $0.to == versionPath }, "publish move must not run after ownership loss")
    }

    func testWriterRestoresRollbackBackupBeforeReportingOwnershipLoss() async throws {
        let client = InMemoryRemoteStorageClient()
        let original = Data("not json".utf8)
        await client.seedFile(path: versionPath, data: original)
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // direct temp -> final
        await client.setOnMove { _, to in
            if to.hasSuffix(".json.bak") {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // fallback temp -> final
            }
        }
        let gate = BooleanGate([true, true, true, true, false])
        let writer = VersionManifestWriter(
            client: client,
            basePath: basePath,
            assertOwnership: {
                if await gate.next() == false { throw LiteRepoError.ownershipLost }
            }
        )

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("lost ownership before rollback restore must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let moves = await client.movedPaths
        XCTAssertTrue(moves.contains { $0.from == versionPath && $0.to.hasSuffix(".json.bak") })
        XCTAssertTrue(
            moves.contains { $0.from.hasSuffix(".json.bak") && $0.to == versionPath },
            "rollback restore must run before ownership loss is reported"
        )
        let finalData = await client.fileData(path: versionPath)
        XCTAssertEqual(finalData, original, "canonical version.json must be restored before the failure surfaces")
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

private actor BooleanGate {
    private var values: [Bool]

    init(_ values: [Bool]) {
        self.values = values
    }

    func next() -> Bool {
        if values.isEmpty { return false }
        return values.removeFirst()
    }
}
