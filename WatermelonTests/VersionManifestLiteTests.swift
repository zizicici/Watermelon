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
        XCTAssertEqual(manifest.minAppVersion, "1.5.0")
        XCTAssertEqual(manifest.createdAt, createdAt)
        XCTAssertEqual(manifest.createdBy, createdBy)
    }

    func testEncodeProducesCanonicalJSONKeysAndValues() throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let data = try VersionManifestLite.encode(manifest)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])

        XCTAssertEqual(object["format_version"] as? Int, 2)
        XCTAssertEqual(object["min_app_version"] as? String, "1.5.0")
        XCTAssertEqual(object["created_at"] as? String, createdAt)
        XCTAssertEqual(object["created_by"] as? String, createdBy)
        XCTAssertEqual(
            Set(object.keys),
            ["format_version", "min_app_version", "created_at", "created_by"]
        )
    }

    func testEncodeDecodeRoundTrips() throws {
        let manifest = VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        let decoded = try VersionManifestLite.decode(VersionManifestLite.encode(manifest))
        XCTAssertEqual(decoded, manifest)
    }

    func testIsCurrentOnlyForFormat2() {
        XCTAssertTrue(VersionManifestLite.isCurrent(
            VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        ))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 3,
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: nil,
            minAppVersion: nil, createdAt: nil, createdBy: nil
        )))
    }

    func testIsCurrentAcceptsSameFormatFutureMinAppVersion() {
        XCTAssertTrue(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "99.0.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentRejectsAbsentMinAppVersion() {
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: nil, createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentAcceptsOwnMinAppVersion() {
        XCTAssertTrue(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentAcceptsOlderMinAppVersion() {
        XCTAssertTrue(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.4.0", createdAt: createdAt, createdBy: createdBy
        )))
    }

    func testIsCurrentRejectsMissingCreatedAtOrCreatedBy() {
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.5.0", createdAt: nil, createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: nil
        )))
    }

    func testIsCurrentRejectsEmptyCanonicalFields() {
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "", createdAt: createdAt, createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.5.0", createdAt: "", createdBy: createdBy
        )))
        XCTAssertFalse(VersionManifestLite.isCurrent(WatermelonRemoteVersionManifest(
            formatVersion: 2,
            minAppVersion: "1.5.0", createdAt: createdAt, createdBy: ""
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
    }

    // A non-independent MOVE backend commits version.json by direct PUT: no temp, no MOVE (temp→MOVE would alias
    // the temp to the canonical, and deleting that temp — here or in cleanup — would destroy version.json).
    func testWriterOnNonIndependentMoveCommitsByDirectPut() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        let committed = try await writer.commit(createdAt: createdAt, createdBy: createdBy)

        let uploaded = await client.uploadedPaths
        let moved = await client.movedPaths
        XCTAssertEqual(uploaded, [versionPath], "version.json is written straight to the canonical path")
        XCTAssertTrue(moved.isEmpty, "a non-independent MOVE backend must never publish version.json via MOVE")

        let storedBytes = await client.fileData(path: versionPath)
        let persisted = try VersionManifestLite.decode(try XCTUnwrap(storedBytes))
        XCTAssertEqual(persisted, committed)
    }

    // Direct PUT whose response fails but whose bytes landed valid: version.json is a usable commit point and must
    // NOT be removed (its removal would drop a good prior/landed commit).
    func testDirectVersionCommitKeepsValidCanonicalOnPostEffectUploadFailure() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let writer = VersionManifestWriter(client: client, basePath: basePath)
        await client.failUploadAfterWrite(forPathSuffix: versionPath, error: RemoteErrorFixtures.retryable)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a post-effect upload failure must surface")
        } catch {}

        let stored = await client.fileData(path: versionPath)
        let persisted = try VersionManifestLite.decode(try XCTUnwrap(stored))
        XCTAssertEqual(persisted.formatVersion, 2, "a valid landed version.json must be left as a usable commit point")
    }

    // Direct PUT that lands partial/corrupt bytes then fails: the damaged version.json must be removed so the repo
    // routes recoverable (fresh / v1Migrate / malformedVersion) next run instead of wedging terminal .damaged.
    func testDirectVersionCommitRemovesDamagedCanonicalOnCorruptUploadFailure() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let writer = VersionManifestWriter(client: client, basePath: basePath)
        await client.failUploadWritingCorruptBytes(Data([0x00, 0x01]), forPathSuffix: versionPath, error: RemoteErrorFixtures.retryable)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a corrupt upload must surface")
        } catch {}

        let stored = await client.fileData(path: versionPath)
        XCTAssertNil(stored, "a partial/corrupt version.json must be removed, not left to route the repo damaged")
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

    // A backend that exposes bytes different from the published manifest (a corrupt/short publish reported
    // as success) must not leave a damaged canonical commit point — that would route the repo terminal
    // .damaged. The read-back mismatch must remove the canonical so it stays recoverable.
    func testWriterReadBackMismatchRemovesDamagedCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = VersionManifestWriter(client: client, basePath: basePath)
        // Fresh repo: the safe-to-replace probe sees an absent canonical, then the post-publish read-back is
        // scripted to return bytes that differ from the published manifest.
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)
        await client.enqueueDownloadData(Data("corrupt-not-the-manifest".utf8))

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a version read-back mismatch must throw")
        } catch let error as VersionManifestWriter.WriteError {
            XCTAssertEqual(error, .readBackMismatch)
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertNil(
            storedBytes,
            "a read-back mismatch must remove the damaged canonical so the repo stays recoverable, not terminal .damaged"
        )
    }

    // Finding 2 (R04): the mismatch cleanup must not be best-effort. A transient fault on the first cleanup
    // delete must be retried so the proven-bad canonical is still removed (never left terminal .damaged).
    func testWriterReadBackMismatchCleanupRetriesDeleteOnTransientFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let writer = VersionManifestWriter(client: client, basePath: basePath)
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)   // safe-to-replace probe: absent
        await client.enqueueDownloadData(Data("corrupt-not-the-manifest".utf8))   // read-back mismatch
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)    // first cleanup delete faults transiently

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a version read-back mismatch must throw")
        } catch let error as VersionManifestWriter.WriteError {
            XCTAssertEqual(error, .readBackMismatch)
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertNil(
            storedBytes,
            "a transient cleanup-delete fault must be retried so the damaged canonical is still removed"
        )
    }

    // Finding 2 (R05): the cleanup must re-prove ownership before EACH destructive delete. If ownership is
    // lost between retries (a delete that applied remotely but faulted can outlive the lease while a successor
    // commits a valid version.json), the cleanup must stop — never deleting the successor's canonical.
    func testWriterReadBackMismatchCleanupStopsDeletingAfterOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        // commit assert (#1), publish move assert (#2), cleanup attempt 0 assert (#3); attempt 1 → ownership lost.
        let gate = BooleanGate([true, true, true, false])
        let writer = VersionManifestWriter(
            client: client,
            basePath: basePath,
            assertOwnership: { if await gate.next() == false { throw LiteRepoError.ownershipLost } }
        )
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)   // safe-to-replace probe: absent
        await client.enqueueDownloadData(Data("corrupt-not-the-manifest".utf8))   // read-back mismatch
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)    // first cleanup delete faults

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a version read-back mismatch must throw")
        } catch {
            // expected
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertNotNil(
            storedBytes,
            "once ownership can no longer be proven between retries, cleanup must stop — a successor's canonical must survive"
        )
    }

    func testWriterRefusesMalformedCanonicalVersionAndLeavesBytesUnchanged() async throws {
        let client = InMemoryRemoteStorageClient()
        let original = Data("not json".utf8)
        await client.seedFile(path: versionPath, data: original)
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("malformed canonical version.json must fail closed")
        } catch let error as VersionManifestWriter.WriteError {
            XCTAssertEqual(error, .unsafeExistingVersion)
        } catch {
            XCTFail("expected unsafeExistingVersion, got \(error)")
        }

        let storedBytes = await client.fileData(path: versionPath)
        XCTAssertEqual(storedBytes, original, "malformed canonical bytes must survive unchanged")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "writer must not upload a replacement temp after rejecting canonical")
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

    func testWriterBlocksRollbackRestoreBeforeReportingOwnershipLoss() async throws {
        let client = InMemoryRemoteStorageClient()
        let original = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "original")
        )
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
        let backupPath = try XCTUnwrap(moves.last { $0.to.hasSuffix(".json.bak") }?.to)
        XCTAssertFalse(
            moves.contains { $0.from == backupPath && $0.to == versionPath },
            "lost ownership before rollback restore must leave the backup stranded"
        )
        let finalData = await client.fileData(path: versionPath)
        XCTAssertNil(finalData, "canonical version.json must not be restored after ownership loss")
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(backupData, original, "the backup must survive for later owned recovery")
    }

    func testRollbackBlocksRestoreBeforeReportingConfidenceFault() async throws {
        let client = InMemoryRemoteStorageClient()
        let original = try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "original")
        )
        await client.seedFile(path: versionPath, data: original)
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // direct temp -> final
        await client.setOnMove { _, to in
            if to.hasSuffix(".json.bak") {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // fallback publish temp -> final
            }
        }
        let gate = BooleanGate([true, true, true, true, false])
        let writer = VersionManifestWriter(
            client: client,
            basePath: basePath,
            assertOwnership: {
                if await gate.next() == false { throw LiteRepoError.leaseConfidenceLost }
            }
        )

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a confidence fault must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .leaseConfidenceLost)
        }

        let moves = await client.movedPaths
        XCTAssertTrue(moves.contains { $0.from == versionPath && $0.to.hasSuffix(".json.bak") })
        let backupPath = try XCTUnwrap(moves.last { $0.to.hasSuffix(".json.bak") }?.to)
        XCTAssertFalse(
            moves.contains { $0.from == backupPath && $0.to == versionPath },
            "lost confidence before rollback restore must leave the backup stranded"
        )
        let finalData = await client.fileData(path: versionPath)
        XCTAssertNil(finalData, "canonical version.json must not be restored after confidence loss")
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(backupData, original, "the backup must survive for later owned recovery")
    }

    // Regression (R04 Cluster A): when a current-version replace faults on BOTH the publish move and the
    // rollback restore, the temp (the only current version content) must survive as recoverable scratch —
    // deleting it would leave the repo terminal .damaged with no version recovery path.
    func testFailedCurrentVersionReplaceKeepsTempAsRecoveryScratch() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "original")
        ))
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // direct temp -> final
        await client.setOnMove { _, to in
            if to.hasSuffix(".json.bak") {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // publish temp -> final
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // restore backup -> final
            }
        }
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a doubly-faulted replace must surface")
        } catch {
            // expected
        }

        let finalData = await client.fileData(path: versionPath)
        XCTAssertNil(finalData, "the canonical is absent after the restore fault")
        let uploaded = await client.uploadedPaths
        let tempPath = try XCTUnwrap(uploaded.first)
        let tempData = await client.fileData(path: tempPath)
        let recovered = try VersionManifestLite.decode(try XCTUnwrap(tempData))
        XCTAssertTrue(
            VersionManifestLite.isCurrent(recovered),
            "the temp must survive as current version scratch so the router can recover"
        )
    }

    // Regression (R05 Cluster A): when the doubly-faulted replace's backup-scratch LIST also faults, the temp
    // (the only current version content) must still survive — an unresolved LIST must not license deleting it.
    func testFailedCurrentVersionReplaceKeepsTempWhenBackupScratchListFaults() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "original")
        ))
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // direct temp -> final
        await client.setOnMove { _, to in
            if to.hasSuffix(".json.bak") {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // publish temp -> final
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // restore backup -> final
                await client.enqueueListError(RemoteErrorFixtures.terminal)   // repo-dir LIST in keepTempAsRecoveryScratch
            }
        }
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a doubly-faulted replace must surface")
        } catch {
            // expected
        }

        let finalData = await client.fileData(path: versionPath)
        XCTAssertNil(finalData, "the canonical is absent after the restore fault")
        let uploaded = await client.uploadedPaths
        let tempPath = try XCTUnwrap(uploaded.first)
        let tempData = await client.fileData(path: tempPath)
        XCTAssertNotNil(tempData, "the temp must survive when the backup-scratch LIST faults")
        let recovered = try VersionManifestLite.decode(try XCTUnwrap(tempData))
        XCTAssertTrue(
            VersionManifestLite.isCurrent(recovered),
            "an unresolved LIST must not license deleting the only current version scratch"
        )
    }

    // Regression (R04 Cluster C): if the rollback restore's existence probe faults, the restore must no-op
    // rather than move the backup over a final a successor may have committed.
    func testRollbackRestoreNoOpsWhenExistenceProbeFaults() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "original")
        ))
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // direct temp -> final
        await client.setOnMove { _, to in
            if to.hasSuffix(".json.bak") {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // publish temp -> final
                await client.enqueueExistsError(RemoteErrorFixtures.terminal) // restore existence probe
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)   // restore backup -> final (must not run)
            }
        }
        let writer = VersionManifestWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.commit(createdAt: createdAt, createdBy: createdBy)
            XCTFail("a restore-probe fault must surface")
        } catch {
            // expected
        }

        let moves = await client.movedPaths
        let backupPath = try XCTUnwrap(moves.last { $0.to.hasSuffix(".json.bak") }?.to)
        XCTAssertFalse(
            moves.contains { $0.from == backupPath && $0.to == versionPath },
            "the restore must not move the backup over the final when the existence probe faults"
        )
        let backupData = await client.fileData(path: backupPath)
        XCTAssertNotNil(backupData, "the backup survives when the restore probe is unresolved")
    }

    func testWriterThrowsWhenReadBackDivergesFromWrite() async {
        let client = InMemoryRemoteStorageClient()
        // Read-back returns a valid but different manifest (e.g. a concurrent overwrite).
        let divergent = VersionManifestLite.makeManifest(createdAt: "2000-01-01T00:00:00Z", createdBy: "intruder")
        if let bytes = try? VersionManifestLite.encode(divergent) {
            await client.enqueueDownloadError(RemoteErrorFixtures.notFound)   // preflight canonical probe
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

        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)   // preflight canonical probe
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
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)   // preflight canonical probe
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
