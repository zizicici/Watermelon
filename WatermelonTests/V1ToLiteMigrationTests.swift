import XCTest
import GRDB
@testable import Watermelon

private actor MigrationProgressRecorder {
    private var values: [V1ToLiteMigrationProgress] = []

    func append(_ progress: V1ToLiteMigrationProgress) {
        values.append(progress)
    }

    func snapshots() -> [V1ToLiteMigrationProgress] {
        values
    }
}

// P07 (V1ToLiteMigration): V1→Lite migration. Covers the per-month copy/validate/publish,
// idempotent + interrupted reruns, the TOCTOU re-read, the ownership fail-closed gates, and one true
// real-SQLite run on a disk-backed remote.
final class V1ToLiteMigrationTests: XCTestCase {
    private let basePath = "/photos"

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private func liteMonthPath(_ year: Int, _ month: Int) -> String {
        MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: year, month: month)
    }

    private func v1ManifestPath(_ year: Int, _ month: Int) -> String {
        MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: year, month: month)
    }

    private func versionPath() -> String { RepoLayoutLite.versionPath(basePath: basePath) }

    // A genuine V1 month manifest (real MonthManifestStore SQLite), so a copy actually validates.
    private func seedRealV1Month(
        client: any RemoteStorageClientProtocol,
        year: Int,
        month: Int,
        fileName: String = "a.jpg",
        contentHash: Data = Data([0xAB])
    ) async throws {
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, layout: .v1
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: contentHash, fileName: fileName)
        )
        _ = try await store.flushToRemote()
    }

    private func makeLegacyTimestampManifestData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-v1lite-legacy-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("legacy.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try queue.write { db in
            try db.execute(sql: """
                CREATE TABLE resources (
                  fileName TEXT PRIMARY KEY NOT NULL,
                  contentHash BLOB NOT NULL,
                  fileSize INTEGER NOT NULL,
                  resourceType INTEGER NOT NULL,
                  creationDateNs INTEGER,
                  backedUpAtNs INTEGER NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE assets (
                  assetFingerprint BLOB PRIMARY KEY NOT NULL,
                  creationDateNs INTEGER,
                  backedUpAtNs INTEGER NOT NULL,
                  resourceCount INTEGER NOT NULL,
                  totalFileSizeBytes INTEGER NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE asset_resources (
                  assetFingerprint BLOB NOT NULL,
                  resourceHash BLOB NOT NULL,
                  role INTEGER NOT NULL,
                  slot INTEGER NOT NULL,
                  PRIMARY KEY(assetFingerprint, role, slot)
                )
                """)
        }
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    // MARK: - Per-month copy (temp → move → validate)

    func testPerMonthCopyIsAtomicViaValidatedTempThenMove() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let uploadsBefore = await client.uploadedPaths

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let uploads = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        let tempUploads = uploads.filter { $0.hasPrefix("/photos/.watermelon/months/") && $0.hasSuffix(".tmp") }
        XCTAssertEqual(tempUploads.count, 1, "exactly one temp copy per month, never a direct final write")
        XCTAssertFalse(uploads.contains(liteMonthPath(2024, 3)), "must not upload directly to the final month path")

        let moves = await client.movedPaths
        XCTAssertTrue(
            moves.contains { $0.from == tempUploads[0] && $0.to == liteMonthPath(2024, 3) },
            "publish must rename the temp onto the final month path"
        )
        let finalData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(finalData, "final month present after publish")
        let tempData = await client.fileData(path: tempUploads[0])
        XCTAssertNil(tempData, "temp must not linger after publish")
    }

    func testReportsMigrationProgressAfterEnumerationAndEachMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 1)
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let recorder = MigrationProgressRecorder()

        try await V1ToLiteMigration(
            client: client,
            basePath: basePath,
            onProgress: { progress in
                await recorder.append(progress)
            }
        ).run(createdAt: "t", createdBy: "id")

        let progress = await recorder.snapshots()
        XCTAssertEqual(progress, [
            V1ToLiteMigrationProgress(phase: .copying, current: 0, total: 2),
            V1ToLiteMigrationProgress(phase: .copying, current: 1, total: 2),
            V1ToLiteMigrationProgress(phase: .copying, current: 2, total: 2),
            V1ToLiteMigrationProgress(phase: .validating, current: 0, total: 2),
            V1ToLiteMigrationProgress(phase: .validating, current: 1, total: 2),
            V1ToLiteMigrationProgress(phase: .validating, current: 2, total: 2),
            V1ToLiteMigrationProgress(phase: .finalizing, current: 0, total: 0)
        ])
    }

    func testDirectMigrationWritesPruneMarkerBeforeVersionCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)

        let result = try await V1ToLiteMigration(client: client, basePath: basePath)
            .run(createdAt: "t", createdBy: "id")

        let v1FileData = await client.fileData(path: v1ManifestPath(2024, 3))
        let markerFileData = await client.fileData(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        let versionFileData = await client.fileData(path: versionPath())
        let v1Data = try XCTUnwrap(v1FileData)
        let markerData = try XCTUnwrap(
            markerFileData,
            "direct migration must also record V1 prune provenance before committing version.json"
        )
        let marker = try JSONDecoder().decode(LegacyV1PruneMarker.self, from: markerData)
        XCTAssertEqual(marker.sources.count, 1)
        XCTAssertEqual(marker.sources[0].year, 2024)
        XCTAssertEqual(marker.sources[0].month, 3)
        XCTAssertEqual(marker.sources[0].manifestPath, v1ManifestPath(2024, 3))
        XCTAssertEqual(marker.sources[0].sha256Hex, S3SigV4Signer.sha256Hex(data: v1Data))
        XCTAssertEqual(result.migratedSources.count, 1)
        XCTAssertNotNil(versionFileData)
    }

    func testInvalidCopyDoesNotPublishFinalOrCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        // Present but corrupt: a non-SQLite blob at the V1 manifest path.
        await client.seedFile(path: v1ManifestPath(2024, 3), data: Data([0x01, 0x02, 0x03]))

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a copy that fails manifest schema validation must abort the migration")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .v1MonthManifestUnreadable(month: "2024-03"))
            XCTAssertTrue(error.localizedDescription.contains("2024-03"))
            XCTAssertFalse(error.localizedDescription.contains("v1MonthManifestUnreadable"))
        }

        let finalData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(finalData, "no final month published for an invalid copy")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when a month fails")
        let temps = (await client.uploadedPaths).filter { $0.hasPrefix("/photos/.watermelon/months/") }
        for temp in temps {
            let tempData = await client.fileData(path: temp)
            XCTAssertNil(tempData, "temp copy must be cleaned up on failure")
        }
    }

    func testLegacyTimestampSchemaV1ManifestMigratesWithoutRewritingBytes() async throws {
        let client = InMemoryRemoteStorageClient()
        let legacyData = try makeLegacyTimestampManifestData()
        await client.seedFile(path: v1ManifestPath(2024, 3), data: legacyData)

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let liteData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertEqual(liteData, legacyData, "migration must publish the exact V1 bytes after load-path validation")
        let migrated = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite, pushSchemaUpgrade: false
        )
        XCTAssertNotNil(migrated, "the copied legacy-schema manifest remains loadable through the normal Lite path")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "version.json commits after the loadable legacy manifest is copied")
    }

    func testMigrationManifestValidatorUsesLoadPathWithoutRewritingSourceBytes() throws {
        let client = InMemoryRemoteStorageClient()
        let legacyData = try makeLegacyTimestampManifestData()
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-v1lite-validator-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try legacyData.write(to: url)

        let result = MonthManifestStore.validateMonthManifestFile(
            at: url,
            year: 2024,
            month: 3,
            client: client,
            basePath: basePath,
            layout: .lite
        )

        XCTAssertEqual(result, .valid)
        XCTAssertEqual(try Data(contentsOf: url), legacyData, "validation must materialize a temp copy, not mutate the source bytes")
    }

    // R02 (Codex High): a non-not-found metadata fault on one candidate month manifest must abort
    // enumeration, not silently drop the month and then commit an incomplete `.current` repo.
    func testEnumerationFailsClosedOnNonNotFoundMetadataFault() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3, fileName: "a.jpg", contentHash: Data([0xAB]))
        try await seedRealV1Month(client: client, year: 2024, month: 5, fileName: "b.jpg", contentHash: Data([0xCD]))
        // Script a retryable (non-not-found) metadata fault on the second month's manifest probe.
        await client.failMetadata(
            forPathSuffix: "2024/05/\(MonthManifestStore.manifestFileName)",
            error: RemoteErrorFixtures.retryable
        )

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a non-not-found metadata fault during enumeration must fail closed")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable, "the metadata fault must surface, not read as absence")
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when enumeration hit a non-not-found fault")
        let month3 = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(month3, "no month may be published once enumeration fails closed")
        let month5 = await client.fileData(path: liteMonthPath(2024, 5))
        XCTAssertNil(month5, "the faulted month must not be published")
        let decision = try await RepoFormatRouter(client: client, basePath: basePath).classify()
        XCTAssertEqual(decision, .v1Migrate, "no incomplete .current route may be produced")
    }

    // A directory occupying a V1 month manifest slot must fail the migration closed before committing
    // version.json — never silently drop that month from the migrated Lite index.
    func testDirectoryValuedV1CandidateFailsMigrationClosedBeforeCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 1)   // valid file manifest
        await client.seedDirectory(v1ManifestPath(2024, 2))               // directory-valued candidate sibling

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a directory-valued V1 candidate must fail the migration closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .v1MonthManifestUnreadable(month: "2024-02"))
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit while a V1 month candidate is a directory")
        let migratedMonth1 = await client.fileData(path: liteMonthPath(2024, 1))
        XCTAssertNil(migratedMonth1, "the whole migration fails closed at enumeration; no month is published")
        let stillDirectory = try await client.metadata(path: v1ManifestPath(2024, 2))?.isDirectory
        XCTAssertEqual(stillDirectory, true, "the directory-valued V1 candidate is left intact")
    }

    // MARK: - Idempotency / resume

    func testIdempotentRerunRoutesCurrentWithoutRecopy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()

        let plan1 = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        await plan1.session.stopAndRelease()
        let afterFirst = await client.uploadedPaths

        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()

        let rerun = Array((await client.uploadedPaths).dropFirst(afterFirst.count))
        XCTAssertFalse(rerun.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "an idempotent rerun (.current) must not re-copy month manifests")
        XCTAssertFalse(rerun.contains(versionPath()),
                       "an idempotent rerun (.current) must not rewrite version.json")
    }

    func testInterruptedBeforeVersionResumesAndFinishes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()

        // Faithful "interrupted after month copy, before version commit" state: run the migration
        // copy+commit directly (which leaves the V1 manifest in place — only the P08 gateway path cleans
        // it), then drop version.json. The repo still holds the V1 manifest + the Lite month, so the
        // route re-reads as .v1Migrate and resumes.
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: writerID)
        let committedAfterFirst = await client.fileData(path: versionPath())
        XCTAssertNotNil(committedAfterFirst)

        try await client.delete(path: versionPath())
        let beforeResume = await client.uploadedPaths

        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()

        let versionAfterResume = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionAfterResume, "resume recommits version.json")
        let monthAfterResume = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(monthAfterResume, "the migrated month survives the resume")
        let resume = Array((await client.uploadedPaths).dropFirst(beforeResume.count))
        XCTAssertFalse(resume.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "resume must skip the already-valid Lite month rather than re-copy it")
    }

    func testExistingValidFinalSkippedWithinMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        // Drop version.json so a rerun still classifies as .v1Migrate while the Lite month is already valid.
        try await client.delete(path: versionPath())
        let uploadsBefore = await client.uploadedPaths

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let rerun = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        XCTAssertFalse(rerun.contains { $0.hasPrefix("/photos/.watermelon/months/") && $0.hasSuffix(".tmp") },
                       "an already-valid final month must be skipped, not re-copied")
        // version.json is committed crash-aware now: temp upload under .watermelon, then publish-by-move.
        XCTAssertTrue(rerun.contains { $0.hasPrefix("/photos/.watermelon/") && $0.hasSuffix(".json.tmp") },
                       "rerun still re-commits version.json via its temp publish")
    }

    func testSourceChangedDuringMigrationSurfacesLocalizedLiteError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3, fileName: "before.jpg", contentHash: Data([0xAB]))
        let changedSource = try makeLegacyTimestampManifestData()
        await client.setOnMove { _, to in
            if to == self.liteMonthPath(2024, 3) {
                await client.seedFile(path: self.v1ManifestPath(2024, 3), data: changedSource)
            }
        }

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a V1 source changed after publish must fail closed before version commit")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .v1SourceChangedDuringMigration)
            XCTAssertFalse(error.localizedDescription.contains("v1SourceChangedDuringMigration"))
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when V1 source changes during migration")
    }

    func testVersionCommitFailureRetainsV1Manifest() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnMove { _, to in
            if to == self.liteMonthPath(2024, 3) {
                await client.enqueueMoveError(RemoteErrorFixtures.terminal)
            }
        }

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a version publish failure must abort before post-commit pruning")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .versionCommitFailed)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when publish fails")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "pre-commit failure must retain the V1 manifest")
    }

    // MARK: - Cancellation preservation (M01 / M02)

    func testCancellationDuringVersionCommitIsNotVersionCommitFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        // Publish the month + commit once, then drop version.json so a rerun re-commits.
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        try await client.delete(path: versionPath())
        // The rerun fast-skips the month (metadata match, no download), so the commit read-back is the
        // first/only download — script it cancelled.
        await client.enqueueDownloadError(RemoteErrorFixtures.cancelled)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a cancelled version commit must surface as cancellation")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled, "cancellation must not be wrapped as versionCommitFailed")
            XCTAssertNil(error as? LiteRepoError, "cancellation must not surface as a LiteRepoError")
        }
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "cancelled version commit must retain the V1 manifest")
    }

    func testCancellationDuringSourceDownloadIsNotMonthManifestUnreadable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        // The final Lite month does not exist → copy path; the source download is the first download.
        await client.enqueueDownloadError(RemoteErrorFixtures.cancelled)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a cancelled source download must surface as cancellation")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
            XCTAssertNil(error as? LiteRepoError, "cancellation must not be wrapped as a localized migration failure")
        }
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "no commit when the source download is cancelled")
    }

    func testCancellationDuringFinalValidationIsNotMonthManifestUnreadable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let sourceData = await client.fileData(path: v1ManifestPath(2024, 3))
        let sourceBytes = try XCTUnwrap(sourceData)
        // Source download succeeds with the real bytes; the final-validation download is cancelled.
        await client.enqueueDownloadData(sourceBytes)
        await client.enqueueDownloadError(RemoteErrorFixtures.cancelled)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a cancelled final validation must surface as cancellation")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
            XCTAssertNil(error as? LiteRepoError, "cancellation must not be wrapped as a localized migration failure")
        }
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "no commit when final validation is cancelled")
        let finalData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(finalData, "the moved final remains recoverable for the next migration attempt")
    }

    func testCancellationBetweenMonthsStopsLaterWork() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3, fileName: "a.jpg", contentHash: Data([0xAB]))
        try await seedRealV1Month(client: client, year: 2024, month: 5, fileName: "b.jpg", contentHash: Data([0xCD]))
        let box = MigrationTaskBox()
        // Cancel as the first month's publish move lands; the next month's pre-check must stop the run.
        await client.setOnMove { _, to in
            if to == self.liteMonthPath(2024, 3) { await box.cancel() }
        }
        let task = Task<Void, Error> {
            try await V1ToLiteMigration(client: client, basePath: self.basePath).run(createdAt: "t", createdBy: "id")
        }
        await box.store(task)

        do {
            try await task.value
            XCTFail("a cancelled migration must stop")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .cancelled)
        }

        let month3 = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(month3, "the first month finished before cancellation")
        let month5 = await client.fileData(path: liteMonthPath(2024, 5))
        XCTAssertNil(month5, "later months must not be migrated after cancellation")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when cancelled mid-run")
    }

    // MARK: - Idempotent rerun via byte equality (M03)

    func testMatchingFinalBytesSkipReuploadAfterValidationDownload() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        try await client.delete(path: versionPath())   // rerun still routes .v1Migrate
        let finalPath = liteMonthPath(2024, 3)
        let downloadsBefore = (await client.downloadAttemptPaths).filter { $0 == finalPath }.count
        let uploadsBefore = (await client.uploadedPaths).filter { $0 == finalPath }.count

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let downloadsAfter = (await client.downloadAttemptPaths).filter { $0 == finalPath }.count
        XCTAssertGreaterThan(downloadsAfter, downloadsBefore,
                             "a candidate final must be downloaded so migration can prove byte equality with the V1 source")
        let uploadsAfter = (await client.uploadedPaths).filter { $0 == finalPath }.count
        XCTAssertEqual(uploadsAfter, uploadsBefore, "a byte-identical final must not be reuploaded")
        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "the migrated month remains")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "the rerun re-commits version.json")
    }

    func testFinalSizeMismatchFallsBackToValidationAndRepair() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let finalPath = liteMonthPath(2024, 3)
        // A present-but-invalid final whose size does NOT match the source manifest.
        await client.seedFile(path: finalPath, data: Data([0x01, 0x02, 0x03, 0x04, 0x05]))
        let downloadsBefore = (await client.downloadAttemptPaths).filter { $0 == finalPath }.count

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let downloadsAfter = (await client.downloadAttemptPaths).filter { $0 == finalPath }.count
        XCTAssertGreaterThan(downloadsAfter, downloadsBefore,
                             "a size-mismatched final must be fully validated, not skipped on metadata")
        let migrated = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite, pushSchemaUpgrade: false
        )
        XCTAssertNotNil(migrated, "the invalid final is repaired into a valid Lite manifest")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "the run commits after repair")
    }

    func testLoadableFinalWithDifferentBytesFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3, fileName: "v1.jpg", contentHash: Data([0xAB]))
        let liteStore = try await MonthManifestStore.loadOrCreate(
            client: client,
            basePath: basePath,
            year: 2024,
            month: 3,
            layout: .lite,
            assertOwnership: {}
        )
        try liteStore.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0xCD]), fileName: "lite.jpg")
        )
        _ = try await liteStore.flushToRemote()
        let finalPath = liteMonthPath(2024, 3)
        let originalFinalData = await client.fileData(path: finalPath)
        let originalFinal = try XCTUnwrap(originalFinalData)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a loadable Lite final with different bytes must not be overwritten from V1")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
            XCTAssertTrue(error.localizedDescription.contains("2024-03"))
            XCTAssertFalse(error.localizedDescription.contains("existingLiteManifestConflict"))
        }

        let finalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(finalAfter, originalFinal, "conflicting Lite final must survive unchanged")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit after a Lite/V1 month conflict")
    }

    func testDirectoryValuedFinalFailsClosedWithoutDelete() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let finalPath = liteMonthPath(2024, 3)
        await client.seedDirectory(finalPath)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a directory at the Lite final manifest path must not be repaired by delete")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
            XCTAssertTrue(error.localizedDescription.contains("2024-03"))
            XCTAssertFalse(error.localizedDescription.contains("existingLiteManifestConflict"))
        }

        let stillDirectory = try await client.exists(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertTrue(stillDirectory, "directory-valued final path must survive unchanged")
        XCTAssertFalse(deleted.contains(finalPath), "migration must not delete a directory-valued final path")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit after a Lite final path conflict")
    }

    func testFinalDownloadFaultFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let finalPath = liteMonthPath(2024, 3)
        let sourceData = await client.fileData(path: v1ManifestPath(2024, 3))
        let sourceBytes = try XCTUnwrap(sourceData)
        await client.seedFile(path: finalPath, data: sourceBytes)
        await client.enqueueDownloadData(sourceBytes)              // source download
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)   // final validation download

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a final validation download fault must surface")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
        }

        let finalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(finalAfter, sourceBytes, "final must not be deleted or replaced after a validation fault")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit after a final validation fault")
    }

    func testNonNotFoundFinalMetadataFaultFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        // A retryable (non-not-found) metadata fault on the final Lite month probe must surface.
        await client.failMetadata(
            forPathSuffix: "/.watermelon/months/2024-03.sqlite",
            error: RemoteErrorFixtures.retryable
        )

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a non-not-found final metadata fault must fail closed")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable,
                           "the final metadata fault must surface, not read as absence")
        }
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "no commit when the final probe faults")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(monthData, "no publish when the final probe faults")
    }

    // MARK: - Resource-path preservation

    func testResourcePathsPreservedAndDataBytesNotMoved() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        await client.seedFile(path: "\(basePath)/2024/03/IMG_0001.JPG", data: Data([0xDE, 0xAD]))
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01]), fileName: "IMG_0001.JPG")
        )
        _ = try await store.flushToRemote()
        let writerID = newWriterID()
        let movesBefore = await client.movedPaths

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        await plan.session.stopAndRelease()

        let dataFile = await client.fileData(path: "\(basePath)/2024/03/IMG_0001.JPG")
        XCTAssertNotNil(dataFile, "data resource path preserved")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNil(v1Manifest, "old V1 manifest is pruned after the Lite commit validates")
        let marker = await client.fileData(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        XCTAssertNil(marker, "successful post-commit prune clears the retry marker")

        let migrationMoves = Array((await client.movedPaths).dropFirst(movesBefore.count))
        XCTAssertTrue(migrationMoves.contains { $0.to.hasPrefix("/photos/.watermelon/months/") },
                      "migration must publish at least one Lite month manifest")
        for move in migrationMoves {
            // Month-manifest publishes and the crash-aware version.json publish are the only moves; data
            // bytes under <YYYY>/<MM> are never moved, so every move stays under .watermelon/.
            XCTAssertTrue(move.from.hasPrefix("/photos/.watermelon/"), "only Lite metadata temps move, got \(move.from)")
            XCTAssertTrue(move.to.hasPrefix("/photos/.watermelon/"), "only publishes into Lite metadata, got \(move.to)")
        }
    }

    func testPostCommitPruneDeleteFailureRetainsCommittedLiteRepo() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "delete failure must not roll back version.json")
        let liteManifest = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(liteManifest, "delete failure must not roll back the Lite month")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "delete failure leaves the old V1 manifest for later cleanup")
        let marker = await client.fileData(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        XCTAssertNotNil(marker, "failed post-commit prune leaves a marker so a later current cleanup retries")
        await plan.session.stopAndRelease()
    }

    func testPruneMarkerWriteFailureAbortsBeforeVersionCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.failUpload(
            forPathSuffix: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath),
            error: RemoteErrorFixtures.retryable
        )

        do {
            _ = try await LiteRepoGateway.prepareForegroundWrite(
                client: client,
                lockClient: client,
                basePath: basePath,
                writerID: newWriterID()
            )
            XCTFail("marker write failure must abort before version.json commits")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .versionCommitFailed)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "a failed prune marker write must not leave a committed Lite repo without retry provenance")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "pre-commit marker failure must retain the V1 recovery source")
    }

    func testPostCommitPruneRetainsV1ManifestWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        let writerID = newWriterID()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnDownload { path in
            if path == self.versionPath() {
                await client.removeLock(basePath: self.basePath, writerID: writerID)
            }
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: writerID
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "version.json already committed before prune ownership loss")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "lost ownership before delete must retain the V1 manifest")
        await plan.session.stopAndRelease()
    }

    func testPostCommitPruneRetainsV1ManifestWhenLiteManifestMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnDownload { path in
            if path == self.versionPath() {
                try? await client.delete(path: self.liteMonthPath(2024, 3))
            }
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "version.json already committed before prune validation")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "missing Lite manifest must retain the V1 manifest")
        await plan.session.stopAndRelease()
    }

    func testPostCommitPruneRetainsV1ManifestWhenVersionDisappearsBeforeDelete() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnDownload { path in
            if path == self.liteMonthPath(2024, 3) {
                try? await client.delete(path: self.versionPath())
            }
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "test hook removes version.json after commit but before final prune delete")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "version disappearance before delete must preserve the V1 recovery source")
        let marker = await client.fileData(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        XCTAssertNotNil(marker, "marker remains because prune could not safely finish")
        await plan.session.stopAndRelease()
    }

    func testPostCommitPruneRetainsV1ManifestWhenManifestReadFails() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnDownload { path in
            if path == self.versionPath() {
                await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
            }
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "version.json already committed before prune validation")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "read failure must retain the V1 manifest")
        await plan.session.stopAndRelease()
    }

    func testPostCommitPruneRetainsV1ManifestWhenBytesDiffer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        await client.setOnDownload { path in
            if path == self.versionPath() {
                await client.seedFile(path: self.liteMonthPath(2024, 3), data: Data([0x01, 0x02]))
            }
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionData, "version.json already committed before prune validation")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "byte mismatch must retain the V1 manifest")
        await plan.session.stopAndRelease()
    }

    func testPostCommitPruneRetainsV1ManifestWhenV1ChangesAfterInitialValidation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let changedV1Data = try makeLegacyTimestampManifestData()
        await client.setOnDownload { path in
            guard path == self.liteMonthPath(2024, 3) else { return }
            guard await client.fileData(path: self.versionPath()) != nil else { return }
            await client.setOnDownload(nil)
            await client.seedFile(path: self.v1ManifestPath(2024, 3), data: changedV1Data)
        }

        let plan = try await LiteRepoGateway.prepareForegroundWrite(
            client: client,
            lockClient: client,
            basePath: basePath,
            writerID: newWriterID()
        )

        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertEqual(v1Manifest, changedV1Data, "a V1 rewrite after the first validation must fail the final hash proof")
        let marker = await client.fileData(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        XCTAssertNotNil(marker, "marker remains so later cleanup can retry only if the V1 source matches again")
        await plan.session.stopAndRelease()
    }

    // MARK: - TOCTOU re-read

    func testPostLockRouteReReadHandlesTOCTOU() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()
        // TOCTOU: by the time we hold the lock a concurrent writer committed a Lite version.json. The
        // post-lock re-read sees that committed marker after the lock-body proof completes.
        let committed = VersionManifestLite.makeManifest(createdAt: "x", createdBy: "other")
        let committedData = try VersionManifestLite.encode(committed)
        let ownLockPath = try XCTUnwrap(RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID))
        let committedVersionPath = versionPath()
        await client.setOnDownload { path in
            if path == ownLockPath {
                await client.seedFile(path: committedVersionPath, data: committedData)
            }
        }
        let uploadsBefore = await client.uploadedPaths

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan.layout, .lite)
        await plan.session.stopAndRelease()

        let uploads = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        XCTAssertFalse(uploads.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "post-lock .current must skip month migration copies")
        XCTAssertFalse(uploads.contains(versionPath()),
                       "post-lock .current must not rewrite version.json")
    }

    // The migration is driven by the under-lock decision, not a fresh re-probe. The version reads are
    // under-lock reclassify, commit safety check, commit read-back, and the final pre-prune current check.
    func testForegroundV1MigrateConsumesUnderLockDecisionWithoutThirdClassify() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan.layout, .lite)
        await plan.session.stopAndRelease()

        // Migration ran off the under-lock decision: month relocated + version committed.
        let migratedMonth = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(migratedMonth, "the under-lock .v1Migrate decision drove the migration")
        let committedVersion = await client.fileData(path: versionPath())
        XCTAssertNotNil(committedVersion, "migration committed version.json")

        let versionProbes = (await client.downloadAttemptPaths).filter { $0 == versionPath() }
        XCTAssertEqual(versionProbes.count, 4, "version.json probes must stay to migration commit plus final pre-prune proof")
    }

    // MARK: - Ownership fail-closed

    func testOwnershipLossBeforePublishFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath, assertOwnership: { throw LiteRepoError.ownershipLost })
                .run(createdAt: "t", createdBy: "id")
            XCTFail("lost ownership before publish must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let publishedMonth = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(publishedMonth, "no month published when ownership is lost before publish")
        let committedVersion = await client.fileData(path: versionPath())
        XCTAssertNil(committedVersion, "no version commit when ownership is lost")
    }

    func testOwnershipLossBeforeVersionCommitFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        // Pre-publish the valid Lite month so every month is skipped and only the commit gate remains.
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        try await client.delete(path: versionPath())

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath, assertOwnership: { throw LiteRepoError.ownershipLost })
                .run(createdAt: "t", createdBy: "id")
            XCTFail("lost ownership before the version commit must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit after ownership loss")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(monthData, "the previously-migrated month remains")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "ownership loss before commit must retain the V1 manifest")
    }

    func testOwnershipLossBeforeDeletingInvalidFinalPreservesIt() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let invalidFinal = Data([0x01, 0x02])
        await client.seedFile(path: liteMonthPath(2024, 3), data: invalidFinal)
        let gate = MigrationOwnershipGate([false])

        do {
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: {
                    if await gate.next() == false { throw LiteRepoError.ownershipLost }
                }
            ).run(createdAt: "t", createdBy: "id")
            XCTFail("ownership loss before removing an invalid final must stop before publishing replacement")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when month publish ownership is lost")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertEqual(monthData, invalidFinal, "invalid final must not be deleted after ownership is lost")
    }

    func testOwnershipLossAfterDeletingInvalidFinalDoesNotPublishReplacement() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let invalidFinal = Data([0x01, 0x02])
        await client.seedFile(path: liteMonthPath(2024, 3), data: invalidFinal)
        let gate = MigrationOwnershipGate([true, false])

        do {
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: {
                    if await gate.next() == false { throw LiteRepoError.ownershipLost }
                }
            ).run(createdAt: "t", createdBy: "id")
            XCTFail("ownership loss after deleting an invalid final must stop before publishing replacement")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when month publish ownership is lost")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(monthData, "replacement Lite month must not publish after ownership is lost")
        let sourceData = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(sourceData, "the source V1 manifest remains as the recovery path")
    }

    func testOwnershipLossDuringVersionPublishFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        try await client.delete(path: versionPath())
        let gate = MigrationOwnershipGate([true, false])

        do {
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: {
                    if await gate.next() == false { throw LiteRepoError.ownershipLost }
                }
            ).run(createdAt: "t", createdBy: "id")
            XCTFail("lost ownership during version publish must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when ownership is lost during publish")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(monthData, "the previously-migrated month remains")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNotNil(v1Manifest, "ownership loss during version publish must retain the V1 manifest")
    }

    // MARK: - One true real-SQLite migration on a disk-backed remote

    func testTrueV1RepositoryMigrationSucceedsOnDiskRemote() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-v1lite-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let client = DiskBackedRemoteStorageClient(rootURL: root)
        let writerID = newWriterID()

        // Real V1 month: actual MonthManifestStore SQLite content plus a data resource under YYYY/MM.
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        // Stage the source outside `root`: DiskBackedRemoteStorageClient maps remote paths back into
        // `root`, so a source living at the remote's on-disk location would collide with its own upload.
        let dataURL = FileManager.default.temporaryDirectory.appendingPathComponent("seed_\(UUID().uuidString).JPG")
        try Data([0xDE, 0xAD, 0xBE, 0xEF]).write(to: dataURL)
        defer { try? FileManager.default.removeItem(at: dataURL) }
        try await client.upload(
            localURL: dataURL,
            remotePath: "\(basePath)/2024/03/IMG_0001.JPG",
            respectTaskCancellation: false,
            onProgress: nil
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01, 0x02]), fileName: "IMG_0001.JPG")
        )
        _ = try await store.flushToRemote()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan.layout, .lite)
        await plan.session.stopAndRelease()

        let fm = FileManager.default
        func exists(_ rel: String) -> Bool { fm.fileExists(atPath: root.appendingPathComponent(rel).path) }
        XCTAssertTrue(exists("photos/.watermelon/version.json"), "version.json committed")
        XCTAssertTrue(exists("photos/.watermelon/months/2024-03.sqlite"), "month relocated to the Lite path")
        XCTAssertFalse(exists("photos/2024/03/.watermelon_manifest.sqlite"), "old V1 manifest pruned after migration commit")
        XCTAssertTrue(exists("photos/2024/03/IMG_0001.JPG"), "data resource untouched")

        // The relocated Lite manifest is a valid manifest carrying the real resource.
        let migrated = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite, pushSchemaUpgrade: false
        )
        let liteStore = try XCTUnwrap(migrated)
        XCTAssertNotNil(liteStore.findByFileName("IMG_0001.JPG"), "Lite manifest preserves the migrated resource")

        // A subsequent foreground prepare now routes as .current and re-copies nothing.
        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()
    }
}

// Holds the in-flight migration Task so a client hook can cancel it mid-run.
private actor MigrationTaskBox {
    private var task: Task<Void, Error>?
    func store(_ task: Task<Void, Error>) { self.task = task }
    func cancel() { task?.cancel() }
}

private actor MigrationOwnershipGate {
    private var values: [Bool]

    init(_ values: [Bool]) {
        self.values = values
    }

    func next() -> Bool {
        if values.isEmpty { return false }
        return values.removeFirst()
    }
}
