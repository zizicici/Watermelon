import XCTest
import GRDB
@testable import Watermelon

/// Unit-024 M5 first slice: post-open inspection sharing across the V2 runtime lease and the
/// sync gate. Behavior-preservation tests covering each open action — only `.openExistingV2`
/// publishes a non-nil `postOpenSyncInspection`; bootstrap / migration / cleanup paths publish
/// `nil` and force sync to re-inspect the post-mutation remote.
final class BackupV2InspectionSharingTests: XCTestCase {
    private let basePath = "/repo"
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    private var activeServices: BackupV2RuntimeServices?

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDown() async throws {
        if let services = activeServices {
            await services.shutdown()
            activeServices = nil
        }
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    // MARK: - .openExistingV2 — publishes inspection, sync skips re-inspect

    func testOpenExistingV2_publishesPostOpenSyncInspection_v2() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let canonicalRepoID = "11111111-2222-3333-4444-555555555555"
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        activeServices = services

        XCTAssertEqual(
            services.postOpenSyncInspection,
            .v2(formatVersion: RepoLayout.formatVersion),
            ".openExistingV2 must publish the pre-open inspection (which equals the post-open shape)"
        )
        // Sync consumes the shared inspection and routes via .v2 (allowPreMaterialized: true).
        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            preInspection: services.postOpenSyncInspection,
            expectV2: true,
            localRepoID: services.repoID
        )
    }

    // MARK: - .bootstrapFresh — publishes nil, sync re-inspects post-bootstrap state

    func testBootstrapFresh_publishesNil_andSyncReinspectsToV2() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        var bootstrapCalled = false
        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: true,
            onBootstrap: { bootstrapCalled = true }
        )
        activeServices = services

        XCTAssertTrue(bootstrapCalled, "preflight: fresh path must take the bootstrap branch")
        XCTAssertNil(
            services.postOpenSyncInspection,
            ".bootstrapFresh writes version.json; the pre-open .fresh inspection is unsafe to share"
        )

        // Forward services.postOpenSyncInspection (== nil) to sync. Sync must re-inspect,
        // observe .v2 post-bootstrap, and NOT throw damagedV2Repo.
        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            preInspection: services.postOpenSyncInspection,
            expectV2: true,
            localRepoID: services.repoID
        )
    }

    // MARK: - .migrateFromV1 — publishes nil, sync re-inspects post-migration state

    func testMigrateFromV1_publishesNil_andSyncReinspectsToV2() async throws {
        // Single InMemoryRemoteStorageClient stands in for the same remote — data and
        // metadata clients diverge only in production where they're separate connections
        // to the same backend. Keeping them collocated here lets cleanup/migration writes
        // performed via the metadata client be visible to syncIndex via the data client.
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        // V1 layout: real V1 sqlite manifest so V1MigrationService.runFullMigration can
        // complete. A synthetic 1-byte sentinel fails sqlite parsing and never reaches the
        // post-migration state under test.
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: TestFixtures.fingerprint(0xA1),
            resourceHash: TestFixtures.fingerprint(0xB1),
            logicalName: "IMG_M1.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        let profile = try insertProfile()

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: client,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: true
        )
        activeServices = services

        XCTAssertNil(
            services.postOpenSyncInspection,
            ".migrateFromV1 writes version.json and rewrites V1 manifests; pre-open .v1 inspection is unsafe to share"
        )

        // Sync must re-inspect and observe .v2 post-migration; must NOT throw requiresForegroundMigration.
        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            preInspection: services.postOpenSyncInspection,
            expectV2: true,
            localRepoID: services.repoID
        )
    }

    // MARK: - .migrateFromV1 via .v2WithV1Manifests — publishes nil, sync re-inspects

    func testMigrateFromV2WithV1Manifests_publishesNil_andSyncReinspectsToV2() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        // V2 version marker AND lingering V1 manifest → inspect route .v2WithV1Manifests → action .migrateFromV1.
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: TestFixtures.fingerprint(0xA2),
            resourceHash: TestFixtures.fingerprint(0xB2),
            logicalName: "IMG_M2.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2024, 12)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        let profile = try insertProfile()

        // Pin the inspect route — defensive: if a future change reroutes this shape away from
        // .v2WithV1Manifests, the test would silently pass for the wrong reason.
        let inspection = try await RemoteFormatCompatibilityService().inspectRemoteFormat(
            client: client, profile: profile
        )
        guard case .v2WithV1Manifests = inspection else {
            XCTFail("preflight: expected .v2WithV1Manifests inspection, got \(inspection)")
            return
        }

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: client,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: true
        )
        activeServices = services

        XCTAssertNil(
            services.postOpenSyncInspection,
            ".v2WithV1Manifests routes through full migration; pre-open inspection is unsafe to share"
        )

        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            preInspection: services.postOpenSyncInspection,
            expectV2: true,
            localRepoID: services.repoID
        )
    }

    // MARK: - .openWithCleanupV2 — publishes nil, sync re-inspects post-cleanup

    func testOpenWithCleanupV2_publishesNil_andSyncReinspectsToV2() async throws {
        // Cleanup's identity-publish uses the metadata client's RepoBootstrap. Production
        // sees the same remote on both connections; here, a single shared InMemoryRemoteStorageClient
        // stands in.
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let cleanupWriterID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: cleanupWriterID)
        let markerDict: [String: Any] = [
            "v": 2,
            "writer_id": cleanupWriterID,
            "run_id": "stale-run",
            "phase": 1,
            "started_at_ms": Int64(0),
            "last_step_at_ms": Int64(0)
        ]
        let markerData = try JSONSerialization.data(withJSONObject: markerDict)
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: cleanupWriterID),
            data: markerData
        )
        let profile = try insertProfile()

        // Pin the inspect route — cleanup arm requires the marker filename to round-trip.
        let inspection = try await RemoteFormatCompatibilityService().inspectRemoteFormat(
            client: client, profile: profile
        )
        guard case .v2WithPendingMigrationCleanup(_, let ownerWriterID) = inspection,
              ownerWriterID == cleanupWriterID else {
            XCTFail("preflight: expected .v2WithPendingMigrationCleanup for owner \(cleanupWriterID), got \(inspection)")
            return
        }

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: client,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        activeServices = services

        XCTAssertNil(
            services.postOpenSyncInspection,
            ".openWithCleanupV2 removes the migration-in-progress marker; pre-open inspection no longer matches"
        )

        // Sync must re-inspect and observe plain .v2 post-cleanup.
        let remoteIndexService = RemoteIndexSyncService()
        _ = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            preInspection: services.postOpenSyncInspection,
            expectV2: true,
            localRepoID: services.repoID
        )
    }

    // MARK: - Cleanup path: pin current preMaterialized behavior

    func testOpenWithCleanupV2_initialMaterializeOutput_isPopulated() async throws {
        // Per Codex re-review non-blocking note: cleanup path currently materializes
        // unconditionally after cleanup, so `initialMaterializeOutput` is populated.
        // This test pins that current behavior; if a future refactor skips the post-cleanup
        // materialize, this assertion fails loud so the change is reviewed explicitly.
        // Single shared client mirrors the same-remote/two-connection production layout.
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let cleanupWriterID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: cleanupWriterID)
        let markerDict: [String: Any] = [
            "v": 2,
            "writer_id": cleanupWriterID,
            "run_id": "stale-run",
            "phase": 1,
            "started_at_ms": Int64(0),
            "last_step_at_ms": Int64(0)
        ]
        let markerData = try JSONSerialization.data(withJSONObject: markerDict)
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: cleanupWriterID),
            data: markerData
        )
        let profile = try insertProfile()

        let services = try await BackupV2RuntimeBuilder.build(
            client: client,
            metadataClient: client,
            profile: profile,
            databaseManager: databaseManager,
            allowMigration: false
        )
        activeServices = services

        let output = await services.initialMaterializeOutput.peek()
        XCTAssertNotNil(output,
                        "Cleanup path materializes after marker removal — pin current behavior")
    }

    // MARK: - Source-scanning regression: verify path does NOT share

    func testProductionVerifyMonth_syncIndexCalls_doNotPassPreInspection() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackupRunPreparation.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        // Slice the verifyMonthV2 function body so we don't accidentally match prepareRun's call.
        guard let verifyRange = source.range(of: "func verifyMonthV2(") else {
            XCTFail("could not locate verifyMonthV2 in BackupRunPreparation.swift")
            return
        }
        // Look for the next 'private func' or end-of-file as the function's tail boundary.
        let after = source[verifyRange.upperBound...]
        let tail = after.range(of: "private func ")?.lowerBound ?? after.endIndex
        let verifyBody = String(after[..<tail])

        let syncIndexCalls = verifyBody.components(separatedBy: "remoteIndexService.syncIndex(")
        // Two splits → at least one occurrence; we expect two production sites in verifyMonthV2.
        XCTAssertGreaterThanOrEqual(
            syncIndexCalls.count - 1, 1,
            "verifyMonthV2 should contain at least one remoteIndexService.syncIndex( call"
        )
        XCTAssertFalse(
            verifyBody.contains("preInspection:"),
            "Anti-scope: verifyMonthV2 must NOT pass preInspection to syncIndex (verify path keeps its independent inspection lifecycle)"
        )
    }

    // MARK: - Source-scanning regression: FG/BG pass postOpenSyncInspection

    func testProductionPrepareRun_passesPostOpenSyncInspectionToSyncIndex() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackupRunPreparation.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        // Slice the prepareRun function body (it's the first function after the struct's stored properties).
        guard let prepareRange = source.range(of: "func prepareRun(") else {
            XCTFail("could not locate prepareRun in BackupRunPreparation.swift")
            return
        }
        let after = source[prepareRange.upperBound...]
        // Use the next-func boundary (reloadRemoteIndex follows prepareRun).
        let tail = after.range(of: "func reloadRemoteIndex")?.lowerBound ?? after.endIndex
        let prepareBody = String(after[..<tail])

        XCTAssertTrue(
            prepareBody.contains("preInspection: v2Services?.postOpenSyncInspection"),
            "prepareRun must forward v2Services?.postOpenSyncInspection as preInspection to syncIndex"
        )
    }

    func testProductionBackgroundBackupRunner_passesPostOpenSyncInspectionToSyncIndex() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
        let url = root.appendingPathComponent("Watermelon/Services/Backup/BackgroundBackupRunner.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        XCTAssertTrue(
            source.contains("preInspection: v2Services.postOpenSyncInspection"),
            "BackgroundBackupRunner must forward v2Services.postOpenSyncInspection as preInspection to syncIndex"
        )
    }

    // MARK: - Helpers

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }

    /// Mirrors `V1MigrationServiceTests.buildV1ManifestSqlite` — V1MigrationService requires
    /// a real sqlite database at the V1 manifest path; a sentinel byte fails sqlite parsing
    /// and never reaches the post-migration state under test.
    private static func buildV1ManifestSqlite(
        assetFingerprint: Data,
        resourceHash: Data,
        logicalName: String
    ) throws -> Data {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }
        let dbURL = tempDir.appendingPathComponent("v1.sqlite")
        let dbQueue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(dbQueue)
        try dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO resources (fileName, contentHash, fileSize, resourceType, creationDateMs, backedUpAtMs)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                arguments: [logicalName, resourceHash, Int64(2048), ResourceTypeCode.photo, Int64(1_700_000_000_000), Int64(1_700_000_001_000)]
            )
            try db.execute(
                sql: """
                INSERT INTO assets (assetFingerprint, creationDateMs, backedUpAtMs, resourceCount, totalFileSizeBytes)
                VALUES (?, ?, ?, ?, ?)
                """,
                arguments: [assetFingerprint, Int64(1_700_000_000_000), Int64(1_700_000_001_000), 1, Int64(2048)]
            )
            try db.execute(
                sql: """
                INSERT INTO asset_resources (assetFingerprint, resourceHash, role, slot)
                VALUES (?, ?, ?, ?)
                """,
                arguments: [assetFingerprint, resourceHash, ResourceTypeCode.photo, 0]
            )
        }
        try dbQueue.close()
        return try Data(contentsOf: dbURL)
    }
}
