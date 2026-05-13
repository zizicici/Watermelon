import XCTest
import GRDB
@testable import Watermelon

/// V1→V2 migration tests:
///   - scanV1Months propagates list errors (silent skip + phase3 delete = data loss)
///   - phase2 writes version.json + flips migrationCompleted
///   - phase3 deletes scanned V1 manifests
///   - phase1 e2e: real V1 sqlite → V2 commits/snapshots → re-materializes back to
///     equivalent state. Only test exercising the V1→V2 schema mapping path.
final class V1MigrationServiceTests: XCTestCase {
    private let basePath = "/repo"
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    func testScanV1MonthsReturnsAllManifestMonths() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 1)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 12)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let service = makeService(client: client)
        let scanned = try await service.scanV1Months()
        let monthKeys = scanned.map { "\($0.year)-\($0.month)" }.sorted()
        XCTAssertEqual(monthKeys, ["2024-1", "2024-12", "2025-6"])
    }

    /// `try?` on subdirectory list silently skipped months, then phase3's retry-scan
    /// would delete those manifests as orphans — propagate the error instead.
    func testScanV1MonthsSurfacesListErrors() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 1)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        await client.injectListError(.transport, for: "\(basePath)/2025")

        let service = makeService(client: client)
        do {
            _ = try await service.scanV1Months()
            XCTFail("expected list error to propagate so phase3 won't delete an unmigrated month")
        } catch {
            // expected
        }
    }

    func testPhase2WritesVersionJSONAndMarksMigrationCompleted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Pre-write repo.json (production builder writes it before phase1; tests stand in).
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "r", writerID: "w")

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        try await service.runPhase2(profileID: profileID, repoID: "r", writerID: "w", runID: "run-001")

        let exists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(exists, "phase2 must write version.json")

        let state = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(state?.migrationCompleted, 1, "phase2 must mark migration complete")
    }

    /// End-to-end phase1: V1 manifest → V2 commits/snapshots → re-materialize equals input.
    func testPhase1_v1ManifestRoundTripsThroughV2() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let logicalName = "IMG_0001.HEIC"

        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP,
            resourceHash: contentHash,
            logicalName: logicalName
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "test-repo-id"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-001"
        )
        XCTAssertEqual(processed, 1)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[LibraryMonthKey(year: 2025, month: 6)])
        let asset = try XCTUnwrap(monthState.assets[assetFP], "V1 asset must be visible after V2 materialize")
        let resourcePath = "2025/06/\(logicalName)"
        XCTAssertEqual(monthState.resources[resourcePath]?.contentHash, contentHash)
        let arKey = AssetResourceKey(assetFingerprint: assetFP, role: ResourceTypeCode.photo, slot: 0)
        XCTAssertEqual(monthState.assetResources[arKey]?.resourceHash, contentHash)
        // Migration writes V2 snapshot rows; stamp is required so future stale-add
        // replays against this baseline gate via opStampPrecedes.
        let stamp = try XCTUnwrap(asset.stamp, "migration-produced snapshot row must carry an OpStamp")
        XCTAssertEqual(stamp.writerID, writerID)
        XCTAssertGreaterThan(stamp.seq, 0)
        XCTAssertGreaterThan(stamp.clock, 0)
    }

    func testPhase1WritesMigrationMarkerNotVersionJSON() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP,
            resourceHash: contentHash,
            logicalName: "IMG_0001.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(
            profileID: profileID, repoID: "r", writerID: "w", runID: "run-001"
        )

        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertFalse(versionExists,
                       "phase1 must NOT write version.json — a crashed migration would otherwise look like clean V2")
        let markerExists = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "w"))
        XCTAssertTrue(markerExists,
                      "phase1 must write writer-unique migration marker so inspect can route back to .v1")
    }

    func testPhase3DeletesScannedManifests() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let m1Bytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x01), resourceHash: TestFixtures.fingerprint(0x11), logicalName: "IMG_1.HEIC")
        let m2Bytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x02), resourceHash: TestFixtures.fingerprint(0x22), logicalName: "IMG_2.HEIC")
        let path1 = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2024, 3)
        let path2 = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 7)
        await client.injectFile(path: path1, data: m1Bytes)
        await client.injectFile(path: path2, data: m2Bytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(profileID: profileID, repoID: "r", writerID: "w", runID: "run-1")
        try await service.runPhase3(writerID: "w", runID: "run-1")

        let m1 = await client.hasFile(path1)
        let m2 = await client.hasFile(path2)
        XCTAssertFalse(m1)
        XCTAssertFalse(m2)
    }

    /// Phase3 only deletes manifests phase1 scanned — V1 data written between phase1 and
    /// phase3 by an older peer must survive into the next migration cycle.
    func testPhase3PreservesManifestsAddedAfterPhase1Scan() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x01), resourceHash: TestFixtures.fingerprint(0x11), logicalName: "IMG_1.HEIC")
        let scannedPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2024, 3)
        await client.injectFile(path: scannedPath, data: bytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(profileID: profileID, repoID: "r", writerID: "w", runID: "run-1")

        let lateBytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x02), resourceHash: TestFixtures.fingerprint(0x22), logicalName: "IMG_2.HEIC")
        let postScanPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 7)
        await client.injectFile(path: postScanPath, data: lateBytes)

        try await service.runPhase3(writerID: "w", runID: "run-1")

        let scannedRemoved = await client.hasFile(scannedPath)
        let lateSurvived = await client.hasFile(postScanPath)
        XCTAssertFalse(scannedRemoved)
        XCTAssertTrue(lateSurvived)
    }

    /// Phase3 retried on a fully-cleaned-up repo must not throw — `migrations/<writerID>.json`
    /// already gone, no V1 manifests left. Critical for the builder's "completed=1 + .v1 routing"
    /// retry branch: we re-enter phase3 only when ownsMigrationMarker says it's safe.
    func testPhase3IsIdempotent_secondInvocation_noop() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 3)

        let service = makeService(client: client)
        try await service.runPhase3(writerID: "test-writer", runID: "run-1")
        try await service.runPhase3(writerID: "test-writer", runID: "run-1")
    }

    /// ownsMigrationMarker discriminates "our cleanup failed" from "real V2→V1 regression".
    /// True only after phase1 wrote a marker for OUR writerID; false after phase3 cleared it.
    func testOwnsMigrationMarker_reflectsPhase3Cleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: "w")
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent("m.json")
        try Data("{}".utf8).write(to: temp, options: .atomic)
        defer { try? FileManager.default.removeItem(at: temp) }
        _ = try await client.atomicCreate(localURL: temp, remotePath: markerPath, respectTaskCancellation: false)

        let service = makeService(client: client)
        let beforeCleanup = try await service.ownsMigrationMarker(writerID: "w")
        XCTAssertTrue(beforeCleanup)

        try await service.runPhase3(writerID: "w", runID: "run-1")
        let afterCleanup = try await service.ownsMigrationMarker(writerID: "w")
        XCTAssertFalse(afterCleanup, "phase3 must remove marker so routing flips to .v2")
    }

    // MARK: - Helpers

    private func makeService(
        client: InMemoryRemoteStorageClient,
        profileID: Int64 = 1
    ) -> V1MigrationService {
        let identity = RepoIdentity(database: databaseManager)
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        return V1MigrationService(
            client: client,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            bootstrap: bootstrap
        )
    }

    /// Build a real V1 sqlite manifest with one asset+resource+link. Returns bytes
    /// so the in-memory client can stage them at the V1 manifest path.
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
        // GRDB checkpoints WAL into the main db file on `.write` completion under DELETE
        // journal mode; reading the bytes now gives a clean reopen-able file.
        try dbQueue.close()
        return try Data(contentsOf: dbURL)
    }
}
