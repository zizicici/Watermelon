import XCTest
import GRDB
@testable import Watermelon

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

    // ClaudeReviewerC P17 R01: scanV1Months (and thus verifyFinalState) skips out-of-range two-digit month
    // dirs, so detectV1Manifests must skip them too or admission loops forever. Pin the migration side of
    // the shared 01-12 domain so a future change can't reintroduce the asymmetry from either end.
    func testScanV1Months_skipsOutOfRangeMonthDirectories() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 13)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 0)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 6)

        let service = makeService(client: client)
        let scanned = try await service.scanV1Months()
        XCTAssertEqual(scanned.map { "\($0.year)-\($0.month)" }, ["2024-6"],
                       "scanV1Months must skip out-of-range month dirs so detectV1Manifests/verifyFinalState agree on 01-12")
    }

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

    func testPublishVersionAndMarkProfileMigrated_writesVersionJSONAndFlipsCompleted() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        // Pre-write .watermelon directory (production builder creates it before phase1; tests stand in).
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        try await service.ensureVersionPublished(writerID: "w")
        try await service.markProfileMigrated(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-001")

        let exists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(exists, "ensureVersionPublished must write version.json")

        let state = try await identity.loadRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        XCTAssertEqual(state?.migrationCompleted, 1, "markProfileMigrated must flip the flag")
    }

    func testPhase1_v1ManifestRoundTripsThroughV2() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xAB); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
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
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-001"
        )
        XCTAssertEqual(processed, 1)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[LibraryMonthKey(year: 2025, month: 6)])
        let asset = try XCTUnwrap(monthState.assets[assetFPTyped], "V1 asset must be visible after V2 materialize")
        let resourcePath = "2025/06/\(logicalName)"
        XCTAssertEqual(monthState.resources[RemotePhysicalPathKey(resourcePath)]?.contentHash, contentHash)
        let arKey = AssetResourceKey(assetFingerprint: assetFPTyped, role: ResourceTypeCode.photo, slot: 0)
        XCTAssertEqual(monthState.assetResources[arKey]?.resourceHash, contentHash)
        // Migration writes V2 snapshot rows; stamp is required so future stale-add
        // replays against this baseline gate via opStampPrecedes.
        let stamp = try XCTUnwrap(asset.stamp, "migration-produced snapshot row must carry an OpStamp")
        XCTAssertEqual(stamp.writerID, writerID)
        XCTAssertGreaterThan(stamp.seq, 0)
        XCTAssertGreaterThan(stamp.clock, 0)

        // Resource row needs the same stamp so cross-writer path-level LWW (after
        // V1 migration) is also gated by opStampPrecedes. Without it, a peer's
        // stale uncovered add at the same physicalRemotePath would silently
        // overwrite the migration row on next materialize.
        let resourceStamp = try XCTUnwrap(monthState.resources[RemotePhysicalPathKey(resourcePath)]?.stamp,
                                          "migration-produced resource row must carry an OpStamp")
        XCTAssertEqual(resourceStamp, stamp,
                       "resource row stamp must match its producing asset's stamp (same writer/seq/clock)")
    }

    func testPhase1WritesMigrationMarkerNotVersionJSON() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xAB); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
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
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(
            profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-001"
        )

        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertFalse(versionExists,
                       "phase1 must NOT write version.json — a crashed migration would otherwise look like clean V2")
        let markerExists = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "w"))
        XCTAssertTrue(markerExists,
                      "phase1 must write writer-unique migration marker so inspect can route back to .v1")
    }

    // O2a: a cleanly imported month writes an `imported` journal record under
    // .watermelon/migrations/journal/ after commit/snapshot publish.
    func testRunPhase1_cleanImport_writesImportedJournalRecord() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_journal.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(profileID: profileID, repoID: repoID, writerID: "w", runID: "run-journal")
        XCTAssertEqual(processed, 1)

        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        XCTAssertEqual(summary.records.count, 1)
        let record = try XCTUnwrap(summary.records.first)
        XCTAssertEqual(record.outcome, .imported)
        XCTAssertEqual(record.year, 2025)
        XCTAssertEqual(record.month, 6)
        XCTAssertEqual(record.migratedAssetCount, 1)
        XCTAssertEqual(record.totalAssetCount, 1)
        XCTAssertEqual(record.writerID, "w")
    }

    // O2a: a month deferred for overlapping a non-clean V2 month writes a `quarantined` journal
    // record carrying the month, asset count, and a reason.
    func testRunPhase1_nonCleanV2Month_writesQuarantinedJournalRecord() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")

        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let snapshotWriterA = "22222222-2222-2222-2222-bbbbbbbbbbbb"
        let snapshotWriterB = "33333333-3333-3333-3333-cccccccccccc"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let month = LibraryMonthKey(year: 2025, month: 6)

        try await writeEmptyTrustedSnapshot(client: client, month: month, repoID: repoID, writer: snapshotWriterA, high: 2, lamport: 20, runID: "snap-a")
        try await writeEmptyTrustedSnapshot(client: client, month: month, repoID: repoID, writer: snapshotWriterB, high: 2, lamport: 25, runID: "snap-b")

        let assetFP = TestFixtures.fingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_nonclean.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", month.year, month.month)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-nonclean-journal")
        XCTAssertEqual(processed, 0)

        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        let quarantined = summary.records.filter { $0.outcome == .quarantined }
        XCTAssertEqual(quarantined.count, 1)
        let record = try XCTUnwrap(quarantined.first)
        XCTAssertEqual(record.year, 2025)
        XCTAssertEqual(record.month, 6)
        XCTAssertEqual(record.totalAssetCount, 1)
        XCTAssertEqual(record.reason?.contains("not clean"), true)
    }

    // O2a: a per-month failure caught after the month is known records a `failed` journal entry
    // (best-effort) without masking the original error, and must not mark the profile migrated.
    func testRunPhase1_injectedCommitFailure_writesFailedJournalRecord_withoutMaskingError() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xCE)
        let contentHash = TestFixtures.fingerprint(0xCF)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_fail.HEIC"
        )
        let month = LibraryMonthKey(year: 2025, month: 6)
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", month.year, month.month)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        // Non-retryable permission failure on the first commit write (seq=1) drives the per-month catch.
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectUploadError(.permission, for: commitPath)

        let service = makeService(client: client, profileID: profileID)
        do {
            _ = try await service.runPhase1(profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-fail")
            XCTFail("expected the injected commit failure to propagate")
        } catch let error {
            XCTAssertFalse(error is CancellationError, "a permission failure must not surface as cancellation")
            guard case CommitLogWriter.WriteError.ioFailure = error else {
                XCTFail("original commit failure must surface unmasked, got \(error)")
                return
            }
        }

        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        let failed = summary.records.filter { $0.outcome == .failed }
        XCTAssertEqual(failed.count, 1, "a per-month failure after the month is known must record a failed journal entry")
        XCTAssertEqual(failed.first?.year, 2025)
        XCTAssertEqual(failed.first?.month, 6)
        XCTAssertEqual(failed.first?.totalAssetCount, 1, "failed record carries the known asset count")

        let state = try await identity.loadRepoState(profileID: profileID, repoID: repoID)
        XCTAssertEqual(state?.migrationCompleted, 0, "a failed phase1 must not mark the profile migrated")
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
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-1")
        try await service.runPhase3(writerID: "w", runID: "run-1")

        let m1 = await client.hasFile(path1)
        let m2 = await client.hasFile(path2)
        XCTAssertFalse(m1)
        XCTAssertFalse(m2)
    }

    func testPhase3PreservesManifestsAddedAfterPhase1Scan() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x01), resourceHash: TestFixtures.fingerprint(0x11), logicalName: "IMG_1.HEIC")
        let scannedPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2024, 3)
        await client.injectFile(path: scannedPath, data: bytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runPhase1(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-1")

        let lateBytes = try Self.buildV1ManifestSqlite(assetFingerprint: TestFixtures.fingerprint(0x02), resourceHash: TestFixtures.fingerprint(0x22), logicalName: "IMG_2.HEIC")
        let postScanPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 7)
        await client.injectFile(path: postScanPath, data: lateBytes)

        try await service.runPhase3(writerID: "w", runID: "run-1")

        let scannedRemoved = await client.hasFile(scannedPath)
        let lateSurvived = await client.hasFile(postScanPath)
        XCTAssertFalse(scannedRemoved)
        XCTAssertTrue(lateSurvived)
    }

    func testPhase3IsIdempotent_secondInvocation_noop() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 3)

        let service = makeService(client: client)
        try await service.runPhase3(writerID: "test-writer", runID: "run-1")
        try await service.runPhase3(writerID: "test-writer", runID: "run-1")
    }

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


    func testVerifyFinalState_succeedsAfterCleanRun() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        // No V1 manifests, no marker for "w" — verify's two assertions both hold trivially.
        let service = makeService(client: client)
        try await service.verifyFinalState(cleanedWriterID: "w")
    }

    func testVerifyFinalState_failsWhenV1ResidueRemains() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2024, month: 5)

        let service = makeService(client: client)
        do {
            try await service.verifyFinalState(cleanedWriterID: "w")
            XCTFail("verify must reject when V1 manifest still visible")
        } catch V1MigrationService.MigrationError.verifyFailed {
            // expected
        }
    }

    func testVerifyFinalState_ignoresPeerMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await injectMigrationMarker(client: client, writerID: "peer-writer", phase: 1)

        let service = makeService(client: client)
        try await service.verifyFinalState(cleanedWriterID: "w")
    }


    func testRun_v1Inspection_executesFullPathAndFlipsMigrationCompleted() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xA1); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
        let contentHash = TestFixtures.fingerprint(0xB1)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_run1.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        let outcome = try await service.runFullMigration(
            profileID: profileID,
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: "w",
            runID: "run-1"
        )

        XCTAssertEqual(outcome.migratedMonthCount, 1)

        let state = try await identity.loadRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        XCTAssertEqual(state?.migrationCompleted, 1, "full migration path must mark profile migrated")

        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(versionExists)
        let markerExists = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "w"))
        XCTAssertFalse(markerExists, "phase3 cleanup must remove our own marker")
        let residueScan = try await service.scanV1Months()
        XCTAssertTrue(residueScan.isEmpty, "phase1 must quarantine the V1 manifest")
    }

    func testRun_v2WithPendingMigrationCleanup_skipsPhase1AndPreservesMigrationCompletedFlag() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        // Set the v2 stage: version.json + a peer's stale phase1 marker.
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "peer")
        try await injectMigrationMarker(client: client, writerID: "peer", phase: 1)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        try await service.runCleanupOnly(
            ownerWriterID: "peer",
            writerID: "w",
            runID: "cleanup-run-1"
        )

        // Peer marker must be gone after phase3(owner).
        let peerMarker = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "peer"))
        XCTAssertFalse(peerMarker)

        // Critical asymmetry preservation: cleanup writer's profile keeps migrationCompleted=0.
        // (XCTAssertEqual on optional vs Int catches state==nil as well — `!= 1` would let nil pass.)
        let state = try await identity.loadRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        XCTAssertEqual(state?.migrationCompleted, 0,
                       "cleanup-only path must leave migrationCompleted at 0 (pre-refactor behavior)")
    }

    // Bug-X P07 R02 CodexChecker F1: the same partial-marker visibility lag R01 closed for the
    // same-run sweep remains in the cross-run cleanup-only resume, which builds a fresh service with
    // an empty in-memory marker set and calls runPhase3 with no preserve set. runCleanupOnly must
    // give a lagging partial marker the read-after-write window to surface before the sweep can
    // delete its residue — this drives the deadline plumbing end-to-end through runCleanupOnly.
    func testRunCleanupOnly_partialMarkerLagsIntoSweep_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        try await inner.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath, writerID: "peer")
        try await injectMigrationMarker(client: inner, writerID: "peer", phase: 1)

        // A prior interrupted migration already quarantined this month's residue and wrote its
        // partial marker; the marker's listing/metadata visibility now lags the resumed cleanup.
        let year = 2025
        let month = 6
        let monthRel = String(format: "%04d/%02d", year, month)
        let residuePath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("legacy-residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = MarkerVisibilityLagClient(inner: inner, hiddenMarkerPath: markerPath, revealMetadataAfterProbes: 1)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = V1MigrationService(
            client: client,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            bootstrap: RepoBootstrap(client: client, basePath: basePath)
        )
        try await service.runCleanupOnly(ownerWriterID: "peer", writerID: "w", runID: "cleanup-lag")

        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "cross-run cleanup must preserve residue when the partial marker only lags read-after-write visibility"
        )
        let peerMarkerGone = await inner.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "peer")) == false
        XCTAssertTrue(peerMarkerGone, "phase3 must still complete owner marker cleanup after preserving the residue")
    }

    // R06 ClaudeReviewerA: an interrupt in the journal→quarantine window leaves a month journaled
    // `.imported` (commit+snapshot durable) but with its original-named V1 manifest still present.
    // Inspection journal-suppresses it (hasUnresolvedV1Manifests) and routes to cleanup-only, so
    // cleanup-only's verifyFinalState must suppress the same month instead of throwing verifyFailed
    // on a fully-migrated repo.
    func testRunCleanupOnly_journalResolvedOriginalManifest_completesWithoutVerifyFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "peer")
        try await injectMigrationMarker(client: client, writerID: "peer", phase: 1)

        // The original V1 manifest never got quarantined before the interrupt …
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // … but its `.imported` journal record was already written, so the month is durably migrated.
        try await MigrationJournalStore(client: client, basePath: basePath).record(
            MigrationJournalRecord(
                repoID: repoID, writerID: "peer", runID: "interrupted-run",
                year: 2025, month: 6, outcome: .imported, createdAtMs: 0,
                migratedAssetCount: 1, totalAssetCount: 1, skippedAssetCount: 0, reason: nil
            )
        )

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        // Before the fix this threw MigrationError.verifyFailed for the journal-resolved manifest.
        try await service.runCleanupOnly(ownerWriterID: "peer", writerID: "w", runID: "cleanup-journal")

        let peerMarkerGone = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "peer")) == false
        XCTAssertTrue(peerMarkerGone, "phase3 must still complete owner marker cleanup")
        // Direction (a): the inert journal-resolved manifest stays; it is permanently journal-suppressed.
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertTrue(manifestStillPresent, "cleanup-only suppresses the resolved manifest rather than failing; it stays inert")
    }

    // Negative control: an original V1 manifest with NO safe journal record must still fail the
    // cleanup-only post-condition, so the suppression is journal-gated rather than a blanket removal
    // of the guard.
    func testRunCleanupOnly_unjournaledOriginalManifest_stillThrowsVerifyFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "peer")
        try await injectMigrationMarker(client: client, writerID: "peer", phase: 1)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        do {
            try await service.runCleanupOnly(ownerWriterID: "peer", writerID: "w", runID: "cleanup-unjournaled")
            XCTFail("cleanup-only must still reject a genuinely-unresolved V1 manifest")
        } catch V1MigrationService.MigrationError.verifyFailed {
            // expected
        }
    }

    func testDeleteIfPresent_emptyManifestPath_swallowsPeerRaceNotFound() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Empty V1 manifest — schema only, no rows — drives runPhase1 into the
        // "no assets/resources/links → deleteIfPresent" branch.
        let manifestBytes = try Self.buildEmptyV1ManifestSqlite()
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        // Simulate a peer cleanup completing between metadataIfPresent and delete:
        // metadata succeeds, delete throws .notFound. Must NOT propagate.
        await client.injectDeleteError(.notFound, for: manifestPath)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        // Must not throw — peer-race on delete is benign.
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-1"
        )
        XCTAssertEqual(processed, 0, "empty manifest contributes zero migrated months")
        // Peer-race fidelity: the fake mirrors a real backend where the path is
        // gone by the time .notFound is observed. If the manifest were still
        // present, the next detectV1Manifests pass would re-find it and loop.
        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertFalse(manifestStillPresent, "peer-race .notFound must leave the V1 manifest absent")
    }

    func testDeleteIfPresent_emptyManifestPath_propagatesNonNotFoundDeleteError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let manifestBytes = try Self.buildEmptyV1ManifestSqlite()
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        await client.injectDeleteError(.permission, for: manifestPath)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        do {
            _ = try await service.runPhase1(
                profileID: profileID, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w", runID: "run-1"
            )
            XCTFail("expected permission error to propagate; not-found swallow must not catch other shapes")
        } catch {
            // Permission is translated to NSCocoaErrorDomain / NSFileReadNoPermissionError
            // wrapped in RemoteStorageClientError.underlying. Pin that shape so a future
            // regression that broadens the swallow to all errors (or narrows it
            // differently) surfaces here instead of passing on any random thrown error.
            XCTAssertFalse(isStorageNotFoundError(error),
                           "permission error must not classify as not-found (would be swallowed in prod)")
            guard case RemoteStorageClientError.underlying(let underlying) = error else {
                XCTFail("expected RemoteStorageClientError.underlying, got \(error)")
                return
            }
            let nsError = underlying as NSError
            XCTAssertEqual(nsError.domain, NSCocoaErrorDomain)
            XCTAssertEqual(nsError.code, NSFileReadNoPermissionError)
        }
        // Non-mutating injection contract: a propagated error leaves the file in place.
        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertTrue(manifestStillPresent, ".permission injection must not mutate fake storage")
    }

    // O2a R02: an empty valid V1 manifest (no assets/resources/links) is deleted, but a durable
    // `quarantined` journal record must be written first so a processed month is never left without
    // a journal entry.
    func testRunPhase1_emptyManifest_writesQuarantinedJournalRecordThenDeletes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let manifestBytes = try Self.buildEmptyV1ManifestSqlite()
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(profileID: profileID, repoID: repoID, writerID: "w", runID: "run-empty")
        XCTAssertEqual(processed, 0, "empty manifest contributes zero migrated months")

        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertFalse(manifestStillPresent, "empty manifest must be deleted so the V1 scan stops finding it")

        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        XCTAssertEqual(summary.records.count, 1, "the empty-manifest delete must leave exactly one journal record")
        let record = try XCTUnwrap(summary.records.first)
        XCTAssertEqual(record.outcome, .quarantined)
        XCTAssertEqual(record.year, 2025)
        XCTAssertEqual(record.month, 6)
        XCTAssertEqual(record.migratedAssetCount, 0)
        XCTAssertEqual(record.totalAssetCount, 0)
        XCTAssertEqual(record.skippedAssetCount, 0)
        XCTAssertEqual(record.reason, "empty V1 manifest deleted")
    }

    // O2a R02: the journal is written before the destructive delete, and `monthJournaled` stops a
    // later delete failure from emitting a second `failed` record for the same month.
    func testRunPhase1_emptyManifest_deleteFailureAfterJournal_doesNotDoubleRecord() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let manifestBytes = try Self.buildEmptyV1ManifestSqlite()
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        // Delete fails after the journal write lands; the original error must still propagate.
        await client.injectDeleteError(.permission, for: manifestPath)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        do {
            _ = try await service.runPhase1(profileID: profileID, repoID: repoID, writerID: "w", runID: "run-empty-fail")
            XCTFail("expected the delete permission error to propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error), "permission error must not classify as not-found")
        }

        // The journal write happened before the failing delete, so the manifest is still present and the
        // record is the `quarantined` decision — not a duplicate `failed` entry.
        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertTrue(manifestStillPresent, "delete failed, so the manifest must remain")
        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        XCTAssertEqual(summary.records.count, 1, "the guard must prevent a second (failed) record for the same month")
        XCTAssertEqual(summary.records.first?.outcome, .quarantined)
        XCTAssertEqual(summary.records.first?.reason, "empty V1 manifest deleted")
    }

    func testRunFullMigration_atomicCreateURLErrorCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // `.exclusive` so the writer's direct atomicCreate boundary fires (not the
        // gate's staging path). InMemoryRemoteStorageClient's atomicCreate honors
        // `injectAtomicCreateURLErrorCancelled` regardless of the configured guarantee.
        client.setAtomicCreateGuarantee(.exclusive)

        let assetFP = TestFixtures.fingerprint(0xCE); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
        let contentHash = TestFixtures.fingerprint(0xCF)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP,
            resourceHash: contentHash,
            logicalName: "IMG_cancel.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        // SeqAllocator on a fresh repo allocates seq=1 — pin URL-cancel on that
        // exact path so the cancel fires at the commit-write boundary inside
        // V1MigrationService.runPhase1's commit loop.
        let commitPath = RepoLayout.commitFilePath(
            base: basePath,
            month: LibraryMonthKey(year: 2025, month: 6),
            writerID: writerID,
            seq: 1
        )
        await client.injectAtomicCreateURLErrorCancelled(for: commitPath)

        let service = makeService(client: client, profileID: profileID)
        do {
            _ = try await service.runFullMigration(
                profileID: profileID,
                repoID: repoID,
                writerID: writerID,
                runID: "run-cancel"
            )
            XCTFail("expected CancellationError to propagate end-to-end through runFullMigration")
        } catch is CancellationError {
            // expected — gate normalizes URL-cancel → writer surfaces CancellationError →
            // shouldRetryMigrationCommitWrite returns false for cancellation → propagates
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("URL-cancel must NOT exhaust the migrationMaxRetries loop and surface as .alreadyExists")
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URL-cancel must NOT wrap as .ioFailure (got: \(underlying))")
        } catch SnapshotWriter.WriteError.finalizationFailed(let underlying) {
            XCTFail("URL-cancel must NOT wrap as SnapshotWriter.finalizationFailed (got: \(underlying))")
        } catch SnapshotWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URL-cancel must NOT wrap as SnapshotWriter.ioFailure (got: \(underlying))")
        } catch V1MigrationService.MigrationError.ioFailure(let underlying) {
            XCTFail("URL-cancel must NOT wrap as MigrationError.ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }

        // Profile must NOT be marked migrated when cancellation interrupts phase1 —
        // a future run needs to re-enter migration, not skip to verifyFinalState.
        let state = try await identity.loadRepoState(profileID: profileID, repoID: repoID)
        XCTAssertEqual(state?.migrationCompleted, 0,
                       "cancellation mid-phase1 must leave migrationCompleted=0 so the next run resumes")
        // V1 manifest must survive cancellation; quarantine only fires after a
        // successful commit+snapshot publish.
        let v1Survived = await client.hasFile(manifestPath)
        XCTAssertTrue(v1Survived,
                      "phase1 cancellation must NOT quarantine the V1 manifest — next run must still see it")
    }

    func testPhase1_observesRemoteSeqAndClockBeforeAllocating_avoidsCollision() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let preExistingMonth = LibraryMonthKey(year: 2024, month: 12)

        // Plant pre-existing V2 commits at seq=1..4 with clock high-water=200.
        // Without observe-before-allocate, V1 migration burns its 4-attempt
        // retry budget on collisions at seq=1..4.
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        for seq in UInt64(1)...UInt64(4) {
            let assetFP = TestFixtures.fingerprint(UInt8(0x10 + seq)); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
            let contentHash = TestFixtures.fingerprint(UInt8(0x20 + seq))
            let monthRel = String(format: "%04d/%02d", preExistingMonth.year, preExistingMonth.month)
            let leaf = String(format: "pre-%llu.jpg", seq)
            let path = "\(monthRel)/\(leaf)"
            let body = CommitAddAssetBody(
                assetFingerprint: assetFPTyped,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resources: [
                    CommitResourceEntry(
                        physicalRemotePath: path,
                        logicalName: leaf,
                        contentHash: contentHash,
                        fileSize: 100,
                        resourceType: ResourceTypeCode.photo,
                        role: ResourceTypeCode.photo,
                        slot: 0,
                        crypto: nil
                    )
                ]
            )
            let clock = 50 + seq * 10
            let header = TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: seq, runID: "preexist",
                month: preExistingMonth,
                clockMin: clock, clockMax: clock
            )
            _ = try await commitWriter.write(
                header: header,
                ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
                month: preExistingMonth, respectTaskCancellation: false
            )
            await client.injectFile(path: "\(basePath)/\(path)", data: Data(repeating: 0, count: 100))
        }
        let highestPreExistingClock: UInt64 = 50 + 4 * 10

        // V1 manifest in a different month with a different asset FP, so the
        // existing-V2 fingerprint filter doesn't drop it.
        let assetFP = TestFixtures.fingerprint(0xAB); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP,
            resourceHash: contentHash,
            logicalName: "IMG_observe.HEIC"
        )
        let migrationMonth = LibraryMonthKey(year: 2025, month: 6)
        let manifestPath = String(
            format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)",
            migrationMonth.year, migrationMonth.month
        )
        await client.injectFile(path: manifestPath, data: manifestBytes)

        // Local DB starts at seq=0/clock=0 (fresh install state). The migration
        // must observe remote seq=4/clock=highestPreExistingClock first.
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-observe"
        )
        XCTAssertEqual(processed, 1)

        // Materialize and find the migration commit (it's the only one for migrationMonth).
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let migMonthState = try XCTUnwrap(output.state.months[migrationMonth])
        let migAsset = try XCTUnwrap(migMonthState.assets[assetFPTyped], "migration must publish the V1 asset into V2")
        let stamp = try XCTUnwrap(migAsset.stamp)
        XCTAssertGreaterThan(stamp.seq, 4,
                             "allocator must observe remote high-water seq=4 before allocating; got \(stamp.seq)")
        XCTAssertGreaterThan(stamp.clock, highestPreExistingClock,
                             "lamport must observe remote clock=\(highestPreExistingClock) before ticking; got \(stamp.clock)")
    }

    func testPhase1_retryAfterTransientCommitWriteUsesFreshSeqAndClock() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()

        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let migrationMonth = LibraryMonthKey(year: 2025, month: 6)
        let assetFP = TestFixtures.fingerprint(0xAB); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP,
            resourceHash: contentHash,
            logicalName: "IMG_retry.HEIC"
        )
        let manifestPath = String(
            format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)",
            migrationMonth.year,
            migrationMonth.month
        )
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let firstAttemptPath = RepoLayout.commitFilePath(
            base: basePath,
            month: migrationMonth,
            writerID: writerID,
            seq: 1
        )
        await client.injectUploadError(.transport, for: firstAttemptPath)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-retry"
        )
        XCTAssertEqual(processed, 1)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let state = try XCTUnwrap(output.state.months[migrationMonth])
        let stamp = try XCTUnwrap(state.assets[assetFPTyped]?.stamp)
        XCTAssertEqual(stamp.seq, 2)
        XCTAssertEqual(stamp.clock, 2)
    }


    func testPhase1_skipsTombstonedFingerprints_fromExistingV2State() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let month = LibraryMonthKey(year: 2025, month: 6)

        // V2 repo state: add an asset, then tombstone it.
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)

        let tombstoneFP = TestFixtures.fingerprint(0xAB)
        let tombstoneFPTyped = AssetFingerprint(decoding: tombstoneFP)!
        let contentHash = TestFixtures.fingerprint(0xCD)
        let monthRel = String(format: "%04d/%02d", month.year, month.month)
        let leaf = "IMG_tomb.HEIC"
        let path = "\(monthRel)/\(leaf)"
        let addBody = CommitAddAssetBody(
            assetFingerprint: tombstoneFPTyped,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: path,
                    logicalName: leaf,
                    contentHash: contentHash,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        let addHeader = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: 1, runID: "preexist",
            month: month, clockMin: 10, clockMax: 10
        )
        _ = try await commitWriter.write(
            header: addHeader,
            ops: [CommitOp(opSeq: 0, clock: 10, body: .addAsset(addBody))],
            month: month, respectTaskCancellation: false
        )
        await client.injectFile(path: "\(basePath)/\(path)", data: Data(repeating: 0, count: 100))

        let tombHeader = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: 2, runID: "preexist",
            month: month, clockMin: 20, clockMax: 20
        )
        _ = try await commitWriter.write(
            header: tombHeader,
            ops: [CommitOp(opSeq: 0, clock: 20, body: .tombstoneAsset(
                CommitTombstoneBody(
                    assetFingerprint: tombstoneFPTyped,
                    reason: .userDeleted,
                    observedBasis: TombstoneObservationBasis(
                        perWriterMaxSeq: [writerID: 1],
                        lamportWatermark: 10
                    )
                )
            ))],
            month: month, respectTaskCancellation: false
        )

        // V1 manifest in the same month with the tombstoned fingerprint.
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: tombstoneFP,
            resourceHash: contentHash,
            logicalName: leaf
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", month.year, month.month)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-tomb"
        )
        // The tombstoned fingerprint should be excluded from migration;
        // with no remaining migrable assets the month is skipped.
        XCTAssertEqual(processed, 0, "tombstoned V2 fingerprint must not be re-migrated from V1")

        // V2 state must still have the tombstone, not a resurrected asset.
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertNil(monthState.assets[tombstoneFPTyped],
                     "tombstoned fingerprint must not be resurrected by migration")
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(tombstoneFPTyped),
                      "tombstone must survive migration")
    }

    // Bug-X P10 R10 CodexReviewerB F1: runPhase1 read its dedup/publish baseline from
    // existingV2Output.state.months[month] without consulting outcomeByMonth[month]. A non-clean
    // (ambiguous/corrupt) V2 month folds to a best-effort/partial baseline, so migration could re-add
    // stale V1 rows over trusted V2 resource shape and resurrect fingerprints present only in the
    // non-selected/rejected fold. Migration is a V2 write consumer and must fail closed and defer the
    // month — like checkpoint/commit-GC/snapshot-GC/verify — quarantining residue under a partial marker.
    func testPhase1_defersMigrationForNonCleanV2Month_preservesResidueAndTrustedState() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")

        let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
        let snapshotWriterA = "22222222-2222-2222-2222-bbbbbbbbbbbb"
        let snapshotWriterB = "33333333-3333-3333-3333-cccccccccccc"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let month = LibraryMonthKey(year: 2025, month: 6)

        // Two trusted snapshots with incomparable coverage → the month materializes as .ambiguous
        // (best-effort read baseline, no covered-max winner).
        try await writeEmptyTrustedSnapshot(client: client, month: month, repoID: repoID, writer: snapshotWriterA, high: 2, lamport: 20, runID: "snap-a")
        try await writeEmptyTrustedSnapshot(client: client, month: month, repoID: repoID, writer: snapshotWriterB, high: 2, lamport: 25, runID: "snap-b")

        let preOutput = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(preOutput.outcomeByMonth[month], .ambiguous, "precondition: V2 month must be ambiguous")

        // A V1 manifest with a migrable asset in the same month.
        let assetFP = TestFixtures.fingerprint(0xAB); let assetFPTyped = AssetFingerprint(decoding: assetFP)!
        let contentHash = TestFixtures.fingerprint(0xCD)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_nonclean.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", month.year, month.month)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)

        let service = makeService(client: client, profileID: profileID)
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: repoID, writerID: writerID, runID: "run-nonclean"
        )

        // Migration must defer the non-clean month instead of publishing from a best-effort baseline.
        XCTAssertEqual(processed, 0, "migration must not migrate a V1 month overlapping a non-clean V2 month")

        let monthRel = String(format: "%04d/%02d", month.year, month.month)
        let residuePath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        let residueSurvived = await client.hasFile(residuePath)
        XCTAssertTrue(residueSurvived, "deferred month's V1 manifest must be quarantined as residue, not migrated")
        let markerSurvived = await client.hasFile(markerPath)
        XCTAssertTrue(markerSurvived, "deferred month must carry a partial-migration marker so phase3 preserves the residue")
        let liveManifestGone = await client.hasFile(manifestPath) == false
        XCTAssertTrue(liveManifestGone, "live V1 manifest must be moved aside so verifyFinalState's lingering check passes")

        // Trusted V2 state must be untouched: no migration commit/snapshot re-added the V1 fingerprint.
        let postOutput = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(postOutput.outcomeByMonth[month], .ambiguous, "deferred month's V2 outcome must be unchanged")
        XCTAssertNil(postOutput.state.months[month]?.assets[assetFPTyped],
                     "migration must not resurrect the V1 fingerprint into the non-clean V2 month")
    }

    private func writeEmptyTrustedSnapshot(
        client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey,
        repoID: String,
        writer: String,
        high: UInt64,
        lamport: UInt64,
        runID: String
    ) async throws {
        let covered = CoveredRanges(rangesByWriter: [writer: [ClosedSeqRange(low: 1, high: high)]])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writer,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: .empty)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private func injectMigrationMarker(client: InMemoryRemoteStorageClient, writerID: String, phase: Int) async throws {
        let dict: [String: Any] = [
            "v": 2,
            "writer_id": writerID,
            "run_id": "test-run",
            "phase": phase,
            "started_at_ms": Int64(0),
            "last_step_at_ms": Int64(0)
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID), data: data)
    }

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

    private static func buildEmptyV1ManifestSqlite() throws -> Data {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }
        let dbURL = tempDir.appendingPathComponent("v1-empty.sqlite")
        let dbQueue = try DatabaseQueue(path: dbURL.path)
        try MonthManifestStore.migrate(dbQueue)
        try dbQueue.close()
        return try Data(contentsOf: dbURL)
    }

    /// Resources but no assets — V1 manifest is structurally inconsistent. Drives runPhase1
    /// into the "snapshot.assets.isEmpty && !snapshot.resources.isEmpty" quarantine branch.
    private static func buildInconsistentV1ManifestSqlite(
        resourceHash: Data,
        logicalName: String
    ) throws -> Data {
        let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }
        let dbURL = tempDir.appendingPathComponent("v1-inconsistent.sqlite")
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
        }
        try dbQueue.close()
        return try Data(contentsOf: dbURL)
    }

    // Bug-IX P01 R07 Codex A Finding 1: a V1 manifest with resources/links but no assets is
    // structurally inconsistent and was being quarantined without a partial-migration marker.
    // Phase 3's sweep then deleted the only legacy metadata evidence for that month. The
    // marker-before-quarantine pairing matches the assets-but-no-migrable path and protects
    // the residue across the same foreground migration that identified it.
    func testRunFullMigration_inconsistentV1Manifest_preservesResidueUnderPartialMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let resourceHash = TestFixtures.fingerprint(0xEF)
        let manifestBytes = try Self.buildInconsistentV1ManifestSqlite(
            resourceHash: resourceHash,
            logicalName: "IMG_inconsistent.HEIC"
        )
        let year = 2025
        let month = 6
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", year, month)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        _ = try await service.runFullMigration(
            profileID: profileID,
            repoID: repoID,
            writerID: "w",
            runID: "run-inconsistent"
        )

        let monthRel = String(format: "%04d/%02d", year, month)
        let residuePath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"

        let residueSurvived = await client.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "residue must survive phase-3 sweep so future repair tooling retains the original V1 metadata"
        )
        let markerSurvived = await client.hasFile(markerPath)
        XCTAssertTrue(
            markerSurvived,
            "partial-migration marker must be written before quarantine so the sweep preservation gate fires"
        )
    }

    // Bug-X P07 R01 CodexChecker F1: the partial-migration marker is written and verified in
    // phase1, but on a grace backend its listing/metadata visibility can lag into the phase3
    // sweep that runs in the same run. A single non-grace marker probe then reads a stale
    // not-found and deletes the residue. runPhase1 records the marker's month so runPhase3
    // preserves it despite the lag — this drives the real `%04d/%02d` month plumbing end-to-end.
    func testRunFullMigration_partialMarkerListingLagsIntoSweep_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        try await inner.connect()

        let resourceHash = TestFixtures.fingerprint(0xEF)
        let manifestBytes = try Self.buildInconsistentV1ManifestSqlite(
            resourceHash: resourceHash,
            logicalName: "IMG_inconsistent.HEIC"
        )
        let year = 2025
        let month = 6
        let monthRel = String(format: "%04d/%02d", year, month)
        let manifestPath = "\(basePath)/\(monthRel)/\(MonthManifestStore.manifestFileName)"
        await inner.injectFile(path: manifestPath, data: manifestBytes)

        // Marker is created/verified normally (move + download), but stays invisible to the
        // phase3 sweep's LIST and metadata probe — read-after-write visibility lag.
        let markerPath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        let client = MarkerVisibilityLagClient(inner: inner, hiddenMarkerPath: markerPath)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = V1MigrationService(
            client: client,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            bootstrap: RepoBootstrap(client: client, basePath: basePath)
        )
        _ = try await service.runFullMigration(
            profileID: profileID,
            repoID: repoID,
            writerID: "w",
            runID: "run-marker-lag"
        )

        let residuePath = "\(basePath)/\(monthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "phase3 must preserve residue for a month it migrated this run even when the partial marker lags listing/metadata"
        )
        let markerSurvived = await inner.hasFile(markerPath)
        XCTAssertTrue(markerSurvived, "marker bytes were written; only their listing/metadata visibility lagged")
    }

    // Bug-X P07 R03 ClaudeReviewerC F1: an interrupted multi-month migration leaves a prior-run
    // partial-marker month as residue (no live manifest) while a later month still has a live V1
    // manifest. The next foreground open routes to .migrateFromV1 → runFullMigration (NOT
    // cleanup-only, because hasUnresolvedV1Manifests() short-circuits inspection). The prior-run partial
    // month is residue now, so it is never re-scanned into the in-memory set; runFullMigration must
    // give it the same cross-run marker-visibility window cleanup-only got in R02, or its forensic
    // residue is deleted on a single stale not-found within read-after-write grace. The month this
    // run migrated cleanly still sweeps immediately (no per-migration latency regression).
    func testRunFullMigration_resumeWithPriorRunPartialMarkerLag_preservesPriorResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        // Prior interrupted run already published version.json and quarantined month B (2024/03) with
        // a partial marker, then died before processing month C. B has NO live manifest now.
        try await inner.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath, writerID: "w")
        let priorMonthRel = "2024/03"
        let priorResidue = "\(basePath)/\(priorMonthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let priorMarker = "\(basePath)/\(priorMonthRel)/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: priorResidue, data: Data("prior-legacy-residue".utf8))
        await inner.injectFile(path: priorMarker, data: Data("{}".utf8))

        // Month C still has a live V1 manifest → resume routes through runFullMigration.
        let assetFP = TestFixtures.fingerprint(0xC1)
        let contentHash = TestFixtures.fingerprint(0xD1)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_resume.HEIC"
        )
        let liveMonthRel = "2025/06"
        let manifestPath = "\(basePath)/\(liveMonthRel)/\(MonthManifestStore.manifestFileName)"
        await inner.injectFile(path: manifestPath, data: manifestBytes)

        // The prior-run partial marker's read-after-write visibility lags into this resume sweep.
        let client = MarkerVisibilityLagClient(inner: inner, hiddenMarkerPath: priorMarker, revealMetadataAfterProbes: 1)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: "w")

        let service = V1MigrationService(
            client: client,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            bootstrap: RepoBootstrap(client: client, basePath: basePath)
        )
        _ = try await service.runFullMigration(profileID: profileID, repoID: repoID, writerID: "w", runID: "resume-run")

        let priorSurvived = await inner.hasFile(priorResidue)
        XCTAssertTrue(
            priorSurvived,
            "runFullMigration resume must preserve a prior-run partial-marker month's residue when the marker only lags read-after-write visibility (pre-fix: deleted on the single non-grace probe)"
        )
        // The month migrated this run leaves clean residue (no marker, in the scanned set) that still sweeps.
        let liveResidue = "\(basePath)/\(liveMonthRel)/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let liveResidueGone = await inner.hasFile(liveResidue) == false
        XCTAssertTrue(liveResidueGone, "this run's cleanly-migrated month residue must still be swept")
    }
}

/// Wraps an InMemory client so a single marker path is invisible to `list` and `metadata`
/// (read-after-write visibility lag) while remaining fully readable via `download`/`move`.
private final class MarkerVisibilityLagClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let hiddenMarkerPath: String
    private let hiddenMarkerName: String
    /// nil → marker stays invisible to `metadata` forever (same-run lag). Non-nil → `metadata`
    /// 404s for the first N probes, then reveals — models the cross-run cleanup resume window.
    private let revealMetadataAfterProbes: Int?
    private let lock = NSLock()
    private var metadataProbes = 0

    init(inner: InMemoryRemoteStorageClient, hiddenMarkerPath: String, revealMetadataAfterProbes: Int? = nil) {
        self.inner = inner
        self.hiddenMarkerPath = hiddenMarkerPath
        self.hiddenMarkerName = (hiddenMarkerPath as NSString).lastPathComponent
        self.revealMetadataAfterProbes = revealMetadataAfterProbes
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { inner.concurrencyMode }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { inner.moveIfAbsentGuarantee }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { inner.readAfterWriteGraceSeconds }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { $0.name != hiddenMarkerName }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if path == hiddenMarkerPath {
            guard let threshold = revealMetadataAfterProbes else { return nil }
            let reveal = lock.withLock { () -> Bool in
                metadataProbes += 1
                return metadataProbes > threshold
            }
            if !reveal { return nil }
        }
        return try await inner.metadata(path: path)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}
