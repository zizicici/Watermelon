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

        // Pre-write repo.json (production builder writes it before phase1; tests stand in).
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "w")

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
        XCTAssertEqual(monthState.resources[resourcePath]?.contentHash, contentHash)
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
        let resourceStamp = try XCTUnwrap(monthState.resources[resourcePath]?.stamp,
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

        // Set the v2 stage: repo.json + version.json + a peer's stale phase1 marker.
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", writerID: "peer")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
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
}
