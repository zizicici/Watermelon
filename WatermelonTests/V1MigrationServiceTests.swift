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

    func testPublishVersionAndMarkProfileMigrated_writesVersionJSONAndFlipsCompleted() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        // Pre-write repo.json (production builder writes it before phase1; tests stand in).
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "r", writerID: "w")

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        try await service.ensureVersionPublished(writerID: "w")
        try await service.markProfileMigrated(profileID: profileID, repoID: "r", writerID: "w", runID: "run-001")

        let exists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(exists, "ensureVersionPublished must write version.json")

        let state = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(state?.migrationCompleted, 1, "markProfileMigrated must flip the flag")
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

    // MARK: - verifyFinalState

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

    /// Peer markers from prior aborted runs are the next inspection's job, not verify's.
    /// Treating any-marker-exists as failure would loop full migrations on partial state.
    func testVerifyFinalState_ignoresPeerMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await injectMigrationMarker(client: client, writerID: "peer-writer", phase: 1)

        let service = makeService(client: client)
        try await service.verifyFinalState(cleanedWriterID: "w")
    }

    // MARK: - Migration entrypoint integration

    func testRun_v1Inspection_executesFullPathAndFlipsMigrationCompleted() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let assetFP = TestFixtures.fingerprint(0xA1)
        let contentHash = TestFixtures.fingerprint(0xB1)
        let manifestBytes = try Self.buildV1ManifestSqlite(
            assetFingerprint: assetFP, resourceHash: contentHash, logicalName: "IMG_run1.HEIC"
        )
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        let outcome = try await service.runFullMigration(
            profileID: profileID,
            repoID: "r",
            writerID: "w",
            runID: "run-1"
        )

        XCTAssertEqual(outcome.migratedMonthCount, 1)

        let state = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(state?.migrationCompleted, 1, "full migration path must mark profile migrated")

        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        XCTAssertTrue(versionExists)
        let markerExists = await client.hasFile(RepoLayout.migrationMarkerPath(base: basePath, writerID: "w"))
        XCTAssertFalse(markerExists, "phase3 cleanup must remove our own marker")
        let residueScan = try await service.scanV1Months()
        XCTAssertTrue(residueScan.isEmpty, "phase1 must quarantine the V1 manifest")
    }

    /// Cleanup-only path takes over a peer's incomplete migration. Critical asymmetry:
    /// `markProfileMigrated` is NOT called (preserves pre-refactor behavior — the cleanup writer
    /// is not the migrating writer, so its profile's `migrationCompleted` stays 0).
    func testRun_v2WithPendingMigrationCleanup_skipsPhase1AndPreservesMigrationCompletedFlag() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        // Set the v2 stage: repo.json + version.json + a peer's stale phase1 marker.
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "r", writerID: "peer")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "peer")
        try await injectMigrationMarker(client: client, writerID: "peer", phase: 1)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

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
        let state = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(state?.migrationCompleted, 0,
                       "cleanup-only path must leave migrationCompleted at 0 (pre-refactor behavior)")
    }

    /// Peer-race tolerance: phase1's empty-manifest branch deletes the source V1
    /// manifest, but a sibling cleanup may have removed it between our
    /// `metadataIfPresent` check and our `delete` call. `deleteIfPresent` must
    /// swallow not-found from non-idempotent backends so phase1 doesn't abort
    /// the whole migration over a benign race. Non-not-found errors must still
    /// propagate (transport flap is not the same as a peer who already cleaned up).
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
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        // Must not throw — peer-race on delete is benign.
        let processed = try await service.runPhase1(
            profileID: profileID, repoID: "r", writerID: "w", runID: "run-1"
        )
        XCTAssertEqual(processed, 0, "empty manifest contributes zero migrated months")
        // Peer-race fidelity: the fake mirrors a real backend where the path is
        // gone by the time .notFound is observed. If the manifest were still
        // present, the next detectV1Manifests pass would re-find it and loop.
        let manifestStillPresent = await client.hasFile(manifestPath)
        XCTAssertFalse(manifestStillPresent, "peer-race .notFound must leave the V1 manifest absent")
    }

    /// Parity: deleteIfPresent must still propagate non-not-found delete errors.
    /// A transport/permission failure is not "peer cleaned it up first" — surfacing
    /// it keeps real backend faults visible instead of being swallowed as race.
    func testDeleteIfPresent_emptyManifestPath_propagatesNonNotFoundDeleteError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let manifestBytes = try Self.buildEmptyV1ManifestSqlite()
        let manifestPath = String(format: "\(basePath)/%04d/%02d/\(MonthManifestStore.manifestFileName)", 2025, 6)
        await client.injectFile(path: manifestPath, data: manifestBytes)
        await client.injectDeleteError(.permission, for: manifestPath)

        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w", basePath: basePath, storageType: .webdav)
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let service = makeService(client: client, profileID: profileID)
        do {
            _ = try await service.runPhase1(
                profileID: profileID, repoID: "r", writerID: "w", runID: "run-1"
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

    /// End-to-end cancellation contract: when URLSession-shape cancellation fires
    /// inside `CommitLogWriter.atomicCreate` mid-migration, `runFullMigration` must
    /// throw `CancellationError` — not a `CommitLogWriter.WriteError` variant, not
    /// retry-exhausted `.alreadyExists`. Per-writer cancellation contracts are pinned
    /// separately; this test guards the seam: a regression in gate normalization,
    /// writer catch ordering, or `shouldRetryMigrationCommitWrite`'s classifier
    /// (any one) breaks end-to-end while individual unit tests still pass.
    func testRunFullMigration_atomicCreateURLErrorCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // `.exclusive` so the writer's direct atomicCreate boundary fires (not the
        // gate's staging path). InMemoryRemoteStorageClient's atomicCreate honors
        // `injectAtomicCreateURLErrorCancelled` regardless of the configured guarantee.
        client.setAtomicCreateGuarantee(.exclusive)

        let assetFP = TestFixtures.fingerprint(0xCE)
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
        let repoID = "test-repo-id"
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

    // MARK: - Helpers

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

    /// Schema-only manifest, no rows. Drives runPhase1 into the empty-manifest branch
    /// which calls `deleteIfPresent` on the source path.
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
}
