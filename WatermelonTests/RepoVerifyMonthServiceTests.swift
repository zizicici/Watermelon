import XCTest
@testable import Watermelon

/// `RepoVerifyMonthService` is the post-backup integrity sweep producing the 5
/// `VerifyMonthReportKind` categories. Tests pin the diagnosis logic plus the
/// multi-path OR-check (a hash counts as "present" if ANY of its known physical
/// paths exists on remote).
final class RepoVerifyMonthServiceTests: XCTestCase {
    private let basePath = "/repo"
    private let repoID = "repo-test-id"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let runID = "run-001"
    private let month = LibraryMonthKey(year: 2026, month: 1)

    /// Verify and the manifest's incomplete check must use the SAME metadata-only set.
    /// Otherwise the same asset can be "complete" per backup but "cleanup candidate"
    /// per verify → verify wrongly tombstones a healthy asset.
    func testMetadataOnlyRolesAreSharedAcrossBackupAndVerify() {
        // Both must reference ResourceTypeCode.metadataOnlyRoles (the canonical set).
        let canonical = ResourceTypeCode.metadataOnlyRoles
        XCTAssertTrue(canonical.contains(ResourceTypeCode.adjustmentData),
                      "adjustmentData is the edit instructions — metadata-only by definition")
        XCTAssertTrue(canonical.contains(ResourceTypeCode.adjustmentBasePhoto),
                      "adjustmentBasePhoto is the pre-edit reference — without a primary photo, asset is unrestorable")
        XCTAssertTrue(canonical.contains(ResourceTypeCode.adjustmentBasePairedVideo))
        XCTAssertTrue(canonical.contains(ResourceTypeCode.adjustmentBaseVideo))
        XCTAssertFalse(canonical.contains(ResourceTypeCode.photo),
                       "primary content roles must NOT be metadata-only")
    }

    func testHealthyMonth_emitsEmptyReport() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0xAA)
        // Use the actual recomputed fingerprint; verify now also detects
        // recomputed-vs-stored mismatch so an arbitrary fp would be flagged.
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        await client.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty, "all resources present → no items")
    }

    func testAllResourcesGone_flagsCleanupEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.fingerprint(0x20)
        let hash = TestFixtures.fingerprint(0xBB)
        let path = "2026/01/missing.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        try await client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.count, 1)
        let item = try XCTUnwrap(report.items.first)
        XCTAssertEqual(item.kind, .allResourcesGone)
        XCTAssertEqual(item.assetFingerprint, fp)
        XCTAssertTrue(item.allowsCleanup, "all-resources-gone is cleanup-eligible")
    }

    /// Asset has only an adjustmentData (role=7) resource left → metadata-only,
    /// eligible for cleanup tombstone.
    func testMetadataOnlyLeft_flagsCleanupEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0xCC)
        // Use the actual recomputed fp; classifier checks fingerprintMismatch
        // before metadataOnlyLeft when resources exist.
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.adjustmentData, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/adjustment.bin"

        let body = CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: path,
                    logicalName: "adjustment.bin",
                    contentHash: hash,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.adjustmentData,
                    role: ResourceTypeCode.adjustmentData,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerA, seq: 1, runID: runID, month: month
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: 1, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: false
        )
        await client.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.count, 1)
        XCTAssertEqual(report.items.first?.kind, .metadataOnlyLeft)
    }

    /// Multi-writer collision-rename can publish the same content under different
    /// physical paths. The OR-check across `pathsByHash[hash]` must keep the asset
    /// healthy if ANY path exists; first-wins would falsely flag missing.
    func testMultiPathSameHash_anyPathPresent_isHealthy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0xDD)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let pathA = "2026/01/photo.jpg"
        let pathB = "2026/01/photo~widB.jpg"

        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: pathA, writerID: writerA)
        try await writeAssetCommit(writer: writer, seq: 2, clock: 2, fp: fp, hash: hash, path: pathB, writerID: writerB)
        await client.injectFile(path: "\(basePath)/\(pathB)", data: Self.expectedSizedBytes())
        try await client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty,
                      "multi-path OR check: alternate present → asset healthy, no items")
    }

    /// stored fp ≠ recomputed-from-links is a tampering / partial-commit signal that
    /// presence-only checks would miss. verify must surface it explicitly so health UI
    /// and `applyTombstones` can decide what to do (currently report-only, not auto-cleanup).
    func testFingerprintMismatch_whenStoredFpDoesNotMatchRecomputed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        // Store an arbitrary fp that does NOT equal the SHA256 of (role|slot|hash).
        let fp = TestFixtures.fingerprint(0x60)
        let hash = TestFixtures.fingerprint(0xFF)
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        await client.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.count, 1)
        let item = try XCTUnwrap(report.items.first)
        XCTAssertEqual(item.kind, .fingerprintMismatch)
        XCTAssertEqual(item.assetFingerprint, fp)
        XCTAssertFalse(item.allowsCleanup,
                       "fingerprintMismatch is report-only — auto-tombstoning could destroy a recoverable asset")
    }

    /// Surface list errors — silently treating "list failed" as "directory empty"
    /// would tombstone every asset on a network blip / 401 / permission error.
    func testListErrorPropagates_doesNotTombstoneEverything() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.fingerprint(0x50)
        let hash = TestFixtures.fingerprint(0xEE)
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/p.jpg")
        await client.injectFile(path: "\(basePath)/2026/01/p.jpg", contents: "x")
        await client.injectListError(.transport, for: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        do {
            _ = try await verifier.verify(month: month)
            XCTFail("expected list error to surface")
        } catch {
            // expected
        }
    }

    /// Commit log stores `Photo.HEIC`, server case-folds the filename to `photo.heic`
    /// on disk. Exact path compare would false-tombstone; the collisionKey predicate
    /// (matching V2MonthSession / probeMonthForMissing) keeps it healthy.
    func testCaseFoldedFilename_doesNotTombstone() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0xC0)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let storedPath = "2026/01/Photo.HEIC"
        let landedLeaf = "photo.heic"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: storedPath)
        await client.injectFile(path: "\(basePath)/2026/01/\(landedLeaf)", data: Self.expectedSizedBytes())

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty,
                      "collisionKey match — case-folded leaf still counts as present")
    }

    // MARK: - applyTombstones (Step 3: file-truth re-verify)

    /// Candidate stays missing → tombstone IS written.
    func testApplyTombstones_allResourcesGoneCandidate_writesTombstoneWhenStillMissing() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let fp = TestFixtures.fingerprint(0x21)
        let hash = TestFixtures.fingerprint(0x91)
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/gone.jpg")
        try await scaffold.client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: scaffold.client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.first?.kind, .allResourcesGone)

        try await verifier.applyTombstones(month: month, cleanupItems: report.items, services: v2)

        let materializer = RepoMaterializer(client: scaffold.client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertTrue(monthState.deletedAssetFingerprints.contains(fp), "tombstone must be written when file truly gone")
    }

    /// Candidate healed BETWEEN verify and apply → tombstone NOT written.
    /// The fix: re-verify uses directory listing, not commit-log hash set, so a
    /// late-arriving file resolves the candidate's `.allResourcesGone` to `.healthy`.
    func testApplyTombstones_healedBetweenVerifyAndApply_doesNotWriteTombstone() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0x92)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/heals.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        try await scaffold.client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: scaffold.client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.first?.kind, .allResourcesGone)

        // Heal: peer uploads the file before we apply tombstone.
        await scaffold.client.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())

        try await verifier.applyTombstones(month: month, cleanupItems: report.items, services: v2)

        let materializer = RepoMaterializer(client: scaffold.client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertFalse(monthState.deletedAssetFingerprints.contains(fp),
                       "healed candidate must NOT be tombstoned on re-verify")
        XCTAssertNotNil(monthState.assets[fp])
    }

    /// Hash had multiple physical paths (multi-writer collision-rename). Both gone
    /// at verify; one reappears before apply → asset healthy via OR-check, no tombstone.
    func testApplyTombstones_multiPathHeal_skipsTombstoneWhenAnyPathReappears() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let hash = TestFixtures.fingerprint(0x93)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let pathA = "2026/01/photo.jpg"
        let pathB = "2026/01/photo~widB.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: pathA, writerID: writerA)
        try await writeAssetCommit(writer: writer, seq: 2, clock: 2, fp: fp, hash: hash, path: pathB, writerID: writerA)
        try await scaffold.client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: scaffold.client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.first?.kind, .allResourcesGone)

        // Only pathB reappears.
        await scaffold.client.injectFile(path: "\(basePath)/\(pathB)", data: Self.expectedSizedBytes())

        try await verifier.applyTombstones(month: month, cleanupItems: report.items, services: v2)

        let materializer = RepoMaterializer(client: scaffold.client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: repoID)
        let monthState = try XCTUnwrap(output.state.months[month])
        XCTAssertFalse(monthState.deletedAssetFingerprints.contains(fp),
                       "OR-check across paths: pathB present is sufficient to keep asset healthy")
    }

    // MARK: - V2 services scaffolding (mirrors V2FlushTests)

    private struct ApplyTombstonesScaffold {
        let client: InMemoryRemoteStorageClient
        let database: DatabaseManager
        let dbURL: URL
        let cleanup: () -> Void
    }

    private func makeScaffold() throws -> ApplyTombstonesScaffold {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let dbURL = dir.appendingPathComponent("verify-test.sqlite")
        let db = try DatabaseManager(databaseURL: dbURL)
        return ApplyTombstonesScaffold(
            client: InMemoryRemoteStorageClient(),
            database: db,
            dbURL: dbURL,
            cleanup: { try? FileManager.default.removeItem(at: dir) }
        )
    }

    private func makeV2Services(scaffold: ApplyTombstonesScaffold) async throws -> BackupV2RuntimeServices {
        var profile = ServerProfileRecord(
            id: nil, name: "T", storageType: StorageType.smb.rawValue,
            connectionParams: nil, sortOrder: 0,
            host: "h", port: 0, shareName: "s", basePath: basePath,
            username: "u", domain: nil, credentialRef: "c",
            backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(),
            writerID: nil
        )
        try scaffold.database.saveServerProfile(&profile)
        let profileID = profile.id ?? 0
        let identity = RepoIdentity(database: scaffold.database)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerA)
        let allocator = SeqAllocator(database: scaffold.database, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: scaffold.database, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: scaffold.client, basePath: basePath)
        let liveness = LivenessTracker(client: scaffold.client, basePath: basePath, writerID: writerA, isLocalVolume: true)
        return BackupV2RuntimeServices(
            writerID: writerA, repoID: repoID, runID: runID,
            basePath: basePath, database: scaffold.database, identity: identity,
            seqAllocator: allocator, lamport: lamport,
            commitWriter: commitWriter, snapshotWriter: snapshotWriter,
            liveness: liveness, metadataClient: scaffold.client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    /// All fixtures write `fileSize: 100`. Size-aware presence requires the listed file to match — pad to 100 bytes.
    private static func expectedSizedBytes() -> Data {
        Data(repeating: 0x2A, count: 100)
    }

    private func writeAssetCommit(
        writer: CommitLogWriter,
        seq: UInt64,
        clock: UInt64,
        fp: Data,
        hash: Data,
        path: String,
        writerID: String? = nil
    ) async throws {
        let body = CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: path,
                    logicalName: (path as NSString).lastPathComponent,
                    contentHash: hash,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID,
            writerID: writerID ?? writerA,
            seq: seq,
            runID: runID,
            month: month,
            clockMin: clock,
            clockMax: clock
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: false
        )
    }
}
