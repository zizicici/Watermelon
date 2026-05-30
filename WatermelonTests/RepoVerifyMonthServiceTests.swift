import CryptoKit
import XCTest
@testable import Watermelon

final class RepoVerifyMonthServiceTests: XCTestCase {
    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let runID = "run-001"
    private let month = LibraryMonthKey(year: 2026, month: 1)

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
        let hash = Self.expectedSizedHash
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
        let fp = TestFixtures.assetFingerprint(0x20)
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

    func testMetadataOnlyLeft_flagsCleanupEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = Self.expectedSizedHash
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

    func testMultiPathSameHash_anyPathPresent_isHealthy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = Self.expectedSizedHash
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

    func testFingerprintMismatch_whenStoredFpDoesNotMatchRecomputed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        // Keep content valid so the test isolates stored fingerprint drift.
        let fp = TestFixtures.assetFingerprint(0x60)
        let hash = Self.expectedSizedHash
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

    func testListErrorPropagates_doesNotTombstoneEverything() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x50)
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

    func testCaseFoldedFilename_doesNotTombstone() async throws {
        // InMemoryRemoteStorageClient declares case-sensitive; using the same leaf on disk and in the commit forces exact-name match.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let storedPath = "2026/01/Photo.HEIC"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: storedPath)
        await client.injectFile(path: "\(basePath)/\(storedPath)", data: Self.expectedSizedBytes())

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty,
                      "exact-name match — case-sensitive lookup at stored leaf finds the file")
    }


    func testApplyTombstones_allResourcesGoneCandidate_writesTombstoneWhenStillMissing() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x21)
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
        XCTAssertTrue(monthState.deletedAssetStamps.keys.contains(fp), "tombstone must be written when file truly gone")
    }

    func testApplyTombstones_healedBetweenVerifyAndApply_doesNotWriteTombstone() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let hash = Self.expectedSizedHash
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
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp),
                       "healed candidate must NOT be tombstoned on re-verify")
        XCTAssertNotNil(monthState.assets[fp])
    }

    func testApplyTombstones_multiPathHeal_skipsTombstoneWhenAnyPathReappears() async throws {
        let scaffold = try makeScaffold()
        defer { scaffold.cleanup() }
        try await scaffold.client.connect()
        let v2 = try await makeV2Services(scaffold: scaffold)

        let writer = CommitLogWriter(client: scaffold.client, basePath: basePath)
        let hash = Self.expectedSizedHash
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
        XCTAssertFalse(monthState.deletedAssetStamps.keys.contains(fp),
                       "OR-check across paths: pathB present is sufficient to keep asset healthy")
    }

    /// A listed-file metadata 404 after the file appeared in the month listing must be
    /// inconclusive, not authoritative absence. Previously the metadata 404 path in
    /// RemoteContentTrust.verifyHashResult returned .noContent, which let verify-month
    /// classify the resource as missing and issue tombstones against a healthy file.
    func testListedFileMetadataNotFound_isInconclusive_notMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        // File is on remote and will be listed.
        await client.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())
        // Metadata probe returns not-found despite the file being listed.
        await client.injectMetadataError(.notFound, for: "\(basePath)/\(path)")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        // The resource was listed but metadata was uncertain — must not be cleanup-eligible.
        let cleanupEligible = report.items.filter(\.allowsCleanup)
        XCTAssertTrue(cleanupEligible.isEmpty,
                      "listed file with metadata 404 must not be cleanup-eligible, got \(report.items.map(\.kind))")
        // Should surface as verification-incomplete, not allResourcesGone.
        XCTAssertEqual(report.items.count, 1)
        XCTAssertEqual(report.items.first?.kind, .verificationIncomplete)
    }

    /// Production clients (S3/WebDAV/SFTP/SMB) return nil for not-found metadata rather than
    /// throwing. This tests the nil-return path specifically: file is listed but metadata
    /// returns nil (simulating stale listing, eventual-consistency race, etc.).
    func testListedFileMetadataNil_isInconclusive_notMissing() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let writer = CommitLogWriter(client: inner, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        // File exists on remote so the listing will see it.
        await inner.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())

        // Wrap the client so metadata returns nil for the resource path (production not-found shape).
        let resourcePath = "\(basePath)/\(path)"
        let client = MetadataNilWrapper(inner: inner, nilPaths: [resourcePath])

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        let cleanupEligible = report.items.filter(\.allowsCleanup)
        XCTAssertTrue(cleanupEligible.isEmpty,
                      "listed file with metadata nil must not be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertEqual(report.items.count, 1)
        XCTAssertEqual(report.items.first?.kind, .verificationIncomplete)
    }


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
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: scaffold.database, identity: identity,
            seqAllocator: allocator, lamport: lamport,
            commitWriter: commitWriter, snapshotWriter: snapshotWriter,
            liveness: liveness,
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: scaffold.client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private static func expectedSizedBytes() -> Data {
        Data(repeating: 0x2A, count: 100)
    }

    private static var expectedSizedHash: Data {
        Data(SHA256.hash(data: expectedSizedBytes()))
    }

    private func writeAssetCommit(
        writer: CommitLogWriter,
        seq: UInt64,
        clock: UInt64,
        fp: AssetFingerprint,
        hash: Data,
        path: String,
        writerID: String? = nil,
        backedUpAtMs: Int64 = 1
    ) async throws {
        let body = CommitAddAssetBody(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: backedUpAtMs,
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

    /// A grace backend's month LIST can omit a just-written, durable resource. Verify must probe the
    /// recorded path directly before concluding absence, not tombstone healthy bytes from one stale LIST.
    func testGraceBackend_listOmitsPresentResource_recoveredAsHealthy() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let writer = CommitLogWriter(client: inner, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        // File is durable on remote, but the month listing (grace lag) omits it.
        await inner.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())
        let client = ListOmitGraceWrapper(inner: inner, omittedPaths: ["\(basePath)/\(path)"], grace: 30)

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty,
                      "grace backend: a present resource omitted from one stale LIST must be recovered via direct probe, got \(report.items.map(\.kind))")
    }

    /// Same path on a grace backend when a *recently* written resource is genuinely unreadable: verify
    /// must stay inconclusive (not cleanup-eligible) rather than tombstone — list omission within the
    /// read-after-write window is not proof of absence. Cleanup defers to a later verify outside it.
    func testGraceBackend_listOmitsFreshUnreadableResource_isInconclusiveNotCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x22)
        let hash = TestFixtures.fingerprint(0x92)
        let freshMs = Int64(Date().timeIntervalSince1970 * 1000)
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/lagging.jpg", backedUpAtMs: freshMs)
        // Month dir exists (listing succeeds) but the data file is not present yet.
        try await client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        let cleanupEligible = report.items.filter(\.allowsCleanup)
        XCTAssertTrue(cleanupEligible.isEmpty,
                      "grace backend: a freshly-written list-omitted resource must not be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertEqual(report.items.first?.kind, .verificationIncomplete)
    }

    /// A grace backend's recorded-path probe must be gated on freshness, not just capability: an OLD
    /// committed resource whose physical file is genuinely gone has had ample time to become consistent,
    /// so verify must classify it `.allResourcesGone` (cleanup-eligible) rather than loop on
    /// `.verificationIncomplete` forever and never tombstone the stale commit/snapshot state.
    func testGraceBackend_listOmitsStaleUnreadableResource_isCleanupEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x23)
        let hash = TestFixtures.fingerprint(0x93)
        // backedUpAtMs: 1 — committed in 1970, far outside any read-after-write window.
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/gone.jpg", backedUpAtMs: 1)
        // Month dir exists (listing succeeds) but the data file is genuinely absent.
        try await client.createDirectory(path: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.count, 1)
        let item = try XCTUnwrap(report.items.first)
        XCTAssertEqual(item.kind, .allResourcesGone,
                       "grace backend: an old genuinely-gone resource must be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertTrue(item.allowsCleanup)
    }

    /// The whole physical month directory genuinely 404s on a grace backend (not just one file). An OLD
    /// committed resource must still be cleanup-eligible — the directory-level boundary gets the same
    /// freshness gate as the file-level recorded-path probe, so verify can finally tombstone gone bytes
    /// instead of looping `.verificationIncomplete` forever.
    func testGraceBackend_monthDirNotFound_staleResource_isCleanupEligible() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x24)
        let hash = TestFixtures.fingerprint(0x94)
        // backedUpAtMs: 1 — committed in 1970, far outside any read-after-write window.
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/gone.jpg", backedUpAtMs: 1)
        // Entire month directory LIST 404s (directory genuinely removed), not just the file.
        await client.injectListError(.notFound, for: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.count, 1)
        let item = try XCTUnwrap(report.items.first)
        XCTAssertEqual(item.kind, .allResourcesGone,
                       "grace backend: an old resource whose whole month dir is gone must be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertTrue(item.allowsCleanup)
    }

    /// Same whole-month 404 but the resource was written within the grace window: the directory listing
    /// may simply be lagging the just-written month, so verify must stay `.verificationIncomplete`
    /// (not cleanup-eligible) and defer to a later verify outside the window.
    func testGraceBackend_monthDirNotFound_freshResource_isInconclusiveNotCleanup() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let fp = TestFixtures.assetFingerprint(0x25)
        let hash = TestFixtures.fingerprint(0x95)
        let freshMs = Int64(Date().timeIntervalSince1970 * 1000)
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/fresh.jpg", backedUpAtMs: freshMs)
        await client.injectListError(.notFound, for: "\(basePath)/2026/01")

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        let cleanupEligible = report.items.filter(\.allowsCleanup)
        XCTAssertTrue(cleanupEligible.isEmpty,
                      "grace backend: a fresh resource whose whole month dir 404s must not be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertEqual(report.items.first?.kind, .verificationIncomplete)
    }

    // P04 R25: nil metadata must be .inconclusive, not .noContent. Callers
    // (verify-month, overlay probe) pre-list the directory and use .noContent as
    // "try next match" → falls through to .missing → tombstones healthy bytes.
    func testVerifyHashResult_nilMetadata_returnsInconclusive() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let hash = Data(SHA256.hash(data: Data("test".utf8)))
        let result = try await RemoteContentTrust.verifyHashResult(
            client: client,
            remotePath: "/repo/nonexistent.dat",
            expectedSize: 100,
            expectedHash: hash
        )
        XCTAssertEqual(result, .inconclusive)
    }

    // P04 R26: metadata size disagreement after the caller's listing is an
    // overwrite/truncation race, same class as nil. Must be .inconclusive
    // to avoid tombstoning healthy bytes via the same caller fall-through.
    func testVerifyHashResult_metadataSizeDisagreement_returnsInconclusive() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let path = "/repo/2026/01/photo.jpg"
        await inner.injectFile(path: path, data: Self.expectedSizedBytes())
        let client = MetadataSizeOverrideWrapper(inner: inner, sizeOverrides: [path: 50])
        let result = try await RemoteContentTrust.verifyHashResult(
            client: client,
            remotePath: path,
            expectedSize: 100,
            expectedHash: Self.expectedSizedHash
        )
        XCTAssertEqual(result, .inconclusive)
    }

    // P04 R26: end-to-end — verify-month must not tombstone a listed file whose
    // metadata HEAD races to a different size (overwrite in flight).
    func testListedFileMetadataSizeDisagreement_isInconclusive_notMissing() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let writer = CommitLogWriter(client: inner, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let path = "2026/01/photo.jpg"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        await inner.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())
        // metadata reports a different size than the listing (truncation in flight).
        let client = MetadataSizeOverrideWrapper(inner: inner, sizeOverrides: ["\(basePath)/\(path)": 50])

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        let cleanupEligible = report.items.filter(\.allowsCleanup)
        XCTAssertTrue(cleanupEligible.isEmpty,
                      "listed file whose metadata size raced must not be cleanup-eligible, got \(report.items.map(\.kind))")
        XCTAssertEqual(report.items.count, 1)
        XCTAssertEqual(report.items.first?.kind, .verificationIncomplete)
    }

    /// Zero-grace case-sensitive backend (SFTP / case-sensitive external volume) that stores the leaf
    /// in a different Unicode normalization than the recorded NFC path. Byte-exact presenceKey no longer
    /// matches the listed leaf, so verify must direct-probe the recorded path before tombstoning healthy bytes.
    func testZeroGraceCaseSensitive_listNFDvsRecordedNFC_recoveredAsHealthy() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let writer = CommitLogWriter(client: inner, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let baseLeaf = "cafe\u{0301}.jpg"
        let nfcLeaf = baseLeaf.precomposedStringWithCanonicalMapping
        let nfdLeaf = baseLeaf.decomposedStringWithCanonicalMapping
        XCTAssertNotEqual(Data(nfcLeaf.utf8), Data(nfdLeaf.utf8), "test premise: NFC and NFD bytes differ")

        let path = "2026/01/\(nfcLeaf)"
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: path)
        await inner.injectFile(path: "\(basePath)/\(path)", data: Self.expectedSizedBytes())
        // Case-sensitive backend lists the same file under its NFD leaf; metadata/download still serve NFC.
        let client = ListLeafNormalizationWrapper(inner: inner, recordedLeafToListedLeaf: [nfcLeaf: nfdLeaf])

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertTrue(report.items.isEmpty,
                      "zero-grace case-sensitive backend: a present resource listed under a canonically-equivalent NFD leaf must be recovered via direct probe, got \(report.items.map(\.kind))")
    }

    /// Byte-exact backend: the committed (recorded) NFC path is genuinely absent and only a same-size,
    /// same-hash *orphan* exists under the canonically-equivalent NFD leaf. The canonical-equivalence
    /// fallback must prove the recorded path before returning present — restore only ever fetches the
    /// committed path, so hashing the orphan would mark a non-restorable asset healthy.
    func testZeroGraceCaseSensitive_committedPathAbsent_orphanSiblingSameHash_notHealthy() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let writer = CommitLogWriter(client: inner, basePath: basePath)
        let hash = Self.expectedSizedHash
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let baseLeaf = "cafe\u{0301}.jpg"
        let nfcLeaf = baseLeaf.precomposedStringWithCanonicalMapping
        let nfdLeaf = baseLeaf.decomposedStringWithCanonicalMapping
        XCTAssertNotEqual(Data(nfcLeaf.utf8), Data(nfdLeaf.utf8), "test premise: NFC and NFD bytes differ")

        // Recorded committed path is the NFC leaf; it is genuinely gone (never injected at NFC).
        try await writeAssetCommit(writer: writer, seq: 1, clock: 1, fp: fp, hash: hash, path: "2026/01/\(nfcLeaf)", backedUpAtMs: 1)
        let client = OrphanCanonicalSiblingWrapper(
            inner: inner,
            monthDirAbs: "\(basePath)/2026/01",
            orphanLeaf: nfdLeaf,
            orphanData: Self.expectedSizedBytes()
        )

        let verifier = RepoVerifyMonthService(client: client, basePath: basePath, expectedRepoID: repoID)
        let report = try await verifier.verify(month: month)
        XCTAssertEqual(report.items.first?.kind, .allResourcesGone,
                       "committed path absent must not be proven present from an uncommitted canonical sibling, got \(report.items.map(\.kind))")
    }
}

/// Byte-exact (case-sensitive) backend where the recorded committed path is genuinely absent and only a
/// same-size, same-hash *orphan* exists under a canonically-equivalent (NFD) leaf. Unlike InMemory's
/// Swift-String keys (which collapse NFC/NFD), metadata/download here keep the two byte-distinct, so a
/// recorded-NFC probe 404s while the orphan-NFD probe would succeed. `.watermelon/` paths pass through.
private struct OrphanCanonicalSiblingWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let monthDirAbs: String
    let orphanLeaf: String
    let orphanData: Data

    private var orphanAbs: String { "\(monthDirAbs)/\(orphanLeaf)" }
    private func byteEqual(_ a: String, _ b: String) -> Bool { Data(a.utf8) == Data(b.utf8) }
    private var orphanEntry: RemoteStorageEntry {
        RemoteStorageEntry(path: orphanAbs, name: orphanLeaf, isDirectory: false,
                           size: Int64(orphanData.count), creationDate: nil, modificationDate: nil)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        if byteEqual(path, monthDirAbs) { return [orphanEntry] }
        return try await inner.list(path: path)
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if byteEqual(path, orphanAbs) { return orphanEntry }
        if path.hasPrefix("\(monthDirAbs)/") { return nil }
        return try await inner.metadata(path: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        if byteEqual(remotePath, orphanAbs) { try orphanData.write(to: localURL); return }
        if remotePath.hasPrefix("\(monthDirAbs)/") {
            throw NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError)
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Zero-grace, case-sensitive backend whose listing returns a leaf under a canonically-equivalent but
/// byte-different Unicode normalization than the recorded path, while metadata/download serve the
/// recorded path. Simulates an HFS+/SFTP endpoint that stores NFD while the manifest recorded NFC.
private struct ListLeafNormalizationWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let recordedLeafToListedLeaf: [String: String]

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).map { entry in
            guard let listed = recordedLeafToListedLeaf[entry.name] else { return entry }
            let parent = (entry.path as NSString).deletingLastPathComponent
            return RemoteStorageEntry(
                path: parent.isEmpty ? listed : "\(parent)/\(listed)",
                name: listed,
                isDirectory: entry.isDirectory,
                size: entry.size,
                creationDate: entry.creationDate,
                modificationDate: entry.modificationDate
            )
        }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Wraps a storage client and returns nil from metadata for specific paths,
/// simulating the production S3/WebDAV/SFTP/SMB not-found behavior.
private struct MetadataNilWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let nilPaths: Set<String>

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if nilPaths.contains(path) { return nil }
        return try await inner.metadata(path: path)
    }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Wraps a storage client and drops specific paths from `list` results while still serving them
/// via metadata/download, simulating a grace backend whose month listing omits a durable file.
private struct ListOmitGraceWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let omittedPaths: Set<String>
    let grace: TimeInterval

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var readAfterWriteGraceSeconds: TimeInterval { grace }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { !omittedPaths.contains($0.path) }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Wraps a storage client and overrides metadata().size for specific paths,
/// simulating an overwrite/truncation race observed between LIST and HEAD.
private struct MetadataSizeOverrideWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let sizeOverrides: [String: Int64]

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        guard let original = try await inner.metadata(path: path) else { return nil }
        guard let overrideSize = sizeOverrides[path] else { return original }
        return RemoteStorageEntry(
            path: original.path,
            name: original.name,
            isDirectory: original.isDirectory,
            size: overrideSize,
            creationDate: original.creationDate,
            modificationDate: original.modificationDate
        )
    }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}
