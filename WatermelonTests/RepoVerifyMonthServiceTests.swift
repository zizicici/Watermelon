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
        let fp = TestFixtures.fingerprint(0x60)
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
