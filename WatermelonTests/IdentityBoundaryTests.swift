import CryptoKit
import GRDB
import XCTest
@testable import Watermelon

// Pins the U03 identity-boundary invariant: PhotoKit `localIdentifier` must
// never reach repo-identity surfaces. Tests are pure static-shape /
// byte-output checks so they survive PhotoKit availability.
final class IdentityBoundaryTests: XCTestCase {

    // MARK: - Pattern A: Mirror-based row-shape scan

    /// Repo wire-format types must not expose a `String`-typed property whose
    /// name contains "localidentifier" — that would indicate a PhotoKit id is
    /// being carried in a snapshot/commit row, violating the U03 invariant.
    func testRepoRowShapes_doNotCarryLocalIdentifierFields() {
        let fingerprint = TestFixtures.assetFingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let stamp = TestFixtures.opStamp()

        let assetRow = SnapshotAssetRow(
            assetFingerprint: fingerprint,
            creationDateMs: 0,
            backedUpAtMs: 0,
            resourceCount: 1,
            totalFileSizeBytes: 0,
            stamp: stamp
        )
        let resourceRow = SnapshotResourceRow(
            physicalRemotePath: "p",
            contentHash: contentHash,
            fileSize: 0,
            resourceType: 1,
            creationDateMs: nil,
            backedUpAtMs: 0,
            crypto: nil,
            stamp: stamp
        )
        let assetResourceRow = SnapshotAssetResourceRow(
            assetFingerprint: fingerprint,
            role: 1,
            slot: 0,
            resourceHash: contentHash,
            logicalName: "name"
        )
        let addBody = CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: 0,
            resources: [CommitResourceEntry(
                physicalRemotePath: "p",
                logicalName: "n",
                contentHash: contentHash,
                fileSize: 0,
                resourceType: 1,
                role: 1,
                slot: 0,
                crypto: nil
            )]
        )
        let tombstoneBody = CommitTombstoneBody(
            assetFingerprint: fingerprint,
            reason: .userDeleted
        )
        let manifestAsset = RemoteManifestAsset(
            year: 2025,
            month: 1,
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: 0,
            resourceCount: 1,
            totalFileSizeBytes: 0
        )
        let link = RemoteAssetResourceLink(
            year: 2025,
            month: 1,
            assetFingerprint: fingerprint,
            resourceHash: contentHash,
            role: 1,
            slot: 0,
            logicalName: "n"
        )

        assertNoLocalIdentifierFields(in: assetRow)
        assertNoLocalIdentifierFields(in: resourceRow)
        assertNoLocalIdentifierFields(in: assetResourceRow)
        assertNoLocalIdentifierFields(in: addBody)
        assertNoLocalIdentifierFields(in: tombstoneBody)
        assertNoLocalIdentifierFields(in: manifestAsset)
        assertNoLocalIdentifierFields(in: link)
    }

    private func assertNoLocalIdentifierFields(in value: Any) {
        let mirror = Mirror(reflecting: value)
        let typeName = String(describing: type(of: value))
        for child in mirror.children {
            guard let label = child.label else { continue }
            XCTAssertFalse(
                label.lowercased().contains("localidentifier"),
                "Repo type \(typeName) has property '\(label)' that looks like a PhotoKit localIdentifier — U03 forbids this on the repo identity surface."
            )
        }
    }

    // MARK: - Pattern B: JSONL byte-leak round-trip

    /// CommitOpMapper / SnapshotRowMapper emit hex-encoded fingerprints. The
    /// emitted bytes must not contain the substring "localIdentifier" — a
    /// regression that introduces a localId field anywhere in the row codec
    /// would surface here.
    func testCommitOpMapper_emittedJSONL_doesNotMentionLocalIdentifier() throws {
        let fingerprint = TestFixtures.assetFingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let header = TestFixtures.makeCommitHeader(
            repoID: "repo-canary",
            writerID: "writer-canary",
            seq: 1,
            runID: "run-canary",
            month: LibraryMonthKey(year: 2025, month: 1)
        )
        let op = CommitOp(
            opSeq: 0,
            clock: 1,
            body: .addAsset(CommitAddAssetBody(
                assetFingerprint: fingerprint,
                creationDateMs: nil,
                backedUpAtMs: 0,
                resources: [CommitResourceEntry(
                    physicalRemotePath: "2025/01/IMG_1234.HEIC",
                    logicalName: "IMG_1234.HEIC",
                    contentHash: contentHash,
                    fileSize: 1,
                    resourceType: 1,
                    role: 1,
                    slot: 0,
                    crypto: nil
                )]
            ))
        )
        var lines: [String] = []
        lines.append(try CommitOpMapper.encodeHeaderLine(header))
        lines.append(try CommitOpMapper.encodeOpLine(op))
        let joined = lines.joined(separator: "\n")
        XCTAssertFalse(
            joined.localizedCaseInsensitiveContains("localidentifier"),
            "CommitOp JSONL must not mention 'localIdentifier' — found: \(joined)"
        )
    }

    func testSnapshotRowMapper_emittedJSONL_doesNotMentionLocalIdentifier() throws {
        let fingerprint = TestFixtures.assetFingerprint(0xAB)
        let contentHash = TestFixtures.fingerprint(0xCD)
        let stamp = TestFixtures.opStamp()
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(LibraryMonthKey(year: 2025, month: 1)),
            writerID: "writer-canary",
            repoID: "repo-canary",
            covered: CoveredRanges(rangesByWriter: [:])
        )
        let assetRow = SnapshotAssetRow(
            assetFingerprint: fingerprint,
            creationDateMs: 0,
            backedUpAtMs: 0,
            resourceCount: 1,
            totalFileSizeBytes: 0,
            stamp: stamp
        )
        let resourceRow = SnapshotResourceRow(
            physicalRemotePath: "2025/01/IMG_1234.HEIC",
            contentHash: contentHash,
            fileSize: 1,
            resourceType: 1,
            creationDateMs: nil,
            backedUpAtMs: 0,
            crypto: nil,
            stamp: stamp
        )
        let assetResourceRow = SnapshotAssetResourceRow(
            assetFingerprint: fingerprint,
            role: 1,
            slot: 0,
            resourceHash: contentHash,
            logicalName: "IMG_1234.HEIC"
        )
        var lines: [String] = []
        lines.append(try SnapshotRowMapper.encodeHeaderLine(header))
        lines.append(try SnapshotRowMapper.encodeAssetLine(assetRow))
        lines.append(try SnapshotRowMapper.encodeResourceLine(resourceRow))
        lines.append(try SnapshotRowMapper.encodeAssetResourceLine(assetResourceRow))
        let joined = lines.joined(separator: "\n")
        XCTAssertFalse(
            joined.localizedCaseInsensitiveContains("localidentifier"),
            "Snapshot JSONL must not mention 'localIdentifier' — found: \(joined)"
        )
    }

    // MARK: - Pattern C: Type-signature witness

    /// Compile-time pin: the repo identity row constructors take the typed
    /// `AssetFingerprint`, not raw `Data` and not `PhotoKitLocalIdentifier`.
    /// If a future refactor regresses the seam type, this file stops compiling.
    func testRepoSeamSignatures_acceptAssetFingerprintNotLocalIdentifier() {
        let _: (AssetFingerprint, Int64?, Int64, [CommitResourceEntry]) -> CommitAddAssetBody =
            CommitAddAssetBody.init
        let _: (AssetFingerprint, Int, Int, Data, String) -> SnapshotAssetResourceRow =
            SnapshotAssetResourceRow.init
    }

    /// Battle-plan Pattern C extensions: every restore- and snapshot-state seam
    /// that touches the asset-fingerprint identity must compile only against
    /// `AssetFingerprint`. Witness keypath/return-type access; the lookups
    /// fail to compile if any hop regresses to raw `Data`.
    func testRestorePipelineSeams_useAssetFingerprintEndToEnd() {
        let _: (RemoteAlbumItem) -> AssetFingerprint = { $0.assetFingerprint }
        let _: (RestoreService.RestoreItemDescriptor) -> AssetFingerprint = { $0.assetFingerprint }
        let _: (RestoreService.RestoredItem) -> AssetFingerprint = { $0.assetFingerprint }
    }

    func testRepoSnapshotStateSeams_useAssetFingerprintKey() {
        let _: (AssetResourceKey) -> AssetFingerprint = { $0.assetFingerprint }
        let _: (RepoMonthState) -> [AssetFingerprint: SnapshotAssetRow] = { $0.assets }
        let _: (RepoMonthState) -> [AssetFingerprint: OpStamp] = { $0.deletedAssetStamps }
        let _: (RepoMonthState) -> [AssetResourceKey: SnapshotAssetResourceRow] = { $0.assetResources }
    }

    // MARK: - Pattern G: AssetFingerprint short-data rejection

    /// `AssetFingerprint(decoding:)` is the only `Data`-taking constructor and
    /// must reject any payload that isn't exactly 32 bytes. If a future change
    /// loosens this 32-byte check, every wire/SQLite decode boundary loses its
    /// asset-fp length guarantee.
    func testAssetFingerprint_rejectsNon32ByteData() {
        XCTAssertNil(AssetFingerprint(decoding: Data()))
        XCTAssertNil(AssetFingerprint(decoding: Data([0x01])))
        XCTAssertNil(AssetFingerprint(decoding: Data(repeating: 0xFF, count: 16)))
        XCTAssertNil(AssetFingerprint(decoding: Data(repeating: 0xFF, count: 31)))
        XCTAssertNil(AssetFingerprint(decoding: Data(repeating: 0xFF, count: 33)))
        XCTAssertNil(AssetFingerprint(decoding: Data(repeating: 0xFF, count: 64)))
    }

    func testAssetFingerprint_acceptsExactly32Bytes() {
        XCTAssertNotNil(AssetFingerprint(decoding: Data(repeating: 0xAB, count: 32)))
        XCTAssertNotNil(AssetFingerprint(decoding: Data(repeating: 0x00, count: 32)))
        XCTAssertNotNil(AssetFingerprint(decoding: Data(repeating: 0xFF, count: 32)))
    }

    func testAssetFingerprintFromDigest_alwaysSucceeds() {
        let digest = SHA256.hash(data: Data("hello".utf8))
        let fp = AssetFingerprint(digest)
        XCTAssertEqual(fp.rawValue.count, 32)
        XCTAssertEqual(fp.rawValue, Data(digest))
    }

    // MARK: - Pattern H: Manifest blob corruption surfaces as manifest error

    /// Pattern H — integration. Drives the **real** manifest load/reload boundary:
    /// (1) loadSeeded creates a fresh manifest sqlite via the production path,
    /// (2) we inject a 16-byte (non-32) `assetFingerprint` BLOB directly into the
    ///     `assets` table to simulate a corrupt manifest row,
    /// (3) reloadCache() must throw the manifest-corruption error from
    ///     `MonthManifestStore+Loading.invalidAssetFingerprintBlobError`, not
    ///     silently drop the row.
    /// If the loader regresses to `continue` on `AssetFingerprint(decoding:) == nil`,
    /// this test fails.
    func testMonthManifestStore_invalidAssetFingerprintBlob_inAssetsTable_failsClosedOnReload() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let basePath = "/repo"
        let year = 2026
        let month = 5
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: year, month: month,
            seed: MonthManifestStore.Seed(resources: [], assets: [], assetResourceLinks: [])
        )

        // Inject a 16-byte BLOB into assets.assetFingerprint, bypassing the typed
        // upsertAsset path (which would refuse non-32-byte input at compile time).
        let invalidBlob = Data(repeating: 0x55, count: 16)
        try await store.dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO assets (assetFingerprint, creationDateMs, backedUpAtMs, resourceCount, totalFileSizeBytes)
                VALUES (?, ?, ?, ?, ?)
                """,
                arguments: [invalidBlob, Int64(0), Int64(0), 0, Int64(0)]
            )
        }

        // The real reload boundary must throw, not silently skip the row.
        XCTAssertThrowsError(try store.reloadCache()) { error in
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, "MonthManifestStore",
                           "manifest corruption must surface via MonthManifestStore domain so callers route on it")
            XCTAssertEqual(nsError.code, -42,
                           "manifest corruption code -42 is the contract between loader and recovery paths")
            XCTAssertTrue(nsError.localizedDescription.contains("assets"),
                          "error message must identify the table whose row was corrupt")
            XCTAssertTrue(nsError.localizedDescription.contains("16"),
                          "error message must report the observed (invalid) byte count")
        }
    }

    /// Same fail-closed invariant for the `asset_resources` link table. A short
    /// `assetFingerprint` on the link side must also surface as a corruption
    /// error, not silently drop the link from the reloaded in-memory index.
    func testMonthManifestStore_invalidAssetFingerprintBlob_inAssetResourcesTable_failsClosedOnReload() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let basePath = "/repo"
        let year = 2026
        let month = 6
        let store = try await MonthManifestStore.loadSeeded(
            client: client, basePath: basePath, year: year, month: month,
            seed: MonthManifestStore.Seed(resources: [], assets: [], assetResourceLinks: [])
        )

        let invalidBlob = Data(repeating: 0x77, count: 8)
        try await store.dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO asset_resources (assetFingerprint, resourceHash, role, slot)
                VALUES (?, ?, ?, ?)
                """,
                arguments: [invalidBlob, Data(repeating: 0xCC, count: 32), 1, 0]
            )
        }

        XCTAssertThrowsError(try store.reloadCache()) { error in
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, "MonthManifestStore")
            XCTAssertEqual(nsError.code, -42)
            XCTAssertTrue(nsError.localizedDescription.contains("asset_resources"),
                          "link-side corruption must identify asset_resources table")
            XCTAssertTrue(nsError.localizedDescription.contains("8"),
                          "error message must report the observed (invalid) byte count")
        }
    }

    /// Secondary coverage for Pattern H: the helper itself shapes the error.
    /// Kept alongside the integration tests so a regression on either side
    /// (helper signature drift or load-path silent skip) surfaces individually.
    func testMonthManifestStore_invalidAssetFingerprintBlob_isManifestCorruptionError() throws {
        let invalidLengthBlob = Data([0x01, 0x02, 0x03])
        let error = MonthManifestStore.invalidAssetFingerprintBlobError(
            table: "assets",
            actualByteCount: invalidLengthBlob.count
        )
        XCTAssertEqual(error.domain, "MonthManifestStore")
        XCTAssertEqual(error.code, -42, "manifest corruption error code must not drift; reload-time callers route on it")
        let message = error.localizedDescription
        XCTAssertTrue(message.contains("assets"), "error must identify which manifest table failed")
        XCTAssertTrue(message.contains("32"), "error must report the required byte count")
        XCTAssertTrue(message.contains("3"), "error must report the observed byte count")
    }

    /// Pattern G companion: confirms the failable init refuses common short
    /// fixtures that legacy tests once used as identity placeholders.
    func testAssetFingerprint_rejectsLegacyShortFixtures() {
        XCTAssertNil(AssetFingerprint(decoding: Data([0x01])))
        XCTAssertNil(AssetFingerprint(decoding: Data([0xAA, 0xBB])))
    }

    /// Compile-time pin: PhotoKitLocalIdentifier is iOS-target only. If this
    /// file (in WatermelonTests, iOS target) can reference it, the module
    /// graph is intact. If Shared/ ever pulls it in, that file will fail to
    /// compile.
    func testPhotoKitLocalIdentifier_isReachableFromIOSTarget() {
        let id = PhotoKitLocalIdentifier(rawValue: "test")
        XCTAssertEqual(id.rawValue, "test")
    }

    // MARK: - Pattern D: Remote naming invariance

    /// Remote filenames are derived from originalFilename + role + slot, not
    /// from PHAsset.localIdentifier. If a future refactor leaks the PhotoKit
    /// id into the remote path, this regresses.
    func testPreferredRemoteFileName_invariantToLocalIdentifier() {
        let stem = "IMG_1234"
        let resource = RemoteFileNaming.ResourceIdentity(
            role: ResourceTypeCode.photo,
            slot: 0,
            originalFilename: "IMG_1234.HEIC"
        )
        let name1 = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: stem,
            resource: resource
        )
        let name2 = RemoteFileNaming.preferredRemoteFileName(
            preferredAssetNameStem: stem,
            resource: resource
        )
        XCTAssertEqual(name1, name2, "Remote filename must be a deterministic function of (stem, originalFilename, role, slot).")
        XCTAssertFalse(
            name1.localizedCaseInsensitiveContains("localidentifier"),
            "Remote filename must not embed a 'localIdentifier' substring."
        )
    }
}
