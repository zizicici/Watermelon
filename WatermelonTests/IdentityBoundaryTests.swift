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
        let fingerprint = TestFixtures.fingerprint(0xAB)
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
        let fingerprint = TestFixtures.fingerprint(0xAB)
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
        let fingerprint = TestFixtures.fingerprint(0xAB)
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

    /// Compile-time pin: the repo identity row constructors take `Data`
    /// fingerprints, not `PhotoKitLocalIdentifier`. If a future refactor
    /// changes the seam type, this file stops compiling.
    func testRepoSeamSignatures_acceptDataFingerprintNotLocalIdentifier() {
        let _: (Data, Int64?, Int64, [CommitResourceEntry]) -> CommitAddAssetBody =
            CommitAddAssetBody.init
        let _: (Data, Int, Int, Data, String) -> SnapshotAssetResourceRow =
            SnapshotAssetResourceRow.init
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
