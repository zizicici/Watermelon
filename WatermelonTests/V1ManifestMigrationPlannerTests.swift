import XCTest
@testable import Watermelon

/// Pure-transform contract for the V1 → V2 per-month conversion.
///
/// Skip-reason strings are part of the on-remote `partial-migration-marker.json` schema;
/// every assertion that pins a string is intentionally byte-for-byte.
final class V1ManifestMigrationPlannerTests: XCTestCase {
    private let year = 2025
    private let month = 6


    func testPlan_emptyInput_returnsEmptyPlan() {
        let plan = V1ManifestMigrationPlanner.plan(assets: [], resources: [], links: [])
        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, [])
    }

    func testPlan_oneAssetOneResourceOneLink_buildsOneCommitResourceEntry() {
        let fp = Self.bytes(0xAA)
        let contentHash = Self.bytes(0xBB)
        let asset = makeAsset(fp: fp, creationDateMs: 1_000, backedUpAtMs: 2_000, resourceCount: 1, totalFileSize: 7)
        let resource = makeResource(
            contentHash: contentHash,
            physicalRemotePath: "2025/06/AAAAA-r0-s0.heic",
            fileSize: 7,
            resourceType: 1,
            crypto: ResourceCryptoMetadata(scheme: "none")
        )
        let link = makeLink(assetFP: fp, resourceHash: contentHash, role: 0, slot: 0, logicalName: "IMG_0001.HEIC")

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])

        XCTAssertEqual(plan.skippedFailures, [])
        XCTAssertEqual(plan.migrable.count, 1)
        let entry = try? XCTUnwrap(plan.migrable.first)
        XCTAssertEqual(entry?.asset.assetFingerprint, AssetFingerprint(decoding: fp))
        XCTAssertEqual(entry?.resources.count, 1)
        let res = entry?.resources.first
        XCTAssertEqual(res?.physicalRemotePath, "2025/06/AAAAA-r0-s0.heic")
        XCTAssertEqual(res?.logicalName, "IMG_0001.HEIC", "non-empty link.logicalName must win over resource.logicalName")
        XCTAssertEqual(res?.contentHash, contentHash)
        XCTAssertEqual(res?.fileSize, 7)
        XCTAssertEqual(res?.resourceType, 1)
        XCTAssertEqual(res?.role, 0)
        XCTAssertEqual(res?.slot, 0)
        XCTAssertEqual(res?.crypto?.scheme, "none")
    }


    // testPlan_assetWithInvalidFingerprintLength_skippedAndReasonStringPinned was removed in U06.
    // Length enforcement moved from the planner to the wire/SQLite decode boundary via
    // `AssetFingerprint(decoding:)`. Pattern G in IdentityBoundaryTests pins the new check:
    // short/long blobs return nil at construction; they never reach the planner.

    func testPlan_assetWithNoMatchingLinks_skippedAndReasonStringPinned() {
        let fp = Self.bytes(0xAB)
        let asset = makeAsset(fp: fp)
        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [], links: [])
        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) has no resource links"])
    }

    func testPlan_linkReferencesMissingResource_skippedAndReasonStringPinned() {
        let fp = Self.bytes(0xAB)
        let referencedHash = Self.bytes(0xCC)
        let asset = makeAsset(fp: fp)
        let link = makeLink(assetFP: fp, resourceHash: referencedHash)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [], links: [link])

        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references missing resource \(referencedHash.hexString)"])
    }

    func testPlan_linkReferencesInvalidResourceHashLength_skippedAndReasonStringPinned() {
        let fp = Self.bytes(0xAB)
        let badHash = Data(repeating: 0x22, count: 31)
        let asset = makeAsset(fp: fp)
        let link = makeLink(assetFP: fp, resourceHash: badHash)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [], links: [link])

        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references invalid resource hash length 31"])
    }


    func testPlan_invalidBeforeMissingForSingleAsset() {
        let fp = Self.bytes(0xAB)
        let invalidHash = Data(repeating: 0x33, count: 33)
        let missingHash = Self.bytes(0xCD)
        let asset = makeAsset(fp: fp)
        // Order matters: invalid link first → invalid reason wins per the loop precedence.
        let badLink = makeLink(assetFP: fp, resourceHash: invalidHash, role: 0, slot: 0)
        let missingLink = makeLink(assetFP: fp, resourceHash: missingHash, role: 0, slot: 1)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [], links: [badLink, missingLink])

        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references invalid resource hash length 33"])
    }

    func testPlan_perAssetAtomic_noPartialCommitResourceEntriesOnFailure() {
        let fp = Self.bytes(0xAB)
        let goodHash = Self.bytes(0xCC)
        let missingHash = Self.bytes(0xDD)
        let asset = makeAsset(fp: fp)
        let resource = makeResource(contentHash: goodHash)
        let goodLink = makeLink(assetFP: fp, resourceHash: goodHash, role: 0, slot: 0)
        let missingLink = makeLink(assetFP: fp, resourceHash: missingHash, role: 0, slot: 1)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [goodLink, missingLink])

        XCTAssertEqual(plan.migrable.count, 0, "one bad link on an asset must skip the entire asset")
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references missing resource \(missingHash.hexString)"])
    }


    func testPlan_mixed_oneValidOneInvalidAsset_keepsValidAndReportsInvalid() {
        let validFP = Self.bytes(0xAA)
        let invalidFP = Data(repeating: 0x11, count: 31)
        let contentHash = Self.bytes(0xBB)
        let validAsset = makeAsset(fp: validFP)
        let invalidAsset = makeAsset(fp: invalidFP)
        let resource = makeResource(contentHash: contentHash, physicalRemotePath: "2025/06/x.bin")
        let link = makeLink(assetFP: validFP, resourceHash: contentHash, logicalName: "x.bin")

        let plan = V1ManifestMigrationPlanner.plan(
            assets: [invalidAsset, validAsset],
            resources: [resource],
            links: [link]
        )

        XCTAssertEqual(plan.migrable.count, 1)
        XCTAssertEqual(plan.migrable.first?.asset.assetFingerprint, AssetFingerprint(decoding: validFP))
        // V1ManifestMigrationPlanner no longer prefilters fingerprint length: AssetFingerprint
        // typing enforces 32 bytes at the wire/SQLite decode boundary. The plannerHelper above
        // pads short fixture bytes for compile-time compatibility, so this case now sees the
        // invalid asset as migrable too — the original "skipped" assertion has no analogue.
    }


    func testPlan_logicalName_emptyLinkFallsBackToResource() {
        let fp = Self.bytes(0xAA)
        let contentHash = Self.bytes(0xBB)
        let asset = makeAsset(fp: fp)
        let resource = makeResource(
            contentHash: contentHash,
            physicalRemotePath: "2025/06/asset-aaaa.heic"
        )
        let link = makeLink(assetFP: fp, resourceHash: contentHash, logicalName: "")

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])

        XCTAssertEqual(plan.migrable.count, 1)
        // resource.logicalName is derived from physicalRemotePath's last component.
        XCTAssertEqual(plan.migrable.first?.resources.first?.logicalName, "asset-aaaa.heic")
    }

    func testPlan_logicalName_nonEmptyLinkWinsOverResource() {
        let fp = Self.bytes(0xAA)
        let contentHash = Self.bytes(0xBB)
        let asset = makeAsset(fp: fp)
        let resource = makeResource(
            contentHash: contentHash,
            physicalRemotePath: "2025/06/asset-aaaa.heic"
        )
        let link = makeLink(assetFP: fp, resourceHash: contentHash, logicalName: "user-facing.heic")

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])

        XCTAssertEqual(plan.migrable.first?.resources.first?.logicalName, "user-facing.heic")
    }


    func testPlan_commitResourceEntryCarriesAllFields() {
        let fp = Self.bytes(0xAA)
        let contentHash = Self.bytes(0xBB)
        let crypto = ResourceCryptoMetadata(scheme: "v1", payload: ["k": "v"])
        let asset = makeAsset(fp: fp)
        let resource = makeResource(
            contentHash: contentHash,
            physicalRemotePath: "2025/06/pass-through.bin",
            fileSize: 12_345,
            resourceType: 2,
            crypto: crypto
        )
        let link = makeLink(assetFP: fp, resourceHash: contentHash, role: 3, slot: 5, logicalName: "pass-through.bin")

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])
        let res = try? XCTUnwrap(plan.migrable.first?.resources.first)

        XCTAssertEqual(res?.physicalRemotePath, "2025/06/pass-through.bin")
        XCTAssertEqual(res?.contentHash, contentHash)
        XCTAssertEqual(res?.fileSize, 12_345)
        XCTAssertEqual(res?.resourceType, 2)
        XCTAssertEqual(res?.role, 3)
        XCTAssertEqual(res?.slot, 5)
        XCTAssertEqual(res?.crypto?.scheme, "v1")
        XCTAssertEqual(res?.crypto?.payload, ["k": "v"])
    }


    // testPlan_hashLength31IsInvalid_assetFingerprint / testPlan_hashLength33IsInvalid_assetFingerprint
    // were removed in U06. The planner no longer enforces fingerprint length; AssetFingerprint(decoding:)
    // refuses non-32-byte input at the wire/SQLite decode boundary. Pattern G in
    // IdentityBoundaryTests covers the new check.
    func testPlan_hashLength32IsValid_assetFingerprint() {
        let fp = Data(repeating: 0x55, count: 32)
        let plan = V1ManifestMigrationPlanner.plan(assets: [makeAsset(fp: fp)], resources: [], links: [])
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) has no resource links"], "32-byte fp passes the hash gate; failure must be downstream (no links)")
    }

    func testPlan_linkResourceHash31_failsLinkHashGateBeforeResourceLookup() {
        let fp = Self.bytes(0xAA)
        let shortHash = Data(repeating: 0x55, count: 31)
        let asset = makeAsset(fp: fp)
        let resource = makeResource(contentHash: shortHash)
        let link = makeLink(assetFP: fp, resourceHash: shortHash)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])

        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references invalid resource hash length 31"])
    }

    func testPlan_resourceContentHash31_droppedFromIndex_linkWithValidHashReportsMissing() {
        let fp = Self.bytes(0xAA)
        let droppedResourceHash = Data(repeating: 0x55, count: 31)
        let linkHash = Self.bytes(0xCC)
        let asset = makeAsset(fp: fp)
        let resource = makeResource(contentHash: droppedResourceHash)
        let link = makeLink(assetFP: fp, resourceHash: linkHash)

        let plan = V1ManifestMigrationPlanner.plan(assets: [asset], resources: [resource], links: [link])

        XCTAssertEqual(plan.migrable.count, 0)
        XCTAssertEqual(plan.skippedFailures, ["asset \(fp.hexString) references missing resource \(linkHash.hexString)"])
    }


    private static func bytes(_ b: UInt8) -> Data {
        Data(repeating: b, count: 32)
    }

    private func makeAsset(
        fp: Data,
        creationDateMs: Int64? = 0,
        backedUpAtMs: Int64 = 0,
        resourceCount: Int = 1,
        totalFileSize: Int64 = 0
    ) -> RemoteManifestAsset {
        // V1 migration planner used to accept short fingerprints; current tests still inject
        // sub-32-byte data through this helper. Pad to 32 bytes so AssetFingerprint(decoding:)
        // succeeds; the test cases that probe "invalid fingerprint" use planner internals at the
        // V1MigrationService level, not this helper.
        let padded = fp.count == 32 ? fp : (fp + Data(repeating: 0, count: max(0, 32 - fp.count))).prefix(32)
        let typedFP = AssetFingerprint(decoding: Data(padded))!
        return RemoteManifestAsset(
            year: year,
            month: month,
            assetFingerprint: typedFP,
            creationDateMs: creationDateMs,
            backedUpAtMs: backedUpAtMs,
            resourceCount: resourceCount,
            totalFileSizeBytes: totalFileSize,
            stamp: nil
        )
    }

    private func makeResource(
        contentHash: Data,
        physicalRemotePath: String = "2025/06/r.bin",
        fileSize: Int64 = 1,
        resourceType: Int = 0,
        crypto: ResourceCryptoMetadata? = nil
    ) -> RemoteManifestResource {
        RemoteManifestResource(
            year: year,
            month: month,
            physicalRemotePath: physicalRemotePath,
            contentHash: contentHash,
            fileSize: fileSize,
            resourceType: resourceType,
            creationDateMs: 0,
            backedUpAtMs: 0,
            crypto: crypto
        )
    }

    private func makeLink(
        assetFP: Data,
        resourceHash: Data,
        role: Int = 0,
        slot: Int = 0,
        logicalName: String = "link.bin"
    ) -> RemoteAssetResourceLink {
        let padded = assetFP.count == 32 ? assetFP : (assetFP + Data(repeating: 0, count: max(0, 32 - assetFP.count))).prefix(32)
        let typedFP = AssetFingerprint(decoding: Data(padded))!
        return RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: typedFP,
            resourceHash: resourceHash,
            role: role,
            slot: slot,
            logicalName: logicalName
        )
    }
}
