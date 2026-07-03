import XCTest
@testable import Watermelon

// buildRemoteItems is a consumer of the "backed up = has real media" rule. A config-only (adjustment sidecar
// only) or phantom record must be dropped: otherwise it inflates the incomplete-download prompt count and,
// under `.createNewAsset`, feeds a base-less PHAssetCreationRequest that throws and aborts the whole month's
// restore. This must agree with RemoteBrowserAssetBuilder / RemoteMonthResolver / hasBackedUpMedia.
final class HomeAlbumMatchingTests: XCTestCase {
    private let year = 2024
    private let month = 1

    private func build(assets: [RemoteManifestAsset], resources: [RemoteManifestResource], links: [RemoteAssetResourceLink]) -> [RemoteAlbumItem] {
        HomeAlbumMatching.buildRemoteItems(assets: assets, resources: resources, links: links)
    }

    private func fingerprint(of roleHashes: [(role: Int, slot: Int, contentHash: Data)]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: roleHashes)
    }

    func testClassifiesByLinkRoleNotResourceType() {
        // A content-addressed resource row's stored type can diverge from a link's role (dedup). Classification must
        // follow the LINK role — a photo link to a resource row whose stored type says adjustmentData is still real
        // media (matching RemoteBrowserAssetBuilder / RemoteMonthResolver / hasBackedUpMedia). The old code read
        // resource.resourceType here and would have wrongly dropped it.
        let fp = Data([0x41]); let hash = Data([0x42])
        let items = build(
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: hash, resourceType: ResourceTypeCode.adjustmentData)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash, role: ResourceTypeCode.photo)]
        )
        XCTAssertEqual(items.count, 1, "the photo link is real media even though the resource row's stored type is adjustmentData")
        XCTAssertEqual(items.first?.mediaKind, .photo, "media kind follows the link role")
    }

    func testDropsConfigOnlyAsset() {
        let fp = Data([0x11]); let metaHash = Data([0x12])
        let items = build(
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: metaHash, resourceType: ResourceTypeCode.adjustmentData)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: metaHash, role: ResourceTypeCode.adjustmentData)]
        )
        XCTAssertTrue(items.isEmpty, "a config-only record has no restorable media and must be dropped")
    }

    func testKeepsPartialWithMediaFlaggedIncomplete() {
        let fp = Data([0x21]); let photoHash = Data([0x22]); let missing = Data([0x29])
        // role photo resolves; a fullSizePhoto side points at a missing resource → incomplete but has real media.
        let items = build(
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: photoHash, resourceType: ResourceTypeCode.photo)],
            links: [
                TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: photoHash, role: ResourceTypeCode.photo),
                TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: missing, role: ResourceTypeCode.fullSizePhoto)
            ]
        )
        XCTAssertEqual(items.count, 1, "a partial record that still resolves a photo is kept")
        XCTAssertTrue(items.first?.isIncomplete == true, "…and flagged incomplete")
        XCTAssertFalse(ResourceRole.isMetadataOnly(items.first!.representative.resourceType), "representative is real media, not the sidecar")
    }

    func testKeepsCompletePhotoNotFlagged() {
        let photoHash = Data([0x32])
        let fp = fingerprint(of: [(role: ResourceTypeCode.photo, slot: 0, contentHash: photoHash)])
        let items = build(
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: photoHash, resourceType: ResourceTypeCode.photo)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: photoHash, role: ResourceTypeCode.photo)]
        )
        XCTAssertEqual(items.count, 1)
        XCTAssertFalse(items.first?.isIncomplete == true, "a complete record is not flagged incomplete")
    }
}
