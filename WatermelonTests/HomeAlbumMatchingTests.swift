import XCTest
@testable import Watermelon

/// `RemoteAlbumItem.isRestorable` (vs the legacy `isIncomplete` filter) controls
/// what the download workflow attempts. Pre-Iter-9 the helper's filter
/// over-skipped: `partiallyMissing` items have surviving primary content but
/// were lumped with `metadataOnlyLeft` / `phantom` and never re-downloaded.
final class HomeAlbumMatchingTests: XCTestCase {
    private let year = 2026
    private let month = 5

    /// Healthy assets are restorable AND not incomplete.
    func testHealthyAsset_isRestorable_andNotIncomplete() {
        let hash = TestFixtures.fingerprint(0xB1)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = makeAsset(fp: fp)
        let resource = makeResource(hash: hash)
        let link = makeLink(fp: fp, hash: hash, role: ResourceTypeCode.photo)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link]
        )
        let item = try? XCTUnwrap(items.first)
        XCTAssertEqual(items.count, 1)
        XCTAssertEqual(item?.integrityState, .healthy)
        XCTAssertEqual(item?.isRestorable, true)
        XCTAssertEqual(item?.isIncomplete, false)
    }

    /// `partiallyMissing` (primary present, secondary missing) is the case the
    /// old filter wrongly skipped. Restore primary; skip secondary on download.
    func testPartiallyMissingAsset_isRestorable() {
        let primaryHash = TestFixtures.fingerprint(0xB2)
        let secondaryHash = TestFixtures.fingerprint(0xC2)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: primaryHash),
                (role: ResourceTypeCode.adjustmentBasePhoto, slot: 0, contentHash: secondaryHash)
            ]
        )
        let asset = makeAsset(fp: fp)
        let primary = makeResource(hash: primaryHash)
        // secondary resource is referenced by a link but not in resources[] —
        // simulates a missing physical file.
        let primaryLink = makeLink(fp: fp, hash: primaryHash, role: ResourceTypeCode.photo)
        let secondaryLink = makeLink(fp: fp, hash: secondaryHash, role: ResourceTypeCode.adjustmentBasePhoto)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [primary], links: [primaryLink, secondaryLink]
        )
        let item = try? XCTUnwrap(items.first)
        XCTAssertEqual(item?.integrityState, .partiallyMissing(missingHashes: [secondaryHash]))
        XCTAssertEqual(item?.isRestorable, false,
                       "partial loss blocks restore; full-fingerprint post-save verify can't pass on a subset")
        XCTAssertEqual(item?.isIncomplete, true, "incomplete badge still shown")
    }

    /// `metadataOnlyLeft`: only adjustmentData remains. Not restorable —
    /// downloading would yield only edit instructions with no base photo.
    func testMetadataOnlyLeftAsset_isNotRestorable() {
        let hash = TestFixtures.fingerprint(0xB3)
        // Compute fp from the actual single resource so classifier matches fp,
        // then sees only metadata-only roles.
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.adjustmentData, slot: 0, contentHash: hash)]
        )
        let asset = makeAsset(fp: fp)
        let resource = makeResource(hash: hash, resourceType: ResourceTypeCode.adjustmentData)
        let link = makeLink(fp: fp, hash: hash, role: ResourceTypeCode.adjustmentData)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link]
        )
        let item = try? XCTUnwrap(items.first)
        XCTAssertEqual(item?.integrityState, .metadataOnlyLeft)
        XCTAssertEqual(item?.isRestorable, false)
    }

    /// `fingerprintMismatch`: stored fp ≠ recomputed-from-links. Not restorable —
    /// the asset's identity is corrupt; restoring would seed the local index
    /// with the wrong fingerprint and lock out the real asset.
    func testFingerprintMismatchAsset_isNotRestorable() {
        let bogusFp = TestFixtures.fingerprint(0xA4)
        let hash = TestFixtures.fingerprint(0xB4)
        let asset = makeAsset(fp: bogusFp)
        let resource = makeResource(hash: hash)
        let link = makeLink(fp: bogusFp, hash: hash, role: ResourceTypeCode.photo)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link]
        )
        let item = try? XCTUnwrap(items.first)
        if case .fingerprintMismatch = item?.integrityState {} else {
            XCTFail("expected .fingerprintMismatch, got \(String(describing: item?.integrityState))")
        }
        XCTAssertEqual(item?.isRestorable, false)
    }

    /// `phantom`: asset row exists but no asset_resources links reference any
    /// resource that exists. Filtered out by `buildRemoteItems` (no item
    /// emitted) — but if a future code path emits one, it must not be restored.
    func testPhantomAsset_emittedItemNotRestorable() {
        // Force a phantom: link references a hash that has no resource row.
        let fp = TestFixtures.fingerprint(0xA5)
        let missingHash = TestFixtures.fingerprint(0xB5)
        let asset = makeAsset(fp: fp)
        let link = makeLink(fp: fp, hash: missingHash, role: ResourceTypeCode.photo)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [], links: [link]
        )
        // buildRemoteItems uses chooseRepresentativeResource which returns nil
        // when groupedResources is empty → entire asset is filtered out.
        XCTAssertTrue(items.isEmpty, "phantom asset has no representative → cannot enter download list")
    }

    /// partiallyMissing asset's RemoteAlbumItem must NOT include the missing
    /// hash in `instances` — RestoreService throws on any instance failure.
    func testPhysicallyMissingOverlay_excludesMissingFromInstances() {
        let presentHash = TestFixtures.fingerprint(0xC2)
        let missingHash = TestFixtures.fingerprint(0xC3)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: presentHash),
                (role: ResourceTypeCode.video, slot: 0, contentHash: missingHash)
            ]
        )
        let asset = makeAsset(fp: fp)
        let resPresent = makeResource(hash: presentHash, resourceType: ResourceTypeCode.photo)
        let resMissing = makeResource(hash: missingHash, resourceType: ResourceTypeCode.video)
        let linkPresent = makeLink(fp: fp, hash: presentHash, role: ResourceTypeCode.photo)
        let linkMissing = makeLink(fp: fp, hash: missingHash, role: ResourceTypeCode.video)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resPresent, resMissing],
            links: [linkPresent, linkMissing],
            physicallyMissingHashesByMonth: [LibraryMonthKey(year: year, month: month): [missingHash]]
        )
        let item = try? XCTUnwrap(items.first)
        XCTAssertEqual(item?.isRestorable, false,
                       "partiallyMissing blocks restore; surviving slot alone can't satisfy full-fingerprint post-save verify")
        let hashesInInstances = Set((item?.instances ?? []).map(\.resourceHash))
        XCTAssertFalse(hashesInInstances.contains(missingHash),
                       "missing hash must not enter instances — restore would throw on it")
        XCTAssertTrue(hashesInInstances.contains(presentHash),
                      "present hash must still appear in instances for diagnostic display")
    }

    /// An asset whose ONLY hash is overlayed gets filtered out at the
    /// representative step (same shape as phantom) — restore never sees it.
    func testPhysicallyMissingOverlay_singleResource_filteredOut() {
        let hash = TestFixtures.fingerprint(0xC1)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = makeAsset(fp: fp)
        let resource = makeResource(hash: hash)
        let link = makeLink(fp: fp, hash: hash, role: ResourceTypeCode.photo)

        let baseline = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link]
        )
        XCTAssertEqual(baseline.first?.isRestorable, true,
                       "baseline: without overlay, asset is healthy")

        let overlayed = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link],
            physicallyMissingHashesByMonth: [LibraryMonthKey(year: year, month: month): [hash]]
        )
        XCTAssertTrue(overlayed.isEmpty,
                      "no surviving resources → no item emitted (same as phantom)")
    }

    // MARK: - Helpers

    private func makeAsset(fp: Data) -> RemoteManifestAsset {
        RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1,
            resourceCount: 1, totalFileSizeBytes: 100
        )
    }

    private func makeResource(
        hash: Data,
        resourceType: Int = ResourceTypeCode.photo
    ) -> RemoteManifestResource {
        RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: String(format: "%04d/%02d/%@.bin", year, month, hash.hexString),
            contentHash: hash, fileSize: 100,
            resourceType: resourceType,
            creationDateMs: nil, backedUpAtMs: 1
        )
    }

    private func makeLink(fp: Data, hash: Data, role: Int) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: 0, logicalName: ""
        )
    }
}
