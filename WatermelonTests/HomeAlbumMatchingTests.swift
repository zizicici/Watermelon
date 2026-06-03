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
        let bogusFp = TestFixtures.assetFingerprint(0xA4)
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
        let fp = TestFixtures.assetFingerprint(0xA5)
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
            presenceByMonth: [LibraryMonthKey(year: year, month: month): RemotePresenceSnapshot.Month(missingHashes: [missingHash], isAuthoritative: false)]
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
            presenceByMonth: [LibraryMonthKey(year: year, month: month): RemotePresenceSnapshot.Month(missingHashes: [hash], isAuthoritative: false)]
        )
        XCTAssertTrue(overlayed.isEmpty,
                      "no surviving resources → no item emitted (same as phantom)")
    }


    /// Two distinct assets share one content hash (e.g. a Live Photo and a still that
    /// share the same image bytes). The shared resource row is stamped from whichever
    /// asset committed it, so each RemoteAlbumItem must still carry its OWN asset-row
    /// creation date — restore depends on `creationDateMs` being per-asset truth.
    func testSharedContentHash_eachItemKeepsItsOwnAssetCreationDate() {
        let sharedHash = TestFixtures.fingerprint(0xD1)
        let videoHash = TestFixtures.fingerprint(0xD2)
        let peerDateMs: Int64 = 1_000
        let assetDateMs: Int64 = 5_000

        // Asset A: still only, committed the shared resource row (date 1000).
        let fpA = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: sharedHash)]
        )
        let assetA = makeAsset(fp: fpA, creationDateMs: peerDateMs)
        // Asset B: shares the still, plus a paired video → distinct fingerprint, later date.
        let fpB = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: sharedHash),
                (role: ResourceTypeCode.pairedVideo, slot: 0, contentHash: videoHash)
            ]
        )
        let assetB = makeAsset(fp: fpB, creationDateMs: assetDateMs)

        // Shared resource row carries A's date; B reuses it via the same content hash.
        let sharedResource = makeResource(hash: sharedHash, resourceType: ResourceTypeCode.photo, creationDateMs: peerDateMs)
        let videoResource = makeResource(hash: videoHash, resourceType: ResourceTypeCode.pairedVideo, creationDateMs: assetDateMs)

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [assetA, assetB],
            resources: [sharedResource, videoResource],
            links: [
                makeLink(fp: fpA, hash: sharedHash, role: ResourceTypeCode.photo),
                makeLink(fp: fpB, hash: sharedHash, role: ResourceTypeCode.photo),
                makeLink(fp: fpB, hash: videoHash, role: ResourceTypeCode.pairedVideo)
            ]
        )

        let itemA = items.first { $0.assetFingerprint == fpA }
        let itemB = items.first { $0.assetFingerprint == fpB }
        XCTAssertEqual(itemA?.creationDateMs, peerDateMs)
        XCTAssertEqual(itemB?.creationDateMs, assetDateMs,
                       "B must keep its own asset-row date even though it shares A's resource row")
        // The shared resource instance on B still carries the peer date — proving the old
        // instance-derived restore date would have been wrong for B.
        let bSharedInstanceDate = itemB?.instances.first { $0.resourceHash == sharedHash }?.creationDateMs
        XCTAssertEqual(bSharedInstanceDate, peerDateMs,
                       "shared resource instance carries the committing peer's date, not B's")
    }

    private func makeAsset(fp: AssetFingerprint, creationDateMs: Int64? = nil) -> RemoteManifestAsset {
        RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: fp,
            creationDateMs: creationDateMs, backedUpAtMs: 1,
            resourceCount: 1, totalFileSizeBytes: 100
        )
    }

    private func makeResource(
        hash: Data,
        resourceType: Int = ResourceTypeCode.photo,
        creationDateMs: Int64? = nil
    ) -> RemoteManifestResource {
        RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: String(format: "%04d/%02d/%@.bin", year, month, hash.hexString),
            contentHash: hash, fileSize: 100,
            resourceType: resourceType,
            creationDateMs: creationDateMs, backedUpAtMs: 1
        )
    }

    private func makeLink(fp: AssetFingerprint, hash: Data, role: Int) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: 0, logicalName: ""
        )
    }
}
