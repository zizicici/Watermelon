import XCTest
@testable import Watermelon

/// RemoteMonthResolver is the single source of truth for resolving a remote month's raw manifest into the
/// displayable intermediate: partial-flush drop, media classification, and hash-deduped byte sums.
final class RemoteMonthResolverTests: XCTestCase {
    private let key = LibraryMonthKey(year: 2024, month: 1)

    private func resolve(
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> RemoteMonthResolved {
        RemoteMonthResolver.resolve(month: key, resources: resources, assets: assets, links: links)
    }

    func testResolve_plainPhoto() {
        let fp = Data([0x01])
        let hash = Data([0xA1])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(year: 2024, month: 1, contentHash: hash, fileSize: 100)],
            links: [TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: hash)]
        )
        XCTAssertEqual(r.assetCount, 1)
        XCTAssertEqual(r.photoCount, 1)
        XCTAssertEqual(r.videoCount, 0)
        XCTAssertEqual(r.totalSizeBytes, 100)
        XCTAssertEqual(r.fingerprints, [fp])
    }

    func testResolve_emptyAssets_zeroCount() {
        let r = resolve(assets: [], resources: [], links: [])
        XCTAssertEqual(r.assetCount, 0)
        XCTAssertTrue(r.fingerprints.isEmpty)
        XCTAssertEqual(r.totalSizeBytes, 0)
    }

    func testResolve_dropsAssetWithoutResolvableLink() {
        // Partial-flush: assets + links can land before resource rows. Such orphans must not contribute,
        // otherwise matchedCount would over-report against locals whose hashes we can't serve.
        let fp = Data([0x60])
        let absentHash = Data([0xFF])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [],
            links: [TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: absentHash)]
        )
        XCTAssertEqual(r.assetCount, 0)
        XCTAssertTrue(r.fingerprints.isEmpty)
        XCTAssertEqual(r.totalSizeBytes, 0)
    }

    func testResolve_dropsConfigOnlyAsset() {
        // A record whose only resolvable resource is a config-only adjustment sidecar (role 7) is not a real
        // backup — it must not count or contribute a fingerprint (else it masks a local upload need).
        let fp = Data([0x90])
        let metaHash = Data([0x91])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(
                year: 2024, month: 1, contentHash: metaHash, fileSize: 40, resourceType: ResourceTypeCode.adjustmentData
            )],
            links: [TestFixtures.remoteLink(
                year: 2024, month: 1, assetFingerprint: fp, resourceHash: metaHash, role: ResourceTypeCode.adjustmentData
            )]
        )
        XCTAssertEqual(r.assetCount, 0)
        XCTAssertTrue(r.fingerprints.isEmpty)
        XCTAssertEqual(r.totalSizeBytes, 0)
    }

    func testResolve_keepsPhotoWithAdjustmentSidecar() {
        // A real photo plus an adjustment sidecar is a meaningful backup — the config-only drop must not
        // over-prune a record that also has media.
        let fp = Data([0x92])
        let photoHash = Data([0x93])
        let metaHash = Data([0x94])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [
                TestFixtures.remoteResource(year: 2024, month: 1, contentHash: photoHash, fileSize: 100, resourceType: ResourceTypeCode.photo),
                TestFixtures.remoteResource(year: 2024, month: 1, contentHash: metaHash, fileSize: 40, resourceType: ResourceTypeCode.adjustmentData)
            ],
            links: [
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: photoHash, role: ResourceTypeCode.photo, slot: 0),
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: metaHash, role: ResourceTypeCode.adjustmentData, slot: 0)
            ]
        )
        XCTAssertEqual(r.assetCount, 1)
        XCTAssertEqual(r.photoCount, 1)
        XCTAssertEqual(r.fingerprints, [fp])
        XCTAssertEqual(r.totalSizeBytes, 140, "both deduped resolvable hashes summed")
    }

    func testResolve_videoOnly_classifiedAsVideo() {
        let fp = Data([0x80])
        let videoHash = Data([0x81])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [TestFixtures.remoteResource(
                year: 2024, month: 1, contentHash: videoHash, fileSize: 500, resourceType: ResourceTypeCode.video
            )],
            links: [TestFixtures.remoteLink(
                year: 2024, month: 1, assetFingerprint: fp, resourceHash: videoHash, role: ResourceTypeCode.video
            )]
        )
        XCTAssertEqual(r.videoCount, 1)
        XCTAssertEqual(r.photoCount, 0)
        XCTAssertEqual(r.totalSizeBytes, 500)
    }

    func testResolve_pairedVideoPlusPhotoLike_classifiedAsPhoto() {
        // livePhoto folds into photoCount (two-bucket taxonomy); bytes sum the deduped resolvable hashes.
        let fp = Data([0x70])
        let photoHash = Data([0x71])
        let videoHash = Data([0x72])
        let r = resolve(
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [
                TestFixtures.remoteResource(
                    year: 2024, month: 1, contentHash: photoHash, fileSize: 80, resourceType: ResourceTypeCode.photo
                ),
                TestFixtures.remoteResource(
                    year: 2024, month: 1, contentHash: videoHash, fileSize: 120, resourceType: ResourceTypeCode.pairedVideo
                )
            ],
            links: [
                TestFixtures.remoteLink(
                    year: 2024, month: 1, assetFingerprint: fp, resourceHash: photoHash, role: ResourceTypeCode.photo, slot: 0
                ),
                TestFixtures.remoteLink(
                    year: 2024, month: 1, assetFingerprint: fp, resourceHash: videoHash, role: ResourceTypeCode.pairedVideo, slot: 1
                )
            ]
        )
        XCTAssertEqual(r.assetCount, 1)
        XCTAssertEqual(r.photoCount, 1, "livePhoto folds into photoCount")
        XCTAssertEqual(r.videoCount, 0)
        XCTAssertEqual(r.totalSizeBytes, 200, "bytes summed across deduped resolvable hashes")
    }

    func testResolve_sizeDedupAcrossAssetsSharingHash() {
        // Two assets referencing the same resource hash contribute one resource on disk.
        let fpA = Data([0x01])
        let fpB = Data([0x02])
        let sharedHash = Data([0xAB])
        let r = resolve(
            assets: [
                TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fpA),
                TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fpB)
            ],
            resources: [TestFixtures.remoteResource(year: 2024, month: 1, contentHash: sharedHash, fileSize: 100)],
            links: [
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fpA, resourceHash: sharedHash),
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fpB, resourceHash: sharedHash)
            ]
        )
        XCTAssertEqual(r.assetCount, 2)
        XCTAssertEqual(r.photoCount, 2)
        XCTAssertEqual(r.fingerprints, [fpA, fpB])
        XCTAssertEqual(r.totalSizeBytes, 100, "shared hash counted once across assets")
    }
}
