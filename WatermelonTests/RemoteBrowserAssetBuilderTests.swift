import XCTest
@testable import Watermelon

// The remote/merged browser shows MEANINGFUL records (complete or partial-but-has-media), flagged when
// incomplete so the user decides at download time. The meaningless ones are dropped: a phantom (no resolvable
// resource) or a config-only record (only an adjustment sidecar) has no photo/video to show and isn't a real
// backup — the future "incomplete resources" entry will own those.
final class RemoteBrowserAssetBuilderTests: XCTestCase {
    private let year = 2024
    private let month = 3
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }

    private func resource(
        _ name: String,
        _ hash: Data,
        role: Int,
        storageCodec: Int = RemoteManifestResource.plaintextStorageCodec,
        storedFileSize: Int64? = nil,
        encryptionKeyID: String? = nil
    ) -> RemoteManifestResource {
        RemoteManifestResource(
            year: year,
            month: month,
            fileName: name,
            contentHash: hash,
            fileSize: 100,
            resourceType: role,
            creationDateMs: 0,
            backedUpAtMs: 0,
            storageCodec: storageCodec,
            storedFileSize: storedFileSize,
            encryptionKeyID: encryptionKeyID
        )
    }
    private func link(_ fp: Data, _ hash: Data, role: Int, slot: Int = 0) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash, role: role, slot: slot)
    }
    private func asset(_ fp: Data, count: Int) -> RemoteManifestAsset {
        RemoteManifestAsset(year: year, month: month, assetFingerprint: fp, creationDateMs: 0, backedUpAtMs: 0, resourceCount: count, totalFileSizeBytes: 100)
    }
    private func fingerprint(of links: [RemoteAssetResourceLink]) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: links.map { (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash) })
    }

    func testHasBackedUpMedia() {
        let hPhoto = Data([1]); let hMeta = Data([7]); let hMissing = Data([9])
        let available: Set<Data> = [hPhoto, hMeta]
        let isAvail: (Data) -> Bool = { available.contains($0) }
        // Photo present → backed up.
        XCTAssertTrue(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hPhoto, role: 1)], isResourceAvailable: isAvail))
        // Photo present + a missing paired video (role 5) → still backed up (it has real media).
        XCTAssertTrue(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hPhoto, role: 1), link(Data(), hMissing, role: 5)], isResourceAvailable: isAvail))
        // Only a config-only adjustment sidecar (role 7) → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hMeta, role: 7)], isResourceAvailable: isAvail))
        // A media role whose resource is absent → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [link(Data(), hMissing, role: 1)], isResourceAvailable: isAvail))
        // No links → not backed up.
        XCTAssertFalse(MonthManifestStore.hasBackedUpMedia(links: [], isResourceAvailable: isAvail))
    }

    func testBuilderFlagsIncompleteAndDropsUndisplayable() {
        let hPhoto = Data([1]); let hPhotoB = Data([2]); let hMissing = Data([9]); let hMeta = Data([7])

        // Complete photo (role 1) — shown, not incomplete.
        let completeLinks = [link(Data(), hPhoto, role: 1)]
        let fpComplete = fingerprint(of: completeLinks)
        let complete = completeLinks.map { link(fpComplete, $0.resourceHash, role: $0.role) }

        // Partial: role 1 present + role 5 (fullSizePhoto) missing → incomplete but has a resolvable link → shown, flagged.
        let partialLinks = [link(Data(), hPhotoB, role: 1), link(Data(), hMissing, role: 5)]
        let fpPartial = fingerprint(of: partialLinks)
        let partial = partialLinks.map { link(fpPartial, $0.resourceHash, role: $0.role) }

        // Metadata-only (role 7 == adjustmentData) present → no real media → meaningless → dropped.
        let metaLinks = [link(Data(), hMeta, role: 7)]
        let fpMeta = fingerprint(of: metaLinks)
        let meta = metaLinks.map { link(fpMeta, $0.resourceHash, role: $0.role) }

        // Fully unresolvable (its only resource is absent) → nothing to display → dropped.
        let fpGhost = Data([0xEE])
        let ghost = [link(fpGhost, Data([0xEF]), role: 1)]

        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [resource("a.jpg", hPhoto, role: 1), resource("b.jpg", hPhotoB, role: 1), resource("m.json", hMeta, role: 7)],
            assets: [asset(fpComplete, count: 1), asset(fpPartial, count: 2), asset(fpMeta, count: 1), asset(fpGhost, count: 1)],
            assetResourceLinks: complete + partial + meta + ghost
        )
        let state = RemoteLibrarySnapshotState(revision: 1, isFullSnapshot: true, monthDeltas: [delta], profileKey: "p")

        let built = RemoteBrowserAssetBuilder.build(from: state)
        let byFp = Dictionary(uniqueKeysWithValues: (built.assetsByMonth[monthKey] ?? []).map { ($0.fingerprint, $0.isIncomplete) })

        XCTAssertEqual(byFp[fpComplete], false, "complete asset shown, not flagged")
        XCTAssertEqual(byFp[fpPartial], true, "partial-but-has-media asset shown, flagged incomplete")
        XCTAssertNil(byFp[fpMeta], "config-only (metadata) asset is dropped (no real media, not a backup)")
        XCTAssertNil(byFp[fpGhost], "fully-unresolvable asset is dropped (nothing to display)")
    }

    func testBuilderCarriesEncryptedResourceStorageFields() {
        let hPhoto = Data([1])
        let links = [link(Data(), hPhoto, role: ResourceTypeCode.photo)]
        let fp = fingerprint(of: links)
        let delta = RemoteLibraryMonthDelta(
            month: monthKey,
            resources: [
                resource(
                    "opaque.wmenc",
                    hPhoto,
                    role: ResourceTypeCode.photo,
                    storageCodec: RemoteManifestResource.encryptedStorageCodec,
                    storedFileSize: 456,
                    encryptionKeyID: "key-1"
                )
            ],
            assets: [asset(fp, count: 1)],
            assetResourceLinks: links.map { link(fp, $0.resourceHash, role: $0.role) }
        )
        let state = RemoteLibrarySnapshotState(revision: 1, isFullSnapshot: true, monthDeltas: [delta], profileKey: "p")

        let item = RemoteBrowserAssetBuilder.build(from: state).assetsByMonth[monthKey]?.first

        XCTAssertEqual(item?.photoStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(item?.photoStoredFileSize, 456)
        XCTAssertEqual(item?.photoEncryptionKeyID, "key-1")
        XCTAssertEqual(item?.photoRemoteRelativePath, "2024/03/opaque.wmenc")
    }

    func testVideoOnlyItemUsesVideoStorageForThumbnailPolicy() {
        let item = MediaBrowserItem(
            id: "video",
            kind: .video,
            creationDateMs: 0,
            presence: .remoteOnly,
            localIdentifier: nil,
            fingerprint: Data([0x01]),
            photoRemoteRelativePath: nil,
            videoRemoteRelativePath: "2024/03/video.wmenc",
            videoStorageCodec: RemoteManifestResource.encryptedStorageCodec,
            videoEncryptionKeyID: "key-video",
            remoteMonth: monthKey
        )

        XCTAssertEqual(item.thumbnailStorageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(item.thumbnailEncryptionKeyID, "key-video")
    }
}
