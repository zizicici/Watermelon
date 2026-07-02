import Foundation
import Photos

// One browsable remote asset, projected from the remote manifest snapshot.
struct RemoteBrowserAsset: Hashable, Sendable {
    let fingerprint: Data
    let month: LibraryMonthKey
    let creationDateMs: Int64
    let isVideo: Bool
    let isLivePhoto: Bool
    let photoRemoteRelativePath: String?
    let videoRemoteRelativePath: String?

    var fingerprintHex: String { fingerprint.hexString }
}

// Projects a remote snapshot into per-month, date-sorted browser assets. Mirrors the resolver drop
// rule (assets without a resolvable link are skipped) and the display-resource priority used elsewhere.
enum RemoteBrowserAssetBuilder {
    static func build(from state: RemoteLibrarySnapshotState) -> (months: [LibraryMonthKey], assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]]) {
        var assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]] = [:]
        for delta in state.monthDeltas {
            let resourceByHash = Dictionary(delta.resources.map { ($0.contentHash, $0) }, uniquingKeysWith: { first, _ in first })
            var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
            for link in delta.assetResourceLinks {
                linksByFingerprint[link.assetFingerprint, default: []].append(link)
            }
            var items: [RemoteBrowserAsset] = []
            for asset in delta.assets {
                // Mirror RemoteMonthResolver's drop rule: only links whose resource is actually present
                // count (partial-flush assets are skipped), and classification runs over resolvable links.
                let links = (linksByFingerprint[asset.assetFingerprint] ?? [])
                    .filter { resourceByHash[$0.resourceHash] != nil }
                guard !links.isEmpty else { continue }
                items.append(makeAsset(asset: asset, links: links, resourceByHash: resourceByHash, month: delta.month))
            }
            items.sort { $0.creationDateMs > $1.creationDateMs }
            if !items.isEmpty { assetsByMonth[delta.month] = items }
        }
        let months = assetsByMonth.keys.sorted(by: >)
        return (months, assetsByMonth)
    }

    private static func makeAsset(
        asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        resourceByHash: [Data: RemoteManifestResource],
        month: LibraryMonthKey
    ) -> RemoteBrowserAsset {
        let roles = links.map(\.role)
        let hasPaired = roles.contains { ResourceTypeCode.isPairedVideo($0) }
        let hasPhoto = roles.contains { ResourceTypeCode.isPhotoLike($0) }
        let hasVideo = roles.contains { ResourceTypeCode.isVideoLike($0) }
        let isLivePhoto = hasPaired && hasPhoto
        // Match RemoteMonthResolver: anything with a video resource that isn't a Live Photo is a video.
        let isVideo = !isLivePhoto && hasVideo

        func resource(preferring rolePriority: [Int]) -> RemoteManifestResource? {
            for role in rolePriority {
                if let link = links.first(where: { $0.role == role && $0.slot == 0 }) ?? links.first(where: { $0.role == role }),
                   let resource = resourceByHash[link.resourceHash] {
                    return resource
                }
            }
            return nil
        }
        let photoResource = resource(preferring: [
            ResourceTypeCode.photo, ResourceTypeCode.fullSizePhoto,
            ResourceTypeCode.alternatePhoto, ResourceTypeCode.photoProxy,
        ])
        // Include every paired-video variant that isVideoLike/isPairedVideo recognise — otherwise a Live
        // Photo whose paired clip is a fullSize/adjustmentBase paired video gets flagged Live but has no
        // video path (reconstruction fails, falls back to a still).
        let videoResource = resource(preferring: [
            ResourceTypeCode.video, ResourceTypeCode.fullSizeVideo, ResourceTypeCode.pairedVideo,
            ResourceTypeCode.fullSizePairedVideo, ResourceTypeCode.adjustmentBasePairedVideo,
            ResourceTypeCode.adjustmentBaseVideo,
        ])

        return RemoteBrowserAsset(
            fingerprint: asset.assetFingerprint,
            month: month,
            creationDateMs: asset.creationDateMs ?? asset.backedUpAtMs,
            isVideo: isVideo,
            isLivePhoto: isLivePhoto,
            photoRemoteRelativePath: photoResource?.remoteRelativePath,
            videoRemoteRelativePath: videoResource?.remoteRelativePath
        )
    }
}
