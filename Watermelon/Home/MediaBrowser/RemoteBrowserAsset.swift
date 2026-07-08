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
    // Manifest-recorded content hashes for the display resources (nil for a legacy no-hash manifest) —
    // lets materialization verify downloaded bytes before persisting them under the fingerprint.
    let photoContentHash: Data?
    let videoContentHash: Data?
    // The manifest record is incomplete: only the resolvable subset can be downloaded, producing a new,
    // differently-fingerprinted asset. Shown (marked), not hidden — the user decides at download time.
    let isIncomplete: Bool

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
                let allLinks = linksByFingerprint[asset.assetFingerprint] ?? []
                // Show meaningful records (complete OR partial-but-has-media), flagged when incomplete so the
                // user is asked to confirm at download time. Drop the meaningless ones — a phantom (no resolvable
                // link) or a config-only record (only an adjustment sidecar resolves) has no photo/video to show
                // and isn't a real backup; the future "incomplete resources" entry will own those.
                let isIncomplete = MonthManifestStore.isAssetIncomplete(links: allLinks, isResourceAvailable: { resourceByHash[$0] != nil }, assetFingerprint: asset.assetFingerprint)
                let links = allLinks.filter { resourceByHash[$0.resourceHash] != nil }
                guard ResourceRole.containsRealMedia(links.map(\.role)) else { continue }
                items.append(makeAsset(asset: asset, links: links, resourceByHash: resourceByHash, month: delta.month, isIncomplete: isIncomplete))
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
        month: LibraryMonthKey,
        isIncomplete: Bool
    ) -> RemoteBrowserAsset {
        // Classification and side-picking both derive from ResourceRole, so a Live Photo's photo/video side
        // is always resolvable (picker priority == classifier set — no drift).
        let (isLivePhoto, isVideo) = ResourceRole.classify(roles: links.map(\.role))

        func resource(preferring rolePriority: [Int]) -> RemoteManifestResource? {
            for role in rolePriority {
                if let link = links.first(where: { $0.role == role && $0.slot == 0 }) ?? links.first(where: { $0.role == role }),
                   let resource = resourceByHash[link.resourceHash] {
                    return resource
                }
            }
            return nil
        }
        let photoResource = resource(preferring: ResourceRole.photoSidePriority)
        let videoResource = resource(preferring: ResourceRole.videoSidePriority)

        return RemoteBrowserAsset(
            fingerprint: asset.assetFingerprint,
            month: month,
            creationDateMs: asset.creationDateMs ?? asset.backedUpAtMs,
            isVideo: isVideo,
            isLivePhoto: isLivePhoto,
            photoRemoteRelativePath: photoResource?.remoteRelativePath,
            videoRemoteRelativePath: videoResource?.remoteRelativePath,
            photoContentHash: recordedHash(photoResource),
            videoContentHash: recordedHash(videoResource),
            isIncomplete: isIncomplete
        )
    }

    private static func recordedHash(_ resource: RemoteManifestResource?) -> Data? {
        guard let hash = resource?.contentHash, !hash.isEmpty else { return nil }
        return hash
    }
}
