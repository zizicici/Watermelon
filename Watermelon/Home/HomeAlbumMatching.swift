import Foundation
import Photos

struct RemoteAlbumItem {
    let id: String
    let assetFingerprint: Data
    let creationDate: Date
    let resources: [RemoteManifestResource]
    let instances: [RemoteAssetResourceInstance]
    let representative: RemoteManifestResource
    let mediaKind: AlbumMediaKind
    let contentHashes: [Data]
    let isIncomplete: Bool
    let missingResourceCount: Int
}

enum HomeAlbumMatching {
    private struct ResourceLookupKey: Hashable {
        let year: Int
        let month: Int
        let hash: Data
    }

    static func buildRemoteItems(
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> [RemoteAlbumItem] {
        guard !assets.isEmpty else { return [] }

        let resourcesByMonthHash = Dictionary(uniqueKeysWithValues: resources.map { resource in
            (
                ResourceLookupKey(
                    year: resource.year,
                    month: resource.month,
                    hash: resource.contentHash
                ),
                resource
            )
        })

        var linksByAssetID: [String: [RemoteAssetResourceLink]] = [:]
        linksByAssetID.reserveCapacity(assets.count)
        for link in links {
            linksByAssetID[link.assetID, default: []].append(link)
        }

        var result: [RemoteAlbumItem] = []
        result.reserveCapacity(assets.count)

        for asset in assets {
            let sortedLinks = (linksByAssetID[asset.id] ?? []).sorted { lhs, rhs in
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
                return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
            }

            guard !sortedLinks.isEmpty else { continue }

            var groupedResources: [RemoteManifestResource] = []
            groupedResources.reserveCapacity(sortedLinks.count)
            var instances: [RemoteAssetResourceInstance] = []
            instances.reserveCapacity(sortedLinks.count)
            var contentHashes: [Data] = []
            contentHashes.reserveCapacity(sortedLinks.count)
            var seenHashes = Set<Data>()
            var skippedCount = 0

            for link in sortedLinks {
                let key = ResourceLookupKey(year: asset.year, month: asset.month, hash: link.resourceHash)
                guard let resource = resourcesByMonthHash[key] else {
                    skippedCount += 1
                    continue
                }

                if seenHashes.insert(link.resourceHash).inserted {
                    groupedResources.append(resource)
                    contentHashes.append(link.resourceHash)
                }

                instances.append(
                    RemoteAssetResourceInstance(
                        role: link.role,
                        slot: link.slot,
                        resourceHash: link.resourceHash,
                        fileName: resource.fileName,
                        fileSize: resource.fileSize,
                        remoteRelativePath: resource.remoteRelativePath,
                        creationDateMs: resource.creationDateMs
                    )
                )
            }

            guard let representative = chooseRepresentativeResource(groupedResources) else { continue }
            result.append(
                RemoteAlbumItem(
                    id: asset.id,
                    assetFingerprint: asset.assetFingerprint,
                    creationDate: asset.creationDate,
                    resources: groupedResources,
                    instances: instances,
                    representative: representative,
                    mediaKind: detectMediaKind(from: groupedResources),
                    contentHashes: contentHashes,
                    isIncomplete: skippedCount > 0,
                    missingResourceCount: skippedCount
                )
            )
        }

        return result
    }

    private static func chooseRepresentativeResource(_ resources: [RemoteManifestResource]) -> RemoteManifestResource? {
        let preferred = resources.first {
            ResourceTypeCode.isPhotoLike($0.resourceType)
        }
        return preferred ?? resources.first
    }

    private static func detectMediaKind(from resources: [RemoteManifestResource]) -> AlbumMediaKind {
        let hasPairedVideo = resources.contains { ResourceTypeCode.isPairedVideo($0.resourceType) }
        let hasPhotoLike = resources.contains { ResourceTypeCode.isPhotoLike($0.resourceType) }
        if hasPairedVideo, hasPhotoLike {
            return .livePhoto
        }

        let hasVideo = resources.contains { ResourceTypeCode.isVideoLike($0.resourceType) }
        return hasVideo ? .video : .photo
    }
}
