import Foundation
import Photos

enum AlbumMediaKind {
    case photo
    case video
    case livePhoto
}

enum ItemSourceTag {
    case localOnly
    case remoteOnly
    case both
}

struct LocalAlbumItem {
    let id: String
    let asset: PHAsset
    let creationDate: Date
    let isBackedUp: Bool
    let mediaKind: AlbumMediaKind
    let contentHashes: [Data]
    let fingerprint: Data?
}

struct RemoteAlbumItem {
    let id: String
    let assetFingerprint: Data
    let creationDate: Date
    let resources: [RemoteManifestResource]
    let instances: [RemoteAssetResourceInstance]
    let representative: RemoteManifestResource
    let mediaKind: AlbumMediaKind
    let contentHashes: [Data]
}

struct HomeAlbumItem {
    let id: String
    let creationDate: Date
    let sourceTag: ItemSourceTag
    let mediaKind: AlbumMediaKind
    let localItem: LocalAlbumItem?
    let remoteItem: RemoteAlbumItem?
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

            for link in sortedLinks {
                let key = ResourceLookupKey(year: asset.year, month: asset.month, hash: link.resourceHash)
                guard let resource = resourcesByMonthHash[key] else { continue }

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
                    contentHashes: contentHashes
                )
            )
        }

        return result
    }

    static func mergeItems(
        localItems: [LocalAlbumItem],
        remoteItems: [RemoteAlbumItem],
        localAssetIdentifierByHash: [Data: [String]],
        hasActiveConnection: Bool
    ) -> [HomeAlbumItem] {
        let localByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, $0) })
        let localHashSetByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, Set($0.contentHashes)) })
        var consumedLocalIDs = Set<String>()
        var result: [HomeAlbumItem] = []
        result.reserveCapacity(localItems.count + remoteItems.count)

        if hasActiveConnection {
            // Pass 1: Fingerprint-exact match (highest confidence)
            var localIDByFingerprint: [Data: String] = [:]
            for local in localItems {
                if let fp = local.fingerprint, localIDByFingerprint[fp] == nil {
                    localIDByFingerprint[fp] = local.id
                }
            }

            var unmatchedRemotes: [RemoteAlbumItem] = []
            for remote in remoteItems {
                if let localID = localIDByFingerprint[remote.assetFingerprint],
                   !consumedLocalIDs.contains(localID),
                   let local = localByID[localID] {
                    consumedLocalIDs.insert(localID)
                    result.append(
                        HomeAlbumItem(
                            id: "both:\(local.id)",
                            creationDate: local.creationDate,
                            sourceTag: .both,
                            mediaKind: mergeMediaKind(local: local.mediaKind, remote: remote.mediaKind),
                            localItem: local,
                            remoteItem: remote
                        )
                    )
                } else {
                    unmatchedRemotes.append(remote)
                }
            }

            // Pass 2: Content-hash fallback for unmatched remotes (only considers locals without a fingerprint)
            for remote in unmatchedRemotes {
                var candidateLocalIDSet = Set<String>()
                for hash in remote.contentHashes {
                    guard let localIDs = localAssetIdentifierByHash[hash] else { continue }
                    for localID in localIDs where !consumedLocalIDs.contains(localID) {
                        guard let local = localByID[localID], local.fingerprint == nil else { continue }
                        candidateLocalIDSet.insert(localID)
                    }
                }

                let localID = bestLocalID(
                    candidateLocalIDs: Array(candidateLocalIDSet),
                    remote: remote,
                    localByID: localByID,
                    localHashSetByID: localHashSetByID
                )

                if let localID, let local = localByID[localID] {
                    consumedLocalIDs.insert(localID)
                    result.append(
                        HomeAlbumItem(
                            id: "both:\(local.id)",
                            creationDate: local.creationDate,
                            sourceTag: .both,
                            mediaKind: mergeMediaKind(local: local.mediaKind, remote: remote.mediaKind),
                            localItem: local,
                            remoteItem: remote
                        )
                    )
                } else {
                    result.append(
                        HomeAlbumItem(
                            id: "remote:\(remote.id)",
                            creationDate: remote.creationDate,
                            sourceTag: .remoteOnly,
                            mediaKind: remote.mediaKind,
                            localItem: nil,
                            remoteItem: remote
                        )
                    )
                }
            }
        }

        for local in localItems where !consumedLocalIDs.contains(local.id) {
            result.append(
                HomeAlbumItem(
                    id: "local:\(local.id)",
                    creationDate: local.creationDate,
                    sourceTag: .localOnly,
                    mediaKind: local.mediaKind,
                    localItem: local,
                    remoteItem: nil
                )
            )
        }

        return result
    }

    private static func mergeMediaKind(local: AlbumMediaKind, remote: AlbumMediaKind) -> AlbumMediaKind {
        if local == .livePhoto || remote == .livePhoto {
            return .livePhoto
        }
        if local == .video || remote == .video {
            return .video
        }
        return .photo
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

    private static func bestLocalID(
        candidateLocalIDs: [String],
        remote: RemoteAlbumItem,
        localByID: [String: LocalAlbumItem],
        localHashSetByID: [String: Set<Data>]
    ) -> String? {
        let remoteHashSet = Set(remote.contentHashes)
        return candidateLocalIDs.max { lhs, rhs in
            guard let lhsLocal = localByID[lhs], let rhsLocal = localByID[rhs] else {
                return lhs < rhs
            }
            let lhsHashSet = localHashSetByID[lhs] ?? []
            let rhsHashSet = localHashSetByID[rhs] ?? []

            let lhsExactMatch = !remoteHashSet.isEmpty && lhsHashSet == remoteHashSet
            let rhsExactMatch = !remoteHashSet.isEmpty && rhsHashSet == remoteHashSet
            if lhsExactMatch != rhsExactMatch {
                return !lhsExactMatch && rhsExactMatch
            }

            let lhsIntersection = lhsHashSet.intersection(remoteHashSet).count
            let rhsIntersection = rhsHashSet.intersection(remoteHashSet).count
            if lhsIntersection != rhsIntersection {
                return lhsIntersection < rhsIntersection
            }

            if lhsLocal.isBackedUp != rhsLocal.isBackedUp {
                return !lhsLocal.isBackedUp && rhsLocal.isBackedUp
            }

            let lhsDistance = abs(lhsLocal.creationDate.timeIntervalSince(remote.creationDate))
            let rhsDistance = abs(rhsLocal.creationDate.timeIntervalSince(remote.creationDate))
            if lhsDistance != rhsDistance {
                return lhsDistance > rhsDistance
            }

            return lhs > rhs
        }
    }
}
