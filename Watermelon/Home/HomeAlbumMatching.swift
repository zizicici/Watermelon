import Foundation

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
    let resourceLinks: [RemoteAssetResourceLink]
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
    struct MatchComputation {
        let matchedCount: Int
        let remoteOnlyItems: [RemoteAlbumItem]
    }

    private struct MatchOutcome {
        let matchedPairs: [(local: LocalAlbumItem, remote: RemoteAlbumItem)]
        let remoteOnlyItems: [RemoteAlbumItem]
        let consumedLocalIDs: Set<String>
    }

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
            var contentHashes: [Data] = []
            contentHashes.reserveCapacity(sortedLinks.count)
            var seenHashes = Set<Data>()

            for link in sortedLinks {
                guard seenHashes.insert(link.resourceHash).inserted else { continue }
                let key = ResourceLookupKey(year: asset.year, month: asset.month, hash: link.resourceHash)
                if let resource = resourcesByMonthHash[key] {
                    groupedResources.append(resource)
                }
                contentHashes.append(link.resourceHash)
            }

            guard let representative = chooseRepresentativeResource(groupedResources) else { continue }
            result.append(
                RemoteAlbumItem(
                    id: asset.id,
                    assetFingerprint: asset.assetFingerprint,
                    creationDate: asset.creationDate,
                    resources: groupedResources,
                    resourceLinks: sortedLinks,
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
        let outcome = resolveMatches(
            localItems: localItems,
            remoteItems: remoteItems,
            localAssetIdentifierByHash: localAssetIdentifierByHash,
            hasActiveConnection: hasActiveConnection,
            includeRemoteOnlyItems: true
        )
        var result: [HomeAlbumItem] = []
        result.reserveCapacity(localItems.count + remoteItems.count)

        for pair in outcome.matchedPairs {
            result.append(
                HomeAlbumItem(
                    id: "both:\(pair.local.id)",
                    creationDate: pair.local.creationDate,
                    sourceTag: .both,
                    mediaKind: mergeMediaKind(local: pair.local.mediaKind, remote: pair.remote.mediaKind),
                    localItem: pair.local,
                    remoteItem: pair.remote
                )
            )
        }

        for remote in outcome.remoteOnlyItems {
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

        for local in localItems where !outcome.consumedLocalIDs.contains(local.id) {
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

    static func computeMatch(
        localItems: [LocalAlbumItem],
        remoteItems: [RemoteAlbumItem],
        localAssetIdentifierByHash: [Data: [String]],
        hasActiveConnection: Bool,
        includeRemoteOnlyItems: Bool
    ) -> MatchComputation {
        let outcome = resolveMatches(
            localItems: localItems,
            remoteItems: remoteItems,
            localAssetIdentifierByHash: localAssetIdentifierByHash,
            hasActiveConnection: hasActiveConnection,
            includeRemoteOnlyItems: includeRemoteOnlyItems
        )

        return MatchComputation(
            matchedCount: outcome.matchedPairs.count,
            remoteOnlyItems: outcome.remoteOnlyItems
        )
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

    private static func resolveMatches(
        localItems: [LocalAlbumItem],
        remoteItems: [RemoteAlbumItem],
        localAssetIdentifierByHash: [Data: [String]],
        hasActiveConnection: Bool,
        includeRemoteOnlyItems: Bool
    ) -> MatchOutcome {
        guard hasActiveConnection, !remoteItems.isEmpty else {
            return MatchOutcome(matchedPairs: [], remoteOnlyItems: [], consumedLocalIDs: [])
        }

        let localByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, $0) })
        let localHashSetByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, Set($0.contentHashes)) })
        var consumedLocalIDs = Set<String>()
        var matchedPairs: [(local: LocalAlbumItem, remote: RemoteAlbumItem)] = []
        matchedPairs.reserveCapacity(min(localItems.count, remoteItems.count))

        var localIDByFingerprint: [Data: String] = [:]
        localIDByFingerprint.reserveCapacity(localItems.count)
        for local in localItems {
            if let fp = local.fingerprint, localIDByFingerprint[fp] == nil {
                localIDByFingerprint[fp] = local.id
            }
        }

        var unmatchedRemotes: [RemoteAlbumItem] = []
        unmatchedRemotes.reserveCapacity(remoteItems.count)
        for remote in remoteItems {
            if let localID = localIDByFingerprint[remote.assetFingerprint],
               !consumedLocalIDs.contains(localID),
               let local = localByID[localID] {
                consumedLocalIDs.insert(localID)
                matchedPairs.append((local: local, remote: remote))
            } else {
                unmatchedRemotes.append(remote)
            }
        }

        var remoteOnlyItems: [RemoteAlbumItem] = []
        if includeRemoteOnlyItems {
            remoteOnlyItems.reserveCapacity(unmatchedRemotes.count)
        }

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
                matchedPairs.append((local: local, remote: remote))
            } else if includeRemoteOnlyItems {
                remoteOnlyItems.append(remote)
            }
        }

        return MatchOutcome(
            matchedPairs: matchedPairs,
            remoteOnlyItems: remoteOnlyItems,
            consumedLocalIDs: consumedLocalIDs
        )
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
