import Foundation
import Photos
import os.log

private let albumMatchingLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeAlbumMatching")

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

        // Same hash → same content; SQL unique index should make collisions unreachable.
        var resourcesByMonthHash: [ResourceLookupKey: RemoteManifestResource] = [:]
        resourcesByMonthHash.reserveCapacity(resources.count)
        for resource in resources {
            let key = ResourceLookupKey(
                year: resource.year,
                month: resource.month,
                hash: resource.contentHash
            )
            if let existing = resourcesByMonthHash[key] {
                albumMatchingLog.error("[HomeAlbumMatching] duplicate resource hash month=\(resource.year)-\(resource.month) hash=\(resource.contentHashHex, privacy: .public) existing=\(existing.fileName, privacy: .public) duplicate=\(resource.fileName, privacy: .public)")
                assertionFailure("Duplicate (year, month, contentHash) in buildRemoteItems")
                continue
            }
            resourcesByMonthHash[key] = resource
        }

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

            // Classify by the per-asset LINK role, not `resource.resourceType`: a resource row is content-addressed
            // and deduped, so its stored type can diverge from the role a given link uses for it. Every other path
            // (RemoteBrowserAssetBuilder / RemoteMonthResolver / hasBackedUpMedia) keys on link role — match them.
            let roles = instances.map(\.role)

            // Config-only / phantom records aren't a real backup: they have no restorable media, would inflate the
            // incomplete-download prompt count, and under `.createNewAsset` a base-less adjustmentData import throws
            // from PHAssetCreationRequest and aborts the whole month's restore. Drop them here too (one rule).
            guard ResourceRole.containsRealMedia(roles) else { continue }

            let representative = chooseRepresentative(instances: instances) { hash in
                resourcesByMonthHash[ResourceLookupKey(year: asset.year, month: asset.month, hash: hash)]
            }
            guard let representative else { continue }
            // Canonical incompleteness rule (shared with the browser download path / manifest maintenance):
            // phantom, broken link, fingerprint-vs-linkset divergence, or metadata-only. Strictly stronger than
            // the old `skippedCount > 0`, so a fingerprint-divergent / metadata-only asset is no longer offered
            // for download (which would write a poisoned hash-index row).
            let isIncomplete = MonthManifestStore.isAssetIncomplete(
                links: sortedLinks,
                isResourceAvailable: { hash in
                    resourcesByMonthHash[ResourceLookupKey(year: asset.year, month: asset.month, hash: hash)] != nil
                },
                assetFingerprint: asset.assetFingerprint
            )
            result.append(
                RemoteAlbumItem(
                    id: asset.id,
                    assetFingerprint: asset.assetFingerprint,
                    creationDate: asset.creationDate,
                    resources: groupedResources,
                    instances: instances,
                    representative: representative,
                    mediaKind: detectMediaKind(roles: roles),
                    contentHashes: contentHashes,
                    isIncomplete: isIncomplete,
                    missingResourceCount: skippedCount
                )
            )
        }

        return result
    }

    // Representative resource for the thumbnail, chosen by the link ROLE (not resource.resourceType). Prefer the
    // photo side; never a metadata-only sidecar (callers have already dropped records with no real media).
    private static func chooseRepresentative(
        instances: [RemoteAssetResourceInstance],
        resourceForHash: (Data) -> RemoteManifestResource?
    ) -> RemoteManifestResource? {
        func firstResource(roleMatches: (Int) -> Bool) -> RemoteManifestResource? {
            for instance in instances where roleMatches(instance.role) {
                if let resource = resourceForHash(instance.resourceHash) { return resource }
            }
            return nil
        }
        return firstResource(roleMatches: ResourceRole.isPhotoSide)
            ?? firstResource(roleMatches: { !ResourceRole.isMetadataOnly($0) })
            ?? instances.first.flatMap { resourceForHash($0.resourceHash) }
    }

    private static func detectMediaKind(roles: [Int]) -> AlbumMediaKind {
        let (isLivePhoto, isVideo) = ResourceRole.classify(roles: roles)
        if isLivePhoto { return .livePhoto }
        return isVideo ? .video : .photo
    }
}
