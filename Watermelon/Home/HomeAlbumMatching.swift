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
    static func buildRemoteItems(
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> [RemoteAlbumItem] {
        guard !assets.isEmpty else { return [] }

        let resourceLookup = RemoteResourceLookup(resources)

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
            var seenResourceFileNames = Set<String>()
            var seenHashes = Set<Data>()
            var skippedCount = 0

            for link in sortedLinks {
                guard let resource = resourceLookup.resource(for: link) else {
                    skippedCount += 1
                    continue
                }

                if seenResourceFileNames.insert(resource.fileName).inserted {
                    groupedResources.append(resource)
                }
                if seenHashes.insert(link.resourceHash).inserted {
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
                        creationDateMs: resource.creationDateMs,
                        storageCodec: resource.storageCodec,
                        storedFileSize: resource.storedFileSize,
                        encryptionKeyID: resource.encryptionKeyID
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

            let representative = chooseRepresentative(links: sortedLinks) { link in
                resourceLookup.resource(for: link)
            }
            guard let representative else { continue }
            // Canonical incompleteness rule (shared with the browser download path / manifest maintenance):
            // phantom, broken link, fingerprint-vs-linkset divergence, or metadata-only. Strictly stronger than
            // the old `skippedCount > 0`, so a fingerprint-divergent / metadata-only asset is no longer offered
            // for download (which would write a poisoned hash-index row).
            let isIncomplete = MonthManifestStore.isAssetIncomplete(
                links: sortedLinks,
                isLinkResourceAvailable: { resourceLookup.contains($0) },
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
        links: [RemoteAssetResourceLink],
        resourceForLink: (RemoteAssetResourceLink) -> RemoteManifestResource?
    ) -> RemoteManifestResource? {
        func firstResource(roleMatches: (Int) -> Bool) -> RemoteManifestResource? {
            for link in links where roleMatches(link.role) {
                if let resource = resourceForLink(link) { return resource }
            }
            return nil
        }
        return firstResource(roleMatches: ResourceRole.isPhotoSide)
            ?? firstResource(roleMatches: { !ResourceRole.isMetadataOnly($0) })
            ?? links.first.flatMap { resourceForLink($0) }
    }

    private static func detectMediaKind(roles: [Int]) -> AlbumMediaKind {
        let (isLivePhoto, isVideo) = ResourceRole.classify(roles: roles)
        if isLivePhoto { return .livePhoto }
        return isVideo ? .video : .photo
    }
}
