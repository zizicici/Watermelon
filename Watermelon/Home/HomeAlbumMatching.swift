import Foundation
import Photos
import os.log

private let albumMatchingLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeAlbumMatching")

struct RemoteAlbumItem {
    let id: String
    let assetFingerprint: AssetFingerprint
    let creationDate: Date
    /// Per-asset truth from the asset row. Restore must use this, not resource-instance
    /// dates, which are stamped from whichever asset committed a shared content path.
    let creationDateMs: Int64?
    let resources: [RemoteManifestResource]
    let instances: [RemoteAssetResourceInstance]
    let representative: RemoteManifestResource
    let mediaKind: AlbumMediaKind
    let contentHashes: [Data]
    /// Single source for restorability — downloads require a full-fingerprint match.
    let integrityState: AssetIntegrityState
    let missingResourceCount: Int

    var isIncomplete: Bool {
        missingResourceCount > 0 || !integrityState.isHealthy
    }

    var isRestorable: Bool {
        integrityState.allowsRestore
    }

    var isFingerprintMismatch: Bool {
        if case .fingerprintMismatch = integrityState { return true }
        return false
    }
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
        links: [RemoteAssetResourceLink],
        presenceByMonth: [LibraryMonthKey: RemotePresenceSnapshot.Month] = [:]
    ) -> [RemoteAlbumItem] {
        guard !assets.isEmpty else { return [] }

        // V2 multi-writer can publish the same content under different physical paths;
        // keep all of them so restore can fall back through alternates on download failure.
        // Picked path for the chosen representative is lex-min for deterministic display.
        var resourcesByMonthHash: [ResourceLookupKey: [RemoteManifestResource]] = [:]
        resourcesByMonthHash.reserveCapacity(resources.count)
        for resource in resources {
            let key = ResourceLookupKey(
                year: resource.year,
                month: resource.month,
                hash: resource.contentHash
            )
            resourcesByMonthHash[key, default: []].append(resource)
        }
        for key in resourcesByMonthHash.keys {
            resourcesByMonthHash[key]?.sort { $0.physicalRemotePath < $1.physicalRemotePath }
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
                // Per-link month lookup, not asset.year/month: V2 invariant says they
                // match, but a cross-month link from corrupt manifest / V1 residue would
                // bypass the missing-check via the asset-keyed shortcut.
                let linkMonth = LibraryMonthKey(year: link.year, month: link.month)
                let monthMissing = (presenceByMonth[linkMonth] ?? .absent).missingHashes
                if monthMissing.contains(link.resourceHash) {
                    skippedCount += 1
                    continue
                }
                let key = ResourceLookupKey(year: link.year, month: link.month, hash: link.resourceHash)
                let candidates = resourcesByMonthHash[key] ?? []
                guard let primary = candidates.first else {
                    skippedCount += 1
                    continue
                }
                let alternates = candidates.dropFirst().map(\.physicalRemotePath)

                if seenHashes.insert(link.resourceHash).inserted {
                    groupedResources.append(primary)
                    contentHashes.append(link.resourceHash)
                }

                // Prefer the link's logicalName — it's this asset's original filename.
                // `primary.logicalName` is the lex-min physical-path filename, which
                // can differ when multi-writer multi-path uploaded the same hash under
                // different collision-renamed names; using primary's would leak the
                // wrong original name into Photos on restore.
                let originalFileName = link.logicalName.isEmpty ? primary.logicalName : link.logicalName
                instances.append(
                    RemoteAssetResourceInstance(
                        role: link.role,
                        slot: link.slot,
                        resourceHash: link.resourceHash,
                        fileName: originalFileName,
                        fileSize: primary.fileSize,
                        remoteRelativePath: primary.physicalRemotePath,
                        alternateRemoteRelativePaths: Array(alternates),
                        creationDateMs: primary.creationDateMs
                    )
                )
            }

            guard let representative = chooseRepresentativeResource(groupedResources) else { continue }
            // contentHashes was already filtered above — classifier sees missing
            // hashes as absent from `availableHashes`, partial vs full as usual.
            let availableHashes = Set(contentHashes)
            let integrity = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: asset.assetFingerprint,
                links: sortedLinks,
                isResourceAvailable: { availableHashes.contains($0) }
            )
            result.append(
                RemoteAlbumItem(
                    id: asset.id,
                    assetFingerprint: asset.assetFingerprint,
                    creationDate: asset.creationDate,
                    creationDateMs: asset.creationDateMs,
                    resources: groupedResources,
                    instances: instances,
                    representative: representative,
                    mediaKind: detectMediaKind(from: groupedResources),
                    contentHashes: contentHashes,
                    integrityState: integrity,
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
